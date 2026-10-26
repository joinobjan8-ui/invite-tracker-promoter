"""PROMOTER invite tracking bot - fast edition.

Speed design (what changed vs. the old version):
  * Telegram pacing no longer sleeps while holding a global lock, and only
    sendMessage is paced (getChatMember / deleteMessage / answerCallbackQuery
    are sent immediately).
  * Admin checks are only done when a message is actually restricted, and the
    result is cached.  Normal chat messages cost zero Telegram API calls.
  * Bot settings, invite links and the leaderboard are cached in memory.
  * Leaderboard = 2 Mongo queries (no getChatMember per row).
  * Anonymous admins (sent "as the group") are handled correctly.
  * Webhook auto-registration + self keep-alive so Render Free doesn't sleep.
"""
import os
import re
import time
import heapq
import logging
import threading
from collections import OrderedDict
from datetime import datetime, timezone
from html import escape
from concurrent.futures import ThreadPoolExecutor

import requests
from requests.adapters import HTTPAdapter
from flask import Flask, request, jsonify
from pymongo import MongoClient, ASCENDING

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("promoter-bot")

# --------------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------------- #
BOT_TOKEN = os.environ.get("BOT_TOKEN", "").strip()
GROUP_ID = os.environ.get("GROUP_ID", "").strip()
MONGODB_URI = os.environ.get("MONGODB_URI", "").strip()
DB_NAME = os.environ.get("DB_NAME", "promoter_giveaway").strip()
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "").strip()
RENDER_EXTERNAL_URL = os.environ.get("RENDER_EXTERNAL_URL", "").rstrip("/")
OWNER_ID = os.environ.get("OWNER_ID", "").strip()


def _env_int(name, default):
    try:
        return int(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default


KEEPALIVE_SECONDS = _env_int("KEEPALIVE_SECONDS", 300)
LINK_COOLDOWN_SECONDS = _env_int("LINK_COOLDOWN_SECONDS", 3)
TOP_N = max(1, min(_env_int("TOP_N", 5), 50))
AUTO_SET_WEBHOOK = os.environ.get("AUTO_SET_WEBHOOK", "1") != "0"
ANON_ADMIN_ID = 1087968824      # @GroupAnonymousBot
CHANNEL_BOT_ID = 136817688      # @Channel_Bot (linked-channel auto posts)

app = Flask(__name__)
executor = ThreadPoolExecutor(max_workers=16, thread_name_prefix="upd")

# --------------------------------------------------------------------------- #
# MongoDB
# --------------------------------------------------------------------------- #
client = (
    MongoClient(
        MONGODB_URI,
        serverSelectionTimeoutMS=8000,
        connectTimeoutMS=8000,
        socketTimeoutMS=10000,
        retryWrites=True,
        maxPoolSize=30,
    )
    if MONGODB_URI else None
)
db = client[DB_NAME] if client is not None else None


def _init_indexes():
    if db is None:
        return
    try:
        db.invite_links.create_index(
            [("chat_id", ASCENDING), ("inviter_id", ASCENDING)], unique=True
        )
        # Existing Atlas DB already has a unique (chat_id, invite_link) index.
        # Do NOT recreate it (IndexKeySpecsConflict).
        db.referrals.create_index(
            [("chat_id", ASCENDING), ("referred_id", ASCENDING)], unique=True
        )
        db.referrals.create_index([("chat_id", ASCENDING), ("inviter_id", ASCENDING)])
    except Exception:
        log.exception("Index creation failed (continuing anyway)")


_init_indexes()

# --------------------------------------------------------------------------- #
# Telegram HTTP layer
# --------------------------------------------------------------------------- #
API = f"https://api.telegram.org/bot{BOT_TOKEN}" if BOT_TOKEN else ""
_http = requests.Session()
_adapter = HTTPAdapter(pool_connections=4, pool_maxsize=40)
_http.mount("https://", _adapter)
_http.mount("http://", _adapter)

_bot_username = None

# Only message-sending methods are paced; the slot is *reserved* under the lock
# and the sleep happens outside it, so threads never block each other.
PACED_METHODS = {"sendMessage"}
CHAT_SEND_GAP = 0.35
MAX_TG_ATTEMPTS = 4
_pace_lock = threading.Lock()
_next_send_at = {}


def _pace_send(chat_id):
    if chat_id is None:
        return
    key = str(chat_id)
    with _pace_lock:
        now = time.monotonic()
        start = max(now, _next_send_at.get(key, 0.0))
        _next_send_at[key] = start + CHAT_SEND_GAP
    wait = start - now
    if wait > 0:
        time.sleep(wait)


def tg(method, payload=None):
    if not API:
        return {"ok": False, "description": "BOT_TOKEN is not configured"}
    payload = payload or {}
    last_error = None

    for attempt in range(1, MAX_TG_ATTEMPTS + 1):
        if method in PACED_METHODS:
            _pace_send(payload.get("chat_id"))
        try:
            response = _http.post(f"{API}/{method}", json=payload, timeout=(4, 15))
            try:
                result = response.json()
            except ValueError:
                result = {}

            if response.status_code == 429:
                params = result.get("parameters") or {}
                try:
                    retry_after = max(1, min(int(params.get("retry_after", 2)), 60))
                except (TypeError, ValueError):
                    retry_after = 2
                if attempt >= MAX_TG_ATTEMPTS:
                    return {"ok": False, "error_code": 429,
                            "description": result.get("description", "Too Many Requests")}
                log.warning("429 on %s, retry in %ss (%s/%s)", method, retry_after,
                            attempt, MAX_TG_ATTEMPTS)
                time.sleep(retry_after)
                continue

            if response.status_code >= 500:
                last_error = f"HTTP {response.status_code}"
                if attempt < MAX_TG_ATTEMPTS:
                    time.sleep(min(2 ** (attempt - 1), 4))
                    continue

            if not response.ok or not result.get("ok"):
                log.warning("Telegram %s failed (HTTP %s): %s", method,
                            response.status_code,
                            result.get("description") or response.text[:200])
                return result or {"ok": False, "error_code": response.status_code,
                                  "description": response.text[:200]}
            return result

        except (requests.Timeout, requests.ConnectionError) as exc:
            last_error = str(exc)
            if attempt < MAX_TG_ATTEMPTS:
                time.sleep(min(2 ** (attempt - 1), 4))
                continue
        except requests.RequestException as exc:
            last_error = str(exc)
            break

    log.error("Telegram %s exhausted retries: %s", method, last_error)
    return {"ok": False, "description": str(last_error or "Telegram request failed")}


def send(chat_id, text, **kwargs):
    payload = {"chat_id": chat_id, "text": text, "disable_web_page_preview": True}
    payload.update(kwargs)
    return tg("sendMessage", payload)


def get_bot_username():
    global _bot_username
    if _bot_username:
        return _bot_username
    result = tg("getMe")
    if result.get("ok"):
        _bot_username = result["result"].get("username")
    return _bot_username


# --------------------------------------------------------------------------- #
# Tiny scheduler (one thread instead of one Timer thread per message)
# --------------------------------------------------------------------------- #
_sched_heap = []
_sched_cv = threading.Condition()
_sched_seq = 0


def schedule(delay, fn, *args):
    global _sched_seq
    with _sched_cv:
        _sched_seq += 1
        heapq.heappush(_sched_heap, (time.monotonic() + delay, _sched_seq, fn, args))
        _sched_cv.notify()


def _scheduler_loop():
    while True:
        with _sched_cv:
            while not _sched_heap:
                _sched_cv.wait()
            due, _, fn, args = _sched_heap[0]
            now = time.monotonic()
            if due > now:
                _sched_cv.wait(due - now)
                continue
            heapq.heappop(_sched_heap)
        try:
            executor.submit(fn, *args)
        except Exception:
            log.exception("Scheduled task failed to queue")


def delete_message_later(chat_id, message_id, delay):
    if chat_id is None or not message_id:
        return
    schedule(max(1, int(delay)), lambda: tg("deleteMessage",
                                           {"chat_id": chat_id, "message_id": message_id}))


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def is_target_chat(chat):
    if str(chat.get("id")) == str(GROUP_ID):
        return True
    configured = GROUP_ID.lstrip("@").lower()
    username = str(chat.get("username", "")).lower()
    return bool(configured and username and configured == username)


def is_member_status(member):
    status = member.get("status", "")
    if status in ("creator", "administrator", "member"):
        return True
    return status == "restricted" and bool(member.get("is_member"))


def is_owner(user):
    return bool(OWNER_ID and str((user or {}).get("id", "")) == OWNER_ID)


def is_anonymous_admin(message):
    """Admin posting 'as the group' (anonymous) - Telegram sets sender_chat = group."""
    chat = message.get("chat") or {}
    sc = message.get("sender_chat") or {}
    sender = message.get("from") or {}
    if sender.get("id") == ANON_ADMIN_ID:
        return True
    return bool(sc and sc.get("id") == chat.get("id"))


def is_channel_post(message):
    chat = message.get("chat") or {}
    sc = message.get("sender_chat") or {}
    sender = message.get("from") or {}
    if sender.get("id") == CHANNEL_BOT_ID:
        return True
    return bool(sc and sc.get("id") != chat.get("id"))


def display_label(user):
    if user.get("username"):
        return "@" + user["username"]
    return user.get("first_name") or str(user.get("id", "friend"))


def mention_html(user):
    uid = int(user.get("id", 0))
    if user.get("username"):
        u = user["username"]
        return f'<a href="https://t.me/{escape(u, quote=True)}">@{escape(u)}</a>'
    return f'<a href="tg://user?id={uid}">{escape(str(user.get("first_name") or "Member"))}</a>'


# --------------------------------------------------------------------------- #
# Admin check (cached)
# --------------------------------------------------------------------------- #
ADMIN_CACHE_TTL = 60
_admin_cache = {}


def user_is_group_admin(chat_id, user_id, default=False):
    """Cached getChatMember. On API failure returns `default`."""
    key = (str(chat_id), int(user_id))
    hit = _admin_cache.get(key)
    now = time.monotonic()
    if hit and hit[1] > now:
        return hit[0]
    result = tg("getChatMember", {"chat_id": chat_id, "user_id": int(user_id)})
    if not result.get("ok"):
        return default
    status = (result.get("result") or {}).get("status")
    value = status in ("creator", "administrator")
    _admin_cache[key] = (value, now + ADMIN_CACHE_TTL)
    if len(_admin_cache) > 5000:
        _admin_cache.clear()
    return value


# --------------------------------------------------------------------------- #
# Settings (cached)
# --------------------------------------------------------------------------- #
DEFAULT_SETTINGS = {
    "welcome_enabled": False,
    "welcome_admin_enabled": False,
    "automsg_enabled": True,
    "automsg_interval_seconds": 3600,
    "automsg_duration_seconds": 10,
    "all_messages_enabled": True,
    "linkmedia_enabled": True,
    "autodelete_enabled": True,
    "autodelete_seconds": 15,
}
SETTINGS_TTL = 30
_settings_lock = threading.Lock()
_settings_cache = {"data": None, "at": 0.0}


def _settings():
    if db is None:
        return DEFAULT_SETTINGS
    now = time.monotonic()
    with _settings_lock:
        if _settings_cache["data"] is not None and now - _settings_cache["at"] < SETTINGS_TTL:
            return _settings_cache["data"]
    try:
        row = db.bot_settings.find_one({"_id": "global"}) or {}
        data = {k: row.get(k, v) for k, v in DEFAULT_SETTINGS.items()}
        with _settings_lock:
            _settings_cache["data"] = data
            _settings_cache["at"] = now
        return data
    except Exception:
        log.exception("Could not read bot settings")
        with _settings_lock:
            return _settings_cache["data"] or DEFAULT_SETTINGS


def get_setting(key):
    return _settings().get(key, DEFAULT_SETTINGS.get(key, True))


def set_setting(key, value):
    if db is not None:
        db.bot_settings.update_one({"_id": "global"}, {"$set": {key: value}}, upsert=True)
        with _settings_lock:
            data = dict(_settings_cache["data"] or DEFAULT_SETTINGS)
            data[key] = value
            _settings_cache["data"] = data
            _settings_cache["at"] = time.monotonic()
    else:
        DEFAULT_SETTINGS[key] = value


# --------------------------------------------------------------------------- #
# Invite links (cached)
# --------------------------------------------------------------------------- #
_link_cache = {}          # user_id -> link
_link_lock = threading.Lock()
_name_cache = {}          # user_id -> last stored display name


def make_invite_link(user):
    if db is None:
        return None, "MongoDB is not configured."

    user_id = int(user["id"])
    chat_key = str(GROUP_ID)
    label = display_label(user)

    cached = _link_cache.get(user_id)
    if cached:
        if _name_cache.get(user_id) != label:
            _name_cache[user_id] = label
            executor.submit(_refresh_inviter_name, chat_key, user_id, label)
        return cached, None

    existing = db.invite_links.find_one({"chat_id": chat_key, "inviter_id": user_id})
    if existing:
        _link_cache[user_id] = existing["invite_link"]
        _name_cache[user_id] = label
        if existing.get("inviter_name") != label:
            executor.submit(_refresh_inviter_name, chat_key, user_id, label)
        return existing["invite_link"], None

    result = tg("createChatInviteLink", {
        "chat_id": GROUP_ID,
        "name": f"promoter_{user_id}",
        "creates_join_request": False,
    })
    if not result.get("ok"):
        return None, result.get("description", "Could not create invite link.")

    link = result["result"]["invite_link"]
    db.invite_links.update_one(
        {"chat_id": chat_key, "inviter_id": user_id},
        {"$setOnInsert": {
            "chat_id": chat_key,
            "inviter_id": user_id,
            "invite_link": link,
            "inviter_name": label,
            "created_at": datetime.now(timezone.utc),
        }},
        upsert=True,
    )
    saved = db.invite_links.find_one({"chat_id": chat_key, "inviter_id": user_id})
    final = (saved or {}).get("invite_link", link)
    _link_cache[user_id] = final
    _name_cache[user_id] = label
    return final, None


def _refresh_inviter_name(chat_key, user_id, label):
    try:
        db.invite_links.update_one({"chat_id": chat_key, "inviter_id": user_id},
                                   {"$set": {"inviter_name": label}})
    except Exception:
        log.exception("Could not refresh inviter name")


# Simple per-user cooldown (silently drops spam so the queue never backs up)
_cooldown = {}


def on_cooldown(user_id, key="link"):
    if LINK_COOLDOWN_SECONDS <= 0:
        return False
    now = time.monotonic()
    k = (key, int(user_id))
    last = _cooldown.get(k, 0.0)
    if now - last < LINK_COOLDOWN_SECONDS:
        return True
    _cooldown[k] = now
    if len(_cooldown) > 20000:
        _cooldown.clear()
    return False


# --------------------------------------------------------------------------- #
# Keyboards / messages
# --------------------------------------------------------------------------- #
def private_link_keyboard(link):
    return {
        "inline_keyboard": [
            [{"text": "📋 Copy My Invite Link", "copy_text": {"text": link}}],
            [{"text": "📣 Share My Link",
              "url": "https://t.me/share/url?url=" + requests.utils.quote(link, safe="")}],
        ]
    }


def action_keyboard():
    return {"inline_keyboard": [[
        {"text": "🔗 /mylink", "callback_data": "generate_mylink"},
        {"text": "🏆 /topinviter", "callback_data": "show_topinviter"},
    ]]}


def send_personal_link(chat_id, user):
    link, error = make_invite_link(user)
    if error:
        send(chat_id,
             "❌ I couldn't create your personal invite link.\n\n"
             "Please make sure the bot is an admin and has permission to "
             f"manage invite links.\n\nDetails: {error}")
        return
    message = (
        "🎉 PROMOTER 500 TON GIVEAWAY!\n\n"
        f"👤 Invite link owner: {display_label(user)}\n"
        "🔗 Your personal invite link is ready!\n\n"
        "Share this link with genuine friends. Tracked eligible new members "
        "will count toward your referral total.\n\n"
        "💎 Prize pool: 500 TON • Up to 100 winners\n"
        "📋 Use the buttons below to copy or share your link."
    )
    send(chat_id, message, reply_markup=private_link_keyboard(link))


def send_auto_delete(chat_id, text, delete_after=None, in_group=True, **kwargs):
    """Send; for group replies schedule deletion (single scheduler thread)."""
    result = send(chat_id, text, **kwargs)
    message_id = (result.get("result") or {}).get("message_id")
    if in_group and message_id:
        if delete_after is not None:
            delete_message_later(chat_id, message_id, delete_after)
        elif get_setting("autodelete_enabled"):
            try:
                delay = int(get_setting("autodelete_seconds"))
            except (TypeError, ValueError):
                delay = 15
            delete_message_later(chat_id, message_id, max(1, min(delay, 86400)))
    return result


def handle_mylink(chat_id, user, reply_to=None, in_group=True):
    link, error = make_invite_link(user)
    kw = {"parse_mode": "HTML"}
    if reply_to:
        kw["reply_to_message_id"] = reply_to
        kw["allow_sending_without_reply"] = True

    if error:
        send_auto_delete(
            chat_id,
            f"❌ {mention_html(user)}, I couldn't create your invite link. "
            "Please ask an admin to check the bot's <b>Invite users via link</b> permission.",
            delete_after=15, in_group=in_group, **kw)
        return

    notice = ""
    if in_group and get_setting("autodelete_enabled"):
        notice = f"\n⏳ This message will be deleted in {get_setting('autodelete_seconds')} seconds."
    send_auto_delete(
        chat_id,
        f"🔗 <b>{mention_html(user)}, your personal invite link:</b>\n"
        f"<code>{escape(link)}</code>\n\n"
        f"💎 Share it with genuine friends to join the 500 TON giveaway.{notice}",
        in_group=in_group,
        reply_markup=private_link_keyboard(link), **kw)


# --------------------------------------------------------------------------- #
# Leaderboard (2 queries, cached 10s)
# --------------------------------------------------------------------------- #
_lb_cache = {"rows": None, "at": 0.0}
LB_TTL = 10


def top_rows(chat_key):
    now = time.monotonic()
    if _lb_cache["rows"] is not None and now - _lb_cache["at"] < LB_TTL:
        return _lb_cache["rows"]
    pipeline = [
        {"$match": {"chat_id": chat_key}},
        {"$group": {"_id": "$inviter_id", "count": {"$sum": 1},
                    "fallback": {"$last": "$inviter_name"}}},
        {"$sort": {"count": -1, "_id": 1}},
        {"$limit": TOP_N},
    ]
    rows = list(db.referrals.aggregate(pipeline))
    ids = [int(r["_id"]) for r in rows]
    names = {}
    if ids:
        for doc in db.invite_links.find(
                {"chat_id": chat_key, "inviter_id": {"$in": ids}},
                {"inviter_id": 1, "inviter_name": 1, "_id": 0}):
            names[int(doc["inviter_id"])] = doc.get("inviter_name")
    out = []
    for r in rows:
        uid = int(r["_id"])
        out.append((names.get(uid) or r.get("fallback") or f"User {uid}", r["count"]))
    _lb_cache["rows"] = out
    _lb_cache["at"] = now
    return out


def leaderboard_text(user, in_group):
    chat_key = str(GROUP_ID)
    my_count = db.referrals.count_documents(
        {"chat_id": chat_key, "inviter_id": int(user["id"])})
    rows = top_rows(chat_key)

    lines = [
        "🏆 <b>PROMOTER INVITE LEADERBOARD</b>",
        f"💎 500 TON GIVEAWAY • TOP {TOP_N}",
        "",
        f"👤 <b>{mention_html(user)}, your invites:</b> {my_count}",
        "",
        f"🔥 <b>TOP {TOP_N} INVITERS</b>",
    ]
    if not rows:
        lines.append("No tracked invites yet. Be the first!")
    else:
        for rank, (name, count) in enumerate(rows, 1):
            lines.append(f"{rank}. {escape(str(name))} — <b>{count}</b> invite(s)")
    lines += ["", "📌 Only tracked, unique new members are counted.",
              "🚀 Tap /mylink to get your personal invite link."]
    if in_group and get_setting("autodelete_enabled"):
        lines.append(f"⏳ This message will be deleted in {get_setting('autodelete_seconds')} seconds.")
    return "\n".join(lines)


def handle_topinviter(chat_id, user, reply_to=None, in_group=True):
    if db is None:
        send(chat_id, "⚠️ MongoDB is not configured.")
        return
    kw = {"parse_mode": "HTML"}
    if reply_to:
        kw["reply_to_message_id"] = reply_to
        kw["allow_sending_without_reply"] = True
    send_auto_delete(chat_id, leaderboard_text(user, in_group), in_group=in_group, **kw)


# --------------------------------------------------------------------------- #
# Admin / owner controls
# --------------------------------------------------------------------------- #
ADMIN_CONTROL_COMMANDS = {
    "/help", "/owner",
    "/welcome_on", "/welcome_off", "/welcome_all_on", "/welcome_all_off",
    "/welcome_admin_on", "/welcome_admin_off",
    "/automsg_on", "/automsg_off", "/automsg_interval", "/automsg_duration",
    "/allmsg_on", "/allmsg_off", "/linkmedia_on", "/linkmedia_off",
    "/autodelete_on", "/autodelete_off", "/autodelete_time",
}

SETTING_COMMANDS = {
    "/welcome_off": ("welcome_enabled", False, "Group welcome messages disabled."),
    "/welcome_all_off": ("welcome_enabled", False, "Group welcome messages disabled."),
    "/welcome_admin_off": ("welcome_admin_enabled", False, "Private admin join notifications disabled."),
    "/automsg_on": ("automsg_enabled", True, "Automatic group reminders enabled."),
    "/automsg_off": ("automsg_enabled", False, "Automatic group reminders disabled."),
    "/allmsg_on": ("all_messages_enabled", True, "Normal member messages are allowed."),
    "/allmsg_off": ("all_messages_enabled", False, "All non-admin group messages will be deleted."),
    "/linkmedia_on": ("linkmedia_enabled", True, "Link/media/button filtering enabled."),
    "/linkmedia_off": ("linkmedia_enabled", False, "Link/media/button filtering disabled."),
    "/autodelete_on": ("autodelete_enabled", True, "Auto-delete enabled."),
    "/autodelete_off": ("autodelete_enabled", False, "Auto-delete disabled."),
}


def owner_help_text():
    s = _settings()
    return (
        "🛠 PROMOTER ADMIN COMMANDS\n\n"
        "/help — show this command list\n"
        "/welcome_on — show new-member welcome in the group\n"
        "/welcome_off — turn off group welcome\n"
        "/welcome_admin_on — send join notices to the owner's bot inbox\n"
        "/welcome_admin_off — turn off private admin join notices\n"
        "/automsg_on — enable recurring group reminder\n"
        "/automsg_off — disable recurring group reminder\n"
        "/automsg_interval 3600 — reminder interval in seconds (300–86400)\n"
        "/automsg_duration 10 — reminder visibility in seconds (1–86400)\n"
        "/allmsg_on — allow normal member messages\n"
        "/allmsg_off — delete all non-admin group messages\n"
        "/linkmedia_on — block links/photos/media/buttons from non-admins\n"
        "/linkmedia_off — disable link/media/button filter\n"
        "/autodelete_on — auto-delete bot replies to /mylink and /topinviter\n"
        "/autodelete_off — disable auto-delete\n"
        "/autodelete_time 15 — auto-delete delay in seconds (5–86400)\n\n"
        f"Current settings: group_welcome={s['welcome_enabled']}, "
        f"admin_join_notice={s['welcome_admin_enabled']}, "
        f"autodelete={s['autodelete_enabled']}, autodelete_seconds={s['autodelete_seconds']}, "
        f"automsg={s['automsg_enabled']}, automsg_interval_seconds={s['automsg_interval_seconds']}, "
        f"automsg_duration_seconds={s['automsg_duration_seconds']}, "
        f"allmsg={s['all_messages_enabled']}, linkmedia={s['linkmedia_enabled']}"
    )


def is_authorized_admin_command(message, command):
    """Owner, anonymous group admin, or a current group admin."""
    if command not in ADMIN_CONTROL_COMMANDS:
        return False
    sender = message.get("from") or {}
    if is_owner(sender):
        return True
    if is_anonymous_admin(message):
        return True      # Telegram only lets real admins post anonymously
    if not GROUP_ID or not sender.get("id") or sender.get("is_bot"):
        return False
    return user_is_group_admin(GROUP_ID, sender["id"], default=False)


def run_admin_command(message, command, text):
    chat_id = (message.get("chat") or {}).get("id")
    parts = text.split(maxsplit=1)

    if command in ("/help", "/owner"):
        send(chat_id, owner_help_text())
        return
    if command in ("/welcome_on", "/welcome_all_on"):
        set_setting("welcome_enabled", True)
        set_setting("welcome_admin_enabled", False)
        send(chat_id, "✅ Welcome messages will be posted in the group for all members.")
        return
    if command == "/welcome_admin_on":
        set_setting("welcome_enabled", False)
        set_setting("welcome_admin_enabled", True)
        send(chat_id, "✅ Group welcome disabled; join notices will be sent to OWNER_ID privately. "
                      "Start the bot in private first.")
        return
    if command in ("/automsg_interval", "/automsg_duration"):
        is_interval = command == "/automsg_interval"
        label = "interval" if is_interval else "duration"
        key = "automsg_interval_seconds" if is_interval else "automsg_duration_seconds"
        minimum = 300 if is_interval else 1
        if len(parts) != 2 or not parts[1].strip().isdigit():
            send(chat_id, f"Usage: /automsg_{label} {'3600' if is_interval else '10'} (seconds)")
            return
        seconds = int(parts[1].strip())
        if not minimum <= seconds <= 86400:
            send(chat_id, f"Please choose a {label} from {minimum} to 86400 seconds.")
            return
        set_setting(key, seconds)
        send(chat_id, f"✅ Automatic message {label} set to {seconds} seconds.")
        return
    if command == "/autodelete_time":
        if len(parts) != 2 or not parts[1].strip().isdigit():
            send(chat_id, "Usage: /autodelete_time 30 (seconds; minimum 5, maximum 86400)")
            return
        seconds = int(parts[1].strip())
        if not 5 <= seconds <= 86400:
            send(chat_id, "Please choose a delay from 5 to 86400 seconds.")
            return
        set_setting("autodelete_seconds", seconds)
        send(chat_id, f"✅ Auto-delete delay set to {seconds} seconds.")
        return
    if command in SETTING_COMMANDS:
        key, value, response_text = SETTING_COMMANDS[command]
        set_setting(key, value)
        send(chat_id, "✅ " + response_text)


# --------------------------------------------------------------------------- #
# Moderation (only calls Telegram when a message is actually restricted)
# --------------------------------------------------------------------------- #
URL_RE = re.compile(
    r"(?i)(?:https?://|www\.|t\.me/|telegram\.me/|telegram\.dog/|"
    r"\b[a-z0-9][a-z0-9.-]*\.(?:com|net|org|io|co|xyz|app|site|me|link|"
    r"click|live|online|info|biz|ru|us|uk|gg|dev|ai|shop|store)\b)"
)
MEDIA_FIELDS = ("photo", "video", "animation", "document", "audio", "voice",
                "video_note", "sticker")
SAFE_COMMANDS = {"/mylink", "/topinviter", "/start", "/help"}


def message_has_restricted_content(message):
    if (message.get("reply_markup") or {}).get("inline_keyboard"):
        return True
    if any(message.get(f) for f in MEDIA_FIELDS):
        return True
    text = message.get("text") or message.get("caption") or ""
    if URL_RE.search(text):
        return True
    for field in ("entities", "caption_entities"):
        for entity in message.get(field, []) or []:
            if entity.get("type") in ("url", "text_link"):
                return True
    return False


def moderate_group_message(message, command=""):
    """Delete restricted posts from non-admins. Returns True if deleted."""
    chat = message.get("chat") or {}
    if not is_target_chat(chat) or chat.get("type") not in ("group", "supergroup"):
        return False
    sender = message.get("from") or {}
    if not sender or sender.get("is_bot") or is_anonymous_admin(message) or is_channel_post(message):
        return False
    if is_owner(sender):
        return False

    # Decide cheaply (no network) whether this message would be removed at all.
    if not get_setting("all_messages_enabled"):
        restricted = command not in SAFE_COMMANDS
    else:
        restricted = bool(get_setting("linkmedia_enabled")) and message_has_restricted_content(message)
    if not restricted:
        return False

    # Only now ask Telegram. If the check fails, do NOT delete (never hit admins).
    if user_is_group_admin(chat.get("id"), sender.get("id"), default=True):
        return False

    result = tg("deleteMessage", {"chat_id": chat.get("id"),
                                  "message_id": message.get("message_id")})
    if not result.get("ok"):
        log.warning("Could not delete message: %s", result.get("description"))
    return True


# --------------------------------------------------------------------------- #
# Message handler
# --------------------------------------------------------------------------- #
def handle_message(message):
    text = (message.get("text") or "").strip()
    chat = message.get("chat") or {}
    sender = message.get("from") or {}
    chat_id = chat.get("id")
    chat_type = chat.get("type")
    message_id = message.get("message_id")

    command = ""
    if text.startswith("/"):
        raw = text.split(maxsplit=1)[0]
        command = raw.split("@")[0].lower()
        if "@" in raw:
            target = raw.split("@", 1)[1].lower()
            me = (get_bot_username() or "").lower()
            if me and target != me:
                return          # command meant for another bot

    # 1) Admin/owner controls first (so /allmsg_off can never lock admins out).
    if command and is_authorized_admin_command(message, command):
        run_admin_command(message, command, text)
        return

    # 2) Moderation (no API call unless the message is restricted).
    if moderate_group_message(message, command):
        return

    if not command:
        return

    # 3) Public commands.
    if is_anonymous_admin(message):
        if command in ("/mylink", "/topinviter") and chat_type != "private":
            send_auto_delete(
                chat_id,
                "ℹ️ You're posting as an anonymous admin. Turn off <b>Remain Anonymous</b> "
                "in your admin settings, or tap the buttons below.",
                delete_after=15, parse_mode="HTML",
                reply_markup=action_keyboard())
        return

    if not sender or sender.get("is_bot") or not sender.get("id"):
        return

    if command in ("/start", "/help"):
        if chat_type == "private":
            if text.partition(" ")[2].strip() == "mylink":
                send_personal_link(chat_id, sender)
                return
            send(chat_id,
                 "👋 Welcome to PROMOTER Giveaway Bot!\n\n"
                 "💎 500 TON GIVEAWAY • Up to 100 winners\n"
                 "Use /mylink to get your personal invite link.\n"
                 "Use /topinviter to view the leaderboard.")
        elif is_target_chat(chat):
            send_auto_delete(chat_id,
                             "👋 Welcome to PROMOTER!\nType /mylink in this group to get started.",
                             reply_to_message_id=message_id, allow_sending_without_reply=True)
        return

    if command == "/mylink":
        if chat_type == "private":
            send_personal_link(chat_id, sender)
            return
        if not is_target_chat(chat) or on_cooldown(sender["id"], "link"):
            return
        handle_mylink(chat_id, sender, reply_to=message_id, in_group=True)
        return

    if command == "/topinviter":
        if chat_type != "private" and not is_target_chat(chat):
            return
        if on_cooldown(sender["id"], "top"):
            return
        handle_topinviter(chat_id, sender, reply_to=message_id,
                          in_group=(chat_type != "private"))


# --------------------------------------------------------------------------- #
# Join tracking
# --------------------------------------------------------------------------- #
def welcome_new_member(chat_id, user):
    message = (
        f"🎉 Welcome to <b>PROMOTER</b>, {mention_html(user)}! 🚀\n"
        "💎 <b>500 TON GIVEAWAY • 100 WINNERS</b> 🏆\n"
        "👇 Choose an option below to get started!"
    )
    send(chat_id, message, parse_mode="HTML", reply_markup=action_keyboard())


def track_referral(update, joined):
    if db is None:
        return
    invite = update.get("invite_link") or {}
    link = invite.get("invite_link")
    if not joined.get("id") or not link:
        return
    record = db.invite_links.find_one({"chat_id": str(GROUP_ID), "invite_link": link})
    if not record:
        log.info("Join used an untracked invite link.")
        return
    inviter_id = int(record["inviter_id"])
    referred_id = int(joined["id"])
    if inviter_id == referred_id:
        return
    try:
        db.referrals.insert_one({
            "chat_id": str(GROUP_ID),
            "inviter_id": inviter_id,
            "inviter_name": record.get("inviter_name", str(inviter_id)),
            "referred_id": referred_id,
            "referred_name": joined.get("username") or joined.get("first_name") or str(referred_id),
            "invite_link": link,
            "joined_at": datetime.now(timezone.utc),
        })
        _lb_cache["rows"] = None        # leaderboard changes immediately
        log.info("Tracked referral: inviter=%s joined=%s", inviter_id, referred_id)
    except Exception as exc:
        if exc.__class__.__name__ != "DuplicateKeyError":
            log.exception("Could not save referral")


def handle_chat_member(update):
    chat = update.get("chat", {})
    if not is_target_chat(chat):
        return
    old = update.get("old_chat_member", {})
    new = update.get("new_chat_member", {})
    if is_member_status(old) or not is_member_status(new):
        return
    joined = new.get("user", {})

    # Count the referral FIRST so a slow/failed welcome can never lose it.
    try:
        track_referral(update, joined)
    except Exception:
        log.exception("Referral tracking failed")

    if get_setting("welcome_enabled"):
        try:
            welcome_new_member(chat.get("id"), joined)
        except Exception:
            log.exception("Could not send welcome message")
    elif get_setting("welcome_admin_enabled") and OWNER_ID:
        try:
            send(int(OWNER_ID),
                 f"👋 New member joined the group: {display_label(joined)} (ID: {joined.get('id')})")
        except Exception:
            log.exception("Could not send private join notice")


# --------------------------------------------------------------------------- #
# Callback buttons
# --------------------------------------------------------------------------- #
def handle_callback_query(callback):
    data = callback.get("data", "")
    user = callback.get("from", {}) or {}
    callback_id = callback.get("id", "")
    message = callback.get("message") or {}
    chat = message.get("chat") or {}
    chat_id = chat.get("id", GROUP_ID)

    def answer(text=None):
        if callback_id:
            payload = {"callback_query_id": callback_id}
            if text:
                payload["text"] = text
            tg("answerCallbackQuery", payload)

    if data not in ("generate_mylink", "show_topinviter") or not user.get("id") or user.get("is_bot"):
        answer("Unknown action.")
        return
    if chat and not is_target_chat(chat) and chat.get("type") != "private":
        answer()
        return

    if on_cooldown(user["id"], "cb"):
        answer("Please wait a moment…")
        return

    answer("Generating your personal invite link…" if data == "generate_mylink"
           else "Loading the leaderboard…")

    in_group = chat.get("type") in ("group", "supergroup")
    reply_to = message.get("message_id") if in_group else None
    if data == "show_topinviter":
        handle_topinviter(chat_id, user, reply_to=reply_to, in_group=in_group)
    else:
        handle_mylink(chat_id, user, reply_to=reply_to, in_group=in_group)


# --------------------------------------------------------------------------- #
# Update dispatch (with duplicate protection for Telegram webhook retries)
# --------------------------------------------------------------------------- #
_seen_updates = OrderedDict()
_seen_lock = threading.Lock()


def _is_duplicate(update_id):
    if update_id is None:
        return False
    with _seen_lock:
        if update_id in _seen_updates:
            return True
        _seen_updates[update_id] = 1
        while len(_seen_updates) > 3000:
            _seen_updates.popitem(last=False)
    return False


def process_update(update):
    started = time.monotonic()
    try:
        if "message" in update:
            handle_message(update["message"])
        if "chat_member" in update:
            handle_chat_member(update["chat_member"])
        if "callback_query" in update:
            handle_callback_query(update["callback_query"])
    except Exception:
        log.exception("Error processing update_id=%s", update.get("update_id"))
    finally:
        took = time.monotonic() - started
        if took > 2:
            log.warning("Slow update_id=%s took %.1fs", update.get("update_id"), took)


# --------------------------------------------------------------------------- #
# Recurring reminder
# --------------------------------------------------------------------------- #
def send_mylink_reminder():
    while True:
        try:
            interval = int(get_setting("automsg_interval_seconds"))
        except (TypeError, ValueError):
            interval = 3600
        time.sleep(max(300, min(interval, 86400)))
        try:
            if not get_setting("automsg_enabled") or not GROUP_ID:
                continue
            result = send(
                GROUP_ID,
                "🚀 <b>PROMOTER 500 TON GIVEAWAY</b>\n"
                "💎 Invite genuine friends and compete for prizes!\n"
                "👇 Tap <b>/mylink</b> below to generate your personal invite link.",
                parse_mode="HTML", reply_markup=action_keyboard())
            message_id = (result.get("result") or {}).get("message_id")
            if message_id:
                try:
                    duration = int(get_setting("automsg_duration_seconds"))
                except (TypeError, ValueError):
                    duration = 10
                delete_message_later(GROUP_ID, message_id, max(1, min(duration, 86400)))
        except Exception:
            log.exception("Could not send recurring reminder")


# --------------------------------------------------------------------------- #
# Webhook registration + keep-alive (Render Free)
# --------------------------------------------------------------------------- #
WANTED_UPDATES = ["message", "chat_member", "callback_query"]


def register_webhook(force=False):
    if not (API and RENDER_EXTERNAL_URL):
        return {"ok": False, "description": "BOT_TOKEN or RENDER_EXTERNAL_URL missing"}
    url = f"{RENDER_EXTERNAL_URL}/webhook"
    if not force:
        info = (tg("getWebhookInfo").get("result") or {})
        if info.get("url") == url and set(info.get("allowed_updates") or []) == set(WANTED_UPDATES):
            return {"ok": True, "description": "already registered"}
    payload = {
        "url": url,
        "allowed_updates": WANTED_UPDATES,
        "drop_pending_updates": False,
        "max_connections": 40,
    }
    if WEBHOOK_SECRET:
        payload["secret_token"] = WEBHOOK_SECRET
    return tg("setWebhook", payload)


def _startup_tasks():
    time.sleep(2)
    try:
        get_bot_username()
        _settings()
        if AUTO_SET_WEBHOOK:
            log.info("Webhook registration: %s", register_webhook())
    except Exception:
        log.exception("Startup tasks failed")


def _keepalive_loop():
    if not RENDER_EXTERNAL_URL or KEEPALIVE_SECONDS <= 0:
        return
    while True:
        time.sleep(KEEPALIVE_SECONDS)
        try:
            requests.get(f"{RENDER_EXTERNAL_URL}/health", timeout=10)
            if client is not None:
                client.admin.command("ping")
        except Exception:
            log.warning("Keep-alive ping failed")


# --------------------------------------------------------------------------- #
# Flask routes
# --------------------------------------------------------------------------- #
@app.get("/")
def home():
    return jsonify({"ok": True, "service": "PROMOTER invite tracking bot"})


@app.get("/health")
def health_check():
    return jsonify({"ok": True})


@app.post("/webhook")
def webhook():
    if WEBHOOK_SECRET:
        received = request.headers.get("X-Telegram-Bot-Api-Secret-Token", "")
        if received != WEBHOOK_SECRET:
            return jsonify({"ok": False, "error": "unauthorized"}), 403
    update = request.get_json(silent=True) or {}
    if _is_duplicate(update.get("update_id")):
        return jsonify({"ok": True})
    try:
        executor.submit(process_update, update)
    except Exception:
        log.exception("Could not queue update")
        return jsonify({"ok": False}), 500
    return jsonify({"ok": True})


def _check_key():
    key = request.args.get("key", "")
    return bool(WEBHOOK_SECRET) and key == WEBHOOK_SECRET


@app.get("/setup-webhook")
def setup_webhook():
    if not _check_key():
        return jsonify({"ok": False, "error": "Invalid webhook setup key."}), 403
    return jsonify(register_webhook(force=True))


@app.get("/diag")
def diag():
    if not _check_key():
        return jsonify({"ok": False, "error": "Invalid key."}), 403
    me = tg("getMe")
    out = {
        "group_id_config": GROUP_ID,
        "owner_id_set": bool(OWNER_ID),
        "mongo_configured": db is not None,
        "bot": me.get("result"),
        "webhook": tg("getWebhookInfo").get("result"),
        "chat": tg("getChat", {"chat_id": GROUP_ID}).get("result"),
    }
    bot_id = (me.get("result") or {}).get("id")
    if bot_id:
        member = tg("getChatMember", {"chat_id": GROUP_ID, "user_id": bot_id})
        out["bot_in_group"] = member.get("result") or member
    if db is not None:
        try:
            client.admin.command("ping")
            out["mongo_ping"] = "ok"
        except Exception as exc:
            out["mongo_ping"] = f"FAILED: {exc}"
    return jsonify(out)


@app.get("/test-send")
def test_send():
    if not _check_key():
        return jsonify({"ok": False, "error": "Invalid key."}), 403
    return jsonify(send(GROUP_ID, "✅ Test message from PROMOTER bot."))


# Start background workers (Render runs ONE gunicorn worker -> one of each).
for _target, _name in (
    (_scheduler_loop, "scheduler"),
    (send_mylink_reminder, "reminder"),
    (_keepalive_loop, "keepalive"),
    (_startup_tasks, "startup"),
):
    threading.Thread(target=_target, name=_name, daemon=True).start()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "10000")))
