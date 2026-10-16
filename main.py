import os
import re
import time
import heapq
import hmac
import hashlib
import logging
import threading
import itertools
from collections import OrderedDict
from html import escape
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor

from flask import Flask, request, jsonify
import requests
from requests.adapters import HTTPAdapter
from pymongo import MongoClient, ASCENDING, DESCENDING

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("promoter-bot")


def _int_env(name, default):
    try:
        return int(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default


BOT_TOKEN = os.environ.get("BOT_TOKEN", "").strip()
GROUP_ID = os.environ.get("GROUP_ID", "").strip()
MONGODB_URI = os.environ.get("MONGODB_URI", "").strip()
DB_NAME = os.environ.get("DB_NAME", "promoter_giveaway").strip()
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "").strip()
RENDER_EXTERNAL_URL = os.environ.get("RENDER_EXTERNAL_URL", "").rstrip("/")

# Group messages created by /mylink, the buttons and /topinviter are removed
# after this many seconds to keep the group clean. Set to 0 to keep them.
AUTO_DELETE_SECONDS = _int_env("AUTO_DELETE_SECONDS", 120)
# A user can request a new link message at most once per this many seconds.
LINK_COOLDOWN_SECONDS = _int_env("LINK_COOLDOWN_SECONDS", 10)
# New joins within this window are welcomed with ONE message (avoids flooding).
WELCOME_BATCH_SECONDS = _int_env("WELCOME_BATCH_SECONDS", 2)
# Self-ping interval that keeps the Render free instance (and Mongo) warm.
KEEPALIVE_SECONDS = max(_int_env("KEEPALIVE_SECONDS", 540), 60)
# Set AUTO_SET_WEBHOOK=0 to disable automatic webhook registration at boot.
AUTO_SET_WEBHOOK = os.environ.get("AUTO_SET_WEBHOOK", "1") != "0"

TOP_CACHE_SECONDS = 10
ALLOWED_UPDATES = ["message", "chat_member", "callback_query"]
ANONYMOUS_ADMIN_ID = 1087968824  # Telegram's "GroupAnonymousBot"
MEDALS = {1: "🥇", 2: "🥈", 3: "🥉"}

# Telegram only accepts [A-Za-z0-9_-]{1,256} as secret_token. Render's
# generated values can contain other characters, so derive a safe token.
if WEBHOOK_SECRET and re.fullmatch(r"[A-Za-z0-9_-]{1,256}", WEBHOOK_SECRET):
    TELEGRAM_SECRET_TOKEN = WEBHOOK_SECRET
elif WEBHOOK_SECRET:
    TELEGRAM_SECRET_TOKEN = hashlib.sha256(WEBHOOK_SECRET.encode()).hexdigest()
else:
    TELEGRAM_SECRET_TOKEN = ""

app = Flask(__name__)

# Webhook acknowledges Telegram instantly; real work runs in these threads.
executor = ThreadPoolExecutor(max_workers=8, thread_name_prefix="upd")

client = (
    MongoClient(
        MONGODB_URI,
        serverSelectionTimeoutMS=8000,
        connectTimeoutMS=8000,
        socketTimeoutMS=10000,
        retryWrites=True,
        maxPoolSize=20,
    )
    if MONGODB_URI else None
)
db = client[DB_NAME] if client is not None else None

API = f"https://api.telegram.org/bot{BOT_TOKEN}" if BOT_TOKEN else ""
_bot_username = None


# --------------------------------------------------------------------------
# Telegram HTTP helpers
# --------------------------------------------------------------------------
_tls = threading.local()


def _session():
    """One requests.Session per thread; replaced after any network error so a
    dead keep-alive connection (common on Render free) can never block replies."""
    s = getattr(_tls, "s", None)
    if s is None:
        s = requests.Session()
        s.mount("https://", HTTPAdapter(pool_connections=1, pool_maxsize=2))
        _tls.s = s
    return s


def _reset_session():
    s = getattr(_tls, "s", None)
    if s is not None:
        try:
            s.close()
        except Exception:
            pass
    _tls.s = None


def tg(method, payload=None, retries=3, quiet=False):
    if not API:
        raise RuntimeError("BOT_TOKEN is not configured")

    last = {"ok": False, "description": "request failed"}
    for attempt in range(retries):
        try:
            response = _session().post(
                f"{API}/{method}", json=payload or {}, timeout=(4, 10)
            )
        except (requests.ConnectionError, requests.Timeout) as exc:
            log.warning("Telegram %s network error (try %s): %s", method, attempt + 1, exc)
            _reset_session()
            last = {"ok": False, "description": "network error"}
            time.sleep(0.3 * (attempt + 1))
            continue

        try:
            data = response.json()
        except ValueError:
            data = {"ok": False, "description": f"HTTP {response.status_code}"}

        if data.get("ok"):
            return data

        last = data
        if response.status_code == 429:
            wait = (data.get("parameters") or {}).get("retry_after", 1)
            if wait <= 15:
                time.sleep(wait + 0.2)
                continue
        elif response.status_code >= 500:
            time.sleep(0.4 * (attempt + 1))
            continue
        break

    if quiet:
        log.debug("Telegram API %s failed: %s", method, last)
    else:
        log.error("Telegram API %s failed: %s", method, last)
    return last


def send(chat_id, text, reply_to=None, **kwargs):
    payload = {
        "chat_id": chat_id,
        "text": text,
        "link_preview_options": {"is_disabled": True},
    }
    if reply_to:
        payload["reply_parameters"] = {
            "message_id": reply_to,
            "allow_sending_without_reply": True,
        }
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


# --------------------------------------------------------------------------
# Auto-delete scheduler (one thread instead of one timer thread per message)
# --------------------------------------------------------------------------
_del_heap = []
_del_cv = threading.Condition()
_del_counter = itertools.count()


def schedule_delete(chat_id, message_id, delay=None):
    delay = AUTO_DELETE_SECONDS if delay is None else delay
    if delay <= 0 or not message_id:
        return
    with _del_cv:
        heapq.heappush(
            _del_heap, (time.time() + delay, next(_del_counter), chat_id, message_id)
        )
        _del_cv.notify()


def _delete_worker():
    while True:
        with _del_cv:
            while True:
                if not _del_heap:
                    _del_cv.wait()
                    continue
                remaining = _del_heap[0][0] - time.time()
                if remaining <= 0:
                    break
                _del_cv.wait(remaining)
            _, _, chat_id, message_id = heapq.heappop(_del_heap)
        try:
            tg(
                "deleteMessage",
                {"chat_id": chat_id, "message_id": message_id},
                retries=1,
                quiet=True,
            )
        except Exception:
            log.debug("deleteMessage failed", exc_info=True)


# --------------------------------------------------------------------------
# Small helpers
# --------------------------------------------------------------------------
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


def mention(user):
    username = user.get("username")
    if username:
        return "@" + escape(username)
    name = user.get("first_name") or "friend"
    return f'<a href="tg://user?id={int(user["id"])}">{escape(str(name))}</a>'


def human_seconds(seconds):
    if seconds >= 60 and seconds % 60 == 0:
        return f"{seconds // 60} min"
    return f"{seconds} sec"


_cooldown = {}


def on_cooldown(user_id):
    """True if the user asked too recently. Otherwise records the request."""
    now = time.time()
    if now - _cooldown.get(user_id, 0) < LINK_COOLDOWN_SECONDS:
        return True
    _cooldown[user_id] = now
    if len(_cooldown) > 5000:
        for key in [k for k, v in _cooldown.items() if now - v > 120]:
            _cooldown.pop(key, None)
    return False


# --------------------------------------------------------------------------
# Invite links
# --------------------------------------------------------------------------
_link_cache = {}  # (chat_key, user_id) -> (link, name)
_locks = [threading.Lock() for _ in range(64)]


def _update_name(chat_key, user_id, name):
    try:
        db.invite_links.update_one(
            {"chat_id": chat_key, "inviter_id": user_id},
            {"$set": {"inviter_name": name}},
        )
    except Exception:
        log.debug("Could not refresh inviter name", exc_info=True)


def make_invite_link(user):
    if db is None:
        return None, "MongoDB is not configured."

    user_id = int(user["id"])
    chat_key = str(GROUP_ID)
    name = user.get("username") or user.get("first_name") or str(user_id)
    cache_key = (chat_key, user_id)

    cached = _link_cache.get(cache_key)
    if cached:
        link, old_name = cached
        if old_name != name:
            _link_cache[cache_key] = (link, name)
            executor.submit(_update_name, chat_key, user_id, name)
        return link, None

    with _locks[user_id % len(_locks)]:
        cached = _link_cache.get(cache_key)
        if cached:
            return cached[0], None

        existing = db.invite_links.find_one(
            {"chat_id": chat_key, "inviter_id": user_id}
        )
        if existing:
            link = existing["invite_link"]
            if existing.get("inviter_name") != name:
                executor.submit(_update_name, chat_key, user_id, name)
        else:
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
                    "inviter_name": name,
                    "created_at": datetime.now(timezone.utc),
                }},
                upsert=True,
            )
            saved = db.invite_links.find_one(
                {"chat_id": chat_key, "inviter_id": user_id}
            )
            link = (saved or {}).get("invite_link", link)

        if len(_link_cache) > 20000:
            _link_cache.clear()
        _link_cache[cache_key] = (link, name)
        return link, None


def link_keyboard(link):
    share_url = (
        "https://t.me/share/url?url=" + requests.utils.quote(link, safe="")
        + "&text=" + requests.utils.quote("Join PROMOTER - 500 TON giveaway! 🎁", safe="")
    )
    return {
        "inline_keyboard": [
            [{"text": "📋 Copy My Link", "copy_text": {"text": link}}],
            [{"text": "📣 Share My Link", "url": share_url}],
            [{"text": "🏆 Top Inviters", "callback_data": "top"}],
        ]
    }


def get_link_keyboard():
    return {
        "inline_keyboard": [[
            {"text": "🔗 Get My Invite Link", "callback_data": "getlink"}
        ]]
    }


def deliver_link(chat_id, user, in_group, reply_to=None, command_msg_id=None):
    """Create/fetch the user's personal link and post it right where they asked."""
    link, error = make_invite_link(user)
    if error:
        log.error("Could not create link for %s: %s", user.get("id"), error)
        send(
            chat_id,
            "❌ Couldn't create your invite link right now. Please try again in a moment.",
            reply_to=reply_to,
        )
        return

    text = (
        "🎉 <b>PROMOTER 500 TON GIVEAWAY!</b>\n\n"
        f"👤 {mention(user)}, your personal invite link is ready:\n\n"
        f"<code>{escape(link)}</code>\n\n"
        "👆 Tap the link to copy it, or use the buttons below.\n"
        "💎 Prize pool: 500 TON • Up to 100 winners"
    )
    if in_group and AUTO_DELETE_SECONDS > 0:
        text += (
            f"\n\n⏳ This message disappears in {human_seconds(AUTO_DELETE_SECONDS)}"
            " — copy your link now."
        )

    result = send(
        chat_id,
        text,
        reply_to=reply_to,
        parse_mode="HTML",
        reply_markup=link_keyboard(link),
    )
    if in_group and result.get("ok"):
        schedule_delete(chat_id, result["result"]["message_id"])
        if command_msg_id:
            schedule_delete(chat_id, command_msg_id)


# --------------------------------------------------------------------------
# Leaderboard
# --------------------------------------------------------------------------
_top_cache = {}
_top_last_sent = {}


def build_top_lines():
    now = time.time()
    cached = _top_cache.get("v")
    if cached and now - cached[0] < TOP_CACHE_SECONDS:
        return cached[1]

    pipeline = [
        {"$match": {"chat_id": str(GROUP_ID)}},
        {"$group": {
            "_id": "$inviter_id",
            "count": {"$sum": 1},
            "name": {"$last": "$inviter_name"},
        }},
        {"$sort": {"count": DESCENDING, "_id": ASCENDING}},
        {"$limit": 100},
    ]
    rows = list(db.referrals.aggregate(pipeline))

    # Prefer the freshest display name stored with each personal link.
    names = {}
    ids = [row["_id"] for row in rows]
    if ids:
        for doc in db.invite_links.find(
            {"chat_id": str(GROUP_ID), "inviter_id": {"$in": ids}},
            {"inviter_id": 1, "inviter_name": 1},
        ):
            names[doc["inviter_id"]] = doc.get("inviter_name")

    lines = []
    for rank, row in enumerate(rows, 1):
        name = str(names.get(row["_id"]) or row.get("name") or row["_id"])
        lines.append(f"{MEDALS.get(rank, f'{rank}.')} {name} — {row['count']} valid invite(s)")

    _top_cache["v"] = (now, lines)
    return lines


def send_top(chat_id, in_group, reply_to=None):
    if db is None:
        send(chat_id, "⚠️ MongoDB is not configured.", reply_to=reply_to)
        return

    if in_group:
        now = time.time()
        if now - _top_last_sent.get(chat_id, 0) < 10:
            return
        _top_last_sent[chat_id] = now

    lines = build_top_lines()
    keyboard = get_link_keyboard()
    if not lines:
        result = send(
            chat_id,
            "🏆 PROMOTER TOP INVITERS\n\nNo tracked referrals yet.\n"
            "Tap the button below to get your personal invite link.",
            reply_to=reply_to,
            reply_markup=keyboard,
        )
        if in_group and result.get("ok"):
            schedule_delete(chat_id, result["result"]["message_id"])
        return

    everything = (
        ["🏆 PROMOTER TOP INVITERS", "💎 500 TON GIVEAWAY", ""]
        + lines
        + ["", "📌 Counts are based on tracked unique new members."]
    )

    # Telegram messages are limited to 4096 characters - split safely.
    chunks, current, size = [], [], 0
    for line in everything:
        if size + len(line) + 1 > 3800 and current:
            chunks.append("\n".join(current))
            current, size = [], 0
        current.append(line)
        size += len(line) + 1
    if current:
        chunks.append("\n".join(current))

    for index, chunk in enumerate(chunks):
        extra = {"reply_markup": keyboard} if index == len(chunks) - 1 else {}
        result = send(
            chat_id, chunk, reply_to=reply_to if index == 0 else None, **extra
        )
        if in_group and result.get("ok"):
            schedule_delete(chat_id, result["result"]["message_id"])


# --------------------------------------------------------------------------
# Welcome messages (batched so a burst of joins can't hit Telegram's limits)
# --------------------------------------------------------------------------
_welcome_lock = threading.Lock()
_welcome_buf = {}


def queue_welcome(chat_id, user):
    if not user.get("id"):
        return
    with _welcome_lock:
        buf = _welcome_buf.setdefault(chat_id, [])
        first = not buf
        buf.append(user)
    if first:
        timer = threading.Timer(max(WELCOME_BATCH_SECONDS, 0), flush_welcome, args=(chat_id,))
        timer.daemon = True
        timer.start()


def flush_welcome(chat_id):
    try:
        with _welcome_lock:
            users = _welcome_buf.pop(chat_id, [])
        if not users:
            return
        shown = users[:8]
        extra = len(users) - len(shown)
        names = ", ".join(mention(u) for u in shown)
        if extra > 0:
            names += f" and {extra} more"
        send(
            chat_id,
            f"🎉 Welcome to <b>PROMOTER</b>, {names}! 🚀\n"
            "💎 <b>500 TON GIVEAWAY • 100 WINNERS</b> 🏆\n"
            "👇 Tap the button to get your personal invite link!",
            parse_mode="HTML",
            reply_markup=get_link_keyboard(),
        )
    except Exception:
        log.exception("Could not send new-member welcome message")


# --------------------------------------------------------------------------
# Update handlers
# --------------------------------------------------------------------------
def handle_message(message):
    text = (message.get("text") or "").strip()
    if not text.startswith("/"):
        return

    parts = text.split(maxsplit=1)
    token = parts[0].lower()
    command, _, target_bot = token.partition("@")
    start_payload = parts[1].strip() if len(parts) > 1 else ""

    # Ignore commands addressed to another bot (e.g. /mylink@OtherBot).
    if target_bot:
        mine = (get_bot_username() or "").lower()
        if mine and target_bot != mine:
            return

    chat = message.get("chat", {})
    sender = message.get("from", {})
    chat_id = chat.get("id")
    chat_type = chat.get("type")
    message_id = message.get("message_id")
    in_group = chat_type != "private"

    if not sender:
        return

    if sender.get("id") == ANONYMOUS_ADMIN_ID or message.get("sender_chat"):
        if command == "/mylink" and is_target_chat(chat):
            send(
                chat_id,
                "ℹ️ You're posting as an anonymous admin/channel, so I can't "
                "create a personal link. Turn off anonymous mode and try again.",
                reply_to=message_id,
            )
        return
    if sender.get("is_bot"):
        return

    if in_group and not is_target_chat(chat):
        return

    if command in ("/start", "/help", "/mylink"):
        wants_link = command == "/mylink" or start_payload == "mylink"
        if wants_link:
            if in_group and on_cooldown(sender["id"]):
                return
            deliver_link(
                chat_id,
                sender,
                in_group=in_group,
                reply_to=message_id if in_group else None,
                command_msg_id=message_id if in_group else None,
            )
            return

        if not in_group:
            send(
                chat_id,
                "👋 Welcome to PROMOTER Giveaway Bot!\n\n"
                "💎 500 TON GIVEAWAY • Up to 100 winners\n"
                "Tap the button to get your personal invite link, "
                "or use /topinviter to see the leaderboard.",
                reply_markup=get_link_keyboard(),
            )
        else:
            result = send(
                chat_id,
                "👋 Welcome to PROMOTER!\nTap the button to get your personal invite link.",
                reply_to=message_id,
                reply_markup=get_link_keyboard(),
            )
            if result.get("ok"):
                schedule_delete(chat_id, result["result"]["message_id"])
        return

    if command == "/topinviter":
        send_top(chat_id, in_group, reply_to=message_id if in_group else None)


def handle_callback(cb):
    cb_id = cb.get("id")
    user = cb.get("from") or {}
    data = cb.get("data") or ""
    message = cb.get("message") or {}
    chat = message.get("chat") or {}
    chat_id = chat.get("id")
    in_group = chat.get("type") != "private"

    if not user or user.get("is_bot") or not chat_id:
        tg("answerCallbackQuery", {"callback_query_id": cb_id}, retries=1, quiet=True)
        return
    if in_group and not is_target_chat(chat):
        tg("answerCallbackQuery", {"callback_query_id": cb_id}, retries=1, quiet=True)
        return

    if data == "getlink":
        if in_group and on_cooldown(user["id"]):
            tg("answerCallbackQuery", {
                "callback_query_id": cb_id,
                "text": "✅ Your link was just posted - check the latest messages 👆",
            }, retries=1, quiet=True)
            return
        tg("answerCallbackQuery", {
            "callback_query_id": cb_id,
            "text": "⏳ Creating your link...",
        }, retries=1, quiet=True)
        deliver_link(chat_id, user, in_group=in_group)
    elif data == "top":
        tg("answerCallbackQuery", {"callback_query_id": cb_id}, retries=1, quiet=True)
        send_top(chat_id, in_group)
    else:
        tg("answerCallbackQuery", {"callback_query_id": cb_id}, retries=1, quiet=True)


def track_referral(update, joined):
    if db is None:
        log.warning("MongoDB is not configured; skipping referral tracking")
        return

    invite = update.get("invite_link") or {}
    link = invite.get("invite_link")
    if not joined.get("id") or not link:
        return

    record = db.invite_links.find_one({
        "chat_id": str(GROUP_ID),
        "invite_link": link,
    })
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
            "referred_name": (
                joined.get("username")
                or joined.get("first_name")
                or str(referred_id)
            ),
            "invite_link": link,
            "joined_at": datetime.now(timezone.utc),
        })
        _top_cache.pop("v", None)
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

    # Act only at the moment the user becomes a member.
    if is_member_status(old) or not is_member_status(new):
        return

    joined = new.get("user", {})
    if joined.get("is_bot"):
        return

    # Count the referral first (correctness), then welcome (cosmetic).
    try:
        track_referral(update, joined)
    except Exception:
        log.exception("Referral tracking failed")

    try:
        queue_welcome(chat.get("id"), joined)
    except Exception:
        log.exception("Could not queue welcome message")


def process_update(update):
    try:
        if "callback_query" in update:
            handle_callback(update["callback_query"])
        elif "message" in update:
            handle_message(update["message"])
        elif "chat_member" in update:
            handle_chat_member(update["chat_member"])
    except Exception:
        log.exception("Error processing Telegram update")


# --------------------------------------------------------------------------
# Web endpoints
# --------------------------------------------------------------------------
_seen = OrderedDict()
_seen_lock = threading.Lock()


def is_duplicate(update_id):
    """Telegram re-sends updates it thinks failed; never process one twice."""
    if update_id is None:
        return False
    with _seen_lock:
        if update_id in _seen:
            return True
        _seen[update_id] = True
        while len(_seen) > 2000:
            _seen.popitem(last=False)
    return False


def worth_processing(update):
    if "chat_member" in update or "callback_query" in update:
        return True
    message = update.get("message")
    return bool(message and (message.get("text") or "").startswith("/"))


def secret_ok(received):
    if not WEBHOOK_SECRET:
        return True
    received = received.encode()
    return any(
        hmac.compare_digest(received, candidate.encode())
        for candidate in {WEBHOOK_SECRET, TELEGRAM_SECRET_TOKEN}
    )


@app.get("/")
def health():
    return jsonify({"ok": True, "service": "PROMOTER invite tracking bot"})


@app.get("/health")
def health_check():
    return jsonify({"ok": True})


@app.post("/webhook")
def webhook():
    if not secret_ok(request.headers.get("X-Telegram-Bot-Api-Secret-Token", "")):
        return jsonify({"ok": False, "error": "unauthorized"}), 403

    update = request.get_json(silent=True) or {}

    if not worth_processing(update) or is_duplicate(update.get("update_id")):
        return jsonify({"ok": True})

    try:
        executor.submit(process_update, update)
    except Exception:
        log.exception("Could not queue Telegram update")
        return jsonify({"ok": False}), 500

    return jsonify({"ok": True})


def register_webhook():
    payload = {
        "url": f"{RENDER_EXTERNAL_URL}/webhook",
        "allowed_updates": ALLOWED_UPDATES,
        "drop_pending_updates": False,
        "max_connections": 20,
    }
    if TELEGRAM_SECRET_TOKEN:
        payload["secret_token"] = TELEGRAM_SECRET_TOKEN
    return tg("setWebhook", payload)


@app.get("/setup-webhook")
def setup_webhook():
    key = request.args.get("key", "")
    if not WEBHOOK_SECRET or key != WEBHOOK_SECRET:
        return jsonify({"ok": False, "error": "Invalid webhook setup key."}), 403
    if not RENDER_EXTERNAL_URL:
        return jsonify({"ok": False, "error": "RENDER_EXTERNAL_URL is not set."}), 500
    return jsonify(register_webhook())


# --------------------------------------------------------------------------
# Background jobs: warm-up, auto webhook, keep-alive
# --------------------------------------------------------------------------
def ensure_indexes():
    if db is None:
        return
    # The Atlas database already has a unique index on (chat_id, invite_link).
    # Never recreate it with different options (IndexKeySpecsConflict).
    specs = [
        ("invite_links", [("chat_id", ASCENDING), ("inviter_id", ASCENDING)], True),
        ("referrals", [("chat_id", ASCENDING), ("referred_id", ASCENDING)], True),
        ("referrals", [("chat_id", ASCENDING), ("inviter_id", ASCENDING)], False),
    ]
    for collection, keys, unique in specs:
        try:
            kwargs = {"unique": True} if unique else {}
            db[collection].create_index(keys, **kwargs)
        except Exception as exc:
            log.warning("Index setup skipped for %s: %s", collection, exc)


def auto_register_webhook():
    if not (AUTO_SET_WEBHOOK and API and RENDER_EXTERNAL_URL):
        return
    want_url = f"{RENDER_EXTERNAL_URL}/webhook"
    info = (tg("getWebhookInfo", retries=2).get("result")) or {}
    if (
        info.get("url") == want_url
        and set(info.get("allowed_updates") or []) == set(ALLOWED_UPDATES)
    ):
        log.info("Webhook already configured.")
        return
    result = register_webhook()
    log.info("Webhook registered: %s", result.get("ok"))


def _warmup():
    try:
        if API:
            get_bot_username()
            auto_register_webhook()
    except Exception:
        log.exception("Webhook/bot warm-up failed")
    try:
        ensure_indexes()
    except Exception:
        log.exception("Index setup failed")


def _keepalive_loop():
    while True:
        time.sleep(KEEPALIVE_SECONDS)
        if RENDER_EXTERNAL_URL:
            try:
                requests.get(f"{RENDER_EXTERNAL_URL}/health", timeout=10)
            except Exception:
                log.debug("Keep-alive ping failed", exc_info=True)
        if client is not None:
            try:
                client.admin.command("ping")
            except Exception:
                log.debug("Mongo ping failed", exc_info=True)


_started = False
_start_lock = threading.Lock()


def start_background():
    global _started
    with _start_lock:
        if _started:
            return
        _started = True
    for name, target in (
        ("tg-delete", _delete_worker),
        ("keepalive", _keepalive_loop),
        ("warmup", _warmup),
    ):
        threading.Thread(target=target, name=name, daemon=True).start()


start_background()


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
