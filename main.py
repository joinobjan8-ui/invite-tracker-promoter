import os
import logging
import re
import time
import threading
from html import escape
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor

import requests
from flask import Flask, request, jsonify
from pymongo import MongoClient, ASCENDING

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("promoter-bot")

BOT_TOKEN = os.environ.get("BOT_TOKEN", "").strip()
GROUP_ID = os.environ.get("GROUP_ID", "").strip()
MONGODB_URI = os.environ.get("MONGODB_URI", "").strip()
DB_NAME = os.environ.get("DB_NAME", "promoter_giveaway").strip()
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "").strip()
RENDER_EXTERNAL_URL = os.environ.get("RENDER_EXTERNAL_URL", "").rstrip("/")
OWNER_ID = os.environ.get("OWNER_ID", "").strip()

app = Flask(__name__)
executor = ThreadPoolExecutor(max_workers=4)
_http = requests.Session()
API = f"https://api.telegram.org/bot{BOT_TOKEN}" if BOT_TOKEN else ""
_bot_username = None

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

if db is not None:
    db.invite_links.create_index(
        [("chat_id", ASCENDING), ("inviter_id", ASCENDING)], unique=True
    )
    # Keep any existing unique index on (chat_id, invite_link) unchanged.
    db.referrals.create_index(
        [("chat_id", ASCENDING), ("referred_id", ASCENDING)], unique=True
    )
    db.referrals.create_index(
        [("chat_id", ASCENDING), ("inviter_id", ASCENDING)]
    )

# Telegram request pacing: shared per-chat scheduling across worker threads.
_rate_lock = threading.Lock()
_next_chat_request_at = {}
CHAT_REQUEST_GAP = 3.2
MAX_TG_ATTEMPTS = 6


def _pace_chat(chat_id):
    if chat_id is None:
        return
    key = str(chat_id)
    with _rate_lock:
        now = time.monotonic()
        scheduled = _next_chat_request_at.get(key, 0.0)
        _next_chat_request_at[key] = max(now, scheduled) + CHAT_REQUEST_GAP
    wait = max(0.0, scheduled - now)
    if wait:
        time.sleep(wait)


def tg(method, payload=None):
    if not API:
        raise RuntimeError("BOT_TOKEN is not configured")
    payload = payload or {}
    chat_id = payload.get("chat_id")
    last_error = None

    for attempt in range(1, MAX_TG_ATTEMPTS + 1):
        _pace_chat(chat_id)
        try:
            response = _http.post(
                f"{API}/{method}", json=payload, timeout=(5, 20)
            )
            try:
                result = response.json()
            except ValueError:
                result = {}

            if response.status_code == 429:
                params = result.get("parameters") or {}
                try:
                    retry_after = max(
                        1, min(int(params.get("retry_after", 2)), 120)
                    )
                except (TypeError, ValueError):
                    retry_after = 2
                if attempt >= MAX_TG_ATTEMPTS:
                    log.error("Telegram rate limit persisted for %s", method)
                    return {
                        "ok": False,
                        "error_code": 429,
                        "description": result.get(
                            "description", "Too Many Requests"
                        ),
                        "parameters": params,
                    }
                time.sleep(retry_after)
                continue

            if response.status_code >= 500:
                last_error = f"HTTP {response.status_code}"
                if attempt < MAX_TG_ATTEMPTS:
                    time.sleep(min(2 ** (attempt - 1), 15))
                    continue

            if not response.ok or not result.get("ok"):
                log.error(
                    "Telegram API %s failed (HTTP %s): %s",
                    method, response.status_code,
                    result.get("description") or response.text[:300],
                )
                return result or {
                    "ok": False,
                    "error_code": response.status_code,
                    "description": response.text[:300],
                }
            return result

        except (requests.Timeout, requests.ConnectionError) as exc:
            last_error = str(exc)
            if attempt < MAX_TG_ATTEMPTS:
                time.sleep(min(2 ** (attempt - 1), 15))
                continue
        except requests.RequestException as exc:
            last_error = str(exc)
            break

    log.error("Telegram API %s exhausted retries: %s", method, last_error)
    return {
        "ok": False,
        "description": str(last_error or "Telegram request failed"),
    }


def send(chat_id, text, **kwargs):
    payload = {
        "chat_id": chat_id,
        "text": text,
        "disable_web_page_preview": True,
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


DEFAULT_SETTINGS = {
    "welcome_enabled": False,
    "welcome_admin_enabled": False,
    "automsg_enabled": False,
    "automsg_interval_seconds": 3600,
    "automsg_duration_seconds": 10,
    "all_messages_enabled": True,
    "linkmedia_enabled": True,
    "autodelete_enabled": True,
    "autodelete_seconds": 15,
}


def get_setting(key):
    if db is None:
        return DEFAULT_SETTINGS.get(key, True)
    try:
        row = db.bot_settings.find_one({"_id": "global"}) or {}
        return row.get(key, DEFAULT_SETTINGS.get(key, True))
    except Exception:
        log.exception("Could not read bot setting %s", key)
        return DEFAULT_SETTINGS.get(key, True)


def set_setting(key, value):
    if db is not None:
        db.bot_settings.update_one(
            {"_id": "global"}, {"$set": {key: value}}, upsert=True
        )
    else:
        DEFAULT_SETTINGS[key] = value


def auto_delete_delay():
    try:
        return max(5, min(int(get_setting("autodelete_seconds")), 86400))
    except (TypeError, ValueError):
        return 15


def schedule_delete(chat_id, message_id, delay):
    if not chat_id or not message_id:
        return
    try:
        delay = max(1, min(int(delay), 86400))
    except (TypeError, ValueError):
        delay = 15

    def delete_later():
        time.sleep(delay)
        try:
            result = tg(
                "deleteMessage",
                {"chat_id": chat_id, "message_id": message_id},
            )
            if not result.get("ok"):
                log.warning(
                    "Could not delete message %s: %s",
                    message_id, result.get("description"),
                )
        except Exception:
            log.exception("Failed deleting message %s", message_id)

    threading.Thread(
        target=delete_later,
        name=f"delete-{message_id}",
        daemon=True,
    ).start()


def delete_user_command_later(message, delay=None):
    """Schedule deletion of the original group command message."""
    chat = message.get("chat") or {}
    if chat.get("type") not in ("group", "supergroup"):
        return
    if delay is None:
        delay = auto_delete_delay()
    schedule_delete(chat.get("id"), message.get("message_id"), delay)


def send_auto_delete(chat_id, text, delete_after=None, **kwargs):
    """Send a message and schedule its deletion in the target group."""
    result = send(chat_id, text, **kwargs)
    message_info = result.get("result") or {}
    returned_chat = message_info.get("chat") or {}
    message_id = message_info.get("message_id")

    # Explicit delete_after schedules this message even if the global switch is off.
    # Private messages are never auto-deleted.
    is_group = (
        returned_chat.get("type") in ("group", "supergroup")
        and is_target_chat(returned_chat)
    )
    if is_group and message_id:
        if delete_after is not None:
            schedule_delete(chat_id, message_id, delete_after)
        elif get_setting("autodelete_enabled"):
            schedule_delete(chat_id, message_id, auto_delete_delay())
    return result


def is_owner(user):
    return bool(OWNER_ID and str((user or {}).get("id", "")) == OWNER_ID)


def user_is_group_admin(chat_id, user_id):
    try:
        result = tg(
            "getChatMember",
            {"chat_id": chat_id, "user_id": int(user_id)},
        )
        member = result.get("result") or {}
        return member.get("status") in ("creator", "administrator")
    except Exception:
        log.exception("Could not check admin status for user %s", user_id)
        return False


ADMIN_CONTROL_COMMANDS = {
    "/help", "/owner",
    "/welcome_on", "/welcome_off", "/welcome_all_on", "/welcome_all_off",
    "/welcome_admin_on", "/welcome_admin_off",
    "/automsg_on", "/automsg_off", "/automsg_interval", "/automsg_duration",
    "/allmsg_on", "/allmsg_off", "/linkmedia_on", "/linkmedia_off",
    "/autodelete_on", "/autodelete_off", "/autodelete_time",
}


def is_authorized_admin_command(text, user):
    if is_owner(user):
        return True
    if not GROUP_ID or not user or not user.get("id"):
        return False
    command = text.split(maxsplit=1)[0].split("@")[0].lower() if text else ""
    if command not in ADMIN_CONTROL_COMMANDS:
        return False
    return user_is_group_admin(GROUP_ID, user["id"])


def owner_help_text():
    return (
        "🛠 PROMOTER ADMIN COMMANDS\n\n"
        "/help — show this command list\n"
        "/welcome_on — enable group welcome messages\n"
        "/welcome_off — disable group welcome messages\n"
        "/welcome_all_on — enable group welcome messages\n"
        "/welcome_all_off — disable group welcome messages\n"
        "/welcome_admin_on — send join notices to owner privately\n"
        "/welcome_admin_off — disable private admin join notices\n"
        "/automsg_on — enable recurring group reminder\n"
        "/automsg_off — disable recurring group reminder\n"
        "/automsg_interval 3600 — reminder interval (300–86400 seconds)\n"
        "/automsg_duration 10 — reminder visibility (1–86400 seconds)\n"
        "/allmsg_on — allow normal member messages\n"
        "/allmsg_off — delete all non-admin group messages\n"
        "/linkmedia_on — enable link/media/button filtering\n"
        "/linkmedia_off — disable link/media/button filtering\n"
        "/autodelete_on — enable automatic deletion\n"
        "/autodelete_off — disable automatic deletion for normal bot replies\n"
        "/autodelete_time 15 — command/reply deletion delay (5–86400 seconds)\n\n"
        f"Current settings: group_welcome={get_setting('welcome_enabled')}, "
        f"admin_join_notice={get_setting('welcome_admin_enabled')}, "
        f"autodelete={get_setting('autodelete_enabled')}, "
        f"autodelete_seconds={get_setting('autodelete_seconds')}, "
        f"automsg={get_setting('automsg_enabled')}, "
        f"automsg_interval_seconds={get_setting('automsg_interval_seconds')}, "
        f"automsg_duration_seconds={get_setting('automsg_duration_seconds')}, "
        f"allmsg={get_setting('all_messages_enabled')}, "
        f"linkmedia={get_setting('linkmedia_enabled')}"
    )


def make_invite_link(user):
    if db is None:
        return None, "MongoDB is not configured."

    user_id = int(user["id"])
    chat_key = str(GROUP_ID)
    existing = db.invite_links.find_one({
        "chat_id": chat_key,
        "inviter_id": user_id,
    })
    if existing:
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
            "inviter_name": (
                ("@" + user["username"]) if user.get("username")
                else (user.get("first_name") or str(user_id))
            ),
            "created_at": datetime.now(timezone.utc),
        }},
        upsert=True,
    )
    saved = db.invite_links.find_one({
        "chat_id": chat_key,
        "inviter_id": user_id,
    })
    return (saved or {}).get("invite_link", link), None


def private_link_keyboard(link):
    return {
        "inline_keyboard": [
            [{"text": "📋 Copy My Invite Link", "copy_text": {"text": link}}],
            [{
                "text": "📣 Share My Link",
                "url": "https://t.me/share/url?url="
                       + requests.utils.quote(link, safe=""),
            }],
        ]
    }


def private_start_keyboard():
    username = get_bot_username()
    if not username:
        return None
    return {
        "inline_keyboard": [[{
            "text": "🎁 Get My Personal Invite Link",
            "url": f"https://t.me/{username}?start=mylink",
        }]]
    }


def send_personal_link(chat_id, user):
    link, error = make_invite_link(user)
    if error:
        send(
            chat_id,
            "❌ I couldn't create your personal invite link.\n\n"
            "Please make sure the bot is an admin and has permission to "
            "manage invite links.\n\n"
            f"Details: {error}",
        )
        return

    name = user.get("username")
    label = f"@{name}" if name else (user.get("first_name") or "your account")
    send(
        chat_id,
        "🎉 PROMOTER 500 TON GIVEAWAY!\n\n"
        f"👤 Invite link owner: {label}\n"
        "🔗 Your personal invite link is ready!\n\n"
        "Share this link with genuine friends. Tracked eligible new members "
        "will count toward your referral total.\n\n"
        "💎 Prize pool: 500 TON • Up to 100 winners\n"
        "📋 Use the buttons below to copy or share your link.",
        reply_markup=private_link_keyboard(link),
    )


def get_inviter_display_name(inviter_id, chat_key):
    try:
        result = tg("getChatMember", {
            "chat_id": GROUP_ID,
            "user_id": int(inviter_id),
        })
        member_user = (result.get("result") or {}).get("user") or {}
        if member_user.get("username"):
            return "@" + member_user["username"]
        if member_user.get("first_name"):
            return member_user["first_name"]
    except Exception:
        log.info("Could not fetch current username for inviter %s", inviter_id)

    owner = db.invite_links.find_one(
        {"chat_id": chat_key, "inviter_id": int(inviter_id)},
        {"inviter_name": 1, "_id": 0},
    ) if db is not None else None
    name = (owner or {}).get("inviter_name")
    if not name and db is not None:
        ref = db.referrals.find_one(
            {"chat_id": chat_key, "inviter_id": int(inviter_id)},
            {"inviter_name": 1, "_id": 0},
        )
        name = (ref or {}).get("inviter_name")
    return str(name or f"User {inviter_id}")


URL_RE = re.compile(
    r"(?i)(?:https?://|www\.|t\.me/|telegram\.me/|telegram\.dog/|"
    r"\b[a-z0-9][a-z0-9.-]*\.(?:com|net|org|io|co|xyz|app|site|me|link|"
    r"click|live|online|info|biz|ru|us|uk|gg|dev|ai|shop|store)\b)"
)
MEDIA_FIELDS = (
    "photo", "video", "animation", "document", "audio", "voice",
    "video_note", "sticker",
)


def message_has_restricted_content(message):
    if (message.get("reply_markup") or {}).get("inline_keyboard"):
        return True
    if any(message.get(field) for field in MEDIA_FIELDS):
        return True
    text = message.get("text") or message.get("caption") or ""
    if URL_RE.search(text):
        return True
    for field in ("entities", "caption_entities"):
        for entity in message.get(field, []) or []:
            if entity.get("type") in ("url", "text_link"):
                return True
    return False


def moderate_group_message(message):
    chat = message.get("chat") or {}
    if not is_target_chat(chat) or chat.get("type") not in ("group", "supergroup"):
        return False

    sender = message.get("from") or {}
    if not sender or sender.get("is_bot"):
        return False
    if user_is_group_admin(chat.get("id"), sender.get("id")):
        return False

    if not get_setting("all_messages_enabled"):
        restricted = True
    else:
        restricted = (
            get_setting("linkmedia_enabled")
            and message_has_restricted_content(message)
        )
    if not restricted:
        return False

    try:
        result = tg("deleteMessage", {
            "chat_id": chat.get("id"),
            "message_id": message.get("message_id"),
        })
        if result.get("ok"):
            log.info("Deleted restricted message from user %s", sender.get("id"))
        else:
            log.warning("Telegram could not delete message: %s",
                        result.get("description"))
    except Exception:
        log.exception("Failed deleting restricted group message")
    return True


def build_top_rows(chat_key):
    pipeline = [
        {"$match": {"chat_id": chat_key}},
        {"$group": {"_id": "$inviter_id", "count": {"$sum": 1}}},
        {"$sort": {"count": -1, "_id": 1}},
        {"$limit": 5},
    ]
    return list(db.referrals.aggregate(pipeline))


def leaderboard_text(user, chat_key):
    user_id = int(user["id"])
    my_count = db.referrals.count_documents({
        "chat_id": chat_key,
        "inviter_id": user_id,
    })
    username = user.get("username")
    if username:
        mention = (
            f'<a href="https://t.me/{escape(username, quote=True)}">'
            f'@{escape(username)}</a>'
        )
    else:
        mention = (
            f'<a href="tg://user?id={user_id}">'
            f'{escape(str(user.get("first_name") or "Member"))}</a>'
        )

    rows = build_top_rows(chat_key)
    lines = [
        "🏆 <b>PROMOTER INVITE LEADERBOARD</b>",
        "💎 500 TON GIVEAWAY • TOP 5",
        "",
        f"👤 <b>{mention}, your invites:</b> {my_count}",
        "",
        "🔥 <b>TOP 5 INVITERS</b>",
    ]
    if not rows:
        lines.append("No tracked invites yet. Be the first!")
    else:
        for rank, row in enumerate(rows, 1):
            name = escape(get_inviter_display_name(int(row["_id"]), chat_key))
            lines.append(f"{rank}. {name} — <b>{row['count']}</b> invite(s)")
    lines.extend(["", "📌 Counts include tracked unique new members."])
    return "\n".join(lines)


def handle_message(message):
    text = (message.get("text") or "").strip()
    sender = message.get("from") or {}
    chat = message.get("chat") or {}
    chat_id = chat.get("id")
    chat_type = chat.get("type")

    if not sender or sender.get("is_bot"):
        return

    # Admin commands are handled before moderation.
    if text.startswith("/") and is_authorized_admin_command(text, sender):
        owner_command = text.split(maxsplit=1)[0].split("@")[0].lower()
        setting_commands = {
            "/welcome_on": ("welcome_enabled", True, "Group welcome messages enabled."),
            "/welcome_off": ("welcome_enabled", False, "Group welcome messages disabled."),
            "/welcome_all_on": ("welcome_enabled", True, "Welcome messages enabled for all members."),
            "/welcome_all_off": ("welcome_enabled", False, "Group welcome messages disabled."),
            "/welcome_admin_on": ("welcome_admin_enabled", True, "Private admin join notices enabled. Start the bot in private first."),
            "/welcome_admin_off": ("welcome_admin_enabled", False, "Private admin join notices disabled."),
            "/automsg_on": ("automsg_enabled", True, "Automatic group reminders enabled."),
            "/automsg_off": ("automsg_enabled", False, "Automatic group reminders disabled."),
            "/allmsg_on": ("all_messages_enabled", True, "Normal member messages are allowed."),
            "/allmsg_off": ("all_messages_enabled", False, "All non-admin group messages will be deleted."),
            "/linkmedia_on": ("linkmedia_enabled", True, "Link/media/button filtering enabled."),
            "/linkmedia_off": ("linkmedia_enabled", False, "Link/media/button filtering disabled."),
            "/autodelete_on": ("autodelete_enabled", True, "Auto-delete enabled."),
            "/autodelete_off": ("autodelete_enabled", False, "Auto-delete disabled for replies without a forced timer."),
        }

        if owner_command in ("/help", "/owner"):
            send(chat_id, owner_help_text())
            return

        if owner_command in ("/welcome_on", "/welcome_all_on"):
            set_setting("welcome_enabled", True)
            set_setting("welcome_admin_enabled", False)
            send(chat_id, "✅ Welcome messages will be posted in the group.")
            return

        if owner_command in ("/welcome_off", "/welcome_all_off"):
            set_setting("welcome_enabled", False)
            send(chat_id, "✅ Group welcome messages disabled.")
            return

        if owner_command == "/welcome_admin_on":
            set_setting("welcome_enabled", False)
            set_setting("welcome_admin_enabled", True)
            send(chat_id, "✅ Group welcome disabled; private join notices enabled.")
            return

        if owner_command in ("/automsg_interval", "/automsg_duration"):
            parts = text.split(maxsplit=1)
            label = "interval" if owner_command == "/automsg_interval" else "duration"
            key = (
                "automsg_interval_seconds"
                if label == "interval"
                else "automsg_duration_seconds"
            )
            minimum = 300 if label == "interval" else 1
            if len(parts) != 2 or not parts[1].strip().isdigit():
                example = "3600" if label == "interval" else "10"
                send(chat_id, f"Usage: /automsg_{label} {example} (seconds)")
                return
            seconds = int(parts[1].strip())
            if not minimum <= seconds <= 86400:
                send(chat_id, f"Please choose {minimum}–86400 seconds.")
                return
            set_setting(key, seconds)
            send(chat_id, f"✅ Automatic message {label} set to {seconds} seconds.")
            return

        if owner_command == "/autodelete_time":
            parts = text.split(maxsplit=1)
            if len(parts) != 2 or not parts[1].strip().isdigit():
                send(chat_id, "Usage: /autodelete_time 15 (seconds; minimum 5, maximum 86400)")
                return
            seconds = int(parts[1].strip())
            if not 5 <= seconds <= 86400:
                send(chat_id, "Please choose a delay from 5 to 86400 seconds.")
                return
            set_setting("autodelete_seconds", seconds)
            send(chat_id, f"✅ Command/reply auto-delete delay set to {seconds} seconds.")
            return

        if owner_command in setting_commands:
            key, value, response_text = setting_commands[owner_command]
            set_setting(key, value)
            send(chat_id, "✅ " + response_text)
            return

    # Moderation runs before regular commands, except the three commands that
    # need to work even if the content filter is enabled.
    command = text.split(maxsplit=1)[0].split("@")[0].lower() if text.startswith("/") else ""
    tracked_commands = {"/mylink", "/invite", "/topinviter"}
    is_group = chat_type in ("group", "supergroup")
    if not (is_group and command in tracked_commands):
        if moderate_group_message(message):
            return

    parts = text.split(maxsplit=1)
    command = parts[0].split("@")[0].lower() if parts else ""
    start_payload = parts[1].strip() if len(parts) > 1 else ""

    if command in ("/start", "/help"):
        if chat_type == "private":
            if command == "/start" and start_payload == "mylink":
                send_personal_link(chat_id, sender)
                return
            send(
                chat_id,
                "👋 Welcome to PROMOTER Giveaway Bot!\n\n"
                "💎 500 TON GIVEAWAY • Up to 100 winners\n"
                "Use /mylink or /invite to get your personal invite link.\n"
                "Use /topinviter in the group to view the leaderboard.",
            )
        else:
            send(chat_id, "👋 Welcome to PROMOTER!\nType /mylink in this group to get started.")
        return

    if command in ("/mylink", "/invite"):
        if chat_type == "private":
            send_personal_link(chat_id, sender)
            return
        if not is_group:
            return

        delay = auto_delete_delay()
        delete_user_command_later(message, delay)
        link, error = make_invite_link(sender)
        who = (
            f"@{sender['username']}"
            if sender.get("username")
            else (sender.get("first_name") or "there")
        )

        if error:
            send_auto_delete(
                chat_id,
                f"❌ {escape(str(who))}, I couldn't create your invite link. "
                "Please ask an admin to check Manage Invite Links permission.",
                parse_mode="HTML",
                reply_to_message_id=message.get("message_id"),
                delete_after=delay,
            )
            return

        send_auto_delete(
            chat_id,
            f"🔗 <b>{escape(str(who))}, your personal invite link:</b>\n"
            f"<code>{escape(link)}</code>\n\n"
            "💎 Share it with genuine friends to join the 500 TON giveaway.\n"
            f"⏳ This message will be deleted in {delay} seconds.",
            parse_mode="HTML",
            reply_markup=private_link_keyboard(link),
            reply_to_message_id=message.get("message_id"),
            delete_after=delay,
        )
        return

    if command == "/topinviter":
        if chat_type not in ("private", "group", "supergroup"):
            return
        if is_group:
            delay = auto_delete_delay()
            delete_user_command_later(message, delay)

        if db is None:
            if is_group:
                send_auto_delete(
                    chat_id,
                    "⚠️ MongoDB is not configured.",
                    delete_after=auto_delete_delay(),
                )
            else:
                send(chat_id, "⚠️ MongoDB is not configured.")
            return

        lines = leaderboard_text(sender, str(GROUP_ID))
        if chat_type == "private":
            send(chat_id, lines, parse_mode="HTML")
        else:
            delay = auto_delete_delay()
            lines += f"\n\n⏳ This leaderboard will be deleted in {delay} seconds."
            send_auto_delete(
                chat_id,
                lines,
                parse_mode="HTML",
                reply_to_message_id=message.get("message_id"),
                delete_after=delay,
            )
        return


def welcome_new_member(chat_id, user):
    user_id = user.get("id")
    if not user_id:
        return
    first_name = escape(str(user.get("first_name") or "friend"))
    username = user.get("username")

    if username:
        safe_username = escape(username, quote=True)
        safe_display = escape(username)
        mention = f'<a href="https://t.me/{safe_username}">@{safe_display}</a>'
    else:
        mention = f'<a href="tg://user?id={int(user_id)}">{first_name}</a>'

    message = (
        f"🎉 Welcome to <b>PROMOTER</b>, {mention}! 🚀\n"
        "💎 <b>500 TON GIVEAWAY • 100 WINNERS</b> 🏆\n"
        "👇 Choose an option below to get started!"
    )
    keyboard = {
        "inline_keyboard": [[
            {"text": "🔗 /mylink", "callback_data": "generate_mylink"},
            {"text": "🏆 /topinviter", "callback_data": "show_topinviter"},
        ]]
    }
    send(chat_id, message, parse_mode="HTML", reply_markup=keyboard)


def handle_chat_member(update):
    chat = update.get("chat", {})
    if not is_target_chat(chat):
        return

    old = update.get("old_chat_member", {})
    new = update.get("new_chat_member", {})
    if is_member_status(old) or not is_member_status(new):
        return

    joined = new.get("user", {})
    if get_setting("welcome_enabled"):
        try:
            welcome_new_member(chat.get("id"), joined)
        except Exception:
            log.exception("Could not send new-member welcome message")
    elif get_setting("welcome_admin_enabled") and OWNER_ID:
        try:
            who = (
                ("@" + joined["username"]) if joined.get("username")
                else joined.get("first_name") or str(joined.get("id"))
            )
            send(
                int(OWNER_ID),
                f"👋 New member joined the group: {escape(str(who))} "
                f"(ID: {joined.get('id')})",
            )
        except Exception:
            log.exception("Could not send private join notice to OWNER_ID")

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
                joined.get("username") or joined.get("first_name")
                or str(referred_id)
            ),
            "invite_link": link,
            "joined_at": datetime.now(timezone.utc),
        })
        log.info("Tracked referral: inviter=%s joined=%s", inviter_id, referred_id)
    except Exception as exc:
        if exc.__class__.__name__ != "DuplicateKeyError":
            log.exception("Could not save referral")


def reminder_keyboard():
    return {
        "inline_keyboard": [[
            {"text": "🔗 /mylink", "callback_data": "generate_mylink"},
            {"text": "🏆 /topinviter", "callback_data": "show_topinviter"},
        ]]
    }


def send_mylink_reminder():
    while True:
        try:
            interval = int(get_setting("automsg_interval_seconds"))
        except (TypeError, ValueError):
            interval = 3600
        interval = max(300, min(interval, 86400))
        time.sleep(interval)

        try:
            if not get_setting("automsg_enabled"):
                continue
            if not GROUP_ID:
                log.error("GROUP_ID is empty; skipping reminder")
                continue

            result = send(
                GROUP_ID,
                "🚀 <b>PROMOTER 500 TON GIVEAWAY</b>\n"
                "💎 Invite genuine friends and compete for prizes!\n"
                "👇 Tap <b>/mylink</b> below to generate your personal invite link.",
                parse_mode="HTML",
                reply_markup=reminder_keyboard(),
            )
            message_info = result.get("result") or {}
            message_id = message_info.get("message_id")
            if message_id:
                try:
                    duration = int(get_setting("automsg_duration_seconds"))
                except (TypeError, ValueError):
                    duration = 10
                schedule_delete(GROUP_ID, message_id, max(1, min(duration, 86400)))
        except Exception:
            log.exception("Could not send recurring /mylink reminder")


def handle_callback_query(callback):
    data = callback.get("data", "")
    user = callback.get("from", {})
    callback_id = callback.get("id", "")
    message = callback.get("message") or {}
    chat = message.get("chat") or {}
    chat_id = chat.get("id", GROUP_ID)

    if data not in ("generate_mylink", "show_topinviter"):
        if callback_id:
            tg("answerCallbackQuery", {
                "callback_query_id": callback_id,
                "text": "Unknown action.",
                "show_alert": False,
            })
        return
    if not user.get("id"):
        return

    try:
        tg("answerCallbackQuery", {
            "callback_query_id": callback_id,
            "text": (
                "Generating your personal invite link…"
                if data == "generate_mylink"
                else "Loading your invite stats…"
            ),
            "show_alert": False,
        })
    except Exception:
        log.exception("Could not answer callback query")

    delay = auto_delete_delay()
    if data == "show_topinviter":
        if db is None:
            send_auto_delete(chat_id, "⚠️ MongoDB is not configured.", delete_after=delay)
            return
        try:
            text = leaderboard_text(user, str(GROUP_ID))
            text += f"\n\n⏳ This message will be deleted in {delay} seconds."
            send_auto_delete(
                chat_id,
                text,
                parse_mode="HTML",
                reply_to_message_id=message.get("message_id"),
                delete_after=delay,
            )
        except Exception:
            log.exception("Could not build leaderboard from callback")
            send_auto_delete(
                chat_id,
                "❌ Could not load the leaderboard. Please try again.",
                delete_after=delay,
            )
        return

    link, error = make_invite_link(user)
    if error:
        name = escape(str(
            ("@" + user["username"]) if user.get("username")
            else user.get("first_name") or "this user"
        ))
        send_auto_delete(
            chat_id,
            f"❌ Could not create a link for {name}. "
            "Please check the bot's Manage Invite Links permission.",
            parse_mode="HTML",
            reply_to_message_id=message.get("message_id"),
            delete_after=delay,
        )
        return

    if user.get("username"):
        mention = (
            f'<a href="https://t.me/{escape(user["username"], quote=True)}">'
            f'@{escape(user["username"])}</a>'
        )
    else:
        mention = (
            f'<a href="tg://user?id={int(user["id"])}">'
            f'{escape(str(user.get("first_name") or "Member"))}</a>'
        )

    send_auto_delete(
        chat_id,
        f"🔗 {mention}, your personal invite link is ready:\n"
        f"<code>{escape(link)}</code>\n\n"
        "Share it with genuine friends to join the 500 TON giveaway!\n"
        f"⏳ This message will be deleted in {delay} seconds.",
        parse_mode="HTML",
        reply_markup=private_link_keyboard(link),
        reply_to_message_id=message.get("message_id"),
        delete_after=delay,
    )


def process_update(update):
    try:
        if "message" in update:
            message = update["message"]
            sender = message.get("from") or {}
            chat = message.get("chat") or {}
            text = (message.get("text") or "").strip()
            command = text.split(maxsplit=1)[0][:64] if text.startswith("/") else "<non-command>"
            log.info(
                "Processing message update=%s chat_id=%s chat_type=%s user_id=%s command=%s",
                update.get("update_id"), chat.get("id"), chat.get("type"),
                sender.get("id"), command,
            )
            handle_message(message)
        if "chat_member" in update:
            handle_chat_member(update["chat_member"])
        if "callback_query" in update:
            callback = update["callback_query"]
            log.info(
                "Processing callback update=%s data=%s user_id=%s",
                update.get("update_id"), callback.get("data"),
                (callback.get("from") or {}).get("id"),
            )
            handle_callback_query(callback)
    except Exception:
        log.exception("Error processing Telegram update_id=%s", update.get("update_id"))


@app.get("/")
def health():
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
    log.info(
        "Webhook received update_id=%s keys=%s",
        update.get("update_id"), list(update.keys()),
    )
    try:
        executor.submit(process_update, update)
    except Exception:
        log.exception("Could not queue Telegram update")
        return jsonify({"ok": False}), 500
    return jsonify({"ok": True})


@app.get("/setup-webhook")
def setup_webhook():
    key = request.args.get("key", "")
    if not WEBHOOK_SECRET or key != WEBHOOK_SECRET:
        return jsonify({"ok": False, "error": "Invalid webhook setup key."}), 403
    if not RENDER_EXTERNAL_URL:
        return jsonify({"ok": False, "error": "RENDER_EXTERNAL_URL is not set."}), 500

    result = tg("setWebhook", {
        "url": f"{RENDER_EXTERNAL_URL}/webhook",
        "allowed_updates": ["message", "chat_member", "callback_query"],
        "drop_pending_updates": False,
        "secret_token": WEBHOOK_SECRET,
    })
    return jsonify(result)


# Use one Gunicorn worker to avoid duplicate reminder threads.
_reminder_thread = threading.Thread(
    target=send_mylink_reminder,
    name="mylink-reminder",
    daemon=True,
)
_reminder_thread.start()


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
