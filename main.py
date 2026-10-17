import os
import logging
from html import escape
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
import threading
import time
import re

from flask import Flask, request, jsonify
import requests
from pymongo import MongoClient, ASCENDING, DESCENDING

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("promoter-bot")

BOT_TOKEN = os.environ.get("BOT_TOKEN", "").strip()
GROUP_ID = os.environ.get("GROUP_ID", "").strip()
MONGODB_URI = os.environ.get("MONGODB_URI", "").strip()
DB_NAME = os.environ.get("DB_NAME", "promoter_giveaway").strip()
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "").strip()
RENDER_EXTERNAL_URL = os.environ.get("RENDER_EXTERNAL_URL", "").rstrip("/")

app = Flask(__name__)

# Keep a small pool so the webhook can acknowledge Telegram quickly.
executor = ThreadPoolExecutor(max_workers=4)

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
    # The existing Atlas database already has a unique index on
    # (chat_id, invite_link). Do not recreate it as a non-unique index,
    # which causes IndexKeySpecsConflict and prevents Gunicorn from booting.
    db.referrals.create_index(
        [("chat_id", ASCENDING), ("referred_id", ASCENDING)], unique=True
    )
    db.referrals.create_index(
        [("chat_id", ASCENDING), ("inviter_id", ASCENDING)]
    )

API = f"https://api.telegram.org/bot{BOT_TOKEN}" if BOT_TOKEN else ""
_bot_username = None
_http = requests.Session()


def tg(method, payload=None):
    if not API:
        raise RuntimeError("BOT_TOKEN is not configured")
    response = _http.post(f"{API}/{method}", json=payload or {}, timeout=12)
    response.raise_for_status()
    result = response.json()
    if not result.get("ok"):
        log.error("Telegram API %s failed: %s", method, result)
    return result


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
                "url": "https://t.me/share/url?url=" +
                       requests.utils.quote(link, safe=""),
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
    message = (
        "🎉 PROMOTER 500 TON GIVEAWAY!\n\n"
        f"👤 Invite link owner: {label}\n"
        "🔗 Your personal invite link is ready!\n\n"
        "Share this link with genuine friends. Tracked eligible new members "
        "will count toward your referral total.\n\n"
        "💎 Prize pool: 500 TON • Up to 100 winners\n"
        "📋 Use the buttons below to copy or share your link."
    )
    send(chat_id, message, reply_markup=private_link_keyboard(link))



def get_inviter_display_name(inviter_id, chat_key):
    """Prefer a real Telegram username; fall back to the stored display name."""
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

    owner = db.invite_links.find_one({
        "chat_id": chat_key,
        "inviter_id": int(inviter_id),
    }, {"inviter_name": 1, "_id": 0}) if db is not None else None
    name = (owner or {}).get("inviter_name")
    if not name and db is not None:
        ref = db.referrals.find_one({
            "chat_id": chat_key,
            "inviter_id": int(inviter_id),
        }, {"inviter_name": 1, "_id": 0})
        name = (ref or {}).get("inviter_name")
    return str(name or f"User {inviter_id}")


# --- Group content moderation: only admins may post links/media/buttons ---
URL_RE = re.compile(
    r"(?i)(?:https?://|www\.|t\.me/|telegram\.me/|telegram\.dog/|"
    r"\b[a-z0-9][a-z0-9.-]*\.(?:com|net|org|io|co|xyz|app|site|me|link|"
    r"click|live|online|info|biz|ru|us|uk|gg|dev|ai|shop|store)\b)"
)
MEDIA_FIELDS = (
    "photo", "video", "animation", "document", "audio", "voice",
    "video_note", "sticker",
)

def user_is_group_admin(chat_id, user_id):
    """Return True only for group creator/admin; fail closed if API check fails."""
    try:
        result = tg("getChatMember", {"chat_id": chat_id, "user_id": int(user_id)})
        member = result.get("result") or {}
        return member.get("status") in ("creator", "administrator")
    except Exception:
        log.exception("Could not check admin status for user %s", user_id)
        return False

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
    """Delete non-admin group posts containing links, media, or inline buttons."""
    chat = message.get("chat") or {}
    if not is_target_chat(chat) or chat.get("type") not in ("group", "supergroup"):
        return False

    sender = message.get("from") or {}
    if not sender or sender.get("is_bot"):
        return False
    if not message_has_restricted_content(message):
        return False
    if user_is_group_admin(chat.get("id"), sender.get("id")):
        return False

    try:
        result = tg("deleteMessage", {
            "chat_id": chat.get("id"),
            "message_id": message.get("message_id"),
        })
        if result.get("ok"):
            log.info("Deleted link/media/button message from user %s", sender.get("id"))
        else:
            log.warning("Telegram could not delete message: %s", result.get("description"))
    except Exception:
        log.exception("Failed deleting restricted group message")
    return True

def handle_message(message):
    # Moderation first: non-admin link/media/button posts are removed.
    if moderate_group_message(message):
        return

    text = (message.get("text") or "").strip()
    if not text.startswith("/"):
        return

    parts = text.split(maxsplit=1)
    command = parts[0].split("@")[0].lower()
    start_payload = parts[1].strip() if len(parts) > 1 else ""
    chat = message.get("chat", {})
    sender = message.get("from", {})
    chat_id = chat.get("id")
    chat_type = chat.get("type")

    if not sender or sender.get("is_bot"):
        return

    if command in ("/start", "/help"):
        if chat_type == "private":
            if start_payload == "mylink":
                send_personal_link(chat_id, sender)
                return
            send(
                chat_id,
                "👋 Welcome to PROMOTER Giveaway Bot!\n\n"
                "💎 500 TON GIVEAWAY • Up to 100 winners\n"
                "Use /mylink to get your personal invite link.\n"
                "Use /topinviter in the group to view the leaderboard.",
            )
        else:
            send(
                chat_id,
                "👋 Welcome to PROMOTER!\n"
                "Type /mylink in this group to get started.",
            )
        return

    if command == "/mylink":
        if chat_type == "private":
            send_personal_link(chat_id, sender)
            return
        if not is_target_chat(chat):
            return

        keyboard = private_start_keyboard()
        if not keyboard:
            send(chat_id, "❌ Bot username unavailable. Please try again shortly.")
            return

        username = sender.get("username")
        who = f"@{username}" if username else (sender.get("first_name") or "there")
        send(
            chat_id,
            f"👋 {escape(str(who))}, tap below to open the bot and press START "
            "to receive your personal invite link.\n\n"
            "💎 500 TON GIVEAWAY • Up to 100 winners\n"
            "🔗 Your link is unique to your account.",
            parse_mode="HTML",
            reply_markup=keyboard,
            reply_to_message_id=message.get("message_id"),
        )
        return

    if command == "/topinviter":
        if chat_type != "private" and not is_target_chat(chat):
            return
        if db is None:
            send(chat_id, "⚠️ MongoDB is not configured.")
            return

        chat_key = str(GROUP_ID)
        user_id = int(sender["id"])
        my_count = db.referrals.count_documents({
            "chat_id": chat_key,
            "inviter_id": user_id,
        })

        # Build top 5 from tracked unique referrals; use stored username/name.
        pipeline = [
            {"$match": {"chat_id": chat_key}},
            {"$group": {
                "_id": "$inviter_id",
                "count": {"$sum": 1},
            }},
            {"$sort": {"count": -1, "_id": 1}},
            {"$limit": 5},
        ]
        top_rows = list(db.referrals.aggregate(pipeline))

        # Prefer the inviter username/name saved with their personal invite link.
        lines = [
            "🏆 <b>PROMOTER INVITE LEADERBOARD</b>",
            "💎 500 TON GIVEAWAY • TOP 5",
            "",
            f"👤 <b>Your invites:</b> {my_count}",
            "",
            "🔥 <b>TOP 5 INVITERS</b>",
        ]
        if not top_rows:
            lines.append("No tracked invites yet. Be the first!")
        else:
            for rank, row in enumerate(top_rows, 1):
                inviter_id = int(row["_id"])
                display_name = escape(get_inviter_display_name(inviter_id, chat_key))
                lines.append(f"{rank}. {display_name} — <b>{row['count']}</b> invite(s)")

        lines.extend([
            "",
            "📌 Only tracked, unique new members are counted.",
            "🚀 Tap /mylink to get your personal invite link.",
        ])
        send(chat_id, "\n".join(lines), parse_mode="HTML")



def welcome_new_member(chat_id, user):
    """Welcome new members with My Link and Top Inviter buttons."""
    user_id = user.get("id")
    if not user_id:
        return

    first_name = escape(str(user.get("first_name") or "friend"))
    username = user.get("username")

    # Mention the new member by username or clickable Telegram profile.
    if username:
        safe_username = escape(username, quote=True)
        safe_display = escape(username)
        mention = (
            f'<a href="https://t.me/{safe_username}">'
            f'@{safe_display}</a>'
        )
    else:
        mention = (
            f'<a href="tg://user?id={int(user_id)}">'
            f'{first_name}</a>'
        )

    message = (
        f"🎉 Welcome to <b>PROMOTER</b>, {mention}! 🚀\n"
        "💎 <b>500 TON GIVEAWAY • 100 WINNERS</b> 🏆\n"
        "👇 Choose an option below to get started!"
    )

    keyboard = {
        "inline_keyboard": [
            [
                {
                    "text": "🔗 /mylink",
                    "callback_data": "generate_mylink"
                },
                {
                    "text": "🏆 /topinviter",
                    "callback_data": "show_topinviter"
                }
            ]
        ]
    }

    send(
        chat_id,
        message,
        parse_mode="HTML",
        reply_markup=keyboard
    )



def handle_chat_member(update):
    chat = update.get("chat", {})
    if not is_target_chat(chat):
        return

    old = update.get("old_chat_member", {})
    new = update.get("new_chat_member", {})

    # Send one welcome only when the user becomes a member.
    if is_member_status(old) or not is_member_status(new):
        return

    joined = new.get("user", {})
    try:
        welcome_new_member(chat.get("id"), joined)
    except Exception:
        log.exception("Could not send new-member welcome message")

    # Referral tracking requires MongoDB and a tracked invite link.
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
        log.info("Tracked referral: inviter=%s joined=%s", inviter_id, referred_id)
    except Exception as exc:
        if exc.__class__.__name__ != "DuplicateKeyError":
            log.exception("Could not save referral")



def reminder_keyboard():
    # Inline buttons act for the Telegram user who taps them.
    return {
        "inline_keyboard": [[
            {"text": "🔗 /mylink", "callback_data": "generate_mylink"},
            {"text": "🏆 /topinviter", "callback_data": "show_topinviter"},
        ]]
    }


def send_mylink_reminder():
    """Post a group reminder with a clickable /mylink button every 30 seconds."""
    while True:
        time.sleep(1000)
        try:
            if not GROUP_ID:
                log.error("GROUP_ID is empty; skipping /mylink reminder")
                continue
            send(
                GROUP_ID,
                "🚀 <b>PROMOTER 500 TON GIVEAWAY</b>\n"
                "💎 Invite genuine friends and compete for prizes!\n"
                "👇 Tap <b>/mylink</b> below to generate your personal invite link.",
                parse_mode="HTML",
                reply_markup=reminder_keyboard(),
            )
        except Exception:
            log.exception("Could not send 30-second /mylink reminder")


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

    
    if data == "show_topinviter":
        if db is None:
            send(chat_id, "⚠️ MongoDB is not configured.")
            return

        chat_key = str(GROUP_ID)
        user_id = int(user["id"])

        my_count = db.referrals.count_documents({
            "chat_id": chat_key,
            "inviter_id": user_id,
        })

        # Mention the user who clicked the Top Inviter button.
        username = user.get("username")
        if username:
            user_mention = (
                f'<a href="https://t.me/{escape(username, quote=True)}">'
                f'@{escape(username)}</a>'
            )
        else:
            display_name = escape(str(user.get("first_name") or "Member"))
            user_mention = (
                f'<a href="tg://user?id={user_id}">'
                f'{display_name}</a>'
            )

        pipeline = [
            {"$match": {"chat_id": chat_key}},
            {"$group": {
                "_id": "$inviter_id",
                "count": {"$sum": 1},
            }},
            {"$sort": {"count": -1, "_id": 1}},
            {"$limit": 5},
        ]

        top_rows = list(db.referrals.aggregate(pipeline))

        lines = [
            "🏆 <b>PROMOTER INVITE LEADERBOARD</b>",
            "💎 500 TON GIVEAWAY • TOP 5",
            "",
            f"👤 <b>{user_mention}, your invites:</b> {my_count}",
            "",
            "🔥 <b>TOP 5 INVITERS</b>",
        ]

        if not top_rows:
            lines.append("No tracked invites yet. Be the first!")
        else:
            for rank, row in enumerate(top_rows, 1):
                inviter_id = int(row["_id"])
                display_name = escape(
                    get_inviter_display_name(inviter_id, chat_key)
                )
                lines.append(
                    f"{rank}. {display_name} — "
                    f"<b>{row['count']}</b> invite(s)"
                )

        lines.extend([
            "",
            "📌 Counts include tracked unique new members.",
        ])

        send(
            chat_id,
            "\n".join(lines),
            parse_mode="HTML",
            reply_to_message_id=message.get("message_id"),
        )
        return

    # The clicked user's Telegram identity determines their personal link.
    link, error = make_invite_link(user)
    if error:
        name = escape(
            str(
                user.get("username")
                and ("@" + user["username"])
                or user.get("first_name")
                or "this user"
            )
        )
        send(
            chat_id,
            f"❌ Could not create a link for {name}. "
            "Please check the bot's Manage Invite Links permission.",
            parse_mode="HTML",
            reply_to_message_id=message.get("message_id"),
        )
        return

    username = user.get("username")
    if username:
        mention = (
            f'<a href="https://t.me/{escape(username, quote=True)}">'
            f'@{escape(username)}</a>'
        )
    else:
        mention = (
            f'<a href="tg://user?id={int(user["id"])}">'
            f'{escape(str(user.get("first_name") or "Member"))}</a>'
        )

    send(
        chat_id,
        f"🔗 {mention}, your personal invite link is ready:\n"
        f"<code>{escape(link)}</code>\n\n"
        "Share it with genuine friends to join the 500 TON giveaway!",
        parse_mode="HTML",
        reply_to_message_id=message.get("message_id"),
    )


def process_update(update):
    try:
        if "message" in update:
            handle_message(update["message"])
        if "chat_member" in update:
            handle_chat_member(update["chat_member"])
        if "callback_query" in update:
            handle_callback_query(update["callback_query"])
    except Exception:
        log.exception("Error processing Telegram update")



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

    # Acknowledge immediately; process Telegram API/MongoDB work in background.
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


# Render start command should use one Gunicorn worker to avoid duplicate reminders.
# The daemon stops automatically when the process exits.
_reminder_thread = threading.Thread(
    target=send_mylink_reminder,
    name="mylink-reminder",
    daemon=True,
)
_reminder_thread.start()


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
