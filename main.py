
import os
import logging
from html import escape
from datetime import datetime, timezone

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
RENDER_EXTERNAL_URL = os.environ.get(
    "RENDER_EXTERNAL_URL", ""
).rstrip("/")

app = Flask(__name__)

client = (
    MongoClient(
        MONGODB_URI,
        serverSelectionTimeoutMS=15000,
        connectTimeoutMS=15000,
        retryWrites=True,
    )
    if MONGODB_URI else None
)

db = client[DB_NAME] if client is not None else None

if db is not None:
    db.invite_links.create_index(
        [("chat_id", ASCENDING), ("inviter_id", ASCENDING)],
        unique=True,
    )
    db.referrals.create_index(
        [("chat_id", ASCENDING), ("referred_id", ASCENDING)],
        unique=True,
    )
    db.referrals.create_index(
        [("chat_id", ASCENDING), ("inviter_id", ASCENDING)]
    )

API = f"https://api.telegram.org/bot{BOT_TOKEN}" if BOT_TOKEN else ""
_bot_username = None


def tg(method, payload=None):
    if not API:
        raise RuntimeError("BOT_TOKEN is not configured")

    response = requests.post(
        f"{API}/{method}",
        json=payload or {},
        timeout=25,
    )
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

    # Reuse this user's previously generated link.
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
        return None, result.get(
            "description", "Could not create invite link."
        )

    link = result["result"]["invite_link"]

    db.invite_links.update_one(
        {"chat_id": chat_key, "inviter_id": user_id},
        {
            "$setOnInsert": {
                "chat_id": chat_key,
                "inviter_id": user_id,
                "invite_link": link,
                "inviter_name": (
                    user.get("username")
                    or user.get("first_name")
                    or str(user_id)
                ),
                "created_at": datetime.now(timezone.utc),
            }
        },
        upsert=True,
    )

    saved = db.invite_links.find_one({
        "chat_id": chat_key,
        "inviter_id": user_id,
    })

    return (saved or {}).get("invite_link", link), None


def private_link_keyboard(link):
    return {
        "inline_keyboard": [[
            {
                "text": "📋 Copy My Invite Link",
                "copy_text": {"text": link},
            }
        ], [
            {
                "text": "📣 Share My Link",
                "url": (
                    "https://t.me/share/url?url="
                    + requests.utils.quote(link, safe="")
                ),
            }
        ]],
    }


def private_start_keyboard():
    username = get_bot_username()

    if not username:
        return None

    return {
        "inline_keyboard": [[
            {
                "text": "🎁 Get My Private Invite Link",
                "url": f"https://t.me/{username}?start=mylink",
            }
        ]],
    }


def send_personal_link(chat_id, user):
    link, error = make_invite_link(user)

    if error:
        send(
            chat_id,
            "❌ I couldn't create your personal invite link.\n\n"
            "Please contact the group admin and make sure the bot "
            "has permission to manage invite links.\n\n"
            f"Details: {error}",
        )
        return

    message = (
        "🎉 WELCOME TO PROMOTER 500 TON GIVEAWAY! 🎉\n\n"
        "👑 Your personal invite link is ready!\n\n"
        "🔐 This link belongs to your Telegram account.\n"
        "👥 Share it with your friends to increase your referral count.\n\n"
        "🏆 The more valid new members you invite, the higher your "
        "chance of ranking among the top inviters.\n\n"
        "💎 GIVEAWAY PRIZE POOL: 500 TON\n"
        "🎁 Up to 100 winners, according to the published rules.\n\n"
        "📋 Tap the button below to copy your personal invite link.\n"
        "⚠️ Keep your link private to your account and do not use "
        "another person's link as your own."
    )

    send(
        chat_id,
        message,
        reply_markup=private_link_keyboard(link),
    )


def handle_message(message):
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
                "🎁 Join the 500 TON Giveaway!\n\n"
                "Use /mylink to get your personal invite link.\n"
                "Use that link to invite new members and build "
                "your referral count.\n\n"
                "🏆 The /topinviter command shows the leaderboard "
                "in the PROMOTER group.",
            )
        else:
            send(
                chat_id,
                "👋 Welcome to PROMOTER!\n"
                "Use /mylink to get your personal invite link privately.",
            )
        return

    if command == "/mylink":
        if chat_type == "private":
            send_personal_link(chat_id, sender)
            return

        if not is_target_chat(chat):
            return

        keyboard = private_start_keyboard()

        announcement = (
            "📣🔥 PROMOTER 500 TON GIVEAWAY! 🔥📣\n\n"
            "🎁 Ready to join the giveaway?\n\n"
            "👥 Invite more friends using your own personal link.\n"
            "🏆 The more valid new members you invite, the better "
            "your chance of reaching the top of the leaderboard!\n\n"
            "💎 TOTAL PRIZE POOL: 500 TON\n"
            "🎉 TARGET: UP TO 100 WINNERS\n\n"
            "🔐 YOUR LINK IS PRIVATE!\n"
            "Tap the button below, open the bot, and press Start "
            "to receive your personal invite link in private chat.\n\n"
            "📋 Copy your link and share it with friends.\n"
            "⚠️ Only tracked, eligible new members count. "
            "Please follow the official giveaway rules.\n\n"
            "🚀 PROMOTER — INVITE MORE, GROW MORE!"
        )

        if not keyboard:
            send(
                chat_id,
                "❌ I couldn't find the bot username. "
                "Please check the bot configuration.",
            )
            return

        send(
            chat_id,
            announcement,
            reply_markup=keyboard,
        )
        return

    if command == "/topinviter":
        if chat_type != "private" and not is_target_chat(chat):
            return

        if db is None:
            send(chat_id, "⚠️ MongoDB is not configured.")
            return

        pipeline = [
            {"$match": {"chat_id": str(GROUP_ID)}},
            {
                "$group": {
                    "_id": "$inviter_id",
                    "count": {"$sum": 1},
                    "name": {"$last": "$inviter_name"},
                }
            },
            {"$sort": {"count": DESCENDING, "_id": ASCENDING}},
            {"$limit": 100},
        ]

        rows = list(db.referrals.aggregate(pipeline))

        if not rows:
            send(
                chat_id,
                "🏆 PROMOTER TOP INVITERS\n\n"
                "No tracked referrals yet.\n"
                "Use /mylink to get your personal invite link!",
            )
            return

        lines = [
            "🏆 PROMOTER TOP INVITERS",
            "🎁 500 TON GIVEAWAY",
            "",
        ]

        for rank, row in enumerate(rows, 1):
            name = str(row.get("name") or row["_id"])
            name = name.replace("<", "").replace(">", "")
            lines.append(
                f"{rank}. {name} — {row['count']} valid invite(s)"
            )

        lines += [
            "",
            "📌 Counts are based on tracked unique new members.",
        ]

        send(chat_id, "\n".join(lines))


def welcome_new_member(chat_id, user):
    """Post a short public welcome and giveaway CTA, mentioning the joiner."""
    user_id = user.get("id")
    if not user_id:
        return

    display_name = escape(str(user.get("first_name") or "friend"))
    mention = f'<a href="tg://user?id={int(user_id)}">{display_name}</a>'
    message = (
        f"🎉 Welcome to <b>PROMOTER</b>, {mention}! 🚀\n\n"
        "💎 <b>500 TON GIVEAWAY • 100 WINNERS</b> 🏆\n"
        "Invite genuine friends to compete for prizes!\n\n"
        "👇 Type <code>/mylink</code> in this group to get started."
    )
    send(chat_id, message, parse_mode="HTML")


def handle_chat_member(update):
    chat = update.get("chat", {})
    if not is_target_chat(chat):
        return

    old = update.get("old_chat_member", {})
    new = update.get("new_chat_member", {})

    # Trigger only when someone transitions from non-member to member.
    if is_member_status(old) or not is_member_status(new):
        return

    joined = new.get("user", {})

    # Public group welcome works even if MongoDB is temporarily unavailable.
    try:
        welcome_new_member(chat.get("id"), joined)
    except Exception:
        log.exception("Could not send new-member welcome message")

    # Referral tracking requires MongoDB and a tracked personal invite link.
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


@app.get("/")
def health():
    return jsonify({
        "ok": True,
        "service": "PROMOTER invite tracking bot",
    })


@app.post("/webhook")
def webhook():
    if WEBHOOK_SECRET:
        received = request.headers.get(
            "X-Telegram-Bot-Api-Secret-Token", ""
        )

        if received != WEBHOOK_SECRET:
            return jsonify({
                "ok": False,
                "error": "unauthorized",
            }), 403

    update = request.get_json(silent=True) or {}

    try:
        if "message" in update:
            handle_message(update["message"])

        if "chat_member" in update:
            handle_chat_member(update["chat_member"])

    except Exception:
        log.exception("Error handling Telegram update")

    return jsonify({"ok": True})


@app.get("/setup-webhook")
def setup_webhook():
    key = request.args.get("key", "")

    if not WEBHOOK_SECRET or key != WEBHOOK_SECRET:
        return jsonify({
            "ok": False,
            "error": "Invalid webhook setup key.",
        }), 403

    if not RENDER_EXTERNAL_URL:
        return jsonify({
            "ok": False,
            "error": "RENDER_EXTERNAL_URL is not set.",
        }), 500

    result = tg("setWebhook", {
        "url": f"{RENDER_EXTERNAL_URL}/webhook",
        "allowed_updates": ["message", "chat_member"],
        "drop_pending_updates": False,
        "secret_token": WEBHOOK_SECRET,
    })

    return jsonify(result)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
