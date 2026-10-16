import os
import logging
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
RENDER_EXTERNAL_URL = os.environ.get("RENDER_EXTERNAL_URL", "").rstrip("/")

if not BOT_TOKEN or not GROUP_ID or not MONGODB_URI:
    log.warning("Set BOT_TOKEN, GROUP_ID, and MONGODB_URI in Render environment variables.")

app = Flask(__name__)
client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=10000) if MONGODB_URI else None
db = client[DB_NAME] if client is not None else None
if db is not None:
    db.invite_links.create_index([("chat_id", ASCENDING), ("invite_link", ASCENDING)], unique=True)
    db.referrals.create_index([("chat_id", ASCENDING), ("referred_id", ASCENDING)], unique=True)
    db.referrals.create_index([("chat_id", ASCENDING), ("inviter_id", ASCENDING)])

API = f"https://api.telegram.org/bot{BOT_TOKEN}" if BOT_TOKEN else ""


def tg(method, payload=None):
    if not API:
        raise RuntimeError("BOT_TOKEN is not configured")
    r = requests.post(f"{API}/{method}", json=payload or {}, timeout=20)
    data = r.json()
    if not data.get("ok"):
        log.error("Telegram API %s failed: %s", method, data)
    return data


def send(chat_id, text, **kwargs):
    payload = {"chat_id": chat_id, "text": text, "disable_web_page_preview": True}
    payload.update(kwargs)
    return tg("sendMessage", payload)


def is_target_chat(chat):
    if str(chat.get("id")) == str(GROUP_ID):
        return True
    configured = GROUP_ID.lstrip("@").lower()
    username = str(chat.get("username", "")).lower()
    return bool(configured and username and configured == username)


def member_status_is_member(member):
    status = member.get("status", "")
    if status in ("creator", "administrator", "member"):
        return True
    return status == "restricted" and bool(member.get("is_member"))


def make_invite_link(user, destination_chat_id=None):
    chat_id = destination_chat_id or GROUP_ID
    result = tg("createChatInviteLink", {
        "chat_id": chat_id,
        "name": f"inviter_{user['id']}",
        "creates_join_request": False
    })
    if not result.get("ok"):
        return None, result.get("description", "Could not create invite link")
    link = result["result"]["invite_link"]
    if db is not None:
        db.invite_links.update_one(
            {"chat_id": str(chat_id), "invite_link": link},
            {"$set": {
                "chat_id": str(chat_id), "invite_link": link,
                "inviter_id": int(user["id"]),
                "inviter_name": user.get("username") or user.get("first_name") or str(user["id"]),
                "created_at": datetime.now(timezone.utc)
            }}, upsert=True
        )
    return link, None


def handle_message(message):
    text = (message.get("text") or "").strip()
    if not text.startswith("/"):
        return
    command = text.split()[0].split("@")[0].lower()
    chat = message.get("chat", {})
    sender = message.get("from", {})
    chat_id = chat.get("id")

    if command in ("/start", "/help"):
        send(chat_id,
             "👋 Welcome to PROMOTER Giveaway Bot!\n\n"
             "Use /mylink in this private chat to get your personal group invite link.\n"
             "Use /topinviter in the group to view the leaderboard.\n\n"
             "Only new members who join through your personal link count as referrals.")
        return

    if command == "/mylink":
        if chat.get("type") != "private":
            send(chat_id, "🔐 For privacy, open a private chat with me and send /mylink to get your personal invite link.")
            return
        link, error = make_invite_link(sender)
        if error:
            send(chat_id, "❌ I couldn't create your invite link. Please make sure I'm an administrator in the PROMOTER group with permission to invite users.\n\nDetails: " + error)
        else:
            send(chat_id,
                 "🚀 Your personal PROMOTER invite link:\n\n" + link +
                 "\n\n👥 Share it with friends. New members who join through this link will count toward your giveaway referral total.\n\n"
                 "Use /topinviter in the group to check the leaderboard.")
        return

    if command == "/topinviter":
        if not is_target_chat(chat) and chat.get("type") != "private":
            send(chat_id, "This command is available in the configured PROMOTER group.")
            return
        if db is None:
            send(chat_id, "⚠️ Database is not configured yet.")
            return
        pipeline = [
            {"$match": {"chat_id": str(GROUP_ID)}},
            {"$group": {"_id": "$inviter_id", "count": {"$sum": 1}, "name": {"$last": "$inviter_name"}}},
            {"$sort": {"count": DESCENDING, "_id": ASCENDING}},
            {"$limit": 100}
        ]
        rows = list(db.referrals.aggregate(pipeline))
        if not rows:
            send(chat_id, "🏆 PROMOTER TOP INVITERS\n\nNo tracked referrals yet. Send /mylink to the bot privately and share your personal link!")
            return
        lines = ["🏆 PROMOTER TOP INVITERS", "🎁 500 TON GIVEAWAY • TOP 100", ""]
        for i, row in enumerate(rows, 1):
            name = str(row.get("name") or row["_id"])
            safe_name = name.replace("<", "&lt;").replace(">", "&gt;")
            lines.append(f"{i}. {safe_name} — {row['count']} invite(s)")
        lines += ["", "📌 Counts are unique members tracked through personal invite links."]
        send(chat_id, "\n".join(lines))


def handle_chat_member(update):
    chat = update.get("chat", {})
    if not is_target_chat(chat):
        return
    old = update.get("old_chat_member", {})
    new = update.get("new_chat_member", {})
    if member_status_is_member(old) or not member_status_is_member(new):
        return
    joined = new.get("user", {})
    inviter_link = update.get("invite_link") or {}
    link = inviter_link.get("invite_link")
    if not joined.get("id") or not link or db is None:
        return
    record = db.invite_links.find_one({"chat_id": str(GROUP_ID), "invite_link": link})
    if not record:
        log.info("Join used an untracked invite link")
        return
    inviter_id = int(record["inviter_id"])
    referred_id = int(joined["id"])
    if inviter_id == referred_id:
        return
    try:
        db.referrals.insert_one({
            "chat_id": str(GROUP_ID), "inviter_id": inviter_id,
            "inviter_name": record.get("inviter_name", str(inviter_id)),
            "referred_id": referred_id,
            "referred_name": joined.get("username") or joined.get("first_name") or str(referred_id),
            "invite_link": link,
            "joined_at": datetime.now(timezone.utc)
        })
        log.info("Tracked referral: inviter=%s joined=%s", inviter_id, referred_id)
    except Exception as exc:
        # Duplicate referred_id means this user was already counted.
        if exc.__class__.__name__ != "DuplicateKeyError":
            log.exception("Could not save referral")


@app.get("/")
def health():
    return jsonify({"ok": True, "service": "PROMOTER invite tracking bot"})


@app.post("/webhook")
def webhook():
    if WEBHOOK_SECRET:
        received = request.headers.get("X-Telegram-Bot-Api-Secret-Token", "")
        if received != WEBHOOK_SECRET:
            return jsonify({"ok": False, "error": "unauthorized"}), 403
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
    # Call once after deployment: https://YOUR-SERVICE.onrender.com/setup-webhook?key=YOUR_WEBHOOK_SECRET
    key = request.args.get("key", "")
    if not WEBHOOK_SECRET or key != WEBHOOK_SECRET:
        return jsonify({"ok": False, "error": "Use the configured WEBHOOK_SECRET as key."}), 403
    if not RENDER_EXTERNAL_URL:
        return jsonify({"ok": False, "error": "RENDER_EXTERNAL_URL is not set."}), 500
    payload = {
        "url": f"{RENDER_EXTERNAL_URL}/webhook",
        "allowed_updates": ["message", "chat_member"],
        "drop_pending_updates": False,
        "secret_token": WEBHOOK_SECRET
    }
    result = tg("setWebhook", payload)
    return jsonify(result)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
