import os
import asyncio
import logging
import uuid
from datetime import datetime, timezone, timedelta

from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import ASCENDING, DESCENDING, ReturnDocument
from pymongo.errors import DuplicateKeyError

from telegram import Update, InputMediaVideo, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import TelegramError, RetryAfter, TimedOut, NetworkError
from telegram.ext import (
    Application, CommandHandler, MessageHandler, CallbackQueryHandler,
    ContextTypes, filters
)
from telegram.request import HTTPXRequest

from telethon import TelegramClient, functions
from telethon.sessions import StringSession
from telethon.errors import FloodWaitError


# ============================================================
# CONFIG
# ============================================================

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
MONGO_URI = os.getenv("MONGO_URI", "")
DB_NAME = os.getenv("DB_NAME", "video_storage_bot")

SEND_DELAY = float(os.getenv("SEND_DELAY", "5"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "10"))
ALBUM_SIZE = 10

LOCK_TTL_SECONDS = 90
HEARTBEAT_SECONDS = 25
JOB_LEASE_SECONDS = 180

# Personal Telegram account used for scanning.
API_ID = os.getenv("API_ID", "").strip()
API_HASH = os.getenv("API_HASH", "").strip()
STRING_SESSION = os.getenv("STRING_SESSION", "").strip()
SCAN_DELAY = float(os.getenv("SCAN_DELAY", "0.5"))

# Scan reports are sent to this Telegram group, not to the bot's private chat.
SCAN_LOG_GROUP_ID = os.getenv("SCAN_LOG_GROUP_ID", "").strip()
SCAN_REPORT_INTERVAL_SECONDS = max(
    30, int(os.getenv("SCAN_REPORT_INTERVAL_SECONDS", "150"))
)


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger("VideoStorageBot")


# ============================================================
# VALIDATION
# ============================================================

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is missing")
if not MONGO_URI:
    raise RuntimeError("MONGO_URI is missing")

SCAN_ENABLED = bool(API_ID and API_HASH and STRING_SESSION)


# ============================================================
# MONGODB
# ============================================================

mongo = AsyncIOMotorClient(
    MONGO_URI,
    serverSelectionTimeoutMS=30000,
    connectTimeoutMS=30000,
    socketTimeoutMS=120000,
)
db = mongo[DB_NAME]

videos_col = db["videos"]
jobs_col = db["jobs"]
settings_col = db["settings"]

# Checkpoints and scan counters are kept separately from the existing video data.
scan_states_col = db["scan_states"]


# ============================================================
# INSTANCE / GLOBALS
# ============================================================

INSTANCE_ID = str(uuid.uuid4())

worker_task = None
heartbeat_task = None
application = None

telethon_client = None
telethon_user_id = None
telethon_bot_username = None

scan_groups_cache = []
active_scan_task = None
active_scan_group_id = None
active_scan_state_id = None


# ============================================================
# TIME HELPERS
# ============================================================

def utc_now():
    return datetime.now(timezone.utc)


def utc_plus(seconds):
    return utc_now() + timedelta(seconds=seconds)


# ============================================================
# INDEX SETUP
# ============================================================

async def ensure_index_by_keys(collection, keys, *, unique=False, name=None):
    existing = await collection.index_information()
    wanted_keys = list(keys)

    for index_name, info in existing.items():
        if list(info.get("key", [])) == wanted_keys:
            existing_unique = bool(info.get("unique", False))
            if existing_unique == bool(unique):
                return index_name
            raise RuntimeError(
                f"Index exists with same keys but different unique option: {index_name}"
            )

    kwargs = {"unique": unique}
    if name:
        kwargs["name"] = name
    return await collection.create_index(keys, **kwargs)


async def ensure_indexes():
    logger.info("Checking MongoDB indexes...")

    await ensure_index_by_keys(
        videos_col, [("file_unique_id", ASCENDING)],
        unique=True, name="unique_file"
    )
    await ensure_index_by_keys(
        videos_col, [("seq", ASCENDING)],
        name="seq_index"
    )
    await ensure_index_by_keys(
        videos_col, [("caption", ASCENDING)],
        name="caption_index"
    )
    await ensure_index_by_keys(
        videos_col, [("created_at", DESCENDING)],
        name="created_index"
    )
    await ensure_index_by_keys(
        videos_col, [("source_chat_id", ASCENDING)],
        name="source_chat_index"
    )
    await ensure_index_by_keys(
        jobs_col, [("status", ASCENDING)],
        name="job_status_index"
    )
    await ensure_index_by_keys(
        jobs_col, [("created_at", DESCENDING)],
        name="job_created_index"
    )

    logger.info("MongoDB indexes ready.")


# ============================================================
# BOT INSTANCE LOCK
# ============================================================

async def acquire_instance_lock():
    now = utc_now()
    expires = utc_plus(LOCK_TTL_SECONDS)

    try:
        result = await settings_col.find_one_and_update(
            {
                "_id": "bot_instance_lock",
                "$or": [
                    {"expires_at": {"$lte": now}},
                    {"instance_id": INSTANCE_ID},
                    {"instance_id": {"$exists": False}},
                ],
            },
            {
                "$set": {
                    "instance_id": INSTANCE_ID,
                    "started_at": now,
                    "heartbeat_at": now,
                    "expires_at": expires,
                }
            },
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
        return bool(result and result.get("instance_id") == INSTANCE_ID)
    except DuplicateKeyError:
        return False


async def heartbeat_loop():
    while True:
        try:
            await asyncio.sleep(HEARTBEAT_SECONDS)
            result = await settings_col.update_one(
                {"_id": "bot_instance_lock", "instance_id": INSTANCE_ID},
                {"$set": {
                    "heartbeat_at": utc_now(),
                    "expires_at": utc_plus(LOCK_TTL_SECONDS),
                }},
            )
            if result.modified_count == 0:
                logger.error("Instance lock was lost!")
        except asyncio.CancelledError:
            break
        except Exception:
            logger.exception("Heartbeat error")


async def release_instance_lock():
    try:
        await settings_col.delete_one({
            "_id": "bot_instance_lock",
            "instance_id": INSTANCE_ID,
        })
    except Exception:
        logger.exception("Failed to release instance lock")


# ============================================================
# JOB RECOVERY ON STARTUP
# ============================================================

async def mark_stale_jobs_uncertain():
    now = utc_now()
    cursor = jobs_col.find({
        "status": "running",
        "$or": [
            {"worker_id": {"$ne": INSTANCE_ID}},
            {"lease_until": {"$lt": now}},
        ],
    })

    count = 0
    async for job in cursor:
        current_batch = job.get("current_batch")
        update_data = {
            "status": "uncertain",
            "uncertain_reason": (
                "Previous worker stopped during an active batch. "
                "Album delivery status is unknown."
            ),
            "updated_at": now,
        }

        if current_batch:
            update_data["uncertain_batch"] = {
                "first_seq": int(current_batch["first_seq"]),
                "last_seq": int(current_batch["last_seq"]),
                "count": int(current_batch["count"]),
            }

        await jobs_col.update_one(
            {"_id": job["_id"], "status": "running"},
            {"$set": update_data},
        )
        count += 1

    if count:
        logger.warning("Marked %s stale job(s) as uncertain.", count)


# ============================================================
# VIDEO STORAGE - ORIGINAL STORAGE LOGIC + SCAN COUNTERS
# ============================================================

async def get_next_seq():
    result = await settings_col.find_one_and_update(
        {"_id": "video_counter"},
        {"$inc": {"value": 1}},
        upsert=True,
        return_document=ReturnDocument.AFTER,
    )
    return int(result["value"])


async def save_video_message(message):
    # Only Bot API "video" messages are stored.
    # Files received as documents are not stored by this handler.
    if not message.video:
        return None

    video = message.video
    file_unique_id = video.file_unique_id

    existing = await videos_col.find_one(
        {"file_unique_id": file_unique_id},
        {"_id": 1, "seq": 1},
    )
    if existing:
        logger.info("Duplicate video ignored: %s", file_unique_id)
        return {"seq": existing.get("seq"), "status": "duplicate"}

    seq = await get_next_seq()
    document = {
        "seq": seq,
        "file_id": video.file_id,
        "file_unique_id": file_unique_id,
        "width": video.width,
        "height": video.height,
        "duration": video.duration,
        "file_size": video.file_size,
        "caption": message.caption or "",
        "source_chat_id": message.chat_id,
        "source_message_id": message.message_id,
        "created_at": utc_now(),
    }

    try:
        await videos_col.insert_one(document)
        logger.info("Saved video #%s", seq)
        return {"seq": seq, "status": "stored"}
    except DuplicateKeyError:
        existing = await videos_col.find_one(
            {"file_unique_id": file_unique_id},
            {"seq": 1},
        )
        return {
            "seq": existing.get("seq") if existing else None,
            "status": "duplicate",
        }


# ============================================================
# AUTO SAVE VIDEO - ORIGINAL HANDLER + SCAN STORAGE COUNTS
# ============================================================

def is_forwarded_video_message(message):
    # Telethon forwards to the bot's private chat arrive as forwarded messages.
    # Count only forwarded videos while a scan is active, not ordinary uploads.
    return bool(
        getattr(message, "forward_origin", None)
        or getattr(message, "forward_date", None)
        or getattr(message, "forward_from", None)
        or getattr(message, "forward_from_chat", None)
    )


async def video_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    try:
        result = await save_video_message(update.message)

        if (
            result
            and active_scan_state_id
            and is_forwarded_video_message(update.message)
        ):
            counter = "stored" if result["status"] == "stored" else "duplicates"
            await scan_states_col.update_one(
                {"_id": active_scan_state_id, "status": "running"},
                {"$inc": {counter: 1}, "$set": {"updated_at": utc_now()}},
            )
    except Exception:
        logger.exception("Video save failed")
        if active_scan_state_id:
            try:
                await scan_states_col.update_one(
                    {"_id": active_scan_state_id, "status": "running"},
                    {"$inc": {"storage_errors": 1}},
                    )
            except Exception:
                logger.exception("Could not update scan storage error counter")


# ============================================================
# SCAN FEATURE
# ============================================================

def scan_user_allowed(user):
    return bool(user and telethon_user_id and user.id == telethon_user_id)


def scan_log_chat_id():
    if not SCAN_LOG_GROUP_ID:
        raise RuntimeError(
            "SCAN_LOG_GROUP_ID is missing. Add your Update Group ID in Render."
        )
    try:
        return int(SCAN_LOG_GROUP_ID)
    except ValueError as exc:
        raise RuntimeError("SCAN_LOG_GROUP_ID must be a numeric Telegram chat ID.") from exc


async def get_chat_message_total(entity):
    """Ask Telegram for the total history count without downloading message history."""
    try:
        result = await telethon_client.get_messages(entity, limit=0)
        total = getattr(result, "total", None)
        if total is None:
            total = len(result)
        return int(total)
    except Exception:
        logger.exception("Could not determine total message count")
        return None


async def get_forum_topic_count(entity):
    """Return topic count for forum groups; None means Telegram did not provide it."""
    if not bool(getattr(entity, "forum", False)):
        return 0

    try:
        result = await telethon_client(
            functions.messages.GetForumTopicsRequest(
                peer=entity,
                offset_date=None,
                offset_id=0,
                offset_topic=0,
                limit=1,
            )
        )
        count = getattr(result, "count", None)
        if count is not None:
            return int(count)
        return len(getattr(result, "topics", []) or [])
    except Exception:
        logger.exception("Could not read forum topic count")
        return None


async def send_scan_report(context, text):
    await context.bot.send_message(
        chat_id=scan_log_chat_id(),
        text=text,
        disable_web_page_preview=True,
    )


def scan_progress_text(
    group_title, chat_type, checked, total_messages, video_messages,
    forwarded, stored, duplicates, skipped, topics, last_message_id,
):
    remaining = (
        max(total_messages - checked, 0)
        if total_messages is not None else None
    )
    remaining_text = f"{remaining:,}" if remaining is not None else "Unavailable"
    topic_text = str(topics) if topics is not None else "Unavailable"

    return (
        "🔄 SCAN PROGRESS UPDATE\n\n"
        f"Chat: {group_title}\n"
        f"Type: {chat_type}\n"
        f"Messages checked: {checked:,}\n"
        f"Messages remaining: {remaining_text}\n"
        f"Video messages found: {video_messages:,}\n"
        f"Forwarded to bot: {forwarded:,}\n"
        f"Actually stored: {stored:,}\n"
        f"Duplicates ignored: {duplicates:,}\n"
        f"Could not forward: {skipped:,}\n"
        f"Forum topics: {topic_text}\n"
        f"Last message ID: {last_message_id}\n\n"
        "Note: Stored/duplicate counts update when the bot receives each forwarded video."
    )


async def cmd_scan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global scan_groups_cache

    if not SCAN_ENABLED or not telethon_client or not telethon_user_id:
        await update.message.reply_text(
            "❌ Scan is not configured.\n\n"
            "Set API_ID, API_HASH and STRING_SESSION in Render."
        )
        return

    if not scan_user_allowed(update.effective_user):
        await update.message.reply_text(
            "❌ Only the connected Personal Telegram Account can use /scan."
        )
        return

    try:
        log_id = scan_log_chat_id()
        # Verify the Bot API can access the configured report group before listing chats.
        await context.bot.send_message(
            chat_id=log_id,
            text="🟢 Video Storage Bot: scan reporting connection is ready.",
        )
    except Exception as exc:
        await update.message.reply_text(
            "❌ Update Group is not configured or the bot cannot send messages there.\n\n"
            "Set SCAN_LOG_GROUP_ID to the group's numeric ID in Render, add the bot "
            "to that group, and allow it to send messages.\n\n"
            f"Error: {type(exc).__name__}"
        )
        return

    await update.message.reply_text(
        "🔍 Loading groups and channels from your Personal Telegram Account..."
    )

    try:
        scan_groups_cache = []
        async for dialog in telethon_client.iter_dialogs():
            # Includes groups, supergroups and broadcast channels; excludes users.
            if dialog.is_group or dialog.is_channel:
                is_broadcast = bool(
                    dialog.is_channel and not dialog.is_group
                )
                scan_groups_cache.append({
                    "id": int(dialog.id),
                    "title": dialog.title or "Unnamed chat",
                    "entity": dialog.entity,
                    "chat_type": "Channel" if is_broadcast else "Group/Supergroup",
                })

        if not scan_groups_cache:
            await update.message.reply_text(
                "No accessible groups or channels found in this Telegram account."
            )
            return

        await send_scan_group_page(
            update.effective_chat.id,
            context,
            0,
            edit_message_id=None,
        )

    except Exception:
        logger.exception("Could not list Telegram groups/channels")
        await update.message.reply_text(
            "❌ Could not load groups/channels. Check the Personal Account session and access."
        )


async def send_scan_group_page(chat_id, context, page, edit_message_id=None):
    page_size = 8
    start = page * page_size
    end = start + page_size

    buttons = []
    for group in scan_groups_cache[start:end]:
        title = (group["title"] or "Unnamed chat")[:42]
        label = f"📢 {title}" if group.get("chat_type") == "Channel" else f"👥 {title}"
        buttons.append([
            InlineKeyboardButton(
                label,
                callback_data=f"scanstart:{group['id']}",
            )
        ])

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(
            "⬅️ Previous", callback_data=f"scanpage:{page - 1}"
        ))
    if end < len(scan_groups_cache):
        nav.append(InlineKeyboardButton(
            "Next ➡️", callback_data=f"scanpage:{page + 1}"
        ))
    if nav:
        buttons.append(nav)

    markup = InlineKeyboardMarkup(buttons)
    text = (
        "📂 YOUR TELEGRAM GROUPS & CHANNELS\n\n"
        f"Total chats: {len(scan_groups_cache)}\n"
        f"Page: {page + 1} / {max(1, (len(scan_groups_cache) + page_size - 1) // page_size)}\n\n"
        "Choose a group or channel to scan:"
    )

    if edit_message_id:
        await context.bot.edit_message_text(
            chat_id=chat_id,
            message_id=edit_message_id,
            text=text,
            reply_markup=markup,
        )
    else:
        await context.bot.send_message(
            chat_id=chat_id,
            text=text,
            reply_markup=markup,
        )


async def scan_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global active_scan_task, active_scan_group_id, active_scan_state_id

    query = update.callback_query
    if not scan_user_allowed(query.from_user):
        await query.answer("Not authorized.", show_alert=True)
        return

    await query.answer()
    data = query.data or ""

    if data.startswith("scanpage:"):
        try:
            page = int(data.split(":", 1)[1])
            await send_scan_group_page(
                query.message.chat_id,
                context,
                page,
                edit_message_id=query.message.message_id,
            )
        except Exception:
            logger.exception("Could not change scan page")
        return

    if data.startswith("scanstart:"):
        try:
            chat_id = int(data.split(":", 1)[1])
        except ValueError:
            await query.message.reply_text("Invalid group/channel selection.")
            return

        if active_scan_task and not active_scan_task.done():
            await query.message.reply_text(
                f"⚠️ A scan is already running for chat ID {active_scan_group_id}."
            )
            return

        group = next(
            (g for g in scan_groups_cache if g["id"] == chat_id),
            None,
        )
        if not group:
            await query.message.reply_text(
                "Selection expired. Send /scan again."
            )
            return

        try:
            report_chat_id = scan_log_chat_id()
        except Exception as exc:
            await query.message.reply_text(f"❌ {exc}")
            return

        await query.message.reply_text(
            "🚀 SCAN STARTED\n\n"
            f"Chat: {group['title']}\n"
            f"Type: {group['chat_type']}\n"
            "Progress and completion reports will be sent to the Update Group."
        )

        active_scan_group_id = chat_id
        active_scan_state_id = f"scan_{chat_id}"
        active_scan_task = asyncio.create_task(
            scan_group_history(
                entity=group["entity"],
                chat_id=chat_id,
                group_title=group["title"],
                chat_type=group["chat_type"],
                report_chat_id=report_chat_id,
                context=context,
            )
        )


async def scan_group_history(
    entity,
    chat_id,
    group_title,
    chat_type,
    report_chat_id,
    context,
):
    """
    Scans old messages in ascending order and forwards video messages to the bot.
    The Bot API video handler stores each received video using the existing
    file_unique_id duplicate check. Scan checkpoints and counters are persisted.
    """
    global active_scan_group_id, active_scan_state_id

    state_id = f"scan_{chat_id}"
    state = await scan_states_col.find_one({"_id": state_id})

    last_message_id = int(state.get("last_message_id", 0)) if state else 0
    checked = int(state.get("checked", 0)) if state else 0
    video_messages = int(state.get("video_messages", 0)) if state else 0
    forwarded = int(state.get("forwarded", 0)) if state else 0
    skipped = int(state.get("skipped", 0)) if state else 0
    stored = int(state.get("stored", 0)) if state else 0
    duplicates = int(state.get("duplicates", 0)) if state else 0
    storage_errors = int(state.get("storage_errors", 0)) if state else 0

    total_messages = await get_chat_message_total(entity)
    topics = await get_forum_topic_count(entity)

    await scan_states_col.update_one(
        {"_id": state_id},
        {"$set": {
            "group_id": chat_id,
            "group_title": group_title,
            "chat_type": chat_type,
            "status": "running",
            "total_messages": total_messages,
            "topics": topics,
            "started_at": state.get("started_at", utc_now()) if state else utc_now(),
            "updated_at": utc_now(),
        }, "$setOnInsert": {
            "stored": 0,
            "duplicates": 0,
            "storage_errors": 0,
        }},
        upsert=True,
    )

    loop = asyncio.get_running_loop()
    last_report_time = loop.time()

    try:
        total_text = f"{total_messages:,}" if total_messages is not None else "Unavailable"
        await send_scan_report(
            context,
            "🚀 SCAN STARTED\n\n"
            f"Chat: {group_title}\n"
            f"Type: {chat_type}\n"
            f"Total messages: {total_text}",
        )

        # min_id is exclusive. reverse=True scans oldest to newest.
        async for message in telethon_client.iter_messages(
            entity,
            reverse=True,
            min_id=last_message_id,
        ):
            checked += 1

            if getattr(message, "video", None):
                video_messages += 1
                sent = False

                while not sent:
                    try:
                        # Forwarding preserves Telegram's original media identity.
                        # Protected-content messages cannot be forwarded; no bypass is attempted.
                        await telethon_client.forward_messages(
                            telethon_bot_username,
                            message,
                        )
                        forwarded += 1
                        sent = True

                    except FloodWaitError as exc:
                        wait_seconds = int(exc.seconds) + 2
                        logger.warning("Scan FloodWait: waiting %s seconds", wait_seconds)
                        await asyncio.sleep(wait_seconds)

                    except Exception as exc:
                        skipped += 1
                        logger.warning(
                            "Could not forward source message %s from %s: %s",
                            message.id, chat_id, type(exc).__name__,
                        )
                        sent = True

            last_message_id = int(message.id)

            # Save checkpoint after each source message.
            await scan_states_col.update_one(
                {"_id": state_id},
                {"$set": {
                    "last_message_id": last_message_id,
                    "checked": checked,
                    "video_messages": video_messages,
                    "forwarded": forwarded,
                    "skipped": skipped,
                    "status": "running",
                    "updated_at": utc_now(),
                }},
                upsert=True,
            )

            # Refresh stored/duplicate counters, which are updated by video_handler.
            if SCAN_DELAY > 0:
                await asyncio.sleep(SCAN_DELAY)

            now_mono = loop.time()
            if now_mono - last_report_time >= SCAN_REPORT_INTERVAL_SECONDS:
                current_state = await scan_states_col.find_one({"_id": state_id}) or {}
                stored = int(current_state.get("stored", stored))
                duplicates = int(current_state.get("duplicates", duplicates))
                storage_errors = int(current_state.get("storage_errors", storage_errors))

                await send_scan_report(
                    context,
                    scan_progress_text(
                        group_title, chat_type, checked, total_messages,
                        video_messages, forwarded, stored, duplicates, skipped,
                        topics, last_message_id,
                    ),
                )
                last_report_time = now_mono

        # Give Bot API polling a short time to process the final forwarded videos.
        await asyncio.sleep(5)
        current_state = await scan_states_col.find_one({"_id": state_id}) or {}
        stored = int(current_state.get("stored", stored))
        duplicates = int(current_state.get("duplicates", duplicates))
        storage_errors = int(current_state.get("storage_errors", storage_errors))

        remaining = (
            max(total_messages - checked, 0)
            if total_messages is not None else None
        )
        await scan_states_col.update_one(
            {"_id": state_id},
            {"$set": {
                "status": "completed",
                "checked": checked,
                "video_messages": video_messages,
                "forwarded": forwarded,
                "stored": stored,
                "duplicates": duplicates,
                "skipped": skipped,
                "storage_errors": storage_errors,
                "remaining_messages": remaining,
                "completed_at": utc_now(),
                "updated_at": utc_now(),
            }},
        )

        remaining_text = f"{remaining:,}" if remaining is not None else "Unavailable"
        topic_text = str(topics) if topics is not None else "Unavailable"
        await send_scan_report(
            context,
            "✅ SCAN COMPLETED\n\n"
            f"Chat: {group_title}\n"
            f"Type: {chat_type}\n"
            f"Messages checked: {checked:,}\n"
            f"Messages remaining: {remaining_text}\n"
            f"Video messages found: {video_messages:,}\n"
            f"Forwarded to bot: {forwarded:,}\n"
            f"Actually stored: {stored:,}\n"
            f"Duplicates ignored: {duplicates:,}\n"
            f"Could not forward: {skipped:,}\n"
            f"Storage errors: {storage_errors:,}\n"
            f"Forum topics: {topic_text}\n\n"
            "✅ Scan finished. Checkpoint saved.",
        )

    except asyncio.CancelledError:
        current_state = await scan_states_col.find_one({"_id": state_id}) or {}
        await scan_states_col.update_one(
            {"_id": state_id},
            {"$set": {
                "status": "paused",
                "last_message_id": last_message_id,
                "checked": checked,
                "video_messages": video_messages,
                "forwarded": forwarded,
                "stored": int(current_state.get("stored", stored)),
                "duplicates": int(current_state.get("duplicates", duplicates)),
                "skipped": skipped,
                "updated_at": utc_now(),
            }},
            upsert=True,
        )
        raise

    except Exception as exc:
        logger.exception("Scan failed")
        current_state = await scan_states_col.find_one({"_id": state_id}) or {}
        stored = int(current_state.get("stored", stored))
        duplicates = int(current_state.get("duplicates", duplicates))
        storage_errors = int(current_state.get("storage_errors", storage_errors))

        await scan_states_col.update_one(
            {"_id": state_id},
            {"$set": {
                "status": "paused",
                "last_message_id": last_message_id,
                "checked": checked,
                "video_messages": video_messages,
                "forwarded": forwarded,
                "stored": stored,
                "duplicates": duplicates,
                "skipped": skipped,
                "storage_errors": storage_errors,
                "error": str(exc),
                "updated_at": utc_now(),
            }},
            upsert=True,
        )
        await send_scan_report(
            context,
            "⚠️ SCAN STOPPED / PAUSED\n\n"
            f"Chat: {group_title}\n"
            f"Messages checked: {checked:,}\n"
            f"Video messages found: {video_messages:,}\n"
            f"Forwarded to bot: {forwarded:,}\n"
            f"Actually stored: {stored:,}\n"
            f"Duplicates ignored: {duplicates:,}\n"
            f"Could not forward: {skipped:,}\n"
            f"Last message ID: {last_message_id}\n\n"
            "Progress is saved. Send /scan and select this chat again to continue."
        )
    finally:
        active_scan_group_id = None
        active_scan_state_id = None


# ============================================================
# STATS - ORIGINAL
# ============================================================

async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user:
        return
    total = await videos_col.count_documents({})
    last = await videos_col.find_one({}, sort=[("seq", DESCENDING)])
    last_seq = last.get("seq", 0) if last else 0
    await update.message.reply_text(
        "📊 VIDEO STORAGE STATS\n\n"
        f"Videos: {total:,}\n"
        f"Last ID: {last_seq:,}"
    )


# ============================================================
# LIST - ORIGINAL
# ============================================================

async def cmd_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        limit = 20
        if context.args:
            try:
                limit = int(context.args[0])
            except ValueError:
                limit = 20
        limit = max(1, min(limit, 100))

        cursor = videos_col.find(
            {},
            {"_id": 0, "seq": 1, "caption": 1, "file_unique_id": 1},
        ).sort("seq", ASCENDING).limit(limit)

        rows = []
        async for video in cursor:
            caption = video.get("caption") or ""
            if len(caption) > 50:
                caption = caption[:50] + "..."
            rows.append(f"#{video.get('seq')} {caption}")

        if not rows:
            await update.message.reply_text("No videos found.")
            return

        await update.message.reply_text("🎬 VIDEO LIST\n\n" + "\n".join(rows))
    except Exception:
        logger.exception("List failed")
        await update.message.reply_text("❌ Failed to list videos.")


# ============================================================
# SEARCH - ORIGINAL
# ============================================================

async def cmd_search(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage:\n/search keyword")
        return

    text = " ".join(context.args)
    cursor = videos_col.find(
        {"$or": [
            {"caption": {"$regex": text, "$options": "i"}},
            {"file_unique_id": {"$regex": text, "$options": "i"}},
        ]},
        {"_id": 0, "seq": 1, "caption": 1},
    ).sort("seq", ASCENDING).limit(50)

    rows = []
    async for video in cursor:
        caption = video.get("caption") or ""
        if len(caption) > 80:
            caption = caption[:80] + "..."
        rows.append(f"#{video.get('seq')} {caption}")

    if not rows:
        await update.message.reply_text("No matching videos.")
        return

    await update.message.reply_text("🔎 SEARCH RESULTS\n\n" + "\n".join(rows))


# ============================================================
# SEND SINGLE VIDEO - ORIGINAL
# ============================================================

async def cmd_send(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if len(context.args) < 2:
        await update.message.reply_text("Usage:\n/send VIDEO_ID CHAT_ID")
        return

    try:
        seq = int(context.args[0])
        chat_id = int(context.args[1])
    except ValueError:
        await update.message.reply_text("❌ Invalid ID.")
        return

    video = await videos_col.find_one({"seq": seq})
    if not video:
        await update.message.reply_text(f"❌ Video #{seq} not found.")
        return

    try:
        await context.bot.send_video(
            chat_id=chat_id,
            video=video["file_id"],
            caption=video.get("caption") or None,
            read_timeout=180,
            write_timeout=180,
            connect_timeout=30,
            pool_timeout=30,
        )
        await update.message.reply_text(f"✅ Video #{seq} sent.")
    except Exception as exc:
        logger.exception("Single send failed")
        await update.message.reply_text(f"❌ Send failed:\n{type(exc).__name__}")


# ============================================================
# SENDALL HELPERS - ORIGINAL
# ============================================================

async def get_next_batch(last_seq):
    cursor = videos_col.find(
        {"seq": {"$gt": last_seq}},
        {"_id": 0, "seq": 1, "file_id": 1, "caption": 1},
    ).sort("seq", ASCENDING).limit(ALBUM_SIZE)
    return await cursor.to_list(length=ALBUM_SIZE)


async def claim_batch(job_id, batch):
    if not batch:
        return False

    first_seq = int(batch[0]["seq"])
    last_batch_seq = int(batch[-1]["seq"])
    now = utc_now()

    result = await jobs_col.update_one(
        {"_id": job_id, "worker_id": INSTANCE_ID, "status": "running"},
        {"$set": {
            "current_batch": {
                "first_seq": first_seq,
                "last_seq": last_batch_seq,
                "count": len(batch),
            },
            "batch_first_seq": first_seq,
            "batch_last_seq": last_batch_seq,
            "batch_count": len(batch),
            "batch_started_at": now,
            "lease_until": utc_plus(JOB_LEASE_SECONDS),
            "updated_at": now,
        }},
    )
    return result.modified_count == 1


async def send_album_once(bot, chat_id, batch):
    media = []
    for video in batch:
        media.append(InputMediaVideo(
            media=video["file_id"],
            caption=video.get("caption") or None,
        ))

    retry_count = 0
    while True:
        try:
            await bot.send_media_group(
                chat_id=chat_id,
                media=media,
                connect_timeout=30,
                read_timeout=180,
                write_timeout=180,
                pool_timeout=30,
            )
            return "sent"

        except RetryAfter as exc:
            retry_count += 1
            if retry_count > MAX_RETRIES:
                raise
            wait_time = float(exc.retry_after)
            logger.warning("Telegram rate limit. Waiting %.2fs", wait_time)
            await asyncio.sleep(wait_time + 1)

        except (TimedOut, NetworkError) as exc:
            logger.error("Network error during album send: %s", exc)
            return "uncertain"

        except TelegramError:
            logger.exception("Telegram rejected album")
            return "failed"

        except Exception:
            logger.exception("Unexpected album error")
            return "uncertain"


async def commit_batch(job_id, expected_last_seq, batch_last_seq):
    now = utc_now()
    result = await jobs_col.update_one(
        {
            "_id": job_id,
            "worker_id": INSTANCE_ID,
            "status": "running",
            "last_seq": expected_last_seq,
            "batch_last_seq": batch_last_seq,
        },
        {
            "$set": {
                "last_seq": batch_last_seq,
                "current_batch": None,
                "batch_first_seq": None,
                "batch_last_seq": None,
                "batch_count": 0,
                "batch_started_at": None,
                "updated_at": now,
                "lease_until": utc_plus(JOB_LEASE_SECONDS),
            },
            "$inc": {"sent": 1},
        },
    )
    return result.modified_count == 1


async def mark_job_uncertain(job_id, batch, reason):
    if not batch:
        return
    await jobs_col.update_one(
        {"_id": job_id, "worker_id": INSTANCE_ID},
        {"$set": {
            "status": "uncertain",
            "uncertain_batch": {
                "first_seq": int(batch[0]["seq"]),
                "last_seq": int(batch[-1]["seq"]),
                "count": len(batch),
            },
            "uncertain_reason": reason,
            "updated_at": utc_now(),
        }},
    )


async def process_job(job):
    job_id = job["_id"]
    chat_id = int(job["chat_id"])
    logger.info("Starting job %s for chat %s", job_id, chat_id)

    while True:
        current = await jobs_col.find_one({
            "_id": job_id,
            "worker_id": INSTANCE_ID,
        })
        if not current:
            return

        status = current.get("status")
        if status in ("cancelled", "paused", "uncertain"):
            logger.info("Job %s stopped with status %s", job_id, status)
            return

        last_seq = int(current.get("last_seq", 0))
        batch = await get_next_batch(last_seq)

        if not batch:
            await jobs_col.update_one(
                {"_id": job_id, "worker_id": INSTANCE_ID, "status": "running"},
                {"$set": {
                    "status": "completed",
                    "completed_at": utc_now(),
                    "updated_at": utc_now(),
                    "current_batch": None,
                }},
            )
            logger.info("Job %s completed.", job_id)
            return

        if not await claim_batch(job_id, batch):
            return

        first_seq = int(batch[0]["seq"])
        batch_last_seq = int(batch[-1]["seq"])
        result = await send_album_once(application.bot, chat_id, batch)

        if result == "sent":
            committed = await commit_batch(job_id, last_seq, batch_last_seq)
            if not committed:
                await mark_job_uncertain(
                    job_id, batch,
                    "Album was sent, but progress commit failed. Check destination before recovery.",
                )
                return

            logger.info("Job %s batch %s-%s completed.", job_id, first_seq, batch_last_seq)
            if SEND_DELAY > 0:
                await asyncio.sleep(SEND_DELAY)
            continue

        if result == "uncertain":
            await mark_job_uncertain(
                job_id, batch,
                "Network timeout/error after send attempt. Delivery status is uncertain; automatic retry disabled.",
            )
            return

        if result == "failed":
            await jobs_col.update_one(
                {"_id": job_id, "worker_id": INSTANCE_ID},
                {"$inc": {"failed": 1}, "$set": {
                    "status": "failed",
                    "updated_at": utc_now(),
                }},
            )
            return


async def queue_worker():
    logger.info("Queue worker started.")
    while True:
        try:
            now = utc_now()
            job = await jobs_col.find_one_and_update(
                {"status": "queued"},
                {"$set": {
                    "status": "running",
                    "worker_id": INSTANCE_ID,
                    "started_at": now,
                    "updated_at": now,
                    "lease_until": utc_plus(JOB_LEASE_SECONDS),
                }},
                sort=[("created_at", ASCENDING)],
                return_document=ReturnDocument.AFTER,
            )

            if not job:
                await asyncio.sleep(1)
                continue

            logger.info("Claimed job %s", job["_id"])
            await process_job(job)

        except asyncio.CancelledError:
            break
        except Exception:
            logger.exception("Queue worker error")
            await asyncio.sleep(3)


# ============================================================
# SENDALL COMMANDS - ORIGINAL
# ============================================================

async def cmd_sendall(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage:\n/sendall CHAT_ID")
        return

    try:
        chat_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("❌ Invalid CHAT_ID.")
        return

    active = await jobs_col.find_one(
        {
            "chat_id": chat_id,
            "status": {"$in": ["queued", "running", "paused", "uncertain"]},
        },
        {"_id": 1, "status": 1},
    )
    if active:
        await update.message.reply_text(
            "⚠️ This chat already has an active job.\n\n"
            f"Job: {active['_id']}\nStatus: {active['status']}\n\nUse /job to check it."
        )
        return

    first = await videos_col.find_one({}, {"seq": 1}, sort=[("seq", ASCENDING)])
    if not first:
        await update.message.reply_text("❌ No videos stored.")
        return

    job_id = uuid.uuid4().hex
    job = {
        "_id": job_id,
        "status": "queued",
        "chat_id": chat_id,
        "last_seq": 0,
        "sent": 0,
        "failed": 0,
        "worker_id": None,
        "snapshot": await videos_col.count_documents({}),
        "current_batch": None,
        "batch_first_seq": None,
        "batch_last_seq": None,
        "batch_count": 0,
        "uncertain_batch": None,
        "uncertain_reason": None,
        "created_at": utc_now(),
        "updated_at": utc_now(),
    }
    await jobs_col.insert_one(job)

    await update.message.reply_text(
        "🚀 SENDALL JOB CREATED\n\n"
        f"Job: {job_id}\nChat: {chat_id}\n"
        f"Videos snapshot: {job['snapshot']:,}\n\n"
        "10 videos per album.\nUse /job to monitor."
    )


async def cmd_job(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if context.args:
        job = await jobs_col.find_one({"_id": context.args[0]})
    else:
        job = await jobs_col.find_one({}, sort=[("created_at", DESCENDING)])

    if not job:
        await update.message.reply_text("No jobs found.")
        return

    text = (
        "📦 CURRENT JOB\n\n"
        f"Job: {job['_id']}\n"
        f"Status: {job.get('status')}\n"
        f"Chat: {job.get('chat_id')}\n"
        f"Last ID: {job.get('last_seq', 0)}\n"
        f"Sent batches: {job.get('sent', 0)}\n"
        f"Failed: {job.get('failed', 0)}\n"
        f"Worker: {job.get('worker_id')}\n"
        f"Snapshot: {job.get('snapshot', 0)}\n"
    )

    current_batch = job.get("current_batch")
    if current_batch:
        text += (
            "\nCURRENT BATCH\n"
            f"From: {current_batch.get('first_seq')}\n"
            f"To: {current_batch.get('last_seq')}\n"
            f"Count: {current_batch.get('count')}\n"
        )

    uncertain = job.get("uncertain_batch")
    if uncertain:
        text += (
            "\n⚠️ UNCERTAIN BATCH\n"
            f"From: {uncertain.get('first_seq')}\n"
            f"To: {uncertain.get('last_seq')}\n"
            f"Count: {uncertain.get('count')}\n"
            f"Reason: {job.get('uncertain_reason')}\n"
        )

    await update.message.reply_text(text)


async def cmd_pause(update: Update, context: ContextTypes.DEFAULT_TYPE):
    job = await jobs_col.find_one(
        {"status": "running"},
        sort=[("created_at", DESCENDING)],
    )
    if not job:
        await update.message.reply_text("No running job.")
        return

    await jobs_col.update_one(
        {"_id": job["_id"], "status": "running"},
        {"$set": {"status": "paused", "updated_at": utc_now()}},
    )
    await update.message.reply_text(f"⏸ Job paused.\n\nJob: {job['_id']}")


async def cmd_resume(update: Update, context: ContextTypes.DEFAULT_TYPE):
    job = await jobs_col.find_one(
        {"status": "paused"},
        sort=[("created_at", DESCENDING)],
    )
    if not job:
        await update.message.reply_text("No paused job found.")
        return

    await jobs_col.update_one(
        {"_id": job["_id"], "status": "paused"},
        {"$set": {
            "status": "queued",
            "worker_id": None,
            "updated_at": utc_now(),
        }},
    )
    await update.message.reply_text(f"▶️ Job resumed.\n\nJob: {job['_id']}")


async def cmd_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if context.args:
        job = await jobs_col.find_one({"_id": context.args[0]})
    else:
        job = await jobs_col.find_one(
            {"status": {"$in": ["queued", "running", "paused", "uncertain"]}},
            sort=[("created_at", DESCENDING)],
        )

    if not job:
        await update.message.reply_text("No active job found.")
        return

    await jobs_col.update_one(
        {
            "_id": job["_id"],
            "status": {"$in": ["queued", "running", "paused", "uncertain"]},
        },
        {"$set": {"status": "cancelled", "updated_at": utc_now()}},
    )
    await update.message.reply_text(f"🛑 Job cancelled.\n\nJob: {job['_id']}")


async def cmd_recover(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if len(context.args) < 2:
        await update.message.reply_text(
            "Usage:\n/recover JOB_ID SENT_LAST_SEQ\n\n"
            "Examples:\n/recover JOB_ID 0\n/recover JOB_ID 10"
        )
        return

    job_id = context.args[0]
    try:
        confirmed_last_seq = int(context.args[1])
    except ValueError:
        await update.message.reply_text("❌ SENT_LAST_SEQ must be a number.")
        return

    job = await jobs_col.find_one({"_id": job_id})
    if not job:
        await update.message.reply_text("❌ Job not found.")
        return
    if job.get("status") != "uncertain":
        await update.message.reply_text(
            f"❌ Job is not uncertain.\n\nCurrent status: {job.get('status')}"
        )
        return

    uncertain = job.get("uncertain_batch")
    if not uncertain:
        await update.message.reply_text("❌ No uncertain batch information found.")
        return

    first_seq = int(uncertain["first_seq"])
    last_seq = int(uncertain["last_seq"])

    if confirmed_last_seq not in (first_seq - 1, last_seq):
        await update.message.reply_text(
            "❌ For safety, choose only:\n\n"
            f"{first_seq - 1} = batch NOT sent\n"
            f"{last_seq} = batch definitely sent"
        )
        return

    await jobs_col.update_one(
        {"_id": job_id, "status": "uncertain"},
        {"$set": {
            "status": "queued",
            "last_seq": confirmed_last_seq,
            "worker_id": None,
            "current_batch": None,
            "batch_first_seq": None,
            "batch_last_seq": None,
            "batch_count": 0,
            "uncertain_batch": None,
            "uncertain_reason": None,
            "updated_at": utc_now(),
        }},
    )

    if confirmed_last_seq == first_seq - 1:
        msg = (
            "▶️ Recovery complete.\n\n"
            f"Job: {job_id}\nWill resend: {first_seq}-{last_seq}"
        )
    else:
        msg = (
            "▶️ Recovery complete.\n\n"
            f"Job: {job_id}\nConfirmed sent: {first_seq}-{last_seq}\n"
            f"Next: {last_seq + 1}"
        )
    await update.message.reply_text(msg)


# ============================================================
# DELETE SINGLE VIDEO - ORIGINAL
# ============================================================

async def cmd_delete(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage:\n/delete VIDEO_ID")
        return

    try:
        seq = int(context.args[0])
    except ValueError:
        await update.message.reply_text("❌ Invalid video ID.")
        return

    result = await videos_col.delete_one({"seq": seq})
    if result.deleted_count:
        await update.message.reply_text(f"🗑 Video #{seq} deleted.")
    else:
        await update.message.reply_text(f"❌ Video #{seq} not found.")


# ============================================================
# HELP - ORIGINAL PLUS SCAN
# ============================================================

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🎬 VIDEO STORAGE BOT\n\n"
        "Commands:\n\n"
        "/stats - Statistics\n"
        "/list [limit] - List videos\n"
        "/search TEXT - Search\n"
        "/send ID CHAT_ID - Send one video\n"
        "/sendall CHAT_ID - Send all videos\n"
        "/job - Current job\n"
        "/pause - Pause sendall job\n"
        "/resume - Resume sendall job\n"
        "/cancel [JOB_ID] - Cancel sendall job\n"
        "/recover JOB_ID SEQ - Recover uncertain sendall job\n"
        "/delete ID - Delete one video\n"
        "/scan - Scan videos from your Telegram groups and channels\n\n"
        "Videos sent to this bot as Telegram videos are automatically saved."
    )


# ============================================================
# ERROR HANDLER
# ============================================================

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    logger.error("Telegram error: %s", context.error)


# ============================================================
# APPLICATION
# ============================================================

def build_application():
    global application

    request = HTTPXRequest(
        connect_timeout=30,
        read_timeout=180,
        write_timeout=180,
        pool_timeout=30,
    )

    application = (
        Application.builder()
        .token(BOT_TOKEN)
        .request(request)
        .build()
    )

    application.add_handler(CommandHandler("start", cmd_start))
    application.add_handler(CommandHandler("stats", cmd_stats))
    application.add_handler(CommandHandler("list", cmd_list))
    application.add_handler(CommandHandler("search", cmd_search))
    application.add_handler(CommandHandler("send", cmd_send))
    application.add_handler(CommandHandler("sendall", cmd_sendall))
    application.add_handler(CommandHandler("job", cmd_job))
    application.add_handler(CommandHandler("pause", cmd_pause))
    application.add_handler(CommandHandler("resume", cmd_resume))
    application.add_handler(CommandHandler("cancel", cmd_cancel))
    application.add_handler(CommandHandler("recover", cmd_recover))
    application.add_handler(CommandHandler("delete", cmd_delete))

    application.add_handler(CommandHandler("scan", cmd_scan))
    application.add_handler(
        CallbackQueryHandler(scan_callback, pattern=r"^scan(?:page|start):")
    )

    # Original automatic video storage handler remains.
    application.add_handler(MessageHandler(filters.VIDEO, video_handler))

    application.add_error_handler(error_handler)
    return application


# ============================================================
# MAIN
# ============================================================

async def main():
    global heartbeat_task, worker_task
    global telethon_client, telethon_user_id, telethon_bot_username

    logger.info("Starting Video Storage Bot...")

    await mongo.admin.command("ping")
    logger.info("MongoDB connection OK.")

    await ensure_indexes()

    acquired = await acquire_instance_lock()
    if not acquired:
        logger.error(
            "Another bot instance is already running. Stopping this instance."
        )
        mongo.close()
        return

    logger.info("Instance lock acquired: %s", INSTANCE_ID)

    await mark_stale_jobs_uncertain()

    build_application()

    heartbeat_task = asyncio.create_task(heartbeat_loop())
    worker_task = asyncio.create_task(queue_worker())

    try:
        await application.initialize()

        # Connect Personal Telegram Account only when scan variables exist.
        if SCAN_ENABLED:
            telethon_client = TelegramClient(
                StringSession(STRING_SESSION),
                int(API_ID),
                API_HASH,
            )
            await telethon_client.start()

            me = await telethon_client.get_me()
            telethon_user_id = int(me.id)

            bot_info = await application.bot.get_me()
            telethon_bot_username = bot_info.username

            if not telethon_bot_username:
                raise RuntimeError("Could not determine bot username.")

            logger.info(
                "Personal Telegram session connected. User ID: %s",
                telethon_user_id,
            )
            logger.info("Scan feature enabled.")
        else:
            logger.warning(
                "Scan feature disabled: API_ID/API_HASH/STRING_SESSION missing."
            )

        await application.bot.delete_webhook(drop_pending_updates=False)
        await application.start()
        await application.updater.start_polling(
            allowed_updates=Update.ALL_TYPES,
            drop_pending_updates=False,
        )

        logger.info("Bot polling started successfully.")
        await asyncio.Event().wait()

    except asyncio.CancelledError:
        logger.info("Main task cancelled.")
    except Exception:
        logger.exception("Fatal application error")
    finally:
        logger.info("Stopping bot...")

        if heartbeat_task:
            heartbeat_task.cancel()
            try:
                await heartbeat_task
            except asyncio.CancelledError:
                pass

        if worker_task:
            worker_task.cancel()
            try:
                await worker_task
            except asyncio.CancelledError:
                pass

        if active_scan_task and not active_scan_task.done():
            active_scan_task.cancel()
            try:
                await active_scan_task
            except asyncio.CancelledError:
                pass
            except Exception:
                logger.exception("Error while stopping scan task")

        if telethon_client:
            try:
                await telethon_client.disconnect()
            except Exception:
                logger.exception("Telethon disconnect error")

        try:
            if application and application.updater:
                await application.updater.stop()
        except Exception:
            logger.exception("Updater stop error")

        try:
            if application:
                await application.stop()
        except Exception:
            logger.exception("Application stop error")

        try:
            if application:
                await application.shutdown()
        except Exception:
            logger.exception("Application shutdown error")

        await release_instance_lock()
        mongo.close()
        logger.info("Bot stopped.")


# ============================================================
# ENTRY
# ============================================================

if __name__ == "__main__":
    asyncio.run(main())
