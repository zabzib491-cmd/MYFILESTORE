import os
import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional

from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import ASCENDING, DESCENDING, ReturnDocument
from pymongo.errors import DuplicateKeyError

from telegram import Update, InputMediaVideo
from telegram.constants import ChatType
from telegram.error import (
    TelegramError,
    RetryAfter,
    TimedOut,
    NetworkError,
)
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)


# ============================================================
# CONFIG
# ============================================================

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
MONGO_URI = os.getenv("MONGO_URI", "")

ADMIN_IDS = {
    int(x.strip())
    for x in os.getenv("ADMIN_IDS", "").split(",")
    if x.strip().isdigit()
}

SEND_DELAY = float(os.getenv("SEND_DELAY", "5"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "10"))

# Telegram maximum media group size
ALBUM_SIZE = 10


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)

logger = logging.getLogger("VideoStorageBot")


# ============================================================
# DATABASE
# ============================================================

mongo_client: Optional[AsyncIOMotorClient] = None

db = None
videos = None
jobs = None
settings = None


# ============================================================
# GLOBAL WORKER
# ============================================================

send_worker_task = None


# ============================================================
# DATABASE INITIALIZATION
# ============================================================

async def init_db():

    global mongo_client
    global db
    global videos
    global jobs
    global settings

    mongo_client = AsyncIOMotorClient(
        MONGO_URI,
        serverSelectionTimeoutMS=10000,
    )

    db = mongo_client["video_storage_bot"]

    videos = db["videos"]
    jobs = db["jobs"]
    settings = db["settings"]

    # --------------------------------------------------------
    # INDEXES
    # --------------------------------------------------------

    await videos.create_index(
        [("file_unique_id", ASCENDING)],
        unique=True,
        name="unique_file",
    )

    await videos.create_index(
        [("seq", DESCENDING)],
        name="seq_desc",
    )

    await videos.create_index(
        [("caption", ASCENDING)],
        name="caption_index",
    )

    await videos.create_index(
        [("created_at", DESCENDING)],
        name="created_index",
    )

    await videos.create_index(
        [("source_chat_id", ASCENDING)],
        name="source_chat",
    )

    await jobs.create_index(
        [("status", ASCENDING)],
        name="job_status",
    )

    await jobs.create_index(
        [("created_at", DESCENDING)],
        name="job_created",
    )

    await db.command("ping")

    logger.info("MongoDB connected successfully")


# ============================================================
# HELPERS
# ============================================================

def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


async def admin_only(update: Update) -> bool:

    user = update.effective_user

    if not user:
        return False

    return is_admin(user.id)


def now():
    return datetime.now(timezone.utc)


# ============================================================
# VIDEO EXTRACTION
# ============================================================

def get_video_from_message(message):

    if not message:
        return None

    if not message.video:
        return None

    video = message.video

    return {
        "file_id": video.file_id,
        "file_unique_id": video.file_unique_id,
        "width": video.width,
        "height": video.height,
        "duration": video.duration,
        "file_size": getattr(video, "file_size", None),
        "caption": message.caption or "",
    }


# ============================================================
# SAVE VIDEO
# ============================================================

async def save_video_message(message):

    data = get_video_from_message(message)

    if not data:
        return False, None

    file_unique_id = data["file_unique_id"]

    # --------------------------------------------------------
    # DUPLICATE CHECK
    # --------------------------------------------------------

    existing = await videos.find_one(
        {
            "file_unique_id": file_unique_id
        },
        {
            "_id": 1,
            "seq": 1,
        },
    )

    if existing:

        return False, existing.get("seq")

    # --------------------------------------------------------
    # ATOMIC SEQUENCE
    # --------------------------------------------------------

    counter = await settings.find_one_and_update(
        {
            "_id": "video_counter"
        },
        {
            "$inc": {
                "value": 1
            }
        },
        upsert=True,
        return_document=ReturnDocument.AFTER,
    )

    seq = counter["value"]

    # --------------------------------------------------------
    # SOURCE
    # --------------------------------------------------------

    chat = message.chat

    source_chat_id = chat.id if chat else None
    source_message_id = message.message_id

    source_title = ""

    if chat:

        source_title = (
            chat.title
            or chat.username
            or (
                message.from_user.username
                if message.from_user
                else ""
            )
            or ""
        )

    # --------------------------------------------------------
    # DOCUMENT
    # --------------------------------------------------------

    document = {

        "seq": seq,

        "file_id": data["file_id"],
        "file_unique_id": data["file_unique_id"],

        "width": data["width"],
        "height": data["height"],
        "duration": data["duration"],
        "file_size": data["file_size"],

        "caption": data["caption"],

        "source_chat_id": source_chat_id,
        "source_message_id": source_message_id,
        "source_title": source_title,

        "created_at": now(),
    }

    try:

        await videos.insert_one(document)

        logger.info(
            "VIDEO SAVED | ID=%s | SOURCE=%s",
            seq,
            source_title,
        )

        return True, seq

    except DuplicateKeyError:

        existing = await videos.find_one(
            {
                "file_unique_id": file_unique_id
            },
            {
                "seq": 1
            },
        )

        return (
            False,
            existing.get("seq")
            if existing
            else None,
        )


# ============================================================
# VIDEO HANDLER
# ============================================================

async def video_handler(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    message = update.effective_message

    if not message:
        return

    if not message.video:
        return

    try:

        saved, seq = await save_video_message(message)

        if (
            saved
            and message.chat.type == ChatType.PRIVATE
        ):

            await message.reply_text(
                f"✅ Video saved\n\n"
                f"🆔 ID: `{seq}`",
                parse_mode="Markdown",
            )

    except Exception:

        logger.exception(
            "Error while saving video"
        )


# ============================================================
# /START
# ============================================================

async def start_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    if not update.effective_user:
        return

    if not is_admin(update.effective_user.id):

        await update.effective_message.reply_text(
            "🎥 Send a video to store it."
        )

        return

    await update.effective_message.reply_text(
        "🎥 VIDEO STORAGE BOT\n\n"

        "Videos are saved automatically.\n\n"

        "Admin commands:\n"
        "/stats\n"
        "/list [limit]\n"
        "/search <text>\n"
        "/send <ID> <CHAT_ID>\n"
        "/sendall <CHAT_ID>\n"
        "/delete <ID>\n"
        "/pause\n"
        "/resume\n"
        "/cancel\n"
        "/job"
    )


# ============================================================
# /STATS
# ============================================================

async def stats_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    if not await admin_only(update):
        return

    total = await videos.count_documents({})

    pending = await jobs.count_documents(
        {
            "status": {
                "$in": [
                    "queued",
                    "running",
                    "paused",
                ]
            }
        }
    )

    completed = await jobs.count_documents(
        {
            "status": "completed"
        }
    )

    failed = await jobs.count_documents(
        {
            "status": "failed"
        }
    )

    await update.effective_message.reply_text(
        "📊 VIDEO STORAGE\n\n"
        f"🎥 Total Videos: {total:,}\n"
        f"⏳ Active/Pending Jobs: {pending:,}\n"
        f"✅ Completed Jobs: {completed:,}\n"
        f"❌ Failed Jobs: {failed:,}"
    )


# ============================================================
# /LIST
# ============================================================

async def list_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    if not await admin_only(update):
        return

    limit = 20

    if context.args:

        try:

            limit = int(context.args[0])

            limit = min(
                max(limit, 1),
                100,
            )

        except ValueError:
            pass

    cursor = (
        videos.find(
            {},
            {
                "seq": 1,
                "duration": 1,
                "source_title": 1,
                "created_at": 1,
            },
        )
        .sort(
            "seq",
            DESCENDING,
        )
        .limit(limit)
    )

    items = await cursor.to_list(
        length=limit
    )

    if not items:

        await update.effective_message.reply_text(
            "📦 Storage is empty."
        )

        return

    lines = [
        "📦 LATEST VIDEOS\n"
    ]

    for item in items:

        source = (
            item.get("source_title")
            or "Unknown"
        )

        lines.append(
            f"🎥 ID: {item['seq']}\n"
            f"📍 {source}\n"
        )

    await update.effective_message.reply_text(
        "\n".join(lines)
    )


# ============================================================
# /SEARCH
# ============================================================

async def search_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    if not await admin_only(update):
        return

    if not context.args:

        await update.effective_message.reply_text(
            "Usage:\n"
            "/search keyword"
        )

        return

    keyword = " ".join(
        context.args
    ).strip()

    regex = {
        "$regex": keyword,
        "$options": "i",
    }

    cursor = (
        videos.find(
            {
                "$or": [
                    {
                        "caption": regex
                    },
                    {
                        "source_title": regex
                    },
                ]
            },
            {
                "seq": 1,
                "caption": 1,
                "source_title": 1,
            },
        )
        .sort(
            "seq",
            DESCENDING,
        )
        .limit(50)
    )

    results = await cursor.to_list(
        length=50
    )

    if not results:

        await update.effective_message.reply_text(
            "❌ No videos found."
        )

        return

    lines = [
        f"🔎 SEARCH: {keyword}\n"
    ]

    for item in results:

        caption = (
            item.get("caption")
            or ""
        )

        source = (
            item.get("source_title")
            or "Unknown"
        )

        caption = caption[:100]

        lines.append(
            f"🎥 ID: {item['seq']}\n"
            f"📍 {source}\n"
            f"📝 {caption}\n"
        )

    await update.effective_message.reply_text(
        "\n".join(lines)
    )


# ============================================================
# SEND SINGLE VIDEO
# ============================================================

async def send_video(
    bot,
    video_doc,
    chat_id,
):

    media = InputMediaVideo(
        media=video_doc["file_id"],
        caption=(
            video_doc.get("caption")
            or None
        ),
    )

    await bot.send_media_group(
        chat_id=chat_id,
        media=[media],
    )


# ============================================================
# /SEND
# ============================================================

async def send_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    if not await admin_only(update):
        return

    if len(context.args) < 2:

        await update.effective_message.reply_text(
            "Usage:\n"
            "/send VIDEO_ID CHAT_ID\n\n"
            "Example:\n"
            "/send 150 -1001234567890"
        )

        return

    try:

        video_id = int(
            context.args[0]
        )

        chat_id = int(
            context.args[1]
        )

    except ValueError:

        await update.effective_message.reply_text(
            "❌ Invalid ID or Chat ID."
        )

        return

    video = await videos.find_one(
        {
            "seq": video_id
        }
    )

    if not video:

        await update.effective_message.reply_text(
            "❌ Video not found."
        )

        return

    try:

        await send_video(
            context.bot,
            video,
            chat_id,
        )

        await update.effective_message.reply_text(
            f"✅ Video {video_id} sent."
        )

    except RetryAfter as e:

        await update.effective_message.reply_text(
            "⏳ Telegram FloodWait.\n"
            f"Please wait {int(e.retry_after)} seconds."
        )

    except Exception as e:

        logger.exception(
            "Send failed"
        )

        await update.effective_message.reply_text(
            f"❌ Send failed:\n"
            f"{str(e)[:1000]}"
        )


# ============================================================
# CREATE SEND JOB
# ============================================================

async def create_send_job(
    chat_id,
    created_by,
):

    job = {

        "type": "sendall",

        "chat_id": chat_id,

        "status": "queued",

        "created_by": created_by,

        "last_seq": 0,

        "sent": 0,

        "failed": 0,

        "created_at": now(),

        "updated_at": now(),
    }

    result = await jobs.insert_one(job)

    return result.inserted_id


# ============================================================
# /SENDALL
# ============================================================

async def sendall_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    if not await admin_only(update):
        return

    if not context.args:

        await update.effective_message.reply_text(
            "Usage:\n"
            "/sendall CHAT_ID\n\n"
            "Example:\n"
            "/sendall -1001234567890"
        )

        return

    try:

        chat_id = int(
            context.args[0]
        )

    except ValueError:

        await update.effective_message.reply_text(
            "❌ Invalid Chat ID."
        )

        return

    active_job = await jobs.find_one(
        {
            "status": {
                "$in": [
                    "queued",
                    "running",
                    "paused",
                ]
            }
        }
    )

    if active_job:

        await update.effective_message.reply_text(
            "⚠️ A send job is already active.\n\n"
            f"Sent: {active_job.get('sent', 0):,}\n"
            f"Failed: {active_job.get('failed', 0):,}\n"
            f"Last ID: {active_job.get('last_seq', 0):,}"
        )

        return

    total = await videos.count_documents({})

    if total == 0:

        await update.effective_message.reply_text(
            "❌ No videos in storage."
        )

        return

    # --------------------------------------------------------
    # CHECK DESTINATION BEFORE CREATING JOB
    # --------------------------------------------------------

    try:

        chat = await context.bot.get_chat(
            chat_id
        )

        logger.info(
            "SENDALL DESTINATION | id=%s | type=%s | title=%s",
            chat_id,
            chat.type,
            getattr(chat, "title", ""),
        )

    except TelegramError as e:

        logger.exception(
            "Destination check failed"
        )

        await update.effective_message.reply_text(
            "❌ Cannot access destination.\n\n"
            f"{str(e)[:1000]}\n\n"
            "Make sure the bot is a member/admin "
            "of the group or channel."
        )

        return

    job_id = await create_send_job(
        chat_id=chat_id,
        created_by=update.effective_user.id,
    )

    await update.effective_message.reply_text(
        "🚀 SEND ALL STARTED\n\n"
        f"🎥 Total Videos: {total:,}\n"
        f"📤 Destination: `{chat_id}`\n\n"
        f"🆔 Job: `{str(job_id)}`\n\n"
        "⏳ Sending will continue in background...",
        parse_mode="Markdown",
    )

    logger.info(
        "SENDALL JOB CREATED | job=%s | chat=%s | total=%s",
        job_id,
        chat_id,
        total,
    )


# ============================================================
# /PAUSE
# ============================================================

async def pause_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    if not await admin_only(update):
        return

    result = await jobs.update_one(
        {
            "status": {
                "$in": [
                    "queued",
                    "running",
                ]
            }
        },
        {
            "$set": {
                "status": "paused",
                "updated_at": now(),
            }
        },
    )

    if result.modified_count:

        await update.effective_message.reply_text(
            "⏸️ Send job paused."
        )

    else:

        await update.effective_message.reply_text(
            "ℹ️ No active send job."
        )


# ============================================================
# /RESUME
# ============================================================

async def resume_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    if not await admin_only(update):
        return

    result = await jobs.update_one(
        {
            "status": "paused"
        },
        {
            "$set": {
                "status": "queued",
                "updated_at": now(),
            }
        },
    )

    if result.modified_count:

        await update.effective_message.reply_text(
            "▶️ Send job resumed."
        )

    else:

        await update.effective_message.reply_text(
            "ℹ️ No paused job."
        )


# ============================================================
# /CANCEL
# ============================================================

async def cancel_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    if not await admin_only(update):
        return

    result = await jobs.update_one(
        {
            "status": {
                "$in": [
                    "queued",
                    "running",
                    "paused",
                ]
            }
        },
        {
            "$set": {
                "status": "cancelled",
                "updated_at": now(),
            }
        },
    )

    if result.modified_count:

        await update.effective_message.reply_text(
            "❌ Send job cancelled."
        )

    else:

        await update.effective_message.reply_text(
            "ℹ️ No active job."
        )


# ============================================================
# /DELETE
# ============================================================

async def delete_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    if not await admin_only(update):
        return

    if not context.args:

        await update.effective_message.reply_text(
            "Usage:\n"
            "/delete VIDEO_ID"
        )

        return

    try:

        video_id = int(
            context.args[0]
        )

    except ValueError:

        await update.effective_message.reply_text(
            "❌ Invalid ID."
        )

        return

    result = await videos.delete_one(
        {
            "seq": video_id
        }
    )

    if result.deleted_count:

        await update.effective_message.reply_text(
            f"🗑️ Video {video_id} deleted."
        )

    else:

        await update.effective_message.reply_text(
            "❌ Video not found."
        )


# ============================================================
# /JOB
# ============================================================

async def job_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    if not await admin_only(update):
        return

    job = await jobs.find_one(
        {},
        sort=[
            (
                "created_at",
                DESCENDING,
            )
        ],
    )

    if not job:

        await update.effective_message.reply_text(
            "No jobs found."
        )

        return

    total = await videos.count_documents({})

    sent = job.get(
        "sent",
        0,
    )

    failed = job.get(
        "failed",
        0,
    )

    remaining = max(
        total - sent - failed,
        0,
    )

    await update.effective_message.reply_text(
        "📤 SEND STATUS\n\n"
        f"Status: {job.get('status')}\n"
        f"🎥 Total: {total:,}\n"
        f"✅ Sent: {sent:,}\n"
        f"❌ Failed: {failed:,}\n"
        f"⏳ Remaining: {remaining:,}\n"
        f"🔢 Last ID: {job.get('last_seq', 0):,}\n"
        f"📍 Chat: {job.get('chat_id')}"
    )


# ============================================================
# FLOODWAIT / RETRY
# ============================================================

async def send_album_with_retry(
    bot,
    chat_id,
    media,
):

    attempts = 0

    while attempts < MAX_RETRIES:

        try:

            return await bot.send_media_group(
                chat_id=chat_id,
                media=media,
            )

        except RetryAfter as e:

            wait_time = int(
                e.retry_after
            ) + 2

            logger.warning(
                "FloodWait | %s seconds",
                wait_time,
            )

            await asyncio.sleep(
                wait_time
            )

        except (
            TimedOut,
            NetworkError,
        ):

            attempts += 1

            wait_time = min(
                5 * attempts,
                60,
            )

            logger.warning(
                "Network error | retry %s/%s | wait=%ss",
                attempts,
                MAX_RETRIES,
                wait_time,
            )

            await asyncio.sleep(
                wait_time
            )

        except TelegramError:

            raise

    raise RuntimeError(
        "Maximum retries exceeded."
    )


# ============================================================
# SEND QUEUE WORKER
# ============================================================

async def send_queue_worker(
    application,
):

    logger.info(
        "SEND QUEUE WORKER STARTED"
    )

    while True:

        try:

            job = await jobs.find_one_and_update(
                {
                    "status": "queued"
                },
                {
                    "$set": {
                        "status": "running",
                        "updated_at": now(),
                    }
                },
                sort=[
                    (
                        "created_at",
                        ASCENDING,
                    )
                ],
                return_document=ReturnDocument.AFTER,
            )

            if not job:

                await asyncio.sleep(2)

                continue

            logger.info(
                "SEND QUEUE PICKED JOB | job=%s | chat=%s",
                job["_id"],
                job["chat_id"],
            )

            await process_send_job(
                application,
                job,
            )

        except asyncio.CancelledError:

            logger.info(
                "SEND QUEUE WORKER STOPPED"
            )

            raise

        except Exception:

            logger.exception(
                "SEND QUEUE WORKER ERROR"
            )

            await asyncio.sleep(5)


# ============================================================
# PROCESS SEND JOB — FIXED
# ============================================================

async def process_send_job(
    application,
    job,
):

    job_id = job["_id"]
    chat_id = job["chat_id"]

    logger.info(
        "PROCESSING SEND JOB | job=%s | chat=%s",
        job_id,
        chat_id,
    )

    # --------------------------------------------------------
    # DESTINATION TEST
    # --------------------------------------------------------

    try:

        chat = await application.bot.get_chat(
            chat_id
        )

        logger.info(
            "DESTINATION OK | id=%s | type=%s | title=%s",
            chat_id,
            chat.type,
            getattr(chat, "title", ""),
        )

    except Exception as e:

        logger.exception(
            "DESTINATION ERROR | chat=%s",
            chat_id,
        )

        await jobs.update_one(
            {
                "_id": job_id
            },
            {
                "$set": {
                    "status": "failed",
                    "error": str(e)[:2000],
                    "updated_at": now(),
                }
            },
        )

        return

    # --------------------------------------------------------
    # MAIN LOOP
    # --------------------------------------------------------

    while True:

        # ====================================================
        # IMPORTANT FIX
        #
        # ALWAYS READ last_seq FROM MONGODB.
        #
        # The database is the single source of truth.
        # ====================================================

        current_job = await jobs.find_one(
            {
                "_id": job_id
            }
        )

        if not current_job:

            logger.error(
                "Job disappeared | %s",
                job_id,
            )

            return

        status = current_job.get(
            "status"
        )

        # ----------------------------------------------------
        # CANCEL
        # ----------------------------------------------------

        if status == "cancelled":

            logger.info(
                "Job cancelled | %s",
                job_id,
            )

            return

        # ----------------------------------------------------
        # PAUSE
        # ----------------------------------------------------

        if status == "paused":

            await asyncio.sleep(3)

            continue

        # ----------------------------------------------------
        # FAILED
        # ----------------------------------------------------

        if status == "failed":

            return

        # ----------------------------------------------------
        # READ CURRENT LAST SEQ
        # ----------------------------------------------------

        last_seq = int(
            current_job.get(
                "last_seq",
                0,
            )
        )

        logger.info(
            "GET NEXT BATCH | job=%s | last_seq=%s",
            job_id,
            last_seq,
        )

        # ----------------------------------------------------
        # GET NEXT 10 VIDEOS
        # ----------------------------------------------------

        cursor = (
            videos.find(
                {
                    "seq": {
                        "$gt": last_seq
                    }
                }
            )
            .sort(
                "seq",
                ASCENDING,
            )
            .limit(ALBUM_SIZE)
        )

        batch = await cursor.to_list(
            length=ALBUM_SIZE
        )

        # ----------------------------------------------------
        # COMPLETED
        # ----------------------------------------------------

        if not batch:

            await jobs.update_one(
                {
                    "_id": job_id,
                    "status": "running",
                },
                {
                    "$set": {
                        "status": "completed",
                        "updated_at": now(),
                    }
                },
            )

            logger.info(
                "SEND JOB COMPLETED | job=%s | sent=%s | failed=%s",
                job_id,
                current_job.get("sent", 0),
                current_job.get("failed", 0),
            )

            return

        # ----------------------------------------------------
        # SAFETY CHECK
        # ----------------------------------------------------

        first_id = batch[0]["seq"]
        last_id = batch[-1]["seq"]

        if first_id <= last_seq:

            logger.error(
                "PROGRESS ERROR | job=%s | "
                "last_seq=%s | first_id=%s | last_id=%s",
                job_id,
                last_seq,
                first_id,
                last_id,
            )

            await jobs.update_one(
                {
                    "_id": job_id
                },
                {
                    "$set": {
                        "status": "failed",
                        "last_error": (
                            "SendAll progress safety check failed."
                        ),
                        "updated_at": now(),
                    }
                },
            )

            return

        # ----------------------------------------------------
        # LOG EXACT VIDEOS BEING SENT
        # ----------------------------------------------------

        logger.info(
            "BATCH READY | job=%s | IDs=%s-%s",
            job_id,
            first_id,
            last_id,
        )

        # ----------------------------------------------------
        # CREATE ALBUM
        # ----------------------------------------------------

        media = []

        for item in batch:

            caption = (
                item.get("caption")
                or None
            )

            media.append(
                InputMediaVideo(
                    media=item["file_id"],
                    caption=caption,
                )
            )

        logger.info(
            "SENDING ALBUM | job=%s | IDs=%s-%s | count=%s",
            job_id,
            first_id,
            last_id,
            len(batch),
        )

        # ----------------------------------------------------
        # SEND
        # ----------------------------------------------------

        try:

            result = await send_album_with_retry(
                application.bot,
                chat_id,
                media,
            )

            sent_count = len(batch)

            # =================================================
            # ATOMIC PROGRESS UPDATE
            #
            # This is the main duplicate-prevention fix.
            #
            # Update ONLY if MongoDB still has the same
            # last_seq that we used to create this batch.
            # =================================================

            update_result = await jobs.update_one(
                {
                    "_id": job_id,
                    "status": "running",
                    "last_seq": last_seq,
                },
                {
                    "$set": {
                        "last_seq": last_id,
                        "updated_at": now(),
                    },
                    "$inc": {
                        "sent": sent_count,
                    },
                },
            )

            # -------------------------------------------------
            # VERIFY PROGRESS
            # -------------------------------------------------

            if update_result.modified_count != 1:

                logger.error(
                    "PROGRESS UPDATE FAILED AFTER SEND | "
                    "job=%s | batch=%s-%s",
                    job_id,
                    first_id,
                    last_id,
                )

                # IMPORTANT:
                # Do NOT send this batch again.
                # Stop to prevent duplicate sending.

                await jobs.update_one(
                    {
                        "_id": job_id
                    },
                    {
                        "$set": {
                            "status": "failed",
                            "last_error": (
                                "MongoDB progress update failed "
                                "after successful send. "
                                "Job stopped to prevent duplicates."
                            ),
                            "updated_at": now(),
                        }
                    },
                )

                return

            logger.info(
                "ALBUM SENT SUCCESSFULLY | "
                "job=%s | IDs=%s-%s | count=%s | "
                "NEW_LAST_SEQ=%s | telegram_messages=%s",
                job_id,
                first_id,
                last_id,
                sent_count,
                last_id,
                len(result) if result else 0,
            )

        # ----------------------------------------------------
        # FLOODWAIT
        # ----------------------------------------------------

        except RetryAfter as e:

            wait_time = int(
                e.retry_after
            ) + 2

            logger.warning(
                "FLOODWAIT DURING JOB | "
                "job=%s | wait=%ss",
                job_id,
                wait_time,
            )

            await asyncio.sleep(
                wait_time
            )

            # last_seq is intentionally unchanged.
            # The batch can be attempted again.

            continue

        # ----------------------------------------------------
        # TELEGRAM ERROR
        # ----------------------------------------------------

        except TelegramError as e:

            logger.exception(
                "TELEGRAM ERROR | "
                "job=%s | IDs=%s-%s",
                job_id,
                first_id,
                last_id,
            )

            # Move past a permanent Telegram error.

            await jobs.update_one(
                {
                    "_id": job_id,
                    "status": "running",
                    "last_seq": last_seq,
                },
                {
                    "$set": {
                        "last_seq": last_id,
                        "updated_at": now(),
                        "last_error": str(e)[:2000],
                    },
                    "$inc": {
                        "failed": len(batch),
                    },
                },
            )

        # ----------------------------------------------------
        # UNKNOWN ERROR
        # ----------------------------------------------------

        except Exception as e:

            logger.exception(
                "UNKNOWN SEND ERROR | "
                "job=%s | IDs=%s-%s",
                job_id,
                first_id,
                last_id,
            )

            await jobs.update_one(
                {
                    "_id": job_id,
                    "status": "running",
                    "last_seq": last_seq,
                },
                {
                    "$set": {
                        "last_seq": last_id,
                        "updated_at": now(),
                        "last_error": str(e)[:2000],
                    },
                    "$inc": {
                        "failed": len(batch),
                    },
                },
            )

        # ----------------------------------------------------
        # DELAY
        # ----------------------------------------------------

        await asyncio.sleep(
            max(
                SEND_DELAY,
                1,
            )
        )


# ============================================================
# STARTUP
# ============================================================

async def on_startup(
    application,
):

    global send_worker_task

    await init_db()

    # --------------------------------------------------------
    # RECOVER STALE RUNNING JOB
    #
    # If Render restarts while job is running,
    # continue it instead of losing it.
    # --------------------------------------------------------

    await jobs.update_many(
        {
            "status": "running"
        },
        {
            "$set": {
                "status": "queued",
                "updated_at": now(),
            }
        },
    )

    send_worker_task = asyncio.create_task(
        send_queue_worker(
            application
        )
    )

    logger.info(
        "Video Storage Bot startup complete"
    )


# ============================================================
# SHUTDOWN
# ============================================================

async def on_shutdown(
    application,
):

    global send_worker_task
    global mongo_client

    logger.info(
        "Bot shutdown started"
    )

    # --------------------------------------------------------
    # STOP WORKER
    # --------------------------------------------------------

    if send_worker_task:

        send_worker_task.cancel()

        try:

            await send_worker_task

        except asyncio.CancelledError:

            pass

        send_worker_task = None

    # --------------------------------------------------------
    # MONGO
    # --------------------------------------------------------

    if mongo_client:

        mongo_client.close()

        mongo_client = None

    logger.info(
        "Bot shutdown completed"
    )


# ============================================================
# ERROR HANDLER
# ============================================================

async def error_handler(
    update: object,
    context: ContextTypes.DEFAULT_TYPE,
):

    logger.error(
        "Unhandled exception: %s",
        context.error,
        exc_info=context.error,
    )


# ============================================================
# PYTHON 3.14 SAFE ASYNC RUNNER
# ============================================================

async def async_main():

    logger.info(
        "Starting Video Storage Bot..."
    )

    application = (
        Application.builder()
        .token(BOT_TOKEN)
        .build()
    )

    # --------------------------------------------------------
    # COMMANDS
    # --------------------------------------------------------

    application.add_handler(
        CommandHandler(
            "start",
            start_command,
        )
    )

    application.add_handler(
        CommandHandler(
            "stats",
            stats_command,
        )
    )

    application.add_handler(
        CommandHandler(
            "list",
            list_command,
        )
    )

    application.add_handler(
        CommandHandler(
            "search",
            search_command,
        )
    )

    application.add_handler(
        CommandHandler(
            "send",
            send_command,
        )
    )

    application.add_handler(
        CommandHandler(
            "sendall",
            sendall_command,
        )
    )

    application.add_handler(
        CommandHandler(
            "delete",
            delete_command,
        )
    )

    application.add_handler(
        CommandHandler(
            "pause",
            pause_command,
        )
    )

    application.add_handler(
        CommandHandler(
            "resume",
            resume_command,
        )
    )

    application.add_handler(
        CommandHandler(
            "cancel",
            cancel_command,
        )
    )

    application.add_handler(
        CommandHandler(
            "job",
            job_command,
        )
    )

    # --------------------------------------------------------
    # VIDEO HANDLER
    # --------------------------------------------------------

    application.add_handler(
        MessageHandler(
            filters.VIDEO,
            video_handler,
        )
    )

    # --------------------------------------------------------
    # ERROR HANDLER
    # --------------------------------------------------------

    application.add_error_handler(
        error_handler
    )

    # ========================================================
    # MANUAL PTB STARTUP
    #
    # NO run_polling()
    #
    # Python 3.14 compatible.
    # ========================================================

    await application.initialize()

    await on_startup(
        application
    )

    await application.start()

    await application.updater.start_polling(
        allowed_updates=Update.ALL_TYPES
    )

    logger.info(
        "BOT IS ONLINE"
    )

    logger.info(
        "SENDALL WORKER IS ONLINE"
    )

    # --------------------------------------------------------
    # KEEP PROCESS ALIVE
    # --------------------------------------------------------

    stop_event = asyncio.Event()

    try:

        await stop_event.wait()

    except asyncio.CancelledError:

        logger.info(
            "Main task cancelled"
        )

    finally:

        # ----------------------------------------------------
        # STOP POLLING
        # ----------------------------------------------------

        try:

            if application.updater.running:

                await application.updater.stop()

        except Exception:

            logger.exception(
                "Error stopping updater"
            )

        # ----------------------------------------------------
        # STOP APPLICATION
        # ----------------------------------------------------

        try:

            if application.running:

                await application.stop()

        except Exception:

            logger.exception(
                "Error stopping application"
            )

        # ----------------------------------------------------
        # CUSTOM SHUTDOWN
        # ----------------------------------------------------

        try:

            await on_shutdown(
                application
            )

        except Exception:

            logger.exception(
                "Error during custom shutdown"
            )

        # ----------------------------------------------------
        # PTB SHUTDOWN
        # ----------------------------------------------------

        try:

            await application.shutdown()

        except Exception:

            logger.exception(
                "Error during PTB shutdown"
            )


# ============================================================
# MAIN
# ============================================================

def main():

    # --------------------------------------------------------
    # ENV CHECK
    # --------------------------------------------------------

    if not BOT_TOKEN:

        raise RuntimeError(
            "BOT_TOKEN environment variable missing"
        )

    if not MONGO_URI:

        raise RuntimeError(
            "MONGO_URI environment variable missing"
        )

    if not ADMIN_IDS:

        raise RuntimeError(
            "ADMIN_IDS environment variable missing"
        )

    # --------------------------------------------------------
    # PYTHON 3.14
    # --------------------------------------------------------

    asyncio.run(
        async_main()
    )


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":

    main()
