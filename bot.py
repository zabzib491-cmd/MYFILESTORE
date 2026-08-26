import os
import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional

from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import ASCENDING, DESCENDING, ReturnDocument
from pymongo.errors import DuplicateKeyError

from telegram import (
    Update,
    InputMediaVideo,
)
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

# Example:
# ADMIN_IDS=123456789,987654321
ADMIN_IDS = {
    int(x.strip())
    for x in os.getenv("ADMIN_IDS", "").split(",")
    if x.strip().isdigit()
}

# Delay between album batches
# Conservative default
SEND_DELAY = float(os.getenv("SEND_DELAY", "5"))

# Maximum retries for temporary network errors
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "10"))

# Telegram album size
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

    # --------------------------------------------------------
    # TEST CONNECTION
    # --------------------------------------------------------

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
    """
    Extract video information.

    Only Telegram VIDEO is accepted.
    Photos, documents, animations, text and links
    are ignored.
    """

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
    # SOURCE INFORMATION
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
            else None
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

        saved, seq = await save_video_message(
            message
        )

        # Only reply in private chat.
        # Never spam groups/channels.

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

    if not is_admin(
        update.effective_user.id
    ):

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
            limit = int(
                context.args[0]
            )

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

    result = await jobs.insert_one(
        job
    )

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

    job_id = await create_send_job(
        chat_id=chat_id,
        created_by=(
            update.effective_user.id
        ),
    )

    await update.effective_message.reply_text(
        "🚀 SEND JOB CREATED\n\n"
        f"🎥 Total: {total:,}\n"
        f"📤 Destination: `{chat_id}`\n\n"
        f"🆔 Job: `{str(job_id)}`",
        parse_mode="Markdown",
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
        f"🔢 Last ID: "
        f"{job.get('last_seq', 0):,}\n"
        f"📍 Chat: "
        f"{job.get('chat_id')}"
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

            wait_time = (
                int(e.retry_after)
                + 2
            )

            logger.warning(
                "FloodWait: sleeping %s seconds",
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
                "Network error. "
                "Retry %s/%s in %ss",
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
        "Send queue worker started"
    )

    while True:

        try:

            job = (
                await jobs.find_one_and_update(
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
                    return_document=(
                        ReturnDocument.AFTER
                    ),
                )
            )

            if not job:

                await asyncio.sleep(2)

                continue

            await process_send_job(
                application,
                job,
            )

        except asyncio.CancelledError:

            logger.info(
                "Queue worker stopped"
            )

            break

        except Exception:

            logger.exception(
                "Queue worker error"
            )

            await asyncio.sleep(5)


# ============================================================
# PROCESS SEND JOB
# ============================================================

async def process_send_job(
    application,
    job,
):

    job_id = job["_id"]

    chat_id = job["chat_id"]

    last_seq = job.get(
        "last_seq",
        0,
    )

    logger.info(
        "Processing send job %s",
        job_id,
    )

    while True:

        # ----------------------------------------------------
        # GET CURRENT JOB
        # ----------------------------------------------------

        current_job = await jobs.find_one(
            {
                "_id": job_id
            }
        )

        if not current_job:

            return

        status = current_job.get(
            "status"
        )

        if status == "cancelled":

            logger.info(
                "Job %s cancelled",
                job_id,
            )

            return

        if status == "paused":

            await asyncio.sleep(3)

            continue

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

        if not batch:

            await jobs.update_one(
                {
                    "_id": job_id
                },
                {
                    "$set": {
                        "status": "completed",
                        "updated_at": now(),
                    }
                },
            )

            logger.info(
                "Job %s completed",
                job_id,
            )

            return

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

        # ----------------------------------------------------
        # SEND ALBUM
        # ----------------------------------------------------

        try:

            await send_album_with_retry(
                application.bot,
                chat_id,
                media,
            )

            sent_count = len(batch)

            last_seq = batch[-1]["seq"]

            await jobs.update_one(
                {
                    "_id": job_id
                },
                {
                    "$set": {
                        "last_seq": last_seq,
                        "updated_at": now(),
                    },
                    "$inc": {
                        "sent": sent_count,
                    },
                },
            )

            logger.info(
                "Job %s | sent=%s | last=%s",
                job_id,
                sent_count,
                last_seq,
            )

        except TelegramError as e:

            logger.error(
                "Album failed for job %s: %s",
                job_id,
                e,
            )

            # ------------------------------------------------
            # Move past failed batch
            # ------------------------------------------------

            last_seq = batch[-1]["seq"]

            await jobs.update_one(
                {
                    "_id": job_id
                },
                {
                    "$set": {
                        "last_seq": last_seq,
                        "updated_at": now(),
                    },
                    "$inc": {
                        "failed": len(batch),
                    },
                },
            )

        # ----------------------------------------------------
        # SAFE DELAY
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

    await init_db()

    application.create_task(
        send_queue_worker(
            application
        )
    )

    logger.info(
        "Video Storage Bot started"
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
# PYTHON 3.14 EVENT LOOP FIX
# ============================================================

def run_application(
    application,
):

    """
    Python 3.14 compatibility.

    python-telegram-bot 22.3's run_polling()
    expects a current event loop.
    """

    loop = asyncio.new_event_loop()

    asyncio.set_event_loop(
        loop
    )

    try:

        application.run_polling(
            allowed_updates=Update.ALL_TYPES
        )

    finally:

        try:

            loop.close()

        except Exception:

            pass


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
    # APPLICATION
    # --------------------------------------------------------

    application = (
        Application.builder()
        .token(BOT_TOKEN)
        .post_init(on_startup)
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
    # AUTO VIDEO SAVE
    #
    # Private
    # Group
    # Supergroup
    # Channel
    #
    # Only VIDEO messages are saved.
    # Text / links / photos / documents are ignored.
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

    logger.info(
        "Starting polling..."
    )

    # --------------------------------------------------------
    # PYTHON 3.14 SAFE RUN
    # --------------------------------------------------------

    run_application(
        application
    )


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    main()
