import os
import asyncio
import logging
import socket
import uuid
from datetime import datetime, timezone, timedelta

from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import ASCENDING, DESCENDING, ReturnDocument
from pymongo.errors import DuplicateKeyError

from telegram import Update, InputMediaVideo
from telegram.error import RetryAfter, TimedOut, NetworkError, TelegramError
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    MessageHandler,
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

ALBUM_SIZE = 10

DB_NAME = "video_storage_bot"

INSTANCE_ID = (
    f"{socket.gethostname()}-"
    f"{os.getpid()}-"
    f"{uuid.uuid4().hex[:8]}"
)

LOCK_TTL_SECONDS = 90
LOCK_HEARTBEAT_SECONDS = 30

JOB_LEASE_SECONDS = 120

WORKER_SLEEP = 2


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    level=logging.INFO,
)

logger = logging.getLogger("video_storage_bot")


# ============================================================
# MONGODB
# ============================================================

mongo_client = None
db = None

videos_col = None
jobs_col = None
settings_col = None


# ============================================================
# HELPERS
# ============================================================

def now_utc():
    return datetime.now(timezone.utc)


def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


async def admin_only(update: Update) -> bool:
    user = update.effective_user

    if not user or not is_admin(user.id):
        if update.effective_message:
            await update.effective_message.reply_text(
                "❌ You are not authorized to use this command."
            )
        return False

    return True


# ============================================================
# DATABASE INIT
# ============================================================

async def init_db():
    global mongo_client
    global db
    global videos_col
    global jobs_col
    global settings_col

    if not MONGO_URI:
        raise RuntimeError("MONGO_URI is missing")

    mongo_client = AsyncIOMotorClient(
        MONGO_URI,
        serverSelectionTimeoutMS=10000,
        connectTimeoutMS=10000,
        socketTimeoutMS=30000,
    )

    db = mongo_client[DB_NAME]

    videos_col = db["videos"]
    jobs_col = db["jobs"]
    settings_col = db["settings"]

    await mongo_client.admin.command("ping")

    # Existing indexes are preserved.
    await videos_col.create_index(
        [("file_unique_id", ASCENDING)],
        unique=True,
        name="file_unique_id_unique",
    )

    await videos_col.create_index(
        [("seq", ASCENDING)],
        name="seq_index",
    )

    await videos_col.create_index(
        [("caption", ASCENDING)],
        name="caption_index",
    )

    await videos_col.create_index(
        [("created_at", DESCENDING)],
        name="created_index",
    )

    await videos_col.create_index(
        [("source_chat_id", ASCENDING)],
        name="source_chat_id_index",
    )

    await jobs_col.create_index(
        [("status", ASCENDING), ("created_at", ASCENDING)],
        name="job_status_created",
    )

    await jobs_col.create_index(
        [("worker_id", ASCENDING)],
        name="worker_index",
    )

    await settings_col.create_index(
        [("_id", ASCENDING)],
        unique=True,
        name="settings_id_unique",
    )

    logger.info("MongoDB connected successfully")


# ============================================================
# SINGLE INSTANCE LOCK
# ============================================================

async def acquire_instance_lock() -> bool:
    """
    Only ONE bot process should be allowed to poll Telegram.

    This prevents:
        Conflict: terminated by other getUpdates request
    """

    now = now_utc()
    expires = now + timedelta(seconds=LOCK_TTL_SECONDS)

    try:
        result = await settings_col.find_one_and_update(
            {
                "_id": "bot_instance_lock",
                "$or": [
                    {"expires_at": {"$lte": now}},
                    {"instance_id": INSTANCE_ID},
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

        if result and result.get("instance_id") == INSTANCE_ID:
            logger.info(
                "INSTANCE LOCK ACQUIRED | instance=%s",
                INSTANCE_ID,
            )
            return True

        logger.error("Another bot instance is already active")
        return False

    except DuplicateKeyError:
        logger.error(
            "Another instance won the MongoDB lock race."
        )
        return False

    except Exception:
        logger.exception("Failed to acquire instance lock")
        return False


async def heartbeat_instance_lock():
    while True:
        try:
            await asyncio.sleep(LOCK_HEARTBEAT_SECONDS)

            now = now_utc()
            expires = now + timedelta(seconds=LOCK_TTL_SECONDS)

            result = await settings_col.update_one(
                {
                    "_id": "bot_instance_lock",
                    "instance_id": INSTANCE_ID,
                },
                {
                    "$set": {
                        "heartbeat_at": now,
                        "expires_at": expires,
                    }
                },
            )

            if result.modified_count == 0:
                logger.error(
                    "INSTANCE LOCK LOST | stopping heartbeat"
                )
                return

        except asyncio.CancelledError:
            return

        except Exception:
            logger.exception("Instance heartbeat error")


async def release_instance_lock():
    try:
        await settings_col.update_one(
            {
                "_id": "bot_instance_lock",
                "instance_id": INSTANCE_ID,
            },
            {
                "$set": {
                    "expires_at": now_utc(),
                }
            },
        )

        logger.info("INSTANCE LOCK RELEASED")

    except Exception:
        logger.exception("Failed to release instance lock")


# ============================================================
# VIDEO COUNTER
# ============================================================

async def get_next_sequence() -> int:
    result = await settings_col.find_one_and_update(
        {"_id": "video_counter"},
        {"$inc": {"value": 1}},
        upsert=True,
        return_document=ReturnDocument.AFTER,
    )

    return int(result["value"])


# ============================================================
# SAVE VIDEO
# ============================================================

async def save_video_message(message):
    if not message or not message.video:
        return False

    video = message.video

    file_unique_id = video.file_unique_id

    if not file_unique_id:
        return False

    existing = await videos_col.find_one(
        {"file_unique_id": file_unique_id},
        {"_id": 1},
    )

    if existing:
        logger.info(
            "DUPLICATE VIDEO SKIPPED | unique_id=%s",
            file_unique_id,
        )
        return False

    seq = await get_next_sequence()

    document = {
        "seq": seq,
        "file_id": video.file_id,
        "file_unique_id": file_unique_id,

        "width": video.width,
        "height": video.height,
        "duration": video.duration,
        "file_size": getattr(video, "file_size", None),

        "caption": message.caption or "",

        "source_chat_id": (
            message.chat_id
            if message.chat
            else None
        ),

        "source_message_id": message.message_id,

        "created_at": now_utc(),
    }

    try:
        await videos_col.insert_one(document)

        logger.info(
            "VIDEO SAVED | ID=%s | unique=%s",
            seq,
            file_unique_id,
        )

        return True

    except DuplicateKeyError:
        logger.info(
            "DUPLICATE INSERT PREVENTED | unique_id=%s",
            file_unique_id,
        )
        return False

    except Exception:
        logger.exception("Failed to save video")
        return False


# ============================================================
# /START
# ============================================================

async def start_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    await update.message.reply_text(
        "🎬 Video Storage Bot\n\n"
        "Videos are automatically stored.\n\n"
        "Admin commands:\n"
        "/stats\n"
        "/list\n"
        "/search TEXT\n"
        "/send ID CHAT_ID\n"
        "/sendall CHAT_ID\n"
        "/delete ID\n"
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

    total = await videos_col.count_documents({})

    running = await jobs_col.count_documents(
        {"status": "running"}
    )

    queued = await jobs_col.count_documents(
        {"status": "queued"}
    )

    paused = await jobs_col.count_documents(
        {"status": "paused"}
    )

    uncertain = await jobs_col.count_documents(
        {"status": "uncertain"}
    )

    failed = await jobs_col.count_documents(
        {"status": "failed"}
    )

    await update.message.reply_text(
        "📊 VIDEO STORAGE STATS\n\n"
        f"🎬 Total Videos: {total}\n"
        f"🟡 Queued Jobs: {queued}\n"
        f"🟢 Running Jobs: {running}\n"
        f"⏸ Paused Jobs: {paused}\n"
        f"⚠️ Uncertain Jobs: {uncertain}\n"
        f"🔴 Failed Jobs: {failed}"
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
            limit = max(1, min(int(context.args[0]), 100))
        except ValueError:
            pass

    cursor = (
        videos_col
        .find({})
        .sort("seq", ASCENDING)
        .limit(limit)
    )

    lines = ["🎬 VIDEO LIST\n"]

    async for doc in cursor:
        seq = doc.get("seq", "?")
        duration = doc.get("duration") or 0

        lines.append(
            f"ID: {seq} | Duration: {duration}s"
        )

    if len(lines) == 1:
        lines.append("No videos found.")

    await update.message.reply_text(
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
        await update.message.reply_text(
            "Usage:\n/search TEXT"
        )
        return

    text = " ".join(context.args)

    cursor = (
        videos_col
        .find(
            {
                "caption": {
                    "$regex": text,
                    "$options": "i",
                }
            }
        )
        .sort("seq", ASCENDING)
        .limit(50)
    )

    lines = [
        f"🔎 SEARCH: {text}\n"
    ]

    found = False

    async for doc in cursor:
        found = True

        lines.append(
            f"ID: {doc.get('seq')}\n"
            f"Caption: {doc.get('caption', '')[:100]}\n"
        )

    if not found:
        lines.append("No results.")

    await update.message.reply_text(
        "\n".join(lines)
    )


# ============================================================
# CHECK ACTIVE SENDALL
# ============================================================

async def has_active_sendall_job() -> bool:
    job = await jobs_col.find_one(
        {
            "type": "sendall",
            "status": {
                "$in": [
                    "queued",
                    "running",
                    "paused",
                    "uncertain",
                ]
            },
        },
        {"_id": 1, "status": 1},
    )

    return job is not None


# ============================================================
# CREATE SENDALL JOB
# ============================================================

async def create_sendall_job(chat_id: int):
    total = await videos_col.count_documents({})

    job_id = uuid.uuid4().hex[:16]

    job = {
        "_id": job_id,

        "type": "sendall",

        "chat_id": chat_id,

        "status": "queued",

        "total": total,

        "last_seq": 0,

        "sent": 0,

        "failed": 0,

        "worker_id": None,

        "lock_until": None,

        "batch_first_seq": None,
        "batch_last_seq": None,

        "uncertain_first_seq": None,
        "uncertain_last_seq": None,

        "error": None,

        "created_at": now_utc(),
        "updated_at": now_utc(),
    }

    await jobs_col.insert_one(job)

    logger.info(
        "SENDALL JOB CREATED | job=%s | chat=%s | total=%s",
        job_id,
        chat_id,
        total,
    )

    return job_id


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
        await update.message.reply_text(
            "Usage:\n/sendall CHAT_ID\n\n"
            "Example:\n/sendall -1001234567890"
        )
        return

    try:
        chat_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text(
            "❌ Invalid CHAT_ID"
        )
        return

    if await has_active_sendall_job():
        await update.message.reply_text(
            "⚠️ Another SENDALL job already exists.\n\n"
            "Use /job to check it."
        )
        return

    total = await videos_col.count_documents({})

    if total == 0:
        await update.message.reply_text(
            "❌ No videos stored."
        )
        return

    job_id = await create_sendall_job(chat_id)

    await update.message.reply_text(
        "🚀 SENDALL JOB CREATED\n\n"
        f"🆔 Job: {job_id}\n"
        f"🎬 Total: {total}\n"
        f"📤 Destination: {chat_id}\n\n"
        "Worker will start automatically."
    )


# ============================================================
# CLAIM JOB
# ============================================================

async def claim_job():
    now = now_utc()
    lease_until = now + timedelta(
        seconds=JOB_LEASE_SECONDS
    )

    result = await jobs_col.find_one_and_update(
        {
            "status": "queued",
        },
        {
            "$set": {
                "status": "running",
                "worker_id": INSTANCE_ID,
                "lock_until": lease_until,
                "updated_at": now,
            }
        },
        sort=[("created_at", ASCENDING)],
        return_document=ReturnDocument.AFTER,
    )

    return result


# ============================================================
# GET NEXT BATCH
# ============================================================

async def get_next_batch(last_seq: int):
    cursor = (
        videos_col
        .find(
            {
                "seq": {
                    "$gt": last_seq
                }
            }
        )
        .sort("seq", ASCENDING)
        .limit(ALBUM_SIZE)
    )

    docs = []

    async for doc in cursor:
        docs.append(doc)

    return docs


# ============================================================
# CLAIM EXACT BATCH
# ============================================================

async def claim_batch(
    job_id: str,
    last_seq: int,
    first_seq: int,
    last_batch_seq: int,
):
    now = now_utc()
    lease_until = now + timedelta(
        seconds=JOB_LEASE_SECONDS
    )

    result = await jobs_col.find_one_and_update(
        {
            "_id": job_id,
            "status": "running",
            "worker_id": INSTANCE_ID,
            "last_seq": last_seq,

            "$or": [
                {"batch_first_seq": None},
                {
                    "batch_first_seq": {
                        "$exists": False
                    }
                },
            ],
        },
        {
            "$set": {
                "batch_first_seq": first_seq,
                "batch_last_seq": last_batch_seq,

                "lock_until": lease_until,

                "updated_at": now,
            }
        },
        return_document=ReturnDocument.AFTER,
    )

    return result


# ============================================================
# SEND SINGLE VIDEO
# ============================================================

async def send_single_video(
    bot,
    chat_id: int,
    video_doc: dict,
):
    await bot.send_video(
        chat_id=chat_id,
        video=video_doc["file_id"],
        caption=video_doc.get("caption") or None,
    )


# ============================================================
# SEND ALBUM - IMPORTANT
# ============================================================

async def send_album_once(
    bot,
    chat_id: int,
    docs: list,
):
    """
    IMPORTANT:

    RetryAfter:
        SAFE to retry because Telegram explicitly asks us to wait.

    NetworkError / TimedOut:
        DO NOT automatically retry.

    Reason:
        Telegram may have accepted sendMediaGroup but response
        may have been lost. Retrying can create duplicate albums.
    """

    media = []

    for doc in docs:
        media.append(
            InputMediaVideo(
                media=doc["file_id"],
                caption=doc.get("caption") or None,
            )
        )

    retry_count = 0

    while True:
        try:
            logger.info(
                "SENDING ALBUM | IDs=%s-%s",
                docs[0]["seq"],
                docs[-1]["seq"],
            )

            result = await bot.send_media_group(
                chat_id=chat_id,
                media=media,
            )

            logger.info(
                "ALBUM SEND SUCCESS | IDs=%s-%s",
                docs[0]["seq"],
                docs[-1]["seq"],
            )

            return result

        except RetryAfter as e:
            retry_count += 1

            if retry_count > MAX_RETRIES:
                raise

            wait_time = float(e.retry_after) + 1

            logger.warning(
                "RetryAfter | retry=%s/%s | wait=%s",
                retry_count,
                MAX_RETRIES,
                wait_time,
            )

            await asyncio.sleep(wait_time)

        except (TimedOut, NetworkError):
            logger.error(
                "AMBIGUOUS NETWORK RESULT | IDs=%s-%s",
                docs[0]["seq"],
                docs[-1]["seq"],
            )

            raise

        except TelegramError:
            logger.exception(
                "Telegram error while sending album"
            )
            raise


# ============================================================
# MARK JOB UNCERTAIN
# ============================================================

async def mark_job_uncertain(
    job_id: str,
    first_seq: int,
    last_seq: int,
    error_text: str,
):
    await jobs_col.update_one(
        {
            "_id": job_id,
            "worker_id": INSTANCE_ID,
        },
        {
            "$set": {
                "status": "uncertain",

                "uncertain_first_seq": first_seq,
                "uncertain_last_seq": last_seq,

                "error": error_text,

                "updated_at": now_utc(),
            }
        },
    )

    logger.error(
        "JOB UNCERTAIN | job=%s | IDs=%s-%s",
        job_id,
        first_seq,
        last_seq,
    )


# ============================================================
# PROCESS SEND JOB
# ============================================================

async def process_send_job(bot, job):
    job_id = job["_id"]

    logger.info(
        "WORKER PICKED JOB | job=%s",
        job_id,
    )

    while True:

        current = await jobs_col.find_one(
            {
                "_id": job_id,
                "worker_id": INSTANCE_ID,
            }
        )

        if not current:
            logger.warning(
                "JOB LOST | job=%s",
                job_id,
            )
            return

        status = current.get("status")

        if status == "cancelled":
            logger.info(
                "JOB CANCELLED | job=%s",
                job_id,
            )
            return

        if status == "paused":
            logger.info(
                "JOB PAUSED | job=%s",
                job_id,
            )
            return

        if status == "uncertain":
            logger.warning(
                "JOB UNCERTAIN - STOPPING | job=%s",
                job_id,
            )
            return

        if status != "running":
            return

        last_seq = int(
            current.get("last_seq", 0)
        )

        logger.info(
            "GET NEXT BATCH | last_seq=%s",
            last_seq,
        )

        docs = await get_next_batch(last_seq)

        if not docs:
            await jobs_col.update_one(
                {
                    "_id": job_id,
                    "worker_id": INSTANCE_ID,
                    "status": "running",
                },
                {
                    "$set": {
                        "status": "completed",
                        "updated_at": now_utc(),
                        "batch_first_seq": None,
                        "batch_last_seq": None,
                        "lock_until": None,
                    }
                },
            )

            logger.info(
                "JOB COMPLETED | job=%s",
                job_id,
            )

            return

        first_seq = docs[0]["seq"]
        last_batch_seq = docs[-1]["seq"]

        logger.info(
            "BATCH READY | IDs=%s-%s",
            first_seq,
            last_batch_seq,
        )

        claimed = await claim_batch(
            job_id,
            last_seq,
            first_seq,
            last_batch_seq,
        )

        if not claimed:
            logger.warning(
                "BATCH CLAIM FAILED | job=%s",
                job_id,
            )
            await asyncio.sleep(1)
            continue

        # ----------------------------------------------------
        # SEND
        # ----------------------------------------------------

        try:
            await send_album_once(
                bot,
                current["chat_id"],
                docs,
            )

        except (TimedOut, NetworkError) as e:

            await mark_job_uncertain(
                job_id,
                first_seq,
                last_batch_seq,
                f"Network ambiguity: {type(e).__name__}. "
                f"Album may have been delivered. "
                f"Automatic retry disabled to prevent duplicates.",
            )

            return

        except RetryAfter as e:

            await mark_job_uncertain(
                job_id,
                first_seq,
                last_batch_seq,
                f"RetryAfter exceeded MAX_RETRIES: {e}",
            )

            return

        except TelegramError as e:

            await jobs_col.update_one(
                {
                    "_id": job_id,
                    "worker_id": INSTANCE_ID,
                },
                {
                    "$set": {
                        "status": "failed",
                        "failed_batch_first_seq": first_seq,
                        "failed_batch_last_seq": last_batch_seq,
                        "error": str(e),
                        "updated_at": now_utc(),
                        "batch_first_seq": None,
                        "batch_last_seq": None,
                    },
                    "$inc": {
                        "failed": len(docs),
                    },
                },
            )

            logger.exception(
                "JOB FAILED | job=%s | IDs=%s-%s",
                job_id,
                first_seq,
                last_batch_seq,
            )

            return

        # ----------------------------------------------------
        # COMMIT PROGRESS
        # ----------------------------------------------------

        committed = await jobs_col.find_one_and_update(
            {
                "_id": job_id,
                "status": "running",
                "worker_id": INSTANCE_ID,

                # Critical guard:
                "last_seq": last_seq,

                "batch_first_seq": first_seq,
                "batch_last_seq": last_batch_seq,
            },
            {
                "$set": {
                    "last_seq": last_batch_seq,

                    "batch_first_seq": None,
                    "batch_last_seq": None,

                    "lock_until": (
                        now_utc()
                        + timedelta(
                            seconds=JOB_LEASE_SECONDS
                        )
                    ),

                    "updated_at": now_utc(),
                },
                "$inc": {
                    "sent": len(docs),
                },
            },
            return_document=ReturnDocument.AFTER,
        )

        if not committed:
            # Telegram send succeeded, but DB commit failed.
            # NEVER resend automatically.
            await mark_job_uncertain(
                job_id,
                first_seq,
                last_batch_seq,
                "Album was sent successfully but "
                "progress commit failed. "
                "Automatic resend disabled.",
            )

            return

        logger.info(
            "PROGRESS COMMITTED | job=%s | last_seq=%s",
            job_id,
            last_batch_seq,
        )

        await asyncio.sleep(SEND_DELAY)


# ============================================================
# SEND QUEUE WORKER
# ============================================================

async def send_queue_worker(bot):
    logger.info("SEND QUEUE WORKER STARTED")

    while True:
        try:

            job = await claim_job()

            if not job:
                await asyncio.sleep(WORKER_SLEEP)
                continue

            try:
                await process_send_job(
                    bot,
                    job,
                )

            except Exception:
                logger.exception(
                    "Unhandled worker error | job=%s",
                    job.get("_id"),
                )

                await jobs_col.update_one(
                    {
                        "_id": job["_id"],
                        "worker_id": INSTANCE_ID,
                    },
                    {
                        "$set": {
                            "status": "uncertain",
                            "error": (
                                "Worker crashed while processing job. "
                                "Manual review required."
                            ),
                            "updated_at": now_utc(),
                        }
                    },
                )

        except asyncio.CancelledError:
            logger.info(
                "SEND QUEUE WORKER STOPPED"
            )
            return

        except Exception:
            logger.exception(
                "Queue worker loop error"
            )

            await asyncio.sleep(5)


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
        await update.message.reply_text(
            "Usage:\n/send VIDEO_ID CHAT_ID"
        )
        return

    try:
        video_id = int(context.args[0])
        chat_id = int(context.args[1])
    except ValueError:
        await update.message.reply_text(
            "❌ Invalid ID or CHAT_ID"
        )
        return

    video = await videos_col.find_one(
        {"seq": video_id}
    )

    if not video:
        await update.message.reply_text(
            "❌ Video not found."
        )
        return

    try:
        await send_single_video(
            context.bot,
            chat_id,
            video,
        )

        await update.message.reply_text(
            f"✅ Video {video_id} sent."
        )

    except TelegramError as e:
        await update.message.reply_text(
            f"❌ Telegram error:\n{e}"
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
        await update.message.reply_text(
            "Usage:\n/delete VIDEO_ID"
        )
        return

    try:
        video_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text(
            "❌ Invalid VIDEO_ID"
        )
        return

    result = await videos_col.delete_one(
        {"seq": video_id}
    )

    if result.deleted_count:
        await update.message.reply_text(
            f"🗑 Deleted video {video_id}"
        )
    else:
        await update.message.reply_text(
            "❌ Video not found."
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

    job = await jobs_col.find_one(
        {
            "type": "sendall",
            "status": "running",
        },
        sort=[("created_at", DESCENDING)],
    )

    if not job:
        await update.message.reply_text(
            "❌ No running SENDALL job."
        )
        return

    await jobs_col.update_one(
        {
            "_id": job["_id"],
            "status": "running",
            "worker_id": INSTANCE_ID,
        },
        {
            "$set": {
                "status": "paused",
                "updated_at": now_utc(),
            }
        },
    )

    await update.message.reply_text(
        f"⏸ Job paused.\n\n"
        f"Job: {job['_id']}\n"
        f"Last ID: {job.get('last_seq', 0)}"
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

    job = await jobs_col.find_one(
        {
            "type": "sendall",
            "status": "paused",
        },
        sort=[("created_at", DESCENDING)],
    )

    if not job:
        await update.message.reply_text(
            "❌ No paused SENDALL job."
        )
        return

    result = await jobs_col.update_one(
        {
            "_id": job["_id"],
            "status": "paused",
        },
        {
            "$set": {
                "status": "queued",
                "worker_id": None,
                "lock_until": None,
                "updated_at": now_utc(),
            }
        },
    )

    if result.modified_count:
        await update.message.reply_text(
            f"▶️ Job resumed.\n\n"
            f"Job: {job['_id']}\n"
            f"Last ID: {job.get('last_seq', 0)}"
        )
    else:
        await update.message.reply_text(
            "❌ Could not resume job."
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

    job = await jobs_col.find_one(
        {
            "type": "sendall",
            "status": {
                "$in": [
                    "queued",
                    "running",
                    "paused",
                    "uncertain",
                ]
            },
        },
        sort=[("created_at", DESCENDING)],
    )

    if not job:
        await update.message.reply_text(
            "❌ No active SENDALL job."
        )
        return

    await jobs_col.update_one(
        {
            "_id": job["_id"],
        },
        {
            "$set": {
                "status": "cancelled",
                "updated_at": now_utc(),
            }
        },
    )

    await update.message.reply_text(
        f"🛑 Job cancelled.\n\n"
        f"Job: {job['_id']}\n"
        f"Last committed ID: {job.get('last_seq', 0)}"
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

    job = await jobs_col.find_one(
        {
            "type": "sendall",
            "status": {
                "$in": [
                    "queued",
                    "running",
                    "paused",
                    "uncertain",
                ]
            },
        },
        sort=[("created_at", DESCENDING)],
    )

    if not job:
        await update.message.reply_text(
            "ℹ️ No active SENDALL job."
        )
        return

    total = job.get("total", 0)
    sent = job.get("sent", 0)
    last_seq = job.get("last_seq", 0)

    percent = (
        (sent / total * 100)
        if total
        else 0
    )

    text = (
        "📦 SENDALL JOB\n\n"
        f"🆔 Job: {job['_id']}\n"
        f"📌 Status: {job.get('status')}\n"
        f"📤 Destination: {job.get('chat_id')}\n\n"
        f"🎬 Total: {total}\n"
        f"✅ Sent: {sent}\n"
        f"📊 Progress: {percent:.2f}%\n"
        f"🔢 Last committed ID: {last_seq}\n\n"
        f"👷 Worker: {job.get('worker_id')}\n"
        f"🔒 Batch: "
        f"{job.get('batch_first_seq')} - "
        f"{job.get('batch_last_seq')}\n"
    )

    if job.get("status") == "uncertain":
        text += (
            "\n⚠️ UNCERTAIN BATCH\n"
            f"IDs: "
            f"{job.get('uncertain_first_seq')} - "
            f"{job.get('uncertain_last_seq')}\n\n"
            "Automatic retry is disabled to prevent duplicates."
        )

    await update.message.reply_text(text)


# ============================================================
# VIDEO HANDLER
# ============================================================

async def video_handler(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    try:
        message = update.effective_message

        if not message or not message.video:
            return

        await save_video_message(message)

    except Exception:
        logger.exception(
            "Video handler error"
        )


# ============================================================
# STARTUP RECOVERY
# ============================================================

async def mark_stale_jobs_uncertain():
    """
    NEVER automatically requeue running jobs.

    A process may have sent an album to Telegram and crashed before
    writing progress to MongoDB. Requeueing it can duplicate that album.
    """

    result = await jobs_col.update_many(
        {
            "status": "running",
        },
        {
            "$set": {
                "status": "uncertain",
                "error": (
                    "Bot restarted while job was running. "
                    "Manual review required."
                ),
                "updated_at": now_utc(),
            }
        },
    )

    if result.modified_count:
        logger.warning(
            "Marked %s interrupted job(s) as UNCERTAIN",
            result.modified_count,
        )


# ============================================================
# MAIN
# ============================================================

async def main():

    if not BOT_TOKEN:
        raise RuntimeError(
            "BOT_TOKEN is missing"
        )

    await init_db()

    # --------------------------------------------------------
    # IMPORTANT:
    # Acquire Mongo lock BEFORE Telegram polling.
    # --------------------------------------------------------

    locked = await acquire_instance_lock()

    if not locked:
        logger.error(
            "Another instance is active. "
            "This process will NOT start polling."
        )

        if mongo_client:
            mongo_client.close()

        return

    heartbeat_task = None
    worker_task = None
    application = None

    try:

        # Existing interrupted jobs are NOT resumed.
        await mark_stale_jobs_uncertain()

        application = (
            Application
            .builder()
            .token(BOT_TOKEN)
            .build()
        )

        # ----------------------------------------------------
        # HANDLERS
        # ----------------------------------------------------

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

        application.add_handler(
            MessageHandler(
                filters.VIDEO,
                video_handler,
            )
        )

        # ----------------------------------------------------
        # PTB STARTUP
        # ----------------------------------------------------

        await application.initialize()

        await application.start()

        # Make sure Telegram webhook is not interfering.
        await application.bot.delete_webhook(
            drop_pending_updates=False
        )

        # Start polling manually.
        await application.updater.start_polling(
            allowed_updates=Update.ALL_TYPES,
            drop_pending_updates=False,
        )

        logger.info(
            "TELEGRAM POLLING STARTED"
        )

        heartbeat_task = asyncio.create_task(
            heartbeat_instance_lock()
        )

        worker_task = asyncio.create_task(
            send_queue_worker(
                application.bot
            )
        )

        logger.info(
            "BOT IS RUNNING"
        )

        # Keep process alive.
        await asyncio.Event().wait()

    except asyncio.CancelledError:
        logger.info(
            "Main task cancelled"
        )

    except Exception:
        logger.exception(
            "FATAL MAIN ERROR"
        )

    finally:

        logger.info(
            "SHUTTING DOWN..."
        )

        if worker_task:
            worker_task.cancel()

            try:
                await worker_task
            except asyncio.CancelledError:
                pass

        if heartbeat_task:
            heartbeat_task.cancel()

            try:
                await heartbeat_task
            except asyncio.CancelledError:
                pass

        if application:

            try:
                if application.updater:
                    await application.updater.stop()
            except Exception:
                logger.exception(
                    "Updater stop error"
                )

            try:
                await application.stop()
            except Exception:
                logger.exception(
                    "Application stop error"
                )

            try:
                await application.shutdown()
            except Exception:
                logger.exception(
                    "Application shutdown error"
                )

        await release_instance_lock()

        if mongo_client:
            mongo_client.close()

        logger.info(
            "BOT STOPPED"
        )


# ============================================================
# RUN
# ============================================================

if __name__ == "__main__":
    asyncio.run(main())
