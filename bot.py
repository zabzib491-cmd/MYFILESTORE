import os
import asyncio
import logging
import uuid
from datetime import datetime, timezone, timedelta
from typing import Optional

from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import ASCENDING, DESCENDING, ReturnDocument, DuplicateKeyError

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

ALBUM_SIZE = 10

# MongoDB lock settings
INSTANCE_LOCK_SECONDS = 60
INSTANCE_HEARTBEAT_SECONDS = 20

JOB_LOCK_SECONDS = 300


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

client = AsyncIOMotorClient(MONGO_URI)

db = client["video_storage_bot"]

videos = db["videos"]
jobs = db["jobs"]
settings = db["settings"]


# ============================================================
# GLOBAL INSTANCE ID
# ============================================================

INSTANCE_ID = str(uuid.uuid4())

instance_heartbeat_task: Optional[asyncio.Task] = None


# ============================================================
# TIME
# ============================================================

def now():
    return datetime.now(timezone.utc)


# ============================================================
# ADMIN
# ============================================================

def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


# ============================================================
# DATABASE INIT
# ============================================================

async def init_db():
    logger.info("Initializing MongoDB...")

    await videos.create_index(
        [("file_unique_id", ASCENDING)],
        unique=True,
        name="unique_file_unique_id",
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
        name="created_at_desc",
    )

    await videos.create_index(
        [("source_chat_id", ASCENDING)],
        name="source_chat_id_index",
    )

    await jobs.create_index(
        [("status", ASCENDING)],
        name="job_status_index",
    )

    await jobs.create_index(
        [("created_at", DESCENDING)],
        name="job_created_index",
    )

    await jobs.create_index(
        [("worker_id", ASCENDING)],
        name="worker_id_index",
    )

    await settings.create_index(
        [("_id", ASCENDING)],
        name="settings_id_index",
    )

    logger.info("MongoDB connected successfully")


# ============================================================
# SINGLE INSTANCE LOCK
# ============================================================

async def acquire_instance_lock() -> bool:
    """
    Ensures only one bot polling instance is active.

    IMPORTANT:
    This does NOT delete anything from the database.
    """

    current = now()
    expires = current + timedelta(seconds=INSTANCE_LOCK_SECONDS)

    result = await settings.find_one_and_update(
        {
            "_id": "bot_instance_lock",
            "$or": [
                {"expires_at": {"$lte": current}},
                {"expires_at": {"$exists": False}},
                {"instance_id": INSTANCE_ID},
            ],
        },
        {
            "$set": {
                "instance_id": INSTANCE_ID,
                "started_at": current,
                "expires_at": expires,
            }
        },
        upsert=True,
        return_document=ReturnDocument.AFTER,
    )

    if result and result.get("instance_id") == INSTANCE_ID:
        logger.info(
            "BOT INSTANCE LOCK ACQUIRED | instance=%s",
            INSTANCE_ID,
        )
        return True

    logger.error(
        "ANOTHER BOT INSTANCE IS ALREADY RUNNING"
    )
    return False


async def heartbeat_instance():
    while True:
        try:
            current = now()
            expires = current + timedelta(
                seconds=INSTANCE_LOCK_SECONDS
            )

            result = await settings.update_one(
                {
                    "_id": "bot_instance_lock",
                    "instance_id": INSTANCE_ID,
                },
                {
                    "$set": {
                        "expires_at": expires,
                        "heartbeat_at": current,
                    }
                },
            )

            if result.modified_count == 0:
                logger.error(
                    "INSTANCE LOCK LOST | stopping heartbeat"
                )
                return

        except Exception:
            logger.exception(
                "Instance heartbeat error"
            )

        await asyncio.sleep(
            INSTANCE_HEARTBEAT_SECONDS
        )


async def release_instance_lock():
    try:
        await settings.delete_one(
            {
                "_id": "bot_instance_lock",
                "instance_id": INSTANCE_ID,
            }
        )

        logger.info("BOT INSTANCE LOCK RELEASED")

    except Exception:
        logger.exception(
            "Failed to release instance lock"
        )


# ============================================================
# VIDEO STORAGE
# ============================================================

async def save_video_message(message):
    if not message.video:
        return None

    video = message.video

    file_id = video.file_id
    file_unique_id = video.file_unique_id

    existing = await videos.find_one(
        {
            "file_unique_id": file_unique_id
        }
    )

    if existing:
        return existing

    # Atomic sequence counter.
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

    seq = int(counter["value"])

    document = {
        "seq": seq,
        "file_id": file_id,
        "file_unique_id": file_unique_id,
        "width": video.width,
        "height": video.height,
        "duration": video.duration,
        "file_size": video.file_size,
        "caption": message.caption or "",
        "source_chat_id": message.chat_id,
        "source_message_id": message.message_id,
        "created_at": now(),
    }

    try:
        await videos.insert_one(document)

        logger.info(
            "VIDEO SAVED | seq=%s | unique_id=%s",
            seq,
            file_unique_id,
        )

        return document

    except DuplicateKeyError:
        # Another worker saved the same video.
        existing = await videos.find_one(
            {
                "file_unique_id": file_unique_id
            }
        )

        return existing


# ============================================================
# /START
# ============================================================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🎬 Video Storage Bot\n\n"
        "Videos sent to this bot are automatically stored."
    )


# ============================================================
# /STATS
# ============================================================

async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return

    total = await videos.count_documents({})

    latest = await videos.find_one(
        {},
        sort=[("seq", DESCENDING)],
    )

    latest_seq = latest.get("seq", 0) if latest else 0

    await update.message.reply_text(
        f"📊 VIDEO STORAGE STATS\n\n"
        f"🎬 Total Videos: {total}\n"
        f"🔢 Latest ID: {latest_seq}"
    )


# ============================================================
# /LIST
# ============================================================

async def list_videos(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    if not is_admin(update.effective_user.id):
        return

    limit = 20

    if context.args:
        try:
            limit = int(context.args[0])
        except ValueError:
            limit = 20

    limit = max(1, min(limit, 100))

    cursor = (
        videos
        .find({})
        .sort("seq", DESCENDING)
        .limit(limit)
    )

    result = []

    async for video in cursor:
        caption = video.get("caption", "")

        if caption:
            caption = caption[:40]

        result.append(
            f"#{video.get('seq')} "
            f"{caption}"
        )

    if not result:
        await update.message.reply_text(
            "No videos found."
        )
        return

    await update.message.reply_text(
        "📋 VIDEO LIST\n\n"
        + "\n".join(result)
    )


# ============================================================
# /SEARCH
# ============================================================

async def search_videos(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    if not is_admin(update.effective_user.id):
        return

    if not context.args:
        await update.message.reply_text(
            "Usage:\n/search keyword"
        )
        return

    query = " ".join(context.args)

    cursor = (
        videos
        .find(
            {
                "caption": {
                    "$regex": query,
                    "$options": "i",
                }
            }
        )
        .sort("seq", DESCENDING)
        .limit(50)
    )

    result = []

    async for video in cursor:
        result.append(
            f"#{video.get('seq')} "
            f"{video.get('caption', '')[:60]}"
        )

    if not result:
        await update.message.reply_text(
            "No matching videos found."
        )
        return

    await update.message.reply_text(
        "🔎 SEARCH RESULTS\n\n"
        + "\n".join(result)
    )


# ============================================================
# SEND SINGLE VIDEO
# ============================================================

async def send_single_video(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    if not is_admin(update.effective_user.id):
        return

    if len(context.args) < 2:
        await update.message.reply_text(
            "Usage:\n/send VIDEO_ID CHAT_ID"
        )
        return

    try:
        seq = int(context.args[0])
        chat_id = int(context.args[1])
    except ValueError:
        await update.message.reply_text(
            "Invalid ID or CHAT_ID."
        )
        return

    video = await videos.find_one(
        {
            "seq": seq
        }
    )

    if not video:
        await update.message.reply_text(
            f"Video #{seq} not found."
        )
        return

    try:
        await context.bot.send_video(
            chat_id=chat_id,
            video=video["file_id"],
            caption=video.get("caption") or None,
        )

        await update.message.reply_text(
            f"✅ Video #{seq} sent."
        )

    except TelegramError as e:
        await update.message.reply_text(
            f"❌ Send failed:\n{e}"
        )


# ============================================================
# CREATE SENDALL JOB
# ============================================================

async def create_send_job(
    chat_id: int,
    created_by: int,
):
    job_id = uuid.uuid4().hex

    document = {
        "_id": job_id,
        "type": "sendall",
        "chat_id": chat_id,
        "status": "queued",
        "created_by": created_by,

        # IMPORTANT:
        # Existing job structure preserved.
        "last_seq": 0,

        "sent": 0,
        "failed": 0,

        "worker_id": None,
        "lock_until": None,

        "batch_first_seq": None,
        "batch_last_seq": None,

        "created_at": now(),
        "updated_at": now(),
    }

    await jobs.insert_one(document)

    return job_id


# ============================================================
# ACTIVE JOB CHECK
# ============================================================

async def has_active_sendall_job():
    job = await jobs.find_one(
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
        }
    )

    return job


# ============================================================
# /SENDALL
# ============================================================

async def send_all(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    if not is_admin(update.effective_user.id):
        return

    if not context.args:
        await update.message.reply_text(
            "Usage:\n/sendall CHAT_ID"
        )
        return

    try:
        chat_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text(
            "Invalid CHAT_ID."
        )
        return

    active = await has_active_sendall_job()

    if active:
        await update.message.reply_text(
            "⚠️ Another SENDALL job is already active.\n\n"
            f"Job: {active['_id']}\n"
            f"Status: {active['status']}\n"
            f"Last ID: {active.get('last_seq', 0)}"
        )
        return

    total = await videos.count_documents({})

    if total == 0:
        await update.message.reply_text(
            "No videos stored."
        )
        return

    try:
        chat = await context.bot.get_chat(chat_id)

        logger.info(
            "DESTINATION OK | id=%s | type=%s | title=%s",
            chat.id,
            chat.type,
            getattr(chat, "title", None),
        )

    except TelegramError as e:
        await update.message.reply_text(
            f"❌ Destination error:\n{e}"
        )
        return

    job_id = await create_send_job(
        chat_id=chat_id,
        created_by=update.effective_user.id,
    )

    await update.message.reply_text(
        "🚀 SENDALL STARTED\n\n"
        f"🎬 Videos: {total}\n"
        f"📦 Album size: {ALBUM_SIZE}\n"
        f"🆔 Job: {job_id}\n\n"
        "Use /job to check progress."
    )


# ============================================================
# CLAIM JOB
# ============================================================

async def claim_job():
    current = now()
    lock_until = current + timedelta(
        seconds=JOB_LOCK_SECONDS
    )

    job = await jobs.find_one_and_update(
        {
            "type": "sendall",
            "status": "queued",
            "$or": [
                {
                    "lock_until": {
                        "$lte": current
                    }
                },
                {
                    "lock_until": None
                },
                {
                    "lock_until": {
                        "$exists": False
                    }
                },
            ],
        },
        {
            "$set": {
                "status": "running",
                "worker_id": INSTANCE_ID,
                "lock_until": lock_until,
                "updated_at": current,
            }
        },
        sort=[
            ("created_at", ASCENDING)
        ],
        return_document=ReturnDocument.AFTER,
    )

    return job


# ============================================================
# GET NEXT BATCH
# ============================================================

async def get_next_batch(last_seq: int):
    cursor = (
        videos
        .find(
            {
                "seq": {
                    "$gt": last_seq
                }
            }
        )
        .sort(
            "seq",
            ASCENDING
        )
        .limit(ALBUM_SIZE)
    )

    batch = await cursor.to_list(
        length=ALBUM_SIZE
    )

    return batch


# ============================================================
# CLAIM BATCH LOCK
# ============================================================

async def claim_batch(
    job_id: str,
    last_seq: int,
    first_seq: int,
    last_batch_seq: int,
):
    """
    Atomically records which batch this worker owns.

    Only the worker owning the current job lock can claim it.
    """

    current = now()
    lock_until = current + timedelta(
        seconds=JOB_LOCK_SECONDS
    )

    result = await jobs.find_one_and_update(
        {
            "_id": job_id,
            "status": "running",
            "worker_id": INSTANCE_ID,
            "last_seq": last_seq,
            "$or": [
                {
                    "batch_first_seq": None
                },
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
                "lock_until": lock_until,
                "updated_at": current,
            }
        },
        return_document=ReturnDocument.AFTER,
    )

    return result


# ============================================================
# SEND ALBUM
# ============================================================

async def send_album_once(
    bot,
    chat_id: int,
    batch,
):
    media = []

    for video in batch:
        media.append(
            InputMediaVideo(
                media=video["file_id"],
                caption=video.get("caption") or None,
            )
        )

    logger.info(
        "SENDING ALBUM | chat=%s | IDs=%s-%s | count=%s",
        chat_id,
        batch[0]["seq"],
        batch[-1]["seq"],
        len(batch),
    )

    # --------------------------------------------------------
    # IMPORTANT
    #
    # We intentionally DO NOT retry TimedOut/NetworkError.
    #
    # Telegram may have accepted the album even when the client
    # doesn't receive the response.
    #
    # Retrying blindly can create duplicate albums.
    # --------------------------------------------------------

    try:
        await bot.send_media_group(
            chat_id=chat_id,
            media=media,
        )

        return True

    except RetryAfter:
        raise

    except (TimedOut, NetworkError):
        raise

    except TelegramError:
        raise


# ============================================================
# PROCESS SEND JOB
# ============================================================

async def process_send_job(
    job_id: str,
):
    logger.info(
        "PROCESSING SEND JOB | job=%s",
        job_id,
    )

    while True:

        # ----------------------------------------------------
        # Always reload job from MongoDB.
        # ----------------------------------------------------

        job = await jobs.find_one(
            {
                "_id": job_id
            }
        )

        if not job:
            logger.error(
                "JOB NOT FOUND | job=%s",
                job_id,
            )
            return

        status = job.get("status")

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
                "JOB UNCERTAIN | job=%s",
                job_id,
            )
            return

        if status != "running":
            return

        # ----------------------------------------------------
        # Make sure this process still owns the job.
        # ----------------------------------------------------

        if job.get("worker_id") != INSTANCE_ID:
            logger.warning(
                "JOB OWNERSHIP LOST | job=%s",
                job_id,
            )
            return

        # ----------------------------------------------------
        # Refresh job lease.
        # ----------------------------------------------------

        current = now()

        await jobs.update_one(
            {
                "_id": job_id,
                "status": "running",
                "worker_id": INSTANCE_ID,
            },
            {
                "$set": {
                    "lock_until": current
                    + timedelta(
                        seconds=JOB_LOCK_SECONDS
                    ),
                    "updated_at": current,
                }
            },
        )

        last_seq = int(
            job.get("last_seq", 0)
        )

        logger.info(
            "GET NEXT BATCH | job=%s | last_seq=%s",
            job_id,
            last_seq,
        )

        # ----------------------------------------------------
        # Get next batch.
        # ----------------------------------------------------

        batch = await get_next_batch(
            last_seq
        )

        if not batch:
            await jobs.update_one(
                {
                    "_id": job_id,
                    "status": "running",
                    "worker_id": INSTANCE_ID,
                },
                {
                    "$set": {
                        "status": "completed",
                        "worker_id": None,
                        "lock_until": None,
                        "batch_first_seq": None,
                        "batch_last_seq": None,
                        "updated_at": now(),
                    }
                },
            )

            logger.info(
                "SEND JOB COMPLETED | job=%s",
                job_id,
            )

            return

        first_seq = int(
            batch[0]["seq"]
        )

        last_batch_seq = int(
            batch[-1]["seq"]
        )

        # ----------------------------------------------------
        # Safety check.
        # ----------------------------------------------------

        if first_seq <= last_seq:
            logger.error(
                "INVALID BATCH | job=%s | "
                "last_seq=%s | first_seq=%s",
                job_id,
                last_seq,
                first_seq,
            )

            await jobs.update_one(
                {
                    "_id": job_id
                },
                {
                    "$set": {
                        "status": "failed",
                        "error": (
                            "Invalid batch sequence detected"
                        ),
                        "updated_at": now(),
                    }
                },
            )

            return

        logger.info(
            "BATCH READY | job=%s | IDs=%s-%s",
            job_id,
            first_seq,
            last_batch_seq,
        )

        # ----------------------------------------------------
        # Claim this exact batch.
        # ----------------------------------------------------

        claimed = await claim_batch(
            job_id=job_id,
            last_seq=last_seq,
            first_seq=first_seq,
            last_batch_seq=last_batch_seq,
        )

        if not claimed:
            logger.warning(
                "BATCH CLAIM FAILED | job=%s | "
                "IDs=%s-%s",
                job_id,
                first_seq,
                last_batch_seq,
            )

            await asyncio.sleep(1)
            continue

        # ----------------------------------------------------
        # SEND
        # ----------------------------------------------------

        try:

            while True:
                try:
                    success = await send_album_once(
                        bot=application.bot,
                        chat_id=job["chat_id"],
                        batch=batch,
                    )

                    if success:
                        break

                except RetryAfter as e:
                    retry_seconds = int(
                        getattr(
                            e,
                            "retry_after",
                            5,
                        )
                    )

                    logger.warning(
                        "Telegram RetryAfter | "
                        "job=%s | wait=%ss",
                        job_id,
                        retry_seconds,
                    )

                    await asyncio.sleep(
                        retry_seconds
                    )

                    # RetryAfter means Telegram explicitly
                    # told us to retry, so this is safe.

                    continue

                except (TimedOut, NetworkError) as e:
                    # ------------------------------------------------
                    # DO NOT RETRY.
                    #
                    # The request may already have reached Telegram.
                    # Retrying could duplicate this album.
                    # ------------------------------------------------

                    logger.error(
                        "UNCERTAIN TELEGRAM SEND | "
                        "job=%s | IDs=%s-%s | error=%s",
                        job_id,
                        first_seq,
                        last_batch_seq,
                        e,
                    )

                    await jobs.update_one(
                        {
                            "_id": job_id,
                            "worker_id": INSTANCE_ID,
                        },
                        {
                            "$set": {
                                "status": "uncertain",
                                "error": (
                                    "Telegram send returned "
                                    "a network/timeout error. "
                                    "Batch may already have been sent."
                                ),
                                "uncertain_first_seq": first_seq,
                                "uncertain_last_seq": last_batch_seq,
                                "worker_id": None,
                                "lock_until": None,
                                "updated_at": now(),
                            }
                        },
                    )

                    return

                except TelegramError as e:

                    logger.error(
                        "TELEGRAM SEND ERROR | "
                        "job=%s | IDs=%s-%s | error=%s",
                        job_id,
                        first_seq,
                        last_batch_seq,
                        e,
                    )

                    await jobs.update_one(
                        {
                            "_id": job_id,
                            "worker_id": INSTANCE_ID,
                        },
                        {
                            "$inc": {
                                "failed": len(batch)
                            },
                            "$set": {
                                "status": "failed",
                                "error": str(e),
                                "worker_id": None,
                                "lock_until": None,
                                "updated_at": now(),
                            }
                        },
                    )

                    return

        except Exception as e:
            logger.exception(
                "Unexpected SENDALL error | job=%s",
                job_id,
            )

            await jobs.update_one(
                {
                    "_id": job_id,
                    "worker_id": INSTANCE_ID,
                },
                {
                    "$set": {
                        "status": "failed",
                        "error": str(e),
                        "worker_id": None,
                        "lock_until": None,
                        "updated_at": now(),
                    }
                },
            )

            return

        # ----------------------------------------------------
        # CRITICAL PROGRESS COMMIT
        #
        # Only after Telegram confirms successful send.
        #
        # And only if MongoDB still contains the exact
        # last_seq that we started this batch with.
        # ----------------------------------------------------

        updated = await jobs.update_one(
            {
                "_id": job_id,
                "status": "running",
                "worker_id": INSTANCE_ID,
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
                        now()
                        + timedelta(
                            seconds=JOB_LOCK_SECONDS
                        )
                    ),
                    "updated_at": now(),
                },
                "$inc": {
                    "sent": len(batch)
                },
            },
        )

        if updated.modified_count != 1:

            logger.error(
                "PROGRESS COMMIT FAILED | "
                "job=%s | IDs=%s-%s",
                job_id,
                first_seq,
                last_batch_seq,
            )

            # We have already sent this batch, but cannot safely
            # determine whether progress was committed.
            #
            # Do NOT resend it.

            await jobs.update_one(
                {
                    "_id": job_id
                },
                {
                    "$set": {
                        "status": "uncertain",
                        "error": (
                            "Batch was sent but progress "
                            "commit failed."
                        ),
                        "uncertain_first_seq": first_seq,
                        "uncertain_last_seq": last_batch_seq,
                        "worker_id": None,
                        "lock_until": None,
                        "updated_at": now(),
                    }
                },
            )

            return

        logger.info(
            "BATCH COMPLETED | job=%s | "
            "IDs=%s-%s | new_last_seq=%s",
            job_id,
            first_seq,
            last_batch_seq,
            last_batch_seq,
        )

        # ----------------------------------------------------
        # Delay between albums.
        # ----------------------------------------------------

        if SEND_DELAY > 0:
            await asyncio.sleep(
                SEND_DELAY
            )


# ============================================================
# SEND QUEUE WORKER
# ============================================================

async def send_queue_worker():
    logger.info(
        "SEND QUEUE WORKER STARTED"
    )

    while True:

        try:

            job = await claim_job()

            if not job:
                await asyncio.sleep(
                    1
                )
                continue

            job_id = job["_id"]

            logger.info(
                "SEND QUEUE PICKED JOB | "
                "job=%s | chat=%s",
                job_id,
                job["chat_id"],
            )

            await process_send_job(
                job_id
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

            await asyncio.sleep(
                3
            )


# ============================================================
# /PAUSE
# ============================================================

async def pause_job(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    if not is_admin(update.effective_user.id):
        return

    job = await jobs.find_one(
        {
            "type": "sendall",
            "status": "running",
        }
    )

    if not job:
        await update.message.reply_text(
            "No running SENDALL job."
        )
        return

    result = await jobs.update_one(
        {
            "_id": job["_id"],
            "status": "running",
        },
        {
            "$set": {
                "status": "paused",
                "worker_id": None,
                "lock_until": None,
                "updated_at": now(),
            }
        },
    )

    if result.modified_count:
        await update.message.reply_text(
            f"⏸️ Job paused.\n"
            f"Job: {job['_id']}\n"
            f"Last ID: {job.get('last_seq', 0)}"
        )


# ============================================================
# /RESUME
# ============================================================

async def resume_job(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    if not is_admin(update.effective_user.id):
        return

    job = await jobs.find_one(
        {
            "type": "sendall",
            "status": "paused",
        },
        sort=[
            ("created_at", DESCENDING)
        ],
    )

    if not job:
        await update.message.reply_text(
            "No paused SENDALL job."
        )
        return

    await jobs.update_one(
        {
            "_id": job["_id"],
            "status": "paused",
        },
        {
            "$set": {
                "status": "queued",
                "worker_id": None,
                "lock_until": None,
                "updated_at": now(),
            }
        },
    )

    await update.message.reply_text(
        f"▶️ Job resumed.\n"
        f"Job: {job['_id']}\n"
        f"Last ID: {job.get('last_seq', 0)}"
    )


# ============================================================
# /CANCEL
# ============================================================

async def cancel_job(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    if not is_admin(update.effective_user.id):
        return

    job = await jobs.find_one(
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
        sort=[
            ("created_at", DESCENDING)
        ],
    )

    if not job:
        await update.message.reply_text(
            "No active SENDALL job."
        )
        return

    await jobs.update_one(
        {
            "_id": job["_id"],
        },
        {
            "$set": {
                "status": "cancelled",
                "worker_id": None,
                "lock_until": None,
                "updated_at": now(),
            }
        },
    )

    await update.message.reply_text(
        f"🛑 Job cancelled.\n"
        f"Job: {job['_id']}\n"
        f"Last ID: {job.get('last_seq', 0)}"
    )


# ============================================================
# /JOB
# ============================================================

async def job_status(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    if not is_admin(update.effective_user.id):
        return

    job = await jobs.find_one(
        {},
        sort=[
            ("created_at", DESCENDING)
        ],
    )

    if not job:
        await update.message.reply_text(
            "No jobs found."
        )
        return

    await update.message.reply_text(
        "📦 SENDALL JOB\n\n"
        f"🆔 Job: {job.get('_id')}\n"
        f"📌 Status: {job.get('status')}\n"
        f"🔢 Last ID: {job.get('last_seq', 0)}\n"
        f"📤 Sent: {job.get('sent', 0)}\n"
        f"❌ Failed: {job.get('failed', 0)}\n"
        f"🔒 Worker: {job.get('worker_id')}\n"
        f"📦 Current Batch: "
        f"{job.get('batch_first_seq')} - "
        f"{job.get('batch_last_seq')}\n"
        f"⚠️ Uncertain Batch: "
        f"{job.get('uncertain_first_seq')} - "
        f"{job.get('uncertain_last_seq')}"
    )


# ============================================================
# /DELETE
# ============================================================

async def delete_video(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    if not is_admin(update.effective_user.id):
        return

    if not context.args:
        await update.message.reply_text(
            "Usage:\n/delete VIDEO_ID"
        )
        return

    try:
        seq = int(context.args[0])
    except ValueError:
        await update.message.reply_text(
            "Invalid VIDEO_ID."
        )
        return

    result = await videos.delete_one(
        {
            "seq": seq
        }
    )

    if result.deleted_count:
        await update.message.reply_text(
            f"🗑️ Video #{seq} deleted."
        )
    else:
        await update.message.reply_text(
            f"Video #{seq} not found."
        )


# ============================================================
# VIDEO HANDLER
# ============================================================

async def video_handler(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    if not update.message:
        return

    try:
        saved = await save_video_message(
            update.message
        )

        if saved:
            await update.message.reply_text(
                f"✅ Video saved\n"
                f"ID: #{saved.get('seq')}"
            )

    except Exception:
        logger.exception(
            "Failed to save video"
        )


# ============================================================
# STARTUP
# ============================================================

async def on_startup(
    application: Application,
):
    global instance_heartbeat_task

    await init_db()

    acquired = await acquire_instance_lock()

    if not acquired:
        logger.error(
            "STARTUP ABORTED: another bot instance is active"
        )
        raise RuntimeError(
            "Another bot instance is already running"
        )

    instance_heartbeat_task = asyncio.create_task(
        heartbeat_instance()
    )

    logger.info(
        "Video Storage Bot startup complete"
    )


# ============================================================
# SHUTDOWN
# ============================================================

async def on_shutdown(
    application: Application,
):
    global instance_heartbeat_task

    if instance_heartbeat_task:
        instance_heartbeat_task.cancel()

        try:
            await instance_heartbeat_task
        except asyncio.CancelledError:
            pass

        instance_heartbeat_task = None

    await release_instance_lock()

    logger.info(
        "Video Storage Bot shutdown complete"
    )


# ============================================================
# MAIN
# ============================================================

application: Optional[Application] = None

send_worker_task: Optional[asyncio.Task] = None


async def main():
    global application
    global send_worker_task

    if not BOT_TOKEN:
        raise RuntimeError(
            "BOT_TOKEN environment variable is missing"
        )

    if not MONGO_URI:
        raise RuntimeError(
            "MONGO_URI environment variable is missing"
        )

    application = (
        Application.builder()
        .token(BOT_TOKEN)
        .post_init(on_startup)
        .post_shutdown(on_shutdown)
        .build()
    )

    # --------------------------------------------------------
    # COMMANDS
    # --------------------------------------------------------

    application.add_handler(
        CommandHandler(
            "start",
            start,
        )
    )

    application.add_handler(
        CommandHandler(
            "stats",
            stats,
        )
    )

    application.add_handler(
        CommandHandler(
            "list",
            list_videos,
        )
    )

    application.add_handler(
        CommandHandler(
            "search",
            search_videos,
        )
    )

    application.add_handler(
        CommandHandler(
            "send",
            send_single_video,
        )
    )

    application.add_handler(
        CommandHandler(
            "sendall",
            send_all,
        )
    )

    application.add_handler(
        CommandHandler(
            "pause",
            pause_job,
        )
    )

    application.add_handler(
        CommandHandler(
            "resume",
            resume_job,
        )
    )

    application.add_handler(
        CommandHandler(
            "cancel",
            cancel_job,
        )
    )

    application.add_handler(
        CommandHandler(
            "job",
            job_status,
        )
    )

    application.add_handler(
        CommandHandler(
            "delete",
            delete_video,
        )
    )

    # --------------------------------------------------------
    # VIDEO AUTO SAVE
    # --------------------------------------------------------

    application.add_handler(
        MessageHandler(
            filters.VIDEO,
            video_handler,
        )
    )

    # --------------------------------------------------------
    # MANUAL ASYNC STARTUP
    # --------------------------------------------------------

    await application.initialize()

    try:

        await application.start()

        logger.info(
            "BOT IS ONLINE"
        )

        # Start background SENDALL worker.
        send_worker_task = asyncio.create_task(
            send_queue_worker()
        )

        logger.info(
            "SENDALL WORKER IS ONLINE"
        )

        # ----------------------------------------------------
        # IMPORTANT:
        # Drop webhook before polling.
        # ----------------------------------------------------

        await application.bot.delete_webhook(
            drop_pending_updates=False
        )

        await application.updater.start_polling(
            allowed_updates=Update.ALL_TYPES,
            drop_pending_updates=False,
        )

        # Keep process alive.
        await asyncio.Event().wait()

    finally:

        if send_worker_task:

            send_worker_task.cancel()

            try:
                await send_worker_task
            except asyncio.CancelledError:
                pass

            send_worker_task = None

        if application.updater.running:
            await application.updater.stop()

        if application.running:
            await application.stop()

        await application.shutdown()


if __name__ == "__main__":
    try:
        asyncio.run(main())

    except KeyboardInterrupt:
        logger.info(
            "Bot stopped by user"
        )

    except Exception:
        logger.exception(
            "Fatal application error"
        )
        raise
