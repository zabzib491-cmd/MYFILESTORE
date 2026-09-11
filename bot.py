import os
import asyncio
import logging
import uuid
from datetime import datetime, timezone, timedelta

from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import ASCENDING, DESCENDING, ReturnDocument
from pymongo.errors import DuplicateKeyError

from telegram import Update, InputMediaVideo
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
from telegram.request import HTTPXRequest


# ============================================================
# CONFIG
# ============================================================

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
MONGO_URI = os.getenv("MONGO_URI", "")

DB_NAME = os.getenv("DB_NAME", "video_storage_bot")

SEND_DELAY = float(os.getenv("SEND_DELAY", "5"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "10"))

ALBUM_SIZE = 10

# How long this instance owns the polling lock
LOCK_TTL_SECONDS = 90

# Heartbeat interval
HEARTBEAT_SECONDS = 25

# Job lease
JOB_LEASE_SECONDS = 180


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


# ============================================================
# INSTANCE
# ============================================================

INSTANCE_ID = str(uuid.uuid4())

worker_task = None
heartbeat_task = None


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

async def ensure_index_by_keys(
    collection,
    keys,
    *,
    unique=False,
    name=None,
):
    """
    Create an index only when an equivalent index does not already exist.

    IMPORTANT:
    This prevents:
        IndexOptionsConflict
        Index already exists with a different name

    Existing indexes such as "unique_file" are preserved.
    """

    existing = await collection.index_information()

    wanted_keys = list(keys)

    for index_name, info in existing.items():
        existing_keys = list(info.get("key", []))

        if existing_keys == wanted_keys:
            existing_unique = bool(info.get("unique", False))

            if existing_unique == bool(unique):
                return index_name

            raise RuntimeError(
                f"Index exists with same keys but different unique option: "
                f"{index_name}"
            )

    kwargs = {
        "unique": unique,
    }

    if name:
        kwargs["name"] = name

    return await collection.create_index(
        keys,
        **kwargs,
    )


async def ensure_indexes():
    logger.info("Checking MongoDB indexes...")

    # DO NOT rename/drop existing unique_file index.
    await ensure_index_by_keys(
        videos_col,
        [("file_unique_id", ASCENDING)],
        unique=True,
        name="unique_file",
    )

    await ensure_index_by_keys(
        videos_col,
        [("seq", ASCENDING)],
        unique=False,
        name="seq_index",
    )

    await ensure_index_by_keys(
        videos_col,
        [("caption", ASCENDING)],
        unique=False,
        name="caption_index",
    )

    await ensure_index_by_keys(
        videos_col,
        [("created_at", DESCENDING)],
        unique=False,
        name="created_index",
    )

    await ensure_index_by_keys(
        videos_col,
        [("source_chat_id", ASCENDING)],
        unique=False,
        name="source_chat_index",
    )

    await ensure_index_by_keys(
        jobs_col,
        [("status", ASCENDING)],
        unique=False,
        name="job_status_index",
    )

    await ensure_index_by_keys(
        jobs_col,
        [("created_at", DESCENDING)],
        unique=False,
        name="job_created_index",
    )

    logger.info("MongoDB indexes ready.")


# ============================================================
# BOT INSTANCE LOCK
# ============================================================

async def acquire_instance_lock():
    """
    Allow only one polling instance.

    This prevents:
        Conflict: terminated by other getUpdates request
    """

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

        if not result:
            return False

        return result.get("instance_id") == INSTANCE_ID

    except DuplicateKeyError:
        return False


async def heartbeat_loop():
    while True:
        try:
            await asyncio.sleep(HEARTBEAT_SECONDS)

            now = utc_now()
            expires = utc_plus(LOCK_TTL_SECONDS)

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
                    "Instance lock was lost!"
                )

        except asyncio.CancelledError:
            break

        except Exception:
            logger.exception(
                "Heartbeat error"
            )


async def release_instance_lock():
    try:
        await settings_col.delete_one(
            {
                "_id": "bot_instance_lock",
                "instance_id": INSTANCE_ID,
            }
        )
    except Exception:
        logger.exception(
            "Failed to release instance lock"
        )


# ============================================================
# JOB RECOVERY ON STARTUP
# ============================================================

async def mark_stale_jobs_uncertain():
    """
    Detect jobs left running by a previous bot instance.

    IMPORTANT:
    If an active batch existed when the previous process stopped,
    preserve that batch as uncertain_batch.

    This allows /recover to safely decide whether to:
        - resend the interrupted batch
        - or skip it and continue from the next video
    """

    now = utc_now()

    cursor = jobs_col.find(
        {
            "status": "running",
            "$or": [
                {
                    "worker_id": {
                        "$ne": INSTANCE_ID
                    }
                },
                {
                    "lease_until": {
                        "$lt": now
                    }
                },
            ],
        }
    )

    count = 0

    async for job in cursor:

        current_batch = job.get(
            "current_batch"
        )

        update_data = {
            "status": "uncertain",

            "uncertain_reason": (
                "Previous worker stopped during an active batch. "
                "Album delivery status is unknown."
            ),

            "updated_at": now,
        }

        # Preserve the active batch
        if current_batch:
            update_data["uncertain_batch"] = {
                "first_seq": int(
                    current_batch["first_seq"]
                ),
                "last_seq": int(
                    current_batch["last_seq"]
                ),
                "count": int(
                    current_batch["count"]
                ),
            }

        await jobs_col.update_one(
            {
                "_id": job["_id"],
                "status": "running",
            },
            {
                "$set": update_data
            },
        )

        count += 1

    if count:
        logger.warning(
            "Marked %s stale job(s) as uncertain.",
            count,
        )


# ============================================================
# VIDEO STORAGE
# ============================================================

async def get_next_seq():
    result = await settings_col.find_one_and_update(
        {
            "_id": "video_counter",
        },
        {
            "$inc": {
                "value": 1,
            }
        },
        upsert=True,
        return_document=ReturnDocument.AFTER,
    )

    return int(result["value"])


async def save_video_message(message):
    if not message.video:
        return None

    video = message.video

    file_unique_id = video.file_unique_id

    # Existing duplicate check
    existing = await videos_col.find_one(
        {
            "file_unique_id": file_unique_id,
        },
        {
            "_id": 1,
            "seq": 1,
        },
    )

    if existing:
        logger.info(
            "Duplicate video ignored: %s",
            file_unique_id,
        )
        return existing.get("seq")

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

        logger.info(
            "Saved video #%s",
            seq,
        )

        return seq

    except DuplicateKeyError:
        # Another worker/process inserted the same video.
        existing = await videos_col.find_one(
            {
                "file_unique_id": file_unique_id,
            },
            {
                "seq": 1,
            },
        )

        if existing:
            return existing.get("seq")

        return None


# ============================================================
# AUTO SAVE VIDEO
# ============================================================

async def video_handler(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    if not update.message:
        return

    try:
        await save_video_message(
            update.message
        )

    except Exception:
        logger.exception(
            "Video save failed"
        )


# ============================================================
# STATS
# ============================================================

async def cmd_stats(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    if not update.effective_user:
        return

    total = await videos_col.count_documents({})

    last = await videos_col.find_one(
        {},
        sort=[("seq", DESCENDING)],
    )

    last_seq = last.get("seq", 0) if last else 0

    await update.message.reply_text(
        "📊 VIDEO STORAGE STATS\n\n"
        f"Videos: {total:,}\n"
        f"Last ID: {last_seq:,}"
    )


# ============================================================
# LIST
# ============================================================

async def cmd_list(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
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
            {
                "_id": 0,
                "seq": 1,
                "caption": 1,
                "file_unique_id": 1,
            },
        ).sort(
            "seq",
            ASCENDING,
        ).limit(limit)

        rows = []

        async for video in cursor:
            caption = video.get("caption") or ""

            if len(caption) > 50:
                caption = caption[:50] + "..."

            rows.append(
                f"#{video.get('seq')} "
                f"{caption}"
            )

        if not rows:
            await update.message.reply_text(
                "No videos found."
            )
            return

        await update.message.reply_text(
            "🎬 VIDEO LIST\n\n" +
            "\n".join(rows)
        )

    except Exception:
        logger.exception("List failed")

        await update.message.reply_text(
            "❌ Failed to list videos."
        )


# ============================================================
# SEARCH
# ============================================================

async def cmd_search(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    if not context.args:
        await update.message.reply_text(
            "Usage:\n/search keyword"
        )
        return

    text = " ".join(context.args)

    cursor = videos_col.find(
        {
            "$or": [
                {
                    "caption": {
                        "$regex": text,
                        "$options": "i",
                    }
                },
                {
                    "file_unique_id": {
                        "$regex": text,
                        "$options": "i",
                    }
                },
            ]
        },
        {
            "_id": 0,
            "seq": 1,
            "caption": 1,
        },
    ).sort(
        "seq",
        ASCENDING,
    ).limit(50)

    rows = []

    async for video in cursor:
        caption = video.get("caption") or ""

        if len(caption) > 80:
            caption = caption[:80] + "..."

        rows.append(
            f"#{video.get('seq')} {caption}"
        )

    if not rows:
        await update.message.reply_text(
            "No matching videos."
        )
        return

    await update.message.reply_text(
        "🔎 SEARCH RESULTS\n\n" +
        "\n".join(rows)
    )


# ============================================================
# SEND SINGLE VIDEO
# ============================================================

async def cmd_send(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
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
            "❌ Invalid ID."
        )
        return

    video = await videos_col.find_one(
        {
            "seq": seq,
        }
    )

    if not video:
        await update.message.reply_text(
            f"❌ Video #{seq} not found."
        )
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

        await update.message.reply_text(
            f"✅ Video #{seq} sent."
        )

    except Exception as e:
        logger.exception(
            "Single send failed"
        )

        await update.message.reply_text(
            f"❌ Send failed:\n{type(e).__name__}"
        )


# ============================================================
# GET NEXT BATCH
# ============================================================

async def get_next_batch(last_seq):
    cursor = (
        videos_col
        .find(
            {
                "seq": {
                    "$gt": last_seq
                }
            },
            {
                "_id": 0,
                "seq": 1,
                "file_id": 1,
                "caption": 1,
            },
        )
        .sort(
            "seq",
            ASCENDING,
        )
        .limit(ALBUM_SIZE)
    )

    return await cursor.to_list(
        length=ALBUM_SIZE
    )


# ============================================================
# CLAIM BATCH
# ============================================================

async def claim_batch(job_id, batch):
    if not batch:
        return False

    first_seq = int(batch[0]["seq"])
    last_batch_seq = int(batch[-1]["seq"])

    now = utc_now()
    lease = utc_plus(JOB_LEASE_SECONDS)

    result = await jobs_col.update_one(
        {
            "_id": job_id,
            "worker_id": INSTANCE_ID,
            "status": "running",
        },
        {
            "$set": {
                "current_batch": {
                    "first_seq": first_seq,
                    "last_seq": last_batch_seq,
                    "count": len(batch),
                },
                "batch_first_seq": first_seq,
                "batch_last_seq": last_batch_seq,
                "batch_count": len(batch),

                "batch_started_at": now,
                "lease_until": lease,

                "updated_at": now,
            }
        },
    )

    return result.modified_count == 1


# ============================================================
# SEND ALBUM
# ============================================================

async def send_album_once(
    bot,
    chat_id,
    batch,
):
    """
    Sends exactly one album.

    IMPORTANT:
    TimedOut / NetworkError is NOT automatically retried.

    Why?

    Telegram may have received the album even though
    Render did not receive the response.

    Retrying blindly can create duplicate albums.
    """

    media = []

    for video in batch:
        caption = video.get("caption") or None

        media.append(
            InputMediaVideo(
                media=video["file_id"],
                caption=caption,
            )
        )

    retry_count = 0

    while True:
        try:
            await bot.send_media_group(
                chat_id=chat_id,
                media=media,

                # Increased HTTP timeouts
                connect_timeout=30,
                read_timeout=180,
                write_timeout=180,
                pool_timeout=30,
            )

            return "sent"

        except RetryAfter as e:
            retry_count += 1

            if retry_count > MAX_RETRIES:
                raise

            retry_after = float(e.retry_after)

            logger.warning(
                "Telegram rate limit. Waiting %.2fs",
                retry_after,
            )

            await asyncio.sleep(
                retry_after + 1
            )

            # RetryAfter is safe to retry because
            # Telegram explicitly rejected/throttled
            # the request.

        except (TimedOut, NetworkError) as e:
            logger.error(
                "Network timeout/error during album send: %s",
                e,
            )

            return "uncertain"

        except TelegramError:
            logger.exception(
                "Telegram rejected album"
            )

            return "failed"

        except Exception:
            logger.exception(
                "Unexpected album error"
            )

            return "uncertain"


# ============================================================
# COMMIT SUCCESSFUL BATCH
# ============================================================

async def commit_batch(
    job_id,
    expected_last_seq,
    batch_last_seq,
):
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

                "lease_until": utc_plus(
                    JOB_LEASE_SECONDS
                ),
            },
            "$inc": {
                "sent": 1,
            },
        },
    )

    return result.modified_count == 1


# ============================================================
# MARK UNCERTAIN
# ============================================================

async def mark_job_uncertain(
    job_id,
    batch,
    reason,
):
    if not batch:
        return

    first_seq = int(batch[0]["seq"])
    last_seq = int(batch[-1]["seq"])

    await jobs_col.update_one(
        {
            "_id": job_id,
            "worker_id": INSTANCE_ID,
        },
        {
            "$set": {
                "status": "uncertain",

                "uncertain_batch": {
                    "first_seq": first_seq,
                    "last_seq": last_seq,
                    "count": len(batch),
                },

                "uncertain_reason": reason,

                "updated_at": utc_now(),
            }
        },
    )


# ============================================================
# JOB WORKER
# ============================================================

async def process_job(job):
    job_id = job["_id"]

    chat_id = int(job["chat_id"])

    logger.info(
        "Starting job %s for chat %s",
        job_id,
        chat_id,
    )

    while True:
        current = await jobs_col.find_one(
            {
                "_id": job_id,
                "worker_id": INSTANCE_ID,
            }
        )

        if not current:
            return

        status = current.get("status")

        if status == "cancelled":
            logger.info(
                "Job %s cancelled.",
                job_id,
            )
            return

        if status == "paused":
            logger.info(
                "Job %s paused.",
                job_id,
            )
            return

        if status == "uncertain":
            logger.warning(
                "Job %s is uncertain.",
                job_id,
            )
            return

        last_seq = int(
            current.get(
                "last_seq",
                0,
            )
        )

        batch = await get_next_batch(
            last_seq
        )

        if not batch:
            await jobs_col.update_one(
                {
                    "_id": job_id,
                    "worker_id": INSTANCE_ID,
                    "status": "running",
                },
                {
                    "$set": {
                        "status": "completed",
                        "completed_at": utc_now(),
                        "updated_at": utc_now(),
                        "current_batch": None,
                    }
                },
            )

            logger.info(
                "Job %s completed.",
                job_id,
            )

            return

        # Claim current batch
        claimed = await claim_batch(
            job_id,
            batch,
        )

        if not claimed:
            return

        first_seq = int(
            batch[0]["seq"]
        )

        batch_last_seq = int(
            batch[-1]["seq"]
        )

        logger.info(
            "Sending job %s batch %s-%s",
            job_id,
            first_seq,
            batch_last_seq,
        )

        result = await send_album_once(
            application.bot,
            chat_id,
            batch,
        )

        if result == "sent":

            committed = await commit_batch(
                job_id,
                last_seq,
                batch_last_seq,
            )

            if not committed:
                # Album definitely succeeded,
                # but database progress could not be committed.
                #
                # DO NOT resend.
                await mark_job_uncertain(
                    job_id,
                    batch,
                    (
                        "Album was sent successfully, "
                        "but progress commit failed. "
                        "Do not resend until recovery is confirmed."
                    ),
                )

                logger.error(
                    "Job %s commit failed after successful send.",
                    job_id,
                )

                return

            logger.info(
                "Job %s batch %s-%s completed.",
                job_id,
                first_seq,
                batch_last_seq,
            )

            if SEND_DELAY > 0:
                await asyncio.sleep(
                    SEND_DELAY
                )

            continue

        if result == "uncertain":

            await mark_job_uncertain(
                job_id,
                batch,
                (
                    "Network timeout/error after sending attempt. "
                    "Album status is uncertain. "
                    "Automatic retry disabled to prevent duplicates."
                ),
            )

            return

        if result == "failed":

            await jobs_col.update_one(
                {
                    "_id": job_id,
                    "worker_id": INSTANCE_ID,
                },
                {
                    "$inc": {
                        "failed": 1,
                    },
                    "$set": {
                        "status": "failed",
                        "updated_at": utc_now(),
                    }
                },
            )

            return


# ============================================================
# QUEUE WORKER
# ============================================================

async def queue_worker():
    logger.info(
        "Queue worker started."
    )

    while True:
        try:
            # Find a queued job
            now = utc_now()

            job = await jobs_col.find_one_and_update(
                {
                    "status": "queued",
                },
                {
                    "$set": {
                        "status": "running",
                        "worker_id": INSTANCE_ID,
                        "started_at": now,
                        "updated_at": now,
                        "lease_until": utc_plus(
                            JOB_LEASE_SECONDS
                        ),
                    }
                },
                sort=[
                    ("created_at", ASCENDING)
                ],
                return_document=ReturnDocument.AFTER,
            )

            if not job:
                await asyncio.sleep(1)
                continue

            logger.info(
                "Claimed job %s",
                job["_id"],
            )

            await process_job(
                job
            )

        except asyncio.CancelledError:
            break

        except Exception:
            logger.exception(
                "Queue worker error"
            )

            await asyncio.sleep(3)


# ============================================================
# SEND ALL
# ============================================================

async def cmd_sendall(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    if not context.args:
        await update.message.reply_text(
            "Usage:\n/sendall CHAT_ID"
        )
        return

    try:
        chat_id = int(
            context.args[0]
        )
    except ValueError:
        await update.message.reply_text(
            "❌ Invalid CHAT_ID."
        )
        return

    # Prevent multiple simultaneous sendall jobs
    active = await jobs_col.find_one(
        {
            "chat_id": chat_id,
            "status": {
                "$in": [
                    "queued",
                    "running",
                    "paused",
                    "uncertain",
                ]
            },
        },
        {
            "_id": 1,
            "status": 1,
        },
    )

    if active:
        await update.message.reply_text(
            "⚠️ This chat already has an active job.\n\n"
            f"Job: {active['_id']}\n"
            f"Status: {active['status']}\n\n"
            "Use /job to check it."
        )
        return

    first = await videos_col.find_one(
        {},
        {
            "seq": 1,
        },
        sort=[
            ("seq", ASCENDING)
        ],
    )

    if not first:
        await update.message.reply_text(
            "❌ No videos stored."
        )
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

    await jobs_col.insert_one(
        job
    )

    await update.message.reply_text(
        "🚀 SENDALL JOB CREATED\n\n"
        f"Job: {job_id}\n"
        f"Chat: {chat_id}\n"
        f"Videos snapshot: {job['snapshot']:,}\n\n"
        "10 videos per album.\n"
        "Use /job to monitor."
    )


# ============================================================
# JOB STATUS
# ============================================================

async def cmd_job(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    job = None

    if context.args:
        job = await jobs_col.find_one(
            {
                "_id": context.args[0]
            }
        )

    else:
        job = await jobs_col.find_one(
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

    current_batch = job.get(
        "current_batch"
    )

    if current_batch:
        text += (
            "\nCURRENT BATCH\n"
            f"From: {current_batch.get('first_seq')}\n"
            f"To: {current_batch.get('last_seq')}\n"
            f"Count: {current_batch.get('count')}\n"
        )

    uncertain = job.get(
        "uncertain_batch"
    )

    if uncertain:
        text += (
            "\n⚠️ UNCERTAIN BATCH\n"
            f"From: {uncertain.get('first_seq')}\n"
            f"To: {uncertain.get('last_seq')}\n"
            f"Count: {uncertain.get('count')}\n"
            f"Reason: {job.get('uncertain_reason')}\n"
        )

    await update.message.reply_text(
        text
    )


# ============================================================
# PAUSE
# ============================================================

async def cmd_pause(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    job = await jobs_col.find_one(
        {
            "status": "running",
        },
        sort=[
            ("created_at", DESCENDING)
        ],
    )

    if not job:
        await update.message.reply_text(
            "No running job."
        )
        return

    await jobs_col.update_one(
        {
            "_id": job["_id"],
            "status": "running",
        },
        {
            "$set": {
                "status": "paused",
                "updated_at": utc_now(),
            }
        },
    )

    await update.message.reply_text(
        f"⏸ Job paused.\n\n"
        f"Job: {job['_id']}"
    )


# ============================================================
# RESUME
# ============================================================

async def cmd_resume(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    job = await jobs_col.find_one(
        {
            "status": "paused",
        },
        sort=[
            ("created_at", DESCENDING)
        ],
    )

    if not job:
        await update.message.reply_text(
            "No paused job found."
        )
        return

    await jobs_col.update_one(
        {
            "_id": job["_id"],
            "status": "paused",
        },
        {
            "$set": {
                "status": "queued",
                "worker_id": None,
                "updated_at": utc_now(),
            }
        },
    )

    await update.message.reply_text(
        f"▶️ Job resumed.\n\n"
        f"Job: {job['_id']}"
    )


# ============================================================
# CANCEL
# ============================================================

async def cmd_cancel(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    job = None

    if context.args:
        job = await jobs_col.find_one(
            {
                "_id": context.args[0]
            }
        )
    else:
        job = await jobs_col.find_one(
            {
                "status": {
                    "$in": [
                        "queued",
                        "running",
                        "paused",
                        "uncertain",
                    ]
                }
            },
            sort=[
                ("created_at", DESCENDING)
            ],
        )

    if not job:
        await update.message.reply_text(
            "No active job found."
        )
        return

    await jobs_col.update_one(
        {
            "_id": job["_id"],
            "status": {
                "$in": [
                    "queued",
                    "running",
                    "paused",
                    "uncertain",
                ]
            },
        },
        {
            "$set": {
                "status": "cancelled",
                "updated_at": utc_now(),
            }
        },
    )

    await update.message.reply_text(
        f"🛑 Job cancelled.\n\n"
        f"Job: {job['_id']}"
    )


# ============================================================
# RECOVER UNCERTAIN JOB
# ============================================================

async def cmd_recover(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    """
    Safe recovery:

    /recover JOB_ID 0

    Means:
        First uncertain batch was NOT sent.
        Start it again.

    Example:
        uncertain batch = 1-10

        /recover JOB_ID 0

    -> next batch = 1-10


    /recover JOB_ID 10

    Means:
        1-10 were definitely sent.

    -> next batch = 11-20
    """

    if len(context.args) < 2:
        await update.message.reply_text(
            "Usage:\n\n"
            "/recover JOB_ID SENT_LAST_SEQ\n\n"
            "Examples:\n"
            "/recover JOB_ID 0\n"
            "/recover JOB_ID 10"
        )
        return

    job_id = context.args[0]

    try:
        confirmed_last_seq = int(
            context.args[1]
        )
    except ValueError:
        await update.message.reply_text(
            "❌ SENT_LAST_SEQ must be a number."
        )
        return

    job = await jobs_col.find_one(
        {
            "_id": job_id,
        }
    )

    if not job:
        await update.message.reply_text(
            "❌ Job not found."
        )
        return

    if job.get("status") != "uncertain":
        await update.message.reply_text(
            "❌ Job is not uncertain.\n\n"
            f"Current status: {job.get('status')}"
        )
        return

    uncertain = job.get(
        "uncertain_batch"
    )

    if not uncertain:
        await update.message.reply_text(
            "❌ No uncertain batch information found."
        )
        return

    first_seq = int(
        uncertain["first_seq"]
    )

    last_seq = int(
        uncertain["last_seq"]
    )

    if confirmed_last_seq < first_seq - 1:
        await update.message.reply_text(
            "❌ Invalid recovery value.\n\n"
            f"Allowed:\n"
            f"{first_seq - 1} = batch NOT sent\n"
            f"{last_seq} = batch definitely sent"
        )
        return

    if confirmed_last_seq > last_seq:
        await update.message.reply_text(
            "❌ Recovery value cannot exceed "
            "the uncertain batch's last ID."
        )
        return

    # For a 10-video batch, only these two states
    # make sense.
    if confirmed_last_seq not in (
        first_seq - 1,
        last_seq,
    ):
        await update.message.reply_text(
            "❌ For safety, you can only recover to:\n\n"
            f"{first_seq - 1}\n"
            f"or\n"
            f"{last_seq}"
        )
        return

    await jobs_col.update_one(
        {
            "_id": job_id,
            "status": "uncertain",
        },
        {
            "$set": {
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
            }
        },
    )

    if confirmed_last_seq == first_seq - 1:
        message = (
            f"▶️ Recovery complete.\n\n"
            f"Job: {job_id}\n"
            f"Will resend: {first_seq}-{last_seq}"
        )
    else:
        message = (
            f"▶️ Recovery complete.\n\n"
            f"Job: {job_id}\n"
            f"Confirmed sent: {first_seq}-{last_seq}\n"
            f"Next: {last_seq + 1}"
        )

    await update.message.reply_text(
        message
    )


# ============================================================
# DELETE SINGLE VIDEO
# ============================================================

async def cmd_delete(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    if not context.args:
        await update.message.reply_text(
            "Usage:\n/delete VIDEO_ID"
        )
        return

    try:
        seq = int(
            context.args[0]
        )
    except ValueError:
        await update.message.reply_text(
            "❌ Invalid video ID."
        )
        return

    result = await videos_col.delete_one(
        {
            "seq": seq
        }
    )

    if result.deleted_count:
        await update.message.reply_text(
            f"🗑 Video #{seq} deleted."
        )
    else:
        await update.message.reply_text(
            f"❌ Video #{seq} not found."
        )


# ============================================================
# HELP
# ============================================================

async def cmd_start(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    await update.message.reply_text(
        "🎬 VIDEO STORAGE BOT\n\n"
        "Commands:\n\n"
        "/stats - Statistics\n"
        "/list [limit] - List videos\n"
        "/search TEXT - Search\n"
        "/send ID CHAT_ID - Send one video\n"
        "/sendall CHAT_ID - Send all videos\n"
        "/job - Current job\n"
        "/pause - Pause job\n"
        "/resume - Resume paused job\n"
        "/cancel [JOB_ID] - Cancel job\n"
        "/recover JOB_ID SEQ - Recover uncertain job\n"
        "/delete ID - Delete one video\n\n"
        "Videos are automatically saved."
    )


# ============================================================
# ERROR HANDLER
# ============================================================

async def error_handler(
    update: object,
    context: ContextTypes.DEFAULT_TYPE,
):
    logger.error(
        "Telegram error: %s",
        context.error,
    )


# ============================================================
# APPLICATION
# ============================================================

application = None


def build_application():
    global application

    # IMPORTANT:
    # Larger timeouts for large/slow media-group operations.
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

    # Commands
    application.add_handler(
        CommandHandler(
            "start",
            cmd_start,
        )
    )

    application.add_handler(
        CommandHandler(
            "stats",
            cmd_stats,
        )
    )

    application.add_handler(
        CommandHandler(
            "list",
            cmd_list,
        )
    )

    application.add_handler(
        CommandHandler(
            "search",
            cmd_search,
        )
    )

    application.add_handler(
        CommandHandler(
            "send",
            cmd_send,
        )
    )

    application.add_handler(
        CommandHandler(
            "sendall",
            cmd_sendall,
        )
    )

    application.add_handler(
        CommandHandler(
            "job",
            cmd_job,
        )
    )

    application.add_handler(
        CommandHandler(
            "pause",
            cmd_pause,
        )
    )

    application.add_handler(
        CommandHandler(
            "resume",
            cmd_resume,
        )
    )

    application.add_handler(
        CommandHandler(
            "cancel",
            cmd_cancel,
        )
    )

    application.add_handler(
        CommandHandler(
            "recover",
            cmd_recover,
        )
    )

    application.add_handler(
        CommandHandler(
            "delete",
            cmd_delete,
        )
    )

    # Auto-save videos
    application.add_handler(
        MessageHandler(
            filters.VIDEO,
            video_handler,
        )
    )

    application.add_error_handler(
        error_handler
    )

    return application


# ============================================================
# MAIN
# ============================================================

async def main():
    global heartbeat_task
    global worker_task

    logger.info(
        "Starting Video Storage Bot..."
    )

    await mongo.admin.command(
        "ping"
    )

    logger.info(
        "MongoDB connection OK."
    )

    await ensure_indexes()

    # Only one Render worker should own polling.
    acquired = await acquire_instance_lock()

    if not acquired:
        logger.error(
            "Another bot instance is already running. "
            "Stopping this instance."
        )

        await mongo.close()

        return

    logger.info(
        "Instance lock acquired: %s",
        INSTANCE_ID,
    )

    await mark_stale_jobs_uncertain()

    build_application()

    heartbeat_task = asyncio.create_task(
        heartbeat_loop()
    )

    worker_task = asyncio.create_task(
        queue_worker()
    )

    try:
        await application.initialize()

        # Keep pending updates.
        await application.bot.delete_webhook(
            drop_pending_updates=False
        )

        await application.start()

        await application.updater.start_polling(
            allowed_updates=Update.ALL_TYPES,
            drop_pending_updates=False,
        )

        logger.info(
            "Bot polling started successfully."
        )

        # Keep process alive
        await asyncio.Event().wait()

    except asyncio.CancelledError:
        logger.info(
            "Main task cancelled."
        )

    except Exception:
        logger.exception(
            "Fatal application error"
        )

    finally:

        logger.info(
            "Stopping bot..."
        )

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

        try:
            if application and application.updater:
                await application.updater.stop()
        except Exception:
            logger.exception(
                "Updater stop error"
            )

        try:
            if application:
                await application.stop()
        except Exception:
            logger.exception(
                "Application stop error"
            )

        try:
            if application:
                await application.shutdown()
        except Exception:
            logger.exception(
                "Application shutdown error"
            )

        await release_instance_lock()

        mongo.close()

        logger.info(
            "Bot stopped."
        )


# ============================================================
# ENTRY
# ============================================================

if __name__ == "__main__":
    asyncio.run(main())
