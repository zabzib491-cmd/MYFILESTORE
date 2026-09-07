import os
import asyncio
import logging
import uuid
from datetime import datetime, timezone, timedelta

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

ADMIN_IDS = {
    int(x.strip())
    for x in os.getenv("ADMIN_IDS", "").split(",")
    if x.strip()
}

SEND_DELAY = float(os.getenv("SEND_DELAY", "5"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "10"))

ALBUM_SIZE = 10

DB_NAME = "video_storage_bot"

INSTANCE_ID = str(uuid.uuid4())

LOCK_ID = "bot_instance_lock"
LOCK_TTL_SECONDS = 90
HEARTBEAT_SECONDS = 25

JOB_LEASE_SECONDS = 120


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)

logger = logging.getLogger("VideoStorageBot")


# ============================================================
# MONGODB
# ============================================================

mongo_client = None
db = None

videos_col = None
jobs_col = None
settings_col = None


# ============================================================
# GLOBAL TASKS
# ============================================================

worker_task = None
heartbeat_task = None

shutdown_event = asyncio.Event()


# ============================================================
# HELPERS
# ============================================================

def now_utc():
    return datetime.now(timezone.utc)


def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


async def require_admin(update: Update) -> bool:
    if not update.effective_user:
        return False

    if not is_admin(update.effective_user.id):
        if update.effective_message:
            await update.effective_message.reply_text(
                "❌ You are not authorized to use this command."
            )
        return False

    return True


# ============================================================
# DATABASE INDEX MANAGEMENT
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
    Existing indexes are NEVER dropped or renamed.
    This prevents:
        IndexOptionsConflict
        Index already exists with a different name
    """

    existing = await collection.index_information()

    wanted_keys = list(keys)

    for index_name, info in existing.items():
        existing_keys = list(info.get("key", []))

        if existing_keys == wanted_keys:
            existing_unique = bool(info.get("unique", False))

            if existing_unique == bool(unique):
                logger.info(
                    "INDEX OK | collection=%s | name=%s | keys=%s",
                    collection.name,
                    index_name,
                    wanted_keys,
                )
                return index_name

            raise RuntimeError(
                f"Existing index '{index_name}' on "
                f"{collection.name} has incompatible unique setting."
            )

    kwargs = {
        "unique": unique,
    }

    if name:
        kwargs["name"] = name

    created = await collection.create_index(
        keys,
        **kwargs,
    )

    logger.info(
        "INDEX CREATED | collection=%s | name=%s | keys=%s",
        collection.name,
        created,
        wanted_keys,
    )

    return created


async def init_db():
    global mongo_client
    global db
    global videos_col
    global jobs_col
    global settings_col

    if not MONGO_URI:
        raise RuntimeError("MONGO_URI is missing.")

    mongo_client = AsyncIOMotorClient(
        MONGO_URI,
        serverSelectionTimeoutMS=15000,
        connectTimeoutMS=15000,
        socketTimeoutMS=30000,
        retryWrites=True,
    )

    await mongo_client.admin.command("ping")

    db = mongo_client[DB_NAME]

    videos_col = db["videos"]
    jobs_col = db["jobs"]
    settings_col = db["settings"]

    # --------------------------------------------------------
    # VIDEOS
    # --------------------------------------------------------

    # Existing unique_file index will be detected and reused.
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

    # --------------------------------------------------------
    # JOBS
    # --------------------------------------------------------

    await ensure_index_by_keys(
        jobs_col,
        [
            ("status", ASCENDING),
            ("created_at", ASCENDING),
        ],
        unique=False,
        name="job_status_created",
    )

    await ensure_index_by_keys(
        jobs_col,
        [("worker_id", ASCENDING)],
        unique=False,
        name="job_worker",
    )

    logger.info("DATABASE READY")


# ============================================================
# SINGLE INSTANCE LOCK
# ============================================================

async def acquire_instance_lock() -> bool:
    current = now_utc()
    expires = current + timedelta(seconds=LOCK_TTL_SECONDS)

    result = await settings_col.find_one_and_update(
        {
            "_id": LOCK_ID,
            "$or": [
                {
                    "expires_at": {
                        "$lt": current
                    }
                },
                {
                    "instance_id": INSTANCE_ID
                },
            ],
        },
        {
            "$set": {
                "instance_id": INSTANCE_ID,
                "started_at": current,
                "heartbeat_at": current,
                "expires_at": expires,
            }
        },
        upsert=True,
        return_document=ReturnDocument.AFTER,
    )

    if not result:
        logger.error(
            "ANOTHER BOT INSTANCE IS ALREADY RUNNING."
        )
        return False

    if result.get("instance_id") != INSTANCE_ID:
        logger.error(
            "ANOTHER BOT INSTANCE IS ALREADY RUNNING."
        )
        return False

    logger.info(
        "INSTANCE LOCK ACQUIRED | instance=%s",
        INSTANCE_ID,
    )

    return True


async def heartbeat_instance_lock():
    while not shutdown_event.is_set():
        try:
            current = now_utc()
            expires = current + timedelta(
                seconds=LOCK_TTL_SECONDS
            )

            result = await settings_col.update_one(
                {
                    "_id": LOCK_ID,
                    "instance_id": INSTANCE_ID,
                },
                {
                    "$set": {
                        "heartbeat_at": current,
                        "expires_at": expires,
                    }
                },
            )

            if result.modified_count == 0:
                logger.error(
                    "INSTANCE LOCK LOST!"
                )

        except Exception:
            logger.exception(
                "Heartbeat error"
            )

        await asyncio.sleep(
            HEARTBEAT_SECONDS
        )


async def release_instance_lock():
    try:
        await settings_col.delete_one(
            {
                "_id": LOCK_ID,
                "instance_id": INSTANCE_ID,
            }
        )

        logger.info(
            "INSTANCE LOCK RELEASED"
        )

    except Exception:
        logger.exception(
            "Failed to release instance lock"
        )


# ============================================================
# STALE JOB PROTECTION
# ============================================================

async def mark_stale_jobs_uncertain():
    """
    NEVER automatically re-queue a running job after restart.

    The previous process may already have sent the album.
    Re-queueing would cause duplicates.

    Therefore running jobs from a previous process become:
        uncertain
    """

    result = await jobs_col.update_many(
        {
            "status": "running",
            "$or": [
                {
                    "worker_id": {
                        "$ne": INSTANCE_ID
                    }
                },
                {
                    "worker_id": None
                },
            ],
        },
        {
            "$set": {
                "status": "uncertain",
                "uncertain_at": now_utc(),
                "uncertain_reason": (
                    "Previous worker stopped or restarted. "
                    "Last album may have already been delivered."
                ),
            }
        },
    )

    if result.modified_count:
        logger.warning(
            "STALE JOBS MARKED UNCERTAIN | count=%s",
            result.modified_count,
        )


# ============================================================
# VIDEO STORAGE
# ============================================================

async def get_next_sequence():
    """
    Atomically generates next sequence number.
    """

    result = await settings_col.find_one_and_update(
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

    return int(result["value"])


async def save_video_message(message):
    if not message or not message.video:
        return None

    video = message.video

    file_unique_id = video.file_unique_id

    if not file_unique_id:
        return None

    # Fast duplicate check
    existing = await videos_col.find_one(
        {
            "file_unique_id": file_unique_id
        },
        {
            "seq": 1,
        },
    )

    if existing:
        logger.info(
            "DUPLICATE VIDEO SKIPPED | seq=%s",
            existing.get("seq"),
        )
        return existing.get("seq")

    seq = await get_next_sequence()

    document = {
        "seq": seq,
        "file_id": video.file_id,
        "file_unique_id": file_unique_id,

        "width": video.width,
        "height": video.height,
        "duration": video.duration,

        "file_size": getattr(
            video,
            "file_size",
            None,
        ),

        "caption": message.caption,

        "source_chat_id": (
            message.chat_id
            if message.chat_id
            else None
        ),

        "source_message_id": (
            message.message_id
            if message.message_id
            else None
        ),

        "created_at": now_utc(),
    }

    try:
        await videos_col.insert_one(
            document
        )

        logger.info(
            "VIDEO SAVED | seq=%s | unique_id=%s",
            seq,
            file_unique_id,
        )

        return seq

    except DuplicateKeyError:
        # Another process/request inserted same file.
        existing = await videos_col.find_one(
            {
                "file_unique_id": file_unique_id
            },
            {
                "seq": 1,
            },
        )

        logger.info(
            "DUPLICATE RACE DETECTED | existing=%s",
            existing.get("seq") if existing else None,
        )

        return (
            existing.get("seq")
            if existing
            else None
        )


# ============================================================
# VIDEO MESSAGE HANDLER
# ============================================================

async def video_handler(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    try:
        message = update.effective_message

        if not message or not message.video:
            return

        await save_video_message(
            message
        )

    except Exception:
        logger.exception(
            "VIDEO SAVE ERROR"
        )


# ============================================================
# /START
# ============================================================

async def start_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    await update.effective_message.reply_text(
        "🎬 Video Storage Bot\n\n"
        "Videos are automatically stored.\n\n"
        "Admin commands:\n"
        "/stats\n"
        "/list\n"
        "/search TEXT\n"
        "/send ID CHAT_ID\n"
        "/sendall CHAT_ID\n"
        "/delete ID\n"
        "/job\n"
        "/pause\n"
        "/resume\n"
        "/cancel"
    )


# ============================================================
# /STATS
# ============================================================

async def stats_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    if not await require_admin(update):
        return

    total = await videos_col.count_documents({})

    queued = await jobs_col.count_documents(
        {
            "status": "queued"
        }
    )

    running = await jobs_col.count_documents(
        {
            "status": "running"
        }
    )

    paused = await jobs_col.count_documents(
        {
            "status": "paused"
        }
    )

    uncertain = await jobs_col.count_documents(
        {
            "status": "uncertain"
        }
    )

    cancelled = await jobs_col.count_documents(
        {
            "status": "cancelled"
        }
    )

    await update.effective_message.reply_text(
        "📊 VIDEO STORAGE STATS\n\n"
        f"🎬 Videos: {total}\n"
        f"🟡 Queued jobs: {queued}\n"
        f"🟢 Running jobs: {running}\n"
        f"⏸ Paused jobs: {paused}\n"
        f"⚠️ Uncertain jobs: {uncertain}\n"
        f"🔴 Cancelled jobs: {cancelled}"
    )


# ============================================================
# /LIST
# ============================================================

async def list_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    if not await require_admin(update):
        return

    limit = 20

    if context.args:
        try:
            limit = min(
                max(
                    int(context.args[0]),
                    1,
                ),
                100,
            )
        except ValueError:
            pass

    cursor = (
        videos_col
        .find(
            {},
            {
                "seq": 1,
                "caption": 1,
                "created_at": 1,
            },
        )
        .sort(
            "seq",
            ASCENDING,
        )
        .limit(limit)
    )

    rows = await cursor.to_list(
        length=limit
    )

    if not rows:
        await update.effective_message.reply_text(
            "No videos found."
        )
        return

    text = "🎬 VIDEO LIST\n\n"

    for item in rows:
        seq = item.get("seq")
        caption = item.get("caption") or ""

        if len(caption) > 40:
            caption = caption[:40] + "..."

        text += (
            f"ID: {seq}"
            f"{' | ' + caption if caption else ''}\n"
        )

    await update.effective_message.reply_text(
        text
    )


# ============================================================
# /SEARCH
# ============================================================

async def search_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    if not await require_admin(update):
        return

    if not context.args:
        await update.effective_message.reply_text(
            "Usage:\n/search text"
        )
        return

    query = " ".join(
        context.args
    )

    cursor = (
        videos_col
        .find(
            {
                "caption": {
                    "$regex": query,
                    "$options": "i",
                }
            },
            {
                "seq": 1,
                "caption": 1,
            },
        )
        .sort(
            "seq",
            ASCENDING,
        )
        .limit(50)
    )

    rows = await cursor.to_list(
        length=50
    )

    if not rows:
        await update.effective_message.reply_text(
            "❌ No matching videos."
        )
        return

    text = "🔎 SEARCH RESULTS\n\n"

    for item in rows:
        text += (
            f"ID: {item.get('seq')}\n"
            f"{item.get('caption') or '(no caption)'}\n\n"
        )

    await update.effective_message.reply_text(
        text
    )


# ============================================================
# /SEND SINGLE VIDEO
# ============================================================

async def send_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    if not await require_admin(update):
        return

    if len(context.args) != 2:
        await update.effective_message.reply_text(
            "Usage:\n/send VIDEO_ID CHAT_ID"
        )
        return

    try:
        seq = int(context.args[0])
        chat_id = int(context.args[1])
    except ValueError:
        await update.effective_message.reply_text(
            "❌ Invalid ID or CHAT_ID."
        )
        return

    video = await videos_col.find_one(
        {
            "seq": seq
        }
    )

    if not video:
        await update.effective_message.reply_text(
            "❌ Video not found."
        )
        return

    try:
        await context.bot.send_video(
            chat_id=chat_id,
            video=video["file_id"],
            caption=video.get("caption"),
        )

        await update.effective_message.reply_text(
            f"✅ Video {seq} sent."
        )

    except TelegramError as e:
        await update.effective_message.reply_text(
            f"❌ Send failed:\n{e}"
        )


# ============================================================
# DESTINATION CHECK
# ============================================================

async def verify_destination(
    bot,
    chat_id: int,
):
    chat = await bot.get_chat(
        chat_id
    )

    logger.info(
        "DESTINATION OK | id=%s | type=%s | title=%s",
        chat_id,
        chat.type,
        getattr(
            chat,
            "title",
            None,
        ),
    )

    return chat


# ============================================================
# CREATE SENDALL JOB
# ============================================================

async def create_send_job(
    chat_id: int,
):
    total = await videos_col.count_documents({})

    if total == 0:
        return None

    job_id = uuid.uuid4().hex

    job = {
        "_id": job_id,

        "type": "sendall",

        "chat_id": chat_id,

        "status": "queued",

        "created_at": now_utc(),
        "updated_at": now_utc(),

        "worker_id": None,

        "last_seq": 0,

        "sent": 0,
        "failed": 0,

        "total_snapshot": total,

        "batch_first_seq": None,
        "batch_last_seq": None,
        "batch_count": 0,

        "uncertain_first_seq": None,
        "uncertain_last_seq": None,

        "error": None,
    }

    await jobs_col.insert_one(
        job
    )

    return job_id


# ============================================================
# CLAIM JOB
# ============================================================

async def claim_job():
    current = now_utc()
    lease_until = current + timedelta(
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
                "claimed_at": current,
                "lease_until": lease_until,
                "updated_at": current,
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

    return result


# ============================================================
# GET NEXT BATCH
# ============================================================

async def get_next_batch(
    last_seq: int,
):
    cursor = (
        videos_col
        .find(
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

    return await cursor.to_list(
        length=ALBUM_SIZE
    )


# ============================================================
# CLAIM CURRENT BATCH
# ============================================================

async def claim_batch(
    job_id: str,
    last_seq: int,
    batch,
):
    if not batch:
        return True

    first_seq = batch[0]["seq"]
    last_batch_seq = batch[-1]["seq"]

    result = await jobs_col.update_one(
        {
            "_id": job_id,
            "status": "running",
            "worker_id": INSTANCE_ID,
            "last_seq": last_seq,
        },
        {
            "$set": {
                "batch_first_seq": first_seq,
                "batch_last_seq": last_batch_seq,
                "batch_count": len(batch),
                "updated_at": now_utc(),
            }
        },
    )

    if result.modified_count != 1:
        logger.error(
            "BATCH CLAIM FAILED | job=%s",
            job_id,
        )
        return False

    logger.info(
        "BATCH READY | job=%s | IDs=%s-%s | count=%s",
        job_id,
        first_seq,
        last_batch_seq,
        len(batch),
    )

    return True


# ============================================================
# SEND ALBUM
# ============================================================

async def send_album_once(
    bot,
    chat_id: int,
    batch,
    job_id: str,
):
    media = []

    for video in batch:
        media.append(
            InputMediaVideo(
                media=video["file_id"],
                caption=video.get("caption"),
            )
        )

    logger.info(
        "SENDING ALBUM | job=%s | IDs=%s-%s | count=%s",
        job_id,
        batch[0]["seq"],
        batch[-1]["seq"],
        len(batch),
    )

    retry_count = 0

    while True:
        try:
            await bot.send_media_group(
                chat_id=chat_id,
                media=media,
            )

            logger.info(
                "ALBUM SENT SUCCESSFULLY | job=%s | IDs=%s-%s",
                job_id,
                batch[0]["seq"],
                batch[-1]["seq"],
            )

            return True, None

        except RetryAfter as e:
            retry_count += 1

            if retry_count > MAX_RETRIES:
                logger.error(
                    "RetryAfter exceeded | job=%s",
                    job_id,
                )

                return False, (
                    "RetryAfter exceeded maximum retries."
                )

            wait_time = int(
                getattr(
                    e,
                    "retry_after",
                    5,
                )
            ) + 1

            logger.warning(
                "RATE LIMIT | retry=%s/%s | wait=%ss | job=%s",
                retry_count,
                MAX_RETRIES,
                wait_time,
                job_id,
            )

            await asyncio.sleep(
                wait_time
            )

        except (TimedOut, NetworkError) as e:
            # =================================================
            # VERY IMPORTANT
            #
            # DO NOT RETRY THIS ALBUM.
            #
            # Telegram may have accepted the request even
            # though our connection timed out.
            #
            # Retrying can duplicate the same 10 videos.
            # =================================================

            logger.error(
                "AMBIGUOUS NETWORK ERROR | job=%s | IDs=%s-%s | error=%s",
                job_id,
                batch[0]["seq"],
                batch[-1]["seq"],
                type(e).__name__,
            )

            return False, (
                "Network timeout/error after sending attempt. "
                "Album status is uncertain. "
                "Automatic retry intentionally disabled."
            )

        except TelegramError as e:
            logger.error(
                "TELEGRAM ERROR | job=%s | error=%s",
                job_id,
                e,
            )

            return False, str(e)

        except Exception as e:
            logger.exception(
                "UNKNOWN SEND ERROR | job=%s",
                job_id,
            )

            return False, str(e)


# ============================================================
# MARK JOB UNCERTAIN
# ============================================================

async def mark_job_uncertain(
    job_id: str,
    batch,
    reason: str,
):
    if not batch:
        return

    await jobs_col.update_one(
        {
            "_id": job_id,
            "worker_id": INSTANCE_ID,
        },
        {
            "$set": {
                "status": "uncertain",

                "uncertain_first_seq": batch[0]["seq"],
                "uncertain_last_seq": batch[-1]["seq"],

                "uncertain_at": now_utc(),

                "uncertain_reason": reason,

                "updated_at": now_utc(),
            }
        },
    )

    logger.warning(
        "JOB UNCERTAIN | job=%s | IDs=%s-%s",
        job_id,
        batch[0]["seq"],
        batch[-1]["seq"],
    )


# ============================================================
# COMMIT SUCCESSFUL BATCH
# ============================================================

async def commit_batch(
    job,
    batch,
):
    if not batch:
        return False

    job_id = job["_id"]

    old_last_seq = job.get(
        "last_seq",
        0,
    )

    new_last_seq = batch[-1]["seq"]

    current_job = await jobs_col.find_one(
        {
            "_id": job_id,
            "worker_id": INSTANCE_ID,
        }
    )

    if not current_job:
        return False

    current_status = current_job.get(
        "status"
    )

    # If another mechanism already marked it uncertain,
    # never overwrite it.
    if current_status == "uncertain":
        return False

    if current_status not in {
        "running",
        "paused",
        "cancelled",
    }:
        return False

    sent_count = int(
        current_job.get(
            "sent",
            0,
        )
    )

    sent_count += len(batch)

    next_status = current_status

    # If paused/cancelled while the album was in flight,
    # still commit the album because it was successfully sent.
    result = await jobs_col.update_one(
        {
            "_id": job_id,

            "worker_id": INSTANCE_ID,

            "last_seq": old_last_seq,

            "batch_first_seq": batch[0]["seq"],
            "batch_last_seq": batch[-1]["seq"],
        },
        {
            "$set": {
                "last_seq": new_last_seq,

                "sent": sent_count,

                "batch_first_seq": None,
                "batch_last_seq": None,
                "batch_count": 0,

                "updated_at": now_utc(),

                "status": next_status,
            }
        },
    )

    if result.modified_count != 1:
        logger.error(
            "COMMIT FAILED AFTER SUCCESSFUL SEND | job=%s | IDs=%s-%s",
            job_id,
            batch[0]["seq"],
            batch[-1]["seq"],
        )

        return False

    logger.info(
        "PROGRESS COMMITTED | job=%s | last_seq=%s | sent=%s",
        job_id,
        new_last_seq,
        sent_count,
    )

    return True


# ============================================================
# PROCESS JOB
# ============================================================

async def process_job(
    bot,
    job,
):
    job_id = job["_id"]

    chat_id = job["chat_id"]

    last_seq = int(
        job.get(
            "last_seq",
            0,
        )
    )

    while True:

        # --------------------------------------------
        # Reload current job state
        # --------------------------------------------

        current_job = await jobs_col.find_one(
            {
                "_id": job_id
            }
        )

        if not current_job:
            return

        status = current_job.get(
            "status"
        )

        if status in {
            "cancelled",
            "uncertain",
        }:
            logger.warning(
                "JOB STOPPED | job=%s | status=%s",
                job_id,
                status,
            )
            return

        if status == "paused":
            logger.info(
                "JOB PAUSED | job=%s",
                job_id,
            )
            return

        last_seq = int(
            current_job.get(
                "last_seq",
                last_seq,
            )
        )

        # --------------------------------------------
        # Get next 10 videos
        # --------------------------------------------

        logger.info(
            "GET NEXT BATCH | job=%s | last_seq=%s",
            job_id,
            last_seq,
        )

        batch = await get_next_batch(
            last_seq
        )

        # --------------------------------------------
        # No more videos
        # --------------------------------------------

        if not batch:
            await jobs_col.update_one(
                {
                    "_id": job_id,
                    "worker_id": INSTANCE_ID,
                    "status": {
                        "$in": [
                            "running",
                            "paused",
                        ]
                    },
                },
                {
                    "$set": {
                        "status": "completed",
                        "updated_at": now_utc(),
                        "completed_at": now_utc(),
                        "batch_first_seq": None,
                        "batch_last_seq": None,
                        "batch_count": 0,
                    }
                },
            )

            logger.info(
                "JOB COMPLETED | job=%s",
                job_id,
            )

            return

        # --------------------------------------------
        # Claim batch
        # --------------------------------------------

        claimed = await claim_batch(
            job_id,
            last_seq,
            batch,
        )

        if not claimed:
            await mark_job_uncertain(
                job_id,
                batch,
                "Could not safely claim batch.",
            )
            return

        # --------------------------------------------
        # Send album
        # --------------------------------------------

        success, error = await send_album_once(
            bot,
            chat_id,
            batch,
            job_id,
        )

        # --------------------------------------------
        # NETWORK / AMBIGUOUS FAILURE
        # --------------------------------------------

        if not success:

            await mark_job_uncertain(
                job_id,
                batch,
                error or "Unknown send failure.",
            )

            return

        # --------------------------------------------
        # Successful send → commit progress
        # --------------------------------------------

        committed = await commit_batch(
            current_job,
            batch,
        )

        if not committed:

            await mark_job_uncertain(
                job_id,
                batch,
                (
                    "Album was successfully sent, "
                    "but MongoDB progress commit failed. "
                    "Resending is disabled to prevent duplicates."
                ),
            )

            return

        # --------------------------------------------
        # Delay between albums
        # --------------------------------------------

        if SEND_DELAY > 0:
            await asyncio.sleep(
                SEND_DELAY
            )


# ============================================================
# QUEUE WORKER
# ============================================================

async def send_queue_worker(bot):
    logger.info(
        "SEND QUEUE WORKER STARTED"
    )

    while not shutdown_event.is_set():

        try:
            job = await claim_job()

            if not job:
                await asyncio.sleep(
                    2
                )
                continue

            logger.info(
                "JOB CLAIMED | job=%s | chat=%s",
                job["_id"],
                job["chat_id"],
            )

            await process_job(
                bot,
                job,
            )

        except asyncio.CancelledError:
            raise

        except Exception:
            logger.exception(
                "QUEUE WORKER ERROR"
            )

            await asyncio.sleep(
                5
            )


# ============================================================
# /SENDALL
# ============================================================

async def sendall_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    if not await require_admin(update):
        return

    if len(context.args) != 1:
        await update.effective_message.reply_text(
            "Usage:\n/sendall CHAT_ID"
        )
        return

    try:
        chat_id = int(
            context.args[0]
        )
    except ValueError:
        await update.effective_message.reply_text(
            "❌ Invalid CHAT_ID."
        )
        return

    # --------------------------------------------------------
    # Verify destination
    # --------------------------------------------------------

    try:
        await verify_destination(
            context.bot,
            chat_id,
        )

    except TelegramError as e:
        await update.effective_message.reply_text(
            "❌ Cannot access destination chat.\n\n"
            f"{e}"
        )
        return

    # --------------------------------------------------------
    # Check active jobs
    # --------------------------------------------------------

    active = await jobs_col.find_one(
        {
            "status": {
                "$in": [
                    "queued",
                    "running",
                    "paused",
                    "uncertain",
                ]
            }
        }
    )

    if active:
        await update.effective_message.reply_text(
            "⚠️ An existing job is still active.\n\n"
            f"Job ID: `{active['_id']}`\n"
            f"Status: `{active.get('status')}`\n"
            f"Last ID: `{active.get('last_seq', 0)}`\n\n"
            "Use /job to inspect it."
        )

        return

    total = await videos_col.count_documents({})

    if total == 0:
        await update.effective_message.reply_text(
            "❌ No videos stored."
        )
        return

    job_id = await create_send_job(
        chat_id
    )

    if not job_id:
        await update.effective_message.reply_text(
            "❌ Could not create job."
        )
        return

    await update.effective_message.reply_text(
        "🚀 SENDALL JOB CREATED\n\n"
        f"🆔 Job: `{job_id}`\n"
        f"🎬 Videos: {total}\n"
        f"📦 Album size: {ALBUM_SIZE}\n"
        f"⏱ Delay: {SEND_DELAY}s\n\n"
        "The queue worker will start sending."
    )


# ============================================================
# /JOB
# ============================================================

async def job_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    if not await require_admin(update):
        return

    job = await jobs_col.find_one(
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

    text = (
        "📦 CURRENT JOB\n\n"
        f"🆔 Job: `{job['_id']}`\n"
        f"📌 Status: `{job.get('status')}`\n"
        f"💬 Chat: `{job.get('chat_id')}`\n"
        f"▶️ Last ID: `{job.get('last_seq', 0)}`\n"
        f"✅ Sent: `{job.get('sent', 0)}`\n"
        f"❌ Failed: `{job.get('failed', 0)}`\n"
        f"👷 Worker: `{job.get('worker_id')}`\n"
        f"📊 Snapshot: `{job.get('total_snapshot', 0)}`\n"
    )

    if job.get("batch_first_seq") is not None:
        text += (
            "\n📦 CURRENT BATCH\n"
            f"From: `{job.get('batch_first_seq')}`\n"
            f"To: `{job.get('batch_last_seq')}`\n"
            f"Count: `{job.get('batch_count')}`\n"
        )

    if job.get("uncertain_first_seq") is not None:
        text += (
            "\n⚠️ UNCERTAIN BATCH\n"
            f"From: `{job.get('uncertain_first_seq')}`\n"
            f"To: `{job.get('uncertain_last_seq')}`\n"
            f"Reason: {job.get('uncertain_reason', 'Unknown')}\n"
        )

    await update.effective_message.reply_text(
        text
    )


# ============================================================
# /PAUSE
# ============================================================

async def pause_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    if not await require_admin(update):
        return

    job = await jobs_col.find_one(
        {
            "status": "running",
            "worker_id": INSTANCE_ID,
        },
        sort=[
            (
                "created_at",
                DESCENDING,
            )
        ],
    )

    if not job:
        await update.effective_message.reply_text(
            "No running job found."
        )
        return

    await jobs_col.update_one(
        {
            "_id": job["_id"],
            "worker_id": INSTANCE_ID,
            "status": "running",
        },
        {
            "$set": {
                "status": "paused",
                "updated_at": now_utc(),
            }
        },
    )

    await update.effective_message.reply_text(
        f"⏸ Job paused.\n\n"
        f"Job: `{job['_id']}`\n"
        "If an album was already in-flight, it will be "
        "committed only if Telegram confirmed success."
    )


# ============================================================
# /RESUME
# ============================================================

async def resume_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    if not await require_admin(update):
        return

    job = await jobs_col.find_one(
        {
            "status": "paused",
        },
        sort=[
            (
                "created_at",
                DESCENDING,
            )
        ],
    )

    if not job:
        await update.effective_message.reply_text(
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
                "updated_at": now_utc(),
            }
        },
    )

    await update.effective_message.reply_text(
        f"▶️ Job resumed.\n\n"
        f"Job: `{job['_id']}`"
    )


# ============================================================
# /CANCEL
# ============================================================

async def cancel_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    if not await require_admin(update):
        return

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
            (
                "created_at",
                DESCENDING,
            )
        ],
    )

    if not job:
        await update.effective_message.reply_text(
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
                "updated_at": now_utc(),
                "cancelled_at": now_utc(),
            }
        },
    )

    await update.effective_message.reply_text(
        f"🛑 Job cancelled.\n\n"
        f"Job: `{job['_id']}`\n"
        f"Last committed ID: `{job.get('last_seq', 0)}`"
    )


# ============================================================
# /DELETE
# ============================================================

async def delete_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    if not await require_admin(update):
        return

    if len(context.args) != 1:
        await update.effective_message.reply_text(
            "Usage:\n/delete VIDEO_ID"
        )
        return

    try:
        seq = int(
            context.args[0]
        )
    except ValueError:
        await update.effective_message.reply_text(
            "❌ Invalid VIDEO_ID."
        )
        return

    # IMPORTANT:
    # This deletes ONLY the requested video record.
    # It does NOT reset the database.
    result = await videos_col.delete_one(
        {
            "seq": seq
        }
    )

    if result.deleted_count:
        await update.effective_message.reply_text(
            f"🗑 Video {seq} deleted from storage."
        )
    else:
        await update.effective_message.reply_text(
            "❌ Video not found."
        )


# ============================================================
# ERROR HANDLER
# ============================================================

async def error_handler(
    update: object,
    context: ContextTypes.DEFAULT_TYPE,
):
    logger.exception(
        "UNHANDLED ERROR",
        exc_info=context.error,
    )


# ============================================================
# APPLICATION
# ============================================================

async def main():
    global worker_task
    global heartbeat_task

    if not BOT_TOKEN:
        raise RuntimeError(
            "BOT_TOKEN is missing."
        )

    if not ADMIN_IDS:
        raise RuntimeError(
            "ADMIN_IDS is missing."
        )

    # --------------------------------------------------------
    # DATABASE
    # --------------------------------------------------------

    await init_db()

    # --------------------------------------------------------
    # SINGLE INSTANCE LOCK
    # --------------------------------------------------------

    acquired = await acquire_instance_lock()

    if not acquired:
        logger.error(
            "Exiting because another instance is active."
        )
        return

    # --------------------------------------------------------
    # OLD RUNNING JOBS
    # --------------------------------------------------------

    await mark_stale_jobs_uncertain()

    # --------------------------------------------------------
    # TELEGRAM APPLICATION
    # --------------------------------------------------------

    application = (
        Application
        .builder()
        .token(BOT_TOKEN)
        .build()
    )

    # Commands
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
            "job",
            job_command,
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

    # --------------------------------------------------------
    # MANUAL ASYNC STARTUP
    # --------------------------------------------------------

    await application.initialize()

    await application.start()

    # Remove webhook so polling can start cleanly.
    # Pending updates are NOT intentionally discarded.
    await application.bot.delete_webhook(
        drop_pending_updates=False
    )

    await application.updater.start_polling(
        allowed_updates=Update.ALL_TYPES
    )

    logger.info(
        "BOT POLLING STARTED"
    )

    # --------------------------------------------------------
    # BACKGROUND TASKS
    # --------------------------------------------------------

    heartbeat_task = asyncio.create_task(
        heartbeat_instance_lock()
    )

    worker_task = asyncio.create_task(
        send_queue_worker(
            application.bot
        )
    )

    logger.info(
        "ALL SYSTEMS RUNNING"
    )

    try:
        await shutdown_event.wait()

    finally:

        logger.info(
            "SHUTTING DOWN..."
        )

        shutdown_event.set()

        if worker_task:
            worker_task.cancel()

        if heartbeat_task:
            heartbeat_task.cancel()

        await asyncio.gather(
            worker_task,
            heartbeat_task,
            return_exceptions=True,
        )

        try:
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
            "SHUTDOWN COMPLETE"
        )


# ============================================================
# RUN
# ============================================================

if __name__ == "__main__":
    try:
        asyncio.run(
            main()
        )

    except KeyboardInterrupt:
        pass

    except Exception:
        logger.exception(
            "FATAL ERROR"
        )
