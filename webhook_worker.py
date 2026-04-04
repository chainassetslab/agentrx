"""
AgentRx Webhook Worker — v2.3
==============================
Standalone process that consumes the agentrx:webhook_stream Redis Stream
and delivers HTTP POST webhooks to tenant-registered URLs.

The API never fires outbound HTTP directly. It only does XADD (a fast
in-memory Redis write). This worker is the only process that does network
I/O to external webhook endpoints.
"""

import asyncio
import json
import logging
import os
import time

import httpx
import redis.asyncio as aioredis

REDIS_URL       = os.getenv("AGENTRX_REDIS_URL", "redis://localhost:6379/0")
STREAM_KEY      = "agentrx:webhook_stream"
CONSUMER_GROUP  = "agentrx_webhook_workers"
CONSUMER_NAME   = f"worker-{os.getpid()}"
BLOCK_MS        = 5_000
BATCH_SIZE      = 10
MAX_RETRIES     = 3
RETRY_BACKOFF   = [1, 3, 10]
DEAD_LETTER_KEY = "agentrx:webhook_dead_letter"
WEBHOOK_TIMEOUT = int(os.getenv("AGENTRX_WEBHOOK_TIMEOUT_SECONDS", "5"))

logging.basicConfig(
    level=logging.INFO,
    format='{"ts": "%(asctime)s", "level": "%(levelname)s", "msg": "%(message)s"}',
)
logger = logging.getLogger("agentrx.webhook_worker")

async def ensure_consumer_group(client: aioredis.Redis) -> None:
    try:
        await client.xgroup_create(
            STREAM_KEY,
            CONSUMER_GROUP,
            id="$",
            mkstream=True,
        )
        logger.info(f"Consumer group '{CONSUMER_GROUP}' created.")
    except Exception as e:
        if "BUSYGROUP" in str(e):
            logger.info(f"Consumer group '{CONSUMER_GROUP}' already exists.")
        else:
            raise


async def deliver_webhook(
    client: aioredis.Redis,
    message_id: str,
    data: dict,
) -> None:
    webhook_url = data.get("webhook_url", "")
    trace_id    = data.get("trace_id", "unknown")

    if not webhook_url:
        logger.warning(f"No webhook_url in message {message_id}, skipping.")
        await client.xack(STREAM_KEY, CONSUMER_GROUP, message_id)
        return

    payload    = {k: v for k, v in data.items() if k != "webhook_url"}
    last_error = None

    for attempt in range(MAX_RETRIES):
        try:
            async with httpx.AsyncClient(timeout=WEBHOOK_TIMEOUT) as http:
                resp = await http.post(webhook_url, json=payload)
                resp.raise_for_status()
                logger.info(
                    f"Webhook delivered: status={resp.status_code} "
                    f"trace_id={trace_id} attempt={attempt + 1}"
                )
                await client.xack(STREAM_KEY, CONSUMER_GROUP, message_id)
                return
        except Exception as e:
            last_error = e
            logger.warning(
                f"Webhook attempt {attempt + 1}/{MAX_RETRIES} failed: {e} "
                f"trace_id={trace_id}"
            )
            if attempt < MAX_RETRIES - 1:
                await asyncio.sleep(RETRY_BACKOFF[attempt])

    logger.error(
        f"Webhook permanently failed after {MAX_RETRIES} attempts. "
        f"Dead-lettering message {message_id}. Last error: {last_error} "
        f"trace_id={trace_id}"
    )
    await client.xadd(
        DEAD_LETTER_KEY,
        {
            **data,
            "original_message_id": message_id,
            "failed_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "error": str(last_error),
        },
    )
    await client.xack(STREAM_KEY, CONSUMER_GROUP, message_id)


async def reclaim_stale_pending(client: aioredis.Redis) -> None:
    try:
        pending = await client.xpending_range(
            STREAM_KEY,
            CONSUMER_GROUP,
            min="-",
            max="+",
            count=BATCH_SIZE,
        )
        now_ms    = int(time.time() * 1000)
        stale_ids = [
            entry["message_id"]
            for entry in pending
            if now_ms - entry["time_since_delivered"] > 60_000
        ]
        if stale_ids:
            claimed = await client.xclaim(
                STREAM_KEY,
                CONSUMER_GROUP,
                CONSUMER_NAME,
                min_idle_time=60_000,
                message_ids=stale_ids,
            )
            logger.info(f"Reclaimed {len(claimed)} stale pending messages.")
    except Exception as e:
        logger.error(f"Failed to reclaim stale pending messages: {e}")

async def run_worker() -> None:
    client = aioredis.from_url(REDIS_URL, encoding="utf-8", decode_responses=True)
    await ensure_consumer_group(client)

    logger.info(
        f"Webhook worker started. "
        f"Stream={STREAM_KEY} Group={CONSUMER_GROUP} Consumer={CONSUMER_NAME}"
    )

    while True:
        try:
            await reclaim_stale_pending(client)

            results = await client.xreadgroup(
                groupname=CONSUMER_GROUP,
                consumername=CONSUMER_NAME,
                streams={STREAM_KEY: ">"},
                count=BATCH_SIZE,
                block=BLOCK_MS,
            )

            if not results:
                continue

            for stream_name, messages in results:
                # Process entire batch concurrently — not sequentially.
                # return_exceptions=True ensures one failure does not
                # cancel remaining tasks or leave messages unACKed.
                outcomes = await asyncio.gather(
                    *[deliver_webhook(client, msg_id, data)
                      for msg_id, data in messages],
                    return_exceptions=True,
                )
                for (msg_id, _), outcome in zip(messages, outcomes):
                    if isinstance(outcome, Exception):
                        logger.error(
                            f"Unhandled exception in deliver_webhook for "
                            f"message {msg_id}: {outcome}"
                        )

        except asyncio.CancelledError:
            logger.info("Worker shutting down gracefully.")
            break
        except Exception as e:
            logger.error(f"Worker loop error: {e}. Restarting in 2s.")
            await asyncio.sleep(2)

    await client.aclose()


if __name__ == "__main__":
    asyncio.run(run_worker())
