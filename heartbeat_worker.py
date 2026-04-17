"""
AgentRx Heartbeat Worker — v1.1
================================
Listens for Redis key expirations on heartbeat keys and fires HUMAN_HANDOFF
webhooks when agents go silent.

Architecture:
- Primary path: Redis keyspace notifications (Ex) for instant detection.
- Backup path: ZSET-based sweeper that catches deaths whose keyspace
  notifications were lost (worker restart, Redis pub/sub hiccup, etc.).

Dependencies:
    pip install httpx redis pydantic-settings

Environment variables:
    AGENTRX_REDIS_URL              — Redis connection URL
    AGENTRX_WEBHOOK_URL            — Webhook endpoint for silent-death alerts
    AGENTRX_SWEEPER_INTERVAL_SEC   — Sweeper run interval (default: 300)
"""

import asyncio
import logging
import time
from typing import Optional

import httpx
import redis.asyncio as aioredis
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    redis_url: str = "redis://localhost:6379/0"
    webhook_url: str = ""
    sweeper_interval_sec: int = 300

    class Config:
        env_prefix = "AGENTRX_"


settings = Settings()

logging.basicConfig(
    level=logging.INFO,
    format='{"ts": "%(asctime)s", "level": "%(levelname)s", "msg": "%(message)s"}',
)
logger = logging.getLogger("agentrx.heartbeat_worker")

EXPECTED_EXPIRATIONS_ZSET = "agentrx:expected_expirations"

_recently_fired: dict = {}
_DEDUPE_WINDOW_SEC = 300


def _should_fire(agent_ref: str) -> bool:
    """Prevents duplicate webhook fires across listener + sweeper."""
    now = time.time()
    last_fired = _recently_fired.get(agent_ref, 0)
    if now - last_fired < _DEDUPE_WINDOW_SEC:
        return False
    _recently_fired[agent_ref] = now
    for k in list(_recently_fired.keys()):
        if now - _recently_fired[k] > _DEDUPE_WINDOW_SEC * 2:
            del _recently_fired[k]
    return True


def _parse_heartbeat_key(expired_key: str) -> Optional[tuple]:
    if not expired_key.startswith("agentrx:heartbeat:"):
        return None
    parts = expired_key.split(":", 3)
    if len(parts) != 4:
        logger.warning(f"Malformed heartbeat key: {expired_key}")
        return None
    _, _, tenant_id, agent_id = parts
    if not tenant_id or not agent_id:
        logger.warning(f"Empty tenant_id or agent_id in key: {expired_key}")
        return None
    return tenant_id, agent_id


def _parse_zset_member(member: str) -> Optional[tuple]:
    parts = member.split(":", 1)
    if len(parts) != 2 or not parts[0] or not parts[1]:
        logger.warning(f"Malformed ZSET member: {member}")
        return None
    return parts[0], parts[1]


async def _fire_webhook(tenant_id: str, agent_id: str, source: str) -> None:
    """POST the HUMAN_HANDOFF webhook. All errors logged, never re-raised."""
    if not settings.webhook_url:
        logger.warning(
            f"Silent death: {agent_id} (tenant={tenant_id}) — "
            f"AGENTRX_WEBHOOK_URL not configured. Skipping webhook."
        )
        return

    payload = {
        "event": "agentrx.alert",
        "action_type": "HUMAN_HANDOFF",
        "failure_signature": "AGENT_UNRESPONSIVE",
        "agent_id": agent_id,
        "tenant_id": tenant_id,
        "detection_source": source,
        "recovery_prompt": (
            f"Agent {agent_id} missed its heartbeat. It may be stuck, "
            f"crashed silently, or rate-limited at the transport layer "
            f"(detection source: {source})."
        ),
    }

    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            response = await client.post(settings.webhook_url, json=payload)
            response.raise_for_status()
            logger.info(
                f"Webhook fired: agent={agent_id} tenant={tenant_id} "
                f"source={source} status={response.status_code}"
            )
    except httpx.TimeoutException:
        logger.error(f"Webhook timed out for {agent_id} (tenant={tenant_id}).")
    except httpx.HTTPStatusError as e:
        logger.error(
            f"Webhook returned {e.response.status_code} for {agent_id}: "
            f"{e.response.text[:200]}"
        )
    except Exception as e:
        logger.error(f"Webhook failed for {agent_id}: {type(e).__name__}: {e}")


async def _listen_for_deaths() -> None:
    """Primary detection path via Redis keyspace notifications."""
    backoff = 1.0
    max_backoff = 30.0

    while True:
        try:
            r = aioredis.from_url(settings.redis_url, decode_responses=True)

            try:
                await r.config_set("notify-keyspace-events", "Ex")
                logger.info("Redis keyspace notifications enabled (Ex).")
            except Exception as e:
                logger.warning(
                    f"CONFIG SET failed ({e}). Ensure notify-keyspace-events "
                    f"is configured at the Redis service level."
                )

            pubsub = r.pubsub()
            await pubsub.psubscribe("__keyevent@0__:expired")
            logger.info("Heartbeat listener active. Waiting for agent deaths...")
            backoff = 1.0

            async for message in pubsub.listen():
                if message["type"] != "pmessage":
                    continue

                expired_key = message["data"]
                parsed = _parse_heartbeat_key(expired_key)
                if parsed is None:
                    continue

                tenant_id, agent_id = parsed
                agent_ref = f"{tenant_id}:{agent_id}"

                if not _should_fire(agent_ref):
                    continue

                logger.warning(
                    f"SILENT DEATH via keyspace: agent={agent_id} tenant={tenant_id}"
                )
                await _fire_webhook(tenant_id, agent_id, source="keyspace")

                try:
                    await r.zrem(EXPECTED_EXPIRATIONS_ZSET, agent_ref)
                except Exception as e:
                    logger.warning(f"ZREM after webhook failed: {e}")

        except asyncio.CancelledError:
            logger.info("Listener shutting down gracefully.")
            raise
        except Exception as e:
            logger.error(
                f"Listener crashed: {type(e).__name__}: {e}. "
                f"Reconnecting in {backoff:.1f}s..."
            )
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, max_backoff)


async def _sweeper() -> None:
    """Backup detection path via ZSET sweeper."""
    while True:
        try:
            await asyncio.sleep(settings.sweeper_interval_sec)

            r = aioredis.from_url(settings.redis_url, decode_responses=True)
            now_ts = int(time.time())

            overdue = await r.zrangebyscore(
                EXPECTED_EXPIRATIONS_ZSET,
                min=0,
                max=now_ts,
                withscores=False,
            )

            if not overdue:
                logger.debug("Sweeper pass: no overdue agents.")
                continue

            logger.info(f"Sweeper pass: {len(overdue)} overdue ZSET entries.")
            fired = 0
            cleaned = 0

            for member in overdue:
                parsed = _parse_zset_member(member)
                if parsed is None:
                    await r.zrem(EXPECTED_EXPIRATIONS_ZSET, member)
                    continue

                tenant_id, agent_id = parsed
                heartbeat_key = f"agentrx:heartbeat:{tenant_id}:{agent_id}"
                key_exists = await r.exists(heartbeat_key)

                if key_exists:
                    await r.zrem(EXPECTED_EXPIRATIONS_ZSET, member)
                    cleaned += 1
                    continue

                agent_ref = f"{tenant_id}:{agent_id}"
                if _should_fire(agent_ref):
                    logger.warning(
                        f"SILENT DEATH via sweeper: agent={agent_id} "
                        f"tenant={tenant_id}"
                    )
                    await _fire_webhook(tenant_id, agent_id, source="sweeper")
                    fired += 1

                await r.zrem(EXPECTED_EXPIRATIONS_ZSET, member)

            logger.info(
                f"Sweeper pass complete. Fired={fired} cleaned={cleaned} "
                f"out of {len(overdue)} overdue entries."
            )

        except asyncio.CancelledError:
            logger.info("Sweeper shutting down gracefully.")
            raise
        except Exception as e:
            logger.error(f"Sweeper pass failed: {type(e).__name__}: {e}")


async def main() -> None:
    logger.info(
        f"AgentRx Heartbeat Worker v1.1 starting. "
        f"Redis: {settings.redis_url}, "
        f"Webhook: {'configured' if settings.webhook_url else 'NOT CONFIGURED'}, "
        f"Sweeper interval: {settings.sweeper_interval_sec}s"
    )

    listener_task = asyncio.create_task(_listen_for_deaths())
    sweeper_task = asyncio.create_task(_sweeper())

    try:
        await asyncio.gather(listener_task, sweeper_task)
    except KeyboardInterrupt:
        logger.info("Received interrupt, shutting down.")
        listener_task.cancel()
        sweeper_task.cancel()
        await asyncio.gather(listener_task, sweeper_task, return_exceptions=True)


if __name__ == "__main__":
    asyncio.run(main())
