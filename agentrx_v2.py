"""
AgentRx: Metacognitive Recovery API — v2.5 (OpenClaw Integration)
=================================================================
Changes in v2.5:

  OPENCLAW 1 — CORS Middleware
  OPENCLAW 2 — Optional latency_ms (default=0)
  OPENCLAW 3 — _diagnose_internal() extracted
  OPENCLAW 4 — /v1/openclaw/recover endpoint
  OPENCLAW 5 — openclaw_instruction field on RecoveryAction

Previous patches (v2.4):
  FIX 11 — Sequential Webhook Bottleneck
  FIX 12 — IP-Based Rate Limiting (Serverless Trap)
  FIX 13 — Schema Cache Poisoning / OOM Vector

Run API:
    uvicorn agentrx_v2:app --host 0.0.0.0 --port 8000 --workers 2
Run webhook worker:
    python webhook_worker.py
"""

import hashlib
import json
import logging
import re
import time
import uuid
from contextlib import asynccontextmanager
from enum import Enum
from typing import Any, Dict, List, Optional

import httpx
import redis.asyncio as aioredis
from fastapi import Depends, FastAPI, HTTPException, Request, Security, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.security.api_key import APIKeyHeader
from pydantic import BaseModel, Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="AGENTRX_", env_file=".env")

    redis_url:                str   = "redis://localhost:6379/0"
    api_keys:                 str   = "dev_key_change_me_in_production"
    agent_state_ttl_seconds:  int   = 3600
    loop_detection_threshold: int   = 20
    rate_limit:               str   = "60/minute"
    min_auto_confidence:      float = 0.70
    environment:              str   = "development"
    webhook_url:              str   = ""
    webhook_timeout_seconds:  int   = 5


settings = Settings()


def _get_ttl_for_tenant(tenant_id: str) -> int:
    """
    Beta tenants get a shorter TTL to flush garbage data quickly.
    Free beta key maps to tenant_id starting with 'tenant_beta'.
    Paid tenants get the full configured TTL.
    """
    if tenant_id.startswith("tenant_beta"):
        return 600  # 10 minutes — auto-garbage-collect beta abuse
    return settings.agent_state_ttl_seconds


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        log = {
            "ts":    time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(record.created)),
            "level": record.levelname,
            "msg":   record.getMessage(),
            "env":   settings.environment,
        }
        if hasattr(record, "trace_id"):
            log["trace_id"] = record.trace_id
        if hasattr(record, "agent_id"):
            log["agent_id"] = record.agent_id
        if record.exc_info:
            log["exc"] = self.formatException(record.exc_info)
        return json.dumps(log)


handler = logging.StreamHandler()
handler.setFormatter(JsonFormatter())
logger = logging.getLogger("agentrx")
logger.addHandler(handler)
logger.setLevel(logging.INFO)


redis_client: Optional[aioredis.Redis] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global redis_client
    redis_client = aioredis.from_url(
        settings.redis_url,
        encoding="utf-8",
        decode_responses=True,
        max_connections=20,
    )
    logger.info("Redis connection pool opened.")
    yield
    await redis_client.aclose()
    logger.info("Redis connection pool closed.")

def rate_limit_key_func(request: Request) -> str:
    """
    FIX 12: Rate limit by tenant_id not IP (serverless-safe).
    BETA FIX: Free beta tier keys by IP not tenant_id.
    All beta users share one tenant_id — if we key by tenant,
    one runaway script locks out every other beta user.
    Keying by IP isolates each developer independently.
    """
    tenant = getattr(request.state, "tenant", None)
    if tenant:
        if tenant.get("tier") == "free_beta":
            ip = get_remote_address(request)
            return f"beta_ip:{ip}"
        if tenant.get("tenant_id"):
            return f"tenant:{tenant['tenant_id']}"
    return get_remote_address(request)


limiter = Limiter(key_func=rate_limit_key_func)

app = FastAPI(
    title="AgentRx: Metacognitive Recovery API",
    version="2.5.0",
    description=(
        "Stateful failure diagnosis and recovery for AI agents. "
        "Classifies MCP tool failures, injects corrections, detects loops, "
        "and scores preflight risk. Native OpenClaw integration via "
        "/v1/openclaw/recover."
    ),
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)

app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# OPENCLAW 1 — CORS Middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST", "DELETE"],
    allow_headers=["X-API-Key", "Content-Type", "X-Trace-Id"],
)

API_KEY_HEADER = APIKeyHeader(name="X-API-Key", auto_error=False)
_KEY_CACHE: Dict[str, Dict[str, Any]] = {}
_KEY_CACHE_TTL_SECONDS = 60


async def _lookup_api_key(api_key: str) -> Optional[Dict[str, Any]]:
    """
    DB-backed API key lookup with 60s in-memory cache.
    Replace static_keys stub with asyncpg Supabase query:
        row = await db.fetchrow(
            "SELECT tenant_id, tier FROM active_api_keys WHERE key_hash = $1",
            hashlib.sha256(api_key.encode()).hexdigest()
        )
    """
    now    = time.time()
    cached = _KEY_CACHE.get(api_key)
    if cached and (now - cached["cached_at"]) < _KEY_CACHE_TTL_SECONDS:
        return cached

    static_keys = set(k.strip() for k in settings.api_keys.split(","))
    if api_key not in static_keys:
        _KEY_CACHE.pop(api_key, None)
        return None
    # Tag the public beta key with free_beta tier so rate limiter
    # can isolate beta users by IP instead of shared tenant bucket.
    if api_key.startswith("beta_"):
        tenant_data = {"tenant_id": f"tenant_{api_key[:8]}", "tier": "free_beta"}
    else:
        tenant_data = {"tenant_id": f"tenant_{api_key[:8]}", "tier": "dev"}

    entry = {**tenant_data, "cached_at": now}
    _KEY_CACHE[api_key] = entry

    expired = [k for k, v in _KEY_CACHE.items()
               if now - v["cached_at"] > _KEY_CACHE_TTL_SECONDS * 2]
    for k in expired:
        _KEY_CACHE.pop(k, None)

    return entry


async def require_api_key(
    request: Request,
    api_key: str = Security(API_KEY_HEADER),
) -> Dict[str, Any]:
    if not api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing X-API-Key header.",
            headers={"WWW-Authenticate": "ApiKey"},
        )
    try:
        tenant = await _lookup_api_key(api_key)
    except Exception as e:
        logger.error(f"API key lookup failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Authentication service temporarily unavailable.",
        )
    if tenant is None:
        logger.warning("Invalid or revoked API key attempt.")
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid or revoked API key.",
        )
    request.state.tenant = tenant
    return tenant

class FailureSignature(str, Enum):
    SCHEMA_MISMATCH     = "SCHEMA_MISMATCH"
    RESOURCE_MISSING    = "RESOURCE_MISSING"
    NETWORK_LATENCY     = "NETWORK_LATENCY"
    RATE_LIMIT_EXCEEDED = "RATE_LIMIT_EXCEEDED"
    AUTH_FAILURE        = "AUTH_FAILURE"
    TOOL_DEPRECATED     = "TOOL_DEPRECATED"
    HALLUCINATED_PARAM  = "HALLUCINATED_PARAM"
    HALLUCINATED_VALUE  = "HALLUCINATED_VALUE"
    AGENT_LOOP          = "AGENT_LOOP"
    UNKNOWN             = "UNKNOWN"


class RecoveryActionType(str, Enum):
    RELAX_SCHEMA       = "RELAX_SCHEMA"
    INJECT_KNOWLEDGE   = "INJECT_KNOWLEDGE"
    RETRY_WITH_BACKOFF = "RETRY_WITH_BACKOFF"
    HUMAN_HANDOFF      = "HUMAN_HANDOFF"
    REFRESH_AUTH       = "REFRESH_AUTH"
    SKIP_AND_CONTINUE  = "SKIP_AND_CONTINUE"
    ABORT              = "ABORT"


SCHEMA_CACHE_TTL = 86_400


def _build_openclaw_instruction(
    action_type:      RecoveryActionType,
    recovery_prompt:  Optional[str],
    corrected_payload: Optional[Dict[str, Any]],
    retry_after_ms:   Optional[int],
    tool_name:        str,
) -> str:
    """
    OPENCLAW 5 — Builds a direct plaintext instruction for OpenClaw agents.

    LLMs are notoriously bad at mapping ENUM strings like RELAX_SCHEMA
    to physical actions. This field removes the cognitive load entirely —
    the agent reads one sentence and knows exactly what to do next.
    No interpretation required. No hallucinated recovery paths.
    """
    if action_type == RecoveryActionType.RETRY_WITH_BACKOFF:
        ms = retry_after_ms or 2000
        return f"Wait {ms}ms then retry the same tool call to '{tool_name}'."

    elif action_type == RecoveryActionType.RELAX_SCHEMA:
        if corrected_payload:
            return (
                f"Retry '{tool_name}' with this corrected payload: "
                f"{json.dumps(corrected_payload)}"
            )
        return f"Schema mismatch on '{tool_name}'. Review the tool schema before retrying."

    elif action_type == RecoveryActionType.INJECT_KNOWLEDGE:
        return recovery_prompt or f"Inject correction context before retrying '{tool_name}'."

    elif action_type == RecoveryActionType.HUMAN_HANDOFF:
        return (
            f"STOP EXECUTION. Do not retry '{tool_name}'. "
            f"Alert Steven via Telegram immediately. "
            f"Reason: {recovery_prompt or 'escalated by AgentRx'}"
        )

    elif action_type == RecoveryActionType.REFRESH_AUTH:
        return (
            f"Your credentials for '{tool_name}' have expired or are invalid. "
            f"Request a fresh token before retrying. Do not retry with current credentials."
        )

    elif action_type == RecoveryActionType.SKIP_AND_CONTINUE:
        return (
            f"Tool '{tool_name}' is deprecated or unavailable. "
            f"Skip this step and continue with your next planned action."
        )

    elif action_type == RecoveryActionType.ABORT:
        return (
            f"This failure on '{tool_name}' is unrecoverable. "
            f"Stop the current task entirely."
        )

    return recovery_prompt or "Review the failure and retry if appropriate."

class AgentState(BaseModel):
    agent_id:          str                  = Field(..., min_length=1, max_length=128)
    goal:              str                  = Field(..., min_length=1, max_length=2048)
    active_plan:       List[str]            = Field(default_factory=list)
    execution_history: List[Dict[str, Any]] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_history(self) -> "AgentState":
        if len(self.execution_history) == 0:
            logger.warning(
                "Agent submitted diagnosis with empty execution_history.",
                extra={"agent_id": self.agent_id},
            )
        return self


class FailedToolCall(BaseModel):
    mcp_tool_name:     str            = Field(..., min_length=1, max_length=256)
    attempted_payload: Dict[str, Any] = Field(default_factory=dict)
    error_response:    Dict[str, Any] = Field(default_factory=dict)
    # OPENCLAW 2: default=0 so shell scripts don't need timing data.
    latency_ms:        int            = Field(default=0, ge=0)

    @model_validator(mode="after")
    def flag_latency_outlier(self) -> "FailedToolCall":
        if self.latency_ms > 30_000:
            logger.warning(
                f"Extreme latency reported: {self.latency_ms}ms "
                f"for '{self.mcp_tool_name}'."
            )
        return self


class RecoveryRequest(BaseModel):
    state:       AgentState
    failure:     FailedToolCall
    tool_schema: Optional[Dict[str, Any]] = None
    schema_hash: Optional[str]            = Field(None, min_length=64, max_length=64)


# OPENCLAW 4 — Flat simplified model for OpenClaw shell script callers.
class OpenClawRecoveryRequest(BaseModel):
    agent_id:          str            = Field(..., min_length=1, max_length=128)
    tool_name:         str            = Field(..., min_length=1, max_length=256)
    error_message:     str            = Field(default="", max_length=1000)
    error_code:        int            = Field(default=0)
    attempted_payload: Dict[str, Any] = Field(default_factory=dict)
    goal:              str            = Field(default="Complete current task", max_length=2048)


SCHEMA_MAX_BYTES = 65_536
SCHEMA_MAX_DEPTH = 10


def _measure_depth(obj: Any, current: int = 0) -> int:
    if current > SCHEMA_MAX_DEPTH:
        return current
    if isinstance(obj, dict):
        if not obj:
            return current
        return max(_measure_depth(v, current + 1) for v in obj.values())
    if isinstance(obj, list):
        if not obj:
            return current
        return max(_measure_depth(item, current + 1) for item in obj)
    return current


class SchemaRegisterRequest(BaseModel):
    """FIX 13: Validates byte size (<=64KB) and depth (<=10) before Redis write."""
    tool_schema: Dict[str, Any]

    @model_validator(mode="after")
    def validate_schema_size_and_depth(self) -> "SchemaRegisterRequest":
        serialized = json.dumps(self.tool_schema, separators=(",", ":"))
        byte_size  = len(serialized.encode("utf-8"))
        if byte_size > SCHEMA_MAX_BYTES:
            raise ValueError(
                f"Schema size {byte_size:,} bytes exceeds maximum of "
                f"{SCHEMA_MAX_BYTES:,} bytes. Split large schemas."
            )
        depth = _measure_depth(self.tool_schema)
        if depth > SCHEMA_MAX_DEPTH:
            raise ValueError(
                f"Schema nesting depth {depth} exceeds maximum of "
                f"{SCHEMA_MAX_DEPTH} levels. Flatten the schema structure."
            )
        return self


class SchemaRegisterResponse(BaseModel):
    schema_hash: str
    cached:      bool
    ttl_seconds: int = SCHEMA_CACHE_TTL


# OPENCLAW 5 — openclaw_instruction added to RecoveryAction.
# Direct plaintext command for OpenClaw agents — removes cognitive load
# of mapping ENUM strings to physical actions. Priority requirement.
class RecoveryAction(BaseModel):
    action_type:           RecoveryActionType
    failure_signature:     FailureSignature
    corrected_payload:     Optional[Dict[str, Any]] = None
    recovery_prompt:       Optional[str]            = None
    confidence_score:      float                    = Field(..., ge=0.0, le=1.0)
    retry_after_ms:        Optional[int]            = None
    openclaw_instruction:  Optional[str]            = None
    trace_id:              str                      = Field(default_factory=lambda: str(uuid.uuid4()))


class PreflightRequest(BaseModel):
    agent_id:         str            = Field(..., min_length=1, max_length=128)
    mcp_tool_name:    str
    intended_payload: Dict[str, Any]
    tool_schema:      Optional[Dict[str, Any]] = None
    schema_hash:      Optional[str]            = Field(None, min_length=64, max_length=64)


class PreflightResult(BaseModel):
    risk_score:           float                      = Field(..., ge=0.0, le=1.0)
    predicted_signature:  Optional[FailureSignature] = None
    suggested_correction: Optional[Dict[str, Any]]   = None
    proceed:              bool
    warnings:             List[str]                  = Field(default_factory=list)
    trace_id:             str                        = Field(default_factory=lambda: str(uuid.uuid4()))


class HeartbeatRequest(BaseModel):
    agent_id:         str = Field(..., min_length=1, max_length=128, pattern=r"^[^:]+$")
    status:           str = Field(default="active", max_length=50)
    turn_count:       int = Field(default=0, ge=0)
    last_tool:        str = Field(default="unknown", max_length=128)
    interval_seconds: int = Field(default=60, ge=10, le=600)


class HeartbeatStopRequest(BaseModel):
    agent_id: str = Field(..., min_length=1, max_length=128, pattern=r"^[^:]+$")

class RedisUnavailableError(Exception):
    """FAIL-CLOSED: raised on Redis errors to prevent bypassing loop circuit breaker."""
    pass


def _failure_key(tenant_id: str, agent_id: str) -> str:
    return f"agentrx:failures:{tenant_id}:{agent_id}"


def _same_call_key(tenant_id: str, agent_id: str, tool_name: str) -> str:
    return f"agentrx:samecall:{tenant_id}:{agent_id}:{tool_name}"


async def get_failure_history(tenant_id: str, agent_id: str) -> List[str]:
    try:
        raw = await redis_client.lrange(_failure_key(tenant_id, agent_id), 0, -1)
        return raw or []
    except Exception as e:
        logger.error(f"Redis read failed for tenant={tenant_id} agent={agent_id}: {e}")
        raise RedisUnavailableError(str(e))


async def record_failure(
    tenant_id: str,
    agent_id:  str,
    signature: FailureSignature,
) -> None:
    try:
        key  = _failure_key(tenant_id, agent_id)
        pipe = redis_client.pipeline()
        pipe.rpush(key, signature.value)
        pipe.ltrim(key, -50, -1)
        pipe.expire(key, _get_ttl_for_tenant(tenant_id))
        await pipe.execute()
    except Exception as e:
        logger.error(f"Redis write failed for tenant={tenant_id} agent={agent_id}: {e}")


async def atomic_increment_and_get(
    tenant_id: str,
    agent_id:  str,
    tool_name: str,
) -> int:
    """FIX 4: Atomic INCR eliminates TOCTOU race in loop detection."""
    try:
        key   = _same_call_key(tenant_id, agent_id, tool_name)
        count = await redis_client.incr(key)
        await redis_client.expire(key, _get_ttl_for_tenant(tenant_id))
        return count
    except Exception as e:
        logger.error(f"Redis INCR failed for tenant={tenant_id} agent={agent_id}: {e}")
        raise RedisUnavailableError(str(e))


_INJECTION_PATTERN = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")
_NEWLINE_PATTERN   = re.compile(r"[\r\n]+")
_MAX_EXTERNAL_STRING_LEN = 200


def sanitize_external_string(raw: str) -> str:
    """FIX 6: Strips control chars, wraps in [EXTERNAL ERROR: "..."] delimiter."""
    if not isinstance(raw, str):
        raw = str(raw)
    cleaned = _INJECTION_PATTERN.sub("", raw)
    cleaned = _NEWLINE_PATTERN.sub(" ", cleaned)
    cleaned = cleaned[:_MAX_EXTERNAL_STRING_LEN]
    return f'[EXTERNAL ERROR: "{cleaned}"]'


async def enqueue_alert_to_stream(
    tenant_id:         str,
    agent_id:          str,
    action_type:       "RecoveryActionType",
    failure_signature: "FailureSignature",
    recovery_prompt:   Optional[str],
    trace_id:          str,
) -> None:
    """FIX 7: XADD to Redis Stream — microseconds. Worker handles HTTP delivery."""
    if not settings.webhook_url:
        return
    try:
        await redis_client.xadd(
            "agentrx:webhook_stream",
            {
                "event":             "agentrx.alert",
                "action_type":       action_type.value,
                "failure_signature": failure_signature.value,
                "agent_id":          agent_id,
                "tenant_id":         tenant_id,
                "recovery_prompt":   recovery_prompt or "",
                "trace_id":          trace_id,
                "webhook_url":       settings.webhook_url,
                "ts":                time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            },
            maxlen=10_000,
            approximate=True,
        )
    except Exception as e:
        logger.error(f"Failed to enqueue alert: {e}", extra={"trace_id": trace_id})


def _schema_cache_key(schema_hash: str) -> str:
    return f"agentrx:schema:{schema_hash}"


async def get_cached_schema(schema_hash: str) -> Optional[Dict[str, Any]]:
    try:
        raw = await redis_client.get(_schema_cache_key(schema_hash))
        return json.loads(raw) if raw else None
    except Exception as e:
        logger.error(f"Schema cache read failed: {e}")
        return None


async def store_schema(schema: Dict[str, Any]) -> str:
    serialized  = json.dumps(schema, sort_keys=True)
    schema_hash = hashlib.sha256(serialized.encode()).hexdigest()
    try:
        await redis_client.set(
            _schema_cache_key(schema_hash),
            serialized,
            ex=SCHEMA_CACHE_TTL,
        )
    except Exception as e:
        logger.error(f"Schema cache write failed: {e}")
    return schema_hash

def classify_failure(
    failure:         FailedToolCall,
    past_signatures: List[str],
    same_call_count: int,
    tool_schema:     Optional[Dict[str, Any]] = None,
) -> FailureSignature:
    err  = failure.error_response
    msg  = str(err.get("message", "")).lower()
    code = err.get("status_code") or err.get("code") or err.get("status") or 0

    try:
        code = int(code)
    except (ValueError, TypeError):
        code = 0

    if same_call_count >= 3:
        return FailureSignature.AGENT_LOOP

    if code in (401, 403) or any(kw in msg for kw in (
        "unauthorized", "forbidden", "auth", "token expired", "permission denied"
    )):
        return FailureSignature.AUTH_FAILURE

    tool_lower = failure.mcp_tool_name.lower()
    if any(kw in msg for kw in (
        "deprecated", "no longer available", "removed", "end of life", "sunset"
    )):
        return FailureSignature.TOOL_DEPRECATED
    if code == 404 and tool_lower in msg:
        return FailureSignature.TOOL_DEPRECATED

    if code == 429 or any(kw in msg for kw in (
        "rate limit", "too many requests", "quota exceeded", "throttl"
    )):
        return FailureSignature.RATE_LIMIT_EXCEEDED

    if any(kw in msg for kw in (
        "timeout", "timed out", "connection refused", "network", "unreachable", "gateway"
    )):
        return FailureSignature.NETWORK_LATENCY
    if failure.latency_ms > 10_000 and code == 0:
        return FailureSignature.NETWORK_LATENCY

    if code == 404 or any(kw in msg for kw in (
        "not found", "does not exist", "no such", "missing resource"
    )):
        return FailureSignature.RESOURCE_MISSING

    if any(kw in msg for kw in (
        "validation error", "invalid type", "schema", "expected",
        "must be", "required field", "bad request"
    )) or code == 422:
        if tool_schema:
            schema_props = set((tool_schema.get("properties") or {}).keys())
            payload_keys = set(failure.attempted_payload.keys())
            unknown_keys = payload_keys - schema_props
            if unknown_keys:
                return FailureSignature.HALLUCINATED_PARAM
            return FailureSignature.HALLUCINATED_VALUE
        return FailureSignature.SCHEMA_MISMATCH

    if past_signatures.count(FailureSignature.UNKNOWN.value) >= 2:
        return FailureSignature.HALLUCINATED_PARAM

    return FailureSignature.UNKNOWN


def fix_payload(
    payload:     Dict[str, Any],
    tool_schema: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    if not payload:
        return payload

    schema_props: Dict[str, Any] = {}
    if tool_schema and "properties" in tool_schema:
        schema_props = tool_schema["properties"]

    corrected: Dict[str, Any] = {}

    for key, value in payload.items():
        if schema_props and key not in schema_props:
            logger.info(f"Dropping hallucinated key '{key}' not in tool schema.")
            continue
        expected_type = schema_props[key].get("type") if key in schema_props else None
        corrected[key] = _coerce_value(value, expected_type)

    if tool_schema:
        for req_key in tool_schema.get("required", []):
            if req_key not in corrected:
                logger.warning(f"Required field '{req_key}' missing after correction.")

    return corrected


def _coerce_value(value: Any, expected_type: Optional[str]) -> Any:
    if expected_type == "integer":
        if isinstance(value, str) and value.isdigit():
            return int(value)
        if isinstance(value, float) and value.is_integer():
            return int(value)
    elif expected_type == "number":
        if isinstance(value, str):
            try:
                return float(value)
            except ValueError:
                pass
    elif expected_type == "boolean":
        if isinstance(value, str):
            if value.lower() in ("true", "1", "yes"):
                return True
            if value.lower() in ("false", "0", "no"):
                return False
    elif expected_type == "string":
        if isinstance(value, (int, float, bool)):
            return str(value)
    elif expected_type == "array":
        if isinstance(value, str) and "," in value:
            return [v.strip() for v in value.split(",")]
        if not isinstance(value, list):
            return [value]
    elif expected_type == "object":
        if isinstance(value, dict):
            return value
    return value

def score_preflight_risk(request: PreflightRequest) -> PreflightResult:
    warnings:   List[str]                  = []
    risk                                   = 0.0
    predicted:  Optional[FailureSignature] = None
    suggestion: Optional[Dict[str, Any]]   = None

    schema_props = request.tool_schema.get("properties", {})
    required     = request.tool_schema.get("required", [])

    unknown_keys = set(request.intended_payload.keys()) - set(schema_props.keys())
    if unknown_keys:
        risk     += 0.40
        predicted = FailureSignature.HALLUCINATED_PARAM
        warnings.append(f"Payload contains unknown keys not in schema: {unknown_keys}")
        suggestion = {
            k: v for k, v in request.intended_payload.items()
            if k in schema_props
        }

    missing_required = [r for r in required if r not in request.intended_payload]
    if missing_required:
        risk     += 0.35
        predicted = predicted or FailureSignature.SCHEMA_MISMATCH
        warnings.append(f"Missing required fields: {missing_required}")

    for key, value in request.intended_payload.items():
        if key not in schema_props:
            continue
        expected_type = schema_props[key].get("type")
        if expected_type == "integer" and not isinstance(value, int):
            risk     += 0.10
            predicted = predicted or FailureSignature.SCHEMA_MISMATCH
            warnings.append(f"Field '{key}' should be integer, got {type(value).__name__}")
        elif expected_type == "boolean" and not isinstance(value, bool):
            risk += 0.10
            warnings.append(f"Field '{key}' should be boolean, got {type(value).__name__}")
        elif expected_type == "array" and not isinstance(value, list):
            risk += 0.10
            warnings.append(f"Field '{key}' should be array, got {type(value).__name__}")

    if not request.intended_payload and schema_props:
        risk     += 0.30
        predicted = FailureSignature.SCHEMA_MISMATCH
        warnings.append("Payload is empty but schema defines expected fields.")

    risk    = min(risk, 1.0)
    proceed = risk < 0.50

    return PreflightResult(
        risk_score           = round(risk, 2),
        predicted_signature  = predicted,
        suggested_correction = suggestion,
        proceed              = proceed,
        warnings             = warnings,
    )

async def build_recovery_action(
    request:         RecoveryRequest,
    signature:       FailureSignature,
    past_signatures: List[str],
    trace_id:        str,
) -> RecoveryAction:
    history_len    = len(request.state.execution_history)
    loop_threshold = settings.loop_detection_threshold

    def _make_action(**kwargs) -> RecoveryAction:
        """Helper that auto-populates openclaw_instruction from the other fields."""
        action = RecoveryAction(**kwargs, trace_id=trace_id)
        action.openclaw_instruction = _build_openclaw_instruction(
            action_type       = action.action_type,
            recovery_prompt   = action.recovery_prompt,
            corrected_payload = action.corrected_payload,
            retry_after_ms    = action.retry_after_ms,
            tool_name         = request.failure.mcp_tool_name,
        )
        return action

    if history_len > loop_threshold:
        return _make_action(
            action_type       = RecoveryActionType.HUMAN_HANDOFF,
            failure_signature = FailureSignature.AGENT_LOOP,
            recovery_prompt   = (
                f"CIRCUIT BREAKER: Agent '{request.state.agent_id}' has exceeded "
                f"{loop_threshold} execution steps. Goal: '{request.state.goal}'. "
                f"Pausing for human review. Do not retry."
            ),
            confidence_score  = 1.0,
        )

    same_sig_count = past_signatures.count(signature.value)
    if same_sig_count >= 2:
        return _make_action(
            action_type       = RecoveryActionType.HUMAN_HANDOFF,
            failure_signature = signature,
            recovery_prompt   = (
                f"Repeated failure ({same_sig_count + 1}x) with signature "
                f"{signature.value} on tool '{request.failure.mcp_tool_name}'. "
                f"Automatic recovery exhausted. Escalating to human."
            ),
            confidence_score  = 1.0,
        )

    if signature == FailureSignature.SCHEMA_MISMATCH:
        corrected = fix_payload(request.failure.attempted_payload, request.tool_schema)
        changed   = corrected != request.failure.attempted_payload
        return _make_action(
            action_type       = RecoveryActionType.RELAX_SCHEMA,
            failure_signature = signature,
            corrected_payload = corrected,
            recovery_prompt   = (
                None if changed else
                f"Schema mismatch on '{request.failure.mcp_tool_name}' but "
                f"no automatic correction was possible. Review the tool schema."
            ),
            confidence_score  = 0.90 if changed else 0.45,
        )

    elif signature == FailureSignature.HALLUCINATED_PARAM:
        corrected = fix_payload(request.failure.attempted_payload, request.tool_schema)
        return _make_action(
            action_type       = RecoveryActionType.INJECT_KNOWLEDGE,
            failure_signature = signature,
            corrected_payload = corrected,
            recovery_prompt   = (
                f"SYSTEM CORRECTION: Your call to '{request.failure.mcp_tool_name}' "
                f"included parameters that do not exist in the tool schema. "
                f"Use ONLY the parameters defined in the official schema. "
                f"Do not invent parameter names."
            ),
            confidence_score  = 0.85,
        )

    elif signature == FailureSignature.HALLUCINATED_VALUE:
        return _make_action(
            action_type       = RecoveryActionType.INJECT_KNOWLEDGE,
            failure_signature = signature,
            corrected_payload = fix_payload(
                request.failure.attempted_payload, request.tool_schema
            ),
            recovery_prompt   = (
                f"SYSTEM CORRECTION: Parameters for '{request.failure.mcp_tool_name}' "
                f"exist in schema but values were invalid. "
                f"Check enum constraints, string formats, and value ranges."
            ),
            confidence_score  = 0.78,
        )

    elif signature == FailureSignature.RATE_LIMIT_EXCEEDED:
        backoff_ms = min(2000 * (2 ** min(same_sig_count, 4)), 60_000)
        return _make_action(
            action_type       = RecoveryActionType.RETRY_WITH_BACKOFF,
            failure_signature = signature,
            recovery_prompt   = f"Rate limit hit. Retry after {backoff_ms}ms.",
            confidence_score  = 0.95,
            retry_after_ms    = backoff_ms,
        )

    elif signature == FailureSignature.NETWORK_LATENCY:
        backoff_ms = 3000 * (same_sig_count + 1)
        return _make_action(
            action_type       = RecoveryActionType.RETRY_WITH_BACKOFF,
            failure_signature = signature,
            recovery_prompt   = (
                f"Network error on '{request.failure.mcp_tool_name}'. "
                f"Latency was {request.failure.latency_ms}ms. "
                f"Retry after {backoff_ms}ms."
            ),
            confidence_score  = 0.88,
            retry_after_ms    = backoff_ms,
        )

    elif signature == FailureSignature.RESOURCE_MISSING:
        return _make_action(
            action_type       = RecoveryActionType.INJECT_KNOWLEDGE,
            failure_signature = signature,
            recovery_prompt   = (
                f"Resource referenced in call to '{request.failure.mcp_tool_name}' "
                f"was not found. Verify the resource ID/path in your context "
                f"before retrying. Do not guess IDs."
            ),
            confidence_score  = 0.75,
        )

    elif signature == FailureSignature.AUTH_FAILURE:
        return _make_action(
            action_type       = RecoveryActionType.REFRESH_AUTH,
            failure_signature = signature,
            recovery_prompt   = (
                f"Authentication failed on '{request.failure.mcp_tool_name}'. "
                f"Request a fresh token or credential refresh before retrying. "
                f"Do not retry with the same credentials."
            ),
            confidence_score  = 0.92,
        )

    elif signature == FailureSignature.TOOL_DEPRECATED:
        return _make_action(
            action_type       = RecoveryActionType.SKIP_AND_CONTINUE,
            failure_signature = signature,
            recovery_prompt   = (
                f"Tool '{request.failure.mcp_tool_name}' appears to be deprecated "
                f"or no longer available. Check for a replacement tool in the "
                f"tool registry and update your plan accordingly."
            ),
            confidence_score  = 0.80,
        )

    elif signature == FailureSignature.AGENT_LOOP:
        return _make_action(
            action_type       = RecoveryActionType.HUMAN_HANDOFF,
            failure_signature = signature,
            recovery_prompt   = (
                f"Loop detected: tool '{request.failure.mcp_tool_name}' called "
                f"repeatedly with identical parameters. Execution halted."
            ),
            confidence_score  = 1.0,
        )

    safe_error = sanitize_external_string(
        request.failure.error_response.get("message", "no message")
    )
    return _make_action(
        action_type       = RecoveryActionType.HUMAN_HANDOFF,
        failure_signature = FailureSignature.UNKNOWN,
        recovery_prompt   = (
            f"Unclassified failure on '{request.failure.mcp_tool_name}'. "
            f"Error: {safe_error}. Escalating to human review."
        ),
        confidence_score  = 0.50,
    )

@app.exception_handler(RedisUnavailableError)
async def redis_unavailable_handler(request: Request, exc: RedisUnavailableError):
    trace_id = getattr(exc, "trace_id", str(uuid.uuid4()))
    return JSONResponse(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        content=RecoveryAction(
            action_type          = RecoveryActionType.HUMAN_HANDOFF,
            failure_signature    = FailureSignature.UNKNOWN,
            recovery_prompt      = (
                "AgentRx state store is temporarily unavailable. "
                "Loop history cannot be verified. Defaulting to HUMAN_HANDOFF "
                "as a safety measure. Retry once the service recovers."
            ),
            openclaw_instruction = (
                "STOP EXECUTION. AgentRx Redis is unavailable. "
                "Alert Steven via Telegram immediately. Do not retry any tool calls."
            ),
            confidence_score     = 1.0,
            trace_id             = trace_id,
        ).model_dump(),
    )


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    trace_id = str(uuid.uuid4())
    logger.exception(f"Unhandled exception: {exc}", extra={"trace_id": trace_id})
    return JSONResponse(
        status_code=500,
        content={
            "detail":   "Internal server error. Please retry or contact support.",
            "trace_id": trace_id,
        },
    )


async def _diagnose_internal(
    body:     RecoveryRequest,
    tenant:   Dict[str, Any],
    trace_id: str,
) -> RecoveryAction:
    """
    OPENCLAW 3 — Core logic extracted from route handler.
    Shared by /v1/diagnose_and_recover and /v1/openclaw/recover.
    """
    agent_id  = body.state.agent_id
    tenant_id = tenant["tenant_id"]

    logger.info(
        "Recovery request received.",
        extra={"trace_id": trace_id, "agent_id": agent_id, "tenant_id": tenant_id},
    )

    tool_schema = body.tool_schema
    if tool_schema is None and body.schema_hash:
        tool_schema = await get_cached_schema(body.schema_hash)
        if tool_schema is None:
            raise HTTPException(
                status_code=428,
                detail={
                    "error":   "SCHEMA_NOT_CACHED",
                    "message": (
                        f"Schema hash '{body.schema_hash}' not found in cache. "
                        "Register the full schema via POST /v1/schema/register, "
                        "then retry with the returned schema_hash."
                    ),
                },
            )

    past_signatures = await get_failure_history(tenant_id, agent_id)
    same_call_count = await atomic_increment_and_get(
        tenant_id, agent_id, body.failure.mcp_tool_name
    )

    signature = classify_failure(
        failure         = body.failure,
        past_signatures = past_signatures,
        same_call_count = same_call_count,
        tool_schema     = tool_schema,
    )

    await record_failure(tenant_id, agent_id, signature)

    action = await build_recovery_action(
        request         = body,
        signature       = signature,
        past_signatures = past_signatures,
        trace_id        = trace_id,
    )

    logger.info(
        f"Recovery action: {action.action_type} (confidence={action.confidence_score})",
        extra={"trace_id": trace_id, "agent_id": agent_id},
    )

    if action.confidence_score < settings.min_auto_confidence:
        action.action_type       = RecoveryActionType.HUMAN_HANDOFF
        action.recovery_prompt   = (
            (action.recovery_prompt or "") +
            " [Low confidence — escalated to human review.]"
        )
        action.openclaw_instruction = (
            f"STOP EXECUTION. Low confidence recovery on "
            f"'{body.failure.mcp_tool_name}'. "
            f"Alert Steven via Telegram immediately."
        )

    if action.action_type == RecoveryActionType.HUMAN_HANDOFF:
        await enqueue_alert_to_stream(
            tenant_id         = tenant_id,
            agent_id          = agent_id,
            action_type       = action.action_type,
            failure_signature = action.failure_signature,
            recovery_prompt   = action.recovery_prompt,
            trace_id          = trace_id,
        )

    return action

@app.get("/health", tags=["Ops"])
async def health():
    return {"status": "ok"}


@app.get("/ready", tags=["Ops"])
async def readiness():
    try:
        await redis_client.ping()
        return {"status": "ready", "redis": "ok"}
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Redis not reachable: {e}",
        )


@app.post(
    "/v1/diagnose_and_recover",
    response_model=RecoveryAction,
    responses={503: {"model": RecoveryAction, "description": "Redis unavailable"}},
    tags=["Recovery"],
    summary="Diagnose a failed agent tool call and return a recovery action.",
)
@limiter.limit(settings.rate_limit)
async def diagnose_and_recover(
    request: Request,
    body:    RecoveryRequest,
    tenant:  Dict[str, Any] = Depends(require_api_key),
) -> RecoveryAction:
    """
    Primary recovery endpoint for LangChain, CrewAI, PydanticAI, AutoGen,
    and the Python SDK. Accepts full nested RecoveryRequest payload.
    For OpenClaw shell scripts use /v1/openclaw/recover instead.
    """
    trace_id = request.headers.get("x-trace-id") or str(uuid.uuid4())
    return await _diagnose_internal(body, tenant, trace_id)


@app.post(
    "/v1/openclaw/recover",
    response_model=RecoveryAction,
    responses={503: {"model": RecoveryAction, "description": "Redis unavailable"}},
    tags=["OpenClaw"],
    summary="Simplified recovery endpoint for OpenClaw agents and shell scripts.",
)
@limiter.limit(settings.rate_limit)
async def openclaw_recover(
    request: Request,
    body:    OpenClawRecoveryRequest,
    tenant:  Dict[str, Any] = Depends(require_api_key),
) -> RecoveryAction:
    """
    OPENCLAW 4 — Flat-payload endpoint for OpenClaw agents using curl.

    Accepts 4-6 flat fields. Internally builds a full RecoveryRequest
    and calls _diagnose_internal(). Same logic, same Redis state tracking,
    same circuit breaker. Response includes openclaw_instruction — a
    direct plaintext command the agent executes without interpretation.

    Minimum valid curl call:
        curl -X POST .../v1/openclaw/recover
          -H "X-API-Key: your_key"
          -H "Content-Type: application/json"
          -d '{
            "agent_id": "lamar_cmo",
            "tool_name": "web_search",
            "error_message": "connection timeout",
            "error_code": 0
          }'
    """
    trace_id = request.headers.get("x-trace-id") or str(uuid.uuid4())

    recovery_request = RecoveryRequest(
        state=AgentState(
            agent_id          = body.agent_id,
            goal              = body.goal,
            active_plan       = [],
            execution_history = [],
        ),
        failure=FailedToolCall(
            mcp_tool_name     = body.tool_name,
            attempted_payload = body.attempted_payload,
            error_response    = {
                "message":     body.error_message,
                "status_code": body.error_code,
            },
            latency_ms = 0,
        ),
    )

    return await _diagnose_internal(recovery_request, tenant, trace_id)

@app.post(
    "/v1/preflight",
    response_model=PreflightResult,
    tags=["Preflight"],
    summary="Score the risk of an intended tool call before execution.",
)
@limiter.limit(settings.rate_limit)
async def preflight_check(
    request: Request,
    body:    PreflightRequest,
    tenant:  Dict[str, Any] = Depends(require_api_key),
) -> PreflightResult:
    """
    Proactive risk scoring. Call BEFORE executing a tool.
    Pass tool_schema once, get back schema_hash, use hash on future calls.
    Returns proceed=False if risk >= 0.50.
    """
    trace_id = request.headers.get("x-trace-id") or str(uuid.uuid4())
    logger.info(
        "Preflight check.",
        extra={"trace_id": trace_id, "agent_id": body.agent_id},
    )

    tool_schema = body.tool_schema
    if tool_schema is None and body.schema_hash:
        tool_schema = await get_cached_schema(body.schema_hash)
        if tool_schema is None:
            raise HTTPException(
                status_code=428,
                detail={
                    "error":   "SCHEMA_NOT_CACHED",
                    "message": (
                        f"Schema hash '{body.schema_hash}' not found. "
                        "Register via POST /v1/schema/register first."
                    ),
                },
            )

    if tool_schema is None:
        raise HTTPException(
            status_code=422,
            detail="Preflight requires either tool_schema or schema_hash.",
        )

    result          = score_preflight_risk(
        PreflightRequest(
            agent_id         = body.agent_id,
            mcp_tool_name    = body.mcp_tool_name,
            intended_payload = body.intended_payload,
            tool_schema      = tool_schema,
        )
    )
    result.trace_id = trace_id
    return result


@app.post(
    "/v1/schema/register",
    response_model=SchemaRegisterResponse,
    tags=["Schema"],
    summary="Register a tool schema and receive a hash for use in future calls.",
)
@limiter.limit(settings.rate_limit)
async def register_schema(
    request: Request,
    body:    SchemaRegisterRequest,
    tenant:  Dict[str, Any] = Depends(require_api_key),
) -> SchemaRegisterResponse:
    """
    Upload a tool schema once. AgentRx stores it for 24 hours and returns
    a SHA-256 hash. Pass this hash in subsequent preflight and recovery
    calls instead of re-uploading the full schema every time.
    """
    schema_hash = await store_schema(body.tool_schema)
    return SchemaRegisterResponse(schema_hash=schema_hash, cached=True)

@app.post("/v1/heartbeat", tags=["Heartbeat"],
    summary="Ping to keep agent marked as active. Triggers alert if missed.")
@limiter.limit(settings.rate_limit)
async def heartbeat(
    request: Request,
    body: HeartbeatRequest,
    tenant: Dict[str, Any] = Depends(require_api_key),
):
    ttl = body.interval_seconds * 3
    tenant_id = tenant["tenant_id"]
    key = f"agentrx:heartbeat:{tenant_id}:{body.agent_id}"
    expected_expiration = int(time.time()) + ttl
    zset_member = f"{tenant_id}:{body.agent_id}"

    heartbeat_data = {
        "agent_id": body.agent_id,
        "status": body.status,
        "turn_count": body.turn_count,
        "last_tool": body.last_tool,
        "interval_seconds": body.interval_seconds,
        "updated_at": int(time.time()),
    }

    pipe = redis_client.pipeline(transaction=True)
    pipe.set(key, json.dumps(heartbeat_data), ex=ttl)
    pipe.zadd("agentrx:expected_expirations", {zset_member: expected_expiration})
    await pipe.execute()

    return {"status": "tracked", "ttl_seconds": ttl, "state": "active"}


@app.post("/v1/heartbeat/stop", tags=["Heartbeat"],
    summary="Gracefully stop heartbeat tracking to prevent false death alerts.")
@limiter.limit(settings.rate_limit)
async def heartbeat_stop(
    request: Request,
    body: HeartbeatStopRequest,
    tenant: Dict[str, Any] = Depends(require_api_key),
):
    tenant_id = tenant["tenant_id"]
    key = f"agentrx:heartbeat:{tenant_id}:{body.agent_id}"
    zset_member = f"{tenant_id}:{body.agent_id}"

    pipe = redis_client.pipeline(transaction=True)
    pipe.delete(key)
    pipe.zrem("agentrx:expected_expirations", zset_member)
    await pipe.execute()

    return {"status": "stopped"}


@app.delete(
    "/v1/state/{agent_id}",
    tags=["State"],
    summary="Clear failure state for an agent. Scoped to the authenticated tenant.",
)
async def clear_agent_state(
    agent_id: str,
    request:  Request,
    tenant:   Dict[str, Any] = Depends(require_api_key),
):
    """
    FIX 5 — Cross-Tenant Hijacking:
    Keys are scoped to tenant namespace. Tenant A cannot delete
    agent state belonging to tenant B even with a valid API key.
    """
    tenant_id = tenant["tenant_id"]
    try:
        pattern = f"agentrx:*:{tenant_id}:{agent_id}*"
        keys    = await redis_client.keys(pattern)
        if keys:
            await redis_client.delete(*keys)
        return {
            "cleared":      True,
            "agent_id":     agent_id,
            "tenant_id":    tenant_id,
            "keys_deleted": len(keys),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to clear state: {e}")

@app.get(
    "/v1/openclaw/status",
    tags=["OpenClaw"],
    summary="Verify AgentRx is reachable and the OpenClaw integration is active.",
)
async def openclaw_status(
    tenant: Dict[str, Any] = Depends(require_api_key),
):
    """
    Health check specifically for OpenClaw skill verification.
    Returns integration status and available endpoints.
    Called by the SKILL.md on first install to confirm connectivity.
    """
    return {
        "status":       "active",
        "version":      "2.5.0",
        "integration":  "openclaw",
        "tenant_id":    tenant["tenant_id"],
        "endpoints": {
            "recover":   "/v1/openclaw/recover",
            "preflight": "/v1/preflight",
            "schema":    "/v1/schema/register",
            "state":     "/v1/state/{agent_id}",
        },
    }
