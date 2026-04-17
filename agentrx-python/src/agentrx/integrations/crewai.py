"""
agentrx.integrations.crewai — v2
================================
Async-aware decorator that wraps CrewAI tool functions with AgentRx
recovery. On tool failure, calls AgentRx's OpenClaw recovery endpoint
and returns the recovery instruction as the tool's string output.

Usage:
    from crewai.tools import tool
    from agentrx.integrations.crewai import with_agentrx

    @tool("Search Web")
    @with_agentrx(
        api_key="your_key",
        agent_id="unique_crew_researcher_id",
        goal="Research competitors in the EV market",
    )
    def search_web(query: str) -> str:
        ...

Dependencies:
    pip install httpx
"""

from __future__ import annotations

import functools
import inspect
import json
import logging
import time
from typing import Any, Callable, Dict, Optional

import httpx

logger = logging.getLogger("agentrx.crewai")

_DEFAULT_BASE_URL = "https://agentrx-production.up.railway.app"
_DEFAULT_TIMEOUT = 5.0


def _serialize_args(args: tuple, kwargs: Dict[str, Any]) -> Dict[str, Any]:
    def _safe(value: Any) -> Any:
        try:
            json.dumps(value)
            return value
        except (TypeError, ValueError):
            return repr(value)
    return {
        "args": [_safe(a) for a in args],
        "kwargs": {k: _safe(v) for k, v in kwargs.items()},
    }


def _handle_response(response: Optional[httpx.Response]) -> Optional[Dict[str, Any]]:
    if response is None:
        return None
    if response.status_code not in (200, 503):
        logger.warning(
            f"AgentRx returned unexpected status {response.status_code}: "
            f"{response.text[:200]}"
        )
        return None
    try:
        return response.json()
    except ValueError:
        logger.warning(f"AgentRx returned non-JSON: {response.text[:200]}")
        return None


async def _call_agentrx_async(
    base_url: str,
    api_key: str,
    payload: Dict[str, Any],
    timeout: float,
) -> Optional[Dict[str, Any]]:
    headers = {"X-API-Key": api_key, "Content-Type": "application/json"}
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.post(
                f"{base_url}/v1/openclaw/recover",
                headers=headers,
                json=payload,
            )
    except httpx.TimeoutException:
        logger.warning(f"AgentRx recovery call timed out after {timeout}s.")
        return None
    except httpx.RequestError as e:
        logger.warning(f"AgentRx recovery call failed: {type(e).__name__}: {e}")
        return None
    return _handle_response(response)


def _call_agentrx_sync(
    base_url: str,
    api_key: str,
    payload: Dict[str, Any],
    timeout: float,
) -> Optional[Dict[str, Any]]:
    headers = {"X-API-Key": api_key, "Content-Type": "application/json"}
    try:
        with httpx.Client(timeout=timeout) as client:
            response = client.post(
                f"{base_url}/v1/openclaw/recover",
                headers=headers,
                json=payload,
            )
    except httpx.TimeoutException:
        logger.warning(f"AgentRx recovery call timed out after {timeout}s.")
        return None
    except httpx.RequestError as e:
        logger.warning(f"AgentRx recovery call failed: {type(e).__name__}: {e}")
        return None
    return _handle_response(response)


def _format_recovery_output(
    data: Optional[Dict[str, Any]],
    original_error: BaseException,
    tool_name: str,
) -> str:
    if data is None:
        return (
            f"Error executing {tool_name}: {original_error}. "
            f"(AgentRx was unreachable; no recovery instruction available.)"
        )
    instruction = data.get("openclaw_instruction") or data.get("recovery_prompt")
    action_type = data.get("action_type", "UNKNOWN")
    trace_id = data.get("trace_id", "unknown")
    if not instruction:
        return (
            f"Error executing {tool_name}: {original_error}. "
            f"(AgentRx trace_id={trace_id}, no actionable instruction returned.)"
        )
    return (
        f"TOOL FAILED: {tool_name}\n"
        f"Original error: {original_error}\n"
        f"AgentRx Recovery [{action_type}] (trace_id={trace_id}): {instruction}"
    )


def _build_payload(
    agent_id: str,
    tool_name: str,
    error: BaseException,
    latency_ms: int,
    args: tuple,
    kwargs: Dict[str, Any],
    goal: str = "CrewAI task execution",
) -> Dict[str, Any]:
    return {
        "agent_id": agent_id,
        "tool_name": tool_name,
        "error_message": str(error),
        "error_code": 0,
        "latency_ms": latency_ms,
        "attempted_payload": _serialize_args(args, kwargs),
        "goal": goal,
    }


def with_agentrx(
    api_key: str,
    agent_id: str,
    base_url: str = _DEFAULT_BASE_URL,
    timeout: float = _DEFAULT_TIMEOUT,
    goal: str = "CrewAI task execution",
) -> Callable:
    """
    Decorator factory that wraps a CrewAI tool function with AgentRx recovery.

    Args:
        api_key:  Your AgentRx API key.
        agent_id: Unique identifier for this agent. Required.
        base_url: AgentRx API base URL. Defaults to production.
        timeout:  HTTP timeout in seconds for the recovery call.
        goal:     Description of what this agent is trying to accomplish.
                  Used by AgentRx to generate more specific recovery prompts.
    """
    if not api_key:
        raise ValueError("api_key must be provided.")
    if not agent_id or not agent_id.strip():
        raise ValueError(
            "agent_id must be a non-empty string to ensure per-agent state "
            "isolation in AgentRx. Do not use a shared default across crews."
        )

    def decorator(func: Callable) -> Callable:
        is_async = inspect.iscoroutinefunction(func)
        tool_name = getattr(func, "__name__", "unknown_tool")

        if is_async:
            @functools.wraps(func)
            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                start = time.time()
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    latency_ms = int((time.time() - start) * 1000)
                    payload = _build_payload(
                        agent_id, tool_name, e, latency_ms, args, kwargs, goal
                    )
                    data = await _call_agentrx_async(base_url, api_key, payload, timeout)
                    return _format_recovery_output(data, e, tool_name)
            return async_wrapper
        else:
            @functools.wraps(func)
            def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
                start = time.time()
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    latency_ms = int((time.time() - start) * 1000)
                    payload = _build_payload(
                        agent_id, tool_name, e, latency_ms, args, kwargs, goal
                    )
                    data = _call_agentrx_sync(base_url, api_key, payload, timeout)
                    return _format_recovery_output(data, e, tool_name)
            return sync_wrapper

    return decorator
