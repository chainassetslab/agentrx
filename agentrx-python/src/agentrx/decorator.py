"""
agentrx.decorator
=================
The @with_recovery decorator — the primary entry point for the SDK.

    from agentrx import with_recovery

    @with_recovery(api_key="your_key", agent_id="my_agent")
    async def call_my_tool(payload: dict) -> dict:
        return await some_api.call(payload)
"""

from __future__ import annotations

import asyncio
import functools
import inspect
import time
from typing import Any, Callable, Dict, Optional

from .client import AgentRxClient
from .models import (
    ActionType,
    AgentRxError,
    HumanHandoffRequired,
    RecoveryAction,
    RecoveryException,
)


def with_recovery(
    api_key:           Optional[str]      = None,
    agent_id:          str                = "default_agent",
    goal:              str                = "Complete the current task",
    tool_schema:       Optional[Dict]     = None,
    max_retries:       int                = 2,
    base_url:          Optional[str]      = None,
    on_handoff:        Optional[Callable] = None,
    on_recovery:       Optional[Callable] = None,
    ignore_exceptions: tuple              = (),
) -> Callable:
    """
    Decorator that wraps an agent tool function with AgentRx recovery.
    Works on both async and sync functions.

    Args:
        api_key:           Your AgentRx API key.
        agent_id:          Stable identifier for this agent instance.
        goal:              The agent's current high-level goal.
        tool_schema:       JSON Schema dict for the wrapped tool.
        max_retries:       Maximum automatic retry attempts. Default 2.
        base_url:          AgentRx API URL.
        on_handoff:        Callback fired on HUMAN_HANDOFF.
        on_recovery:       Callback fired after every diagnosis.
        ignore_exceptions: Exception types to pass through without diagnosis.
    """
    def decorator(func: Callable) -> Callable:
        is_async = asyncio.iscoroutinefunction(func)

        @functools.wraps(func)
        async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
            return await _execute(
                func, args, kwargs, is_async=True,
                api_key=api_key, agent_id=agent_id, goal=goal,
                tool_schema=tool_schema, max_retries=max_retries,
                base_url=base_url, on_handoff=on_handoff,
                on_recovery=on_recovery, ignore_exceptions=ignore_exceptions,
            )

        @functools.wraps(func)
        def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                future = pool.submit(
                    asyncio.run,
                    _execute(
                        func, args, kwargs, is_async=False,
                        api_key=api_key, agent_id=agent_id, goal=goal,
                        tool_schema=tool_schema, max_retries=max_retries,
                        base_url=base_url, on_handoff=on_handoff,
                        on_recovery=on_recovery, ignore_exceptions=ignore_exceptions,
                    )
                )
                return future.result()

        return async_wrapper if is_async else sync_wrapper

    return decorator

async def _execute(
    func:              Callable,
    args:              tuple,
    kwargs:            dict,
    is_async:          bool,
    api_key:           Optional[str],
    agent_id:          str,
    goal:              str,
    tool_schema:       Optional[Dict],
    max_retries:       int,
    base_url:          Optional[str],
    on_handoff:        Optional[Callable],
    on_recovery:       Optional[Callable],
    ignore_exceptions: tuple,
) -> Any:
    current_payload = _extract_payload(func, args, kwargs)

    async with AgentRxClient(api_key=api_key, base_url=base_url) as rx:
        attempt = 0

        while attempt <= max_retries:
            start_ms = int(time.monotonic() * 1000)

            try:
                if is_async:
                    return await func(*args, **kwargs)
                else:
                    return await asyncio.get_event_loop().run_in_executor(
                        None, functools.partial(func, *args, **kwargs)
                    )

            except ignore_exceptions:
                raise

            except (KeyboardInterrupt, SystemExit):
                raise

            except Exception as exc:
                latency_ms     = int(time.monotonic() * 1000) - start_ms
                error_response = _extract_error_response(exc)

                try:
                    action = await rx.diagnose(
                        agent_id=agent_id,
                        goal=goal,
                        tool_name=func.__name__,
                        payload=current_payload or {},
                        error_response=error_response,
                        latency_ms=latency_ms,
                        tool_schema=tool_schema,
                    )
                except AgentRxError as rx_err:
                    raise type(exc)(
                        f"{exc} [AgentRx unavailable: {rx_err}]"
                    ) from exc

                if on_recovery:
                    await _call_maybe_async(on_recovery, action)

                if action.action_type == ActionType.RETRY_WITH_BACKOFF:
                    if attempt >= max_retries:
                        raise exc
                    wait_s = (action.retry_after_ms or 2000) / 1000
                    await asyncio.sleep(wait_s)
                    attempt += 1
                    continue

                elif action.action_type == ActionType.RELAX_SCHEMA:
                    if attempt >= max_retries:
                        raise exc
                    if action.corrected_payload:
                        args, kwargs = _inject_payload(
                            func, args, kwargs, action.corrected_payload
                        )
                        current_payload = action.corrected_payload
                    attempt += 1
                    continue

                elif action.action_type == ActionType.HUMAN_HANDOFF:
                    if on_handoff:
                        await _call_maybe_async(on_handoff, action)
                        return None
                    raise HumanHandoffRequired(
                        action=action,
                        agent_id=agent_id,
                        tool_name=func.__name__,
                    )

                elif action.action_type == ActionType.SKIP_AND_CONTINUE:
                    return None

                elif action.action_type == ActionType.ABORT:
                    raise exc

                else:
                    raise RecoveryException(
                        action=action,
                        original_error=exc,
                        agent_id=agent_id,
                        tool_name=func.__name__,
                    ) from exc

        raise RuntimeError("AgentRx decorator: retry loop exited unexpectedly")

def _extract_payload(
    func:   Callable,
    args:   tuple,
    kwargs: dict,
) -> Optional[Dict[str, Any]]:
    for name in ("payload", "params", "data", "body", "inputs"):
        if name in kwargs and isinstance(kwargs[name], dict):
            return kwargs[name]

    sig    = inspect.signature(func)
    params = list(sig.parameters.keys())
    offset = 1 if params and params[0] in ("self", "cls") else 0

    for arg in args[offset:]:
        if isinstance(arg, dict):
            return arg

    return None


def _inject_payload(
    func:              Callable,
    args:              tuple,
    kwargs:            dict,
    corrected_payload: Dict[str, Any],
) -> tuple[tuple, dict]:
    for name in ("payload", "params", "data", "body", "inputs"):
        if name in kwargs and isinstance(kwargs[name], dict):
            return args, {**kwargs, name: corrected_payload}

    sig    = inspect.signature(func)
    params = list(sig.parameters.keys())
    offset = 1 if params and params[0] in ("self", "cls") else 0

    new_args = list(args)
    for i, arg in enumerate(args[offset:], start=offset):
        if isinstance(arg, dict):
            new_args[i] = corrected_payload
            return tuple(new_args), kwargs

    return args, kwargs


def _extract_error_response(exc: Exception) -> Dict[str, Any]:
    response = getattr(exc, "response", None)
    if response is not None:
        status_code = getattr(response, "status_code", 0)
        try:
            body    = response.json()
            message = body.get("message") or body.get("detail") or str(body)
        except Exception:
            message = getattr(response, "text", str(exc))[:500]
        return {"status_code": status_code, "message": message}

    status_code = getattr(exc, "status_code", getattr(exc, "code", 0))
    return {
        "status_code": int(status_code) if status_code else 0,
        "message":     str(exc)[:500],
    }


async def _call_maybe_async(callback: Callable, *args: Any) -> None:
    if asyncio.iscoroutinefunction(callback):
        await callback(*args)
    else:
        callback(*args)
