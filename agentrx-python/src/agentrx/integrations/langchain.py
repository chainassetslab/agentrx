"""
agentrx.integrations.langchain
==============================
Async LangChain callback handler that intercepts tool failures, calls
AgentRx for a recovery action, and threads the recovery instruction back
into the agent's scratchpad.

Requires your AgentExecutor to be initialized with handle_tool_error=True
so the recovery instruction is treated as a correctable error instead of
halting execution.

Usage:
    from agentrx.integrations.langchain import AgentRxAsyncCallbackHandler
    from langchain.agents import AgentExecutor, initialize_agent

    handler = AgentRxAsyncCallbackHandler(
        api_key="your_api_key",
        agent_id="my_unique_agent_id",
        goal="Research and summarize recent AI papers",
    )

    agent = initialize_agent(
        tools,
        llm,
        callbacks=[handler],
        handle_tool_error=True,
    )

Dependencies:
    pip install httpx langchain-core
"""

from __future__ import annotations

import json
import logging
import time
import uuid
from typing import Any, Dict, Optional

import httpx

try:
    from langchain_core.callbacks import AsyncCallbackHandler
except ImportError:
    from langchain.callbacks.base import AsyncCallbackHandler  # type: ignore

logger = logging.getLogger("agentrx.langchain")

_DEFAULT_BASE_URL = "https://agentrx-production.up.railway.app"
_DEFAULT_TIMEOUT = 5.0


class AgentRxAsyncCallbackHandler(AsyncCallbackHandler):
    """
    Async callback handler that wires AgentRx into a LangChain agent.

    Thread-safe: uses run_id-keyed state storage so concurrent tool
    calls never overwrite each other's context. LangChain guarantees
    a unique run_id per tool invocation.

    Args:
        api_key:   Your AgentRx API key.
        agent_id:  REQUIRED. Unique identifier for this agent instance.
                   Sharing agent_ids across users or sessions causes
                   state pollution in AgentRx's classifier.
        base_url:  AgentRx API base URL. Defaults to production.
        timeout:   HTTP timeout in seconds for the recovery call.
        goal:      Description of what this agent is trying to accomplish.
                   Used by AgentRx to generate more specific recovery prompts.
    """

    def __init__(
        self,
        api_key: str,
        agent_id: str,
        base_url: str = _DEFAULT_BASE_URL,
        timeout: float = _DEFAULT_TIMEOUT,
        goal: str = "LangChain agent execution",
    ) -> None:
        super().__init__()

        if not api_key:
            raise ValueError("api_key must be provided.")
        if not agent_id or not agent_id.strip():
            raise ValueError(
                "agent_id must be a non-empty string to ensure per-agent state "
                "isolation in AgentRx. Do not use a shared default across users."
            )

        self._api_key = api_key
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout
        self._agent_id = agent_id
        self._goal = goal

        self._headers = {
            "X-API-Key": self._api_key,
            "Content-Type": "application/json",
        }

        # Thread-safe state storage keyed by LangChain run_id.
        # Using a dict instead of instance variables prevents concurrent
        # tool calls from overwriting each other's context. Each tool
        # invocation gets its own isolated state bucket.
        self._active_runs: Dict[str, Dict[str, Any]] = {}

    async def on_tool_start(
        self,
        serialized: Dict[str, Any],
        input_str: str,
        *,
        run_id: uuid.UUID,
        **kwargs: Any,
    ) -> None:
        """Capture tool context before execution, keyed by run_id."""
        tool_name = serialized.get("name", "unknown_tool")

        try:
            parsed = json.loads(input_str)
            current_input = parsed if isinstance(parsed, dict) else {"input": parsed}
        except (json.JSONDecodeError, TypeError):
            current_input = {"raw_input": input_str}

        self._active_runs[str(run_id)] = {
            "tool": tool_name,
            "input": current_input,
            "start_ts": time.time(),
        }

    async def on_tool_error(
        self,
        error: BaseException,
        *,
        run_id: uuid.UUID,
        **kwargs: Any,
    ) -> None:
        """Intercept tool failure and thread an AgentRx recovery action back."""
        run_state = self._active_runs.pop(str(run_id), None)
        if not run_state:
            logger.debug(
                "on_tool_error fired but no run state found for "
                f"run_id={run_id}; skipping AgentRx call."
            )
            return

        latency_ms = int((time.time() - run_state["start_ts"]) * 1000)
        payload = self._build_recovery_payload(
            error, latency_ms, run_state["tool"], run_state["input"]
        )

        try:
            async with httpx.AsyncClient(timeout=self._timeout) as client:
                response = await client.post(
                    f"{self._base_url}/v1/diagnose_and_recover",
                    headers=self._headers,
                    json=payload,
                )
        except httpx.TimeoutException:
            logger.warning(
                f"AgentRx recovery call timed out after {self._timeout}s. "
                f"Original error will propagate unchanged."
            )
            return
        except httpx.RequestError as e:
            logger.warning(
                f"AgentRx recovery call failed: {type(e).__name__}: {e}. "
                f"Original error will propagate unchanged."
            )
            return

        if response.status_code not in (200, 503):
            logger.warning(
                f"AgentRx returned unexpected status {response.status_code}: "
                f"{response.text[:200]}. Original error will propagate unchanged."
            )
            return

        try:
            data = response.json()
        except ValueError:
            logger.warning(
                f"AgentRx returned non-JSON response: {response.text[:200]}"
            )
            return

        instruction = data.get("openclaw_instruction") or data.get("recovery_prompt")
        action_type = data.get("action_type", "UNKNOWN")
        trace_id = data.get("trace_id", "unknown")
        confidence = data.get("confidence_score")

        if not instruction:
            logger.info(
                f"AgentRx returned no actionable instruction (trace_id={trace_id}). "
                f"Original error will propagate unchanged."
            )
            return

        logger.info(
            f"AgentRx recovery: action={action_type} "
            f"confidence={confidence} trace_id={trace_id}"
        )

        raise RuntimeError(
            f"AgentRx Recovery [{action_type}] (trace_id={trace_id}): {instruction}"
        ) from error

    def _build_recovery_payload(
        self,
        error: BaseException,
        latency_ms: int,
        tool_name: str,
        input_dict: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Build the /v1/diagnose_and_recover request body."""
        return {
            "state": self._build_state_payload(),
            "failure": {
                "mcp_tool_name": tool_name,
                "attempted_payload": input_dict,
                "error_response": {
                    "message": str(error),
                    "type": type(error).__name__,
                    "status_code": 0,
                },
                "latency_ms": latency_ms,
            },
        }

    def _build_state_payload(self) -> Dict[str, Any]:
        """
        Build the agent state block. Override in a subclass to inject
        richer goal tracking, active_plan, or execution_history from
        your LangChain agent's memory.
        """
        return {
            "agent_id": self._agent_id,
            "goal": self._goal,
            "active_plan": [],
            "execution_history": [],
        }
