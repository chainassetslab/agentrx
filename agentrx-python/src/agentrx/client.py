"""
agentrx.client
==============
Async HTTP client for the AgentRx API.
"""

from __future__ import annotations

import hashlib
import json
import os
import time
from typing import Any, Dict, List, Optional

import httpx

from .models import (
    AgentRxError,
    PreflightResult,
    RecoveryAction,
)

_DEFAULT_BASE_URL = "http://localhost:8000"

# Global schema cache — persists across client instances for the
# lifetime of the Python process. Prevents re-uploading the same
# schema on every tool failure when the decorator creates a new
# client instance each call.
_GLOBAL_SCHEMA_CACHE: Dict[str, bool] = {}


class AgentRxClient:
    """
    Async HTTP client for the AgentRx API.

    Usage as async context manager (recommended):
        async with AgentRxClient(api_key="...") as rx:
            action = await rx.diagnose(...)

    Args:
        api_key:  Your AgentRx API key. Falls back to AGENTRX_API_KEY env var.
        base_url: AgentRx API base URL. Falls back to AGENTRX_BASE_URL env var.
        timeout:  HTTP timeout in seconds. Default 10.
        trace_id: Optional trace ID to propagate to AgentRx logs.
    """

    def __init__(
        self,
        api_key:  Optional[str] = None,
        base_url: Optional[str] = None,
        timeout:  float = 10.0,
        trace_id: Optional[str] = None,
    ) -> None:
        self._api_key  = api_key  or os.environ.get("AGENTRX_API_KEY", "")
        self._base_url = (base_url or os.environ.get("AGENTRX_BASE_URL", _DEFAULT_BASE_URL)).rstrip("/")
        self._timeout  = timeout
        self._trace_id = trace_id
        self._schema_cache = _GLOBAL_SCHEMA_CACHE
        self._http: Optional[httpx.AsyncClient] = None

    async def open(self) -> None:
        if not self._api_key:
            raise ValueError(
                "No API key provided. Pass api_key= or set AGENTRX_API_KEY env var."
            )
        self._http = httpx.AsyncClient(
            base_url=self._base_url,
            timeout=self._timeout,
            headers=self._build_headers(),
        )

    async def close(self) -> None:
        if self._http:
            await self._http.aclose()
            self._http = None

    async def __aenter__(self) -> "AgentRxClient":
        await self.open()
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self.close()

    async def diagnose(
        self,
        agent_id:          str,
        goal:              str,
        tool_name:         str,
        payload:           Dict[str, Any],
        error_response:    Dict[str, Any],
        latency_ms:        int                   = 0,
        execution_history: List[Dict[str, Any]]  = None,
        active_plan:       List[str]             = None,
        tool_schema:       Optional[Dict[str, Any]] = None,
    ) -> RecoveryAction:
        self._ensure_open()

        schema_hash = None
        if tool_schema is not None:
            schema_hash = await self._ensure_schema_cached(tool_schema)

        body = {
            "state": {
                "agent_id":          agent_id,
                "goal":              goal,
                "active_plan":       active_plan or [],
                "execution_history": execution_history or [],
            },
            "failure": {
                "mcp_tool_name":     tool_name,
                "attempted_payload": payload,
                "error_response":    error_response,
                "latency_ms":        latency_ms,
            },
        }
        if schema_hash:
            body["schema_hash"] = schema_hash

        response = await self._post("/v1/diagnose_and_recover", body)
        return RecoveryAction.from_dict(response)

    async def preflight(
        self,
        agent_id:    str,
        tool_name:   str,
        payload:     Dict[str, Any],
        tool_schema: Dict[str, Any],
    ) -> PreflightResult:
        self._ensure_open()

        schema_hash = await self._ensure_schema_cached(tool_schema)

        body = {
            "agent_id":         agent_id,
            "mcp_tool_name":    tool_name,
            "intended_payload": payload,
            "schema_hash":      schema_hash,
        }

        response = await self._post("/v1/preflight", body)
        return PreflightResult.from_dict(response)

    async def register_schema(self, tool_schema: Dict[str, Any]) -> str:
        self._ensure_open()
        response    = await self._post("/v1/schema/register", {"tool_schema": tool_schema})
        schema_hash = response["schema_hash"]
        _GLOBAL_SCHEMA_CACHE[schema_hash] = True
        return schema_hash

    async def clear_agent_state(self, agent_id: str) -> Dict[str, Any]:
        self._ensure_open()
        resp = await self._http.delete(f"/v1/state/{agent_id}")
        return self._handle_response(resp)

    def _build_headers(self) -> Dict[str, str]:
        headers: Dict[str, str] = {
            "X-API-Key":    self._api_key,
            "Content-Type": "application/json",
            "User-Agent":   "agentrx-python/0.1.0",
        }
        trace_id = (
            self._trace_id
            or os.environ.get("OTEL_TRACE_ID")
            or os.environ.get("LANGSMITH_RUN_ID")
        )
        if trace_id:
            headers["X-Trace-Id"] = trace_id
        return headers

    async def _ensure_schema_cached(self, tool_schema: Dict[str, Any]) -> str:
        serialized  = json.dumps(tool_schema, sort_keys=True, separators=(",", ":"))
        schema_hash = hashlib.sha256(serialized.encode()).hexdigest()

        if not _GLOBAL_SCHEMA_CACHE.get(schema_hash):
            await self.register_schema(tool_schema)

        return schema_hash

    async def _post(self, path: str, body: Dict[str, Any]) -> Dict[str, Any]:
        resp = await self._http.post(path, json=body)
        return self._handle_response(resp)

    def _handle_response(self, resp: httpx.Response) -> Dict[str, Any]:
        try:
            data = resp.json()
        except Exception:
            raise AgentRxError(
                status_code=resp.status_code,
                detail=f"Non-JSON response from AgentRx: {resp.text[:200]}",
            )

        if resp.status_code in (200, 503):
            return data

        if resp.status_code == 428:
            raise AgentRxError(
                status_code=428,
                detail=(
                    "Schema not cached on AgentRx server. "
                    f"Detail: {data.get('detail', data)}"
                ),
                trace_id=data.get("trace_id"),
            )

        detail = data.get("detail") or data.get("message") or str(data)
        raise AgentRxError(
            status_code=resp.status_code,
            detail=str(detail),
            trace_id=data.get("trace_id"),
        )

    def _ensure_open(self) -> None:
        if self._http is None:
            raise RuntimeError(
                "AgentRxClient is not open. "
                "Use 'async with AgentRxClient(...) as rx:' or call await rx.open() first."
            )
