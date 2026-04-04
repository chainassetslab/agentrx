"""
agentrx.models
==============
Typed dataclasses matching the AgentRx API response shapes.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional


class ActionType(str, Enum):
    RELAX_SCHEMA       = "RELAX_SCHEMA"
    INJECT_KNOWLEDGE   = "INJECT_KNOWLEDGE"
    RETRY_WITH_BACKOFF = "RETRY_WITH_BACKOFF"
    HUMAN_HANDOFF      = "HUMAN_HANDOFF"
    REFRESH_AUTH       = "REFRESH_AUTH"
    SKIP_AND_CONTINUE  = "SKIP_AND_CONTINUE"
    ABORT              = "ABORT"


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


@dataclass
class RecoveryAction:
    action_type:        ActionType
    failure_signature:  FailureSignature
    confidence_score:   float
    trace_id:           str
    recovery_prompt:    Optional[str]            = None
    corrected_payload:  Optional[Dict[str, Any]] = None
    retry_after_ms:     Optional[int]            = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "RecoveryAction":
        return cls(
            action_type       = ActionType(data["action_type"]),
            failure_signature = FailureSignature(data["failure_signature"]),
            confidence_score  = float(data["confidence_score"]),
            trace_id          = data["trace_id"],
            recovery_prompt   = data.get("recovery_prompt"),
            corrected_payload = data.get("corrected_payload"),
            retry_after_ms    = data.get("retry_after_ms"),
        )

    @property
    def should_retry(self) -> bool:
        return self.action_type == ActionType.RETRY_WITH_BACKOFF

    @property
    def should_handoff(self) -> bool:
        return self.action_type == ActionType.HUMAN_HANDOFF

    @property
    def should_skip(self) -> bool:
        return self.action_type == ActionType.SKIP_AND_CONTINUE


@dataclass
class PreflightResult:
    proceed:              bool
    risk_score:           float
    warnings:             List[str]
    trace_id:             str
    suggested_correction: Optional[Dict[str, Any]]   = None
    predicted_signature:  Optional[FailureSignature] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "PreflightResult":
        sig = data.get("predicted_signature")
        return cls(
            proceed              = bool(data["proceed"]),
            risk_score           = float(data["risk_score"]),
            warnings             = data.get("warnings", []),
            trace_id             = data["trace_id"],
            suggested_correction = data.get("suggested_correction"),
            predicted_signature  = FailureSignature(sig) if sig else None,
        )

@dataclass
class AgentRxError(Exception):
    status_code: int
    detail:      str
    trace_id:    Optional[str] = None

    def __str__(self) -> str:
        tid = f" [trace_id={self.trace_id}]" if self.trace_id else ""
        return f"AgentRxError {self.status_code}: {self.detail}{tid}"


@dataclass
class HumanHandoffRequired(Exception):
    action:    RecoveryAction
    agent_id:  str
    tool_name: str

    def __str__(self) -> str:
        return (
            f"Agent '{self.agent_id}' requires human review. "
            f"Tool: '{self.tool_name}'. "
            f"Reason: {self.action.recovery_prompt or self.action.failure_signature}. "
            f"trace_id={self.action.trace_id}"
        )


@dataclass
class RecoveryException(Exception):
    action:         RecoveryAction
    original_error: Exception
    agent_id:       str
    tool_name:      str

    def __post_init__(self) -> None:
        self.__cause__ = self.original_error

    def __str__(self) -> str:
        return (
            f"AgentRx recovery required for agent '{self.agent_id}' "
            f"tool '{self.tool_name}': {self.action.action_type.value}. "
            f"Recovery prompt: {self.action.recovery_prompt or 'none'}. "
            f"Original error: {self.original_error}. "
            f"trace_id={self.action.trace_id}"
        )
