"""
agentrx — Python SDK for the AgentRx Metacognitive Recovery API
"""

from .client    import AgentRxClient
from .decorator import with_recovery
from .models    import (
    ActionType,
    AgentRxError,
    FailureSignature,
    HumanHandoffRequired,
    PreflightResult,
    RecoveryAction,
    RecoveryException,
)

__version__ = "0.1.0"
__all__ = [
    "with_recovery",
    "AgentRxClient",
    "HumanHandoffRequired",
    "RecoveryException",
    "AgentRxError",
    "RecoveryAction",
    "PreflightResult",
    "ActionType",
    "FailureSignature",
]
