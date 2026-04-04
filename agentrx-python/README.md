# agentrx-python

Make your AI agents bulletproof in two lines.

## Installation

pip install agentrx-sdk

## Quick Start

from agentrx import with_recovery

@with_recovery(api_key="your_key", agent_id="my_agent")
async def call_my_tool(payload: dict) -> dict:
    return await some_api.call(payload)

When call_my_tool raises an exception, AgentRx diagnoses it and
automatically retries, corrects the payload, or tells you exactly
what went wrong.

## Environment Variables

AGENTRX_API_KEY  — Your API key (required)
AGENTRX_BASE_URL — AgentRx server URL (default: http://localhost:8000)
OTEL_TRACE_ID    — OpenTelemetry trace ID (optional)
LANGSMITH_RUN_ID — LangSmith run ID (optional)

## License

MIT
