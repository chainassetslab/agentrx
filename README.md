# AgentRx

**Metacognitive recovery API for AI agents.**

When your agents fail mid-task, they either hallucinate through it or give up.
AgentRx diagnoses the failure and tells your agent exactly what to do next —
automatically retrying, correcting payloads, or escalating to a human.

## Quick Start

pip install agentrx-sdk

from agentrx import with_recovery

@with_recovery(api_key="your_key", agent_id="my_agent")
async def call_my_tool(payload: dict) -> dict:
    return await your_existing_tool(payload)

That's it. Works with LangChain, CrewAI, PydanticAI, or plain Python.

## What It Does

When a tool call fails, AgentRx:
- RETRY_WITH_BACKOFF   — waits and retries automatically
- RELAX_SCHEMA        — corrects the payload and retries
- INJECT_KNOWLEDGE    — tells the agent exactly what went wrong
- HUMAN_HANDOFF       — alerts you via webhook when intervention is needed
- REFRESH_AUTH        — flags expired credentials
- SKIP_AND_CONTINUE   — bypasses deprecated tools

## Live API

Base URL: https://agentrx-production.up.railway.app
Docs:     https://agentrx-production.up.railway.app/docs

## Get an API Key

Contact: support@chainassetslab.com

## Built By

Chain Assets LLC — https://chainassetslab.com
