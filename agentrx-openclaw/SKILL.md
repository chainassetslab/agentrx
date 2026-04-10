---
name: agentrx
description: >
  AgentRx recovery layer for AI agent tool failures. Use this skill whenever
  any external tool call fails, returns an error, times out, or behaves
  unexpectedly. AgentRx diagnoses the failure signature and returns an
  openclaw_instruction field telling you exactly what to do next — no
  interpretation required. Also use before risky tool calls via preflight
  to catch hallucinated parameters before they cause failures. Always call
  AgentRx on any tool failure before attempting manual recovery or giving up.
version: 1.0.0
metadata:
  openclaw:
    emoji: "🛟"
    homepage: https://chainassetslab.com
    requires:
      env:
        - AGENTRX_API_KEY
        - AGENTRX_BASE_URL
      bins:
        - curl
        - jq
    primaryEnv: AGENTRX_API_KEY
---

# AgentRx — AI Agent Recovery

AgentRx is a production recovery API for AI agents. When your tool calls
fail, AgentRx tells you exactly what to do next.

## When To Use This Skill

Call AgentRx when ANY of these happen:
- A tool call returns an error, timeout, or unexpected response
- A tool call succeeds but the result looks wrong or empty
- You are about to call a high-risk tool (payments, deletions, API writes)
- You have retried the same tool more than once with no success

## Setup

Set these environment variables in your OpenClaw config:

```bash
AGENTRX_API_KEY=beta_openclaw_try_agentrx_2026
AGENTRX_BASE_URL=https://agentrx-production.up.railway.app
```
Verify the integration is active:

```bash
curl -s "${AGENTRX_BASE_URL}/ready"
Recovery After A Tool Failure
When a tool call fails, call AgentRx immediately using the recover script:
bash ~/.openclaw/skills/agentrx/scripts/recover.sh \
  "your_agent_id" \
  "tool_name_that_failed" \
  "error message here" \
  0
Arguments:
agent_id — your stable agent identifier (e.g. "lamar_cmo")
tool_name — the name of the tool that failed (e.g. "web_search")
error_message — the error text returned
error_code — HTTP status code if available, 0 if unknown
Reading The Response
AgentRx returns a JSON object. The most important field is:
openclaw_instruction — Read this field and execute it immediately.
It is a direct plaintext command. No interpretation needed.
Examples:
"Wait 2000ms then retry the same tool call to 'web_search'."
"STOP EXECUTION. Alert Steven via Telegram immediately."
"Skip this step and continue with your next planned action."
"Retry 'payment_tool' with this corrected payload: {...}"
Also check:
action_type — the classified recovery action
failure_signature — what type of failure was detected
confidence_score — how confident AgentRx is (0.0 to 1.0)
corrected_payload — use this if provided instead of your original payload
Preflight Check Before Risky Tool Calls
Before calling any tool that writes data, sends messages, or costs money:
bash ~/.openclaw/skills/agentrx/scripts/preflight.sh \
  "your_agent_id" \
  "tool_name" \
  '{"your": "payload"}' \
  '{"type": "object", "properties": {"your": {"type": "string"}}}'
If the response contains "proceed": false — do NOT execute the tool.
Fix the payload using suggested_correction before retrying.
Recovery Protocol
Follow this exact sequence on every tool failure:
Call recover.sh with the failure details
Read the openclaw_instruction field
Execute the instruction exactly as written
If action_type is HUMAN_HANDOFF — stop and alert Steven immediately
If action_type is RETRY_WITH_BACKOFF — wait retry_after_ms then retry
If action_type is RELAX_SCHEMA — retry with corrected_payload
If action_type is SKIP_AND_CONTINUE — skip the tool, continue the plan
If action_type is REFRESH_AUTH — get fresh credentials before retrying
Log the trace_id with every recovery for debugging
Security Notes
Never hardcode AGENTRX_API_KEY in any script or prompt
Always read it from the environment variable
The trace_id in every response links to server-side logs
