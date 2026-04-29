# AgentRx

Production recovery layer for AI agents. When your tool calls fail, AgentRx classifies the failure and tells your agent exactly what to do next.

Native integrations for LangChain, CrewAI, and OpenClaw. Live API, real production traces, drop-in installation.

- Live API: https://agentrx-production.up.railway.app/docs
- PyPI: https://pypi.org/project/agentrx-sdk
- ClawHub: https://clawhub.ai/skills/agentrx
- Website: https://chainassetslab.com

---

## Installation

Pick the integration path that matches your stack.

Python SDK (any framework):

    pip install agentrx-sdk

LangChain agents:

    pip install "agentrx-sdk[langchain]"

CrewAI agents:

    pip install "agentrx-sdk[crewai]"

OpenClaw skills (no Python required):

    npx clawhub install agentrx

---

## Quick Start

Get a free trial key for evaluation:

    AGENTRX_API_KEY=beta_openclaw_try_agentrx_2026
    AGENTRX_BASE_URL=https://agentrx-production.up.railway.app

Verify the integration is live:

    curl -s -X GET "${AGENTRX_BASE_URL}/v1/openclaw/status" -H "X-API-Key: ${AGENTRX_API_KEY}"

The shared trial key is rate-limited at 20 requests per minute per IP. For production use or higher limits, see Pricing below or email chainassetslab@gmail.com for a dedicated key.

---

## Real Production Example

This is a real failure AgentRx caught in production. AI marketing agent on a DigitalOcean droplet hits an Anthropic API rate limit, screenshots are forwarded for awareness, the agent calls AgentRx twice in quick succession with the same parameters.

Request:

    curl -X POST "https://agentrx-production.up.railway.app/v1/openclaw/recover" \
      -H "X-API-Key: $AGENTRX_API_KEY" \
      -H "Content-Type: application/json" \
      -d '{
        "agent_id": "lamar_cmo",
        "tool_name": "anthropic_api",
        "error_message": "API rate limit reached. Please try again later.",
        "error_code": 429
      }'

Response:

    {
      "trace_id": "58e94db3-7f17-44ae-b19d-d1d74eb15169",
      "action_type": "HUMAN_HANDOFF",
      "failure_signature": "AGENT_LOOP",
      "confidence_score": 1.0,
      "openclaw_instruction": "STOP EXECUTION. Do not retry anthropic_api. Alert operator immediately. Reason: Loop detected — tool called repeatedly with identical parameters.",
      "retry_after_ms": null,
      "corrected_payload": null
    }

What happened: AgentRx detected that the recovery calls themselves were forming a loop pattern, not just the underlying tool. It refused to keep spinning and surfaced HUMAN_HANDOFF with confidence 1.0. This is second-order failure detection — the system caught itself looping on its own recovery calls.

---

## Failure Signatures

AgentRx classifies tool failures into 10 signatures:

- AGENT_LOOP — repeated tool calls forming a loop pattern
- AUTH_FAILURE — credentials expired or invalid
- RATE_LIMIT_EXCEEDED — upstream service throttling the agent
- NETWORK_LATENCY — slow or hanging network calls
- HALLUCINATED_PARAM — payload field that does not exist in the tool schema
- HALLUCINATED_VALUE — value that violates schema constraints
- SCHEMA_MISMATCH — payload structure does not match expected schema
- RESOURCE_MISSING — referenced resource (file, record, ID) does not exist
- TOOL_DEPRECATED — endpoint or method has been removed
- UNKNOWN — unrecognized failure pattern, falls back to safe defaults

---

## Recovery Action Types

For every failure, AgentRx returns one of these action_type values:

- HUMAN_HANDOFF — stop execution, surface to operator
- RETRY_WITH_BACKOFF — wait retry_after_ms then retry
- RELAX_SCHEMA — retry with corrected_payload
- SKIP_AND_CONTINUE — bypass the failed tool, proceed with next step
- REFRESH_AUTH — get fresh credentials before retrying
- INJECT_KNOWLEDGE — supplemental context returned in recovery_prompt
- ABORT — failure is unrecoverable, stop the task

---

## Pricing

Free — $0/month
- Shared beta key (beta_openclaw_try_agentrx_2026)
- 20 requests/minute per IP
- No SLA, community support via GitHub issues

Pro — $29/month
- Dedicated API key
- 120 requests/minute
- Webhook delivery
- Heartbeat monitoring
- Email support (48hr response)

Team — $99/month
- 5 dedicated API keys
- 120 requests/minute per key
- Isolated tenant environment
- Priority email support
- 99.9% uptime SLA

Enterprise — Contact us
- Self-hosted Docker deployment
- Custom SLA
- On-call support
- Email chainassetslab@gmail.com

For dedicated keys or upgrades, email chainassetslab@gmail.com or visit https://chainassetslab.com

---

## Security and Privacy

AgentRx is a remote service. When your agent calls AgentRx, the following data is transmitted:

- agent_id — your agent identifier
- tool_name — the name of the failed tool
- error_message — the error text returned
- attempted_payload — the payload sent to the failed tool

Sanitize sensitive data before calling AgentRx. Never include credentials, API keys, payment details, or PII in error messages or payloads. The trace_id in every response links to server-side logs retained per your tier.

AgentRx makes recommendations, not decisions. Your agent always retains authority over what actions to take. Treat openclaw_instruction as advisory — evaluate every suggestion before acting on it.

---

## Why AgentRx

AI agents fail in ways their developers never see — tool errors, silent crashes, rate limit blindness, hallucinated parameters, recovery loops. Most agents either retry forever or give up. AgentRx classifies the failure and returns a specific recovery action, so your agent can recover gracefully or hand off to a human at the right moment.

Built by Chain Assets LLC. Production infrastructure for AI agents — recovery, monitoring, and reliability tooling for teams deploying agents in the real world.

---

## Links

- Live API and Docs: https://agentrx-production.up.railway.app/docs
- PyPI Package: https://pypi.org/project/agentrx-sdk
- ClawHub Skill: https://clawhub.ai/skills/agentrx
- GitHub: https://github.com/chainassetslab/agentrx
- OpenClaw Skill Repo: https://github.com/chainassetslab/agentrx-openclaw
- Website: https://chainassetslab.com
- Contact: chainassetslab@gmail.com

---

## License

MIT — see LICENSE file.
