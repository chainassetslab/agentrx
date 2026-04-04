# AgentRx — Railway Deployment Guide

## Before You Start
You need:
- A Railway account (railway.app)
- This repo pushed to GitHub
- Files in repo root: Dockerfile, railway.toml, requirements.txt, agentrx_v2.py, webhook_worker.py

## Step 1 — Create the Railway Project
1. Go to railway.app → "New Project"
2. Select "Deploy from GitHub repo"
3. Select the agentrx repo
4. Rename the service to "agentrx-api"

## Step 2 — Add the Redis Plugin
1. Click "+ New" → "Database" → "Add Redis"
2. Wait ~30 seconds for Railway to provision it

## Step 3 — Configure API Environment Variables
1. Click "agentrx-api" service → "Variables" tab → "Raw Editor"
2. Paste:

AGENTRX_REDIS_URL=${{Redis.REDIS_URL}}
AGENTRX_API_KEYS=YOUR_STRONG_KEY_HERE
AGENTRX_AGENT_STATE_TTL_SECONDS=3600
AGENTRX_LOOP_DETECTION_THRESHOLD=20
AGENTRX_RATE_LIMIT=60/minute
AGENTRX_MIN_AUTO_CONFIDENCE=0.70
AGENTRX_ENVIRONMENT=production
AGENTRX_WEBHOOK_URL=
AGENTRX_WEBHOOK_TIMEOUT_SECONDS=5
PORT=8000

3. Generate a strong key first:
   python3 -c "import secrets; print(secrets.token_urlsafe(32))"
4. Click "Update Variables"

## Step 4 — Add the Worker Service
1. Click "+ New" → "GitHub Repo" → select agentrx repo
2. Rename to "agentrx-worker"
3. Settings → Custom Start Command → enable toggle
   Enter: python webhook_worker.py
4. IMPORTANT: Settings → Deploy → Healthcheck Path → clear the field
5. Variables → Raw Editor → paste:

AGENTRX_REDIS_URL=${{Redis.REDIS_URL}}
AGENTRX_ENVIRONMENT=production

6. Click "Update Variables"

## Step 5 — Generate Your Public URL
1. Click "agentrx-api" service → Settings → Networking
2. Public Networking → "Generate Domain"
3. Save the URL — this is your AGENTRX_BASE_URL

## Step 6 — Verify the Deployment
Run these checks with your live URL:

CHECK 1 — Liveness:
GET https://YOUR-URL.up.railway.app/health
Expected: {"status": "ok"}

CHECK 2 — Readiness (confirms Redis connected):
GET https://YOUR-URL.up.railway.app/ready
Expected: {"status": "ready", "redis": "ok"}

CHECK 3 — Auth rejection:
POST https://YOUR-URL.up.railway.app/v1/diagnose_and_recover
Headers: (no X-API-Key)
Expected: 401 Unauthorized

CHECK 4 — Full request:
POST https://YOUR-URL.up.railway.app/v1/diagnose_and_recover
Headers: X-API-Key: YOUR_STRONG_KEY_HERE
Body:
{
  "state": {
    "agent_id": "deploy_test_001",
    "goal": "Verify deployment",
    "active_plan": [],
    "execution_history": [{"step": 1}]
  },
  "failure": {
    "mcp_tool_name": "test_tool",
    "attempted_payload": {"amount": "500"},
    "error_response": {"message": "validation error", "status_code": 422},
    "latency_ms": 100
  }
}
Expected: 200 with RecoveryAction body

CHECK 5 — Swagger UI:
GET https://YOUR-URL.up.railway.app/docs
Expected: Interactive docs page loads

## Troubleshooting
- /ready returns error: Redis not linked — recheck Step 2 and 3
- Worker crashes: confirm start command is exactly "python webhook_worker.py"
- Auth fails: confirm key in AGENTRX_API_KEYS matches X-API-Key header exactly
