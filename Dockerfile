# ─────────────────────────────────────────────
# AgentRx Dockerfile
# Used by both the API service and the webhook
# worker — the CMD is overridden per-service in
# docker-compose.yml so we only need one image.
# ─────────────────────────────────────────────

FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN addgroup --system agentrx && \
    adduser --system --ingroup agentrx agentrx

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY agentrx_v2.py .
COPY webhook_worker.py .

# Document the port this container listens on.
# Helps Railway auto-detect the ingress port.
EXPOSE 8000

# Switch to non-root user
USER agentrx

# Shell form so Railway's $PORT variable is expanded.
# Fallback to 8000 for local Docker runs.
CMD uvicorn agentrx_v2:app --host 0.0.0.0 --port ${PORT:-8000} --workers 2
