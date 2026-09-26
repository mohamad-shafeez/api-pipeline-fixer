"""
FastAPI application entry point for Resilient Integration Pipeline.

Establishes the core HTTP ingestion boundary (POST /webhook) and health
status check (GET /).
"""

from fastapi import FastAPI, Request, status
from app.schemas import WebhookPayload, WebhookResponse

app = FastAPI(
    title="Resilient Integration Pipeline",
    description="Synchronous webhook ingestion pipeline demonstrating resilience patterns.",
    version="0.1.0",
)


@app.get(
    "/",
    status_code=status.HTTP_200_OK,
    summary="Health & Service Information",
    tags=["Health"],
)
async def root() -> dict[str, str]:
    """Basic health check and service information endpoint."""
    return {
        "service": "Resilient Integration Pipeline",
        "status": "operational",
    }


@app.post(
    "/webhook",
    status_code=status.HTTP_200_OK,
    response_model=WebhookResponse,
    summary="Ingest Webhook Event",
    tags=["Ingestion"],
)
async def ingest_webhook(payload: WebhookPayload, request: Request) -> WebhookResponse:
    """
    Ingest and validate an incoming webhook event.

    HTTP Semantics:
    - Returns HTTP 200 OK with `{"status": "accepted", ...}` when the payload
      satisfies schema validation at the ingestion boundary.
    - Returns HTTP 422 Unprocessable Entity via FastAPI's standard validation
      boundary when required fields are missing, typed incorrectly, or the JSON
      is malformed.

    Raw Request Preservation:
    - Retains `request: Request` access so future phases can read `await request.body()`
      to compute the canonical SHA-256 idempotency hash and persist raw payload evidence
      in dead-letter records (DLQ) without changing the route signature.
    """
    return WebhookResponse(
        status="accepted",
        event_id=payload.event_id,
        message="Event accepted by ingestion boundary",
    )
