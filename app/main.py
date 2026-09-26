"""
FastAPI application entry point for Resilient Integration Pipeline.

Establishes the core HTTP ingestion boundary (POST /webhook), database
lifespan initialization, and idempotency lifecycle protection.
"""

from contextlib import asynccontextmanager
import json
import sqlite3
from fastapi import FastAPI, HTTPException, Request, Response, status
from fastapi.responses import JSONResponse
from app.canonical import hash_payload
from app.database import (
    claim_idempotency_key,
    complete_idempotency_record,
    get_connection,
    init_db,
    rollback_idempotency_claim,
)
from app.schemas import WebhookPayload, WebhookResponse


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize database tables upon application startup."""
    init_db()
    yield


app = FastAPI(
    title="Resilient Integration Pipeline",
    description="Synchronous webhook ingestion pipeline demonstrating resilience patterns.",
    version="0.1.0",
    lifespan=lifespan,
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
async def ingest_webhook(payload: WebhookPayload, request: Request) -> Response:
    """
    Ingest, validate, canonicalize, hash, and process an incoming webhook event.

    Lifecycle:
    1. Validation: Pydantic v2 validates incoming schema.
    2. Canonicalization & Hashing: Computes SHA-256 digest of canonical representation.
    3. Idempotency Claim / Lookup:
       - First request: Atomically claimed with status=PROCESSING, transitioned to
         COMPLETED with cached response body, returns HTTP 200 OK.
       - Duplicate request (same key, same payload, status=COMPLETED):
         Returns cached HTTP 200 OK with header `X-Idempotent-Replay: true`.
       - Duplicate request (same key, different payload):
         Rejects with HTTP 409 Conflict.
       - Duplicate request (same key, status=PROCESSING):
         Rejects with HTTP 409 Conflict.
       - Duplicate request (same key, status=FAILED):
         Returns cached failure response (e.g. HTTP 502 Bad Gateway) with
         `X-Idempotent-Replay: true` per PROJECT_SPEC.md Case E.
       - Persistence / database failure:
         Rolls back transaction, un-poisons key, returns HTTP 500 Internal Server Error.
    """
    payload_hash = hash_payload(payload)

    try:
        conn = get_connection()
    except sqlite3.Error as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Database connection failure",
        ) from exc

    try:
        claim_outcome = claim_idempotency_key(conn, payload.event_id, payload_hash)

        if claim_outcome.is_new:
            # Case 1: First request
            response_data = WebhookResponse(
                status="accepted",
                event_id=payload.event_id,
                message="Event accepted by ingestion boundary",
            ).model_dump()
            response_json_str = json.dumps(response_data)

            try:
                complete_idempotency_record(
                    conn,
                    idempotency_key=payload.event_id,
                    status_code=status.HTTP_200_OK,
                    response_body=response_json_str,
                )
            except sqlite3.Error as exc:
                rollback_idempotency_claim(conn, payload.event_id)
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail="Database persistence error during completion",
                ) from exc

            return JSONResponse(
                status_code=status.HTTP_200_OK,
                content=response_data,
            )

        existing = claim_outcome.record

        # Case 3: Same key + different payload -> 409 Conflict
        if existing.payload_hash != payload_hash:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=(
                    f"Idempotency key '{payload.event_id}' has already been used "
                    "with a different payload"
                ),
            )

        # Case 4: Same key + PROCESSING -> 409 Conflict
        if existing.status == "PROCESSING":
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=(
                    f"A request with idempotency key '{payload.event_id}' "
                    "is currently being processed"
                ),
            )

        # Case 5: Same key + FAILED -> Return previous failure per Case E
        if existing.status == "FAILED":
            status_code = existing.response_status_code or status.HTTP_502_BAD_GATEWAY
            content = (
                json.loads(existing.response_body)
                if existing.response_body
                else {
                    "detail": (
                        f"Request with idempotency key '{payload.event_id}' "
                        "previously failed and cannot be reprocessed"
                    )
                }
            )
            return JSONResponse(
                status_code=status_code,
                content=content,
                headers={"X-Idempotent-Replay": "true"},
            )

        # Case 2: Same key + COMPLETED + same payload -> Return cached response
        if existing.status == "COMPLETED":
            content = (
                json.loads(existing.response_body)
                if existing.response_body
                else {
                    "status": "accepted",
                    "event_id": payload.event_id,
                    "message": "Event accepted by ingestion boundary",
                }
            )
            return JSONResponse(
                status_code=existing.response_status_code or status.HTTP_200_OK,
                content=content,
                headers={"X-Idempotent-Replay": "true"},
            )

        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Unhandled idempotency status '{existing.status}'",
        )

    except HTTPException:
        raise
    except sqlite3.Error as exc:
        rollback_idempotency_claim(conn, payload.event_id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Database persistence error",
        ) from exc
    finally:
        conn.close()

