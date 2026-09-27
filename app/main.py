"""
FastAPI application entry point for Resilient Integration Pipeline.

Establishes the core HTTP ingestion boundary (POST /webhook), database
lifespan initialization, idempotency lifecycle protection, dead-letter queue (DLQ)
evidence persistence, and structured JSON lifecycle logging.
"""

from contextlib import asynccontextmanager
import json
import logging
import sqlite3
from fastapi import FastAPI, HTTPException, Request, Response, status
from fastapi.responses import JSONResponse
from app.canonical import canonicalize_payload, hash_canonical_json
from app.database import (
    claim_idempotency_key,
    complete_idempotency_record,
    get_connection,
    init_db,
    record_terminal_failure,
    rollback_idempotency_claim,
)
from app.destination import (
    execute_delivery,
    get_destination_adapter,
    get_retry_config,
)
from app.logging_conf import log_event, setup_logging
from app.schemas import WebhookPayload, WebhookResponse


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize structured logging and database tables upon application startup."""
    setup_logging()
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
    1. Ingestion: Pydantic v2 validates incoming schema. Raw request bytes preserved.
    2. Canonicalization & Hashing: Computes SHA-256 digest of canonical representation.
    3. Idempotency Claim / Lookup:
       - First request: Atomically claimed with status=PROCESSING.
       - Duplicate COMPLETED: Returns cached HTTP 200 OK with `X-Idempotent-Replay: true`.
       - Duplicate FAILED: Returns cached failure response with `X-Idempotent-Replay: true`
         without re-executing delivery or creating duplicate DLQ records.
       - Payload mismatch or in-flight PROCESSING: Rejects with HTTP 409 Conflict.
    4. Destination Delivery:
       - Bounded retry loop with exponential backoff via DestinationAdapter.
       - Success (200, 201): Transitions to COMPLETED, caches response, returns HTTP 200.
       - Terminal failure: Transitions to FAILED, persists complete forensic evidence
         (raw payload, normalized payload, attempt history) to Dead-Letter Queue (DLQ),
         and returns HTTP 502 Bad Gateway.
    5. Persistence Failure:
       - Rolls back transaction, un-poisons key, returns HTTP 500 Internal Server Error.
    """
    # 1. Capture exact raw payload bytes for forensic DLQ evidence
    raw_bytes = await request.body()
    raw_payload = raw_bytes.decode("utf-8", errors="replace")

    # 2. Compute canonical normalized representation and SHA-256 hash
    canonical_payload = canonicalize_payload(payload)
    payload_hash = hash_canonical_json(canonical_payload)

    # Log ingestion event
    log_event(
        "event_received",
        level=logging.INFO,
        event_id=payload.event_id,
        event_type=payload.event_type,
    )

    try:
        conn = get_connection()
    except sqlite3.Error as exc:
        log_event(
            "database_failure",
            level=logging.ERROR,
            event_id=payload.event_id,
            outcome="connection_error",
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Database connection failure",
        ) from exc

    try:
        claim_outcome = claim_idempotency_key(conn, payload.event_id, payload_hash)

        if claim_outcome.is_new:
            log_event(
                "idempotency_claimed",
                level=logging.INFO,
                event_id=payload.event_id,
                idempotency_status="PROCESSING",
            )

            # Deliver payload via destination adapter with bounded retries
            adapter = get_destination_adapter()
            retry_config = get_retry_config()
            delivery_result = execute_delivery(payload, adapter, retry_config)

            if delivery_result.success:
                log_event(
                    "delivery_success",
                    level=logging.INFO,
                    event_id=payload.event_id,
                    attempt=delivery_result.attempt_count,
                    outcome="success",
                )
                response_data = WebhookResponse(
                    status="accepted",
                    event_id=payload.event_id,
                    message="Event accepted and delivered successfully",
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
                    log_event(
                        "database_failure",
                        level=logging.ERROR,
                        event_id=payload.event_id,
                        outcome="rollback",
                    )
                    rollback_idempotency_claim(conn, payload.event_id)
                    raise HTTPException(
                        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                        detail="Database persistence error during completion",
                    ) from exc

                return JSONResponse(
                    status_code=status.HTTP_200_OK,
                    content=response_data,
                )
            else:
                # Terminal failure: retries exhausted or non-retryable error
                log_event(
                    "terminal_failure",
                    level=logging.ERROR,
                    event_id=payload.event_id,
                    attempt=delivery_result.attempt_count,
                    error_category=delivery_result.error_category,
                    outcome="failed",
                )
                failure_data = {
                    "detail": (
                        f"Destination delivery failed ({delivery_result.error_category}): "
                        f"{delivery_result.final_response.error_message or 'Delivery failure'}"
                    ),
                    "event_id": payload.event_id,
                    "attempt_count": delivery_result.attempt_count,
                    "error_category": delivery_result.error_category,
                }
                failure_json_str = json.dumps(failure_data)

                try:
                    dlq_id = record_terminal_failure(
                        conn=conn,
                        idempotency_key=payload.event_id,
                        raw_payload=raw_payload,
                        normalized_payload=canonical_payload,
                        error_category=delivery_result.error_category or "UNKNOWN",
                        http_status=delivery_result.final_response.status_code
                        if not delivery_result.final_response.is_timeout
                        else None,
                        error_message=delivery_result.final_response.error_message
                        or "Destination delivery failed",
                        attempt_count=delivery_result.attempt_count,
                        attempt_history=delivery_result.attempt_history,
                        status_code=status.HTTP_502_BAD_GATEWAY,
                        response_body=failure_json_str,
                    )
                    log_event(
                        "dlq_record_created",
                        level=logging.INFO,
                        event_id=payload.event_id,
                        dlq_id=dlq_id,
                        error_category=delivery_result.error_category,
                    )
                except sqlite3.Error as exc:
                    log_event(
                        "dlq_persistence_failure",
                        level=logging.ERROR,
                        event_id=payload.event_id,
                    )
                    rollback_idempotency_claim(conn, payload.event_id)
                    raise HTTPException(
                        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                        detail="Database persistence error during DLQ failure recording",
                    ) from exc

                return JSONResponse(
                    status_code=status.HTTP_502_BAD_GATEWAY,
                    content=failure_data,
                )

        existing = claim_outcome.record

        # Case 3: Same key + different payload -> 409 Conflict
        if existing.payload_hash != payload_hash:
            log_event(
                "payload_mismatch",
                level=logging.WARNING,
                event_id=payload.event_id,
                outcome="conflict_409",
            )
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=(
                    f"Idempotency key '{payload.event_id}' has already been used "
                    "with a different payload"
                ),
            )

        # Case 4: Same key + PROCESSING -> 409 Conflict
        if existing.status == "PROCESSING":
            log_event(
                "processing_conflict",
                level=logging.WARNING,
                event_id=payload.event_id,
                outcome="conflict_409",
            )
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=(
                    f"A request with idempotency key '{payload.event_id}' "
                    "is currently being processed"
                ),
            )

        # Case 5: Same key + FAILED -> Return previous failure per Case E without DLQ duplication
        if existing.status == "FAILED":
            log_event(
                "duplicate_failed_detected",
                level=logging.INFO,
                event_id=payload.event_id,
                idempotency_status="FAILED",
                outcome="replay_failure_502",
            )
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
            log_event(
                "duplicate_detected",
                level=logging.INFO,
                event_id=payload.event_id,
                idempotency_status="COMPLETED",
                outcome="replay",
            )
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
        log_event(
            "database_failure",
            level=logging.ERROR,
            event_id=payload.event_id,
            outcome="rollback",
        )
        rollback_idempotency_claim(conn, payload.event_id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Database persistence error",
        ) from exc
    finally:
        conn.close()


