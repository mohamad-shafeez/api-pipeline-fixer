"""
Unit and integration tests for Dead-Letter Queue (DLQ) and structured logging.

Verifies:
- Test 1: Retry exhaustion creates DLQ record with complete history
- Test 2: Non-retryable destination failure creates DLQ record
- Test 3: Timeout exhaustion creates DLQ record
- Test 4: Successful retry does NOT create DLQ record
- Test 5: Ingestion validation failure does NOT create DLQ record
- Test 6: Duplicate failed event does NOT create a second DLQ record
- Test 7: Different payload conflict does NOT create a DLQ record
- Test 8: DLQ preserves raw payload bytes intact
- Test 9: DLQ stores normalized canonical payload matching Phase 3
- Test 10: Attempt history persistence structure
- Test 11: Structured logging emits expected lifecycle events and context
- Test 12: DLQ inspection utility functions (list_dead_letters, get_dead_letter)
"""

import json
import logging
import pytest
from fastapi.testclient import TestClient
from app.canonical import canonicalize_payload
from app.database import get_connection, get_dlq_count, get_idempotency_record
from app.destination import (
    DestinationResponse,
    SimulatedDestinationAdapter,
    set_destination_adapter,
)
from app.dlq import get_dead_letter, list_dead_letters
from app.main import app
from app.schemas import WebhookPayload


@pytest.fixture
def client() -> TestClient:
    """Fixture providing a FastAPI TestClient instance."""
    return TestClient(app)


def test_retry_exhaustion_creates_dlq(client: TestClient) -> None:
    """
    Test 1 — Retry exhaustion creates DLQ.

    Destination: 503 -> 503 -> 503.
    Asserts:
    - HTTP 502 Bad Gateway
    - Idempotency status is FAILED
    - Exactly one DLQ record created
    - attempt_count = 3
    - attempt_history contains 3 sequential attempts
    """
    sim = SimulatedDestinationAdapter()
    sim.set_responses(
        [
            DestinationResponse(status_code=503, error_message="Unavailable 1"),
            DestinationResponse(status_code=503, error_message="Unavailable 2"),
            DestinationResponse(status_code=503, error_message="Unavailable 3"),
        ]
    )
    set_destination_adapter(sim)

    payload = {
        "event_id": "evt_dlq_exhausted_001",
        "event_type": "order.sync",
        "timestamp": "2026-09-27T01:00:00Z",
        "data": {"order_id": "ord_dlq_1"},
    }

    response = client.post("/webhook", json=payload)
    assert response.status_code == 502

    conn = get_connection()
    try:
        # Check idempotency record
        idem_rec = get_idempotency_record(conn, "evt_dlq_exhausted_001")
        assert idem_rec is not None
        assert idem_rec.status == "FAILED"
        assert idem_rec.response_status_code == 502

        # Check DLQ record
        assert get_dlq_count(conn, "evt_dlq_exhausted_001") == 1
        dlqs = list_dead_letters()
        assert len(dlqs) == 1
        dlq = dlqs[0]
        assert dlq.idempotency_key == "evt_dlq_exhausted_001"
        assert dlq.error_category == "SERVER_ERROR"
        assert dlq.http_status == 503
        assert dlq.attempt_count == 3
        assert len(dlq.attempt_history) == 3
        assert dlq.attempt_history[0]["attempt"] == 1
        assert dlq.attempt_history[1]["attempt"] == 2
        assert dlq.attempt_history[2]["attempt"] == 3
    finally:
        conn.close()


def test_non_retryable_destination_failure_creates_dlq(client: TestClient) -> None:
    """
    Test 2 — Non-retryable destination failure creates DLQ.

    Destination: 400.
    Asserts:
    - HTTP 502 Bad Gateway
    - Idempotency status is FAILED
    - Exactly one DLQ record created
    - attempt_count = 1
    """
    sim = SimulatedDestinationAdapter()
    sim.set_responses([DestinationResponse(status_code=400, error_message="Bad request")])
    set_destination_adapter(sim)

    payload = {
        "event_id": "evt_dlq_400_002",
        "event_type": "payment.charge",
        "timestamp": "2026-09-27T01:00:00Z",
        "data": {"amount": 500},
    }

    response = client.post("/webhook", json=payload)
    assert response.status_code == 502

    conn = get_connection()
    try:
        assert get_dlq_count(conn, "evt_dlq_400_002") == 1
        dlqs = list_dead_letters()
        assert len(dlqs) == 1
        dlq = dlqs[0]
        assert dlq.error_category == "CLIENT_ERROR"
        assert dlq.http_status == 400
        assert dlq.attempt_count == 1
        assert len(dlq.attempt_history) == 1
    finally:
        conn.close()


def test_timeout_exhaustion_creates_dlq(client: TestClient) -> None:
    """
    Test 3 — Timeout exhaustion creates DLQ.

    Destination: timeout -> timeout -> timeout.
    Asserts:
    - HTTP 502 Bad Gateway
    - Exactly one DLQ record created
    - error_category is TIMEOUT
    - attempt_count = 3
    """
    sim = SimulatedDestinationAdapter()
    sim.set_responses(
        [
            DestinationResponse(status_code=0, is_timeout=True, error_message="Timeout 1"),
            DestinationResponse(status_code=0, is_timeout=True, error_message="Timeout 2"),
            DestinationResponse(status_code=0, is_timeout=True, error_message="Timeout 3"),
        ]
    )
    set_destination_adapter(sim)

    payload = {
        "event_id": "evt_dlq_timeout_003",
        "event_type": "user.sync",
        "timestamp": "2026-09-27T01:00:00Z",
        "data": {"user_id": "usr_99"},
    }

    response = client.post("/webhook", json=payload)
    assert response.status_code == 502

    conn = get_connection()
    try:
        dlqs = list_dead_letters()
        assert len(dlqs) == 1
        dlq = dlqs[0]
        assert dlq.error_category == "TIMEOUT"
        assert dlq.http_status is None
        assert dlq.attempt_count == 3
    finally:
        conn.close()


def test_successful_retry_does_not_create_dlq(client: TestClient) -> None:
    """
    Test 4 — Successful retry does NOT create DLQ.

    Sequence: 503 -> 200.
    Asserts:
    - HTTP 200 OK
    - Idempotency status is COMPLETED
    - Exactly zero DLQ records
    """
    sim = SimulatedDestinationAdapter()
    sim.set_responses(
        [
            DestinationResponse(status_code=503, error_message="Temp error"),
            DestinationResponse(status_code=200, data={"ok": True}),
        ]
    )
    set_destination_adapter(sim)

    payload = {
        "event_id": "evt_success_retry_004",
        "event_type": "order.sync",
        "timestamp": "2026-09-27T01:00:00Z",
        "data": {"item": "notebook"},
    }

    response = client.post("/webhook", json=payload)
    assert response.status_code == 200

    conn = get_connection()
    try:
        assert get_dlq_count(conn) == 0
        rec = get_idempotency_record(conn, "evt_success_retry_004")
        assert rec is not None
        assert rec.status == "COMPLETED"
    finally:
        conn.close()


def test_validation_failure_does_not_create_dlq(client: TestClient) -> None:
    """
    Test 5 — Ingestion validation failure does NOT create DLQ.

    Payload missing event_id.
    Asserts:
    - HTTP 422 Unprocessable Entity
    - Exactly zero DLQ records created
    """
    invalid_payload = {
        "event_type": "order.sync",
        "timestamp": "2026-09-27T01:00:00Z",
        "data": {},
    }

    response = client.post("/webhook", json=invalid_payload)
    assert response.status_code == 422

    conn = get_connection()
    try:
        assert get_dlq_count(conn) == 0
    finally:
        conn.close()


def test_duplicate_failed_event_does_not_create_second_dlq(client: TestClient) -> None:
    """
    Test 6 — Duplicate failed event does NOT create second DLQ.

    First request exhausts retries -> FAILED -> 1 DLQ record.
    Second request with same key + payload -> returns cached 502.
    Asserts:
    - First request creates 1 DLQ record, call_count = 3
    - Second request returns HTTP 502 with X-Idempotent-Replay: true
    - Destination call count remains 3
    - DLQ count remains 1 (no duplicate DLQ records!)
    """
    sim = SimulatedDestinationAdapter()
    sim.set_responses(
        [
            DestinationResponse(status_code=503, error_message="Fail 1"),
            DestinationResponse(status_code=503, error_message="Fail 2"),
            DestinationResponse(status_code=503, error_message="Fail 3"),
        ]
    )
    set_destination_adapter(sim)

    payload = {
        "event_id": "evt_dlq_dup_006",
        "event_type": "order.sync",
        "timestamp": "2026-09-27T01:00:00Z",
        "data": {"order_id": "ord_dup_fail"},
    }

    # First request
    resp1 = client.post("/webhook", json=payload)
    assert resp1.status_code == 502
    assert sim.call_count == 3

    conn = get_connection()
    try:
        assert get_dlq_count(conn, "evt_dlq_dup_006") == 1
    finally:
        conn.close()

    # Second request (duplicate of failed event)
    resp2 = client.post("/webhook", json=payload)
    assert resp2.status_code == 502
    assert resp2.headers.get("X-Idempotent-Replay") == "true"
    assert sim.call_count == 3  # No new destination call!

    # DLQ count must still be exactly 1
    conn = get_connection()
    try:
        assert get_dlq_count(conn, "evt_dlq_dup_006") == 1
    finally:
        conn.close()


def test_different_payload_conflict_does_not_create_dlq(client: TestClient) -> None:
    """
    Test 7 — Different payload conflict does NOT create DLQ.

    First request reaches terminal failure.
    Second request with same event_id but changed payload.
    Asserts:
    - Second request returns HTTP 409 Conflict
    - DLQ count remains 1 (no extra DLQ record)
    """
    sim = SimulatedDestinationAdapter()
    sim.set_responses([DestinationResponse(status_code=400, error_message="Invalid")])
    set_destination_adapter(sim)

    payload_a = {
        "event_id": "evt_dlq_conflict_007",
        "event_type": "order.sync",
        "timestamp": "2026-09-27T01:00:00Z",
        "data": {"val": 10},
    }
    payload_b = {
        "event_id": "evt_dlq_conflict_007",
        "event_type": "order.sync",
        "timestamp": "2026-09-27T01:00:00Z",
        "data": {"val": 999},  # Changed
    }

    resp1 = client.post("/webhook", json=payload_a)
    assert resp1.status_code == 502

    resp2 = client.post("/webhook", json=payload_b)
    assert resp2.status_code == 409

    conn = get_connection()
    try:
        assert get_dlq_count(conn, "evt_dlq_conflict_007") == 1
    finally:
        conn.close()


def test_dlq_preserves_raw_payload(client: TestClient) -> None:
    """
    Test 8 — DLQ preserves raw payload bytes intact.

    Send a raw payload with arbitrary whitespace and formatting.
    Asserts:
    - Stored raw_payload in DLQ matches the exact incoming string byte-for-byte.
    """
    sim = SimulatedDestinationAdapter()
    sim.set_responses([DestinationResponse(status_code=400, error_message="Client error")])
    set_destination_adapter(sim)

    raw_json_string = (
        '{\n  "event_id":  "evt_raw_check_008",\n'
        '  "event_type":   "order.sync",\n'
        '  "timestamp": "2026-09-27T01:00:00Z",\n'
        '  "data":  {\n    "item": "keyboard",\n    "price": 89.99\n  }\n}'
    )

    response = client.post(
        "/webhook",
        content=raw_json_string,
        headers={"content-type": "application/json"},
    )
    assert response.status_code == 502

    dlqs = list_dead_letters()
    assert len(dlqs) == 1
    dlq = dlqs[0]
    assert dlq.raw_payload == raw_json_string


def test_dlq_stores_normalized_payload(client: TestClient) -> None:
    """
    Test 9 — DLQ stores normalized payload matching Phase 3 canonical output.

    Asserts:
    - DLQ normalized_payload field matches canonicalize_payload() output.
    """
    sim = SimulatedDestinationAdapter()
    sim.set_responses([DestinationResponse(status_code=400, error_message="Client error")])
    set_destination_adapter(sim)

    payload_dict = {
        "event_id": "evt_norm_check_009",
        "event_type": "order.sync",
        "timestamp": "2026-09-27T01:00:00Z",
        "data": {"b": 2, "a": 1},
    }

    response = client.post("/webhook", json=payload_dict)
    assert response.status_code == 502

    model = WebhookPayload.model_validate(payload_dict)
    expected_canonical = canonicalize_payload(model)

    dlqs = list_dead_letters()
    assert len(dlqs) == 1
    dlq = dlqs[0]
    assert dlq.normalized_payload == expected_canonical


def test_attempt_history_persistence(client: TestClient) -> None:
    """
    Test 10 — Attempt history persistence.

    Verify each attempt record in attempt_history includes:
    - attempt number
    - timestamp
    - status_code
    - timeout state
    - error_message
    """
    sim = SimulatedDestinationAdapter()
    sim.set_responses(
        [
            DestinationResponse(status_code=503, error_message="Gate 1"),
            DestinationResponse(status_code=0, is_timeout=True, error_message="Timeout 2"),
            DestinationResponse(status_code=502, error_message="Bad Gate 3"),
        ]
    )
    set_destination_adapter(sim)

    payload = {
        "event_id": "evt_hist_check_010",
        "event_type": "order.sync",
        "timestamp": "2026-09-27T01:00:00Z",
        "data": {"check": "history"},
    }

    response = client.post("/webhook", json=payload)
    assert response.status_code == 502

    dlqs = list_dead_letters()
    assert len(dlqs) == 1
    history = dlqs[0].attempt_history
    assert len(history) == 3

    assert history[0]["attempt"] == 1
    assert history[0]["status_code"] == 503
    assert history[0]["is_timeout"] is False
    assert "Gate 1" in history[0]["error_message"]

    assert history[1]["attempt"] == 2
    assert history[1]["status_code"] == 0
    assert history[1]["is_timeout"] is True
    assert "Timeout 2" in history[1]["error_message"]

    assert history[2]["attempt"] == 3
    assert history[2]["status_code"] == 502
    assert history[2]["is_timeout"] is False


def test_structured_logging(client: TestClient, caplog: pytest.LogCaptureFixture) -> None:
    """
    Test 11 — Structured logging.

    Asserts that important lifecycle events emit log records with structured context:
    - event_received
    - idempotency_claimed
    - delivery_attempt
    - retry_scheduled
    - terminal_failure
    - dlq_record_created
    """
    caplog.set_level(logging.INFO, logger="api_pipeline")

    sim = SimulatedDestinationAdapter()
    sim.set_responses(
        [
            DestinationResponse(status_code=503, error_message="Err 1"),
            DestinationResponse(status_code=400, error_message="Err 2"),
        ]
    )
    set_destination_adapter(sim)

    payload = {
        "event_id": "evt_logging_011",
        "event_type": "audit.test",
        "timestamp": "2026-09-27T01:00:00Z",
        "data": {"test": "logging"},
    }

    response = client.post("/webhook", json=payload)
    assert response.status_code == 502

    # Verify structured logs captured
    events_logged = [
        getattr(record, "event", None) for record in caplog.records
    ]

    assert "event_received" in events_logged
    assert "idempotency_claimed" in events_logged
    assert "delivery_attempt" in events_logged
    assert "retry_scheduled" in events_logged
    assert "terminal_failure" in events_logged
    assert "dlq_record_created" in events_logged

    # Verify contextual attribute presence on captured records
    event_received_rec = next(
        r for r in caplog.records if getattr(r, "event", None) == "event_received"
    )
    assert event_received_rec.event_id == "evt_logging_011"
    assert event_received_rec.event_type == "audit.test"

    dlq_rec = next(
        r for r in caplog.records if getattr(r, "event", None) == "dlq_record_created"
    )
    assert dlq_rec.event_id == "evt_logging_011"
    assert dlq_rec.dlq_id is not None


def test_dlq_inspection_utilities(client: TestClient) -> None:
    """
    Test 12 — DLQ inspection utility functions.

    Verifies list_dead_letters and get_dead_letter work as expected.
    """
    sim = SimulatedDestinationAdapter()
    sim.set_responses([DestinationResponse(status_code=400, error_message="Fail 1")])
    set_destination_adapter(sim)

    payload = {
        "event_id": "evt_dlq_inspect_012",
        "event_type": "order.sync",
        "timestamp": "2026-09-27T01:00:00Z",
        "data": {"val": 1},
    }

    client.post("/webhook", json=payload)

    # Test list_dead_letters
    all_dlq = list_dead_letters()
    assert len(all_dlq) == 1
    record_id = all_dlq[0].id

    # Test get_dead_letter
    single = get_dead_letter(record_id)
    assert single is not None
    assert single.id == record_id
    assert single.idempotency_key == "evt_dlq_inspect_012"

    # Non-existent ID returns None
    assert get_dead_letter(99999) is None
