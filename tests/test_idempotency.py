"""
Unit and integration tests for SQLite persistence and idempotency lifecycle.

Verifies:
- Test 1: First request processes and transitions to COMPLETED.
- Test 2: Exact duplicate returns cached response with X-Idempotent-Replay header.
- Test 3: Same event_id with modified payload returns HTTP 409 Conflict.
- Test 4: Same event_id while status is PROCESSING returns HTTP 409 Conflict.
- Test 5: Existing FAILED record returns cached failure without retry storm (Case E).
- Test 6: Database uniqueness prevents duplicate ownership on concurrent claims.
- Test 7: Persistence failure triggers rollback, returns HTTP 500, and does not poison key.
- Test 8: Phase 3 canonical hashing is actively used for duplicate detection.
"""

from datetime import datetime, timezone
import json
import sqlite3
import pytest
from fastapi.testclient import TestClient
from app.canonical import hash_payload
from app.database import (
    claim_idempotency_key,
    get_connection,
    get_idempotency_record,
    record_idempotency_failure,
)
from app.main import app
from app.schemas import WebhookPayload


@pytest.fixture
def client() -> TestClient:
    """Fixture providing a FastAPI TestClient instance."""
    return TestClient(app)


def test_first_request_lifecycle(client: TestClient) -> None:
    """
    Test 1 — First request.

    Asserts:
    - HTTP 200 OK
    - Normal accepted response
    - Database record exists
    - Status becomes COMPLETED
    - Cached status code and body are stored
    """
    payload = {
        "event_id": "evt_test_first_001",
        "event_type": "order.created",
        "timestamp": "2026-09-26T12:00:00Z",
        "data": {"order_id": "ord_101", "amount": 250.00},
    }

    response = client.post("/webhook", json=payload)
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "accepted"
    assert data["event_id"] == "evt_test_first_001"
    assert "X-Idempotent-Replay" not in response.headers

    # Verify database record state
    conn = get_connection()
    try:
        record = get_idempotency_record(conn, "evt_test_first_001")
        assert record is not None
        assert record.idempotency_key == "evt_test_first_001"
        assert record.status == "COMPLETED"
        assert record.response_status_code == 200
        assert record.response_body is not None

        cached_body = json.loads(record.response_body)
        assert cached_body["status"] == "accepted"
        assert cached_body["event_id"] == "evt_test_first_001"
    finally:
        conn.close()


def test_exact_duplicate_returns_replay(client: TestClient) -> None:
    """
    Test 2 — Exact duplicate.

    Send the exact same payload twice.
    Asserts:
    - First request processes normally (HTTP 200, no replay header)
    - Second request returns HTTP 200
    - Second request has header 'X-Idempotent-Replay: true'
    - Cached response matches original response
    - Database contains exactly 1 record for this key
    """
    payload = {
        "event_id": "evt_test_duplicate_002",
        "event_type": "payment.processed",
        "timestamp": "2026-09-26T12:00:00Z",
        "data": {"payment_id": "pay_202", "amount": 75.50},
    }

    # First request
    resp1 = client.post("/webhook", json=payload)
    assert resp1.status_code == 200
    assert "X-Idempotent-Replay" not in resp1.headers

    # Second request (exact duplicate)
    resp2 = client.post("/webhook", json=payload)
    assert resp2.status_code == 200
    assert resp2.headers.get("X-Idempotent-Replay") == "true"
    assert resp2.json() == resp1.json()

    # Ensure no extra records were created
    conn = get_connection()
    try:
        cursor = conn.execute(
            "SELECT COUNT(*) FROM idempotency_records WHERE idempotency_key = ?",
            ("evt_test_duplicate_002",),
        )
        assert cursor.fetchone()[0] == 1
    finally:
        conn.close()


def test_same_event_id_different_payload_conflict(client: TestClient) -> None:
    """
    Test 3 — Same event_id, different payload.

    Send payload A, then send payload B with same event_id but changed data.
    Asserts:
    - First request succeeds (HTTP 200)
    - Second request returns HTTP 409 Conflict
    - Conflict error message clearly explains key was reused with differing payload
    - Original record remains untouched in COMPLETED state
    """
    payload_a = {
        "event_id": "evt_test_conflict_003",
        "event_type": "order.created",
        "timestamp": "2026-09-26T12:00:00Z",
        "data": {"amount": 50.00},
    }
    payload_b = {
        "event_id": "evt_test_conflict_003",
        "event_type": "order.created",
        "timestamp": "2026-09-26T12:00:00Z",
        "data": {"amount": 999.00},  # Changed data
    }

    resp1 = client.post("/webhook", json=payload_a)
    assert resp1.status_code == 200

    resp2 = client.post("/webhook", json=payload_b)
    assert resp2.status_code == 409
    assert "different payload" in resp2.json()["detail"].lower()

    # Verify original record payload_hash was not overwritten
    conn = get_connection()
    try:
        record = get_idempotency_record(conn, "evt_test_conflict_003")
        assert record is not None
        assert record.status == "COMPLETED"

        model_a = WebhookPayload.model_validate(payload_a)
        assert record.payload_hash == hash_payload(model_a)
    finally:
        conn.close()


def test_processing_duplicate_conflict(client: TestClient) -> None:
    """
    Test 4 — PROCESSING duplicate.

    Simulate an in-progress request by pre-creating a PROCESSING record.
    Asserts:
    - Arrival of duplicate request returns HTTP 409 Conflict
    - Detail message explains request is currently being processed
    - Record status remains PROCESSING
    - No duplicate record created
    """
    key = "evt_test_processing_004"
    payload = {
        "event_id": key,
        "event_type": "inventory.updated",
        "timestamp": "2026-09-26T12:00:00Z",
        "data": {"sku": "ITEM_99", "qty": 10},
    }
    model = WebhookPayload.model_validate(payload)
    p_hash = hash_payload(model)

    # Insert a PROCESSING record
    conn = get_connection()
    try:
        claim_idempotency_key(conn, key, p_hash)
        # Verify it is in PROCESSING status
        rec = get_idempotency_record(conn, key)
        assert rec is not None
        assert rec.status == "PROCESSING"
    finally:
        conn.close()

    # Send incoming request while key is PROCESSING
    response = client.post("/webhook", json=payload)
    assert response.status_code == 409
    assert "currently being processed" in response.json()["detail"].lower()

    # Verify state was not modified
    conn = get_connection()
    try:
        rec = get_idempotency_record(conn, key)
        assert rec is not None
        assert rec.status == "PROCESSING"
    finally:
        conn.close()


def test_failed_duplicate_handling(client: TestClient) -> None:
    """
    Test 5 — FAILED duplicate.

    Pre-create a FAILED idempotency record per PROJECT_SPEC.md Case E.
    Asserts:
    - System recognizes existing FAILED state
    - Returns cached failure response without re-executing or creating second record
    - Replay header is included
    - Status remains FAILED
    """
    key = "evt_test_failed_005"
    payload = {
        "event_id": key,
        "event_type": "order.created",
        "timestamp": "2026-09-26T12:00:00Z",
        "data": {"order_id": "ord_fail"},
    }
    model = WebhookPayload.model_validate(payload)
    p_hash = hash_payload(model)

    failed_response_body = json.dumps(
        {"detail": "Downstream destination rejected delivery: HTTP 502 Bad Gateway"}
    )

    conn = get_connection()
    try:
        record_idempotency_failure(
            conn=conn,
            idempotency_key=key,
            payload_hash=p_hash,
            status_code=502,
            response_body=failed_response_body,
        )
    finally:
        conn.close()

    # Send duplicate request
    response = client.post("/webhook", json=payload)
    assert response.status_code == 502
    assert response.headers.get("X-Idempotent-Replay") == "true"
    assert "downstream destination rejected" in response.json()["detail"].lower()

    # Verify record in DB remains FAILED and count is 1
    conn = get_connection()
    try:
        cursor = conn.execute(
            "SELECT status, COUNT(*) FROM idempotency_records WHERE idempotency_key = ?",
            (key,),
        )
        row = cursor.fetchone()
        assert row[0] == "FAILED"
        assert row[1] == 1
    finally:
        conn.close()


def test_database_uniqueness_atomic_claim() -> None:
    """
    Test 6 — Database uniqueness.

    Directly test atomic claim logic across two operations.
    Asserts:
    - First claim succeeds with is_new=True
    - Second claim on same key returns is_new=False
    - SQLite PRIMARY KEY constraint prevents multiple ownership
    """
    key = "evt_atomic_006"
    p_hash = "abc123hash"

    conn = get_connection()
    try:
        outcome1 = claim_idempotency_key(conn, key, p_hash)
        assert outcome1.is_new is True
        assert outcome1.record.status == "PROCESSING"

        outcome2 = claim_idempotency_key(conn, key, p_hash)
        assert outcome2.is_new is False
        assert outcome2.record.idempotency_key == key
        assert outcome2.record.status == "PROCESSING"

        # Direct insert violates constraint
        with pytest.raises(sqlite3.IntegrityError):
            with conn:
                conn.execute(
                    "INSERT INTO idempotency_records (idempotency_key, payload_hash, status, created_at, updated_at) "
                    "VALUES (?, ?, 'PROCESSING', '2026-01-01', '2026-01-01')",
                    (key, p_hash),
                )
    finally:
        conn.close()


def test_database_persistence_failure_and_rollback(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    Test 7 — Database rollback/failure.

    Simulate a database persistence failure during record completion.
    Asserts:
    - Persistence failure returns HTTP 500 Internal Server Error
    - Does NOT return HTTP 200
    - The key is NOT marked FAILED (infrastructure != business failure)
    - Orphaned PROCESSING claim is rolled back / cleaned up so key is not poisoned
    - Subsequent valid request succeeds with HTTP 200
    """
    import app.main

    key = "evt_fail_sim_007"
    payload = {
        "event_id": key,
        "event_type": "order.created",
        "timestamp": "2026-09-26T12:00:00Z",
        "data": {"order_id": "ord_sim"},
    }

    # Simulate database disk error on complete_idempotency_record
    def mock_complete_fail(*args, **kwargs):
        raise sqlite3.OperationalError("Simulated disk I/O error during completion")

    monkeypatch.setattr(app.main, "complete_idempotency_record", mock_complete_fail)

    # Execute request
    response = client.post("/webhook", json=payload)
    assert response.status_code == 500
    assert "database persistence error" in response.json()["detail"].lower()

    # Verify key was NOT left as FAILED or COMPLETED
    conn = get_connection()
    try:
        rec = get_idempotency_record(conn, key)
        # Because of rollback, either no record exists or it is not poisoned
        assert rec is None or rec.status != "FAILED"
    finally:
        conn.close()

    # Restore normal behavior and verify key is NOT poisoned (retry succeeds with HTTP 200)
    monkeypatch.undo()
    retry_response = client.post("/webhook", json=payload)
    assert retry_response.status_code == 200
    assert retry_response.json()["status"] == "accepted"


def test_different_payload_hash_used_for_comparison(client: TestClient) -> None:
    """
    Test 8 — Different payload hash.

    Verifies that Phase 3 canonical hashing is actively used by the endpoint
    for idempotency verification.
    """
    key = "evt_hash_test_008"
    payload_a = {
        "event_id": key,
        "event_type": "order.created",
        "timestamp": "2026-09-26T12:00:00Z",
        "data": {"amount": 100.00, "currency": "USD"},
    }
    model_a = WebhookPayload.model_validate(payload_a)
    expected_hash_a = hash_payload(model_a)

    resp_a = client.post("/webhook", json=payload_a)
    assert resp_a.status_code == 200

    # Verify stored hash matches Phase 3 hash_payload exactly
    conn = get_connection()
    try:
        rec = get_idempotency_record(conn, key)
        assert rec is not None
        assert rec.payload_hash == expected_hash_a
    finally:
        conn.close()

    # Changing any value alters Phase 3 hash and must trigger 409
    payload_b = {
        "event_id": key,
        "event_type": "order.created",
        "timestamp": "2026-09-26T12:00:00Z",
        "data": {"amount": 100.01, "currency": "USD"},  # 1 cent difference
    }
    model_b = WebhookPayload.model_validate(payload_b)
    expected_hash_b = hash_payload(model_b)
    assert expected_hash_a != expected_hash_b

    resp_b = client.post("/webhook", json=payload_b)
    assert resp_b.status_code == 409
