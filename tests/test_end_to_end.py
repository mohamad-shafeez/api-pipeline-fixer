"""
tests/test_end_to_end.py

Comprehensive end-to-end pipeline integration tests verifying the full lifecycle
from HTTP ingestion through validation, canonicalization, idempotency claim,
destination adapter invocation, retry/backoff, DLQ persistence, and replay caching.
"""

from unittest.mock import patch
from fastapi.testclient import TestClient
import pytest
from app.database import get_connection, get_dlq_count, get_idempotency_record
from app.destination import DestinationResponse, SimulatedDestinationAdapter
from app.main import app

client = TestClient(app)


def test_e2e_scenario_a_valid_event_success_completed(
    isolated_test_db: str, reset_destination_simulator: SimulatedDestinationAdapter
):
    """
    Scenario A: Valid event -> successful destination (200) -> 200 accepted -> COMPLETED in DB, no DLQ.
    """
    payload = {
        "event_id": "evt_e2e_a_001",
        "event_type": "payment.succeeded",
        "timestamp": "2026-09-27T10:00:00Z",
        "data": {"order_id": "ord_1001", "amount": 150.0},
    }

    response = client.post("/webhook", json=payload)
    assert response.status_code == 200
    assert response.json()["status"] == "accepted"
    assert response.json()["event_id"] == "evt_e2e_a_001"

    # Verify DB state
    with get_connection(isolated_test_db) as conn:
        record = get_idempotency_record(conn, "evt_e2e_a_001")
        assert record is not None
        assert record.status == "COMPLETED"
        assert record.response_status_code == 200

        dlq_count = get_dlq_count(conn)
        assert dlq_count == 0

    assert reset_destination_simulator.call_count == 1


def test_e2e_scenario_b_retryable_503_then_success_completed_no_dlq(
    isolated_test_db: str, reset_destination_simulator: SimulatedDestinationAdapter
):
    """
    Scenario B: Valid event -> 503 -> retry -> success (200) -> 200 accepted -> COMPLETED in DB -> no DLQ.
    """
    reset_destination_simulator.set_responses(
        [
            DestinationResponse(status_code=503, error_message="Service Unavailable"),
            DestinationResponse(status_code=200, data={"status": "delivered"}),
        ]
    )

    payload = {
        "event_id": "evt_e2e_b_002",
        "event_type": "invoice.created",
        "timestamp": "2026-09-27T10:05:00Z",
        "data": {"invoice_id": "inv_2002"},
    }

    response = client.post("/webhook", json=payload)
    assert response.status_code == 200
    assert response.json()["status"] == "accepted"

    with get_connection(isolated_test_db) as conn:
        record = get_idempotency_record(conn, "evt_e2e_b_002")
        assert record is not None
        assert record.status == "COMPLETED"

        dlq_count = get_dlq_count(conn)
        assert dlq_count == 0

    assert reset_destination_simulator.call_count == 2


def test_e2e_scenario_c_retryable_exhaustion_creates_dlq(
    isolated_test_db: str, reset_destination_simulator: SimulatedDestinationAdapter
):
    """
    Scenario C: Valid event -> retryable failure (503 -> 503 -> 503) -> retries exhausted ->
    HTTP 502 -> FAILED in DB -> exactly one DLQ record with attempt_count=3.
    """
    reset_destination_simulator.set_responses(
        [
            DestinationResponse(status_code=503, error_message="Unavailable 1"),
            DestinationResponse(status_code=503, error_message="Unavailable 2"),
            DestinationResponse(status_code=503, error_message="Unavailable 3"),
        ]
    )

    payload = {
        "event_id": "evt_e2e_c_003",
        "event_type": "order.cancelled",
        "timestamp": "2026-09-27T10:10:00Z",
        "data": {"order_id": "ord_3003"},
    }

    response = client.post("/webhook", json=payload)
    assert response.status_code == 502
    assert response.json()["attempt_count"] == 3
    assert response.json()["error_category"] == "SERVER_ERROR"

    with get_connection(isolated_test_db) as conn:
        record = get_idempotency_record(conn, "evt_e2e_c_003")
        assert record is not None
        assert record.status == "FAILED"

        dlq_count = get_dlq_count(conn)
        assert dlq_count == 1

        cursor = conn.cursor()
        cursor.execute("SELECT * FROM dead_letter_records WHERE idempotency_key = ?", ("evt_e2e_c_003",))
        row = cursor.fetchone()
        assert row is not None
        assert row["attempt_count"] == 3
        assert row["error_category"] == "SERVER_ERROR"

    assert reset_destination_simulator.call_count == 3


def test_e2e_scenario_d_non_retryable_400_creates_dlq(
    isolated_test_db: str, reset_destination_simulator: SimulatedDestinationAdapter
):
    """
    Scenario D: Valid event -> non-retryable 400 -> HTTP 502 -> FAILED in DB ->
    exactly one DLQ record with attempt_count=1 (no retries).
    """
    reset_destination_simulator.set_responses(
        [DestinationResponse(status_code=400, error_message="Bad Request: Schema Mismatch")]
    )

    payload = {
        "event_id": "evt_e2e_d_004",
        "event_type": "user.created",
        "timestamp": "2026-09-27T10:15:00Z",
        "data": {"user_id": "usr_4004"},
    }

    response = client.post("/webhook", json=payload)
    assert response.status_code == 502
    assert response.json()["attempt_count"] == 1
    assert response.json()["error_category"] == "CLIENT_ERROR"

    with get_connection(isolated_test_db) as conn:
        record = get_idempotency_record(conn, "evt_e2e_d_004")
        assert record is not None
        assert record.status == "FAILED"

        dlq_count = get_dlq_count(conn)
        assert dlq_count == 1

        cursor = conn.cursor()
        cursor.execute("SELECT * FROM dead_letter_records WHERE idempotency_key = ?", ("evt_e2e_d_004",))
        row = cursor.fetchone()
        assert row is not None
        assert row["attempt_count"] == 1
        assert row["error_category"] == "CLIENT_ERROR"

    assert reset_destination_simulator.call_count == 1


def test_e2e_scenario_e_failed_duplicate_replay_cached_502(
    isolated_test_db: str, reset_destination_simulator: SimulatedDestinationAdapter
):
    """
    Scenario E: Failed event replay -> cached 502 -> X-Idempotent-Replay: true ->
    no second destination call -> no second DLQ record.
    """
    reset_destination_simulator.set_responses(
        [
            DestinationResponse(status_code=503, error_message="Downstream Down 1"),
            DestinationResponse(status_code=503, error_message="Downstream Down 2"),
            DestinationResponse(status_code=503, error_message="Downstream Down 3"),
        ]
    )

    payload = {
        "event_id": "evt_e2e_e_005",
        "event_type": "subscription.renewed",
        "timestamp": "2026-09-27T10:20:00Z",
        "data": {"sub_id": "sub_5005"},
    }

    # Initial request -> 502 terminal failure
    resp1 = client.post("/webhook", json=payload)
    assert resp1.status_code == 502
    assert "X-Idempotent-Replay" not in resp1.headers
    assert reset_destination_simulator.call_count == 3

    with get_connection(isolated_test_db) as conn:
        assert get_dlq_count(conn) == 1

    # Exact duplicate request -> cached 502 replay
    resp2 = client.post("/webhook", json=payload)
    assert resp2.status_code == 502
    assert resp2.headers.get("X-Idempotent-Replay") == "true"
    assert resp2.json()["event_id"] == "evt_e2e_e_005"

    # Destination calls must remain 3 (no new call)
    assert reset_destination_simulator.call_count == 3

    # DLQ count must remain 1 (no duplicate record)
    with get_connection(isolated_test_db) as conn:
        assert get_dlq_count(conn) == 1


def test_e2e_scenario_f_same_event_id_changed_payload_conflict_409(
    isolated_test_db: str, reset_destination_simulator: SimulatedDestinationAdapter
):
    """
    Scenario F: Same event_id with changed payload -> HTTP 409 Conflict ->
    no destination delivery -> no additional DLQ record.
    """
    payload_orig = {
        "event_id": "evt_e2e_f_006",
        "event_type": "transfer.initiated",
        "timestamp": "2026-09-27T10:25:00Z",
        "data": {"amount": 500},
    }

    resp1 = client.post("/webhook", json=payload_orig)
    assert resp1.status_code == 200
    assert reset_destination_simulator.call_count == 1

    # Re-send same event_id with different payload data
    payload_changed = {
        "event_id": "evt_e2e_f_006",
        "event_type": "transfer.initiated",
        "timestamp": "2026-09-27T10:25:00Z",
        "data": {"amount": 99999},  # Changed amount
    }

    resp2 = client.post("/webhook", json=payload_changed)
    assert resp2.status_code == 409
    assert "different payload" in resp2.json()["detail"]

    # Destination calls must not increase
    assert reset_destination_simulator.call_count == 1

    # No DLQ record
    with get_connection(isolated_test_db) as conn:
        assert get_dlq_count(conn) == 0


def test_e2e_scenario_g_invalid_incoming_payload_422_no_dlq(
    isolated_test_db: str, reset_destination_simulator: SimulatedDestinationAdapter
):
    """
    Scenario G: Invalid incoming payload -> HTTP 422 -> no destination delivery -> no DLQ.
    """
    invalid_payload = {
        # missing event_id
        "event_type": "test.event",
        "timestamp": "not-a-timestamp",
        "data": {"key": "val"},
    }

    response = client.post("/webhook", json=invalid_payload)
    assert response.status_code == 422
    assert reset_destination_simulator.call_count == 0

    with get_connection(isolated_test_db) as conn:
        assert get_dlq_count(conn) == 0


def test_e2e_scenario_h_database_failure_rollback_500_no_false_completion(
    isolated_test_db: str, reset_destination_simulator: SimulatedDestinationAdapter
):
    """
    Scenario H: Database persistence failure during completion -> rollback -> HTTP 500 ->
    no falsely completed event in database.
    """
    payload = {
        "event_id": "evt_e2e_h_008",
        "event_type": "payout.processed",
        "timestamp": "2026-09-27T10:35:00Z",
        "data": {"payout_id": "pay_8008"},
    }

    import sqlite3

    with patch("app.main.complete_idempotency_record", side_effect=sqlite3.OperationalError("disk I/O error")):
        response = client.post("/webhook", json=payload)
        assert response.status_code == 500
        assert "Database persistence error" in response.json()["detail"]

    # Verify claim was rolled back, not left in COMPLETED
    with get_connection(isolated_test_db) as conn:
        record = get_idempotency_record(conn, "evt_e2e_h_008")
        assert record is None or record.status != "COMPLETED"
