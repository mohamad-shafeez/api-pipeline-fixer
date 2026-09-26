"""
Unit and integration tests for destination adapter, retry policy, and backoff engine.

Verifies:
- Test 1: Immediate success (200 OK -> 1 attempt, COMPLETED, HTTP 200)
- Test 2: 201 success (201 Created -> 1 attempt, COMPLETED, HTTP 200)
- Test 3: Retryable 503 then success (503 -> 200 -> 2 attempts, COMPLETED, HTTP 200)
- Test 4: Retryable failures exhausted (503 -> 503 -> 503 -> 3 attempts, FAILED, HTTP 502)
- Test 5: 429 retry (429 -> 200 -> 2 attempts, COMPLETED)
- Test 6: 500, 502, 504 retry classification (All classified as retryable)
- Test 7: 400 non-retryable (400 -> 1 attempt, FAILED, no retry)
- Test 8: 401, 403, 422 non-retryable (All classified as non-retryable)
- Test 9: Timeout retry (timeout -> 200 -> 2 attempts, COMPLETED)
- Test 10: Timeout exhaustion (timeout -> timeout -> timeout -> 3 attempts, FAILED, HTTP 502)
- Test 11: Backoff calculation (1, 2, capped at 10)
- Test 12: No retry after success (503 -> 200 -> 503 -> only 2 attempts consumed)
- Test 13: Duplicate completed event (no extra destination call, cached replay)
- Test 14: Duplicate failed event (no extra destination call, cached terminal replay)
"""

import pytest
from fastapi.testclient import TestClient
from app.database import get_connection, get_idempotency_record
from app.destination import (
    DestinationResponse,
    RetryConfig,
    SimulatedDestinationAdapter,
    calculate_backoff,
    classify_failure,
    is_retryable,
    set_destination_adapter,
)
from app.main import app


@pytest.fixture
def client() -> TestClient:
    """Fixture providing a FastAPI TestClient instance."""
    return TestClient(app)


def test_immediate_success_200(client: TestClient) -> None:
    """
    Test 1 — Immediate success (200).

    Asserts:
    - Exactly 1 attempt made
    - Status in database becomes COMPLETED
    - Webhook returns HTTP 200 OK
    """
    sim = SimulatedDestinationAdapter()
    sim.set_responses([DestinationResponse(status_code=200, data={"ok": True})])
    set_destination_adapter(sim)

    payload = {
        "event_id": "evt_test_success_200",
        "event_type": "order.completed",
        "timestamp": "2026-09-26T12:00:00Z",
        "data": {"order_id": "ord_100"},
    }

    response = client.post("/webhook", json=payload)
    assert response.status_code == 200
    assert sim.call_count == 1

    conn = get_connection()
    try:
        rec = get_idempotency_record(conn, "evt_test_success_200")
        assert rec is not None
        assert rec.status == "COMPLETED"
        assert rec.response_status_code == 200
    finally:
        conn.close()


def test_immediate_success_201(client: TestClient) -> None:
    """
    Test 2 — 201 success.

    Asserts:
    - Destination returns 201 Created
    - Exactly 1 attempt made
    - Status becomes COMPLETED
    - Webhook returns HTTP 200 OK
    """
    sim = SimulatedDestinationAdapter()
    sim.set_responses([DestinationResponse(status_code=201, data={"created": True})])
    set_destination_adapter(sim)

    payload = {
        "event_id": "evt_test_success_201",
        "event_type": "item.created",
        "timestamp": "2026-09-26T12:00:00Z",
        "data": {"item_id": "item_201"},
    }

    response = client.post("/webhook", json=payload)
    assert response.status_code == 200
    assert sim.call_count == 1

    conn = get_connection()
    try:
        rec = get_idempotency_record(conn, "evt_test_success_201")
        assert rec is not None
        assert rec.status == "COMPLETED"
    finally:
        conn.close()


def test_retryable_503_then_success(client: TestClient) -> None:
    """
    Test 3 — Retryable 503 then success.

    Simulate 503 -> 200.
    Asserts:
    - Retries once (total 2 attempts)
    - Final result is success (HTTP 200)
    - Status becomes COMPLETED
    """
    sim = SimulatedDestinationAdapter()
    sim.set_responses(
        [
            DestinationResponse(status_code=503, error_message="Service Unavailable"),
            DestinationResponse(status_code=200, data={"ok": True}),
        ]
    )
    set_destination_adapter(sim)

    payload = {
        "event_id": "evt_test_503_success",
        "event_type": "payment.processed",
        "timestamp": "2026-09-26T12:00:00Z",
        "data": {"amount": 50.00},
    }

    response = client.post("/webhook", json=payload)
    assert response.status_code == 200
    assert sim.call_count == 2

    conn = get_connection()
    try:
        rec = get_idempotency_record(conn, "evt_test_503_success")
        assert rec is not None
        assert rec.status == "COMPLETED"
    finally:
        conn.close()


def test_retryable_failures_exhausted(client: TestClient) -> None:
    """
    Test 4 — Retryable failures exhausted.

    Simulate 503 -> 503 -> 503.
    Asserts:
    - Exactly 3 destination attempts made (no fourth attempt)
    - Webhook returns HTTP 502 Bad Gateway
    - Status becomes FAILED
    """
    sim = SimulatedDestinationAdapter()
    sim.set_responses(
        [
            DestinationResponse(status_code=503, error_message="Service Unavailable"),
            DestinationResponse(status_code=503, error_message="Service Unavailable"),
            DestinationResponse(status_code=503, error_message="Service Unavailable"),
        ]
    )
    set_destination_adapter(sim)

    payload = {
        "event_id": "evt_test_exhausted_503",
        "event_type": "payment.processed",
        "timestamp": "2026-09-26T12:00:00Z",
        "data": {"amount": 75.00},
    }

    response = client.post("/webhook", json=payload)
    assert response.status_code == 502
    assert sim.call_count == 3

    conn = get_connection()
    try:
        rec = get_idempotency_record(conn, "evt_test_exhausted_503")
        assert rec is not None
        assert rec.status == "FAILED"
        assert rec.response_status_code == 502
    finally:
        conn.close()


def test_rate_limit_429_retry(client: TestClient) -> None:
    """
    Test 5 — 429 retry.

    Simulate 429 -> 200.
    Asserts:
    - Retries on HTTP 429 (total 2 attempts)
    - Status becomes COMPLETED
    """
    sim = SimulatedDestinationAdapter()
    sim.set_responses(
        [
            DestinationResponse(status_code=429, error_message="Rate limit exceeded"),
            DestinationResponse(status_code=200, data={"ok": True}),
        ]
    )
    set_destination_adapter(sim)

    payload = {
        "event_id": "evt_test_429_retry",
        "event_type": "order.sync",
        "timestamp": "2026-09-26T12:00:00Z",
        "data": {"id": 1},
    }

    response = client.post("/webhook", json=payload)
    assert response.status_code == 200
    assert sim.call_count == 2

    conn = get_connection()
    try:
        rec = get_idempotency_record(conn, "evt_test_429_retry")
        assert rec is not None
        assert rec.status == "COMPLETED"
    finally:
        conn.close()


def test_server_errors_retry_classification() -> None:
    """
    Test 6 — 500/502/504 retry classification.

    Verify each server error code is classified as retryable.
    """
    for code in (500, 502, 503, 504):
        resp = DestinationResponse(status_code=code, error_message=f"HTTP {code}")
        assert is_retryable(resp) is True
        assert classify_failure(resp) == "SERVER_ERROR"

    # Also verify 429
    resp_429 = DestinationResponse(status_code=429, error_message="HTTP 429")
    assert is_retryable(resp_429) is True
    assert classify_failure(resp_429) == "RATE_LIMIT"


def test_400_non_retryable(client: TestClient) -> None:
    """
    Test 7 — 400 non-retryable.

    Simulate downstream 400 Bad Request.
    Asserts:
    - Exactly 1 destination attempt made
    - Zero retries triggered
    - Status becomes FAILED
    - Webhook returns HTTP 502 Bad Gateway
    """
    sim = SimulatedDestinationAdapter()
    sim.set_responses(
        [
            DestinationResponse(status_code=400, error_message="Invalid downstream body"),
            DestinationResponse(status_code=200),  # Should never be called
        ]
    )
    set_destination_adapter(sim)

    payload = {
        "event_id": "evt_test_400_non_retryable",
        "event_type": "order.sync",
        "timestamp": "2026-09-26T12:00:00Z",
        "data": {"id": 2},
    }

    response = client.post("/webhook", json=payload)
    assert response.status_code == 502
    assert sim.call_count == 1  # No second attempt

    conn = get_connection()
    try:
        rec = get_idempotency_record(conn, "evt_test_400_non_retryable")
        assert rec is not None
        assert rec.status == "FAILED"
    finally:
        conn.close()


def test_401_403_422_non_retryable() -> None:
    """
    Test 8 — 401/403/422 non-retryable.

    Verify all permanent client errors are classified as non-retryable.
    """
    for code in (400, 401, 403, 422):
        resp = DestinationResponse(status_code=code, error_message=f"Client error {code}")
        assert is_retryable(resp) is False
        assert classify_failure(resp) == "CLIENT_ERROR"


def test_timeout_retry(client: TestClient) -> None:
    """
    Test 9 — Timeout retry.

    Simulate timeout -> 200.
    Asserts:
    - Retries once on timeout (total 2 attempts)
    - Status becomes COMPLETED
    - Webhook returns HTTP 200 OK
    """
    sim = SimulatedDestinationAdapter()
    sim.set_responses(
        [
            DestinationResponse(
                status_code=0, is_timeout=True, error_message="Socket timeout"
            ),
            DestinationResponse(status_code=200, data={"ok": True}),
        ]
    )
    set_destination_adapter(sim)

    payload = {
        "event_id": "evt_test_timeout_success",
        "event_type": "order.sync",
        "timestamp": "2026-09-26T12:00:00Z",
        "data": {"id": 3},
    }

    response = client.post("/webhook", json=payload)
    assert response.status_code == 200
    assert sim.call_count == 2

    conn = get_connection()
    try:
        rec = get_idempotency_record(conn, "evt_test_timeout_success")
        assert rec is not None
        assert rec.status == "COMPLETED"
    finally:
        conn.close()


def test_timeout_exhaustion(client: TestClient) -> None:
    """
    Test 10 — Timeout exhaustion.

    Simulate timeout -> timeout -> timeout.
    Asserts:
    - Exactly 3 attempts made
    - Status becomes FAILED
    - Webhook returns HTTP 502 Bad Gateway
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
        "event_id": "evt_test_timeout_exhausted",
        "event_type": "order.sync",
        "timestamp": "2026-09-26T12:00:00Z",
        "data": {"id": 4},
    }

    response = client.post("/webhook", json=payload)
    assert response.status_code == 502
    assert sim.call_count == 3

    conn = get_connection()
    try:
        rec = get_idempotency_record(conn, "evt_test_timeout_exhausted")
        assert rec is not None
        assert rec.status == "FAILED"
    finally:
        conn.close()


def test_backoff_calculation() -> None:
    """
    Test 11 — Backoff calculation.

    Verify exponential delay formula:
    attempt 1 -> 1.0s
    attempt 2 -> 2.0s
    attempt 3 -> 4.0s
    capped at max_delay (10.0s)
    """
    config = RetryConfig(max_attempts=5, base_delay=1.0, max_delay=10.0, backoff_factor=2.0)

    assert calculate_backoff(1, config) == 1.0
    assert calculate_backoff(2, config) == 2.0
    assert calculate_backoff(3, config) == 4.0
    assert calculate_backoff(4, config) == 8.0
    assert calculate_backoff(5, config) == 10.0  # Capped at 10.0 (min(16.0, 10.0))

    # Zero base delay in test mode
    test_config = RetryConfig(base_delay=0.0)
    assert calculate_backoff(1, test_config) == 0.0
    assert calculate_backoff(2, test_config) == 0.0


def test_no_retry_after_success(client: TestClient) -> None:
    """
    Test 12 — No retry after success.

    Sequence: 503 -> 200 -> 503.
    Asserts:
    - Retries once after 503, gets 200, stops immediately.
    - Exactly 2 attempts consumed.
    - Third configured response is NEVER consumed.
    """
    sim = SimulatedDestinationAdapter()
    sim.set_responses(
        [
            DestinationResponse(status_code=503, error_message="Service Unavailable"),
            DestinationResponse(status_code=200, data={"ok": True}),
            DestinationResponse(status_code=503, error_message="Should not be consumed"),
        ]
    )
    set_destination_adapter(sim)

    payload = {
        "event_id": "evt_test_no_retry_after_success",
        "event_type": "order.sync",
        "timestamp": "2026-09-26T12:00:00Z",
        "data": {"id": 5},
    }

    response = client.post("/webhook", json=payload)
    assert response.status_code == 200
    assert sim.call_count == 2
    assert len(sim.responses) == 1  # 3rd response remained unconsumed


def test_duplicate_completed_event_no_extra_destination_call(client: TestClient) -> None:
    """
    Test 13 — Duplicate completed event.

    Send the same successful event twice.
    Asserts:
    - First request delivers to destination (call_count = 1)
    - Second request returns cached replay (call_count remains 1)
    - X-Idempotent-Replay: true on second request
    """
    sim = SimulatedDestinationAdapter()
    sim.set_responses([DestinationResponse(status_code=200, data={"ok": True})])
    set_destination_adapter(sim)

    payload = {
        "event_id": "evt_test_dup_completed",
        "event_type": "order.sync",
        "timestamp": "2026-09-26T12:00:00Z",
        "data": {"id": 6},
    }

    resp1 = client.post("/webhook", json=payload)
    assert resp1.status_code == 200
    assert sim.call_count == 1

    resp2 = client.post("/webhook", json=payload)
    assert resp2.status_code == 200
    assert resp2.headers.get("X-Idempotent-Replay") == "true"
    assert sim.call_count == 1  # No additional destination call!


def test_duplicate_failed_event_no_extra_destination_call(client: TestClient) -> None:
    """
    Test 14 — Duplicate failed event.

    Send an event that reaches terminal failure.
    Send it again.
    Asserts:
    - First request exhausts retries (call_count = 3, returns 502)
    - Second request returns cached 502 replay without new destination calls (call_count remains 3)
    - X-Idempotent-Replay: true on second request
    - No retry storm!
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
        "event_id": "evt_test_dup_failed",
        "event_type": "order.sync",
        "timestamp": "2026-09-26T12:00:00Z",
        "data": {"id": 7},
    }

    resp1 = client.post("/webhook", json=payload)
    assert resp1.status_code == 502
    assert sim.call_count == 3

    resp2 = client.post("/webhook", json=payload)
    assert resp2.status_code == 502
    assert resp2.headers.get("X-Idempotent-Replay") == "true"
    assert sim.call_count == 3  # No additional destination call!
