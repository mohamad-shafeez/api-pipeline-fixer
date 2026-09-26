"""
Automated pytest suite for FastAPI webhook ingestion boundary.

Validates the ingestion endpoint behavior against:
- Valid payload ingestion (HTTP success, structured acceptance response)
- Missing required event identifier (HTTP 422)
- Invalid field types (HTTP 422)
- Malformed JSON payloads (HTTP 422, graceful rejection)
- Empty / structurally invalid request bodies (HTTP 422)
- Root health / service info endpoint (HTTP 200)
"""

import pytest
from fastapi.testclient import TestClient
from app.main import app


@pytest.fixture
def client() -> TestClient:
    """Fixture providing a FastAPI TestClient instance."""
    return TestClient(app)


def test_root_endpoint(client: TestClient) -> None:
    """Verify that the root endpoint returns service information and HTTP 200."""
    response = client.get("/")
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("application/json")
    data = response.json()
    assert data["service"] == "Resilient Integration Pipeline"
    assert data["status"] == "operational"


def test_valid_webhook_ingestion(client: TestClient) -> None:
    """
    Test 1 — Valid webhook.

    Asserts:
    - HTTP success status (HTTP 200 OK)
    - Response is JSON
    - Response indicates ingestion/acceptance
    - Expected event identifier is represented appropriately
    """
    valid_payload = {
        "event_id": "evt_test_001",
        "event_type": "order.created",
        "timestamp": "2026-09-26T12:00:00Z",
        "data": {
            "order_id": "ord_987",
            "customer_id": "cust_456",
            "amount": 99.95,
            "currency": "USD",
        },
    }

    response = client.post("/webhook", json=valid_payload)

    # 1. HTTP success status
    assert response.status_code == 200
    assert response.is_success

    # 2. Response is JSON
    assert response.headers["content-type"].startswith("application/json")
    data = response.json()

    # 3. Response indicates ingestion/acceptance
    assert data["status"] == "accepted"
    assert "accepted" in data["message"].lower()

    # 4. Expected event identifier is represented appropriately
    assert data["event_id"] == "evt_test_001"


def test_missing_event_identifier(client: TestClient) -> None:
    """
    Test 2 — Missing event identifier.

    Asserts:
    - HTTP 422 Unprocessable Entity
    - Validation error is present with details targeting the missing event_id field
    """
    payload_without_id = {
        "event_type": "order.created",
        "timestamp": "2026-09-26T12:00:00Z",
        "data": {"order_id": "ord_987"},
    }

    response = client.post("/webhook", json=payload_without_id)

    assert response.status_code == 422
    data = response.json()
    assert "detail" in data
    errors = data["detail"]
    assert any("event_id" in err["loc"] for err in errors)
    assert any(err["type"] == "missing" for err in errors)


def test_invalid_field_type(client: TestClient) -> None:
    """
    Test 3 — Invalid field type.

    Asserts:
    - HTTP 422 Unprocessable Entity
    - Validation error reports the type mismatch
    """
    # Test with invalid timestamp (non-ISO string)
    payload_invalid_timestamp = {
        "event_id": "evt_test_002",
        "event_type": "order.created",
        "timestamp": "not-a-valid-iso-timestamp",
        "data": {"amount": 100},
    }

    response = client.post("/webhook", json=payload_invalid_timestamp)
    assert response.status_code == 422
    data = response.json()
    errors = data["detail"]
    assert any("timestamp" in err["loc"] for err in errors)

    # Test with invalid data type (string instead of dict)
    payload_invalid_data_type = {
        "event_id": "evt_test_003",
        "event_type": "order.created",
        "timestamp": "2026-09-26T12:00:00Z",
        "data": "should_be_a_dict_not_a_string",
    }

    response = client.post("/webhook", json=payload_invalid_data_type)
    assert response.status_code == 422
    data = response.json()
    errors = data["detail"]
    assert any("data" in err["loc"] for err in errors)


def test_malformed_json(client: TestClient) -> None:
    """
    Test 4 — Malformed JSON.

    Asserts:
    - HTTP 422 (or appropriate parsing/validation error status)
    - Server does not crash
    """
    malformed_raw_body = '{"event_id": "evt_004", "event_type": broken_json'

    response = client.post(
        "/webhook",
        content=malformed_raw_body,
        headers={"content-type": "application/json"},
    )

    assert response.status_code == 422
    data = response.json()
    assert "detail" in data
    errors = data["detail"]
    assert any(err.get("type") == "json_invalid" for err in errors)


def test_empty_or_invalid_request_body(client: TestClient) -> None:
    """
    Test 5 — Empty/invalid request body.

    Asserts:
    - Empty JSON object {} is rejected with HTTP 422 (missing required fields)
    - Empty raw content is rejected with HTTP 422 (missing body)
    - Non-object JSON (array, scalar) is rejected with HTTP 422
    """
    # 1. Empty JSON object
    response_empty_obj = client.post("/webhook", json={})
    assert response_empty_obj.status_code == 422
    data_obj = response_empty_obj.json()
    assert "detail" in data_obj

    # 2. Empty raw content
    response_empty_body = client.post(
        "/webhook",
        content="",
        headers={"content-type": "application/json"},
    )
    assert response_empty_body.status_code == 422
    data_body = response_empty_body.json()
    assert "detail" in data_body

    # 3. Non-object JSON (array)
    response_array = client.post(
        "/webhook",
        content="[]",
        headers={"content-type": "application/json"},
    )
    assert response_array.status_code == 422


def test_empty_string_event_id_rejected(client: TestClient) -> None:
    """Verify that an empty string event_id is rejected by min_length constraint."""
    payload_empty_id = {
        "event_id": "",
        "event_type": "order.created",
        "timestamp": "2026-09-26T12:00:00Z",
        "data": {},
    }

    response = client.post("/webhook", json=payload_empty_id)
    assert response.status_code == 422
    errors = response.json()["detail"]
    assert any("event_id" in err["loc"] for err in errors)
