"""
Unit tests for canonical payload normalization and SHA-256 hashing.

Verifies deterministic serialization, key order independence, field change detection,
nested dictionary handling, and SHA-256 formatting.
"""

import re
from datetime import datetime, timezone
from app.canonical import canonicalize_payload, hash_canonical_json, hash_payload
from app.schemas import WebhookPayload


def test_deterministic_canonical_output() -> None:
    """
    Test 1 — Deterministic canonical output.

    Verify that repeated canonicalization of the exact same payload instance
    produces identical canonical JSON strings and hashes.
    """
    payload = WebhookPayload(
        event_id="evt_001",
        event_type="order.created",
        timestamp=datetime(2026, 9, 26, 12, 0, 0, tzinfo=timezone.utc),
        data={"order_id": "ord_100", "amount": 99.50, "currency": "USD"},
    )

    first_canonical = canonicalize_payload(payload)
    second_canonical = canonicalize_payload(payload)
    first_hash = hash_payload(payload)
    second_hash = hash_payload(payload)

    assert first_canonical == second_canonical
    assert first_hash == second_hash


def test_input_key_ordering_does_not_affect_canonical_output() -> None:
    """
    Test 2 — Input JSON key ordering does not affect canonical output.

    Construct two equivalent payload dictionaries with different insertion/key ordering.
    After Pydantic validation, canonicalization must produce the same canonical JSON
    and SHA-256 hash.
    """
    # Order A: event_id first
    raw_a = {
        "event_id": "evt_001",
        "event_type": "order.created",
        "timestamp": "2026-09-26T12:00:00Z",
        "data": {
            "order_id": "ord_001",
            "amount": 99.50,
            "currency": "USD",
        },
    }

    # Order B: data first, then timestamp, event_type, event_id
    raw_b = {
        "data": {
            "currency": "USD",
            "amount": 99.50,
            "order_id": "ord_001",
        },
        "timestamp": "2026-09-26T12:00:00Z",
        "event_type": "order.created",
        "event_id": "evt_001",
    }

    payload_a = WebhookPayload.model_validate(raw_a)
    payload_b = WebhookPayload.model_validate(raw_b)

    canonical_a = canonicalize_payload(payload_a)
    canonical_b = canonicalize_payload(payload_b)

    hash_a = hash_payload(payload_a)
    hash_b = hash_payload(payload_b)

    assert canonical_a == canonical_b
    assert hash_a == hash_b


def test_payload_data_change_changes_hash() -> None:
    """
    Test 3 — Payload data change changes hash.

    Create two otherwise identical valid payloads.
    Change one meaningful value (amount: 99.50 -> 100.50).
    Hashes must differ.
    """
    raw_a = {
        "event_id": "evt_001",
        "event_type": "order.created",
        "timestamp": "2026-09-26T12:00:00Z",
        "data": {"amount": 99.50},
    }
    raw_b = {
        "event_id": "evt_001",
        "event_type": "order.created",
        "timestamp": "2026-09-26T12:00:00Z",
        "data": {"amount": 100.50},
    }

    payload_a = WebhookPayload.model_validate(raw_a)
    payload_b = WebhookPayload.model_validate(raw_b)

    assert canonicalize_payload(payload_a) != canonicalize_payload(payload_b)
    assert hash_payload(payload_a) != hash_payload(payload_b)


def test_event_id_change_changes_hash() -> None:
    """
    Test 4 — Event ID change changes hash.

    Two otherwise identical payloads with different event_id values must produce
    different canonical strings and hashes.
    """
    raw_a = {
        "event_id": "evt_001",
        "event_type": "order.created",
        "timestamp": "2026-09-26T12:00:00Z",
        "data": {"item": "widget"},
    }
    raw_b = {
        "event_id": "evt_002",
        "event_type": "order.created",
        "timestamp": "2026-09-26T12:00:00Z",
        "data": {"item": "widget"},
    }

    payload_a = WebhookPayload.model_validate(raw_a)
    payload_b = WebhookPayload.model_validate(raw_b)

    assert canonicalize_payload(payload_a) != canonicalize_payload(payload_b)
    assert hash_payload(payload_a) != hash_payload(payload_b)


def test_event_type_change_changes_hash() -> None:
    """
    Test 5 — Event type change changes hash.

    Changing event_type must change the resulting canonical JSON and hash.
    """
    raw_a = {
        "event_id": "evt_001",
        "event_type": "order.created",
        "timestamp": "2026-09-26T12:00:00Z",
        "data": {"order_id": "123"},
    }
    raw_b = {
        "event_id": "evt_001",
        "event_type": "order.cancelled",
        "timestamp": "2026-09-26T12:00:00Z",
        "data": {"order_id": "123"},
    }

    payload_a = WebhookPayload.model_validate(raw_a)
    payload_b = WebhookPayload.model_validate(raw_b)

    assert canonicalize_payload(payload_a) != canonicalize_payload(payload_b)
    assert hash_payload(payload_a) != hash_payload(payload_b)


def test_timestamp_participates_in_canonicalization() -> None:
    """
    Test 6 — Timestamp participates in canonicalization.

    Changing the timestamp must change the canonical JSON and hash.
    """
    raw_a = {
        "event_id": "evt_001",
        "event_type": "order.created",
        "timestamp": "2026-09-26T12:00:00Z",
        "data": {"status": "ok"},
    }
    raw_b = {
        "event_id": "evt_001",
        "event_type": "order.created",
        "timestamp": "2026-09-26T12:00:01Z",
        "data": {"status": "ok"},
    }

    payload_a = WebhookPayload.model_validate(raw_a)
    payload_b = WebhookPayload.model_validate(raw_b)

    assert canonicalize_payload(payload_a) != canonicalize_payload(payload_b)
    assert hash_payload(payload_a) != hash_payload(payload_b)


def test_nested_data_ordering_is_deterministic() -> None:
    """
    Test 7 — Nested data ordering is deterministic.

    Construct deeply nested dictionaries inside `data` with deliberately varied
    key insertion order. Assert that canonicalization sorts all levels.
    """
    nested_a = {
        "level1_b": {
            "level2_z": 100,
            "level2_a": 200,
            "level2_m": {"sub_2": "val2", "sub_1": "val1"},
        },
        "level1_a": "first",
    }

    nested_b = {
        "level1_a": "first",
        "level1_b": {
            "level2_a": 200,
            "level2_m": {"sub_1": "val1", "sub_2": "val2"},
            "level2_z": 100,
        },
    }

    payload_a = WebhookPayload(
        event_id="evt_nested",
        event_type="nested.test",
        timestamp=datetime(2026, 9, 26, 12, 0, 0, tzinfo=timezone.utc),
        data=nested_a,
    )
    payload_b = WebhookPayload(
        event_id="evt_nested",
        event_type="nested.test",
        timestamp=datetime(2026, 9, 26, 12, 0, 0, tzinfo=timezone.utc),
        data=nested_b,
    )

    canonical_a = canonicalize_payload(payload_a)
    canonical_b = canonicalize_payload(payload_b)

    assert canonical_a == canonical_b
    assert hash_payload(payload_a) == hash_payload(payload_b)


def test_sha256_format() -> None:
    """
    Test 8 — SHA-256 format.

    Verify the resulting hash:
    - is a string
    - contains only hexadecimal characters ([0-9a-f])
    - is exactly 64 characters long
    """
    payload = WebhookPayload(
        event_id="evt_hash_format",
        event_type="format.test",
        timestamp=datetime(2026, 9, 26, 12, 0, 0, tzinfo=timezone.utc),
        data={"key": "value"},
    )

    digest = hash_payload(payload)

    assert isinstance(digest, str)
    assert len(digest) == 64
    assert re.fullmatch(r"^[0-9a-f]{64}$", digest) is not None


def test_canonical_json_compact_and_lexicographical() -> None:
    """
    Verify canonical JSON has no extraneous whitespace and top-level keys
    are in exact lexicographical order: data, event_id, event_type, timestamp.
    """
    payload = WebhookPayload(
        event_id="evt_order_check",
        event_type="order.created",
        timestamp=datetime(2026, 9, 26, 12, 0, 0, tzinfo=timezone.utc),
        data={"sku": "ABC", "qty": 5},
    )

    canonical = canonicalize_payload(payload)

    # Must contain no whitespace after delimiters
    assert ", " not in canonical
    assert ": " not in canonical

    # Top-level keys must start with "data", followed by "event_id", "event_type", "timestamp"
    expected_start = '{"data":{"qty":5,"sku":"ABC"},"event_id":"evt_order_check","event_type":"order.created","timestamp":"2026-09-26T12:00:00Z"}'
    assert canonical == expected_start


def test_unicode_preservation_and_consistency() -> None:
    """Verify that unicode characters (e.g., currency symbols, accents) are preserved."""
    payload = WebhookPayload(
        event_id="evt_unicode",
        event_type="payment.processed",
        timestamp=datetime(2026, 9, 26, 12, 0, 0, tzinfo=timezone.utc),
        data={"amount_str": "100€", "customer": "José Müller", "city": "São Paulo"},
    )

    canonical = canonicalize_payload(payload)
    assert "100€" in canonical
    assert "José Müller" in canonical
    assert "São Paulo" in canonical

    # Hash helper consistency check
    assert hash_payload(payload) == hash_canonical_json(canonical)
