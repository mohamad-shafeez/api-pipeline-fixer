"""
Canonical payload normalization and SHA-256 hashing.

Provides deterministic serialization and cryptographic hashing for validated
WebhookPayload models, ensuring stable representations for duplicate detection
and idempotency tracking.
"""

import hashlib
import json
from app.schemas import WebhookPayload


def canonicalize_payload(payload: WebhookPayload) -> str:
    """
    Produce a deterministic, canonical JSON string representation of a WebhookPayload.

    Canonicalization rules:
    - Serializes validated Pydantic model using `model_dump(mode='json')` so that
      special types (e.g., datetime timestamps) are converted to standard ISO 8601 strings.
    - Sorts all dictionary keys lexicographically at every level (`sort_keys=True`).
    - Eliminates superfluous whitespace using compact separators (`,`, `:`).
    - Preserves UTF-8 characters without ASCII-escaping (`ensure_ascii=False`).

    Args:
        payload: Validated WebhookPayload instance.

    Returns:
        Deterministic canonical JSON string.
    """
    dumped = payload.model_dump(mode="json")
    return json.dumps(dumped, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def hash_canonical_json(canonical_json: str) -> str:
    """
    Compute the SHA-256 cryptographic digest directly from a canonical JSON string.

    The hash is computed over the exact UTF-8 encoded bytes of the canonical JSON string.

    Args:
        canonical_json: Deterministic canonical JSON string.

    Returns:
        64-character lowercase hexadecimal SHA-256 digest.
    """
    return hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()


def hash_payload(payload: WebhookPayload) -> str:
    """
    Compute the SHA-256 cryptographic digest of a canonicalized WebhookPayload.

    Args:
        payload: Validated WebhookPayload instance.

    Returns:
        64-character lowercase hexadecimal SHA-256 digest.
    """
    canonical_json = canonicalize_payload(payload)
    return hash_canonical_json(canonical_json)
