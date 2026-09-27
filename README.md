# Resilient Integration Pipeline

A synchronous FastAPI webhook ingestion pipeline demonstrating defensive backend integration patterns including schema validation, canonical payload normalization, atomic idempotency, retry/exponential backoff, failure classification, dead-letter persistence, and structured JSON logging.

> **Note**: This is an independent personal proof-of-work project. It uses a deterministic simulated destination and local SQLite persistence to demonstrate transferable backend patterns. It is not an integration built for a real commercial client or external production vendor.

---

## What Problem This Demonstrates

In real-world webhook and API integrations, downstream systems and networks fail frequently. Unhandled edge cases lead to:
- **Duplicate deliveries**: Clients retry requests, causing duplicate charges, double-inserted records, or race conditions.
- **Malformed payloads**: Unexpected field types or missing identifiers cause uncaught exceptions without meaningful error responses.
- **Transient downstream failures**: Temporary 503s, 502s, 429 rate limits, and network timeouts crash pipelines if retries are not attempted.
- **Permanent downstream failures**: 400 Bad Request or 422 Unprocessable Entity waste system resources if retried blindly.
- **Silent data loss**: Terminal failures vanish without forensic evidence of what arrived or why it failed.
- **Opaque debugging**: Unstructured log output makes it difficult to trace the lifecycle of a specific event across retries.

This repository provides an auditable, resilient architecture that handles each of these failure modes deterministically.

---

## Architecture

The pipeline processes each incoming webhook request **synchronously/inline** within the HTTP request lifecycle. There are no external message brokers, background workers, or task queues.

```
Client
  |
  v
POST /webhook
  |
  v
[1] Pydantic Validation  ------------------------> HTTP 422 (No DB / No DLQ)
  |
  v (valid payload)
[2] Canonicalization + SHA-256 Hashing
  |
  v
[3] SQLite Atomic Idempotency Claim
  |
  +---> Key exists (COMPLETED) & same hash -----> HTTP 200 (Cached replay, X-Idempotent-Replay: true)
  +---> Key exists (FAILED) & same hash --------> HTTP 502 (Cached failure, X-Idempotent-Replay: true)
  +---> Key exists & different hash ------------> HTTP 409 Conflict (Payload mismatch)
  +---> Key exists & status is PROCESSING ------> HTTP 409 Conflict (Concurrent in-flight)
  |
  v (status: PROCESSING)
[4] DestinationAdapter (Delivery & Retries)
  |
  +---> Delivery Success (200/201) -------------> DB: COMPLETED -> HTTP 200 Accepted
  |
  +---> Retryable Error (503, 429, timeout) ----> Exponential Backoff -> Retry (up to 3 attempts)
  |
  v (Retries Exhausted OR Permanent 4xx Error)
[5] Atomic Terminal Failure Transaction
  |
  +---> Update idempotency_records to FAILED
  +---> Insert forensic record into dead_letter_records (DLQ)
  |
  v
HTTP 502 Bad Gateway
```

---

## Request Lifecycle

1. **Ingestion**: Raw HTTP request body is captured as byte-exact evidence before parsing.
2. **Validation**: Pydantic validates data types, required fields, and ISO-8601 timestamps.
3. **Normalization**: The payload is normalized into canonical JSON and hashed with SHA-256.
4. **Idempotency**: An atomic SQLite transaction checks existing claims:
   - Returns cached responses for duplicate requests.
   - Rejects mismatched payloads or concurrent in-flight requests with HTTP 409.
   - Acquires a `PROCESSING` lock for new keys.
5. **Destination Delivery**: The payload is dispatched to the downstream destination adapter.
6. **Retry & Backoff**: Transient errors trigger exponential backoff.
7. **Terminal Persistence**: On success, the key transitions to `COMPLETED`. On terminal failure, the key transitions to `FAILED` and a DLQ record is inserted atomically.
8. **Response**: Appropriate HTTP response (200, 409, 422, 500, or 502) is returned to the client.

---

## API Reference

### Health Check

```http
GET /
```

**Response (HTTP 200 OK):**
```json
{
  "status": "healthy",
  "service": "resilient-integration-pipeline"
}
```

---

### Webhook Ingestion

```http
POST /webhook
Content-Type: application/json
```

#### Valid Request Example

```json
{
  "event_id": "evt_order_1001",
  "event_type": "order.completed",
  "timestamp": "2026-09-27T10:00:00Z",
  "data": {
    "order_id": "ord_9988",
    "customer_id": "cust_1234",
    "amount": 149.99
  }
}
```

#### Success Response (HTTP 200 OK)

```json
{
  "status": "accepted",
  "event_id": "evt_order_1001",
  "message": "Event accepted and delivered successfully"
}
```

#### Duplicate Completed Request (HTTP 200 OK)

When an identical completed request is re-submitted, the cached response is returned without contacting the destination:
- **Header**: `X-Idempotent-Replay: true`
- **Body**: Same as the original HTTP 200 response.

#### Duplicate Failed Request (HTTP 502 Bad Gateway)

When an identical previously failed request is re-submitted, the cached terminal error is returned without contacting the destination:
- **Header**: `X-Idempotent-Replay: true`
- **Status Code**: `502 Bad Gateway`
- **Body**:
```json
{
  "detail": "Destination delivery failed (SERVER_ERROR): Downstream service unavailable",
  "event_id": "evt_order_1001",
  "attempt_count": 3,
  "error_category": "SERVER_ERROR"
}
```

#### Payload Mismatch Conflict (HTTP 409 Conflict)

Reusing an `event_id` with different payload contents is rejected:
```json
{
  "detail": "Idempotency key 'evt_order_1001' has already been used with a different payload"
}
```

#### Validation Error (HTTP 422 Unprocessable Entity)

Malformed JSON or invalid schemas fail at the ingestion boundary:
```json
{
  "detail": [
    {
      "type": "missing",
      "loc": ["body", "event_id"],
      "msg": "Field required",
      "input": {}
    }
  ]
}
```

#### Terminal Destination Failure vs Pipeline HTTP Response

- **Destination HTTP Status**: The status code returned by the downstream destination (e.g. 503, 500, 429, 400).
- **Pipeline HTTP Response**: The status returned to the webhook sender. If the destination suffers retry exhaustion or a non-retryable 4xx client error, the pipeline returns **HTTP 502 Bad Gateway** to signal an upstream integration failure.

---

## Idempotency Model

Idempotency is keyed by `event_id`. To ensure correctness:
- **Canonical Hash**: A SHA-256 hash of the normalized payload is stored with the claim.
- **State Machine**:
  - `PROCESSING`: Claimed upon ingestion. Blocks concurrent attempts with HTTP 409.
  - `COMPLETED`: Delivery succeeded. Returns cached HTTP 200 on identical replay.
  - `FAILED`: Destination failed permanently. Returns cached HTTP 502 on identical replay without duplicate destination calls or duplicate DLQ entries.
- **Payload Mismatch Protection**: If the `event_id` exists but the SHA-256 hash differs, HTTP 409 is returned.
- **Rollback on Error**: If a database error occurs during downstream delivery processing, the claim is rolled back so the key is not permanently locked in `PROCESSING`.

---

## Canonicalization & Hashing

To avoid false mismatch conflicts caused by cosmetic JSON variations (such as key ordering or whitespace), the pipeline implements deterministic canonicalization:
1. Validated Pydantic model is serialized.
2. Dictionary keys are sorted lexicographically at all nesting levels.
3. Separators are minimized (`','` and `':'` with zero extra whitespace).
4. Timestamps are normalized to ISO-8601 UTC representation.
5. The resulting canonical string is hashed via standard SHA-256.

```python
canonical_bytes = json.dumps(data, sort_keys=True, separators=(",", ":")).encode("utf-8")
payload_hash = hashlib.sha256(canonical_bytes).hexdigest()
```

---

## Retry Policy & Failure Classification

Failures are categorized into retryable and non-retryable errors:

| Classification | Destination Status Codes / Conditions | Pipeline Action |
|---|---|---|
| **Retryable** | `429`, `500`, `502`, `503`, `504`, Socket/Connection Timeouts | Exponential backoff up to 3 attempts |
| **Non-Retryable** | `400`, `401`, `403`, `422` | Fail immediately on attempt 1 (no retries) |

### Retry Configuration Defaults
- **Max Attempts**: 3 (1 initial attempt + 2 retries)
- **Base Delay**: 1.0 second
- **Backoff Factor**: 2.0 (attempt 1 $\to$ 1.0s, attempt 2 $\to$ 2.0s)
- **Max Delay**: 10.0 seconds
- **Test Optimization**: Automated unit and integration test fixtures inject `base_delay = 0.0` so test runs complete instantly without artificial delays.

---

## Dead-Letter Queue (DLQ)

When an event experiences a **terminal destination failure**, a forensic record is persisted in SQLite.

### What Enters the DLQ
- Retries exhausted on retryable failures (e.g. 503 $\to$ 503 $\to$ 503 $\to$ FAILED).
- Non-retryable permanent client errors from downstream (e.g. 400 $\to$ FAILED).
- Timeout exhaustion on persistent connection drops.

### What Does NOT Enter the DLQ
- Webhook schema validation errors (HTTP 422).
- Duplicate requests for previously failed events (cached failure returned, DLQ count unchanged).
- Mismatched payload conflicts (HTTP 409).
- Successful deliveries after retry (e.g. 503 $\to$ 200 $\to$ COMPLETED).

### Stored Forensic Evidence
Each DLQ record contains:
- `id`: Auto-incrementing record ID.
- `idempotency_key`: The `event_id`.
- `raw_payload`: Exact UTF-8 request body bytes as transmitted over the wire.
- `normalized_payload`: Canonical JSON representation for structured comparison.
- `error_category`: `SERVER_ERROR`, `CLIENT_ERROR`, `RATE_LIMIT`, or `TIMEOUT`.
- `http_status`: Final downstream HTTP status code (or `null` on timeout).
- `error_message`: Forensic error message.
- `attempt_count`: Total attempts made (e.g. 1 for 400, 3 for exhausted 503).
- `attempt_history`: Complete JSON array containing timestamps, status codes, timeout flags, and error messages for each attempt.
- `created_at`: UTC ISO-8601 creation timestamp.

*Note: DLQ inspection is strictly read-only. There is no automated replay, deletion, or administrative API.*

---

## Read-Only DLQ Inspection CLI

A minimal CLI is provided to inspect dead-letter records:

```powershell
# List all DLQ records
.venv\Scripts\python.exe -m app.dlq list

# Show full details for a specific DLQ record
.venv\Scripts\python.exe -m app.dlq show 1
```

**Example CLI Output:**
```text
=== Dead-Letter Records (1 found in pipeline.db) ===
ID    IDEMPOTENCY KEY          CATEGORY        STATUS   ATTEMPTS   CREATED AT
-----------------------------------------------------------------------------------------------
1     evt_order_1001           SERVER_ERROR    503      3          2026-09-27T01:02:55+00:00

=== DLQ Record ID: 1 ===
Idempotency Key:    evt_order_1001
Error Category:     SERVER_ERROR
HTTP Status:        503
Error Message:      Destination delivery failed
Total Attempts:     3
Created At:         2026-09-27T01:02:55+00:00

Raw Payload:
{"event_id": "evt_order_1001", "event_type": "order.completed", ...}

Normalized Payload:
{"data":{"amount":149.99,"customer_id":"cust_1234","order_id":"ord_9988"},"event_id":"evt_order_1001",...}

Attempt History:
  - Attempt 1: status=503, timeout=False, error=None
  - Attempt 2: status=503, timeout=False, error=None
  - Attempt 3: status=503, timeout=False, error=None
```

---

## Structured Logging

The pipeline uses Python's standard library `logging` module configured with a JSON formatter. Every log entry is written as single-line JSON.

### Key Lifecycle Events
- `event_received` (`INFO`): Event ingested with `event_id` and `event_type`.
- `idempotency_claimed` (`INFO`): Atomic lock set to `PROCESSING`.
- `duplicate_detected` (`INFO`): Replay served from cache (`COMPLETED`).
- `duplicate_failed_detected` (`INFO`): Replay served from cache (`FAILED`).
- `delivery_attempt` (`INFO`): Downstream adapter call attempt number.
- `delivery_success` (`INFO`): Successful downstream delivery.
- `retry_scheduled` (`WARNING`): Retryable status logged with backoff duration.
- `payload_mismatch` (`WARNING`): Conflicting payload for existing key.
- `processing_conflict` (`WARNING`): Concurrent request while `PROCESSING`.
- `terminal_failure` (`ERROR`): Exhausted retries or non-retryable error.
- `dlq_record_created` (`INFO`): DLQ record persisted with `dlq_id`.
- `database_failure` (`ERROR`): SQLite error triggering transaction rollback.

**Security Rule**: Passwords, authorization tokens, secrets, and raw request bodies are never included in log records.

---

## Database Design

SQLite is used for local persistence with WAL mode and busy timeouts:

- `idempotency_records`: Tracks state machine transitions (`PROCESSING`, `COMPLETED`, `FAILED`), canonical hashes, and cached response payloads.
- `dead_letter_records`: Stores terminal failure evidence and attempt histories.
- `dead_letter_queue`: SQL view mirroring `dead_letter_records` for naming compatibility.

**Atomic Terminal Transition**: When a delivery fails terminally, updating `idempotency_records` to `FAILED` and inserting into `dead_letter_records` occur within a single SQLite transaction.

---

## Local Setup & Development

### 1. Prerequisites
- Python 3.11+
- Git

### 2. Clone and Setup Environment

```bash
git clone <repository-url>
cd api-pipeline-fixer

# Create virtual environment
python -m venv .venv

# Activate environment (Windows PowerShell)
.venv\Scripts\Activate.ps1

# Activate environment (Linux / macOS)
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### 3. Run the Automated Test Suite

```bash
.venv\Scripts\python.exe -m pytest -q
```
*(All 59 automated tests pass with code 0).*

### 4. Run the API Locally

```bash
.venv\Scripts\python.exe -m uvicorn app.main:app --reload
```

The API will be live at `http://127.0.0.1:8000`.

---

## Security Considerations

This proof-of-work project highlights several security and architectural factors:
- **Webhook Authentication**: In production, webhooks should be verified using HMAC-SHA256 signatures or shared secrets (e.g. `X-Signature` headers).
- **Payload Size Limits**: Strict byte-length limits should be enforced at the gateway/proxy boundary to prevent memory exhaustion attacks.
- **Log Sanitation**: Sensitive personal information (PII) and credentials must be scrubbed before logging.
- **Transport Security**: Deployments must terminate TLS (HTTPS) to prevent interception.
- **Error Obfuscation**: Internal database error details should never be exposed in client HTTP responses.

---

## Limitations & Non-Goals

- **Synchronous Execution**: Webhook requests are processed synchronously. For heavy downstream workloads, an asynchronous queue (e.g. Celery, SQS) would be preferred in production.
- **SQLite Persistence**: SQLite is used for self-contained execution without external services; it is not suited for multi-instance distributed scaling.
- **Simulated Destination**: Downstream services are simulated locally for deterministic testing.
- **No Background Workers**: There are no out-of-band polling workers or queue daemons.
- **No Automated Replay**: DLQ records can be inspected via CLI but are not automatically re-driven.
- **No Dashboard / UI**: Admin operations are CLI-based.

---

## Project Structure

```
api-pipeline-fixer/
├── app/
│   ├── __init__.py           # Application package marker
│   ├── canonical.py          # Deterministic JSON canonicalization & SHA-256
│   ├── database.py           # SQLite connection, schemas, and transactions
│   ├── destination.py        # Destination adapter, simulator, and retry policy
│   ├── dlq.py                # Read-only DLQ queries and CLI interface
│   ├── logging_conf.py       # Structured JSON logging formatter and setup
│   ├── main.py               # FastAPI application & webhook ingestion endpoint
│   └── schemas.py            # Pydantic v2 request & response models
├── docs/
│   └── README.md             # Additional documentation placeholder
├── tests/
│   ├── __init__.py           # Test package marker
│   ├── conftest.py           # Shared test fixtures & database isolation
│   ├── test_canonical.py     # Canonicalization & hashing unit tests
│   ├── test_dlq.py           # Dead-letter queue & failure persistence tests
│   ├── test_end_to_end.py    # Full pipeline integration tests (Scenarios A-H)
│   ├── test_idempotency.py   # State machine & idempotency lifecycle tests
│   ├── test_retry.py         # Retry classification & backoff calculation tests
│   └── test_webhook_ingestion.py # Ingestion & Pydantic validation tests
├── .gitignore                # Git exclusions (caches, .db, .log, .venv)
├── PROJECT_SPEC.md           # Authoritative architectural specification
├── pytest.ini                # Pytest configuration
├── requirements.txt          # Fully pinned dependencies
└── README.md                 # Project documentation
```

---

## Proof-of-Work Summary

This project demonstrates core backend integration capabilities:
- **FastAPI / Python 3.11**: Asynchronous endpoint handling, dependency injection, and clean exception handling.
- **Pydantic v2**: Strict schema validation and data parsing.
- **Relational Transactions**: Atomic multi-table updates using standard SQLite transactions without ORM overhead.
- **Defensive Idempotency**: State machine management preventing double-execution and payload tampering.
- **Resilient Retry Logic**: Exponential backoff and deterministic status code classification.
- **Auditable Observability**: Structured JSON logging and dead-letter evidence preservation.
- **Rigorous Testing**: 59 automated unit and end-to-end integration tests.
