# Resilient Integration Pipeline (`api-pipeline-fixer`)

## 1. Project Disclaimer
This project is an independent, personal proof-of-work engineering demonstration. It is not built for, commissioned by, or affiliated with any commercial client or organization. It does not use real client data, private credentials, or live external commercial endpoints, nor does it claim to reproduce a private proprietary system. 

It explicitly avoids unsupported claims of "enterprise-grade reliability," "zero data loss," or "production-ready SaaS." It represents a focused, rigorous software demonstration of resilient backend integration patterns.

---

## 2. Project Name & Repository Slug
- **Project Name:** Resilient Integration Pipeline
- **Repository / Slug:** `api-pipeline-fixer`

---

## 3. Project Purpose
The purpose of this project is to serve as a focused, high-signal proof-of-work demonstration of backend engineering competency in Python. It directly demonstrates solutions to recurring failure modes in webhook ingestion and REST API integrations:
- Brittle webhook handlers that crash on schema variations
- Duplicate payload processing caused by upstream delivery retries
- Unhandled downstream rate limits (HTTP 429) and server errors (HTTP 500)
- Network timeouts without backoff or retry bounds
- Silent data loss when destination systems fail
- Opaque debugging caused by missing structured logging

The project demonstrates how to structure a clean, maintainable integration pipeline with strict input validation, canonical payload normalization, idempotency protection, classified retries with exponential backoff, dead-letter storage, and structured observability.

---

## 4. Target Buyer Category
The intended audience consists of prospective clients, engineering hiring managers, and technical leads seeking competent, reliable engineering for:
- Python backend engineering
- Webhook ingestion pipelines
- REST API integrations and adapters
- Data synchronization and field mapping
- Payload normalization between disparate formats
- Retry and recovery logic for external service communication
- Idempotency and duplicate prevention in event-driven systems
- Debugging, hardening, and repairing brittle integration pipelines

---

## 5. Problem Being Demonstrated
Real-world API integrations frequently break in production due to predictable edge cases:
1. **Malformed or Variant Payloads:** Upstream services send payloads missing mandatory fields or using unexpected types.
2. **Duplicate Webhook Deliveries:** Upstream dispatchers (such as payment gateways, eCommerce platforms, or event brokers) guarantee at-least-once delivery, sending the same event multiple times.
3. **Downstream Transient Failures:** Destination APIs periodically experience transient HTTP 500 (Internal Server Error), HTTP 429 (Rate Limit Exceeded), or network socket timeouts.
4. **Permanent Client Errors:** Downstream APIs reject invalid requests with HTTP 400, 401, 403, or 422, where retrying is futile and wastes resources.
5. **Silent Data Loss:** When an integration fails, raw payloads are often discarded or buried in plain-text logs without structured failure persistence.
6. **Opaque Observability:** Unstructured log output makes tracing the lifecycle of a failed request difficult and slow.

---

## 6. Execution Mode & Request Lifecycle
To remain self-contained, easily testable, and free of heavy infrastructure dependencies, this demonstration processes each incoming webhook request **synchronously / inline**.

### Synchronous Lifecycle
Each HTTP request executes the following end-to-end sequence before returning an HTTP response:

```text
Incoming HTTP Request
  │
  ▼
[ 1. Validation ] ─────────(Schema Invalid)───► Return HTTP 422 (No DLQ, No Retries)
  │ (Valid)
  ▼
[ 2. Normalization ]
  │
  ▼
[ 3. Idempotency Check ] ──(Duplicate Found)──► Return Stored / Conflict Response
  │ (New Key: Status = PROCESSING)
  ▼
[ 4. Destination Delivery via Adapter ]
  │
  ├─► (Success) ──────────────────────────────► Persist Success, Status = COMPLETED, Return HTTP 200/201
  │
  ├─► (Non-Retryable Failure: 400, 401, etc.) ─► Persist DLQ, Status = FAILED, Return HTTP 502/400
  │
  └─► (Retryable Failure: 429, 500, Timeout) ──► Retry Loop with Backoff
                                                    │
                                      ┌─────────────┴─────────────┐
                                      │                           │
                               (Retry Succeeds)          (Retries Exhausted)
                                      │                           │
                                      ▼                           ▼
                           Persist Success,             Persist to DLQ,
                           Status = COMPLETED,          Status = FAILED,
                           Return HTTP 200/201          Return HTTP 502 Bad Gateway
```

### Scope Boundary
- The inline processing model is an intentional design choice for a portable, local proof-of-work.
- The project does **not** include background task queues, Redis, Celery, RabbitMQ, Kafka, or worker processes.
- While high-volume production systems often decouple webhook ingestion via asynchronous queues, synchronous processing allows direct verification of retries, idempotency states, and error handling in a single reproducible runtime.

---

## 7. Proposed High-Level Architecture
The system cleanly separates ingestion, business logic, persistence, and external destination communication:

```text
┌────────────────────────────────────────────────────────────────────────┐
│                        FastAPI Application                             │
│                                                                        │
│   POST /webhook                                                        │
│         │                                                              │
│         ▼                                                              │
│   Validation Layer (Pydantic Models)                                   │
│         │                                                              │
│         ▼                                                              │
│   Normalization Layer (Domain Schemas)                                 │
│         │                                                              │
│         ▼                                                              │
│   Pipeline Coordinator                                                 │
│     ├── Idempotency Service ───► [ SQLite Persistence Layer ]          │
│     │                              - idempotency_records               │
│     │                              - processed_records                 │
│     │                              - dead_letter_records               │
│     │                                                                  │
│     ├── Retry Engine (Configurable Delays)                             │
│     │     │                                                            │
│     │     ▼                                                            │
│     └── Destination Boundary ──► [ DestinationAdapter Interface ]       │
│                                        │                               │
│                                        ▼                               │
│                                 Simulated Destination                  │
│                                 (Deterministic Mock / Local Adapter)   │
└────────────────────────────────────────────────────────────────────────┘
```

### Architectural Distinctions
- **Persistence Layer (SQLite):** Acts solely as the local storage mechanism for idempotency locks, successfully processed records, and the dead-letter store. SQLite does **not** simulate network or HTTP failures.
- **Destination Layer (`DestinationAdapter`):** An explicit interface representing communication with an external third-party destination API.
- **Deterministic Mock Adapter:** An implementation of `DestinationAdapter` that simulates realistic downstream responses (200, 429, 500, timeouts, 400) according to test configurations or payload directives.

---

## 8. DestinationAdapter Boundary & Simulation

### Boundary Definition
The application core depends strictly on an abstract `DestinationAdapter` interface:

```python
class DestinationResponse:
    status_code: int
    data: dict | None
    error_message: str | None
    is_timeout: bool = False

class DestinationAdapter(ABC):
    @abstractmethod
    def deliver(self, record: NormalizedRecord) -> DestinationResponse:
        pass
```

### Simulated Destination Outcomes
The deterministic simulation adapter supports the following response categories:
- **Success:** HTTP `200 OK` or `201 Created`
- **Rate Limited (Retryable):** HTTP `429 Too Many Requests`
- **Internal Server Error (Retryable):** HTTP `500 Internal Server Error`, `502 Bad Gateway`, `503 Service Unavailable`
- **Network / Socket Timeout (Retryable):** Simulated timeout exception / flag
- **Client Error (Non-Retryable):** HTTP `400 Bad Request`, `401 Unauthorized`, `403 Forbidden`, `422 Unprocessable Entity`

The adapter exists specifically so the pipeline's retry engine, failure classifier, and recovery handlers can be tested deterministically without depending on real external networks or paid APIs.

---

## 9. Idempotency Specification & Lifecycle

### Key Definition & Stored Attributes
Every incoming request must provide an explicit transaction/event identifier (e.g., `event_id` in the webhook payload) which acts as the **Idempotency Key**.

The persistence layer maintains an `idempotency_records` table storing:
- `idempotency_key` (TEXT, Primary Key / Unique)
- `payload_hash` (TEXT, SHA-256 hash of the canonicalized JSON request payload)
- `status` (TEXT: `PROCESSING`, `COMPLETED`, `FAILED`)
- `created_at` (TIMESTAMP)
- `updated_at` (TIMESTAMP)
- `response_status_code` (INTEGER, nullable)
- `response_body` (TEXT, nullable JSON string of cached response)

### Payload Hashing & Canonicalization
To prevent false mismatches between semantically identical payloads caused by variable whitespace or key ordering:
1. **Raw Payload Retention:** The raw incoming payload bytes are preserved unedited for evidence and dead-letter queue records.
2. **Canonical Hash Generation:** Incoming payload is parsed and validated, converted to a canonical JSON representation (keys sorted alphabetically, compact separators `","` and `":"` without whitespace), and hashed using SHA-256.
3. **Idempotency Comparison:** The resulting SHA-256 digest is stored as `payload_hash` and used for all duplicate detection and payload drift comparisons.

### Explicit Handling Cases

#### Case A — First Request
- Key does not exist in `idempotency_records`.
- Atomically insert `(idempotency_key, payload_hash, status='PROCESSING')`.
- Proceed with destination delivery.

#### Case B — Duplicate Request with Identical Payload (Previous COMPLETED)
- Key exists, stored `payload_hash` matches current request's SHA-256 hash, and `status` is `COMPLETED`.
- Do **not** execute the destination operation again.
- Return the stored `response_status_code` and `response_body` (with an HTTP header such as `X-Idempotent-Replay: true`).

#### Case C — Duplicate Key with Different Payload
- Key exists, but stored `payload_hash` differs from the current request's hash.
- Return HTTP `409 Conflict` immediately with an explanation that the idempotency key was reused for a differing payload.
- Do **not** execute the destination operation.

#### Case D — Concurrent In-Progress Request
- Key exists and current `status` is `PROCESSING`.
- Return HTTP `409 Conflict` (or `425 Too Early`) indicating a request with this key is currently being processed.
- Prevents concurrent calls from triggering duplicate destination deliveries.

#### Case E — Duplicate Request after Previous FAILED
- Key exists, stored `payload_hash` matches, and `status` is `FAILED`.
- **Deterministic Behavior:** Return the previously recorded failure response (e.g., HTTP `502 Bad Gateway` referencing the prior failed execution and DLQ entry) without re-executing the destination operation.
- **Scope & Distinction:** This cached failure behavior applies strictly to **business/destination failures** (e.g., retry exhaustion or non-retryable downstream rejections). It does **not** apply to internal database/persistence failures (see Section 13), which roll back and do not commit a terminal `FAILED` record, preventing temporary infrastructure faults from permanently poisoning an idempotency key.
- **Rationale:** Prevents endless retry storms from repeated webhook deliveries when a transaction has already exhausted all configured retries and was permanently transferred to dead-letter storage.

### Lifecycle State Machine & Concurrency Prevention
```text
           [Incoming Request]
                   │
                   ▼
        (Insert status=PROCESSING)
        ┌──────────┴──────────┐
   (Key Exists)          (Key Inserted)
        │                     │
  [Reject 409]                ▼
                      [Execute Pipeline]
                              │
                    ┌─────────┴─────────┐
                (Success)           (Failure)
                    │                   │
                    ▼                   ▼
           Update status=COMPLETED  Update status=FAILED
```

- **Concurrency Mechanism:** SQLite enforces a `UNIQUE` constraint on `idempotency_key`. The initial insert uses `BEGIN IMMEDIATE` or an atomic INSERT. If two concurrent requests arrive simultaneously with the same key, the database raises an integrity constraint error on the second attempt, ensuring only one thread enters the execution pipeline.

---

## 10. Retry Policy & Failure Classification

### Classification Rules
Failures reported by the `DestinationAdapter` are strictly classified into two categories:

#### 1. Retryable Failures
- `HTTP 429 Too Many Requests`
- `HTTP 500 Internal Server Error`, `HTTP 502 Bad Gateway`, `HTTP 503 Service Unavailable`, `HTTP 504 Gateway Timeout`
- Destination network socket timeout / connection timeout

#### 2. Non-Retryable Failures
- `HTTP 400 Bad Request`
- `HTTP 401 Unauthorized` / `HTTP 403 Forbidden`
- `HTTP 422 Unprocessable Entity` (destination rejected normalized model)
- Any other client-side permanent rejection

### Retry Configuration & Backoff Formula
- **Maximum Attempts:** Configurable (Default: 3 attempts total — 1 initial + 2 retries).
- **Backoff Formula:** Exponential backoff with configurable multiplier:
  $$\text{delay} = \min(\text{base\_delay} \times 2^{(\text{attempt} - 1)}, \text{max\_delay})$$
- **Base Delay:** Configurable (Default: 1.0 second in standard runs; **0.0 seconds** in automated test environments).
- **Max Delay:** Configurable (Default: 10.0 seconds).
- **Behavior After Final Failed Attempt:**
  1. Cease retries.
  2. Transition idempotency record status to `FAILED`.
  3. Write full failure context into the Dead-Letter Queue (`dead_letter_records`).
  4. Return an HTTP `502 Bad Gateway` response to the caller.

---

## 11. Deterministic Testing Requirements
To ensure fast, reliable test execution:
- Retry delays must be fully injectable and configurable via application settings or test fixtures.
- Test suites **must not** sleep for real wall-clock delays (e.g., no waiting for 1s, 2s, 4s).
- The test configuration sets `base_delay = 0.0`, allowing tests to complete in milliseconds while still asserting:
  - Total attempt count exactly matches configuration.
  - Intermediate failures are recorded in the attempt history in proper chronological order.
  - Destination adapter receives the expected retry sequence.
  - Final terminal state (`COMPLETED` or `FAILED`) is correctly asserted.

---

## 12. Dead-Letter Queue (DLQ) Specification

### Entry Triggers
An event is persisted to `dead_letter_records` **only** when:
1. A retryable destination failure has exhausted all configured retry attempts.
2. A non-retryable destination failure occurs during delivery (e.g., downstream 400 or 422).

*Note: Payloads that fail initial incoming schema validation (Pydantic) are rejected at the HTTP edge with HTTP 422 and do NOT enter the DLQ, as they never reached the execution or integration phase.*

### DLQ Schema & Retained Attributes
The `dead_letter_records` table retains:
- `id` (INTEGER, Primary Key Autoincrement)
- `idempotency_key` (TEXT, Indexed)
- `raw_payload` (TEXT, original incoming JSON string, preserved intact)
- `normalized_payload` (TEXT, JSON string of normalized record, if normalization succeeded)
- `error_category` (TEXT: `RATE_LIMIT`, `SERVER_ERROR`, `TIMEOUT`, `CLIENT_ERROR`, `DATABASE_ERROR`)
- `http_status` (INTEGER, nullable)
- `error_message` (TEXT)
- `attempt_count` (INTEGER)
- `attempt_history` (TEXT, JSON-serialized array of each attempt's timestamp, status code, and error details)
- `created_at` (TIMESTAMP)

### Inspection & Scope
- DLQ replay is explicitly **out of scope** for the core implementation.
- Inspection is accomplished via standard SQLite queries or a simple CLI inspection helper script, not an administrative web UI or API.

---

## 13. Database Failure Handling & Transaction Integrity
The persistence layer must protect against partial writes and inconsistent state:
1. **Transaction Atomicity:** All status transitions, processed record stores, and DLQ inserts must be executed within transactional database scopes (`BEGIN` ... `COMMIT` / `ROLLBACK`).
2. **Persistence Failure Handling (Infrastructure vs. Business Failure):** If a database write fails (e.g., SQLite disk error or lock timeout) during record processing:
   - The transaction is rolled back completely.
   - The system must **never** return a success (200/201) response to the caller if persistence fails.
   - The API returns an HTTP `500 Internal Server Error`.
   - **No Key Poisoning:** Because the transaction is rolled back, the idempotency record is **not** committed to a permanent terminal `FAILED` state. A temporary infrastructure failure does not poison the idempotency key, allowing subsequent legitimate retries to be evaluated cleanly once database connectivity/locking clears.
3. **Automated Testing:** Tests must simulate a database error/rollback scenario and assert that uncommitted operations do not leave orphaned `PROCESSING` states or return false-positive success codes.

---

## 14. Security Considerations
While this project is a local demonstration and deliberately avoids heavy authentication infrastructure, the design documents the following real-world security practices:
1. **Webhook Signature Verification (Production Context):** In live deployments, webhook endpoints should verify cryptographic signatures (e.g., HMAC-SHA256 headers) using shared secrets before processing payloads.
2. **Request Body Size Limits:** Endpoints should enforce reasonable payload size limits (e.g., 1MB) to prevent memory exhaustion / denial of service.
3. **Information Disclosure Prevention:** Internal stack traces, raw database error strings, or system paths must never be returned in HTTP error response bodies; clients receive sanitized error descriptions.
4. **Credential Isolation:** The demonstration uses no third-party API keys, bearer tokens, or sensitive credentials.

---

## 15. Testing Requirements
Automated tests in `pytest` must validate the system deterministically without external network requests:
1. **Validation Tests:** Missing fields, invalid types, and malformed JSON return HTTP 422 with predictable error structures.
2. **Normalization Tests:** Diverse external field formats (e.g., snake_case vs camelCase, string timestamps to ISO formats) convert correctly to canonical internal representations.
3. **Idempotency Tests:**
   - Case A: First request processes to completion.
   - Case B: Duplicate request with identical payload returns cached completed response without re-invoking the destination adapter.
   - Case C: Duplicate key with modified payload returns HTTP 409 Conflict.
   - Case D: Concurrent requests with same key are blocked from double processing.
   - Case E: Duplicate request after `FAILED` state returns previous failure without re-triggering retries.
4. **Retry Behavior Tests:**
   - Deterministic retries on simulated 429, 500, and timeouts up to configured max attempts.
   - Assertion of attempt count and attempt history records with zero test sleep delays.
5. **Non-Retryable Tests:** Immediate failure on 400/401/403/422 without triggering retries.
6. **DLQ Preservation Tests:** Exhausted attempts and non-retryable errors correctly populate `dead_letter_records` with raw payload and attempt history.
7. **Database Failure Tests:** Transaction failure/rollback behavior verified on simulated database error.

---

## 16. Technology Direction
- **Language:** Python (3.11+)
- **API Framework:** FastAPI
- **Validation & Serialization:** Pydantic (v2)
- **Local Persistence:** SQLite (standard library `sqlite3` or lightweight SQLAlchemy core)
- **HTTP Client / Abstract Boundary:** `httpx` (or standard library for mock boundaries)
- **Test Suite:** `pytest` + `pytest-asyncio` / FastAPI `TestClient`
- **Structured Logging:** Standard library `logging` formatted as JSON lines

---

## 17. Non-Goals
The project explicitly excludes:
- Multi-tenancy, user authentication, or RBAC systems
- SaaS billing, subscriptions, or customer management
- Distributed message brokers (Kafka, RabbitMQ)
- Background worker processes (Celery, RQ, Dramatiq)
- In-memory cache clusters (Redis, Memcached)
- Cloud infrastructure or Kubernetes deployment manifests
- Third-party workflow platforms (n8n, Make.com, Zapier)
- Specific vendor platform integrations (Shopify, Stripe, HubSpot, WhatsApp)
- Third-party AI model integrations (OpenAI, Gemini)
- Automated DLQ replay engines or administrative web dashboards

---

## 18. Frontend Decision
- **Decision:** No frontend will be created during Phase 0 or the core backend proof phases.
- **Rationale:** The engineering signal is strictly focused on backend API reliability, payload handling, retry logic, idempotency, and failure tolerance.
- **Future Reconsideration:** A simple inspection UI will only be considered later if there is a distinct visual presentation requirement.

---

## 19. Proof-of-Work Objective
Provide a clean, self-contained repository where, after the documented Python environment setup, any reviewer can run the test suite with a single command and without paid cloud accounts or external services, inspecting it as proof of disciplined backend engineering for API integrations.

---

## 20. Definition of Done (Overall Project)
The project will be complete when:
1. `PROJECT_SPEC.md` is approved and verified as internally consistent.
2. Webhook endpoint is running and processes requests synchronously.
3. Input validation, normalization, and idempotency protection function for all defined test cases (A through E).
4. `DestinationAdapter` interface cleanly isolates destination behavior.
5. Configurable retry engine handles 429, 500, and timeouts with exponential backoff and runs in sub-second time in tests.
6. DLQ reliably persists failed requests with complete context and history.
7. Structured JSON logging logs all lifecycle events with request correlation IDs.
8. 100% of test scenarios pass locally with zero network or cloud infrastructure requirements.
9. Documentation clearly explains setup, architecture, and verification steps.
