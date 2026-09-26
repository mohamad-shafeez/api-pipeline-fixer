# Resilient Integration Pipeline

**Personal proof-of-work** · Python · FastAPI · SQLite · pytest

---

## Purpose

This repository is an independent, personal proof-of-work engineering demonstration. It implements a resilient webhook ingestion pipeline in Python, designed to showcase disciplined backend engineering around the problems that reliably break real-world API integrations:

- Schema validation and clean error responses for malformed payloads
- Idempotency protection against duplicate webhook deliveries
- Classified retry logic with exponential backoff for transient downstream failures (HTTP 429, 500, timeouts)
- Dead-letter storage with full context when retries are exhausted
- Structured JSON logging for complete request lifecycle observability

The project uses only local, self-contained infrastructure (SQLite, deterministic adapter simulation) — no paid cloud services, no external API credentials, and no proprietary client data.

This is **not** a client project. It demonstrates transferable backend integration patterns for potential clients and technical reviewers.

---

## Current Status

**Phase 1 — Environment & Repository Foundation**

The project specification is frozen in [`PROJECT_SPEC.md`](PROJECT_SPEC.md). Core pipeline implementation begins in Phase 2.

---

## Technology Stack

| Concern | Tool |
|---|---|
| Language | Python 3.11+ |
| API Framework | FastAPI |
| Validation | Pydantic v2 |
| Persistence | SQLite (stdlib `sqlite3`) |
| HTTP Client | httpx |
| Tests | pytest |
| Logging | stdlib `logging` (JSON output) |

---

## Environment Setup

### Prerequisites

- Python 3.11 or later
- git

### 1. Clone the repository

```bash
git clone <repository-url>
cd api-pipeline-fixer
```

### 2. Create and activate the virtual environment

**Windows (PowerShell):**
```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
```

**macOS / Linux:**
```bash
python -m venv .venv
source .venv/bin/activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Run the test suite

```bash
pytest
```

At this stage (Phase 1), no application tests exist yet. `pytest` should exit cleanly reporting no tests collected. Tests will be added in later phases alongside the pipeline implementation.

---

## Project Structure

```
api-pipeline-fixer/
├── app/                  # Application source (populated in Phase 2+)
├── tests/                # Test suite (populated in Phase 2+)
├── docs/                 # Additional documentation
├── PROJECT_SPEC.md       # Frozen architectural specification
├── requirements.txt      # Pinned dependencies
├── README.md             # This file
└── .gitignore
```

---

## Planned Functionality (per PROJECT_SPEC.md)

- `POST /webhook` — ingestion endpoint
- Pydantic schema validation with clean 422 responses
- Canonical payload normalization
- Idempotency service (5 defined cases, A–E)
- DestinationAdapter interface with deterministic simulation
- Retry engine with configurable exponential backoff
- Dead-letter queue (SQLite-backed)
- Structured JSON lifecycle logging
- Full pytest test coverage of all scenarios including simulated failures

See [`PROJECT_SPEC.md`](PROJECT_SPEC.md) for the complete specification.

---

## Disclaimer

This is an independent personal demonstration project. It is not affiliated with, commissioned by, or representative of any commercial client. It contains no real client data, no proprietary credentials, and makes no claims of production deployment.
