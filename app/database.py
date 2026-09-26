"""
SQLite persistence and idempotency repository for Resilient Integration Pipeline.

Implements atomic idempotency key claims, status lifecycle tracking
(PROCESSING, COMPLETED, FAILED), cached response storage, and transaction
integrity per PROJECT_SPEC.md Sections 9 and 13.
"""

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import sqlite3
from typing import Generator, Optional

DEFAULT_DB_PATH: str = "pipeline.db"
_DB_PATH: str = DEFAULT_DB_PATH


def set_db_path(path: str) -> None:
    """Set the active SQLite database file path."""
    global _DB_PATH
    _DB_PATH = path


def get_db_path() -> str:
    """Get the active SQLite database file path."""
    return _DB_PATH


def get_connection(db_path: Optional[str] = None) -> sqlite3.Connection:
    """
    Create a new SQLite connection with optimal concurrency settings.

    Sets row_factory to sqlite3.Row and establishes a 5.0 second busy timeout.
    """
    path = db_path if db_path is not None else get_db_path()
    conn = sqlite3.connect(path, timeout=5.0)
    conn.row_factory = sqlite3.Row
    return conn


@contextmanager
def get_db(db_path: Optional[str] = None) -> Generator[sqlite3.Connection, None, None]:
    """Context manager for acquiring and safely closing a database connection."""
    conn = get_connection(db_path)
    try:
        yield conn
    finally:
        conn.close()


def init_db(db_path: Optional[str] = None) -> None:
    """
    Initialize the SQLite database schema if not already present.

    Creates the `idempotency_records` table with a PRIMARY KEY constraint
    on `idempotency_key` ensuring atomic claim uniqueness.
    """
    with get_db(db_path) as conn:
        with conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS idempotency_records (
                    idempotency_key TEXT PRIMARY KEY,
                    payload_hash TEXT NOT NULL,
                    status TEXT NOT NULL CHECK (status IN ('PROCESSING', 'COMPLETED', 'FAILED')),
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    response_status_code INTEGER,
                    response_body TEXT
                );
                """
            )


@dataclass
class IdempotencyRecord:
    """Domain representation of a stored idempotency record."""

    idempotency_key: str
    payload_hash: str
    status: str
    created_at: str
    updated_at: str
    response_status_code: Optional[int] = None
    response_body: Optional[str] = None


@dataclass
class ClaimOutcome:
    """Result of an atomic idempotency key claim attempt."""

    is_new: bool
    record: IdempotencyRecord


def claim_idempotency_key(
    conn: sqlite3.Connection,
    idempotency_key: str,
    payload_hash: str,
) -> ClaimOutcome:
    """
    Atomically claim an idempotency key with status='PROCESSING'.

    Uses SQLite's PRIMARY KEY constraint to guarantee atomicity. If two concurrent
    requests attempt to claim the same key, exactly one INSERT succeeds. The other
    raises sqlite3.IntegrityError, which triggers a lookup of the existing record.

    Returns:
        ClaimOutcome with is_new=True if the key was newly claimed by this caller,
        or is_new=False containing the existing record if already present.
    """
    now = datetime.now(timezone.utc).isoformat()
    try:
        with conn:
            conn.execute(
                """
                INSERT INTO idempotency_records (
                    idempotency_key, payload_hash, status, created_at, updated_at
                ) VALUES (?, ?, 'PROCESSING', ?, ?)
                """,
                (idempotency_key, payload_hash, now, now),
            )
        return ClaimOutcome(
            is_new=True,
            record=IdempotencyRecord(
                idempotency_key=idempotency_key,
                payload_hash=payload_hash,
                status="PROCESSING",
                created_at=now,
                updated_at=now,
            ),
        )
    except sqlite3.IntegrityError:
        cursor = conn.execute(
            """
            SELECT idempotency_key, payload_hash, status, created_at, updated_at,
                   response_status_code, response_body
            FROM idempotency_records
            WHERE idempotency_key = ?
            """,
            (idempotency_key,),
        )
        row = cursor.fetchone()
        if row is None:
            raise
        return ClaimOutcome(
            is_new=False,
            record=IdempotencyRecord(
                idempotency_key=row["idempotency_key"],
                payload_hash=row["payload_hash"],
                status=row["status"],
                created_at=row["created_at"],
                updated_at=row["updated_at"],
                response_status_code=row["response_status_code"],
                response_body=row["response_body"],
            ),
        )


def complete_idempotency_record(
    conn: sqlite3.Connection,
    idempotency_key: str,
    status_code: int,
    response_body: str,
) -> None:
    """
    Transition a claimed idempotency record from 'PROCESSING' to 'COMPLETED'.

    Persists the HTTP status code and serialized response body for cached replays.
    """
    now = datetime.now(timezone.utc).isoformat()
    with conn:
        cursor = conn.execute(
            """
            UPDATE idempotency_records
            SET status = 'COMPLETED',
                response_status_code = ?,
                response_body = ?,
                updated_at = ?
            WHERE idempotency_key = ?
            """,
            (status_code, response_body, now, idempotency_key),
        )
        if cursor.rowcount == 0:
            raise sqlite3.OperationalError(
                f"Failed to complete record: {idempotency_key} not found"
            )


def rollback_idempotency_claim(
    conn: sqlite3.Connection,
    idempotency_key: str,
) -> None:
    """
    Remove an orphaned 'PROCESSING' claim following an infrastructure/persistence failure.

    Ensures that temporary persistence errors do not permanently poison the idempotency key.
    """
    try:
        with conn:
            conn.execute(
                """
                DELETE FROM idempotency_records
                WHERE idempotency_key = ? AND status = 'PROCESSING'
                """,
                (idempotency_key,),
            )
    except Exception:
        # Ignore errors during best-effort cleanup
        pass


def fail_idempotency_record(
    conn: sqlite3.Connection,
    idempotency_key: str,
    status_code: int = 502,
    response_body: Optional[str] = None,
) -> None:
    """
    Transition a claimed idempotency record from 'PROCESSING' to 'FAILED'.
    """
    now = datetime.now(timezone.utc).isoformat()
    with conn:
        conn.execute(
            """
            UPDATE idempotency_records
            SET status = 'FAILED',
                response_status_code = ?,
                response_body = ?,
                updated_at = ?
            WHERE idempotency_key = ?
            """,
            (status_code, response_body, now, idempotency_key),
        )


def record_idempotency_failure(
    conn: sqlite3.Connection,
    idempotency_key: str,
    payload_hash: str,
    status_code: int = 502,
    response_body: Optional[str] = None,
) -> None:
    """
    Insert or update a record with status='FAILED' for testing or terminal failure handling.
    """
    now = datetime.now(timezone.utc).isoformat()
    with conn:
        conn.execute(
            """
            INSERT INTO idempotency_records (
                idempotency_key, payload_hash, status, created_at, updated_at,
                response_status_code, response_body
            ) VALUES (?, ?, 'FAILED', ?, ?, ?, ?)
            ON CONFLICT(idempotency_key) DO UPDATE SET
                status = 'FAILED',
                response_status_code = excluded.response_status_code,
                response_body = excluded.response_body,
                updated_at = excluded.updated_at
            """,
            (idempotency_key, payload_hash, now, now, status_code, response_body),
        )


def get_idempotency_record(
    conn: sqlite3.Connection,
    idempotency_key: str,
) -> Optional[IdempotencyRecord]:
    """Retrieve an idempotency record by key, if present."""
    cursor = conn.execute(
        """
        SELECT idempotency_key, payload_hash, status, created_at, updated_at,
               response_status_code, response_body
        FROM idempotency_records
        WHERE idempotency_key = ?
        """,
        (idempotency_key,),
    )
    row = cursor.fetchone()
    if row is None:
        return None
    return IdempotencyRecord(
        idempotency_key=row["idempotency_key"],
        payload_hash=row["payload_hash"],
        status=row["status"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        response_status_code=row["response_status_code"],
        response_body=row["response_body"],
    )
