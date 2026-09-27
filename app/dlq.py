"""
Dead-Letter Queue (DLQ) inspection utilities and CLI helper.

Provides read-only inspection of dead-letter records persisted during terminal
destination failures per PROJECT_SPEC.md Section 12.
"""

import argparse
from dataclasses import asdict, dataclass
import json
import sqlite3
import sys
from typing import Any, Optional
from app.database import get_connection, get_db_path


@dataclass
class DeadLetterRecord:
    """Domain model representing a dead-letter queue record."""

    id: int
    idempotency_key: str
    raw_payload: str
    normalized_payload: Optional[str]
    error_category: str
    http_status: Optional[int]
    error_message: str
    attempt_count: int
    attempt_history: list[dict[str, Any]]
    created_at: str


def list_dead_letters(db_path: Optional[str] = None) -> list[DeadLetterRecord]:
    """Retrieve all dead-letter records in chronological order."""
    conn = get_connection(db_path)
    try:
        cursor = conn.execute(
            """
            SELECT id, idempotency_key, raw_payload, normalized_payload,
                   error_category, http_status, error_message, attempt_count,
                   attempt_history, created_at
            FROM dead_letter_records
            ORDER BY id ASC
            """
        )
        records: list[DeadLetterRecord] = []
        for row in cursor.fetchall():
            records.append(
                DeadLetterRecord(
                    id=row["id"],
                    idempotency_key=row["idempotency_key"],
                    raw_payload=row["raw_payload"],
                    normalized_payload=row["normalized_payload"],
                    error_category=row["error_category"],
                    http_status=row["http_status"],
                    error_message=row["error_message"],
                    attempt_count=row["attempt_count"],
                    attempt_history=json.loads(row["attempt_history"])
                    if row["attempt_history"]
                    else [],
                    created_at=row["created_at"],
                )
            )
        return records
    finally:
        conn.close()


def get_dead_letter(
    dlq_id: int, db_path: Optional[str] = None
) -> Optional[DeadLetterRecord]:
    """Retrieve a single dead-letter record by its DLQ record ID."""
    conn = get_connection(db_path)
    try:
        cursor = conn.execute(
            """
            SELECT id, idempotency_key, raw_payload, normalized_payload,
                   error_category, http_status, error_message, attempt_count,
                   attempt_history, created_at
            FROM dead_letter_records
            WHERE id = ?
            """,
            (dlq_id,),
        )
        row = cursor.fetchone()
        if row is None:
            return None
        return DeadLetterRecord(
            id=row["id"],
            idempotency_key=row["idempotency_key"],
            raw_payload=row["raw_payload"],
            normalized_payload=row["normalized_payload"],
            error_category=row["error_category"],
            http_status=row["http_status"],
            error_message=row["error_message"],
            attempt_count=row["attempt_count"],
            attempt_history=json.loads(row["attempt_history"])
            if row["attempt_history"]
            else [],
            created_at=row["created_at"],
        )
    finally:
        conn.close()


def main() -> None:
    """CLI entry point for read-only DLQ inspection."""
    parser = argparse.ArgumentParser(
        prog="python -m app.dlq",
        description="Inspect dead-letter records from the Resilient Integration Pipeline.",
    )
    parser.add_argument(
        "--db",
        type=str,
        default=None,
        help="Path to SQLite database file (default: active pipeline.db)",
    )

    subparsers = parser.add_subparsers(dest="command")

    # Command: list
    subparsers.add_parser("list", help="List all dead-letter records")

    # Command: show <id>
    show_parser = subparsers.add_parser("show", help="Show full details of a DLQ record")
    show_parser.add_argument("id", type=int, help="DLQ record ID to inspect")

    args = parser.parse_args()
    target_db = args.db or get_db_path()

    if args.command == "list" or args.command is None:
        records = list_dead_letters(target_db)
        if not records:
            print(f"No dead-letter records found in {target_db}.")
            return

        print(f"=== Dead-Letter Records ({len(records)} found in {target_db}) ===")
        print(
            f"{'ID':<5} {'IDEMPOTENCY KEY':<30} {'CATEGORY':<15} {'STATUS':<8} {'ATTEMPTS':<10} {'CREATED AT'}"
        )
        print("-" * 95)
        for r in records:
            status_str = str(r.http_status) if r.http_status is not None else "N/A"
            print(
                f"{r.id:<5} {r.idempotency_key:<30} {r.error_category:<15} {status_str:<8} {r.attempt_count:<10} {r.created_at}"
            )

    elif args.command == "show":
        record = get_dead_letter(args.id, target_db)
        if record is None:
            print(f"DLQ Record ID {args.id} not found in {target_db}.")
            sys.exit(1)

        print(f"=== DLQ Record ID: {record.id} ===")
        print(f"Idempotency Key:    {record.idempotency_key}")
        print(f"Error Category:     {record.error_category}")
        print(f"HTTP Status:        {record.http_status}")
        print(f"Error Message:      {record.error_message}")
        print(f"Total Attempts:     {record.attempt_count}")
        print(f"Created At:         {record.created_at}")
        print(f"\nRaw Payload:\n{record.raw_payload}")
        print(f"\nNormalized Payload:\n{record.normalized_payload}")
        print("\nAttempt History:")
        for att in record.attempt_history:
            print(f"  - Attempt {att.get('attempt')}: status={att.get('status_code')}, timeout={att.get('is_timeout')}, error={att.get('error_message')}")


if __name__ == "__main__":
    main()
