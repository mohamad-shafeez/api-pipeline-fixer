"""
Structured JSON logging for Resilient Integration Pipeline.

Provides structured lifecycle logging without third-party dependencies,
formatting log records as compact JSON with correlation metadata.
"""

from datetime import datetime, timezone
import json
import logging
from typing import Any, Optional


class StructuredJsonFormatter(logging.Formatter):
    """
    Standard library logging formatter that renders records as compact JSON objects.
    """

    def format(self, record: logging.LogRecord) -> str:
        log_obj: dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        # Include structured contextual attributes if passed via extra
        context_keys = (
            "event",
            "event_id",
            "event_type",
            "idempotency_status",
            "attempt",
            "destination_status",
            "error_category",
            "outcome",
            "dlq_id",
            "delay_seconds",
        )
        for key in context_keys:
            if hasattr(record, key):
                log_obj[key] = getattr(record, key)

        if hasattr(record, "props") and isinstance(record.props, dict):
            log_obj.update(record.props)

        if record.exc_info:
            log_obj["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_obj, ensure_ascii=False)


logger = logging.getLogger("api_pipeline")


def setup_logging(level: int = logging.INFO) -> None:
    """Configure logger with standard JSON formatter."""
    logger.setLevel(level)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(StructuredJsonFormatter())
        logger.addHandler(handler)
    logger.propagate = True


def log_event(
    event: str,
    level: int = logging.INFO,
    message: Optional[str] = None,
    **kwargs: Any,
) -> None:
    """Emit a structured lifecycle event log entry."""
    msg = message or event
    logger.log(level, msg, extra={"event": event, **kwargs})
