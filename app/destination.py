"""
Destination adapter boundary, deterministic simulator, and retry engine.

Defines the abstract interface for downstream destination delivery,
failure classification (retryable vs non-retryable), exponential backoff
calculations, and deterministic execution per PROJECT_SPEC.md Sections 8 and 10.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
import logging
import time
from typing import Any, Callable, Optional
from app.logging_conf import log_event
from app.schemas import WebhookPayload


@dataclass
class DestinationResponse:
    """Response returned by a destination delivery attempt."""

    status_code: int
    data: Optional[dict[str, Any]] = None
    error_message: Optional[str] = None
    is_timeout: bool = False


class DestinationAdapter(ABC):
    """Abstract interface isolating the external destination delivery boundary."""

    @abstractmethod
    def deliver(self, payload: WebhookPayload) -> DestinationResponse:
        """Deliver the payload to the external destination."""
        pass


class SimulatedDestinationAdapter(DestinationAdapter):
    """
    Deterministic destination simulator for testing and local demonstrations.

    Allows programming a sequence of responses (e.g. [503, 503, 200]) or
    a constant default response. Also supports payload directives inside
    `payload.data` (e.g. 'simulated_responses': [503, 503, 200] or
    'simulated_response': 400 / 'timeout') per PROJECT_SPEC.md Section 7.
    Tracks all delivery calls and payloads.
    """

    def __init__(
        self,
        default_response: Optional[DestinationResponse] = None,
        responses: Optional[list[DestinationResponse]] = None,
    ) -> None:
        self.default_response = default_response or DestinationResponse(
            status_code=200,
            data={"status": "delivered"},
            error_message=None,
            is_timeout=False,
        )
        self.responses: list[DestinationResponse] = list(responses) if responses else []
        self.calls: list[WebhookPayload] = []
        self._directive_queues: dict[str, list[DestinationResponse]] = {}

    def set_responses(self, responses: list[DestinationResponse]) -> None:
        """Queue a sequence of responses to be returned in FIFO order."""
        self.responses = list(responses)

    def set_default_response(self, response: DestinationResponse) -> None:
        """Set the default response used when queued responses are exhausted."""
        self.default_response = response

    def _parse_directive_item(self, item: Any) -> DestinationResponse:
        if isinstance(item, int):
            return DestinationResponse(
                status_code=item,
                error_message=f"Simulated HTTP {item}" if item >= 400 else None,
                data={"status": "delivered"} if item < 400 else None,
            )
        if isinstance(item, str) and item.lower() == "timeout":
            return DestinationResponse(
                status_code=0,
                is_timeout=True,
                error_message="Simulated connection timeout",
            )
        if isinstance(item, dict):
            return DestinationResponse(
                status_code=item.get("status_code", 200),
                is_timeout=item.get("is_timeout", False),
                error_message=item.get("error_message"),
                data=item.get("data"),
            )
        return self.default_response

    def deliver(self, payload: WebhookPayload) -> DestinationResponse:
        self.calls.append(payload)

        # Check for directives in payload.data
        if isinstance(payload.data, dict):
            if "simulated_responses" in payload.data:
                if not self._directive_queues.get(payload.event_id):
                    raw_seq = payload.data["simulated_responses"]
                    self._directive_queues[payload.event_id] = [
                        self._parse_directive_item(item) for item in raw_seq
                    ]
                if self._directive_queues[payload.event_id]:
                    return self._directive_queues[payload.event_id].pop(0)

            if "simulated_response" in payload.data:
                return self._parse_directive_item(payload.data["simulated_response"])

        if self.responses:
            return self.responses.pop(0)
        return self.default_response

    @property
    def call_count(self) -> int:
        return len(self.calls)

    def reset(self) -> None:
        self.calls.clear()
        self.responses.clear()
        self._directive_queues.clear()


@dataclass
class RetryConfig:
    """Configuration parameters for retry and exponential backoff."""

    max_attempts: int = 3
    base_delay: float = 1.0
    max_delay: float = 10.0
    backoff_factor: float = 2.0


def is_retryable(response: DestinationResponse) -> bool:
    """
    Classify whether a destination failure is retryable.

    Retryable:
    - Network / socket timeout (is_timeout=True)
    - HTTP 429 (Rate Limit Exceeded)
    - HTTP 500, 502, 503, 504 (Server Errors)

    Non-retryable:
    - HTTP 400, 401, 403, 422 (Client Errors)
    - Success (200, 201)
    """
    if response.is_timeout:
        return True
    return response.status_code in {429, 500, 502, 503, 504}


def classify_failure(response: DestinationResponse) -> str:
    """
    Categorize destination failure for DLQ evidence context.

    Categories:
    - 'TIMEOUT': network / socket timeout
    - 'RATE_LIMIT': HTTP 429
    - 'SERVER_ERROR': HTTP 500, 502, 503, 504
    - 'CLIENT_ERROR': HTTP 400, 401, 403, 422
    - 'UNKNOWN': unclassified
    """
    if response.is_timeout:
        return "TIMEOUT"
    if response.status_code == 429:
        return "RATE_LIMIT"
    if response.status_code in {500, 502, 503, 504}:
        return "SERVER_ERROR"
    if response.status_code in {400, 401, 403, 422}:
        return "CLIENT_ERROR"
    return "UNKNOWN"


def calculate_backoff(attempt: int, config: RetryConfig) -> float:
    """
    Calculate exponential backoff delay for a given attempt number.

    Formula: min(base_delay * (backoff_factor ** (attempt - 1)), max_delay)
    """
    if config.base_delay <= 0 or attempt <= 0:
        return 0.0
    delay = config.base_delay * (config.backoff_factor ** (attempt - 1))
    return min(delay, config.max_delay)


@dataclass
class DeliveryResult:
    """Result of executing the bounded destination delivery retry loop."""

    success: bool
    final_response: DestinationResponse
    attempt_count: int
    attempt_history: list[dict[str, Any]] = field(default_factory=list)
    error_category: Optional[str] = None


def execute_delivery(
    payload: WebhookPayload,
    adapter: DestinationAdapter,
    config: Optional[RetryConfig] = None,
    sleeper: Callable[[float], None] = time.sleep,
) -> DeliveryResult:
    """
    Execute bounded delivery to the destination adapter with exponential backoff.

    Guarantees:
    - Hard boundary of max_attempts (default 3).
    - Immediate termination on success (200, 201).
    - Immediate termination on non-retryable client errors (400, 401, 403, 422).
    - Exponential backoff sleep between retryable attempts.
    - Full attempt history preserved for DLQ readiness.
    """
    cfg = config or get_retry_config()
    if isinstance(payload.data, dict) and "base_delay" in payload.data:
        try:
            cfg = RetryConfig(
                max_attempts=cfg.max_attempts,
                base_delay=float(payload.data["base_delay"]),
                max_delay=cfg.max_delay,
                backoff_factor=cfg.backoff_factor,
            )
        except (ValueError, TypeError):
            pass
    attempt_history: list[dict[str, Any]] = []
    last_response = DestinationResponse(
        status_code=500, error_message="No delivery attempted"
    )

    for attempt in range(1, cfg.max_attempts + 1):
        log_event(
            "delivery_attempt",
            level=logging.INFO,
            event_id=payload.event_id,
            attempt=attempt,
        )
        try:
            response = adapter.deliver(payload)
        except Exception as exc:
            response = DestinationResponse(
                status_code=0,
                error_message=str(exc),
                is_timeout=False,
            )
        last_response = response

        attempt_history.append(
            {
                "attempt": attempt,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "status_code": response.status_code,
                "is_timeout": response.is_timeout,
                "error_message": response.error_message,
            }
        )

        # Success: 200 OK or 201 Created
        if response.status_code in (200, 201):
            return DeliveryResult(
                success=True,
                final_response=response,
                attempt_count=attempt,
                attempt_history=attempt_history,
                error_category=None,
            )

        # Non-retryable failure: do NOT retry
        if not is_retryable(response):
            return DeliveryResult(
                success=False,
                final_response=response,
                attempt_count=attempt,
                attempt_history=attempt_history,
                error_category=classify_failure(response),
            )

        # Retryable failure: check if attempts exhausted
        if attempt >= cfg.max_attempts:
            return DeliveryResult(
                success=False,
                final_response=response,
                attempt_count=attempt,
                attempt_history=attempt_history,
                error_category=classify_failure(response),
            )

        # Backoff before next attempt
        delay = calculate_backoff(attempt, cfg)
        log_event(
            "retry_scheduled",
            level=logging.WARNING,
            event_id=payload.event_id,
            attempt=attempt,
            destination_status=response.status_code,
            error_category=classify_failure(response),
            delay_seconds=delay,
        )
        if delay > 0:
            sleeper(delay)

    return DeliveryResult(
        success=False,
        final_response=last_response,
        attempt_count=len(attempt_history),
        attempt_history=attempt_history,
        error_category=classify_failure(last_response),
    )


# Global adapter and config instances (injectable for testing / runtime)
_ACTIVE_ADAPTER: DestinationAdapter = SimulatedDestinationAdapter()
_ACTIVE_RETRY_CONFIG: RetryConfig = RetryConfig(max_attempts=3, base_delay=1.0, max_delay=10.0)


def get_destination_adapter() -> DestinationAdapter:
    """Get the active destination adapter instance."""
    return _ACTIVE_ADAPTER


def set_destination_adapter(adapter: DestinationAdapter) -> None:
    """Set the active destination adapter instance."""
    global _ACTIVE_ADAPTER
    _ACTIVE_ADAPTER = adapter


def get_retry_config() -> RetryConfig:
    """Get the active retry configuration."""
    return _ACTIVE_RETRY_CONFIG


def set_retry_config(config: RetryConfig) -> None:
    """Set the active retry configuration."""
    global _ACTIVE_RETRY_CONFIG
    _ACTIVE_RETRY_CONFIG = config
