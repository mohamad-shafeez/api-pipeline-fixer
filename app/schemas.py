"""
Pydantic v2 schemas for webhook ingestion.

Defines the contract for incoming webhook events and the structured response
returned by the ingestion boundary.
"""

from datetime import datetime
from typing import Any
from pydantic import BaseModel, Field


class WebhookPayload(BaseModel):
    """
    Initial webhook payload schema for event-driven integration pipeline.

    Field choices:
    - `event_id`: Unique identifier for the incoming event. Serves as the
      idempotency key for duplicate prevention in subsequent pipeline phases
      (as specified in PROJECT_SPEC.md Section 9).
    - `event_type`: Categorical identifier describing the business event
      (e.g., 'order.created', 'payment.completed').
    - `timestamp`: ISO 8601 timestamp representing when the event originated.
    - `data`: Arbitrary structured dictionary containing the event's domain payload.
    """

    event_id: str = Field(
        ...,
        min_length=1,
        description="Unique event identifier used for idempotency tracking",
        examples=["evt_live_987654321"],
    )
    event_type: str = Field(
        ...,
        min_length=1,
        description="Event category or type identifier",
        examples=["order.created"],
    )
    timestamp: datetime = Field(
        ...,
        description="ISO 8601 event timestamp",
        examples=["2026-09-26T12:00:00Z"],
    )
    data: dict[str, Any] = Field(
        ...,
        description="Nested payload/data dictionary for the event",
        examples=[{"order_id": "ord_123", "amount": 99.50, "currency": "USD"}],
    )


class WebhookResponse(BaseModel):
    """
    Structured response returned upon successful ingestion boundary validation.

    Indicates receipt and schema acceptance. Does not claim downstream delivery.
    """

    status: str = Field(
        default="accepted",
        description="Ingestion status indicating the event was accepted at the boundary",
    )
    event_id: str = Field(
        ...,
        description="The event identifier that was accepted",
    )
    message: str = Field(
        default="Event accepted by ingestion boundary",
        description="Human-readable acknowledgment message",
    )
