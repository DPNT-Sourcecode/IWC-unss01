"""Typed payloads shared across IWC queue layers."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class TaskSubmission:
    """Payload accepted by ``Queue.enqueue``.

    Attributes:
        provider: Upstream service identifier (for example ``credit_check``).
        user_id: Customer identifier used by Rule-of-3 prioritization.
        timestamp: Task creation time as ``datetime`` or ISO-compatible string.
        metadata: Internal mutable metadata used for legacy prioritization.
    """

    provider: str
    user_id: int | str
    timestamp: datetime | str
    metadata: dict[str, object] = field(default_factory=dict)


@dataclass
class TaskDispatch:
    """Payload returned by ``Queue.dequeue``.

    Attributes:
        provider: Upstream service identifier to be processed.
        user_id: Customer identifier for the dispatched task.
    """

    provider: str
    user_id: int | str


__all__ = ["TaskSubmission", "TaskDispatch"]
