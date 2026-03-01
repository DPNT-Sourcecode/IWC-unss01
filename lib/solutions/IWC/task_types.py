"""Typed payloads shared across IWC queue layers.

These dataclasses define the boundary contract used by:
- the queue implementation,
- the IWC queue entrypoint,
- and tests validating IWC_R1 behavior.
"""

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
    user_id: int
    timestamp: datetime | str
    metadata: dict[str, object] = field(default_factory=dict)

@dataclass
class TaskDispatch:
    """Payload returned by ``Queue.dequeue``.

    Note:
        IWC_R1 contract expects ``timestamp`` to be included in dequeue
        responses. This dataclass currently exposes only ``provider`` and
        ``user_id`` and is a planned implementation update point.
    """

    provider: str
    user_id: int


__all__ = ["TaskSubmission", "TaskDispatch"]

