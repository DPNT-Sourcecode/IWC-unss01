"""Shared test helpers for IWC queue contract scenarios."""

from __future__ import annotations

from dataclasses import asdict, is_dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Iterable

from solutions.IWC.queue_solution_entrypoint import QueueSolutionEntrypoint
from solutions.IWC.task_types import TaskDispatch, TaskSubmission


DEFAULT_SCENARIO_BASE = datetime(2025, 1, 1, 12, 0, tzinfo=timezone.utc)


def iso_ts(*, base: datetime = DEFAULT_SCENARIO_BASE, delta_minutes: int = 0) -> str:
    return str(base + timedelta(minutes=delta_minutes))


class QueueActionBuilder:
    def __init__(
        self,
        name: str,
        payload: Any | None = None,
        expect_factory: Callable[..., Any] | None = None,
    ) -> None:
        self._name = name
        self._payload = payload
        self._expect_factory = expect_factory or (lambda value: value)

    def expect(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        expectation = self._expect_factory(*args, **kwargs)
        return {"name": self._name, "input": self._payload, "expect": expectation}


def call_enqueue(provider: str, user_id: int | str, timestamp: str) -> QueueActionBuilder:
    return QueueActionBuilder(
        "enqueue",
        TaskSubmission(provider=provider, user_id=user_id, timestamp=timestamp),
    )


def call_size() -> QueueActionBuilder:
    return QueueActionBuilder("size")


def call_dequeue() -> QueueActionBuilder:
    return QueueActionBuilder(
        "dequeue",
        expect_factory=lambda provider, user_id: TaskDispatch(
            provider=provider, user_id=user_id
        ),
    )


def call_dequeue_none() -> QueueActionBuilder:
    return QueueActionBuilder("dequeue", expect_factory=lambda: None)


def call_age() -> QueueActionBuilder:
    return QueueActionBuilder("age")


def call_purge() -> QueueActionBuilder:
    return QueueActionBuilder("purge")


def _canonicalize(value: Any) -> Any:
    if is_dataclass(value):
        return asdict(value)
    if isinstance(value, dict):
        return {k: _canonicalize(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_canonicalize(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_canonicalize(item) for item in value)
    return value


def run_queue(
    actions: Iterable[dict[str, Any]], queue: QueueSolutionEntrypoint | None = None
) -> None:
    queue = queue or QueueSolutionEntrypoint()
    for position, step in enumerate(actions, start=1):
        method: Callable[..., Any] = getattr(queue, step["name"])
        args = () if step["input"] is None else (step["input"],)
        actual = method(*args)
        expected = step["expect"]

        if callable(expected):
            matched = expected(actual)
        else:
            matched = _canonicalize(actual) == _canonicalize(expected)

        if not matched:
            payload = step.get("input")
            payload_repr = "" if payload is None else f" input={payload!r}"
            raise AssertionError(
                "Step {} '{}'{} expected {!r} but got {!r}".format(
                    position,
                    step["name"],
                    payload_repr,
                    expected,
                    actual,
                )
            )

# Fixed base timestamp for deterministic scenario tests.
# This keeps ordering assertions stable and easy to read.
SCENARIO_BASE = datetime(2025, 10, 20, 12, 0, 0)


def scenario_ts(*, delta_seconds: int = 0) -> str:
    """
    Deterministic timestamp in queue contract format: YYYY-MM-DD HH:MM:SS.
    """
    return (SCENARIO_BASE + timedelta(seconds=delta_seconds)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )


def now_ts(*, delta_seconds: int = 0) -> str:
    """
    Wall-clock timestamp for age behavior tests.
    """
    return (datetime.now() + timedelta(seconds=delta_seconds)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )


def new_queue() -> QueueSolutionEntrypoint:
    """Create a fresh queue instance for each test."""
    return QueueSolutionEntrypoint()


def task_submission(
    provider: str, user_id: int | str, timestamp: str
) -> TaskSubmission:
    """Build TaskSubmission payloads consistently."""
    return TaskSubmission(provider=provider, user_id=user_id, timestamp=timestamp)


def enqueue_task(
    queue: QueueSolutionEntrypoint, provider: str, user_id: int | str, timestamp: str
) -> int:
    """Enqueue helper returning queue size after insertion."""
    return queue.enqueue(task_submission(provider, user_id, timestamp))


def normalize_dispatch(item: Any) -> dict[str, Any] | None:
    """
    Normalize dequeue output so tests can assert plain dictionaries.

    Supports None, dataclass payloads, dict payloads, and generic objects.
    """
    if item is None:
        return None

    if is_dataclass(item):
        return asdict(item)

    if isinstance(item, dict):
        return dict(item)

    return {
        "provider": getattr(item, "provider"),
        "user_id": getattr(item, "user_id"),
    }


def dequeue_task(queue: QueueSolutionEntrypoint) -> dict[str, Any] | None:
    """Dequeue helper returning normalized dict or None."""
    return normalize_dispatch(queue.dequeue())


__all__ = [
    "DEFAULT_SCENARIO_BASE",
    "iso_ts",
    "QueueActionBuilder",
    "call_enqueue",
    "call_size",
    "call_dequeue",
    "call_dequeue_none",
    "call_age",
    "call_purge",
    "run_queue",
    "scenario_ts",
    "now_ts",
    "new_queue",
    "task_submission",
    "enqueue_task",
    "normalize_dispatch",
    "dequeue_task",
]



