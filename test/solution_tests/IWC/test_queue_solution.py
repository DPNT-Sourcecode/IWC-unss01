from __future__ import annotations

from dataclasses import asdict, is_dataclass
from datetime import datetime, timedelta
from typing import Any

from solutions.IWC.queue_solution_entrypoint import QueueSolutionEntrypoint
from solutions.IWC.task_types import TaskSubmission

# Fixed base timestamp for deterministic scenario tests.
# This keeps ordering assertions stable and easy to read.
SCENARIO_BASE = datetime(2025, 10, 20, 12, 0, 0)

def test_enqueue_size_dequeue_flow() -> None:
    run_queue([
        call_enqueue("companies_house", 1, iso_ts(delta_minutes=0)).expect(1),
        call_size().expect(1),
        call_dequeue().expect("companies_house", 1),
    ])

def scenario_ts(*, delta_seconds: int = 0) -> str:
    """
    Deterministic timestamp in the exact challenge format:
    YYYY-MM-DD HH:MM:SS
    """
    return (SCENARIO_BASE + timedelta(seconds=delta_seconds)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )

def now_ts(*, delta_seconds: int = 0) -> str:
    """
    Wall-clock timestamp for age() behavior tests.
    """
    return (datetime.now() + timedelta(seconds=delta_seconds)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )

def new_queue() -> QueueSolutionEntrypoint:
    """Create a fresh queue instance per test."""
    return QueueSolutionEntrypoint()


def task_submission(provider: str, user_id: int, timestamp: str) -> TaskSubmission:
    """Build TaskSubmission payloads consistently."""
    return TaskSubmission(provider=provider, user_id=user_id, timestamp=timestamp)

def enqueue_task(
    queue: QueueSolutionEntrypoint, provider: str, user_id: int, timestamp: str
) -> int:
    """Enqueue helper returning queue size after insertion."""
    return queue.enqueue(task_submission(provider, user_id, timestamp))

