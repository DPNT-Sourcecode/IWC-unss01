from __future__ import annotations

from .utils import call_dequeue, call_enqueue, call_size, iso_ts, run_queue
import time
from dataclasses import asdict, is_dataclass
from datetime import datetime

from solutions.IWC.queue_solution_entrypoint import QueueSolutionEntrypoint
from solutions.IWC.task_types import TaskSubmission

def test_enqueue_size_dequeue_flow() -> None:
    run_queue([
        call_enqueue("companies_house", 1, iso_ts(delta_minutes=0)).expect(1),
        call_size().expect(1),
        call_dequeue().expect("companies_house", 1),
    ])

def _enqueue(
    queue: QueueSolutionEntrypoint, provider: str, user_id: int, timestamp: str
) -> int:
    return queue.enqueue(
        TaskSubmission(provider=provider, user_id=user_id, timestamp=timestamp)
    )


def _dequeue_as_dict(queue: QueueSolutionEntrypoint) -> dict | None:
    item = queue.dequeue()
    if item is None:
        return None
    if is_dataclass(item):
        return asdict(item)
    return dict(item)

def test_empty_queue_contract() -> None:
    queue = QueueSolutionEntrypoint()

    assert queue.size() == 0
    assert _dequeue_as_dict(queue) is None
    assert queue.purge() is True
    assert isinstance(queue.age(), int)
    assert queue.age() >= 0
