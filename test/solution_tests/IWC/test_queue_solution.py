from __future__ import annotations

import time
from datetime import datetime, timedelta

from .utils import dequeue_task, enqueue_task, new_queue, now_ts, scenario_ts


def test_enqueue_size_dequeue_flow() -> None:
    run_queue([
        call_enqueue("companies_house", 1, iso_ts(delta_minutes=0)).expect(1),
        call_size().expect(1),
        call_dequeue().expect("companies_house", 1),
    ])

def test_empty_queue_contract() -> None:
    """
    Baseline API contract from IWC_R1:
    - empty queue has size 0
    - dequeue returns null/None when empty
    - purge reports success
    - age is an integer in seconds (defined as 0 for empty queue)
    """
    queue = new_queue()

    assert queue.size() == 0
    assert dequeue_task(queue) is None
    assert queue.purge() is True
    assert isinstance(queue.age(), int)
    assert queue.age() == 0


def test_enqueue_returns_current_queue_size() -> None:
    """
    enqueue(task) should return current queue size after insertion.
    """
    queue = new_queue()

    assert enqueue_task(queue, "companies_house", 1, scenario_ts(delta_seconds=0)) == 1
    assert enqueue_task(queue, "bank_statements", 2, scenario_ts(delta_seconds=1)) == 2
    assert queue.size() == 2


def test_dequeue_returns_required_fields_including_timestamp() -> None:
    """
    IWC_R1 method contract for dequeue includes:
    provider, user_id, timestamp.
    """
    queue = new_queue()
    ts = scenario_ts(delta_seconds=0)

    enqueue_task(queue, "bank_statements", 7, ts)
    result = dequeue_task(queue)

    assert result == {
        "provider": "bank_statements",
        "user_id": 7,
        "timestamp": ts,
    }


def test_rule_of_three_example_from_challenge() -> None:
    """
    IWC_R1 Example #1:
    Once a user reaches 3 queued tasks, all of their tasks are prioritized first.
    """
    queue = new_queue()
    ts = scenario_ts(delta_seconds=0)

    assert enqueue_task(queue, "companies_house", 1, ts) == 1
    assert enqueue_task(queue, "bank_statements", 2, ts) == 2
    assert enqueue_task(queue, "id_verification", 1, ts) == 3
    assert enqueue_task(queue, "bank_statements", 1, ts) == 4

    first = dequeue_task(queue)
    second = dequeue_task(queue)
    third = dequeue_task(queue)
    fourth = dequeue_task(queue)

    assert [first, second, third, fourth] == [
        {"provider": "companies_house", "user_id": 1, "timestamp": ts},
        {"provider": "id_verification", "user_id": 1, "timestamp": ts},
        {"provider": "bank_statements", "user_id": 1, "timestamp": ts},
        {"provider": "bank_statements", "user_id": 2, "timestamp": ts},
    ]


def test_timestamp_ordering_example_from_challenge() -> None:
    """
    IWC_R1 Example #2:
    For tasks with equal priority, older timestamp is dequeued first.
    """
    queue = new_queue()

    newer = "2025-10-20 12:05:00"
    older = "2025-10-20 12:00:00"

    assert enqueue_task(queue, "bank_statements", 1, newer) == 1
    assert enqueue_task(queue, "bank_statements", 2, older) == 2

    first = dequeue_task(queue)
    second = dequeue_task(queue)

    assert first == {"provider": "bank_statements", "user_id": 2, "timestamp": older}
    assert second == {"provider": "bank_statements", "user_id": 1, "timestamp": newer}


def test_dependency_resolution_example_from_challenge() -> None:
    """
    IWC_R1 Example #3:
    Enqueuing credit_check adds required dependency tasks before it.
    """
    queue = new_queue()
    ts = scenario_ts(delta_seconds=0)

    assert enqueue_task(queue, "credit_check", 1, ts) == 2
    assert queue.size() == 2

    first = dequeue_task(queue)
    second = dequeue_task(queue)

    assert first == {"provider": "companies_house", "user_id": 1, "timestamp": ts}
    assert second == {"provider": "credit_check", "user_id": 1, "timestamp": ts}


def test_size_tracks_pending_items_across_operations() -> None:
    """
    size() must reflect pending queue length after enqueue/dequeue operations.
    """
    queue = new_queue()

    assert queue.size() == 0
    enqueue_task(queue, "bank_statements", 1, scenario_ts(delta_seconds=0))
    enqueue_task(queue, "id_verification", 2, scenario_ts(delta_seconds=1))
    assert queue.size() == 2

    assert dequeue_task(queue) is not None
    assert queue.size() == 1

    assert dequeue_task(queue) is not None
    assert queue.size() == 0
    assert dequeue_task(queue) is None


def test_purge_clears_queue_and_instance_is_reusable() -> None:
    """
    purge() should clear all pending tasks and return True.
    The queue object should continue working after purge.
    """
    queue = new_queue()

    enqueue_task(queue, "bank_statements", 1, scenario_ts(delta_seconds=0))
    enqueue_task(queue, "id_verification", 2, scenario_ts(delta_seconds=1))
    assert queue.size() == 2

    assert queue.purge() is True
    assert queue.size() == 0
    assert dequeue_task(queue) is None

    assert enqueue_task(queue, "companies_house", 9, scenario_ts(delta_seconds=2)) == 1
    assert queue.size() == 1


def test_age_empty_queue_is_zero_seconds() -> None:
    """
    age() is the internal queue age in seconds.
    For this suite, empty queue age is defined as 0.
    """
    queue = new_queue()
    assert queue.age() == 0


def test_age_tracks_oldest_pending_task_in_seconds() -> None:
    """
    age() should be based on the oldest pending task timestamp:
    - integer seconds
    - increases over time while queue is non-empty
    """
    queue = new_queue()

    ninety_seconds_ago = (datetime.now() - timedelta(seconds=90)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    enqueue_task(queue, "bank_statements", 42, ninety_seconds_ago)

    first_age = queue.age()
    assert isinstance(first_age, int)
    assert 80 <= first_age <= 180

    time.sleep(1.1)
    second_age = queue.age()
    assert isinstance(second_age, int)
    assert second_age >= first_age + 1


def test_age_returns_zero_after_queue_becomes_empty() -> None:
    """
    After all pending tasks are drained, age() should return 0 again.
    """
    queue = new_queue()
    enqueue_task(queue, "bank_statements", 1, now_ts(delta_seconds=0))

    assert dequeue_task(queue) is not None
    assert queue.size() == 0
    assert queue.age() == 0


