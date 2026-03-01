"""Contract tests for the IWC queue implementation."""

from __future__ import annotations

import time
from datetime import datetime, timedelta

from .utils import (
    call_age,
    call_dequeue,
    call_dequeue_none,
    call_enqueue,
    call_purge,
    call_size,
    enqueue_task,
    new_queue,
    run_queue,
    scenario_ts,
)


def test_enqueue_size_dequeue_flow() -> None:
    """
    Keep the original test shape:
    action list + fluent expectations in one flow.
    """
    run_queue(
        [
            call_enqueue("companies_house", 1, scenario_ts(delta_seconds=0)).expect(1),
            call_size().expect(1),
            call_dequeue().expect("companies_house", 1),
            call_size().expect(0),
        ]
    )


def test_empty_queue_contract() -> None:
    """
    Baseline contract for a brand-new queue instance.
    """
    run_queue(
        [
            call_size().expect(0),
            call_dequeue_none().expect(),
            call_purge().expect(True),
            call_age().expect(0),
        ]
    )


def test_enqueue_returns_current_queue_size() -> None:
    """
    enqueue(...) must return queue size after insertion.
    """
    run_queue(
        [
            call_enqueue("companies_house", 1, scenario_ts(delta_seconds=0)).expect(1),
            call_enqueue("bank_statements", 2, scenario_ts(delta_seconds=1)).expect(2),
            call_size().expect(2),
        ]
    )


def test_dequeue_returns_required_fields() -> None:
    """
    Dequeue payload should expose provider and user_id.
    """
    ts = scenario_ts(delta_seconds=0)
    run_queue(
        [
            call_enqueue("bank_statements", 7, ts).expect(1),
            call_dequeue().expect("bank_statements", 7),
            call_size().expect(0),
        ]
    )


def test_rule_of_three_prioritizes_user_with_three_or_more_pending_tasks() -> None:
    """
    Rule-of-three reference example:
    once a user has >= 3 queued tasks, all of that user's tasks are prioritized first.
    """
    ts = scenario_ts(delta_seconds=0)
    run_queue(
        [
            call_enqueue("companies_house", 1, ts).expect(1),
            call_enqueue("bank_statements", 2, ts).expect(2),
            call_enqueue("id_verification", 1, ts).expect(3),
            call_enqueue("bank_statements", 1, ts).expect(4),
            call_dequeue().expect("companies_house", 1),
            call_dequeue().expect("id_verification", 1),
            call_dequeue().expect("bank_statements", 1),
            call_dequeue().expect("bank_statements", 2),
            call_size().expect(0),
        ]
    )


def test_equal_priority_tasks_dequeue_oldest_first() -> None:
    """
    Timestamp ordering reference example:
    for equal priority items, the older timestamp must dequeue first.
    """
    older = "2025-10-20 12:00:00"
    newer = "2025-10-20 12:05:00"
    run_queue(
        [
            call_enqueue("bank_statements", 1, newer).expect(1),
            call_enqueue("bank_statements", 2, older).expect(2),
            call_dequeue().expect("bank_statements", 2),
            call_dequeue().expect("bank_statements", 1),
            call_size().expect(0),
        ]
    )


def test_credit_check_enqueue_adds_companies_house_dependency_first() -> None:
    """
    Dependency resolution reference example:
    enqueuing credit_check also enqueues companies_house before it.
    """
    ts = scenario_ts(delta_seconds=0)
    run_queue(
        [
            call_enqueue("credit_check", 1, ts).expect(2),
            call_size().expect(2),
            call_dequeue().expect("companies_house", 1),
            call_dequeue().expect("credit_check", 1),
            call_size().expect(0),
        ]
    )


def test_size_tracks_pending_items_across_operations() -> None:
    """
    size() must stay consistent across enqueue, dequeue and purge operations.
    """
    run_queue(
        [
            call_size().expect(0),
            call_enqueue("bank_statements", 1, scenario_ts(delta_seconds=0)).expect(1),
            call_enqueue("id_verification", 2, scenario_ts(delta_seconds=1)).expect(2),
            call_size().expect(2),
            call_dequeue().expect("id_verification", 2),
            call_size().expect(1),
            call_purge().expect(True),
            call_size().expect(0),
            call_dequeue_none().expect(),
        ]
    )


def test_purge_clears_queue_and_instance_is_reusable() -> None:
    """
    purge() must clear all pending items and the queue should still be reusable.
    """
    run_queue(
        [
            call_enqueue("bank_statements", 1, scenario_ts(delta_seconds=0)).expect(1),
            call_enqueue("id_verification", 2, scenario_ts(delta_seconds=1)).expect(2),
            call_purge().expect(True),
            call_size().expect(0),
            call_dequeue_none().expect(),
            call_enqueue("companies_house", 9, scenario_ts(delta_seconds=2)).expect(1),
            call_size().expect(1),
            call_dequeue().expect("companies_house", 9),
            call_size().expect(0),
        ]
    )


def test_age_empty_queue_is_zero_seconds() -> None:
    """
    Empty queue age should be 0 seconds.
    """
    run_queue(
        [
            call_age().expect(0),
            call_purge().expect(True),
            call_age().expect(0),
        ]
    )


def test_age_tracks_oldest_pending_task_in_seconds() -> None:
    """
    age() should represent internal queue age in seconds.
    This test validates non-empty queue aging against an old timestamp.
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
    Once all pending tasks are drained, age() should reset to 0.
    """
    ts = scenario_ts(delta_seconds=0)
    run_queue(
        [
            call_enqueue("bank_statements", 1, ts).expect(1),
            call_dequeue().expect("bank_statements", 1),
            call_size().expect(0),
            call_age().expect(0),
        ]
    )


def test_user_id_integer_is_supported_end_to_end() -> None:
    """
    user_id integer payloads should flow through enqueue/dequeue unchanged.
    """
    ts = scenario_ts(delta_seconds=3)
    run_queue(
        [
            call_enqueue("id_verification", 123, ts).expect(1),
            call_dequeue().expect("id_verification", 123),
            call_size().expect(0),
        ]
    )


def test_user_id_string_is_supported_end_to_end() -> None:
    """
    user_id string payloads should flow through enqueue/dequeue unchanged.
    """
    ts = scenario_ts(delta_seconds=4)
    run_queue(
        [
            call_enqueue("companies_house", "customer-42", ts).expect(1),
            call_dequeue().expect("companies_house", "customer-42"),
            call_size().expect(0),
        ]
    )


def test_queue_keeps_single_identity_when_newer_duplicate_arrives() -> None:
    """
    Duplicate (user_id, provider) tasks should collapse into one item.
    Newer duplicates must not replace older timestamps.
    """
    run_queue(
        [
            call_enqueue("bank_statements", 1, "2025-10-20 12:00:00").expect(1),
            call_enqueue("bank_statements", 1, "2025-10-20 12:05:00").expect(1),
            call_enqueue("id_verification", 1, "2025-10-20 12:05:00").expect(2),
            call_dequeue().expect("id_verification", 1),
            call_dequeue().expect("bank_statements", 1),
            call_size().expect(0),
        ]
    )


def test_queue_keeps_oldest_timestamp_for_same_identity() -> None:
    """
    When duplicate timestamps differ, queue should keep the older one.
    """
    run_queue(
        [
            call_enqueue("bank_statements", 1, "2025-10-20 12:05:00").expect(1),
            call_enqueue("bank_statements", 1, "2025-10-20 12:00:00").expect(1),
            call_enqueue("id_verification", 1, "2025-10-20 12:01:00").expect(2),
            call_dequeue().expect("id_verification", 1),
            call_dequeue().expect("bank_statements", 1),
            call_size().expect(0),
        ]
    )


def test_task_identity_is_scoped_to_user_and_provider() -> None:
    """
    Same provider for different users are distinct tasks and must not deduplicate.
    """
    run_queue(
        [
            call_enqueue("bank_statements", 1, "2025-10-20 12:00:00").expect(1),
            call_enqueue("bank_statements", 2, "2025-10-20 12:01:00").expect(2),
            call_size().expect(2),
            call_dequeue().expect("bank_statements", 1),
            call_dequeue().expect("bank_statements", 2),
            call_size().expect(0),
        ]
    )


def test_identity_uniqueness_applies_to_dependency_tasks() -> None:
    """
    Deduplication applies even when a duplicate is introduced via dependencies.
    """
    run_queue(
        [
            call_enqueue("companies_house", 1, "2025-10-20 12:00:00").expect(1),
            call_enqueue("credit_check", 1, "2025-10-20 12:05:00").expect(2),
            call_dequeue().expect("companies_house", 1),
            call_dequeue().expect("credit_check", 1),
            call_size().expect(0),
        ]
    )


def test_duplicate_credit_check_keeps_single_dependency_chain() -> None:
    """
    Enqueuing duplicate credit_check should not duplicate either
    credit_check itself or its companies_house dependency.
    """
    run_queue(
        [
            call_enqueue("credit_check", 1, "2025-10-20 12:00:00").expect(2),
            call_enqueue("credit_check", 1, "2025-10-20 12:05:00").expect(2),
            call_size().expect(2),
            call_dequeue().expect("companies_house", 1),
            call_dequeue().expect("credit_check", 1),
            call_size().expect(0),
        ]
    )


def test_rule_of_three_uses_unique_pending_tasks() -> None:
    """
    Rule-of-3 should operate on unique queued tasks after deduplication.
    """
    run_queue(
        [
            call_enqueue("bank_statements", 1, "2025-10-20 12:00:00").expect(1),
            call_enqueue("bank_statements", 1, "2025-10-20 12:01:00").expect(1),
            call_enqueue("id_verification", 1, "2025-10-20 12:02:00").expect(2),
            call_enqueue("companies_house", 1, "2025-10-20 12:03:00").expect(3),
            call_enqueue("bank_statements", 2, "2025-10-20 11:00:00").expect(4),
            call_dequeue().expect("id_verification", 1),
            call_dequeue().expect("companies_house", 1),
            call_dequeue().expect("bank_statements", 2),
            call_dequeue().expect("bank_statements", 1),
            call_size().expect(0),
        ]
    )


def test_bank_statements_deprioritized_without_rule_of_three_example_case() -> None:
    """
    R3 reference example:
    bank_statements should run after faster providers when user is not Rule-of-3 prioritized.
    """
    run_queue(
        [
            call_enqueue("bank_statements", 1, "2025-10-20 12:00:00").expect(1),
            call_enqueue("id_verification", 1, "2025-10-20 12:01:00").expect(2),
            call_enqueue("companies_house", 2, "2025-10-20 12:02:00").expect(3),
            call_dequeue().expect("id_verification", 1),
            call_dequeue().expect("companies_house", 2),
            call_dequeue().expect("bank_statements", 1),
            call_size().expect(0),
        ]
    )


def test_rule_of_three_user_bank_statements_runs_after_users_other_tasks() -> None:
    """
    R3 rule interaction:
    when a user is Rule-of-3 prioritized, their bank_statements task runs after
    all their other tasks.
    """
    run_queue(
        [
            call_enqueue("bank_statements", 1, "2025-10-20 12:00:00").expect(1),
            call_enqueue("companies_house", 1, "2025-10-20 12:01:00").expect(2),
            call_enqueue("id_verification", 1, "2025-10-20 12:02:00").expect(3),
            call_enqueue("companies_house", 2, "2025-10-20 12:10:00").expect(4),
            call_dequeue().expect("companies_house", 1),
            call_dequeue().expect("id_verification", 1),
            call_dequeue().expect("companies_house", 2),
            call_dequeue().expect("bank_statements", 1),
            call_size().expect(0),
        ]
    )


def test_dependency_and_bank_deprioritization_apply_together() -> None:
    """
    Dependency resolution and bank deprioritization should compose safely.
    """
    run_queue(
        [
            call_enqueue("bank_statements", 1, "2025-10-20 12:00:00").expect(1),
            call_enqueue("credit_check", 1, "2025-10-20 12:01:00").expect(3),
            call_dequeue().expect("companies_house", 1),
            call_dequeue().expect("credit_check", 1),
            call_dequeue().expect("bank_statements", 1),
            call_size().expect(0),
        ]
    )
