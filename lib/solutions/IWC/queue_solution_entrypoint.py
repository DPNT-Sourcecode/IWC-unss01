"""Public IWC queue entrypoint exposed to the challenge runner.

This module provides a small facade around the legacy queue implementation so
the runner can interact with a stable API surface.
"""

from __future__ import annotations

from solutions.IWC.queue_solution_legacy import Queue
from solutions.IWC.task_types import TaskDispatch, TaskSubmission

class QueueSolutionEntrypoint:
    """Runner-facing facade around ``Queue``."""

    def __init__(self) -> None:
        """Initialize a fresh in-memory queue instance.

        Returns:
            None.
        """
        self._queue: Queue = Queue()

    def enqueue(self, task: TaskSubmission) -> int:
        """Enqueue a task into the queue.

        Args:
            task: Submission payload containing provider, user id, and timestamp.

        Returns:
            The total number of queued tasks after enqueue, including any
            dependency tasks added by the legacy implementation.
        """
        return self._queue.enqueue(task)

    def dequeue(self) -> TaskDispatch | None:
        """Return the next task according to queue ordering rules.

        Args:
            None.

        Returns:
            The next dispatch payload, or ``None`` when the queue is empty.
        """
        return self._queue.dequeue()

    def size(self) -> int:
        """Return the current number of pending tasks.

        Args:
            None.

        Returns:
            Pending queue size.
        """
        return self._queue.size

    def age(self) -> int:
        """Return internal queue age in seconds.

        Args:
            None.

        Returns:
            Queue age in seconds, defined by the underlying queue
            implementation.
        """
        return self._queue.age

    def purge(self) -> bool:
        """Clear all queued tasks.

        Args:
            None.

        Returns:
            ``True`` when the queue has been cleared successfully.
        """
        return self._queue.purge()



