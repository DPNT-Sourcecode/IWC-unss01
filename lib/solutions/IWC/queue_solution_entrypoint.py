"""Public IWC queue entrypoint exposed to the challenge runner."""

from __future__ import annotations

from solutions.IWC.queue_solution_legacy import Queue
from solutions.IWC.task_types import TaskDispatch, TaskSubmission

class QueueSolutionEntrypoint:
    """Runner-facing facade around ``Queue``."""

    def __init__(self) -> None:
        """Initialize a fresh in-memory queue instance."""
        self._queue: Queue = Queue()

    def enqueue(self, task: TaskSubmission) -> int:
        """Enqueue a task payload and return current queue size."""
        return self._queue.enqueue(task)

    def dequeue(self) -> TaskDispatch | None:
        """Dequeue the next task payload or ``None`` when queue is empty."""
        return self._queue.dequeue()

    def size(self) -> int:
        """Return number of currently queued tasks."""
        return self._queue.size

    def age(self) -> int:
        """Return internal queue age in seconds."""
        return self._queue.age

    def purge(self) -> bool:
        """Clear all queued tasks and return success status."""
        return self._queue.purge()


