"""IWC legacy queue implementation.

This module intentionally preserves legacy queue behavior while we evolve it
towards the Round 1 (``IWC_R1``) contract.

R1 behavior that must be satisfied end-to-end:
- Rule of 3: if a user has 3 or more queued tasks, all of their tasks are
  processed before normal-priority users.
- Timestamp ordering: within equal priority tiers, older timestamps are
  processed first.
- Dependency resolution: enqueuing a task also enqueues its dependencies before
  the task itself.
- Method contracts:
  - ``enqueue`` returns current queue size.
  - ``dequeue`` returns ``provider``, ``user_id``, and ``timestamp`` or ``None``.
  - ``size`` returns pending task count.
  - ``age`` returns queue internal age in seconds.
  - ``purge`` clears the queue and returns ``True``.

Current known implementation gaps versus R1:
- ``dequeue`` currently drops ``timestamp`` from dispatch payload.
- ``age`` currently returns ``0`` unconditionally.
"""

from dataclasses import dataclass
from datetime import datetime
from enum import IntEnum

# LEGACY CODE ASSET
# RESOLVED on deploy
from solutions.IWC.task_types import TaskSubmission, TaskDispatch

class Priority(IntEnum):
    """Represents the queue ordering tiers observed in the legacy system."""
    HIGH = 1
    NORMAL = 2

@dataclass
class Provider:
    """Provider configuration used to derive task dependency chains."""

    name: str
    base_url: str
    depends_on: list[str]

MAX_TIMESTAMP = datetime.max.replace(tzinfo=None)

COMPANIES_HOUSE_PROVIDER = Provider(
    name="companies_house", base_url="https://fake.companieshouse.co.uk", depends_on=[]
)


CREDIT_CHECK_PROVIDER = Provider(
    name="credit_check",
    base_url="https://fake.creditcheck.co.uk",
    depends_on=["companies_house"],
)


BANK_STATEMENTS_PROVIDER = Provider(
    name="bank_statements", base_url="https://fake.bankstatements.co.uk", depends_on=[]
)

ID_VERIFICATION_PROVIDER = Provider(
    name="id_verification", base_url="https://fake.idv.co.uk", depends_on=[]
)


REGISTERED_PROVIDERS: list[Provider] = [
    BANK_STATEMENTS_PROVIDER,
    COMPANIES_HOUSE_PROVIDER,
    CREDIT_CHECK_PROVIDER,
    ID_VERIFICATION_PROVIDER,
]

class Queue:
    """In-memory legacy queue used by the IWC challenge runner.

    Notes:
    - This class is intentionally small and synchronous to match challenge
      runner expectations.
    - Ordering is computed at dequeue time by mutating task metadata and
      sorting the internal list.

    Implementation checklist for R1 compliance:
    1. Include ``timestamp`` in ``TaskDispatch`` returned by ``dequeue``.
    2. Compute ``age`` as internal queue age in seconds (based on oldest
       pending task; ``0`` when empty).
    3. Keep Rule-of-3, timestamp ordering, and dependency behavior unchanged.
    """

    def __init__(self):
        self._queue = []

    def _collect_dependencies(self, task: TaskSubmission) -> list[TaskSubmission]:
        """Return transitive dependencies for a task in execution order.

        Args:
            task: Task being enqueued.

        Returns:
            A flat list of dependency tasks that must execute before ``task``.
            Dependencies are returned depth-first so deeper prerequisites appear
            earlier in the list.
        """
        provider = next((p for p in REGISTERED_PROVIDERS if p.name == task.provider), None)
        if provider is None:
            return []

        tasks: list[TaskSubmission] = []
        for dependency in provider.depends_on:
            dependency_task = TaskSubmission(
                provider=dependency,
                user_id=task.user_id,
                timestamp=task.timestamp,
            )
            tasks.extend(self._collect_dependencies(dependency_task))
            tasks.append(dependency_task)
        return tasks

    @staticmethod
    def _priority_for_task(task):
        """Extract a normalized priority enum from task metadata."""
        metadata = task.metadata
        raw_priority = metadata.get("priority", Priority.NORMAL)
        try:
            return Priority(raw_priority)
        except (TypeError, ValueError):
            return Priority.NORMAL

    @staticmethod
    def _earliest_group_timestamp_for_task(task):
        """Return group-level earliest timestamp used for user-priority ordering."""
        metadata = task.metadata
        return metadata.get("group_earliest_timestamp", MAX_TIMESTAMP)

    @staticmethod
    def _timestamp_for_task(task):
        """Return a naive ``datetime`` representation for task timestamp sorting."""
        timestamp = task.timestamp
        if isinstance(timestamp, datetime):
            return timestamp.replace(tzinfo=None)
        if isinstance(timestamp, str):
            return datetime.fromisoformat(timestamp).replace(tzinfo=None)
        return timestamp

    def enqueue(self, item: TaskSubmission) -> int:
        """Enqueue a task and its dependencies.

        Args:
            item: Task request payload.

        Returns:
            Current queue size after enqueue completes.
        """
        tasks = [*self._collect_dependencies(item), item]

        for task in tasks:
            metadata = task.metadata
            metadata.setdefault("priority", Priority.NORMAL)
            metadata.setdefault("group_earliest_timestamp", MAX_TIMESTAMP)
            self._queue.append(task)
        return self.size

    def dequeue(self):
        """Dequeue the next task according to legacy priority rules.

        Ordering strategy:
        1. Promote users with at least three queued tasks (Rule of 3).
        2. For promoted users, order by each user's earliest queued timestamp.
        3. Break ties with task timestamp ordering (oldest first).

        Returns:
            ``TaskDispatch`` for the next task, or ``None`` when queue is empty.

        R1 gap:
            Returned dispatch payload currently includes ``provider`` and
            ``user_id`` only. ``timestamp`` must be added for full R1
            compliance.
        """
        if self.size == 0:
            return None

        user_ids = {task.user_id for task in self._queue}
        task_count = {}
        priority_timestamps = {}
        for user_id in user_ids:
            user_tasks = [t for t in self._queue if t.user_id == user_id]
            earliest_timestamp = sorted(user_tasks, key=lambda t: t.timestamp)[0].timestamp
            priority_timestamps[user_id] = earliest_timestamp
            task_count[user_id] = len(user_tasks)

        for task in self._queue:
            metadata = task.metadata
            current_earliest = metadata.get("group_earliest_timestamp", MAX_TIMESTAMP)
            raw_priority = metadata.get("priority")
            try:
                priority_level = Priority(raw_priority)
            except (TypeError, ValueError):
                priority_level = None

            if priority_level is None or priority_level == Priority.NORMAL:
                metadata["group_earliest_timestamp"] = MAX_TIMESTAMP
                if task_count[task.user_id] >= 3:
                    metadata["group_earliest_timestamp"] = priority_timestamps[task.user_id]
                    metadata["priority"] = Priority.HIGH
                else:
                    metadata["priority"] = Priority.NORMAL
            else:
                metadata["group_earliest_timestamp"] = current_earliest
                metadata["priority"] = priority_level

        self._queue.sort(
            key=lambda i: (
                self._priority_for_task(i),
                self._earliest_group_timestamp_for_task(i),
                self._timestamp_for_task(i),
            )
        )

        task = self._queue.pop(0)
        return TaskDispatch(
            provider=task.provider,
            user_id=task.user_id,
        )

    @property
    def size(self):
        """Number of pending tasks currently in the queue."""
        return len(self._queue)

    @property
    def age(self):
        """Internal queue age in seconds.

        R1 target behavior:
            - Return ``0`` when queue is empty.
            - Otherwise return age of the oldest pending task in seconds.

        Current behavior:
            Always returns ``0`` (placeholder).
        """
        return 0

    def purge(self):
        """Clear all pending tasks from the queue.

        Returns:
            ``True`` when purge completes.
        """
        self._queue.clear()
        return True

"""
===================================================================================================

The following code is only to visualise the final usecase.
No changes are needed past this point.

To test the correct behaviour of the queue system, import the `Queue` class directly in your tests.

===================================================================================================

```python
import asyncio
import logging
from fastapi import FastAPI
from contextlib import asynccontextmanager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(queue_worker())
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        logger.info("Queue worker cancelled on shutdown.")


app = FastAPI(lifespan=lifespan)
queue = Queue()


@app.get("/")
def read_root():
    return {
        "registered_providers": [
            {"name": p.name, "base_url": p.base_url} for p in registered_providers
        ]
    }


class DataRequest(BaseModel):
    user_id: int
    providers: list[str]


@app.post("/fetch_customer_data")
def fetch_customer_data(data: DataRequest):
    provider_names = [p.name for p in registered_providers]

    for provider in data.providers:
        if provider not in provider_names:
            logger.warning(f"Provider {provider} doesn't exists. Skipping")
            continue

        queue.enqueue(
            TaskSubmission(
                provider=provider,
                user_id=data.user_id,
                timestamp=datetime.now(),
            )
        )

    return {"status": f"{len(data.providers)} Task(s) added to queue"}


async def queue_worker():
    while True:
        if queue.size == 0:
            await asyncio.sleep(1)
            continue

        task = queue.dequeue()
        if not task:
            continue

        logger.info(f"Processing task: {task}")
        await asyncio.sleep(2)
        logger.info(f"Finished task: {task}")
```
"""

