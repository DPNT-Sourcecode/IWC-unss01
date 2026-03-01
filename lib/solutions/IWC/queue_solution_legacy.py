"""Core in-memory queue implementation for IWC task dispatch.

The queue applies six contract rules:
- User promotion when a user has 3 or more pending tasks.
- Oldest-first ordering for tasks with equal priority.
- Provider dependency insertion during enqueue.
- Identity uniqueness per ``(user_id, provider)`` pair.
- ``bank_statements`` tasks are deprioritized behind other providers.
- Queue age as seconds since the oldest pending task.
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
    """Provider configuration used to derive task dependency chains.

    Attributes:
        name: Provider identifier used in task payloads.
        base_url: Placeholder upstream endpoint for the provider.
        depends_on: Provider names that must run before this provider.
    """

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

BANK_STATEMENTS_PROVIDER_NAME = BANK_STATEMENTS_PROVIDER.name


class Queue:
    """In-memory queue with deterministic ordering and dependency handling."""

    def __init__(self):
        """Create an empty queue instance."""
        self._queue: list[TaskSubmission] = []

    @staticmethod
    def _identity_key(task: TaskSubmission) -> tuple[int | str, str]:
        """Return the uniqueness identity for a task.

        Args:
            task: Task to identify in queue.

        Returns:
            Tuple ``(user_id, provider)`` used for identity uniqueness checks.
        """
        return task.user_id, task.provider

    def _find_index_by_identity(self, task: TaskSubmission) -> int | None:
        """Find index of an existing task with the same identity.

        Args:
            task: Task candidate for insertion.

        Returns:
            Index in internal queue when duplicate exists, otherwise ``None``.
        """
        key = self._identity_key(task)
        for index, queued_task in enumerate(self._queue):
            if self._identity_key(queued_task) == key:
                return index
        return None

    def _collect_dependencies(self, task: TaskSubmission) -> list[TaskSubmission]:
        """Return transitive dependency tasks for a submission.

        Args:
            task: The task being enqueued.

        Returns:
            A list of dependency ``TaskSubmission`` objects in execution order.
            Dependencies appear before the original task.
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
    def _bank_deprioritization_rank(task: TaskSubmission) -> int:
        """Return ordering rank used to delay ``bank_statements`` tasks.

        Args:
            task: Task being considered for dequeue ordering.

        Returns:
            ``1`` for ``bank_statements`` tasks, otherwise ``0``.
        """
        if task.provider == BANK_STATEMENTS_PROVIDER_NAME:
            return 1
        return 0

    @staticmethod
    def _priority_for_task(task: TaskSubmission) -> Priority:
        """Extract normalized priority from task metadata.

        Args:
            task: Task whose metadata may contain a priority marker.

        Returns:
            ``Priority.HIGH`` or ``Priority.NORMAL``. Invalid or missing values
            default to ``Priority.NORMAL``.
        """
        metadata = task.metadata
        raw_priority = metadata.get("priority", Priority.NORMAL)
        try:
            return Priority(raw_priority)
        except (TypeError, ValueError):
            return Priority.NORMAL

    @staticmethod
    def _earliest_group_timestamp_for_task(task: TaskSubmission) -> datetime:
        """Return group-level earliest timestamp used in sort ordering.

        Args:
            task: Task that may contain ``group_earliest_timestamp`` metadata.

        Returns:
            Naive ``datetime`` used for ordering promoted user groups. Falls back
            to ``MAX_TIMESTAMP`` when not set.
        """
        metadata = task.metadata
        group_timestamp = metadata.get("group_earliest_timestamp", MAX_TIMESTAMP)
        if isinstance(group_timestamp, datetime):
            return group_timestamp.replace(tzinfo=None)
        return group_timestamp

    @staticmethod
    def _timestamp_for_task(task: TaskSubmission) -> datetime:
        """Normalize task timestamp into naive ``datetime`` for comparisons.

        Args:
            task: Task whose timestamp may be ``datetime`` or ``str``.

        Returns:
            Timestamp as naive ``datetime`` suitable for consistent ordering.
        """
        timestamp = task.timestamp
        if isinstance(timestamp, datetime):
            return timestamp.replace(tzinfo=None)
        if isinstance(timestamp, str):
            return datetime.fromisoformat(timestamp).replace(tzinfo=None)
        raise TypeError(f"Unsupported timestamp type: {type(timestamp)!r}")

    def enqueue(self, item: TaskSubmission) -> int:
        """Enqueue a task and all required dependencies.

        Args:
            item: Submission payload to insert into the queue.

        Returns:
            Queue size after all dependency and primary tasks are inserted.
        """
        tasks = [*self._collect_dependencies(item), item]

        for task in tasks:
            existing_index = self._find_index_by_identity(task)
            if existing_index is None:
                metadata = task.metadata
                metadata.setdefault("priority", Priority.NORMAL)
                metadata.setdefault("group_earliest_timestamp", MAX_TIMESTAMP)
                self._queue.append(task)
                continue

            # Keep a single identity entry, preferring the older timestamp.
            existing_task = self._queue[existing_index]
            incoming_ts = self._timestamp_for_task(task)
            existing_ts = self._timestamp_for_task(existing_task)
            if incoming_ts < existing_ts:
                existing_task.timestamp = task.timestamp

        return self.size

    def dequeue(self) -> TaskDispatch | None:
        """Dequeue the next task according to queue ordering rules.

        Ordering behavior:
            1. Promote users with 3 or more queued tasks.
            2. Sort promoted groups by each user's earliest queued timestamp.
            3. Deprioritize ``bank_statements`` behind other providers.
            4. Break ties by task timestamp (oldest first).

        Returns:
            The next ``TaskDispatch`` payload, or ``None`` when queue is empty.
        """
        if self.size == 0:
            return None

        task_count: dict[int | str, int] = {}
        priority_timestamps: dict[int | str, datetime] = {}
        # Single-pass aggregation keeps behavior unchanged while avoiding
        # repeated full-queue scans for each user.
        for task in self._queue:
            user_id = task.user_id
            task_count[user_id] = task_count.get(user_id, 0) + 1

            timestamp = self._timestamp_for_task(task)
            earliest_for_user = priority_timestamps.get(user_id)
            if earliest_for_user is None or timestamp < earliest_for_user:
                priority_timestamps[user_id] = timestamp

        for task in self._queue:
            metadata = task.metadata
            if task_count[task.user_id] >= 3:
                metadata["group_earliest_timestamp"] = priority_timestamps[task.user_id]
                metadata["priority"] = Priority.HIGH
            else:
                metadata["group_earliest_timestamp"] = MAX_TIMESTAMP
                metadata["priority"] = Priority.NORMAL

        self._queue.sort(
            key=lambda i: (
                self._priority_for_task(i),
                self._earliest_group_timestamp_for_task(i),
                self._bank_deprioritization_rank(i),
                self._timestamp_for_task(i),
            )
        )

        task = self._queue.pop(0)
        return TaskDispatch(
            provider=task.provider,
            user_id=task.user_id,
        )

    @property
    def size(self) -> int:
        """Number of pending tasks currently in the queue.

        Returns:
            Current queue length.
        """
        return len(self._queue)

    @property
    def age(self) -> int:
        """Internal queue age in seconds.

        Returns:
            ``0`` when empty; otherwise seconds since the oldest pending task
            timestamp. Negative values are clamped to ``0``.
        """
        if self.size == 0:
            return 0

        oldest_task = min(self._queue, key=self._timestamp_for_task)
        oldest_timestamp = self._timestamp_for_task(oldest_task)
        now = datetime.now().replace(tzinfo=None)
        age_seconds = int((now - oldest_timestamp).total_seconds())
        return max(0, age_seconds)

    def purge(self) -> bool:
        """Clear all pending tasks from the queue.

        Returns:
            ``True`` when queue clear operation completes.
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
