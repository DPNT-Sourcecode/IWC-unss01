"""IWC legacy queue implementation."""

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
    """In-memory legacy queue used by the IWC challenge runner."""

    def __init__(self):
        self._queue = []

    def _collect_dependencies(self, task: TaskSubmission) -> list[TaskSubmission]:
        """Return transitive dependencies for a task in execution order."""
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
    def _priority_for_task(task: TaskSubmission) -> Priority:
        """Extract a normalized priority enum from task metadata."""
        metadata = task.metadata
        raw_priority = metadata.get("priority", Priority.NORMAL)
        try:
            return Priority(raw_priority)
        except (TypeError, ValueError):
            return Priority.NORMAL

    @staticmethod
    def _earliest_group_timestamp_for_task(task: TaskSubmission) -> datetime:
        """Return group-level earliest timestamp used for user-priority ordering."""
        metadata = task.metadata
        group_timestamp = metadata.get("group_earliest_timestamp", MAX_TIMESTAMP)
        if isinstance(group_timestamp, datetime):
            return group_timestamp.replace(tzinfo=None)
        return group_timestamp

    @staticmethod
    def _timestamp_for_task(task: TaskSubmission) -> datetime:
        """Return a naive ``datetime`` representation for task timestamp sorting."""
        timestamp = task.timestamp
        if isinstance(timestamp, datetime):
            return timestamp.replace(tzinfo=None)
        if isinstance(timestamp, str):
            return datetime.fromisoformat(timestamp).replace(tzinfo=None)
        return timestamp

    @staticmethod
    def _dispatch_timestamp_for_task(task: TaskSubmission) -> str:
        """Return timestamp payload for dequeue contract.

        Preserve original string timestamps as provided by callers.
        Datetime timestamps are formatted as ``YYYY-MM-DD HH:MM:SS``.
        """
        timestamp = task.timestamp
        if isinstance(timestamp, str):
            return timestamp
        if isinstance(timestamp, datetime):
            return timestamp.replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")
        return str(timestamp)

    def enqueue(self, item: TaskSubmission) -> int:
        """Enqueue a task and its dependencies and return queue size."""
        tasks = [*self._collect_dependencies(item), item]

        for task in tasks:
            metadata = task.metadata
            metadata.setdefault("priority", Priority.NORMAL)
            metadata.setdefault("group_earliest_timestamp", MAX_TIMESTAMP)
            self._queue.append(task)
        return self.size

    def dequeue(self) -> TaskDispatch | None:
        """Dequeue the next task according to legacy priority rules."""
        if self.size == 0:
            return None

        user_ids = {task.user_id for task in self._queue}
        task_count: dict[int | str, int] = {}
        priority_timestamps: dict[int | str, datetime] = {}
        for user_id in user_ids:
            user_tasks = [t for t in self._queue if t.user_id == user_id]
            earliest_task = min(user_tasks, key=self._timestamp_for_task)
            priority_timestamps[user_id] = self._timestamp_for_task(earliest_task)
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
            timestamp=self._dispatch_timestamp_for_task(task),
        )

    @property
    def size(self) -> int:
        """Number of pending tasks currently in the queue."""
        return len(self._queue)

    @property
    def age(self) -> int:
        """Internal queue age in seconds based on oldest pending task."""
        if self.size == 0:
            return 0

        oldest_task = min(self._queue, key=self._timestamp_for_task)
        oldest_timestamp = self._timestamp_for_task(oldest_task)
        now = datetime.now().replace(tzinfo=None)
        age_seconds = int((now - oldest_timestamp).total_seconds())
        return max(0, age_seconds)

    def purge(self) -> bool:
        """Clear all pending tasks from the queue."""
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



