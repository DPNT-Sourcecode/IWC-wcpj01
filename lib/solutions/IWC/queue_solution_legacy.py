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
    def __init__(self):
        self._queue = []

    def _collect_dependencies(self, task: TaskSubmission) -> list[TaskSubmission]:
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
        metadata = task.metadata
        raw_priority = metadata.get("priority", Priority.NORMAL)
        try:
            return Priority(raw_priority)
        except (TypeError, ValueError):
            return Priority.NORMAL

    @staticmethod
    def _earliest_group_timestamp_for_task(task):
        metadata = task.metadata
        return metadata.get("group_earliest_timestamp", MAX_TIMESTAMP)

    @staticmethod
    def _timestamp_for_task(task):
        timestamp = task.timestamp
        if isinstance(timestamp, datetime):
            return timestamp.replace(tzinfo=None)
        if isinstance(timestamp, str):
            return datetime.fromisoformat(timestamp).replace(tzinfo=None)
        return timestamp

    @staticmethod
    def _is_bank_statements(task):
        return task.provider == "bank_statements"

    @staticmethod
    def _rule_of_3_applies(user_id, task_count):
        return task_count.get(user_id, 0) >= 3

    def enqueue(self, item: TaskSubmission) -> int:
        tasks = [*self._collect_dependencies(item), item]

        for task in tasks:
            metadata = task.metadata
            metadata.setdefault("priority", Priority.NORMAL)
            metadata.setdefault("group_earliest_timestamp", MAX_TIMESTAMP)
            # Find existing task with same (user_id, provider)
            existing = next(
                (t for t in self._queue if t.user_id == task.user_id and t.provider == task.provider),
                None
            )
            if existing:
                # Compare timestamps
                if self._timestamp_for_task(task) < self._timestamp_for_task(existing):
                    self._queue.remove(existing)
                    self._queue.append(task)
                # else: do nothing, keep the earlier one already in queue
            else:
                self._queue.append(task)

        return self.size

    def dequeue(self):
        if self.size == 0:
            return None

        user_ids = {task.user_id for task in self._queue}
        task_count = {}
        priority_timestamps = {}
        for user_id in user_ids:
            user_tasks = [t for t in self._queue if t.user_id == user_id]
            earliest_timestamp = sorted(user_tasks, key=lambda t: self._timestamp_for_task(t))[0].timestamp
            # Convert to datetime for consistent comparison
            priority_timestamps[user_id] = self._timestamp_for_task(
                type('obj', (), {'timestamp': earliest_timestamp})()
            )
            task_count[user_id] = len(user_tasks)

        _, queue_newest = self.oldest_and_newest_timestamps()

        for task in self._queue:
            metadata = task.metadata
            current_earliest = metadata.get("group_earliest_timestamp", MAX_TIMESTAMP)
            raw_priority = metadata.get("priority")
            try:
                priority_level = Priority(raw_priority)
            except (TypeError, ValueError):
                priority_level = None

            # Check if this is an old bank_statements task
            task_timestamp = self._timestamp_for_task(task)
            task_age = (queue_newest - task_timestamp).total_seconds()
            is_old_bank = self._is_bank_statements(task) and task_age >= 300

            if priority_level is None or priority_level == Priority.NORMAL:
                metadata["group_earliest_timestamp"] = MAX_TIMESTAMP
                if task_count[task.user_id] >= 3:
                    # Apply Rule of 3 (even to old banks)
                    metadata["group_earliest_timestamp"] = priority_timestamps[task.user_id]
                    metadata["priority"] = Priority.HIGH
                else:
                    metadata["priority"] = Priority.NORMAL
            else:
                metadata["group_earliest_timestamp"] = current_earliest
                metadata["priority"] = priority_level

        def sort_key(t: TaskSubmission):
            """
            Sort key for task prioritization with time-sensitive bank statements.
            
            Priority levels:
            1. HIGH priority (Rule of 3): Comes first
            2. OLD banks (age >= 5min) with NORMAL priority: Between HIGH and regular NORMAL
            3. NORMAL priority: Regular tasks
            4. Fresh banks with NORMAL priority: Deprioritized to end
            
            Within HIGH priority (Rule of 3):
            - Fresh banks are deprioritized within the group (come after non-banks)
            - Old banks are NOT deprioritized (sorted by timestamp like non-banks)
            """
            is_bank = self._is_bank_statements(t)
            task_timestamp = self._timestamp_for_task(t)
            task_age = (queue_newest - task_timestamp).total_seconds()
            is_old_bank = is_bank and task_age >= 300
            priority = self._priority_for_task(t)
            group_timestamp = self._earliest_group_timestamp_for_task(t)
            
            # Determine if this bank should be deprioritized
            is_fresh_bank = is_bank and not is_old_bank
            
            if priority == Priority.HIGH:
                # HIGH priority (Rule of 3)
                # Old banks: sorted by timestamp with non-banks
                # Fresh banks: deprioritized within the group
                if is_fresh_bank:
                    # Fresh bank in Rule of 3: comes after all non-banks in the group
                    return (
                        0,  # Not globally deprioritized
                        priority.value,  # HIGH = 1
                        group_timestamp,  # Group's earliest timestamp
                        1,  # Banks after non-banks
                        task_timestamp,  # Individual task timestamp
                    )
                else:
                    # Non-bank or old bank in Rule of 3: sorted normally by timestamp
                    return (
                        0,  # Not globally deprioritized
                        priority.value,  # HIGH = 1
                        group_timestamp,  # Group's earliest timestamp
                        0,  # Non-banks / old banks first
                        task_timestamp,  # Individual task timestamp
                    )
            elif is_fresh_bank:
                # Fresh NORMAL banks: deprioritized to end of global queue
                return (
                    1,  # Globally deprioritized
                    task_timestamp,  # Sort by timestamp within deprioritized group
                )
            elif is_old_bank:
                # Old NORMAL banks: not deprioritized, can skip HIGH priority tasks
                # Priority 0.5 (before HIGH=1)
                return (
                    0,  # Not deprioritized
                    0.5,  # Before HIGH (1)
                    task_timestamp,  # Sort by timestamp
                    0,  # Old banks before non-banks at same timestamp
                )
            else:
                # NORMAL non-bank
                return (
                    0,  # Not deprioritized
                    priority.value,  # NORMAL = 2 (after HIGH = 1 and old banks = 1.5)
                    task_timestamp,  # Sort by timestamp
                    1,  # Non-banks after old banks at same timestamp
                )

        self._queue.sort(key=sort_key)

        task = self._queue.pop(0)
        return TaskDispatch(
            provider=task.provider,
            user_id=task.user_id,
        )

    @property
    def size(self):
        return len(self._queue)

    @property
    def age(self):
        if self.size <= 1:
            return 0

        oldest, newest = self.oldest_and_newest_timestamps()
        return int((newest - oldest).total_seconds())

    def oldest_and_newest_timestamps(self):
        oldest = MAX_TIMESTAMP
        newest = datetime.min.replace(tzinfo=None)
        for task in self._queue:
            task_timestamp = self._timestamp_for_task(task)
            if task_timestamp > newest:
                newest = task_timestamp
            if task_timestamp < oldest:
                oldest = task_timestamp
        return oldest, newest

    def purge(self):
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



