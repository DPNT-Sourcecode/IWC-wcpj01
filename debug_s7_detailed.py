from datetime import datetime, timedelta
from lib.solutions.IWC.queue_solution_legacy import Queue
from lib.solutions.IWC.task_types import TaskSubmission

def iso_ts(delta_minutes=0):
    base = datetime(2025, 1, 1, 12, 0, 0)
    return (base + timedelta(minutes=delta_minutes)).isoformat()

queue = Queue()

# test_IWC_R5_S7
print("=== test_IWC_R5_S7 ===\n")
queue.enqueue(TaskSubmission("companies_house", 2, iso_ts(0)))
queue.enqueue(TaskSubmission("bank_statements", 1, iso_ts(1)))
queue.enqueue(TaskSubmission("id_verification", 2, iso_ts(2)))
queue.enqueue(TaskSubmission("bank_statements", 2, iso_ts(7)))
queue.enqueue(TaskSubmission("companies_house", 1, iso_ts(8)))
queue.enqueue(TaskSubmission("id_verification", 1, iso_ts(9)))

print(f"Queue age: {queue.age} seconds")
print(f"Queue size: {queue.size}\n")

# Check what's in the queue
print("Tasks in queue:")
for i, task in enumerate(queue._queue, 1):
    ts = datetime.fromisoformat(task.timestamp).replace(tzinfo=None)
    age = (datetime(2025, 1, 1, 12, 9) - ts).total_seconds()
    priority = task.metadata.get('priority', 'UNKNOWN')
    group_ts = task.metadata.get('group_earliest_timestamp', 'N/A')
    print(f"{i}. {task.provider:20} user={task.user_id} ts={task.timestamp[11:16]} age={age/60:.1f}min priority={priority} group_ts={group_ts}")

print("\nExpected dequeue order:")
expected = [
    ("companies_house", 2),
    ("bank_statements", 1),
    ("id_verification", 2),
    ("bank_statements", 2),
    ("companies_house", 1),
    ("id_verification", 1),
]
for i, (provider, user_id) in enumerate(expected, 1):
    print(f"  {i}. {provider:20} user={user_id}")

print("\nActual dequeue order:")
for i in range(6):
    task = queue.dequeue()
    expected_provider, expected_user = expected[i]
    match = "✓" if (task.provider == expected_provider and task.user_id == expected_user) else "✗"
    print(f"  {i+1}. {task.provider:20} user={task.user_id} {match}")
