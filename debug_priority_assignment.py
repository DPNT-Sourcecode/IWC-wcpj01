from datetime import datetime, timedelta
from lib.solutions.IWC.queue_solution_legacy import Queue
from lib.solutions.IWC.task_types import TaskSubmission

def iso_ts(delta_minutes=0):
    base = datetime(2025, 1, 1, 12, 0, 0)
    return (base + timedelta(minutes=delta_minutes)).isoformat()

queue = Queue()

# test_IWC_R5_S7
print("=== test_IWC_R5_S7 - Step by step ===\n")
queue.enqueue(TaskSubmission("companies_house", 2, iso_ts(0)))
print(f"After enqueue 1: size={queue.size}, user 2 task count=1")
queue.enqueue(TaskSubmission("bank_statements", 1, iso_ts(1)))
print(f"After enqueue 2: size={queue.size}, user 1 task count=1, user 2 task count=1")
queue.enqueue(TaskSubmission("id_verification", 2, iso_ts(2)))
print(f"After enqueue 3: size={queue.size}, user 1 task count=1, user 2 task count=2")
queue.enqueue(TaskSubmission("bank_statements", 2, iso_ts(7)))
print(f"After enqueue 4: size={queue.size}, user 1 task count=1, user 2 task count=3 **RULE OF 3**")
queue.enqueue(TaskSubmission("companies_house", 1, iso_ts(8)))
print(f"After enqueue 5: size={queue.size}, user 1 task count=2, user 2 task count=3")
queue.enqueue(TaskSubmission("id_verification", 1, iso_ts(9)))
print(f"After enqueue 6: size={queue.size}, user 1 task count=3 **RULE OF 3**, user 2 task count=3\n")

print("First dequeue call - this triggers priority assignment")
task1 = queue.dequeue()
print(f"Dequeued: {task1.provider} user={task1.user_id}\n")

print("Tasks remaining in queue:")
for i, task in enumerate(queue._queue, 1):
    ts = datetime.fromisoformat(task.timestamp).replace(tzinfo=None)
    priority = task.metadata.get('priority', 'UNKNOWN')
    group_ts = task.metadata.get('group_earliest_timestamp', 'N/A')
    if group_ts != 'N/A' and group_ts.year < 9000:
        group_ts_str = str(group_ts)[11:16]
    else:
        group_ts_str = 'MAX'
    print(f"{i}. {task.provider:20} user={task.user_id} priority={priority} group_ts={group_ts_str}")
