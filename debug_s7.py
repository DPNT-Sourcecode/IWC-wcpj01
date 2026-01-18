#!/usr/bin/env python
import sys
sys.path.insert(0, 'lib')

from solutions.IWC.queue_solution_entrypoint import QueueSolutionEntrypoint
from solutions.IWC.task_types import TaskSubmission
from test.solution_tests.IWC.utils import iso_ts

queue = QueueSolutionEntrypoint()

# Enqueue all tasks
queue.enqueue(TaskSubmission(provider="companies_house", user_id=2, timestamp=iso_ts(delta_minutes=0)))
queue.enqueue(TaskSubmission(provider="bank_statements", user_id=1, timestamp=iso_ts(delta_minutes=1)))
queue.enqueue(TaskSubmission(provider="id_verification", user_id=2, timestamp=iso_ts(delta_minutes=2)))
queue.enqueue(TaskSubmission(provider="bank_statements", user_id=2, timestamp=iso_ts(delta_minutes=7)))
queue.enqueue(TaskSubmission(provider="companies_house", user_id=1, timestamp=iso_ts(delta_minutes=8)))
queue.enqueue(TaskSubmission(provider="id_verification", user_id=1, timestamp=iso_ts(delta_minutes=9)))

print("Initial queue size:", queue.size())
print()

# Dequeue and show results
for i in range(1, 7):
    result = queue.dequeue()
    print(f"Step {i+7}: Dequeued {result.provider} user={result.user_id}")
    print(f"  Remaining: {queue.size()}")
    print()
