#!/usr/bin/env python
import sys
sys.path.insert(0, 'lib')

from solutions.IWC.queue_solution_entrypoint import QueueSolutionEntrypoint
from solutions.IWC.task_types import TaskSubmission
from test.solution_tests.IWC.utils import iso_ts

queue = QueueSolutionEntrypoint()

# Enqueue all tasks
queue.enqueue(TaskSubmission(provider="bank_statements", user_id=1, timestamp=iso_ts(delta_minutes=0)))
queue.enqueue(TaskSubmission(provider="id_verification", user_id=1, timestamp=iso_ts(delta_minutes=1)))
queue.enqueue(TaskSubmission(provider="companies_house", user_id=1, timestamp=iso_ts(delta_minutes=2)))
queue.enqueue(TaskSubmission(provider="companies_house", user_id=2, timestamp=iso_ts(delta_minutes=3)))

print("Queue size:", queue.size())
print("\nDequeuing:")

for i in range(4):
    result = queue.dequeue()
    print(f"{i+1}. {result.provider} user={result.user_id}")
