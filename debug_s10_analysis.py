from datetime import datetime, timedelta
from lib.solutions.IWC.queue_solution_entrypoint import QueueSolutionEntrypoint
from lib.solutions.IWC.task_types import TaskSubmission

def iso_ts(delta_minutes=0):
    base = datetime(2025, 1, 1, 12, 0, 0)
    return (base + timedelta(minutes=delta_minutes)).isoformat()

queue = QueueSolutionEntrypoint()

# Test IWC_R5_S10
print("=== Test IWC_R5_S10 ===")
print(f"1. Enqueue bank_statements user=1 ts=0: {queue.enqueue(TaskSubmission('bank_statements', 1, iso_ts(0)))}")
print(f"2. Enqueue id_verification user=1 ts=1: {queue.enqueue(TaskSubmission('id_verification', 1, iso_ts(1)))}")
print(f"3. Enqueue companies_house user=1 ts=2: {queue.enqueue(TaskSubmission('companies_house', 1, iso_ts(2)))}")
print(f"4. Enqueue companies_house user=2 ts=3: {queue.enqueue(TaskSubmission('companies_house', 2, iso_ts(3)))}")
print(f"\nQueue size: {queue.size()}")
print(f"Queue age: {queue.age()} seconds")

print("\nDequeue order:")
for i in range(1, 5):
    task = queue.dequeue()
    print(f"{i}. {task}")

print("\n" + "="*60)
print("=== Test old_bank_statements_priority ===")
queue2 = QueueSolutionEntrypoint()
print(f"1. Enqueue id_verification user=1 ts=0: {queue2.enqueue(TaskSubmission('id_verification', 1, iso_ts(0)))}")
print(f"2. Enqueue bank_statements user=2 ts=1: {queue2.enqueue(TaskSubmission('bank_statements', 2, iso_ts(1)))}")
print(f"3. Enqueue companies_house user=3 ts=7: {queue2.enqueue(TaskSubmission('companies_house', 3, iso_ts(7)))}")
print(f"\nQueue size: {queue2.size()}")
print(f"Queue age: {queue2.age()} seconds")

print("\nDequeue order:")
for i in range(1, 4):
    task = queue2.dequeue()
    print(f"{i}. {task}")
