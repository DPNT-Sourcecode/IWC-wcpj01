from datetime import datetime, timedelta
from lib.solutions.IWC.queue_solution_entrypoint import QueueSolutionEntrypoint
from lib.solutions.IWC.task_types import TaskSubmission

def iso_ts(delta_minutes=0):
    base = datetime(2025, 1, 1, 12, 0, 0)
    return (base + timedelta(minutes=delta_minutes)).isoformat()

def test_scenario(name, enqueues, expected_order):
    print(f"\n{'='*70}")
    print(f"Test: {name}")
    print('='*70)
    
    queue = QueueSolutionEntrypoint()
    for provider, user_id, ts_delta in enqueues:
        size = queue.enqueue(TaskSubmission(provider, user_id, iso_ts(ts_delta)))
        print(f"Enqueue {provider:20} user={user_id} ts={ts_delta:2}min -> size={size}")
    
    print(f"\nQueue age: {queue.age()} seconds")
    print("\nExpected order:")
    for i, (provider, user_id) in enumerate(expected_order, 1):
        print(f"  {i}. {provider:20} user={user_id}")
    
    print("\nActual order:")
    for i in range(len(expected_order)):
        task = queue.dequeue()
        expected_provider, expected_user = expected_order[i]
        match = "✓" if (task.provider == expected_provider and task.user_id == expected_user) else "✗"
        print(f"  {i}. {task.provider:20} user={task.user_id} {match}")


# Test deduplication
test_scenario(
    "test_deduplication",
    [
        ("bank_statements", 1, 0),
        ("bank_statements", 1, 5),
        ("id_verification", 1, 5),
    ],
    [
        ("id_verification", 1),
        ("bank_statements", 1),
    ]
)

# Test old_bank_statements_priority_rule_of_3
test_scenario(
    "test_old_bank_statements_priority_rule_of_3",
    [
        ("id_verification", 1, 0),
        ("bank_statements", 1, 1),
        ("companies_house", 1, 7),
    ],
    [
        ("id_verification", 1),
        ("bank_statements", 1),
        ("companies_house", 1),
    ]
)

# Test IWC_R5_S5
test_scenario(
    "test_IWC_R5_S5",
    [
        ("companies_house", 1, 0),
        ("bank_statements", 1, 0),
        ("id_verification", 6, 6),
    ],
    [
        ("bank_statements", 1),
        ("companies_house", 1),
        ("id_verification", 6),
    ]
)

# Test IWC_R5_S6
test_scenario(
    "test_IWC_R5_S6",
    [
        ("bank_statements", 1, 0),
        ("companies_house", 2, 1),
        ("id_verification", 2, 6),
        ("bank_statements", 2, 7),
    ],
    [
        ("bank_statements", 1),
        ("companies_house", 2),
        ("id_verification", 2),
        ("bank_statements", 2),
    ]
)

# Test IWC_R5_S7
test_scenario(
    "test_IWC_R5_S7",
    [
        ("companies_house", 2, 0),
        ("bank_statements", 1, 1),
        ("id_verification", 2, 2),
        ("bank_statements", 2, 7),
        ("companies_house", 1, 8),
        ("id_verification", 1, 9),
    ],
    [
        ("companies_house", 2),
        ("bank_statements", 1),
        ("id_verification", 2),
        ("bank_statements", 2),
        ("companies_house", 1),
        ("id_verification", 1),
    ]
)
