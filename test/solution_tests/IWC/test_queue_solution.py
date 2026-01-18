from __future__ import annotations

from .utils import call_dequeue, call_enqueue, call_size, iso_ts, run_queue


def test_enqueue_size_dequeue_flow() -> None:
    run_queue([
        call_enqueue("companies_house", 1, iso_ts(delta_minutes=0)).expect(1),
        call_size().expect(1),
        call_dequeue().expect("companies_house", 1),
    ])

def test_rule_of_3() -> None:
    run_queue([
        call_enqueue("companies_house", 1, iso_ts(delta_minutes=0)).expect(1),
        call_enqueue("bank_statements", 2, iso_ts(delta_minutes=0)).expect(2),
        call_enqueue("id_verification", 1, iso_ts(delta_minutes=0)).expect(3),
        call_enqueue("bank_statements", 1, iso_ts(delta_minutes=0)).expect(4),
        call_size().expect(4),
        call_dequeue().expect("companies_house", 1),
        call_dequeue().expect("id_verification", 1),
        call_dequeue().expect("bank_statements", 1),
        call_dequeue().expect("bank_statements", 2),
    ])

def test_timestamp_order() -> None:
    run_queue([
        call_enqueue("bank_statements", 1, iso_ts(delta_minutes=5)).expect(1),
        call_enqueue("bank_statements", 2, iso_ts(delta_minutes=0)).expect(2),
        call_size().expect(2),
        call_dequeue().expect("bank_statements", 2),
        call_dequeue().expect("bank_statements", 1),
    ])

def test_dependency_resolution() -> None:
    run_queue([
        call_enqueue("credit_check", 1, iso_ts(delta_minutes=0)).expect(2),
        call_size().expect(2),
        call_dequeue().expect("companies_house", 1),
        call_dequeue().expect("credit_check", 1),
    ])

def test_deduplication() -> None:
    run_queue([
        call_enqueue("bank_statements", 1, iso_ts(delta_minutes=0)).expect(1),
        call_enqueue("bank_statements", 1, iso_ts(delta_minutes=5)).expect(1),
        call_enqueue("id_verification", 1, iso_ts(delta_minutes=5)).expect(2),
        call_size().expect(2),
        call_dequeue().expect("bank_statements", 1),
        call_dequeue().expect("id_verification", 1),
    ])

def test_bank_statements_deprioritization() -> None:
    run_queue([
        call_enqueue("bank_statements", 1, iso_ts(delta_minutes=0)).expect(1),
        call_enqueue("id_verification", 1, iso_ts(delta_minutes=1)).expect(2),
        call_enqueue("companies_house", 2, iso_ts(delta_minutes=2)).expect(3),
        call_size().expect(3),
        call_dequeue().expect("id_verification", 1),
        call_dequeue().expect("companies_house", 2),
        call_dequeue().expect("bank_statements", 1),
    ])

def test_rule_of_3_deprioritization() -> None:
    run_queue([
        call_enqueue("bank_statements", 1, iso_ts(delta_minutes=0)).expect(1),
        call_enqueue("bank_statements", 2, iso_ts(delta_minutes=0)).expect(2),
        call_enqueue("id_verification", 1, iso_ts(delta_minutes=0)).expect(3),
        call_enqueue("companies_house", 1, iso_ts(delta_minutes=0)).expect(4),
        call_size().expect(4),
        call_dequeue().expect("id_verification", 1),
        call_dequeue().expect("companies_house", 1),        
        call_dequeue().expect("bank_statements", 1),
        call_dequeue().expect("bank_statements", 2),
    ])

"""
New Requirement:
----------------
Adjust the queueing logic to deprioritize "bank_statements" tasks relative to other providers.
- If a customer has fewer than 3 tasks (no Rule of 3), their "bank_statements" task must go
  to the end of the global queue.
- If a customer is prioritised under the Rule of 3, their "bank_statements" task must be
  scheduled after all their other tasks.

1. Enqueue: user_id=1, provider="bank_statements", timestamp='2025-10-20 12:00:00' -> 1 (queue size)
2. Enqueue: user_id=1, provider="id_verification", timestamp='2025-10-20 12:01:00' -> 2 (queue size)  
3. Enqueue: user_id=2, provider="companies_house", timestamp='2025-10-20 12:02:00' -> 3 (queue size)  
4. Dequeue -> {"user_id": 1, "provider": "id_verification"}  
5. Dequeue -> {"user_id": 2, "provider": "companies_house"}  
6. Dequeue -> {"user_id": 1, "provider": "bank_statements"}  

The following operations show how deduplication works:

1. Enqueue: user_id=1, provider="bank_statements", timestamp='2025-10-20 12:00:00'  -> 1 (queue size)
2. Enqueue: user_id=1, provider="bank_statements", timestamp='2025-10-20 12:05:00'  -> 1 (queue size)
3. Enqueue: user_id=1, provider="id_verification", timestamp='2025-10-20 12:05:00'  -> 2 (queue size)
4. Dequeue -> {"user_id": 1, "provider": "bank_statements"}
5. Dequeue -> {"user_id": 1, "provider": "id_verification"}


Example #1 - Rule of 3:
--------
The following operations show how the Rule of 3 affects queue priority.

1. Enqueue: user_id=1, provider="companies_house",   timestamp='2025-10-20 12:00:00'  -> 1 (queue size)  
2. Enqueue: user_id=2, provider="bank_statements",   timestamp='2025-10-20 12:00:00'  -> 2 (queue size)  
3. Enqueue: user_id=1, provider="id_verification",   timestamp='2025-10-20 12:00:00'  -> 3 (queue size)  
4. Enqueue: user_id=1, provider="bank_statements",   timestamp='2025-10-20 12:00:00'  -> 4 (queue size)  
5. Dequeue -> {"user_id": 1, "provider": "companies_house"}  
6. Dequeue -> {"user_id": 1, "provider": "id_verification"}  
7. Dequeue -> {"user_id": 1, "provider": "bank_statements"}  
8. Dequeue -> {"user_id": 2, "provider": "bank_statements"}  

Once user 1 reaches 3 tasks, all of their jobs are moved ahead of user 2's, regardless of the
original enqueue order.


Example #2 - Timestamp Ordering:
--------
The following operations show how the order of tasks is determined by their timestamp.

1. Enqueue: user_id=1, provider="bank_statements", timestamp='2025-10-20 12:05:00'  -> 1 (queue size)
2. Enqueue: user_id=2, provider="bank_statements", timestamp='2025-10-20 12:00:00'  -> 2 (queue size)
3. Dequeue -> {"user_id": 2, "provider": "bank_statements"}  
4. Dequeue -> {"user_id": 1, "provider": "bank_statements"}  


Example #3 - Dependency Resolution:
--------
The following operations show that the when a task is enqueued, all its dependencies are also added.

1. Enqueue: user_id=1, provider="credit_check", timestamp='2025-10-20 12:00:00'  -> 2 (queue size)
2. Dequeue -> {"user_id": 1, "provider": "companies_house"}  
3. Dequeue -> {"user_id": 1, "provider": "credit_check"}


"""


