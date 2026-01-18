from __future__ import annotations

from .utils import call_dequeue, call_enqueue, call_size, iso_ts, run_queue, call_age


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
        call_dequeue().expect("id_verification", 1),
        call_dequeue().expect("bank_statements", 1),
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

def test_age_calculation() -> None:
    run_queue([
        call_enqueue("id_verification", 1, iso_ts(delta_minutes=0)).expect(1),
        call_enqueue("id_verification", 2, iso_ts(delta_minutes=5)).expect(2),
        call_size().expect(2),
        call_age().expect(300),  # 5 minutes in seconds
    ])

def test_age_calculation_empty_queue() -> None:
    run_queue([
        call_size().expect(0),
        call_age().expect(0),
    ])

def test_age_calculation_single_element() -> None:
    run_queue([
        call_enqueue("id_verification", 1, iso_ts(delta_minutes=0)).expect(1),
        call_size().expect(1),
        call_age().expect(0),
    ])

def test_age_calculation() -> None:
    run_queue([
        call_enqueue("companies_house", 1, iso_ts(delta_minutes=0)).expect(1),
        call_enqueue("bank_statements", 2, iso_ts(delta_minutes=1)).expect(2),
        call_enqueue("id_verification", 1, iso_ts(delta_minutes=2)).expect(3),
        call_enqueue("bank_statements", 1, iso_ts(delta_minutes=5)).expect(4),
        call_size().expect(4),
        call_age().expect(300),  # 5 minutes in seconds
    ])

def test_old_bank_statements_priority_rule_of_3() -> None:
    run_queue([
        call_enqueue("id_verification", 1, iso_ts(delta_minutes=0)).expect(1),
        call_enqueue("bank_statements", 1, iso_ts(delta_minutes=1)).expect(2),
        call_enqueue("companies_house", 1, iso_ts(delta_minutes=7)).expect(3),
        call_size().expect(3),
        call_dequeue().expect("id_verification", 1),
        call_dequeue().expect("bank_statements", 1),
        call_dequeue().expect("companies_house", 1),
    ])

def test_old_bank_statements_priority() -> None:
    run_queue([
        call_enqueue("id_verification", 1, iso_ts(delta_minutes=0)).expect(1),
        call_enqueue("bank_statements", 2, iso_ts(delta_minutes=1)).expect(2),
        call_enqueue("companies_house", 3, iso_ts(delta_minutes=7)).expect(3),
        call_size().expect(3),
        call_dequeue().expect("id_verification", 1),
        call_dequeue().expect("bank_statements", 2),
        call_dequeue().expect("companies_house", 3),
    ])

def test_IWC_R5_S5() -> None:
    """IWC_R5_S5: Same timestamp for companies_house and bank_statements, both old.
    Expected: bank_statements comes first when both are old (>5 min)."""
    run_queue([
        call_enqueue("companies_house", 1, iso_ts(delta_minutes=0)).expect(1),
        call_enqueue("bank_statements", 1, iso_ts(delta_minutes=0)).expect(2),
        call_enqueue("id_verification", 6, iso_ts(delta_minutes=6)).expect(3),
        call_size().expect(3),
        call_dequeue().expect("bank_statements", 1),  # Both are 6min old, bank_statements prioritized
        call_dequeue().expect("companies_house", 1),
        call_dequeue().expect("id_verification", 6),
    ])

def test_IWC_R5_S6() -> None:
    """IWC_R5_S6: From server test - old bank_statements interaction with rule of 3."""
    run_queue([
        call_enqueue("bank_statements", 1, iso_ts(delta_minutes=0)).expect(1),
        call_enqueue("companies_house", 2, iso_ts(delta_minutes=1)).expect(2),
        call_enqueue("id_verification", 2, iso_ts(delta_minutes=6)).expect(3),
        call_enqueue("bank_statements", 2, iso_ts(delta_minutes=7)).expect(4),
        call_size().expect(4),
        call_dequeue().expect("bank_statements", 1),  # Old bank_statements (7min old)
        call_dequeue().expect("companies_house", 2),  # Rule of 3 first (user 2 has 3 tasks)
        call_dequeue().expect("id_verification", 2),        
        call_dequeue().expect("bank_statements", 2),  # Fresh bank_statements last
    ])

def test_IWC_R5_S7() -> None:
    """IWC_R5_S7: Rule of 3 with fresh bank_statements in the group.
    Server expected order shows old bank can skip HIGH priority if it has older timestamp."""
    run_queue([
        call_enqueue("companies_house", 2, iso_ts(delta_minutes=0)).expect(1),
        call_enqueue("bank_statements", 1, iso_ts(delta_minutes=1)).expect(2),
        call_enqueue("id_verification", 2, iso_ts(delta_minutes=2)).expect(3),
        call_enqueue("bank_statements", 2, iso_ts(delta_minutes=7)).expect(4),
        call_enqueue("companies_house", 1, iso_ts(delta_minutes=8)).expect(5),
        call_enqueue("id_verification", 1, iso_ts(delta_minutes=9)).expect(6),
        call_size().expect(6),
        call_dequeue().expect("companies_house", 2),  # Rule of 3, earliest timestamp (0min)
        call_dequeue().expect("bank_statements", 1),  # OLD (8min old), timestamp=1min - skips HIGH priority
        call_dequeue().expect("id_verification", 2),  # Rule of 3 (2min)
        call_dequeue().expect("bank_statements", 2),  # Rule of 3 (7min)
        call_dequeue().expect("companies_house", 1),  # Rule of 3 (8min)
        call_dequeue().expect("id_verification", 1),  # Rule of 3 (9min)
    ])


"""

## This is what the server test output was for IWC_R5_S7:
id = IWC_R5_S7_001, req = enqueue({"provider":"companies_house","timestamp":"2025-10-20 12:00:00","user_id":2}), resp = 1
id = IWC_R5_S7_002, req = enqueue({"provider":"bank_statements","timestamp":"2025-10-20 12:01:00","user_id":1}), resp = 2
id = IWC_R5_S7_003, req = enqueue({"provider":"id_verification","timestamp":"2025-10-20 12:02:00","user_id":2}), resp = 3
id = IWC_R5_S7_004, req = enqueue({"provider":"bank_statements","timestamp":"2025-10-20 12:07:00","user_id":2}), resp = 4
id = IWC_R5_S7_005, req = enqueue({"provider":"companies_house","timestamp":"2025-10-20 12:08:00","user_id":1}), resp = 5
id = IWC_R5_S7_006, req = enqueue({"provider":"id_verification","timestamp":"2025-10-20 12:09:00","user_id":1}), resp = 6
id = IWC_R5_S7_007, req = dequeue(), resp = {"provider":"companies_house","user_id":2}
id = IWC_R5_S7_008, req = dequeue(), resp = {"provider":"bank_statements","user_id":1}
id = IWC_R5_S7_009, req = dequeue(), resp = {"provider":"id_verification","user_id":2}
id = IWC_R5_S7_010, req = dequeue(), resp = {"provider":"companies_house","user_id":1}
id = IWC_R5_S7_011, req = dequeue(), resp = {"provider":"id_verification","user_id":1}
id = IWC_R5_S7_012, req = dequeue(), resp = {"provider":"bank_statements","user_id":2}


Result is: FAILED
Some requests have failed (5/98). Here are some of them:

Test: IWC_R5_S7_010 | Method: dequeue | Params: []
Assertion: equals
Expected: {"provider":"bank_statements","user_id":2}
Actual:   {"provider":"companies_house","user_id":1}

----------------------------------------

Test: IWC_R5_S7_011 | Method: dequeue | Params: []
Assertion: equals
Expected: {"provider":"companies_house","user_id":1}
Actual:   {"provider":"id_verification","user_id":1}

----------------------------------------

Test: IWC_R5_S7_012 | Method: dequeue | Params: []
Assertion: equals
Expected: {"provider":"id_verification","user_id":1}
Actual:   {"provider":"bank_statements","user_id":2}
"""