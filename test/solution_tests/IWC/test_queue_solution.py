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

"""
id = IWC_R5_S6_001, req = enqueue({"provider":"bank_statements","timestamp":"2025-10-20 12:00:00","user_id":1}), resp = 1
id = IWC_R5_S6_002, req = enqueue({"provider":"companies_house","timestamp":"2025-10-20 12:01:00","user_id":2}), resp = 2
id = IWC_R5_S6_003, req = enqueue({"provider":"id_verification","timestamp":"2025-10-20 12:06:00","user_id":2}), resp = 3
id = IWC_R5_S6_004, req = enqueue({"provider":"bank_statements","timestamp":"2025-10-20 12:07:00","user_id":2}), resp = 4
"""


"""
id = IWC_R5_S1_000, req = purge(), resp = true
id = IWC_R5_S1_001, req = purge(), resp = true
id = IWC_R5_S1_002, req = enqueue({"provider":"companies_house","timestamp":"2025-10-20 12:00:00","user_id":1}), resp = 1
id = IWC_R5_S1_003, req = size(), resp = 1
id = IWC_R5_S1_004, req = dequeue(), resp = {"provider":"companies_house","user_id":1}
id = IWC_R5_S2_000, req = purge(), resp = true
id = IWC_R5_S2_001, req = enqueue({"provider":"bank_statements","timestamp":"2025-10-20 12:00:00","user_id":1}), resp = 1
id = IWC_R5_S2_002, req = enqueue({"provider":"companies_house","timestamp":"2025-10-20 12:01:00","user_id":1}), resp = 2
id = IWC_R5_S2_003, req = enqueue({"provider":"id_verification","timestamp":"2025-10-20 12:06:00","user_id":6}), resp = 3
id = IWC_R5_S2_004, req = dequeue(), resp = {"provider":"bank_statements","user_id":1}
id = IWC_R5_S2_005, req = dequeue(), resp = {"provider":"companies_house","user_id":1}
id = IWC_R5_S2_006, req = dequeue(), resp = {"provider":"id_verification","user_id":6}
id = IWC_R5_S3_000, req = purge(), resp = true
id = IWC_R5_S3_001, req = enqueue({"provider":"bank_statements","timestamp":"2025-10-20 12:00:00","user_id":1}), resp = 1
id = IWC_R5_S3_002, req = enqueue({"provider":"companies_house","timestamp":"2025-10-20 12:01:00","user_id":1}), resp = 2
id = IWC_R5_S3_003, req = enqueue({"provider":"id_verification","timestamp":"2025-10-20 12:06:00","user_id":1}), resp = 3
id = IWC_R5_S3_004, req = dequeue(), resp = {"provider":"bank_statements","user_id":1}
id = IWC_R5_S3_005, req = dequeue(), resp = {"provider":"companies_house","user_id":1}
id = IWC_R5_S3_006, req = dequeue(), resp = {"provider":"id_verification","user_id":1}
id = IWC_R5_S4_000, req = purge(), resp = true
id = IWC_R5_S4_001, req = enqueue({"provider":"id_verification","timestamp":"2025-10-20 12:00:00","user_id":1}), resp = 1
id = IWC_R5_S4_002, req = enqueue({"provider":"bank_statements","timestamp":"2025-10-20 12:01:00","user_id":2}), resp = 2
id = IWC_R5_S4_003, req = enqueue({"provider":"companies_house","timestamp":"2025-10-20 12:07:00","user_id":3}), resp = 3
id = IWC_R5_S4_004, req = dequeue(), resp = {"provider":"id_verification","user_id":1}
id = IWC_R5_S4_005, req = dequeue(), resp = {"provider":"bank_statements","user_id":2}
id = IWC_R5_S4_006, req = dequeue(), resp = {"provider":"companies_house","user_id":3}
id = IWC_R5_S5_000, req = purge(), resp = true
id = IWC_R5_S5_001, req = enqueue({"provider":"companies_house","timestamp":"2025-10-20 12:00:00","user_id":1}), resp = 1
id = IWC_R5_S5_002, req = enqueue({"provider":"bank_statements","timestamp":"2025-10-20 12:00:00","user_id":1}), resp = 2
id = IWC_R5_S5_003, req = enqueue({"provider":"id_verification","timestamp":"2025-10-20 12:06:00","user_id":6}), resp = 3
id = IWC_R5_S5_004, req = dequeue(), resp = {"provider":"companies_house","user_id":1}
id = IWC_R5_S5_005, req = dequeue(), resp = {"provider":"bank_statements","user_id":1}
id = IWC_R5_S5_006, req = dequeue(), resp = {"provider":"id_verification","user_id":6}
id = IWC_R5_S6_000, req = purge(), resp = true
id = IWC_R5_S6_001, req = enqueue({"provider":"bank_statements","timestamp":"2025-10-20 12:00:00","user_id":1}), resp = 1
id = IWC_R5_S6_002, req = enqueue({"provider":"companies_house","timestamp":"2025-10-20 12:01:00","user_id":2}), resp = 2
id = IWC_R5_S6_003, req = enqueue({"provider":"id_verification","timestamp":"2025-10-20 12:06:00","user_id":2}), resp = 3
id = IWC_R5_S6_004, req = enqueue({"provider":"bank_statements","timestamp":"2025-10-20 12:07:00","user_id":2}), resp = 4
id = IWC_R5_S6_005, req = dequeue(), resp = {"provider":"companies_house","user_id":2}
id = IWC_R5_S6_006, req = dequeue(), resp = {"provider":"id_verification","user_id":2}
id = IWC_R5_S6_007, req = dequeue(), resp = {"provider":"bank_statements","user_id":1}
id = IWC_R5_S6_008, req = dequeue(), resp = {"provider":"bank_statements","user_id":2}
id = IWC_R5_S7_000, req = purge(), resp = true
id = IWC_R5_S7_001, req = enqueue({"provider":"companies_house","timestamp":"2025-10-20 12:00:00","user_id":2}), resp = 1
id = IWC_R5_S7_002, req = enqueue({"provider":"bank_statements","timestamp":"2025-10-20 12:01:00","user_id":1}), resp = 2
id = IWC_R5_S7_003, req = enqueue({"provider":"id_verification","timestamp":"2025-10-20 12:02:00","user_id":2}), resp = 3
id = IWC_R5_S7_004, req = enqueue({"provider":"bank_statements","timestamp":"2025-10-20 12:07:00","user_id":2}), resp = 4
id = IWC_R5_S7_005, req = enqueue({"provider":"companies_house","timestamp":"2025-10-20 12:08:00","user_id":1}), resp = 5
id = IWC_R5_S7_006, req = enqueue({"provider":"id_verification","timestamp":"2025-10-20 12:09:00","user_id":1}), resp = 6
id = IWC_R5_S7_007, req = dequeue(), resp = {"provider":"companies_house","user_id":2}
id = IWC_R5_S7_008, req = dequeue(), resp = {"provider":"id_verification","user_id":2}
id = IWC_R5_S7_009, req = dequeue(), resp = {"provider":"bank_statements","user_id":1}
id = IWC_R5_S7_010, req = dequeue(), resp = {"provider":"companies_house","user_id":1}
id = IWC_R5_S7_011, req = dequeue(), resp = {"provider":"id_verification","user_id":1}
id = IWC_R5_S7_012, req = dequeue(), resp = {"provider":"bank_statements","user_id":2}
id = IWC_R5_S8_000, req = purge(), resp = true
id = IWC_R5_S8_001, req = enqueue({"provider":"bank_statements","timestamp":"2025-10-20 12:00:00","user_id":1}), resp = 1
id = IWC_R5_S8_002, req = enqueue({"provider":"bank_statements","timestamp":"2025-10-20 12:00:00","user_id":2}), resp = 2
id = IWC_R5_S8_003, req = enqueue({"provider":"companies_house","timestamp":"2025-10-20 12:01:00","user_id":3}), resp = 3
id = IWC_R5_S8_004, req = enqueue({"provider":"id_verification","timestamp":"2025-10-20 12:07:00","user_id":3}), resp = 4
id = IWC_R5_S8_005, req = dequeue(), resp = {"provider":"bank_statements","user_id":1}
id = IWC_R5_S8_006, req = dequeue(), resp = {"provider":"bank_statements","user_id":2}
id = IWC_R5_S8_007, req = dequeue(), resp = {"provider":"companies_house","user_id":3}
id = IWC_R5_S8_008, req = dequeue(), resp = {"provider":"id_verification","user_id":3}
id = IWC_R5_S9_000, req = purge(), resp = true
id = IWC_R5_S9_001, req = enqueue({"provider":"bank_statements","timestamp":"2025-10-20 12:00:00","user_id":1}), resp = 1
id = IWC_R5_S9_002, req = enqueue({"provider":"bank_statements","timestamp":"2025-10-20 12:00:00","user_id":2}), resp = 2
id = IWC_R5_S9_003, req = enqueue({"provider":"companies_house","timestamp":"2025-10-20 12:01:00","user_id":2}), resp = 3
id = IWC_R5_S9_004, req = enqueue({"provider":"id_verification","timestamp":"2025-10-20 12:07:00","user_id":2}), resp = 4
id = IWC_R5_S9_005, req = dequeue(), resp = {"provider":"bank_statements","user_id":2}
id = IWC_R5_S9_006, req = dequeue(), resp = {"provider":"companies_house","user_id":2}
id = IWC_R5_S9_007, req = dequeue(), resp = {"provider":"id_verification","user_id":2}
id = IWC_R5_S9_008, req = dequeue(), resp = {"provider":"bank_statements","user_id":1}
id = IWC_R5_S10_000, req = purge(), resp = true
id = IWC_R5_S10_001, req = enqueue({"provider":"bank_statements","timestamp":"2025-10-20 12:00:00","user_id":1}), resp = 1
id = IWC_R5_S10_002, req = enqueue({"provider":"id_verification","timestamp":"2025-10-20 12:01:00","user_id":1}), resp = 2
id = IWC_R5_S10_003, req = enqueue({"provider":"companies_house","timestamp":"2025-10-20 12:02:00","user_id":1}), resp = 3
id = IWC_R5_S10_004, req = enqueue({"provider":"companies_house","timestamp":"2025-10-20 12:03:00","user_id":2}), resp = 4
id = IWC_R5_S10_005, req = dequeue(), resp = {"provider":"id_verification","user_id":1}
id = IWC_R5_S10_006, req = dequeue(), resp = {"provider":"companies_house","user_id":1}
id = IWC_R5_S10_007, req = dequeue(), resp = {"provider":"companies_house","user_id":2}
id = IWC_R5_S10_008, req = dequeue(), resp = {"provider":"bank_statements","user_id":1}
id = IWC_R5_S11_000, req = purge(), resp = true
id = IWC_R5_S11_001, req = enqueue({"provider":"companies_house","timestamp":"2025-10-20 12:07:00","user_id":1}), resp = 1
id = IWC_R5_S11_002, req = enqueue({"provider":"bank_statements","timestamp":"2025-10-20 12:01:00","user_id":1}), resp = 2
id = IWC_R5_S11_003, req = enqueue({"provider":"companies_house","timestamp":"2025-10-20 12:00:00","user_id":2}), resp = 3
id = IWC_R5_S11_004, req = dequeue(), resp = {"provider":"companies_house","user_id":2}
id = IWC_R5_S11_005, req = dequeue(), resp = {"provider":"bank_statements","user_id":1}
id = IWC_R5_S11_006, req = dequeue(), resp = {"provider":"companies_house","user_id":1}
id = IWC_R5_S12_000, req = purge(), resp = true
id = IWC_R5_S12_001, req = enqueue({"provider":"companies_house","timestamp":"2025-10-20 12:07:00","user_id":1}), resp = 1
id = IWC_R5_S12_002, req = enqueue({"provider":"id_verification","timestamp":"2025-10-20 12:07:00","user_id":1}), resp = 2
id = IWC_R5_S12_003, req = enqueue({"provider":"bank_statements","timestamp":"2025-10-20 12:01:00","user_id":1}), resp = 3
id = IWC_R5_S12_004, req = enqueue({"provider":"credit_check","timestamp":"2025-10-20 12:00:00","user_id":1}), resp = 4
id = IWC_R5_S12_005, req = dequeue(), resp = {"provider":"companies_house","user_id":1}
id = IWC_R5_S12_006, req = dequeue(), resp = {"provider":"credit_check","user_id":1}
id = IWC_R5_S12_007, req = dequeue(), resp = {"provider":"bank_statements","user_id":1}
id = IWC_R5_S12_008, req = dequeue(), resp = {"provider":"id_verification","user_id":1}
Stopping client
Notify round "IWC_R5", event "deploy"
--------------------------------------------

Result is: FAILED
Some requests have failed (16/98). Here are some of them:

Test: IWC_R5_S5_004 | Method: dequeue | Params: []
Assertion: equals
Expected: {"provider":"bank_statements","user_id":1}
Actual:   {"provider":"companies_house","user_id":1}

----------------------------------------

Test: IWC_R5_S5_005 | Method: dequeue | Params: []
Assertion: equals
Expected: {"provider":"companies_house","user_id":1}
Actual:   {"provider":"bank_statements","user_id":1}

----------------------------------------

Test: IWC_R5_S6_005 | Method: dequeue | Params: []
Assertion: equals
Expected: {"provider":"bank_statements","user_id":1}
Actual:   {"provider":"companies_house","user_id":2}

----------------------------------------

You have received a penalty of: 10 min
The round will restart now

Look at your failed trials and edit your code. When you've finished, deploy your code with "deploy"

Challenge description saved to file: challenges/IWC_R5.txt.

"""
