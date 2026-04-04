"""
Parts 22-23 — Evaluation Monitor Test

Tests:
1.  find_stale_hypotheses() returns list with staleness field
2.  URGENT staleness for unscored hypothesis
3.  CURRENT staleness for hypothesis with valid score and no drift
4.  build_rescore_queue() excludes CURRENT items
5.  build_rescore_queue() sorts URGENT before STALE before DRIFT
6.  stamp_signal_at_validation() writes signal_at_validation to graph
7.  run_monitor() returns report with correct keys
8.  Monitor counts sum correctly (urgent+stale+drift+current == total)
9.  Monitor queue excludes CURRENT items
10. app.py has Editorial Queue and Pipeline Monitor pages

Run: python tests/test_22_23_monitor.py
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv(override=True)

from neo4j import GraphDatabase

PASS = "✓"
FAIL = "✗"
HYP_ID = "HYP_BIM_026_BIM_027"


def get_driver():
    return GraphDatabase.driver(
        os.getenv("NEO4J_URI"),
        auth=(os.getenv("NEO4J_USER", "neo4j"), os.getenv("NEO4J_PASSWORD")),
    )


def test_find_stale_returns_list():
    from evaluation.monitor import find_stale_hypotheses
    driver = get_driver()
    result = find_stale_hypotheses(driver)
    driver.close()
    if not isinstance(result, list):
        return False, f"Expected list, got {type(result)}"
    required_keys = {"hypothesis_id", "staleness", "staleness_reason", "conviction_score"}
    if result:
        missing = required_keys - result[0].keys()
        if missing:
            return False, f"Missing keys in result: {missing}"
    return True, f"find_stale_hypotheses returned {len(result)} hypothesis(es)"


def test_urgent_for_unscored():
    from evaluation.monitor import find_stale_hypotheses
    # Create a temporary hypothesis with no validation_score
    driver = get_driver()
    with driver.session() as s:
        s.run("""
            MERGE (h:DisruptionHypothesis {hypothesis_id: 'HYP_TEST_MONITOR_001'})
            SET h.conviction_score = 0.5,
                h.title = 'Monitor test hypothesis',
                h.status = 'Hypothesis',
                h.validation_score = null
        """)

    result = find_stale_hypotheses(driver)
    driver.close()

    test_hyp = next((h for h in result if h["hypothesis_id"] == "HYP_TEST_MONITOR_001"), None)
    if not test_hyp:
        return False, "Test hypothesis not found in result"
    if test_hyp["staleness"] != "URGENT":
        return False, f"Expected URGENT, got {test_hyp['staleness']}"

    # Cleanup
    driver = get_driver()
    with driver.session() as s:
        s.run("MATCH (h:DisruptionHypothesis {hypothesis_id: 'HYP_TEST_MONITOR_001'}) DELETE h")
    driver.close()

    return True, "Unscored hypothesis correctly classified as URGENT"


def test_current_for_scored_no_drift():
    from evaluation.monitor import find_stale_hypotheses
    # Create hypothesis with validation_score and signal_at_validation = 0 (no linked vector,
    # so signal_now will also be 0 — drift = 0, within threshold → CURRENT)
    driver = get_driver()
    with driver.session() as s:
        s.run("""
            MERGE (h:DisruptionHypothesis {hypothesis_id: 'HYP_TEST_MONITOR_002'})
            SET h.conviction_score = 0.6,
                h.title = 'Monitor test current',
                h.status = 'Hypothesis',
                h.validation_score = 0.55,
                h.validated_at = '2024-01-01T00:00:00+00:00',
                h.researched_at = '2024-01-01T00:00:00+00:00',
                h.signal_at_validation = 0.0
        """)

    result = find_stale_hypotheses(driver, drift_threshold=0.10)
    driver.close()

    test_hyp = next((h for h in result if h["hypothesis_id"] == "HYP_TEST_MONITOR_002"), None)
    if not test_hyp:
        return False, "Test hypothesis not found"
    if test_hyp["staleness"] != "CURRENT":
        return False, f"Expected CURRENT, got {test_hyp['staleness']} — reason: {test_hyp['staleness_reason']}"

    # Cleanup
    driver = get_driver()
    with driver.session() as s:
        s.run("MATCH (h:DisruptionHypothesis {hypothesis_id: 'HYP_TEST_MONITOR_002'}) DELETE h")
    driver.close()

    return True, "Scored hypothesis with no drift correctly classified as CURRENT"


def test_queue_excludes_current():
    from evaluation.monitor import build_rescore_queue
    driver = get_driver()
    queue = build_rescore_queue(driver)
    driver.close()
    current_in_queue = [q for q in queue if q["staleness"] == "CURRENT"]
    if current_in_queue:
        return False, f"{len(current_in_queue)} CURRENT items in queue (should be excluded)"
    return True, f"Queue has {len(queue)} item(s), none CURRENT"


def test_queue_sort_order():
    from evaluation.monitor import build_rescore_queue
    # Inject two hypotheses: one URGENT, one with drift
    driver = get_driver()
    with driver.session() as s:
        s.run("""
            MERGE (h1:DisruptionHypothesis {hypothesis_id: 'HYP_TEST_SORT_001'})
            SET h1.conviction_score = 0.5, h1.title = 'Sort test urgent',
                h1.status = 'Hypothesis', h1.validation_score = null
            MERGE (h2:DisruptionHypothesis {hypothesis_id: 'HYP_TEST_SORT_002'})
            SET h2.conviction_score = 0.8, h2.title = 'Sort test drift',
                h2.status = 'Hypothesis', h2.validation_score = 0.6,
                h2.validated_at = '2024-01-01T00:00:00+00:00',
                h2.signal_at_validation = 0.3
        """)

    queue = build_rescore_queue(driver)
    driver.close()

    # Find our test items
    urgent_idx = next((i for i, q in enumerate(queue) if q["hypothesis_id"] == "HYP_TEST_SORT_001"), None)
    drift_idx  = next((i for i, q in enumerate(queue) if q["hypothesis_id"] == "HYP_TEST_SORT_002"), None)

    # Cleanup
    driver = get_driver()
    with driver.session() as s:
        s.run("""
            MATCH (h:DisruptionHypothesis)
            WHERE h.hypothesis_id IN ['HYP_TEST_SORT_001', 'HYP_TEST_SORT_002']
            DELETE h
        """)
    driver.close()

    if urgent_idx is None or drift_idx is None:
        return False, f"Test items not found in queue (urgent={urgent_idx}, drift={drift_idx})"
    if urgent_idx >= drift_idx:
        return False, f"URGENT should come before DRIFT: urgent_idx={urgent_idx}, drift_idx={drift_idx}"
    return True, f"URGENT at [{urgent_idx}] before DRIFT at [{drift_idx}] — sort correct"


def test_stamp_signal_at_validation():
    from evaluation.monitor import stamp_signal_at_validation
    driver = get_driver()
    with driver.session() as s:
        s.run("""
            MERGE (h:DisruptionHypothesis {hypothesis_id: 'HYP_TEST_STAMP_001'})
            SET h.conviction_score = 0.5, h.title = 'Stamp test',
                h.status = 'Hypothesis'
        """)

    stamp_signal_at_validation(driver, "HYP_TEST_STAMP_001", 0.725)

    with driver.session() as s:
        rec = s.run("""
            MATCH (h:DisruptionHypothesis {hypothesis_id: 'HYP_TEST_STAMP_001'})
            RETURN h.signal_at_validation AS sig
        """).single()
    driver.close()

    written = rec["sig"] if rec else None

    # Cleanup
    driver = get_driver()
    with driver.session() as s:
        s.run("MATCH (h:DisruptionHypothesis {hypothesis_id: 'HYP_TEST_STAMP_001'}) DELETE h")
    driver.close()

    if abs((written or 0) - 0.725) > 0.001:
        return False, f"signal_at_validation={written}, expected 0.725"
    return True, f"signal_at_validation={written} written correctly"


def test_run_monitor_keys():
    from evaluation.monitor import run_monitor
    report = run_monitor(drift_threshold=0.10, dry_run=True)
    required = {"checked_at", "total_hypotheses", "urgent", "stale", "drift", "current", "queue"}
    missing = required - report.keys()
    if missing:
        return False, f"Missing keys in report: {missing}"
    return True, f"Monitor report has all required keys ({len(required)} keys)"


def test_monitor_counts_sum():
    from evaluation.monitor import run_monitor
    report = run_monitor(drift_threshold=0.10, dry_run=True)
    total  = report["total_hypotheses"]
    summed = report["urgent"] + report["stale"] + report["drift"] + report["current"]
    if total != summed:
        return False, f"Counts don't sum: urgent+stale+drift+current={summed} != total={total}"
    return True, f"Counts sum correctly: {total} = {report['urgent']}+{report['stale']}+{report['drift']}+{report['current']}"


def test_monitor_queue_no_current():
    from evaluation.monitor import run_monitor
    report = run_monitor(drift_threshold=0.10, dry_run=True)
    queue  = report.get("queue", [])
    current_in_q = [q for q in queue if q.get("staleness") == "CURRENT"]
    if current_in_q:
        return False, f"{len(current_in_q)} CURRENT items found in queue"
    return True, f"Queue has {len(queue)} items, no CURRENT"


def test_ui_has_new_pages():
    import ast
    ui_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                           "ui", "app.py")
    with open(ui_path) as f:
        source = f.read()
    for page in ("Editorial Queue", "Pipeline Monitor"):
        if page not in source:
            return False, f"'{page}' page not found in app.py"
    return True, "Editorial Queue and Pipeline Monitor pages found in app.py"


def main():
    print("\n=== Parts 22-23 — Evaluation Monitor Test ===\n")

    all_passed = True
    tests = [
        ("find_stale_hypotheses() returns list",         test_find_stale_returns_list),
        ("URGENT for unscored hypothesis",               test_urgent_for_unscored),
        ("CURRENT for scored hypothesis (no drift)",     test_current_for_scored_no_drift),
        ("Queue excludes CURRENT items",                 test_queue_excludes_current),
        ("Queue sort: URGENT before DRIFT",              test_queue_sort_order),
        ("stamp_signal_at_validation() writes to graph", test_stamp_signal_at_validation),
        ("run_monitor() has required keys",              test_run_monitor_keys),
        ("Monitor counts sum to total",                  test_monitor_counts_sum),
        ("Monitor queue has no CURRENT items",           test_monitor_queue_no_current),
        ("app.py has Editorial Queue + Pipeline Monitor",test_ui_has_new_pages),
    ]

    for label, fn in tests:
        print(f"  Running: {label}...")
        try:
            passed, msg = fn()
        except Exception as e:
            passed, msg = False, str(e)
        icon = PASS if passed else FAIL
        print(f"  {icon} {label}: {msg}")
        if not passed:
            all_passed = False

    print()
    if all_passed:
        print("All monitor tests passed.")
        print("Ready to proceed to Part 24.")
    else:
        print("Some tests failed.")
        import sys
        sys.exit(1)


if __name__ == "__main__":
    main()
