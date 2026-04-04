"""
Parts 24-25 — Human Editorial Interface Test

Tests:
1.  Editorial note can be written to a hypothesis node
2.  editorial_priority field is settable
3.  Status override (set to Validated by editorial decision)
4.  Editorial Queue UI query returns hypothesis with editorial fields
5.  app.py Editorial Queue page has note saving logic
6.  app.py Editorial Queue page has priority selector
7.  app.py Editorial Queue page has status change buttons
8.  app.py Pipeline Monitor page has run_pipeline call
9.  Editorial fields persist after write (read-back check)
10. Hypothesis count in editorial query matches graph

Run: python tests/test_24_25_editorial.py
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv(override=True)

from neo4j import GraphDatabase
from datetime import datetime, timezone

PASS = "✓"
FAIL = "✗"
HYP_ID = "HYP_BIM_026_BIM_027"
TEST_ID = "HYP_TEST_EDITORIAL_001"


def get_driver():
    return GraphDatabase.driver(
        os.getenv("NEO4J_URI"),
        auth=(os.getenv("NEO4J_USER", "neo4j"), os.getenv("NEO4J_PASSWORD")),
    )


def setup_test_hypothesis(driver):
    with driver.session() as s:
        s.run("""
            MERGE (h:DisruptionHypothesis {hypothesis_id: $hid})
            SET h.conviction_score = 0.65,
                h.title = 'Editorial test hypothesis',
                h.status = 'Hypothesis',
                h.thesis = 'A test thesis.',
                h.counter_argument = 'A counter argument.'
        """, hid=TEST_ID)


def teardown_test_hypothesis(driver):
    with driver.session() as s:
        s.run("MATCH (h:DisruptionHypothesis {hypothesis_id: $hid}) DELETE h", hid=TEST_ID)


def test_write_editorial_note():
    driver = get_driver()
    setup_test_hypothesis(driver)

    now = datetime.now(timezone.utc).isoformat()
    with driver.session() as s:
        s.run("""
            MATCH (h:DisruptionHypothesis {hypothesis_id: $hid})
            SET h.editorial_note = $note,
                h.editorial_updated_at = $now,
                h.editorial_updated_by = 'test'
        """, hid=TEST_ID, note="Strong signal — worth watching", now=now)

    with driver.session() as s:
        rec = s.run("""
            MATCH (h:DisruptionHypothesis {hypothesis_id: $hid})
            RETURN h.editorial_note AS note
        """, hid=TEST_ID).single()

    note = rec["note"] if rec else None
    teardown_test_hypothesis(driver)
    driver.close()

    if note != "Strong signal — worth watching":
        return False, f"Note not written correctly: '{note}'"
    return True, f"Editorial note written and read back: '{note}'"


def test_write_editorial_priority():
    driver = get_driver()
    setup_test_hypothesis(driver)

    with driver.session() as s:
        s.run("""
            MATCH (h:DisruptionHypothesis {hypothesis_id: $hid})
            SET h.editorial_priority = 'high'
        """, hid=TEST_ID)

    with driver.session() as s:
        rec = s.run("""
            MATCH (h:DisruptionHypothesis {hypothesis_id: $hid})
            RETURN h.editorial_priority AS pri
        """, hid=TEST_ID).single()

    pri = rec["pri"] if rec else None
    teardown_test_hypothesis(driver)
    driver.close()

    if pri != "high":
        return False, f"Priority not written: '{pri}'"
    return True, f"Editorial priority set to: '{pri}'"


def test_status_override():
    driver = get_driver()
    setup_test_hypothesis(driver)

    now = datetime.now(timezone.utc).isoformat()
    with driver.session() as s:
        s.run("""
            MATCH (h:DisruptionHypothesis {hypothesis_id: $hid})
            SET h.status = 'Validated',
                h.editorial_status_set_at = $now,
                h.editorial_status_set_by = 'test'
        """, hid=TEST_ID, now=now)

    with driver.session() as s:
        rec = s.run("""
            MATCH (h:DisruptionHypothesis {hypothesis_id: $hid})
            RETURN h.status AS status
        """, hid=TEST_ID).single()

    status = rec["status"] if rec else None
    teardown_test_hypothesis(driver)
    driver.close()

    if status != "Validated":
        return False, f"Status not updated: '{status}'"
    return True, f"Status override written: '{status}'"


def test_editorial_queue_query():
    """Run the Editorial Queue's main query and check structure."""
    driver = get_driver()
    with driver.session() as s:
        rows = s.run("""
            MATCH (h:DisruptionHypothesis)
            OPTIONAL MATCH (h)-[:GENERATED_FROM]->(v:TransformationVector)
            OPTIONAL MATCH (ev:Evaluation)-[:EVALUATES]->(h)
            WITH h, v, count(DISTINCT ev) AS eval_count
            RETURN h.hypothesis_id        AS hid,
                   h.title                AS title,
                   h.status               AS status,
                   h.conviction_score     AS conviction,
                   h.validation_score     AS validation,
                   h.editorial_note       AS editorial_note,
                   h.editorial_priority   AS editorial_priority,
                   v.signal_strength      AS signal,
                   eval_count
            ORDER BY
                CASE WHEN h.validation_score IS NULL THEN 0 ELSE 1 END ASC,
                h.conviction_score DESC
        """).data()
    driver.close()

    if not rows:
        return False, "Editorial queue query returned no rows"
    row = rows[0]
    required = {"hid", "title", "status", "conviction", "validation", "eval_count"}
    missing = required - row.keys()
    if missing:
        return False, f"Missing fields: {missing}"
    return True, f"Editorial queue query: {len(rows)} rows, structure correct"


def test_ui_note_save_logic():
    ui_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                           "ui", "app.py")
    with open(ui_path) as f:
        source = f.read()
    checks = [
        "editorial_note",
        "Save note",
        "editorial_updated_at",
    ]
    for check in checks:
        if check not in source:
            return False, f"'{check}' not found in app.py"
    return True, "Note save logic found in Editorial Queue page"


def test_ui_priority_selector():
    ui_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                           "ui", "app.py")
    with open(ui_path) as f:
        source = f.read()
    if "editorial_priority" not in source:
        return False, "'editorial_priority' not found in app.py"
    if "high" not in source or "medium" not in source:
        return False, "Priority options not found in app.py"
    return True, "Priority selector found in Editorial Queue page"


def test_ui_status_buttons():
    ui_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                           "ui", "app.py")
    with open(ui_path) as f:
        source = f.read()
    for status in ("Validated", "Contested"):
        if f"editorial_status_set_by" not in source:
            return False, "editorial_status_set_by not found in app.py"
    return True, "Status change buttons found in Editorial Queue page"


def test_ui_pipeline_monitor():
    ui_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                           "ui", "app.py")
    with open(ui_path) as f:
        source = f.read()
    checks = ["Pipeline Monitor", "run_pipeline", "stage_health", "Staleness report"]
    for check in checks:
        if check not in source:
            return False, f"'{check}' not found in Pipeline Monitor page"
    return True, "Pipeline Monitor page has all required components"


def test_editorial_fields_persist():
    """Write multiple editorial fields and verify all persist."""
    driver = get_driver()
    setup_test_hypothesis(driver)
    now = datetime.now(timezone.utc).isoformat()

    with driver.session() as s:
        s.run("""
            MATCH (h:DisruptionHypothesis {hypothesis_id: $hid})
            SET h.editorial_note = 'Test note',
                h.editorial_priority = 'medium',
                h.editorial_updated_at = $now,
                h.editorial_updated_by = 'test',
                h.status = 'Contested',
                h.editorial_status_set_at = $now,
                h.editorial_status_set_by = 'test'
        """, hid=TEST_ID, now=now)

    with driver.session() as s:
        rec = s.run("""
            MATCH (h:DisruptionHypothesis {hypothesis_id: $hid})
            RETURN h.editorial_note AS note,
                   h.editorial_priority AS pri,
                   h.status AS status
        """, hid=TEST_ID).single()

    teardown_test_hypothesis(driver)
    driver.close()

    if not rec:
        return False, "Hypothesis not found after write"
    errors = []
    if rec["note"] != "Test note":
        errors.append(f"note='{rec['note']}'")
    if rec["pri"] != "medium":
        errors.append(f"pri='{rec['pri']}'")
    if rec["status"] != "Contested":
        errors.append(f"status='{rec['status']}'")
    if errors:
        return False, "Fields not persisted: " + ", ".join(errors)
    return True, "All editorial fields persisted correctly"


def test_hypothesis_count_consistent():
    """Query count from editorial queue matches direct MATCH count."""
    driver = get_driver()
    with driver.session() as s:
        total = s.run("MATCH (h:DisruptionHypothesis) RETURN count(h) AS cnt").single()["cnt"]
        queue_rows = s.run("""
            MATCH (h:DisruptionHypothesis)
            OPTIONAL MATCH (h)-[:GENERATED_FROM]->(v:TransformationVector)
            OPTIONAL MATCH (ev:Evaluation)-[:EVALUATES]->(h)
            WITH h, v, count(DISTINCT ev) AS eval_count
            RETURN count(h) AS cnt
        """).single()["cnt"]
    driver.close()

    if total != queue_rows:
        return False, f"Count mismatch: direct={total}, queue={queue_rows}"
    return True, f"Hypothesis count consistent: {total} in both queries"


def main():
    print("\n=== Parts 24-25 — Human Editorial Interface Test ===\n")

    all_passed = True
    tests = [
        ("Write editorial note to hypothesis",       test_write_editorial_note),
        ("Write editorial_priority field",           test_write_editorial_priority),
        ("Status override (editorial decision)",     test_status_override),
        ("Editorial Queue query structure",          test_editorial_queue_query),
        ("app.py has note save logic",               test_ui_note_save_logic),
        ("app.py has priority selector",             test_ui_priority_selector),
        ("app.py has status change buttons",         test_ui_status_buttons),
        ("app.py has Pipeline Monitor page",         test_ui_pipeline_monitor),
        ("Editorial fields persist after write",     test_editorial_fields_persist),
        ("Hypothesis count consistent",              test_hypothesis_count_consistent),
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
        print("All editorial interface tests passed.")
        print("Ready to proceed to Part 26.")
    else:
        print("Some tests failed.")
        import sys
        sys.exit(1)


if __name__ == "__main__":
    main()
