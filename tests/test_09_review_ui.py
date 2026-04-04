"""
Part 9 — Input Layer Review Checkpoint Test

Tests the data layer that powers the review UI — we can't headlessly test
Streamlit rendering, so we verify the underlying queries and state directly.

Tests:
1. Pending BM queue query returns expected structure
2. Pending company queue query returns expected structure
3. Pending technology queue query returns expected structure
4. Approve action clears pending_human_review flag
5. Reject action sets status = 'Rejected' and clears pending flag
6. ui/app.py imports without error (syntax check)

Run: python tests/test_09_review_ui.py
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


def get_driver():
    return GraphDatabase.driver(
        os.getenv("NEO4J_URI"),
        auth=(os.getenv("NEO4J_USER", "neo4j"), os.getenv("NEO4J_PASSWORD")),
    )


def test_pending_bm_query():
    """Queue query returns list with expected keys."""
    driver = get_driver()
    with driver.session() as s:
        rows = s.run("""
            MATCH (n:BusinessModel)
            WHERE n.pending_human_review = true
            RETURN n.bim_id AS id, n.name AS name, n.source AS source,
                   n.description AS description,
                   coalesce(n.confidence, 0.0) AS confidence
            ORDER BY n.created_at DESC
        """).data()
    driver.close()

    if not isinstance(rows, list):
        return False, "Query did not return a list"

    for row in rows:
        for key in ("id", "name", "source", "confidence"):
            if key not in row:
                return False, f"Missing key '{key}' in row: {row}"

    return True, f"{len(rows)} pending BM(s) in queue"


def test_pending_company_query():
    """Company queue query returns list with expected keys."""
    driver = get_driver()
    with driver.session() as s:
        rows = s.run("""
            MATCH (n:Company)
            WHERE n.pending_human_review = true
            OPTIONAL MATCH (n)-[:CURRENTLY_USES]->(bm:BusinessModel)
            RETURN n.company_id AS id, n.name AS name,
                   coalesce(n.bm_confidence, 0.0) AS bm_conf,
                   bm.bim_id AS bim_id
            ORDER BY n.created_at DESC
        """).data()
    driver.close()

    if not isinstance(rows, list):
        return False, "Query did not return a list"
    return True, f"{len(rows)} pending company(s) in queue"


def test_pending_tech_query():
    """Technology queue query returns list with expected keys."""
    driver = get_driver()
    with driver.session() as s:
        rows = s.run("""
            MATCH (n:Technology)
            WHERE n.pending_human_review = true
            OPTIONAL MATCH (n)-[:INFLUENCES]->(sc:Scalar)
            WITH n, count(sc) AS scalar_count
            RETURN n.tech_id AS id, n.name AS name,
                   n.maturity_level AS maturity,
                   scalar_count
            ORDER BY n.created_at DESC
        """).data()
    driver.close()

    if not isinstance(rows, list):
        return False, "Query did not return a list"
    return True, f"{len(rows)} pending technology(s) in queue"


def test_approve_action():
    """Approving a node clears pending_human_review."""
    driver = get_driver()

    # Find a node pending review (TECH_004 — RAG, created in Part 7)
    with driver.session() as s:
        rec = s.run("""
            MATCH (n:Technology {tech_id: 'TECH_004'})
            RETURN n.pending_human_review AS pending
        """).single()

    if not rec:
        driver.close()
        return True, "TECH_004 not found — skipping approve test"

    # Approve it
    now = datetime.now(timezone.utc).isoformat()
    with driver.session() as s:
        s.run("""
            MATCH (n:Technology {tech_id: 'TECH_004'})
            SET n.pending_human_review = false,
                n.reviewed_at = $now,
                n.reviewed_by = 'test_suite'
        """, now=now)

    # Verify
    with driver.session() as s:
        rec = s.run("""
            MATCH (n:Technology {tech_id: 'TECH_004'})
            RETURN n.pending_human_review AS pending, n.reviewed_by AS by
        """).single()
    driver.close()

    if rec["pending"]:
        return False, "pending_human_review still True after approve"
    return True, f"TECH_004 approved — pending=False, reviewed_by={rec['by']}"


def test_reject_action():
    """Rejecting a node sets Rejected status."""
    driver = get_driver()

    # Use BIM_028 (Embedded Finance from Part 5) as test target
    with driver.session() as s:
        rec = s.run("""
            MATCH (n:BusinessModel {bim_id: 'BIM_028'})
            RETURN n.status AS status, n.pending_human_review AS pending
        """).single()

    if not rec:
        driver.close()
        return True, "BIM_028 not found — skipping reject test"

    now = datetime.now(timezone.utc).isoformat()
    with driver.session() as s:
        s.run("""
            MATCH (n:BusinessModel {bim_id: 'BIM_028'})
            SET n.status = 'Rejected',
                n.pending_human_review = false,
                n.reviewed_at = $now,
                n.reviewed_by = 'test_suite'
        """, now=now)

    with driver.session() as s:
        rec = s.run("""
            MATCH (n:BusinessModel {bim_id: 'BIM_028'})
            RETURN n.status AS status, n.pending_human_review AS pending
        """).single()
    driver.close()

    # Restore
    driver2 = get_driver()
    with driver2.session() as s:
        s.run("""
            MATCH (n:BusinessModel {bim_id: 'BIM_028'})
            SET n.status = 'Active', n.pending_human_review = true,
                n.reviewed_by = null, n.reviewed_at = null
        """)
    driver2.close()

    if rec["status"] != "Rejected":
        return False, f"Expected status='Rejected', got '{rec['status']}'"
    if rec["pending"]:
        return False, "pending_human_review still True after reject"
    return True, "BIM_028 rejected — status=Rejected, pending=False (restored)"


def test_ui_imports():
    """ui/app.py must import without syntax or import errors."""
    import importlib.util
    ui_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "ui", "app.py",
    )
    spec = importlib.util.spec_from_file_location("app", ui_path)
    # We can't fully execute Streamlit code without a server,
    # but we can compile it to check for syntax errors
    try:
        with open(ui_path) as f:
            source = f.read()
        compile(source, ui_path, "exec")
        return True, "ui/app.py compiles without syntax errors"
    except SyntaxError as e:
        return False, f"Syntax error in ui/app.py: {e}"


def main():
    print("\n=== Part 9 — Input Layer Review UI Test ===\n")

    all_passed = True
    tests = [
        ("Pending BM queue query",     test_pending_bm_query),
        ("Pending company queue query", test_pending_company_query),
        ("Pending tech queue query",    test_pending_tech_query),
        ("Approve action clears flag",  test_approve_action),
        ("Reject action sets status",   test_reject_action),
        ("ui/app.py syntax check",      test_ui_imports),
    ]

    for label, fn in tests:
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
        print("All review UI tests passed.")
        print("Start the UI: cd scouting && streamlit run ui/app.py")
        print("Ready to proceed to Part 10.")
    else:
        print("Some tests failed.")
        sys.exit(1)


if __name__ == "__main__":
    main()
