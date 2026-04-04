"""
Part 7 — Technology Entry Test

Tests:
1. RAG enriched and created as TECH_004 with all required fields
2. INFLUENCES relationships written to Scalar nodes (≥3)
3. maturity_level is numeric and in 0-100 range
4. disruption_thesis is non-empty
5. Duplicate detection — same tech skipped on second call

Run: python tests/test_07_tech.py
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv(override=True)

from neo4j import GraphDatabase

PASS = "✓"
FAIL = "✗"

REQUIRED_FIELDS = [
    "tech_id", "name", "short_name", "category", "description",
    "maturity_level", "maturity_source", "disruption_thesis",
]


def get_driver():
    return GraphDatabase.driver(
        os.getenv("NEO4J_URI"),
        auth=(os.getenv("NEO4J_USER", "neo4j"), os.getenv("NEO4J_PASSWORD")),
    )


def test_rag_created():
    from input_layer.tech_enrichment import enrich_technology
    result = enrich_technology("Retrieval-Augmented Generation")

    if result["status"] == "error":
        return False, f"Error: {result['message']}"
    if result["status"] not in ("created", "duplicate_skipped"):
        return False, f"Unexpected status: {result['status']}"
    if not result["tech_id"]:
        return False, "No tech_id returned"
    return True, f"Status={result['status']}, ID={result['tech_id']}"


def test_all_fields_populated(tech_id: str):
    driver = get_driver()
    with driver.session() as s:
        rec = s.run("MATCH (n:Technology {tech_id:$id}) RETURN n", id=tech_id).single()
    driver.close()

    if not rec:
        return False, f"Node {tech_id} not found"

    node = dict(rec["n"])
    missing = []
    for field in REQUIRED_FIELDS:
        val = node.get(field)
        if val is None or val == "" or val == []:
            missing.append(field)

    if missing:
        return False, f"Missing or empty: {missing}"
    return True, f"All {len(REQUIRED_FIELDS)} required fields populated"


def test_scalar_influences(tech_id: str):
    driver = get_driver()
    with driver.session() as s:
        result = s.run("""
            MATCH (t:Technology {tech_id:$id})-[r:INFLUENCES]->(sc:Scalar)
            RETURN sc.scalar_id AS sid, r.direction AS dir, r.impact_strength AS strength
        """, id=tech_id)
        rows = result.data()
    driver.close()

    if len(rows) < 3:
        return False, f"Only {len(rows)} INFLUENCES relationships (need ≥3)"
    return True, f"{len(rows)} INFLUENCES relationships created: {[r['sid'] for r in rows]}"


def test_maturity_in_range(tech_id: str):
    driver = get_driver()
    with driver.session() as s:
        rec = s.run(
            "MATCH (n:Technology {tech_id:$id}) RETURN n.maturity_level AS m",
            id=tech_id,
        ).single()
    driver.close()

    if not rec:
        return False, "Node not found"
    m = rec["m"]
    if not isinstance(m, (int, float)) or not (0 <= m <= 100):
        return False, f"maturity_level={m} not in 0-100 range"
    return True, f"maturity_level={m} (valid)"


def test_disruption_thesis(tech_id: str):
    driver = get_driver()
    with driver.session() as s:
        rec = s.run(
            "MATCH (n:Technology {tech_id:$id}) RETURN n.disruption_thesis AS dt",
            id=tech_id,
        ).single()
    driver.close()

    if not rec or not rec["dt"] or len(rec["dt"].strip()) < 20:
        return False, "disruption_thesis missing or too short"
    return True, f"disruption_thesis present ({len(rec['dt'])} chars)"


def test_duplicate_skipped():
    from input_layer.tech_enrichment import enrich_technology
    result = enrich_technology("RAG")  # short name variant

    if result["status"] == "duplicate_skipped":
        return True, f"Duplicate correctly skipped (existing: {result['tech_id']})"
    if result["status"] == "created":
        return False, f"Duplicate RAG was created as {result['tech_id']}"
    return False, f"Unexpected status: {result['status']}"


def main():
    print("\n=== Part 7 — Technology Entry Test ===\n")
    print("  Note: Calls Claude + Tavily. Takes ~1 minute.\n")

    tech_id = None
    all_passed = True

    # Test 1: RAG creation
    print("  Running: RAG enrichment...")
    try:
        passed, msg = test_rag_created()
    except Exception as e:
        passed, msg = False, str(e)
    icon = PASS if passed else FAIL
    print(f"  {icon} RAG enriched: {msg}")
    if not passed:
        all_passed = False
    else:
        driver = get_driver()
        with driver.session() as s:
            rec = s.run("""
                MATCH (n:Technology)
                WHERE toLower(n.name) CONTAINS 'retrieval'
                   OR toLower(n.short_name) = 'rag'
                RETURN n.tech_id AS id LIMIT 1
            """).single()
        driver.close()
        tech_id = rec["id"] if rec else None

    # Tests 2-5: Node validation
    if tech_id:
        for label, fn in [
            ("All schema fields populated",   lambda: test_all_fields_populated(tech_id)),
            ("INFLUENCES relationships (≥3)",  lambda: test_scalar_influences(tech_id)),
            ("maturity_level in 0-100",        lambda: test_maturity_in_range(tech_id)),
            ("disruption_thesis populated",    lambda: test_disruption_thesis(tech_id)),
        ]:
            try:
                passed, msg = fn()
            except Exception as e:
                passed, msg = False, str(e)
            icon = PASS if passed else FAIL
            print(f"  {icon} {label}: {msg}")
            if not passed:
                all_passed = False
    else:
        print(f"  {FAIL} Skipping validation tests — tech node not found")
        all_passed = False

    # Test 6: Duplicate detection
    print("\n  Running: Duplicate detection (RAG short name)...")
    try:
        passed, msg = test_duplicate_skipped()
    except Exception as e:
        passed, msg = False, str(e)
    icon = PASS if passed else FAIL
    print(f"  {icon} Duplicate detection: {msg}")
    if not passed:
        all_passed = False

    print()
    if all_passed:
        print(f"All technology entry tests passed. Created: {tech_id}")
        print("Ready to proceed to Part 8.")
    else:
        print("Some tests failed.")
        sys.exit(1)


if __name__ == "__main__":
    main()
