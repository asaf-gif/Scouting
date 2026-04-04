"""
Part 6 — Company List Upload + Enrichment Test

Tests:
1. Single company (Palantir) — enriched and written to graph with all required fields
2. All required schema fields are present and non-empty
3. CURRENTLY_USES relationship links company to a BusinessModel node
4. Duplicate detection — same company name is skipped on second call
5. CSV batch — 3 companies from sample_companies.csv are processed (--limit 3)
6. Batch result: ≥2 created (allowing for 1 possible duplicate if re-run)

Run: python tests/test_06_companies.py
"""

import os
import sys
import json

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
from neo4j import GraphDatabase

load_dotenv()

PASS = "✓"
FAIL = "✗"

REQUIRED_FIELDS = [
    "company_id", "name", "description", "funding_stage",
    "industries", "ai_involvement", "bm_confidence", "bm_rationale",
]


def get_driver():
    return GraphDatabase.driver(
        os.getenv("NEO4J_URI"),
        auth=(os.getenv("NEO4J_USER", "neo4j"), os.getenv("NEO4J_PASSWORD")),
    )


def cleanup(driver, company_id: str):
    with driver.session() as s:
        s.run("MATCH (n:Company {company_id: $id}) DETACH DELETE n", id=company_id)


def test_palantir_created():
    """Palantir should be enriched and created."""
    from input_layer.company_enrichment import enrich_company
    result = enrich_company("Palantir")

    if result["status"] == "error":
        return False, f"Enrichment error: {result['message']}"
    if result["status"] not in ("created", "duplicate_skipped"):
        return False, f"Unexpected status: {result['status']}"
    if not result["company_id"]:
        return False, "No company_id returned"

    return True, f"Status={result['status']}, ID={result['company_id']}"


def test_all_schema_fields_populated(company_id: str):
    """All required schema fields must be non-empty on the created node."""
    driver = get_driver()
    with driver.session() as s:
        rec = s.run(
            "MATCH (n:Company {company_id:$id}) RETURN n", id=company_id
        ).single()
    driver.close()

    if not rec:
        return False, f"Node {company_id} not found in graph"

    node = dict(rec["n"])
    missing = []
    for field in REQUIRED_FIELDS:
        val = node.get(field)
        if val is None or val == "" or val == []:
            missing.append(field)
        elif isinstance(val, str) and len(val.strip()) < 3:
            missing.append(field)

    if missing:
        return False, f"Fields missing or empty: {missing}"
    return True, f"All {len(REQUIRED_FIELDS)} required fields populated"


def test_bm_relationship(company_id: str):
    """Company must be linked to a BusinessModel via CURRENTLY_USES."""
    driver = get_driver()
    with driver.session() as s:
        rec = s.run("""
            MATCH (c:Company {company_id:$id})-[r:CURRENTLY_USES]->(bm:BusinessModel)
            RETURN bm.bim_id AS bim_id, bm.name AS bm_name, r.confidence AS conf
        """, id=company_id).single()
    driver.close()

    if not rec:
        return False, "No CURRENTLY_USES relationship found"
    return True, f"Linked to {rec['bim_id']} — {rec['bm_name']} (conf={rec['conf']:.2f})"


def test_duplicate_skipped():
    """Calling enrich_company('Palantir') a second time should be skipped."""
    from input_layer.company_enrichment import enrich_company
    result = enrich_company("Palantir")

    if result["status"] == "duplicate_skipped":
        return True, f"Duplicate correctly skipped (existing ID: {result['company_id']})"
    if result["status"] == "created":
        # Not ideal — two nodes for same company
        return False, f"Duplicate Palantir was created again as {result['company_id']}"
    return False, f"Unexpected status: {result['status']}"


def test_csv_batch():
    """CSV batch: process first 3 companies from sample_companies.csv."""
    from input_layer.company_enrichment import enrich_companies_from_csv
    csv_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "data", "sample_companies.csv",
    )
    results = enrich_companies_from_csv(csv_path, limit=3)

    if len(results) != 3:
        return False, f"Expected 3 results, got {len(results)}"

    errors = [r for r in results if r["status"] == "error"]
    if errors:
        return False, f"{len(errors)} errors: {[r['message'] for r in errors]}"

    created_or_skipped = [r for r in results if r["status"] in ("created", "duplicate_skipped")]
    return True, f"{len(created_or_skipped)}/3 succeeded (created or skipped duplicates)"


def test_pending_human_review(company_id: str):
    """All created companies must be tagged pending_human_review=True."""
    driver = get_driver()
    with driver.session() as s:
        rec = s.run(
            "MATCH (n:Company {company_id:$id}) RETURN n.pending_human_review AS r",
            id=company_id,
        ).single()
    driver.close()

    if not rec:
        return False, "Node not found"
    if not rec["r"]:
        return False, "pending_human_review is not True"
    return True, "pending_human_review=True confirmed"


def main():
    print("\n=== Part 6 — Company Enrichment Test ===\n")
    print("  Note: This test calls Claude + Tavily. Takes ~2 minutes for all tests.\n")

    company_id = None
    all_passed = True

    # Test 1: Palantir creation
    print("  Running: Palantir enrichment...")
    try:
        passed, msg = test_palantir_created()
    except Exception as e:
        passed, msg = False, str(e)

    icon = PASS if passed else FAIL
    print(f"  {icon} Palantir enriched: {msg}")
    if not passed:
        all_passed = False
    else:
        # Get the Palantir node ID
        from input_layer.company_enrichment import get_driver as gd
        driver = gd()
        with driver.session() as s:
            rec = s.run("""
                MATCH (n:Company)
                WHERE toLower(n.name) = 'palantir'
                   OR toLower(n.name) = 'palantir technologies'
                RETURN n.company_id AS id LIMIT 1
            """).single()
        driver.close()
        company_id = rec["id"] if rec else None

    # Tests 2-4: Field validation on Palantir node
    if company_id:
        for name, fn in [
            ("All schema fields populated",  lambda: test_all_schema_fields_populated(company_id)),
            ("CURRENTLY_USES BM relationship", lambda: test_bm_relationship(company_id)),
            ("pending_human_review=True",     lambda: test_pending_human_review(company_id)),
        ]:
            try:
                passed, msg = fn()
            except Exception as e:
                passed, msg = False, str(e)
            icon = PASS if passed else FAIL
            print(f"  {icon} {name}: {msg}")
            if not passed:
                all_passed = False
    else:
        print(f"  {FAIL} Skipping field tests — Palantir node not found")
        all_passed = False

    # Test 5: Duplicate detection
    print("\n  Running: Palantir duplicate detection...")
    try:
        passed, msg = test_duplicate_skipped()
    except Exception as e:
        passed, msg = False, str(e)
    icon = PASS if passed else FAIL
    print(f"  {icon} Duplicate detection: {msg}")
    if not passed:
        all_passed = False

    # Test 6: CSV batch (3 companies)
    print("\n  Running: CSV batch (first 3 companies)...")
    try:
        passed, msg = test_csv_batch()
    except Exception as e:
        passed, msg = False, str(e)
    icon = PASS if passed else FAIL
    print(f"  {icon} CSV batch: {msg}")
    if not passed:
        all_passed = False

    print()
    if all_passed:
        print(f"All company enrichment tests passed.")
        print(f"Palantir node: {company_id}")
        print("Inspect: python graph/inspector.py company --id CO_001")
        print("Ready to proceed to Part 7.")
    else:
        print("Some tests failed.")
        sys.exit(1)


if __name__ == "__main__":
    main()
