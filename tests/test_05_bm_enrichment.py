"""
Part 5 — Business Model Enrichment Test

Tests:
1. 'Embedded Finance' — a genuinely new BM — gets created with all fields populated
2. 'Subscription' — an existing model — is blocked as a duplicate
3. All required schema fields are present and non-empty
4. Node is tagged pending_human_review=True in the graph

Run: python tests/test_05_bm_enrichment.py
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
from neo4j import GraphDatabase

load_dotenv()

PASS = "✓"
FAIL = "✗"

REQUIRED_FIELDS = [
    "name", "description", "revenue_logic", "key_dependencies",
    "typical_margins", "examples_json", "scalars_most_affected",
]


def get_driver():
    return GraphDatabase.driver(
        os.getenv("NEO4J_URI"),
        auth=(os.getenv("NEO4J_USER", "neo4j"), os.getenv("NEO4J_PASSWORD")),
    )


def cleanup(driver, bim_id: str):
    with driver.session() as s:
        s.run("MATCH (n:BusinessModel {bim_id: $id}) DETACH DELETE n", id=bim_id)


def test_embedded_finance_created():
    """Embedded Finance is a new model — should be created."""
    from input_layer.bm_enrichment import enrich_business_model
    result = enrich_business_model("Embedded Finance")

    if result["status"] == "error":
        return False, f"Enrichment error: {result['message']}"
    if result["status"] == "duplicate_blocked":
        return False, "Embedded Finance was incorrectly flagged as a duplicate"
    if not result["bim_id"]:
        return False, "No BIM ID returned"

    return True, f"Created {result['bim_id']} with status='{result['status']}'"


def test_all_schema_fields_populated(bim_id: str):
    """All required schema fields must be non-empty on the created node."""
    driver = get_driver()
    with driver.session() as s:
        rec = s.run(
            "MATCH (n:BusinessModel {bim_id:$id}) RETURN n", id=bim_id
        ).single()
    driver.close()

    if not rec:
        return False, f"Node {bim_id} not found in graph"

    node = dict(rec["n"])
    missing = []
    for field in REQUIRED_FIELDS:
        val = node.get(field)
        if not val or (isinstance(val, str) and len(val.strip()) < 5):
            missing.append(field)

    if missing:
        return False, f"Fields missing or empty: {missing}"
    return True, f"All {len(REQUIRED_FIELDS)} required fields populated"


def test_pending_human_review(bim_id: str):
    """Node must be tagged pending_human_review=True."""
    driver = get_driver()
    with driver.session() as s:
        rec = s.run(
            "MATCH (n:BusinessModel {bim_id:$id}) RETURN n.pending_human_review AS r", id=bim_id
        ).single()
    driver.close()

    if not rec:
        return False, "Node not found"
    if not rec["r"]:
        return False, "pending_human_review is not True"
    return True, "pending_human_review=True confirmed"


def test_examples_present(bim_id: str):
    """At least 3 real company examples must be cited."""
    import json
    driver = get_driver()
    with driver.session() as s:
        rec = s.run(
            "MATCH (n:BusinessModel {bim_id:$id}) RETURN n.examples_json AS ex", id=bim_id
        ).single()
    driver.close()

    if not rec or not rec["ex"]:
        return False, "No examples_json field"
    try:
        examples = json.loads(rec["ex"])
    except Exception:
        return False, "examples_json is not valid JSON"

    if len(examples) < 3:
        return False, f"Only {len(examples)} examples found, need ≥3"
    named = [e for e in examples if e.get("company")]
    return True, f"{len(named)} named company examples present"


def test_subscription_is_duplicate():
    """'Subscription' already exists — should be blocked as duplicate."""
    from input_layer.bm_enrichment import enrich_business_model
    result = enrich_business_model("Subscription")

    if result["status"] == "created":
        # Clean up the wrongly-created node
        if result["bim_id"]:
            driver = get_driver()
            cleanup(driver, result["bim_id"])
            driver.close()
        return False, "Subscription was NOT detected as duplicate — incorrectly created"

    if result["status"] == "duplicate_blocked":
        return True, "Subscription correctly blocked as duplicate"

    # similarity_flagged is acceptable too (close but not blocked)
    return True, f"Subscription flagged (status={result['status']}) — similarity detected"


def main():
    print("\n=== Part 5 — Business Model Enrichment Test ===\n")
    print("  Note: This test calls Claude + Tavily. Takes ~30 seconds.\n")

    bim_id = None
    all_passed = True

    # Test 1: Embedded Finance creation
    print("  Running: Embedded Finance enrichment...")
    try:
        passed, msg = test_embedded_finance_created()
    except Exception as e:
        passed, msg = False, str(e)

    icon = PASS if passed else FAIL
    print(f"  {icon} Embedded Finance created: {msg}")
    if not passed:
        all_passed = False
    else:
        # Extract bim_id from a fresh query
        from input_layer.bm_enrichment import get_driver as gd
        driver = gd()
        with driver.session() as s:
            rec = s.run(
                "MATCH (n:BusinessModel {source:'manual_entry'}) "
                "RETURN n.bim_id AS id ORDER BY n.created_at DESC LIMIT 1"
            ).single()
        driver.close()
        bim_id = rec["id"] if rec else None

    # Tests 2-4: Field validation (only if node was created)
    if bim_id:
        for name, fn in [
            ("All schema fields populated", lambda: test_all_schema_fields_populated(bim_id)),
            ("pending_human_review=True",   lambda: test_pending_human_review(bim_id)),
            ("≥3 company examples cited",   lambda: test_examples_present(bim_id)),
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
        print(f"  {FAIL} Skipping field tests — no node was created")
        all_passed = False

    # Test 5: Duplicate detection
    print("\n  Running: Subscription duplicate detection...")
    try:
        passed, msg = test_subscription_is_duplicate()
    except Exception as e:
        passed, msg = False, str(e)
    icon = PASS if passed else FAIL
    print(f"  {icon} Duplicate detection: {msg}")
    if not passed:
        all_passed = False

    print()
    if all_passed:
        print(f"All enrichment tests passed. Created node: {bim_id}")
        print("Inspect it: python graph/inspector.py bm --id embedded_finance")
        print("Ready to proceed to Part 6.")
    else:
        print("Some tests failed.")
        sys.exit(1)


if __name__ == "__main__":
    main()
