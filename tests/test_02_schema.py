"""
Part 2 — Graph Schema Test

Verifies all 11 constraints and 15 indexes are active, and that
the duplicate ID constraint is enforced.

Run: python tests/test_02_schema.py
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
from neo4j import GraphDatabase
from neo4j.exceptions import ConstraintError

load_dotenv()

PASS = "✓"
FAIL = "✗"


def get_driver():
    return GraphDatabase.driver(
        os.getenv("NEO4J_URI"),
        auth=(os.getenv("NEO4J_USER", "neo4j"), os.getenv("NEO4J_PASSWORD")),
    )


def test_constraint_count(driver):
    """11 uniqueness constraints must exist."""
    expected = {
        "bm_id_unique", "vector_id_unique", "scalar_id_unique",
        "company_id_unique", "industry_id_unique", "technology_id_unique",
        "hypothesis_id_unique", "evidence_id_unique", "evaluation_id_unique",
        "review_item_id_unique", "compression_log_id_unique",
    }
    with driver.session() as session:
        active = {r["name"] for r in session.run("SHOW CONSTRAINTS").data()}
    missing = expected - active
    if missing:
        return False, f"Missing constraints: {missing}"
    return True, f"All 11 constraints active"


def test_index_count(driver):
    """15 named indexes must exist (excluding built-in LOOKUP indexes)."""
    expected = {
        "bm_name", "bm_status", "company_name", "company_monitoring_status",
        "industry_name", "technology_name", "technology_tracking_status",
        "hypothesis_status", "hypothesis_confidence", "hypothesis_updated_at",
        "evidence_source_type", "evidence_status", "evaluation_alert_status",
        "review_item_status", "review_item_priority",
    }
    with driver.session() as session:
        active = {r["name"] for r in session.run("SHOW INDEXES").data()}
    missing = expected - active
    if missing:
        return False, f"Missing indexes: {missing}"
    return True, f"All 15 indexes active"


def test_all_node_labels(driver):
    """Verify all 11 node labels are represented in the schema."""
    expected_labels = {
        "BusinessModel", "TransformationVector", "Scalar",
        "Company", "Industry", "Technology",
        "DisruptionHypothesis", "Evidence", "Evaluation",
        "HumanReviewItem", "CompressionLog",
    }
    with driver.session() as session:
        constraint_data = session.run("SHOW CONSTRAINTS YIELD labelsOrTypes").data()
    labels_in_schema = set()
    for row in constraint_data:
        for label in row.get("labelsOrTypes", []):
            labels_in_schema.add(label)
    missing = expected_labels - labels_in_schema
    if missing:
        return False, f"Labels missing from constraints: {missing}"
    return True, f"All 11 node labels present in schema"


def test_duplicate_id_rejected(driver):
    """Creating two BusinessModel nodes with the same bim_id must fail."""
    with driver.session() as session:
        session.run("MATCH (n:BusinessModel {bim_id: '__dup_test__'}) DELETE n")
        session.run("CREATE (:BusinessModel {bim_id: '__dup_test__', name: 'Original'})")
        try:
            session.run("CREATE (:BusinessModel {bim_id: '__dup_test__', name: 'Duplicate'})")
            session.run("MATCH (n:BusinessModel {bim_id: '__dup_test__'}) DELETE n")
            return False, "Duplicate ID was NOT rejected — constraint not enforced"
        except ConstraintError:
            session.run("MATCH (n:BusinessModel {bim_id: '__dup_test__'}) DELETE n")
            return True, "Duplicate ID correctly raised ConstraintError"


def test_relationship_types_documented():
    """Verify all 15 relationship types are documented in schema.cypher."""
    schema_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "graph", "schema.cypher")
    with open(schema_path) as f:
        content = f.read()

    expected_rels = [
        "HAS_TRANSITION", "FROM_BIM", "TO_BIM", "DRIVES", "IMPACTS",
        "CLASSIFIES", "BELONGS_TO", "TRIGGERS", "TARGETS", "PREDICTS",
        "ADVANCES_TO", "MONITORS", "DEMONSTRATES", "SUPPORTS", "REVIEWS",
    ]
    missing = [r for r in expected_rels if r not in content]
    if missing:
        return False, f"Relationship types not documented: {missing}"
    return True, f"All 15 relationship types documented in schema.cypher"


def main():
    print("\n=== Part 2 — Graph Schema Test ===\n")
    driver = get_driver()

    tests = [
        ("11 uniqueness constraints", lambda: test_constraint_count(driver)),
        ("15 indexes active", lambda: test_index_count(driver)),
        ("11 node labels in schema", lambda: test_all_node_labels(driver)),
        ("Duplicate ID rejected", lambda: test_duplicate_id_rejected(driver)),
        ("15 relationship types documented", test_relationship_types_documented),
    ]

    all_passed = True
    for name, fn in tests:
        try:
            passed, msg = fn()
        except Exception as e:
            passed, msg = False, str(e)
        icon = PASS if passed else FAIL
        print(f"  {icon} {name}: {msg}")
        if not passed:
            all_passed = False

    driver.close()
    print()
    if all_passed:
        print("All schema tests passed. Ready to proceed to Part 3.")
    else:
        print("Some tests failed. Run 'python graph/schema.py' to apply the schema first.")
        sys.exit(1)


if __name__ == "__main__":
    main()
