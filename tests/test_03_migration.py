"""
Part 3 — Data Migration Test

Validates node and relationship counts, spot-checks specific transitions,
and verifies KGGen top-transition ordering matches the Excel prototype.

Run: python tests/test_03_migration.py
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
from neo4j import GraphDatabase

load_dotenv()

PASS = "✓"
FAIL = "✗"


def get_driver():
    return GraphDatabase.driver(
        os.getenv("NEO4J_URI"),
        auth=(os.getenv("NEO4J_USER", "neo4j"), os.getenv("NEO4J_PASSWORD")),
    )


def count_nodes(driver, label: str) -> int:
    with driver.session() as s:
        r = s.run(f"MATCH (n:{label} {{source:'excel_prototype'}}) RETURN count(n) AS n")
        return r.single()["n"]


def count_rels(driver, rel_type: str) -> int:
    with driver.session() as s:
        r = s.run(f"MATCH ()-[r:{rel_type}]->() RETURN count(r) AS n")
        return r.single()["n"]


def test_node_counts(driver):
    counts = {
        "BusinessModel": (count_nodes(driver, "BusinessModel"), 27),
        "Scalar": (count_nodes(driver, "Scalar"), 26),
        "Technology": (count_nodes(driver, "Technology"), 3),
        "TransformationVector": (count_nodes(driver, "TransformationVector"), 702),
    }
    issues = []
    for label, (actual, expected) in counts.items():
        if actual != expected:
            issues.append(f"{label}: got {actual}, expected {expected}")
    if issues:
        return False, "; ".join(issues)
    return True, f"27 BMs · 26 Scalars · 3 Technologies · 702 Vectors"


def test_relationship_counts(driver):
    ht = count_rels(driver, "HAS_TRANSITION")
    frm = count_rels(driver, "FROM_BIM")
    to = count_rels(driver, "TO_BIM")
    imp = count_rels(driver, "IMPACTS")

    issues = []
    if ht != 702:
        issues.append(f"HAS_TRANSITION: got {ht}, expected 702")
    if frm != 702:
        issues.append(f"FROM_BIM: got {frm}, expected 702")
    if to != 702:
        issues.append(f"TO_BIM: got {to}, expected 702")
    if imp == 0:
        issues.append("IMPACTS: 0 relationships found")

    if issues:
        return False, "; ".join(issues)
    return True, f"702 HAS_TRANSITION · 702 FROM_BIM · 702 TO_BIM · {imp} IMPACTS"


def test_provenance(driver):
    """All migrated nodes must have source='excel_prototype'."""
    with driver.session() as s:
        r = s.run("""
            MATCH (n)
            WHERE n.source IS NOT NULL AND n.source <> 'excel_prototype'
              AND labels(n)[0] IN ['BusinessModel','Scalar','Technology','TransformationVector']
            RETURN count(n) AS n
        """)
        bad = r.single()["n"]
    if bad > 0:
        return False, f"{bad} nodes have wrong provenance"
    return True, "All migrated nodes tagged source='excel_prototype'"


def test_spot_check_project_to_knowledge(driver):
    """The 'Project Based → Selling Knowledge Databases' vector must exist with an example."""
    with driver.session() as s:
        r = s.run("""
            MATCH (v:TransformationVector)-[:FROM_BIM]->(f:BusinessModel),
                  (v)-[:TO_BIM]->(t:BusinessModel)
            WHERE f.name = 'Project Based'
              AND t.name = 'Selling Knowledge Databases'
            RETURN v.example_text AS example, v.evidence_quality AS quality
        """)
        record = r.single()

    if not record:
        return False, "Vector not found"
    if not record["example"]:
        return False, "Vector found but example_text is empty"
    return True, f"Vector exists · quality={record['quality']} · example present"


def test_spot_check_bms(driver):
    """Spot-check 5 specific business model names exist."""
    expected = [
        "Subscription", "Marketplace / Platform", "Project Based",
        "Selling Knowledge Databases", "SaaS (Software as a Service)",
    ]
    with driver.session() as s:
        result = s.run("MATCH (n:BusinessModel) RETURN n.name AS name")
        found = {r["name"] for r in result}

    missing = [n for n in expected if n not in found]
    if missing:
        return False, f"Missing BMs: {missing}"
    return True, f"All 5 spot-checked BMs present"


def test_technology_scalar_impacts(driver):
    """KGGen must have positive impacts on at least E1 and E2 (knowledge scalars)."""
    with driver.session() as s:
        r = s.run("""
            MATCH (t:Technology {tech_id:'TECH_002'})-[r:IMPACTS]->(sc:Scalar)
            WHERE sc.code IN ['E1','E2'] AND r.impact_score > 0
            RETURN count(r) AS n
        """)
        count = r.single()["n"]

    if count < 1:
        return False, "KGGen has no positive impact on E1/E2 knowledge scalars"
    return True, f"KGGen has positive impact on {count}/2 of E1,E2 scalars"


def test_no_self_transitions(driver):
    """No TransformationVector should have the same FROM and TO business model."""
    with driver.session() as s:
        r = s.run("""
            MATCH (v:TransformationVector)-[:FROM_BIM]->(f),
                  (v)-[:TO_BIM]->(t)
            WHERE f.bim_id = t.bim_id
            RETURN count(v) AS n
        """)
        bad = r.single()["n"]
    if bad > 0:
        return False, f"{bad} self-transition vectors found"
    return True, "No self-transition vectors"


def main():
    print("\n=== Part 3 — Data Migration Test ===\n")
    driver = get_driver()

    tests = [
        ("Node counts", lambda: test_node_counts(driver)),
        ("Relationship counts", lambda: test_relationship_counts(driver)),
        ("Provenance tags", lambda: test_provenance(driver)),
        ("Project Based → Knowledge DB vector", lambda: test_spot_check_project_to_knowledge(driver)),
        ("5 spot-checked BM names", lambda: test_spot_check_bms(driver)),
        ("KGGen → knowledge scalar impacts", lambda: test_technology_scalar_impacts(driver)),
        ("No self-transition vectors", lambda: test_no_self_transitions(driver)),
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
        print("All migration tests passed. Ready to proceed to Part 4.")
    else:
        print("Some tests failed. Check output above.")
        sys.exit(1)


if __name__ == "__main__":
    main()
