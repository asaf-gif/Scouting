"""
Part 28 — Final System Test

End-to-end health check of the complete Systematic Problem Scouting system.

Tests:
1.  Graph has all required node types (11 labels)
2.  Core node counts correct (≥27 BMs, ≥26 Scalars, ≥3 Technologies, ≥702 Vectors)
3.  All pipeline modules importable
4.  At least one DisruptionHypothesis exists with conviction_score ≥ 0.5
5.  At least one Evaluation node exists (EVALUATES relationship)
6.  validation_score written on at least one hypothesis
7.  signal_strength written on at least one vector
8.  opportunity_score written on at least one vector
9.  At least one trend found (Scalar with trend_direction)
10. End-to-end pipeline chain runs (aggregate → trends → rank → monitor → score → health)
11. Inspector CLI importable and has required commands
12. All test modules importable (regression check)

Run: python tests/test_28_final.py
"""

import os
import sys
import importlib

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv(override=True)

from neo4j import GraphDatabase

PASS = "✓"
FAIL = "✗"


def get_driver():
    return GraphDatabase.driver(
        os.getenv("NEO4J_URI"),
        auth=(os.getenv("NEO4J_USER", "neo4j"), os.getenv("NEO4J_PASSWORD")),
    )


def test_all_node_labels():
    driver = get_driver()
    with driver.session() as s:
        labels = {r["label"] for r in s.run(
            "CALL db.labels() YIELD label RETURN label"
        ).data()}
    driver.close()

    required = {
        "BusinessModel", "TransformationVector", "Scalar", "Technology",
        "Company", "Evidence", "DisruptionHypothesis", "Evaluation",
        "CompressionLog",
    }
    missing = required - labels
    if missing:
        return False, f"Missing node labels: {missing}"
    return True, f"All required node labels present ({len(required)} checked)"


def test_core_counts():
    driver = get_driver()
    with driver.session() as s:
        counts = {r["label"]: r["cnt"] for r in s.run("""
            MATCH (n)
            RETURN labels(n)[0] AS label, count(n) AS cnt
        """).data()}
    driver.close()

    checks = [
        ("BusinessModel",         27,  "business models"),
        ("Scalar",                26,  "scalars"),
        ("Technology",             3,  "technologies"),
        ("TransformationVector", 702,  "vectors"),
        ("DisruptionHypothesis",   1,  "hypotheses"),
        ("Evidence",               1,  "evidence nodes"),
        ("Evaluation",             2,  "evaluation nodes"),
    ]
    failures = []
    for label, min_count, name in checks:
        actual = counts.get(label, 0)
        if actual < min_count:
            failures.append(f"{label}={actual} (expected ≥{min_count})")

    if failures:
        return False, "Low counts: " + ", ".join(failures)
    summary = "  ".join(f"{l}={counts.get(l,0)}" for l, _, _ in checks[:4])
    return True, f"Core counts OK — {summary}"


def test_modules_importable():
    modules = [
        "input_layer.bm_enrichment",
        "input_layer.company_enrichment",
        "input_layer.tech_enrichment",
        "input_layer.bm_scanner",
        "extraction.vector_extractor",
        "extraction.scalar_classifier",
        "extraction.hypothesis_generator",
        "analysis.signal_aggregator",
        "analysis.trend_detector",
        "analysis.opportunity_ranker",
        "research.deep_researcher",
        "research.validation_scorer",
        "evaluation.monitor",
        "orchestrator.pipeline",
        "graph.inspector",
    ]
    failures = []
    for mod in modules:
        try:
            importlib.import_module(mod)
        except Exception as e:
            failures.append(f"{mod}: {e}")
    if failures:
        return False, f"{len(failures)} import failure(s): {failures[0]}"
    return True, f"All {len(modules)} pipeline modules importable"


def test_hypothesis_with_conviction():
    driver = get_driver()
    with driver.session() as s:
        rec = s.run("""
            MATCH (h:DisruptionHypothesis)
            WHERE h.conviction_score >= 0.5
            RETURN h.hypothesis_id AS hid, h.conviction_score AS conv
            ORDER BY h.conviction_score DESC LIMIT 1
        """).single()
    driver.close()

    if not rec:
        return False, "No hypothesis with conviction_score ≥ 0.5"
    return True, f"Best hypothesis: {rec['hid']} (conviction={rec['conv']:.2f})"


def test_evaluation_nodes():
    driver = get_driver()
    with driver.session() as s:
        count = s.run("""
            MATCH (ev:Evaluation)-[:EVALUATES]->(h:DisruptionHypothesis)
            RETURN count(ev) AS cnt
        """).single()["cnt"]
    driver.close()

    if count == 0:
        return False, "No Evaluation nodes with EVALUATES relationship"
    return True, f"{count} Evaluation node(s) linked via EVALUATES"


def test_validation_score_written():
    driver = get_driver()
    with driver.session() as s:
        count = s.run("""
            MATCH (h:DisruptionHypothesis)
            WHERE h.validation_score IS NOT NULL
            RETURN count(h) AS cnt
        """).single()["cnt"]
    driver.close()

    if count == 0:
        return False, "No hypotheses with validation_score"
    return True, f"{count} hypothesis(es) with validation_score written"


def test_signal_strength_written():
    driver = get_driver()
    with driver.session() as s:
        count = s.run("""
            MATCH (v:TransformationVector)
            WHERE v.signal_strength IS NOT NULL AND v.signal_strength > 0
            RETURN count(v) AS cnt
        """).single()["cnt"]
    driver.close()

    if count == 0:
        return False, "No vectors with signal_strength > 0"
    return True, f"{count} vector(s) with signal_strength written"


def test_opportunity_score_written():
    driver = get_driver()
    with driver.session() as s:
        count = s.run("""
            MATCH (v:TransformationVector)
            WHERE v.opportunity_score IS NOT NULL
            RETURN count(v) AS cnt
        """).single()["cnt"]
    driver.close()

    if count == 0:
        return False, "No vectors with opportunity_score"
    return True, f"{count} vector(s) with opportunity_score written"


def test_trends_detected():
    driver = get_driver()
    with driver.session() as s:
        count = s.run("""
            MATCH (s:Scalar)
            WHERE s.trend_direction IS NOT NULL
            RETURN count(s) AS cnt
        """).single()["cnt"]
    driver.close()

    if count == 0:
        return False, "No Scalars with trend_direction"
    return True, f"{count} scalar(s) with trend detected"


def test_end_to_end_pipeline():
    from orchestrator.pipeline import run_pipeline
    result = run_pipeline(
        stages=["aggregate", "trends", "rank", "monitor", "score", "health"],
        dry_run=True,
    )
    if not result["success"]:
        return False, f"Pipeline failed: {result['errors']}"
    return True, f"E2E pipeline: {len(result['stages_run'])} stages OK — {', '.join(result['stages_run'])}"


def test_inspector_commands():
    from graph.inspector import COMMANDS
    required_commands = {"bm", "vector", "top-transitions", "hypothesis", "top-opportunities"}
    missing = required_commands - COMMANDS.keys()
    if missing:
        return False, f"Missing inspector commands: {missing}"
    return True, f"Inspector has all {len(required_commands)} required commands"


def test_all_test_modules():
    test_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)))
    test_files = [f for f in os.listdir(test_dir)
                  if f.startswith("test_") and f.endswith(".py")]
    failures = []
    for fname in sorted(test_files):
        if fname == "test_28_final.py":
            continue
        try:
            spec = importlib.util.spec_from_file_location(
                fname[:-3],
                os.path.join(test_dir, fname)
            )
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
        except Exception as e:
            failures.append(f"{fname}: {e}")
    if failures:
        return False, f"{len(failures)} test file(s) failed to import: {failures[0]}"
    return True, f"All {len(test_files)} test modules importable"


def main():
    print("\n=== Part 28 — Final System Test ===\n")
    print("  Complete end-to-end health check of all 32 parts.\n")

    all_passed = True
    tests = [
        ("All node labels present (11 types)",          test_all_node_labels),
        ("Core node counts correct",                    test_core_counts),
        ("All pipeline modules importable",             test_modules_importable),
        ("Hypothesis with conviction ≥ 0.5 exists",    test_hypothesis_with_conviction),
        ("Evaluation nodes with EVALUATES rel",        test_evaluation_nodes),
        ("validation_score written on hypothesis",      test_validation_score_written),
        ("signal_strength written on vectors",          test_signal_strength_written),
        ("opportunity_score written on vectors",        test_opportunity_score_written),
        ("Scalar trends detected",                      test_trends_detected),
        ("End-to-end pipeline (6 stages)",              test_end_to_end_pipeline),
        ("Inspector has all required commands",         test_inspector_commands),
        ("All test modules importable",                 test_all_test_modules),
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
        print("=" * 60)
        print("ALL 28-PART BUILD COMPLETE")
        print("Systematic Problem Scouting system fully operational.")
        print("=" * 60)
    else:
        print("Some final system tests failed — see above.")
        sys.exit(1)


if __name__ == "__main__":
    main()
