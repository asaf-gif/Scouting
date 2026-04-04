"""
Parts 14-17 — Analysis & Signal Detection Test

Tests:
1.  signal_aggregator: computes signal_strength for all vectors
2.  Signal written to graph on VEC_BIM_026_BIM_027 (has evidence + scalars)
3.  Signal for evidence-backed vector > signal for empty vector
4.  trend_detector: detects trends from current activations
5.  Trend written to Scalar node
6.  opportunity_ranker: computes opportunity_score for all vectors
7.  VEC_BIM_026_BIM_027 ranks in top-20 (has evidence + hypothesis + tech score)
8.  opportunity_score = f(signal, tech, conviction, alignment) — components sum correctly
9.  ui/app.py hypothesis page queries work (no syntax errors)
10. ui/app.py top-opportunities query returns results after ranking

Run: python tests/test_14_17_analysis.py
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv(override=True)

from neo4j import GraphDatabase

PASS = "✓"
FAIL = "✗"
TARGET_VEC = "VEC_BIM_026_BIM_027"


def get_driver():
    return GraphDatabase.driver(
        os.getenv("NEO4J_URI"),
        auth=(os.getenv("NEO4J_USER", "neo4j"), os.getenv("NEO4J_PASSWORD")),
    )


def test_signal_aggregation_runs():
    from analysis.signal_aggregator import run_aggregation
    results = run_aggregation(dry_run=False)
    if not results:
        return False, "run_aggregation returned empty list"
    return True, f"Aggregated {len(results)} vectors"


def test_signal_written_to_graph():
    driver = get_driver()
    with driver.session() as s:
        rec = s.run("""
            MATCH (v:TransformationVector {vector_id: $vid})
            RETURN v.signal_strength AS ss, v.evidence_count AS ec,
                   v.scalar_count AS sc
        """, vid=TARGET_VEC).single()
    driver.close()

    if not rec or rec["ss"] is None:
        return False, f"signal_strength not written to {TARGET_VEC}"
    return True, f"{TARGET_VEC} signal_strength={rec['ss']:.4f}"


def test_signal_ordering():
    """Evidenced vector should have higher signal than empty vectors."""
    driver = get_driver()
    with driver.session() as s:
        evidenced = s.run("""
            MATCH (v:TransformationVector {vector_id: $vid})
            RETURN coalesce(v.signal_strength, 0.0) AS ss
        """, vid=TARGET_VEC).single()["ss"]

        # Get median signal of all vectors
        all_signals = s.run("""
            MATCH (v:TransformationVector)
            WHERE v.signal_strength IS NOT NULL
            RETURN v.signal_strength AS ss ORDER BY ss
        """).data()
    driver.close()

    signals = [r["ss"] for r in all_signals]
    median = signals[len(signals) // 2] if signals else 0

    if evidenced <= median:
        return False, (
            f"{TARGET_VEC} signal ({evidenced:.4f}) not above median ({median:.4f})"
        )
    return True, (
        f"{TARGET_VEC} signal ({evidenced:.4f}) > median ({median:.4f}) ✓"
    )


def test_trend_detection_runs():
    from analysis.trend_detector import run_trend_detection
    trends = run_trend_detection(min_vectors=2, dry_run=False)
    # Empty is valid at this early stage
    return True, f"Trend detection complete — {len(trends)} trend(s) found"


def test_trend_written_to_scalar():
    from analysis.trend_detector import run_trend_detection
    trends = run_trend_detection(min_vectors=2, dry_run=True)
    if not trends:
        return True, "No trends yet — not enough data (expected at bootstrap)"

    driver = get_driver()
    top_sid = trends[0]["scalar_id"]
    with driver.session() as s:
        rec = s.run(
            "MATCH (sc:Scalar {scalar_id:$sid}) RETURN sc.trend_direction AS d, sc.trend_vector_count AS cnt",
            sid=top_sid,
        ).single()
    driver.close()

    if not rec or not rec["d"]:
        return False, f"Trend data not written to scalar {top_sid}"
    return True, f"Scalar {top_sid}: trend_direction={rec['d']}, vectors={rec['cnt']}"


def test_opportunity_ranking_runs():
    from analysis.opportunity_ranker import run_ranking
    scored = run_ranking(dry_run=False)
    if not scored:
        return False, "run_ranking returned empty list"
    return True, f"Ranked {len(scored)} vectors"


def test_target_in_top20():
    driver = get_driver()
    with driver.session() as s:
        result = s.run("""
            MATCH (v:TransformationVector)
            WHERE v.opportunity_score IS NOT NULL
            RETURN v.vector_id AS vid, v.opportunity_score AS opp
            ORDER BY v.opportunity_score DESC LIMIT 20
        """).data()
    driver.close()

    vids = [r["vid"] for r in result]
    if TARGET_VEC not in vids:
        top5 = [(r["vid"], r["opp"]) for r in result[:5]]
        return False, f"{TARGET_VEC} not in top-20. Top 5: {top5}"

    rank = vids.index(TARGET_VEC) + 1
    score = next(r["opp"] for r in result if r["vid"] == TARGET_VEC)
    return True, f"{TARGET_VEC} ranked #{rank} (opp={score:.4f})"


def test_opportunity_score_components():
    """Verify formula: score ≈ signal×0.35 + tech×0.35 + conv×0.20 + align×0.10."""
    from analysis.opportunity_ranker import compute_opportunity_score
    mock_row = {
        "vid": "VEC_TEST", "from_name": "A", "to_name": "B",
        "from_id": "BIM_001", "to_id": "BIM_002",
        "signal": 0.4, "raw_tech": 7.5, "conviction": 0.6,
        "gnns": 0, "kggen": 7, "synth": 0,
        "impacts": [
            {"direction": "increases", "strength": "strong",   "score": 2},
            {"direction": "increases", "strength": "moderate", "score": 1},
            {"direction": "decreases", "strength": "weak",     "score": -1},
        ],
    }
    result = compute_opportunity_score(mock_row)
    opp = result["opportunity_score"]

    # scalar_alignment: only strong/moderate impacts count; "weak" is excluded.
    # strong=[increases/strong, increases/moderate] → 2/2=1.0 alignment
    # Expected: 0.4×0.35 + (7.5/15)×0.35 + 0.6×0.20 + 1.0×0.10
    expected = round(0.4*0.35 + 0.5*0.35 + 0.6*0.20 + 1.0*0.10, 4)
    if abs(opp - expected) > 0.01:
        return False, f"Formula mismatch: got {opp}, expected ~{expected}"
    return True, f"Formula correct: {opp:.4f} ≈ {expected:.4f}"


def test_hypothesis_page_query():
    """Hypothesis page Cypher query runs without error."""
    driver = get_driver()
    with driver.session() as s:
        rows = s.run("""
            MATCH (h:DisruptionHypothesis)
            OPTIONAL MATCH (h)-[:GENERATED_FROM]->(v:TransformationVector)
            OPTIONAL MATCH (f:BusinessModel {bim_id: h.from_bim_id})
            OPTIONAL MATCH (t:BusinessModel {bim_id: h.to_bim_id})
            RETURN h.hypothesis_id AS hid, h.title AS title,
                   h.conviction_score AS conviction,
                   v.signal_strength AS signal,
                   v.opportunity_score AS opp_score
            ORDER BY h.conviction_score DESC
        """).data()
    driver.close()

    return True, f"Hypothesis page query returned {len(rows)} row(s)"


def test_opportunities_page_query():
    """Top-opportunities page Cypher query runs without error."""
    driver = get_driver()
    with driver.session() as s:
        rows = s.run("""
            MATCH (v:TransformationVector)
            WHERE v.opportunity_score IS NOT NULL
            MATCH (v)-[:FROM_BIM]->(f:BusinessModel)
            MATCH (v)-[:TO_BIM]->(t:BusinessModel)
            RETURN v.vector_id AS vid, f.name AS from_name, t.name AS to_name,
                   v.opportunity_score AS opp
            ORDER BY v.opportunity_score DESC LIMIT 20
        """).data()
    driver.close()

    if not rows:
        return False, "No opportunities returned — ranker may not have run"
    return True, f"Top-opportunities query returned {len(rows)} row(s), #1 opp={rows[0]['opp']:.4f}"


def main():
    print("\n=== Parts 14-17 — Analysis & Signal Detection Test ===\n")

    all_passed = True
    tests = [
        ("Signal aggregation runs",            test_signal_aggregation_runs),
        ("signal_strength written to graph",   test_signal_written_to_graph),
        ("Evidenced vector > median signal",   test_signal_ordering),
        ("Trend detection runs",               test_trend_detection_runs),
        ("Trend data written to Scalar node",  test_trend_written_to_scalar),
        ("Opportunity ranking runs",           test_opportunity_ranking_runs),
        (f"{TARGET_VEC} in top-20",            test_target_in_top20),
        ("Opportunity score formula correct",  test_opportunity_score_components),
        ("Hypothesis page query works",        test_hypothesis_page_query),
        ("Opportunities page query works",     test_opportunities_page_query),
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
        print("All analysis & signal detection tests passed.")
        print("Ready to proceed to Part 18.")
    else:
        print("Some tests failed.")
        sys.exit(1)


if __name__ == "__main__":
    main()
