"""
Parts 18-21 — Research & Validation Test

Tests:
1.  research_hypothesis() returns brief with supporting + refuting evidence
2.  Deep research Evaluation node written to graph
3.  Evaluation -[:EVALUATES]-> Hypothesis relationship exists
4.  counter_research_hypothesis() returns counter brief with structural barriers
5.  Counter Evaluation node written to graph
6.  score_hypothesis() computes validation_score correctly
7.  Hypothesis status updated in graph (Validated / Hypothesis / Contested)
8.  validation_score formula: research×0.60 + (1-adversarial)×0.40
9.  Research brief text is non-empty and contains key sections
10. Validation Review UI page query works

Run: python tests/test_18_21_research.py
"""

import os
import sys
import json

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


def test_deep_research():
    from research.deep_researcher import research_hypothesis
    result = research_hypothesis(HYP_ID, dry_run=False)

    if result.get("status") == "error":
        return False, result.get("message", "unknown error")
    brief = result.get("brief", {})
    sup = brief.get("supporting_evidence", [])
    ref = brief.get("refuting_evidence", [])
    conf = result.get("validation_confidence", 0)

    if not brief.get("research_summary"):
        return False, "research_summary is empty"
    if len(sup) + len(ref) < 2:
        return False, f"Too few evidence items: {len(sup)} supporting, {len(ref)} refuting"
    return True, (
        f"Brief generated — {len(sup)} supporting, {len(ref)} refuting, "
        f"confidence={conf:.2f}"
    )


def test_research_eval_in_graph():
    driver = get_driver()
    with driver.session() as s:
        rec = s.run("""
            MATCH (ev:Evaluation {evaluation_id:$eid})-[:EVALUATES]->(h:DisruptionHypothesis)
            RETURN ev.evaluation_type AS etype, ev.confidence AS conf,
                   h.hypothesis_id AS hid
        """, eid=f"EVAL_{HYP_ID}_DEEP_RESEARCH").single()
    driver.close()

    if not rec:
        return False, f"EVAL_{HYP_ID}_DEEP_RESEARCH not found in graph"
    return True, (
        f"Evaluation written — type={rec['etype']}, conf={rec['conf']:.2f}, "
        f"linked to {rec['hid']}"
    )


def test_evaluates_relationship():
    driver = get_driver()
    with driver.session() as s:
        count = s.run("""
            MATCH (ev:Evaluation)-[:EVALUATES]->(h:DisruptionHypothesis {hypothesis_id:$hid})
            RETURN count(ev) AS cnt
        """, hid=HYP_ID).single()["cnt"]
    driver.close()

    if count == 0:
        return False, "No EVALUATES relationships found"
    return True, f"{count} Evaluation node(s) linked via EVALUATES"


def test_counter_research():
    from research.deep_researcher import counter_research_hypothesis
    result = counter_research_hypothesis(HYP_ID, dry_run=False)

    if result.get("status") == "error":
        return False, result.get("message", "unknown error")
    counter = result.get("counter", {})
    barriers = counter.get("structural_barriers", [])

    if not counter.get("counter_thesis"):
        return False, "counter_thesis is empty"
    if len(barriers) < 1:
        return False, f"No structural barriers found"
    adv = result.get("adversarial_confidence", 0)
    return True, (
        f"Counter brief generated — {len(barriers)} barriers, "
        f"adversarial_confidence={adv:.2f}"
    )


def test_counter_eval_in_graph():
    driver = get_driver()
    with driver.session() as s:
        rec = s.run("""
            MATCH (ev:Evaluation {evaluation_id:$eid})
            RETURN ev.confidence AS conf, ev.evaluation_type AS etype
        """, eid=f"EVAL_{HYP_ID}_COUNTER_RESEARCH").single()
    driver.close()

    if not rec:
        return False, f"EVAL_{HYP_ID}_COUNTER_RESEARCH not found"
    return True, f"Counter evaluation written — conf={rec['conf']:.2f}"


def test_validation_scoring():
    from research.validation_scorer import score_hypothesis
    result = score_hypothesis(HYP_ID, dry_run=False)

    vs = result.get("validation_score", 0)
    status = result.get("status", "")
    if vs <= 0:
        return False, f"validation_score={vs} — should be > 0"
    if status not in ("Validated", "Hypothesis", "Contested"):
        return False, f"Invalid status: {status}"
    return True, f"validation_score={vs:.4f}, status={status}"


def test_hypothesis_status_updated():
    driver = get_driver()
    with driver.session() as s:
        rec = s.run("""
            MATCH (h:DisruptionHypothesis {hypothesis_id:$hid})
            RETURN h.validation_score AS vs, h.status AS status
        """, hid=HYP_ID).single()
    driver.close()

    if not rec or rec["vs"] is None:
        return False, "validation_score not written to hypothesis node"
    return True, f"Hypothesis updated: validation_score={rec['vs']:.4f}, status={rec['status']}"


def test_validation_formula():
    from research.validation_scorer import compute_validation_score
    rc, ac, conv = 0.80, 0.30, 0.70
    result = compute_validation_score(rc, ac, conv)
    expected = round(rc * 0.60 + (1 - ac) * 0.40, 4)
    vs = result["validation_score"]
    if abs(vs - expected) > 0.001:
        return False, f"Formula wrong: got {vs}, expected {expected}"
    return True, f"Formula correct: {rc}×0.60 + (1-{ac})×0.40 = {vs:.4f}"


def test_brief_text():
    from research.validation_scorer import score_hypothesis
    result = score_hypothesis(HYP_ID, dry_run=True)
    brief = result.get("brief_text", "")
    if len(brief) < 100:
        return False, f"Brief text too short ({len(brief)} chars)"
    for section in ("# Research Brief", "## Thesis"):
        if section not in brief:
            return False, f"Missing section: {section}"
    return True, f"Brief text generated ({len(brief)} chars)"


def test_validation_ui_query():
    driver = get_driver()
    with driver.session() as s:
        rows = s.run("""
            MATCH (h:DisruptionHypothesis)
            OPTIONAL MATCH (ev:Evaluation)-[:EVALUATES]->(h)
            WITH h, collect({type: ev.evaluation_type, conf: ev.confidence}) AS evals
            RETURN h.hypothesis_id AS hid, h.validation_score AS vs,
                   h.status AS status, evals
            ORDER BY coalesce(h.validation_score, h.conviction_score) DESC
        """).data()
    driver.close()

    if not rows:
        return False, "Validation UI query returned no results"
    row = rows[0]
    eval_types = [e.get("type") for e in row.get("evals", []) if e.get("type")]
    return True, (
        f"UI query: {len(rows)} hypothesis(es), "
        f"top={row['hid']} vs={row.get('vs') or 'pending'}, "
        f"eval types={eval_types}"
    )


def main():
    print("\n=== Parts 18-21 — Research & Validation Test ===\n")
    print(f"  Target hypothesis: {HYP_ID}")
    print("  Note: Calls Claude + Tavily. Takes ~3 minutes.\n")

    all_passed = True
    tests = [
        ("Deep research brief generated",       test_deep_research),
        ("Research Evaluation in graph",         test_research_eval_in_graph),
        ("EVALUATES relationship exists",        test_evaluates_relationship),
        ("Counter research brief generated",     test_counter_research),
        ("Counter Evaluation in graph",          test_counter_eval_in_graph),
        ("Validation score computed",            test_validation_scoring),
        ("Hypothesis status updated in graph",   test_hypothesis_status_updated),
        ("Validation formula correct",           test_validation_formula),
        ("Research brief text generated",        test_brief_text),
        ("Validation Review UI query works",     test_validation_ui_query),
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
        print("All research & validation tests passed.")
        print("Ready to proceed to Part 22.")
    else:
        print("Some tests failed.")
        sys.exit(1)


if __name__ == "__main__":
    main()
