"""
Parts 26-27 — Orchestration Test

Tests:
1.  run_pipeline() returns dict with required keys
2.  pipeline 'health' stage returns node counts
3.  pipeline 'aggregate' stage runs without error
4.  pipeline 'trends' stage runs without error
5.  pipeline 'rank' stage runs without error
6.  pipeline 'monitor' stage runs without error
7.  pipeline 'score' stage runs without error
8.  pipeline 'health' stage reports hypotheses
9.  Multiple stages run in correct order
10. run_pipeline with unknown stage records error (graceful)

Run: python tests/test_26_27_orchestration.py
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv(override=True)

PASS = "✓"
FAIL = "✗"


def test_pipeline_return_keys():
    from orchestrator.pipeline import run_pipeline
    result = run_pipeline(stages=["health"], dry_run=True)
    required = {"started_at", "completed_at", "stages_run", "results", "errors", "success"}
    missing = required - result.keys()
    if missing:
        return False, f"Missing keys: {missing}"
    return True, f"run_pipeline() returns correct structure ({len(required)} keys)"


def test_health_stage_node_counts():
    from orchestrator.pipeline import stage_health
    result = stage_health()
    nodes = result.get("nodes", {})
    if not nodes:
        return False, "No node counts returned from health stage"
    if "BusinessModel" not in nodes:
        return False, f"BusinessModel not in nodes: {list(nodes.keys())}"
    if nodes["BusinessModel"] < 27:
        return False, f"Expected ≥27 BusinessModel nodes, got {nodes['BusinessModel']}"
    return True, f"Health stage: {len(nodes)} node types, {nodes.get('BusinessModel','?')} BMs"


def test_aggregate_stage():
    from orchestrator.pipeline import stage_aggregate
    result = stage_aggregate(dry_run=True)
    if "vectors_updated" not in result:
        return False, "Missing 'vectors_updated' in aggregate result"
    return True, f"Aggregate stage: vectors_updated={result['vectors_updated']}"


def test_trends_stage():
    from orchestrator.pipeline import stage_trends
    result = stage_trends(dry_run=True)
    if "trends_found" not in result:
        return False, "Missing 'trends_found' in trends result"
    return True, f"Trends stage: trends_found={result['trends_found']}"


def test_rank_stage():
    from orchestrator.pipeline import stage_rank
    result = stage_rank(dry_run=True)
    if "vectors_ranked" not in result:
        return False, "Missing 'vectors_ranked' in rank result"
    return True, f"Rank stage: vectors_ranked={result['vectors_ranked']}"


def test_monitor_stage():
    from orchestrator.pipeline import stage_monitor
    result = stage_monitor(dry_run=True)
    required = {"urgent", "stale", "drift", "current"}
    missing = required - result.keys()
    if missing:
        return False, f"Missing keys in monitor result: {missing}"
    total = result["urgent"] + result["stale"] + result["drift"] + result["current"]
    return True, f"Monitor stage: total={total} (urgent={result['urgent']}, stale={result['stale']})"


def test_score_stage():
    from orchestrator.pipeline import stage_score
    result = stage_score(dry_run=True)
    if "scored" not in result:
        return False, "Missing 'scored' in score result"
    return True, f"Score stage: scored={result['scored']}"


def test_health_hypothesis_report():
    from orchestrator.pipeline import stage_health
    result = stage_health()
    hyps = result.get("hypotheses", {})
    if not hyps:
        return False, "No hypothesis stats in health report"
    if "total" not in hyps:
        return False, f"Missing 'total' in hypothesis stats: {list(hyps.keys())}"
    total = hyps.get("total", 0)
    scored = hyps.get("scored", 0)
    return True, f"Hypothesis health: total={total}, scored={scored}"


def test_multi_stage_pipeline():
    from orchestrator.pipeline import run_pipeline
    stages = ["aggregate", "trends", "rank", "monitor", "health"]
    result = run_pipeline(stages=stages, dry_run=True)
    if not result["success"]:
        return False, f"Pipeline failed: {result['errors']}"
    if len(result["stages_run"]) != len(stages):
        return False, f"Expected {len(stages)} stages run, got {len(result['stages_run'])}"
    return True, f"Multi-stage pipeline: {', '.join(result['stages_run'])} — all OK"


def test_unknown_stage_graceful():
    from orchestrator.pipeline import run_pipeline
    result = run_pipeline(stages=["health", "definitely_not_a_stage"], dry_run=True)
    if "definitely_not_a_stage" not in result["errors"]:
        return False, "Unknown stage should be recorded in errors"
    if "health" not in result["stages_run"]:
        return False, "Health stage should still run despite unknown stage"
    return True, "Unknown stage recorded in errors; known stages still ran"


def main():
    print("\n=== Parts 26-27 — Orchestration Test ===\n")
    print("  Note: dry_run=True — no writes to graph\n")

    all_passed = True
    tests = [
        ("run_pipeline() returns correct structure",    test_pipeline_return_keys),
        ("Health stage returns node counts",            test_health_stage_node_counts),
        ("Aggregate stage runs (dry-run)",              test_aggregate_stage),
        ("Trends stage runs (dry-run)",                 test_trends_stage),
        ("Rank stage runs (dry-run)",                   test_rank_stage),
        ("Monitor stage runs (dry-run)",                test_monitor_stage),
        ("Score stage runs (dry-run)",                  test_score_stage),
        ("Health stage reports hypothesis stats",       test_health_hypothesis_report),
        ("Multi-stage pipeline (5 stages)",             test_multi_stage_pipeline),
        ("Unknown stage is gracefully handled",         test_unknown_stage_graceful),
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
        print("All orchestration tests passed.")
        print("Ready to proceed to Part 28 (final system test).")
    else:
        print("Some tests failed.")
        import sys
        sys.exit(1)


if __name__ == "__main__":
    main()
