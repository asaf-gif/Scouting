"""
orchestrator/pipeline.py — Full Pipeline Runner (Part 26)

Runs the entire scouting pipeline or individual stages.

Stages (in dependency order):
  1. scan        — Internet scan for new business models (bm_scanner)
  2. aggregate   — Recompute signal_strength on all vectors
  3. trends      — Detect scalar activation trends
  4. rank        — Recompute opportunity scores
  5. monitor     — Check which hypotheses need re-research
  6. research    — Run deep + counter research on top unresearched hypothesis
  7. score       — Compute validation scores for researched hypotheses
  8. health      — Graph health check (node/rel counts)

Usage:
    python orchestrator/pipeline.py
    python orchestrator/pipeline.py --stages aggregate,trends,rank
    python orchestrator/pipeline.py --stages research --target HYP_BIM_026_BIM_027
    python orchestrator/pipeline.py --dry-run
"""

import os
import sys
import json
import argparse
import traceback
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
from neo4j import GraphDatabase
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

load_dotenv(override=True)
console = Console(width=200)

ALL_STAGES = ["scan", "aggregate", "trends", "rank", "monitor", "research", "score", "health"]


def get_driver():
    return GraphDatabase.driver(
        os.getenv("NEO4J_URI"),
        auth=(os.getenv("NEO4J_USER", "neo4j"), os.getenv("NEO4J_PASSWORD")),
    )


# ── Stage implementations ──────────────────────────────────────────────────────

def stage_scan(dry_run: bool = False, **kwargs) -> dict:
    """Scan internet for new business model patterns."""
    from input_layer.bm_scanner import run_scan
    console.print("  [dim]Running BM scanner...[/dim]")
    result = run_scan(dry_run=dry_run, enrich_limit=2)
    return {
        "candidates": result.get("novel_count", 0),
        "enriched":   result.get("enriched_count", 0),
    }


def stage_aggregate(dry_run: bool = False, **kwargs) -> dict:
    """Recompute signal_strength on all TransformationVectors."""
    from analysis.signal_aggregator import run_aggregation
    console.print("  [dim]Aggregating signals...[/dim]")
    result = run_aggregation(dry_run=dry_run)  # returns list[dict]
    return {"vectors_updated": len(result)}


def stage_trends(dry_run: bool = False, **kwargs) -> dict:
    """Detect scalar activation trends."""
    from analysis.trend_detector import run_trend_detection
    console.print("  [dim]Detecting trends...[/dim]")
    trends = run_trend_detection(dry_run=dry_run)  # returns list[dict]
    return {"trends_found": len(trends)}


def stage_rank(dry_run: bool = False, **kwargs) -> dict:
    """Recompute opportunity scores for all vectors."""
    from analysis.opportunity_ranker import run_ranking
    console.print("  [dim]Ranking opportunities...[/dim]")
    scored = run_ranking(dry_run=dry_run)  # returns list[dict]
    return {"vectors_ranked": len(scored)}


def stage_monitor(dry_run: bool = False, **kwargs) -> dict:
    """Check which hypotheses are stale and need re-research."""
    from evaluation.monitor import run_monitor
    console.print("  [dim]Running staleness monitor...[/dim]")
    report = run_monitor(drift_threshold=0.10, dry_run=dry_run)
    return {
        "urgent":  report.get("urgent", 0),
        "stale":   report.get("stale", 0),
        "drift":   report.get("drift", 0),
        "current": report.get("current", 0),
    }


def stage_research(dry_run: bool = False, target: str = None, **kwargs) -> dict:
    """
    Run deep + counter research on unresearched hypotheses.
    If target is given, research only that hypothesis.
    Otherwise, pick the top-priority hypothesis from the monitor queue.
    """
    from research.deep_researcher import research_hypothesis, counter_research_hypothesis
    from evaluation.monitor import build_rescore_queue

    if target:
        hids = [target]
    else:
        driver = get_driver()
        queue = build_rescore_queue(driver)
        driver.close()
        # Take up to 1 URGENT hypothesis, or the top stale one
        urgent = [h["hypothesis_id"] for h in queue if h["staleness"] == "URGENT"]
        hids = urgent[:1] if urgent else [queue[0]["hypothesis_id"]] if queue else []

    if not hids:
        console.print("  [dim]No hypotheses need research.[/dim]")
        return {"researched": 0}

    researched = 0
    for hid in hids:
        console.print(f"  [dim]Deep research: {hid}...[/dim]")
        r_result = research_hypothesis(hid, dry_run=dry_run)
        console.print(f"  [dim]Counter research: {hid}...[/dim]")
        c_result = counter_research_hypothesis(hid, dry_run=dry_run)
        if r_result.get("status") != "error" and c_result.get("status") != "error":
            researched += 1

    return {"researched": researched, "hypothesis_ids": hids}


def stage_score(dry_run: bool = False, target: str = None, **kwargs) -> dict:
    """Compute validation scores for all researched hypotheses."""
    from research.validation_scorer import score_hypothesis

    driver = get_driver()
    with driver.session() as s:
        rows = s.run("""
            MATCH (h:DisruptionHypothesis)
            WHERE h.research_confidence IS NOT NULL
               OR h.counter_confidence IS NOT NULL
            RETURN h.hypothesis_id AS hid
        """).data()
    driver.close()

    hids = [r["hid"] for r in rows]
    if target:
        hids = [target] if target in hids else []

    scored = 0
    for hid in hids:
        console.print(f"  [dim]Scoring {hid}...[/dim]")
        try:
            score_hypothesis(hid, dry_run=dry_run)
            scored += 1
        except Exception as e:
            console.print(f"  [yellow]Warning: scoring {hid} failed: {e}[/yellow]")

    return {"scored": scored}


def stage_health(**kwargs) -> dict:
    """Graph health check — node/rel counts and data quality indicators."""
    driver = get_driver()
    with driver.session() as s:
        counts = s.run("""
            MATCH (n)
            RETURN labels(n)[0] AS label, count(n) AS cnt
            ORDER BY cnt DESC
        """).data()
        rel_counts = s.run("""
            MATCH ()-[r]->()
            RETURN type(r) AS rel_type, count(r) AS cnt
            ORDER BY cnt DESC
        """).data()
        hyp_stats = s.run("""
            MATCH (h:DisruptionHypothesis)
            RETURN
                count(h) AS total,
                count(h.validation_score) AS scored,
                count(CASE WHEN h.status = 'Validated' THEN 1 END) AS validated,
                count(CASE WHEN h.status = 'Contested' THEN 1 END) AS contested,
                count(CASE WHEN h.status = 'Hypothesis' THEN 1 END) AS hypothesis_status
        """).single()
    driver.close()

    node_map = {r["label"]: r["cnt"] for r in counts}
    rel_map  = {r["rel_type"]: r["cnt"] for r in rel_counts}

    console.print(Panel(
        "\n".join(f"  {label}: {cnt}" for label, cnt in node_map.items()),
        title="Node Counts",
    ))
    console.print(Panel(
        f"  Total hypotheses: {hyp_stats['total']}\n"
        f"  Scored: {hyp_stats['scored']}\n"
        f"  Validated: {hyp_stats['validated']}  |  "
        f"Contested: {hyp_stats['contested']}  |  "
        f"Hypothesis: {hyp_stats['hypothesis_status']}",
        title="Hypothesis Health",
    ))

    return {
        "nodes": node_map,
        "relationships": rel_map,
        "hypotheses": dict(hyp_stats),
    }


# ── Pipeline runner ────────────────────────────────────────────────────────────

STAGE_FNS = {
    "scan":      stage_scan,
    "aggregate": stage_aggregate,
    "trends":    stage_trends,
    "rank":      stage_rank,
    "monitor":   stage_monitor,
    "research":  stage_research,
    "score":     stage_score,
    "health":    stage_health,
}


def run_pipeline(stages: list = None, dry_run: bool = False,
                 target: str = None) -> dict:
    """
    Run pipeline stages in order.

    Args:
        stages:   List of stage names. If None, run all stages except 'scan' and 'research'
                  (those are expensive and usually triggered explicitly).
        dry_run:  Skip writes.
        target:   Hypothesis ID to target for research/score stages.

    Returns:
        {
          "started_at": str,
          "completed_at": str,
          "stages_run": [str],
          "results": {stage: result_dict},
          "errors": {stage: error_msg},
          "success": bool,
        }
    """
    if stages is None:
        # Default: analysis + monitoring loop (safe to run frequently)
        stages = ["aggregate", "trends", "rank", "monitor", "score", "health"]

    started_at = datetime.now(timezone.utc).isoformat()
    console.print(f"\n[bold]Pipeline Runner[/bold] — stages: {', '.join(stages)}")
    if dry_run:
        console.print("  [yellow]DRY RUN — no writes[/yellow]")

    results = {}
    errors  = {}

    table = Table(title="Pipeline Execution", show_lines=True)
    table.add_column("Stage",   style="bold", width=12)
    table.add_column("Status",  width=10)
    table.add_column("Result",  width=60)

    for stage in stages:
        if stage not in STAGE_FNS:
            console.print(f"  [red]Unknown stage: {stage}[/red]")
            errors[stage] = f"Unknown stage"
            table.add_row(stage, "[red]UNKNOWN[/red]", "")
            continue

        console.print(f"\n[bold]  Stage: {stage.upper()}[/bold]")
        try:
            result = STAGE_FNS[stage](dry_run=dry_run, target=target)
            results[stage] = result
            summary = "  ".join(f"{k}={v}" for k, v in result.items()
                                if not isinstance(v, (dict, list)))
            table.add_row(stage, "[green]OK[/green]", summary or "done")
        except Exception as e:
            err_msg = str(e)
            errors[stage] = err_msg
            console.print(f"  [red]Stage {stage} failed: {err_msg}[/red]")
            console.print(traceback.format_exc())
            table.add_row(stage, "[red]FAILED[/red]", err_msg[:60])

    completed_at = datetime.now(timezone.utc).isoformat()
    console.print(f"\n")
    console.print(table)

    success = len(errors) == 0
    color = "green" if success else "red"
    console.print(Panel(
        f"Stages run:  {len(results)}/{len(stages)}\n"
        f"Errors:      {len(errors)}\n"
        f"Started:     {started_at}\n"
        f"Completed:   {completed_at}",
        title=f"[{color}]Pipeline {'Complete' if success else 'Finished with errors'}[/{color}]",
    ))

    return {
        "started_at":   started_at,
        "completed_at": completed_at,
        "stages_run":   list(results.keys()),
        "results":      results,
        "errors":       errors,
        "success":      success,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pipeline Runner")
    parser.add_argument("--stages",   default=None,
                        help=f"Comma-separated stages. All: {','.join(ALL_STAGES)}")
    parser.add_argument("--target",   default=None, help="Target hypothesis ID")
    parser.add_argument("--dry-run",  action="store_true", help="No writes")
    parser.add_argument("--json",     action="store_true", help="Output JSON")
    args = parser.parse_args()

    stages = [s.strip() for s in args.stages.split(",")] if args.stages else None

    report = run_pipeline(
        stages=stages,
        dry_run=args.dry_run,
        target=args.target,
    )

    if args.json:
        print(json.dumps(report, indent=2, default=str))
