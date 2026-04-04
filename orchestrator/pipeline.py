"""
orchestrator/pipeline.py — Full Pipeline Runner

Runs the entire scouting pipeline or individual stages.

Stages (in dependency order):
  1. scan        — Internet scan for new business models (bm_scanner)
  2. aggregate   — Recompute signal_strength on all vectors
  3. trends      — Detect scalar activation trends
  4. health      — Graph health check (node/rel counts)

Usage:
    python orchestrator/pipeline.py
    python orchestrator/pipeline.py --stages aggregate,trends
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

try:
    from core import error_log, version_control, snapshot as snap_module
    _CORE_AVAILABLE = True
except ImportError:
    _CORE_AVAILABLE = False

ALL_STAGES = ["scan", "aggregate", "trends", "health"]


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
                count(CASE WHEN h.status = 'Validated' THEN 1 END) AS validated,
                count(CASE WHEN h.status = 'Rejected' THEN 1 END) AS rejected,
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
        f"  Validated: {hyp_stats['validated']}  |  "
        f"Rejected: {hyp_stats['rejected']}  |  "
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
        # Default: analysis + health (safe to run frequently)
        stages = ["aggregate", "trends", "health"]

    started_at = datetime.now(timezone.utc).isoformat()
    console.print(f"\n[bold]Pipeline Runner[/bold] — stages: {', '.join(stages)}")
    if dry_run:
        console.print("  [yellow]DRY RUN — no writes[/yellow]")

    # Pre-run snapshot
    if _CORE_AVAILABLE and not dry_run:
        label = "_".join(stages) if len(stages) <= 4 else f"{stages[0]}_plus_{len(stages)-1}"
        snap = snap_module.take_snapshot(label=f"pre_{label}")
        if "error" not in snap:
            console.print(f"  [dim]Snapshot taken: pre_{label}[/dim]")

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
            if _CORE_AVAILABLE:
                error_log.log_error("orchestrator.pipeline", f"stage_{stage}", e,
                                    context={"stage": stage, "dry_run": dry_run})

    completed_at = datetime.now(timezone.utc).isoformat()
    console.print(f"\n")
    console.print(table)

    success = len(errors) == 0

    # Auto-commit code if successful
    if _CORE_AVAILABLE and success and not dry_run:
        commit_result = version_control.git_commit_if_changed(
            f"Auto: pipeline {','.join(stages)}"
        )
        if commit_result.get("committed"):
            console.print(f"  [dim]Code committed: {commit_result['sha']} — {commit_result['message']}[/dim]")

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
