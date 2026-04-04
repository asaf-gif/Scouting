"""
evaluation/monitor.py — Hypothesis Staleness Monitor (Part 22)

Watches the graph for hypotheses that need re-evaluation:
  1. No validation_score yet (never scored)
  2. New Evidence added since last research run (researched_at)
  3. Signal strength drifted > threshold since last validation

Output is a priority-sorted queue:
  URGENT   — no validation score at all
  STALE    — new evidence since last research
  DRIFT    — signal changed materially (> drift_threshold)
  CURRENT  — up to date

Usage:
    from evaluation.monitor import run_monitor
    report = run_monitor(drift_threshold=0.10)

    python evaluation/monitor.py
    python evaluation/monitor.py --drift 0.05 --json
"""

import os
import sys
import json
import argparse
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
from neo4j import GraphDatabase
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

load_dotenv(override=True)
console = Console(width=200)

DRIFT_THRESHOLD_DEFAULT = 0.10


def get_driver():
    return GraphDatabase.driver(
        os.getenv("NEO4J_URI"),
        auth=(os.getenv("NEO4J_USER", "neo4j"), os.getenv("NEO4J_PASSWORD")),
    )


def find_stale_hypotheses(driver, drift_threshold: float = DRIFT_THRESHOLD_DEFAULT) -> list:
    """
    Query graph for hypotheses in one of three staleness states.

    Returns list of dicts:
    {
      "hypothesis_id": str,
      "title": str,
      "status": str,
      "conviction_score": float,
      "validation_score": float | None,
      "signal_strength": float | None,
      "last_signal": float | None,        # signal at time of last validation
      "researched_at": str | None,
      "validated_at": str | None,
      "latest_evidence_at": str | None,
      "evidence_count": int,
      "staleness": "URGENT" | "STALE" | "DRIFT" | "CURRENT",
      "staleness_reason": str,
    }
    """
    with driver.session() as s:
        rows = s.run("""
            MATCH (h:DisruptionHypothesis)
            OPTIONAL MATCH (h)-[:GENERATED_FROM]->(v:TransformationVector)
            OPTIONAL MATCH (ev_r:Evidence)-[:SUPPORTS]->(v)
            WITH h, v,
                 count(ev_r) AS evidence_count,
                 max(ev_r.created_at) AS latest_evidence_at
            RETURN h.hypothesis_id       AS hid,
                   h.title               AS title,
                   h.status              AS status,
                   h.conviction_score    AS conviction,
                   h.validation_score    AS validation_score,
                   h.research_confidence AS research_conf,
                   h.counter_confidence  AS counter_conf,
                   h.researched_at       AS researched_at,
                   h.validated_at        AS validated_at,
                   h.signal_at_validation AS signal_at_validation,
                   v.signal_strength     AS signal_strength,
                   evidence_count,
                   latest_evidence_at
            ORDER BY h.conviction_score DESC
        """).data()

    results = []
    for row in rows:
        hid            = row["hid"]
        validation     = row.get("validation_score")
        researched_at  = row.get("researched_at")
        validated_at   = row.get("validated_at")
        latest_ev_at   = row.get("latest_evidence_at")
        signal_now     = row.get("signal_strength") or 0.0
        signal_at_val  = row.get("signal_at_validation") or 0.0
        drift          = abs(signal_now - signal_at_val)

        # Classify staleness
        if validation is None:
            staleness = "URGENT"
            reason    = "no validation score computed yet"
        elif researched_at and latest_ev_at and latest_ev_at > researched_at:
            staleness = "STALE"
            reason    = f"new evidence since {researched_at[:10]}"
        elif signal_at_val > 0 and drift > drift_threshold:
            staleness = "DRIFT"
            reason    = f"signal drifted {drift:+.3f} (was {signal_at_val:.3f}, now {signal_now:.3f})"
        else:
            staleness = "CURRENT"
            reason    = "up to date"

        results.append({
            "hypothesis_id":      hid,
            "title":              row.get("title") or "",
            "status":             row.get("status") or "Hypothesis",
            "conviction_score":   float(row.get("conviction") or 0),
            "validation_score":   float(validation) if validation is not None else None,
            "research_conf":      float(row.get("research_conf") or 0),
            "counter_conf":       float(row.get("counter_conf") or 0),
            "signal_strength":    signal_now,
            "signal_at_validation": signal_at_val,
            "signal_drift":       drift,
            "researched_at":      researched_at,
            "validated_at":       validated_at,
            "latest_evidence_at": latest_ev_at,
            "evidence_count":     int(row.get("evidence_count") or 0),
            "staleness":          staleness,
            "staleness_reason":   reason,
        })

    return results


def build_rescore_queue(driver, drift_threshold: float = DRIFT_THRESHOLD_DEFAULT) -> list:
    """
    Return hypotheses that need action, sorted by priority.
    Order: URGENT > STALE > DRIFT (CURRENT excluded).
    """
    all_hyps = find_stale_hypotheses(driver, drift_threshold)
    priority = {"URGENT": 0, "STALE": 1, "DRIFT": 2, "CURRENT": 3}
    queue = [h for h in all_hyps if h["staleness"] != "CURRENT"]
    queue.sort(key=lambda h: (priority[h["staleness"]], -h["conviction_score"]))
    return queue


def stamp_signal_at_validation(driver, hypothesis_id: str, signal: float) -> None:
    """
    Write the current signal strength onto the hypothesis at validation time,
    so future drift checks can compare against it.
    """
    with driver.session() as s:
        s.run("""
            MATCH (h:DisruptionHypothesis {hypothesis_id: $hid})
            SET h.signal_at_validation = $sig,
                h.signal_checked_at = $now
        """, hid=hypothesis_id, sig=signal,
             now=datetime.now(timezone.utc).isoformat())


def run_monitor(drift_threshold: float = DRIFT_THRESHOLD_DEFAULT,
                dry_run: bool = False,
                output_json: bool = False) -> dict:
    """
    Run the monitor and return a report.

    Returns:
    {
      "checked_at": str,
      "total_hypotheses": int,
      "urgent": int,
      "stale": int,
      "drift": int,
      "current": int,
      "queue": [...]   # items needing action, priority-sorted
    }
    """
    console.print(f"\n[bold]Evaluation Monitor[/bold] — drift threshold={drift_threshold:.2f}")

    driver = get_driver()
    all_hyps = find_stale_hypotheses(driver, drift_threshold)
    queue    = build_rescore_queue(driver, drift_threshold)
    driver.close()

    counts = {"URGENT": 0, "STALE": 0, "DRIFT": 0, "CURRENT": 0}
    for h in all_hyps:
        counts[h["staleness"]] += 1

    # Display table
    table = Table(title="Hypothesis Staleness Report", show_lines=True)
    table.add_column("ID",         style="bold", no_wrap=True)
    table.add_column("Status",     width=12)
    table.add_column("Staleness",  width=9)
    table.add_column("Val Score",  justify="right")
    table.add_column("Signal",     justify="right")
    table.add_column("Drift",      justify="right")
    table.add_column("Reason",     width=45)

    staleness_style = {
        "URGENT":  "bold red",
        "STALE":   "yellow",
        "DRIFT":   "cyan",
        "CURRENT": "green",
    }

    for h in all_hyps:
        style = staleness_style.get(h["staleness"], "")
        val   = f"{h['validation_score']:.4f}" if h["validation_score"] is not None else "—"
        sig   = f"{h['signal_strength']:.4f}"
        drift = f"{h['signal_drift']:+.4f}" if h["signal_drift"] else "—"
        table.add_row(
            h["hypothesis_id"],
            h["status"],
            h["staleness"],
            val, sig, drift,
            h["staleness_reason"],
            style=style,
        )

    console.print(table)
    console.print(Panel(
        f"Total: {len(all_hyps)} hypotheses\n"
        f"[red]URGENT[/red]:  {counts['URGENT']}  (no validation score)\n"
        f"[yellow]STALE[/yellow]:   {counts['STALE']}  (new evidence since last research)\n"
        f"[cyan]DRIFT[/cyan]:   {counts['DRIFT']}  (signal drifted > {drift_threshold:.0%})\n"
        f"[green]CURRENT[/green]: {counts['CURRENT']}  (up to date)",
        title="Monitor Summary",
    ))

    if queue:
        console.print(f"\n[bold]{len(queue)} hypothesis(es) need action:[/bold]")
        for h in queue:
            console.print(
                f"  [{staleness_style.get(h['staleness'],'')}]{h['staleness']}[/]  "
                f"{h['hypothesis_id']}  — {h['staleness_reason']}"
            )
    else:
        console.print("\n[green]All hypotheses are current.[/green]")

    report = {
        "checked_at":        datetime.now(timezone.utc).isoformat(),
        "drift_threshold":   drift_threshold,
        "total_hypotheses":  len(all_hyps),
        "urgent":            counts["URGENT"],
        "stale":             counts["STALE"],
        "drift":             counts["DRIFT"],
        "current":           counts["CURRENT"],
        "queue":             queue,
    }

    if output_json:
        print(json.dumps(report, indent=2, default=str))

    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Evaluation Monitor")
    parser.add_argument("--drift",  type=float, default=DRIFT_THRESHOLD_DEFAULT,
                        help="Signal drift threshold (default 0.10)")
    parser.add_argument("--json",   action="store_true", help="Output JSON")
    parser.add_argument("--dry-run", action="store_true", help="No writes")
    args = parser.parse_args()

    run_monitor(
        drift_threshold=args.drift,
        dry_run=args.dry_run,
        output_json=args.json,
    )
