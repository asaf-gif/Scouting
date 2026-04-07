"""
analysis/trend_detector.py — Cross-Vector Trend Detector (Part 15)

Finds scalar conditions that are consistently activated across multiple
high-signal TransformationVectors — indicating a macro-level structural shift
rather than an isolated case.

A TREND is a scalar appearing as a strong/moderate driver across ≥N distinct
vectors, all pointing in the same direction.

Usage:
    python analysis/trend_detector.py                    # detect all trends
    python analysis/trend_detector.py --min-vectors 2    # lower threshold
    python analysis/trend_detector.py --output trends.json
"""

import os
import sys
import json
import argparse
from datetime import datetime, timezone
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
from neo4j import GraphDatabase
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

load_dotenv(override=True)
console = Console(width=200)

try:
    from core.editorial import get_constant as _gc
    MIN_VECTORS_DEFAULT = _gc("trends", "MIN_VECTORS_DEFAULT", 2)
    MIN_SIGNAL          = _gc("trends", "MIN_SIGNAL", 0.0)
except Exception:
    MIN_VECTORS_DEFAULT = 2   # lowered from 3 since we're early in data collection
    MIN_SIGNAL = 0.0          # include all vectors (even those with 0 signal) during bootstrap


def get_driver():
    return GraphDatabase.driver(
        os.getenv("NEO4J_URI"),
        auth=(os.getenv("NEO4J_USER", "neo4j"), os.getenv("NEO4J_PASSWORD")),
    )


def fetch_scalar_activations(driver, min_signal: float = MIN_SIGNAL) -> list[dict]:
    """
    Pull all scalar activations from two sources:
    1. IMPACTS relationships on TransformationVectors (from scalar_classifier)
    2. ACTIVATES relationships on Evidence nodes (from vector_extractor)
    """
    with driver.session() as s:
        # Source 1: vector IMPACTS
        impacts = s.run("""
            MATCH (v:TransformationVector)-[r:IMPACTS]->(sc:Scalar)
            WHERE coalesce(v.signal_strength, 0.0) >= $min_signal
               OR r.classified_by = 'scalar_classifier'
            RETURN v.vector_id AS vector_id,
                   sc.scalar_id AS scalar_id,
                   sc.name AS scalar_name,
                   r.direction AS direction,
                   r.impact_strength AS strength,
                   coalesce(r.impact_score, 0) AS score,
                   'vector_impact' AS source
        """, min_signal=min_signal).data()

        # Source 2: evidence ACTIVATES
        activations = s.run("""
            MATCH (e:Evidence)-[r:ACTIVATES]->(sc:Scalar)
            MATCH (e)-[:SUPPORTS]->(v:TransformationVector)
            RETURN v.vector_id AS vector_id,
                   sc.scalar_id AS scalar_id,
                   sc.name AS scalar_name,
                   r.direction AS direction,
                   'moderate' AS strength,
                   1 AS score,
                   'evidence_activation' AS source
        """).data()

    return impacts + activations


def detect_trends(activations: list[dict], min_vectors: int = MIN_VECTORS_DEFAULT) -> list[dict]:
    """
    Aggregate activations by scalar → count unique vectors per direction.
    Returns list of trend dicts sorted by vector count descending.
    """
    # Group by (scalar_id, direction)
    groups = defaultdict(lambda: {"vectors": set(), "scores": [], "names": set()})

    for a in activations:
        key = (a["scalar_id"], a["direction"])
        groups[key]["vectors"].add(a["vector_id"])
        groups[key]["scores"].append(int(a["score"] or 0))
        groups[key]["names"].add(a.get("scalar_name", ""))

    trends = []
    for (scalar_id, direction), data in groups.items():
        n_vectors = len(data["vectors"])
        if n_vectors < min_vectors:
            continue

        scores = data["scores"]
        mean_score = sum(scores) / len(scores) if scores else 0

        trends.append({
            "scalar_id":    scalar_id,
            "scalar_name":  next(iter(data["names"]), ""),
            "direction":    direction,
            "vector_count": n_vectors,
            "vectors":      sorted(data["vectors"]),
            "mean_score":   round(mean_score, 2),
            "trend_strength": round(n_vectors * abs(mean_score), 2),
        })

    trends.sort(key=lambda x: x["trend_strength"], reverse=True)
    return trends


def write_trends_to_graph(driver, trends: list[dict]) -> int:
    """Store trend summary on each Scalar node."""
    now = datetime.now(timezone.utc).isoformat()
    written = 0
    with driver.session() as s:
        for t in trends:
            s.run("""
                MATCH (sc:Scalar {scalar_id: $sid})
                SET sc.trend_direction    = $dir,
                    sc.trend_vector_count = $cnt,
                    sc.trend_strength     = $ts,
                    sc.trend_vectors      = $vectors,
                    sc.trend_computed_at  = $now
            """,
                sid=t["scalar_id"],
                dir=t["direction"],
                cnt=t["vector_count"],
                ts=t["trend_strength"],
                vectors=t["vectors"],
                now=now,
            )
            written += 1
    return written


def run_trend_detection(min_vectors: int = MIN_VECTORS_DEFAULT,
                        dry_run: bool = False) -> list[dict]:
    driver = get_driver()
    activations = fetch_scalar_activations(driver)
    console.print(f"  Loaded {len(activations)} scalar activations from graph")

    trends = detect_trends(activations, min_vectors=min_vectors)
    console.print(f"  Detected {len(trends)} trend(s) (min_vectors={min_vectors})")

    if not dry_run and trends:
        written = write_trends_to_graph(driver, trends)
        console.print(f"  Wrote trend data to {written} Scalar node(s)")

    driver.close()
    return trends


def display_trends(trends: list[dict]) -> None:
    if not trends:
        console.print("[yellow]No trends detected yet — more evidence needed.[/yellow]")
        return

    table = Table(title="Active Scalar Trends", show_header=True)
    table.add_column("Scalar", width=10)
    table.add_column("Condition", width=50)
    table.add_column("Direction", width=10)
    table.add_column("Vectors", justify="right", width=8)
    table.add_column("Strength", justify="right", width=9)

    for t in trends:
        dir_color = "green" if t["direction"] == "increases" else "red"
        table.add_row(
            t["scalar_id"],
            t["scalar_name"][:49],
            f"[{dir_color}]{t['direction']}[/{dir_color}]",
            str(t["vector_count"]),
            f"{t['trend_strength']:.2f}",
        )
    console.print(table)

    # Top trend detail
    if trends:
        top = trends[0]
        console.print(Panel(
            f"[bold]{top['scalar_id']}[/bold] — {top['scalar_name']}\n\n"
            f"Direction: {top['direction']}  |  "
            f"Vectors: {top['vector_count']}  |  "
            f"Trend strength: {top['trend_strength']}\n\n"
            f"Activated in:\n" +
            "\n".join(f"  • {v}" for v in top["vectors"]),
            title="Top Trend",
        ))


def main():
    parser = argparse.ArgumentParser(description="Cross-vector scalar trend detector")
    parser.add_argument("--min-vectors", type=int, default=MIN_VECTORS_DEFAULT,
                        help=f"Min vectors to constitute a trend (default: {MIN_VECTORS_DEFAULT})")
    parser.add_argument("--dry-run", action="store_true",
                        help="Detect but do not write to graph")
    parser.add_argument("--output", help="Save trend report as JSON")
    args = parser.parse_args()

    console.print(f"\n[bold]Trend Detector[/bold]")
    console.print(f"  Min vectors: {args.min_vectors}  |  Mode: {'DRY RUN' if args.dry_run else 'LIVE'}\n")

    trends = run_trend_detection(min_vectors=args.min_vectors, dry_run=args.dry_run)
    display_trends(trends)

    if args.output:
        with open(args.output, "w") as f:
            json.dump(trends, f, indent=2, default=str)
        console.print(f"\nReport saved to {args.output}")


if __name__ == "__main__":
    main()
