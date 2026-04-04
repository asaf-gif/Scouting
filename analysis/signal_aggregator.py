"""
analysis/signal_aggregator.py — Signal Strength Aggregator (Part 14)

For each TransformationVector, aggregates all linked Evidence and scalar
IMPACTS to compute a unified signal_strength score (0.0–1.0) and writes
it back to the vector node.

signal_strength formula:
    base  = evidence_score × 0.40
           + scalar_coverage × 0.30
           + scalar_magnitude × 0.20
           + hypothesis_conviction × 0.10

Where:
  evidence_score     = tanh(evidence_count / 3) × mean_confidence
  scalar_coverage    = classified_scalars / 8   (capped at 1.0)
  scalar_magnitude   = mean(|impact_score|) / 2  (capped at 1.0)
  hypothesis_conviction = best conviction across linked hypotheses (0 if none)

Usage:
    python analysis/signal_aggregator.py            # aggregate all vectors
    python analysis/signal_aggregator.py --limit 20 # sample run
    python analysis/signal_aggregator.py --vector VEC_BIM_026_BIM_027
"""

import os
import sys
import math
import argparse
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
from neo4j import GraphDatabase
from rich.console import Console
from rich.table import Table
from rich.progress import track

load_dotenv(override=True)
console = Console(width=200)


def get_driver():
    return GraphDatabase.driver(
        os.getenv("NEO4J_URI"),
        auth=(os.getenv("NEO4J_USER", "neo4j"), os.getenv("NEO4J_PASSWORD")),
    )


def fetch_vector_data(driver, vector_id: str = None) -> list[dict]:
    """Pull all data needed to compute signal strength for each vector."""
    filter_clause = "WHERE v.vector_id = $vid" if vector_id else ""
    params = {"vid": vector_id} if vector_id else {}

    with driver.session() as s:
        result = s.run(f"""
            MATCH (v:TransformationVector)
            {filter_clause}
            OPTIONAL MATCH (e:Evidence)-[:SUPPORTS]->(v)
            OPTIONAL MATCH (v)-[r:IMPACTS]->(sc:Scalar)
            OPTIONAL MATCH (h:DisruptionHypothesis)-[:GENERATED_FROM]->(v)
            WITH v,
                 count(DISTINCT e)  AS evidence_count,
                 collect(DISTINCT e.confidence) AS evidence_confs,
                 count(DISTINCT sc) AS scalar_count,
                 collect(DISTINCT r.impact_score) AS impact_scores,
                 max(h.conviction_score) AS best_conviction,
                 v.tech_score_gnns AS gnns,
                 v.tech_score_kggen AS kggen,
                 v.tech_score_synthetic AS synth
            RETURN v.vector_id AS vid,
                   evidence_count,
                   evidence_confs,
                   scalar_count,
                   impact_scores,
                   coalesce(best_conviction, 0.0) AS best_conviction,
                   coalesce(gnns, 0) AS gnns,
                   coalesce(kggen, 0) AS kggen,
                   coalesce(synth, 0) AS synth
        """, **params)
        return result.data()


def compute_signal_strength(row: dict) -> dict:
    """Compute signal_strength and component scores for one vector."""
    # Evidence component
    ev_count = row["evidence_count"]
    ev_confs = [c for c in row["evidence_confs"] if c is not None]
    mean_conf = sum(ev_confs) / len(ev_confs) if ev_confs else 0.0
    evidence_score = math.tanh(ev_count / 3.0) * mean_conf  # 0→0, 3→0.76, 6→0.95

    # Scalar coverage component (target: ≥8 classified scalars)
    scalar_count = row["scalar_count"]
    scalar_coverage = min(scalar_count / 8.0, 1.0)

    # Scalar magnitude component (mean absolute impact score / 2)
    scores = [s for s in row["impact_scores"] if s is not None]
    if scores:
        mean_magnitude = sum(abs(s) for s in scores) / len(scores)
        scalar_magnitude = min(mean_magnitude / 2.0, 1.0)
    else:
        scalar_magnitude = 0.0

    # Hypothesis conviction component
    conviction = float(row["best_conviction"] or 0.0)

    # Weighted composite
    signal_strength = (
        evidence_score  * 0.40 +
        scalar_coverage * 0.30 +
        scalar_magnitude * 0.20 +
        conviction      * 0.10
    )

    # Best tech score (normalised — max observed is ~15)
    best_tech = max(
        int(row.get("gnns", 0) or 0),
        int(row.get("kggen", 0) or 0),
        int(row.get("synth", 0) or 0),
    )

    return {
        "signal_strength":  round(signal_strength, 4),
        "evidence_score":   round(evidence_score, 4),
        "scalar_coverage":  round(scalar_coverage, 4),
        "scalar_magnitude": round(scalar_magnitude, 4),
        "conviction_component": round(conviction * 0.10, 4),
        "evidence_count":  ev_count,
        "scalar_count":    scalar_count,
        "best_conviction": conviction,
        "best_tech_score": best_tech,
    }


def write_signal_strength(driver, vector_id: str, scores: dict) -> None:
    now = datetime.now(timezone.utc).isoformat()
    with driver.session() as s:
        s.run("""
            MATCH (v:TransformationVector {vector_id: $vid})
            SET v.signal_strength       = $ss,
                v.evidence_score        = $es,
                v.scalar_coverage       = $sc,
                v.scalar_magnitude      = $sm,
                v.best_conviction       = $bc,
                v.best_tech_score       = $bt,
                v.signal_computed_at    = $now
        """,
            vid=vector_id,
            ss=scores["signal_strength"],
            es=scores["evidence_score"],
            sc=scores["scalar_coverage"],
            sm=scores["scalar_magnitude"],
            bc=scores["best_conviction"],
            bt=scores["best_tech_score"],
            now=now,
        )


def run_aggregation(vector_id: str = None, limit: int = None,
                    dry_run: bool = False) -> list[dict]:
    """Compute and write signal_strength for all (or filtered) vectors."""
    driver = get_driver()
    rows = fetch_vector_data(driver, vector_id=vector_id)

    if limit:
        rows = rows[:limit]

    results = []
    non_zero = 0

    for row in rows:
        scores = compute_signal_strength(row)
        scores["vector_id"] = row["vid"]

        if not dry_run:
            write_signal_strength(driver, row["vid"], scores)

        if scores["signal_strength"] > 0:
            non_zero += 1

        results.append(scores)

    driver.close()
    return results


def display_top(results: list[dict], n: int = 15) -> None:
    ranked = sorted(results, key=lambda x: x["signal_strength"], reverse=True)[:n]

    table = Table(title=f"Top {n} Vectors by Signal Strength", show_header=True)
    table.add_column("Vector", width=30)
    table.add_column("Signal", justify="right", width=8)
    table.add_column("Evidence", justify="right", width=9)
    table.add_column("Scalars", justify="right", width=8)
    table.add_column("Tech", justify="right", width=6)
    table.add_column("Conviction", justify="right", width=10)

    for r in ranked:
        sig = r["signal_strength"]
        color = "green" if sig >= 0.3 else ("yellow" if sig >= 0.1 else "dim")
        table.add_row(
            r["vector_id"],
            f"[{color}]{sig:.4f}[/{color}]",
            str(r["evidence_count"]),
            str(r["scalar_count"]),
            str(r["best_tech_score"]),
            f"{r['best_conviction']:.2f}",
        )
    console.print(table)


def main():
    parser = argparse.ArgumentParser(description="Signal strength aggregator")
    parser.add_argument("--vector", help="Compute for a single vector ID")
    parser.add_argument("--limit", type=int, help="Process only first N vectors")
    parser.add_argument("--dry-run", action="store_true",
                        help="Compute but do not write to graph")
    parser.add_argument("--top", type=int, default=15,
                        help="Show top N results (default: 15)")
    args = parser.parse_args()

    console.print(f"\n[bold]Signal Aggregator[/bold]")
    console.print(f"  Mode: {'DRY RUN' if args.dry_run else 'LIVE'}\n")

    results = run_aggregation(
        vector_id=args.vector,
        limit=args.limit,
        dry_run=args.dry_run,
    )

    non_zero = sum(1 for r in results if r["signal_strength"] > 0)
    console.print(f"\n  Processed {len(results)} vectors — {non_zero} with signal > 0\n")

    display_top(results, n=args.top)


if __name__ == "__main__":
    main()
