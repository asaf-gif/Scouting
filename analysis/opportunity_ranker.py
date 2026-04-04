"""
analysis/opportunity_ranker.py — Opportunity Ranker (Part 16)

Computes a composite opportunity_score for every TransformationVector,
combining signal strength, tech scores, hypothesis conviction, and
scalar alignment. Writes the score back to each vector and feeds the
inspector's top-opportunities command.

opportunity_score formula:
    signal_strength  × 0.35   (evidence + scalar classification quality)
    + tech_alignment × 0.35   (best tech score normalised to 0-1)
    + conviction     × 0.20   (best hypothesis conviction)
    + scalar_align   × 0.10   (fraction of strong scalars pointing same direction)

Usage:
    python analysis/opportunity_ranker.py
    python analysis/opportunity_ranker.py --top 20
    python analysis/opportunity_ranker.py --tech kggen   # rank by one tech only
    python analysis/opportunity_ranker.py --dry-run
"""

import os
import sys
import argparse
from datetime import datetime, timezone
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
from neo4j import GraphDatabase
from rich.console import Console
from rich.table import Table

load_dotenv(override=True)
console = Console(width=200)

TECH_SCORE_MAX = 15.0   # observed maximum from Excel data


def get_driver():
    return GraphDatabase.driver(
        os.getenv("NEO4J_URI"),
        auth=(os.getenv("NEO4J_USER", "neo4j"), os.getenv("NEO4J_PASSWORD")),
    )


def fetch_all_vectors(driver, tech_filter: str = None) -> list[dict]:
    tech_col = "coalesce(v.tech_score_kggen, 0)"     if tech_filter == "kggen"     else \
               "coalesce(v.tech_score_gnns, 0)"      if tech_filter == "gnns"      else \
               "coalesce(v.tech_score_synthetic, 0)" if tech_filter == "synthetic" else \
               "reduce(m=0, x IN [coalesce(v.tech_score_gnns,0), coalesce(v.tech_score_kggen,0), coalesce(v.tech_score_synthetic,0)] | CASE WHEN x > m THEN x ELSE m END)"

    with driver.session() as s:
        result = s.run(f"""
            MATCH (v:TransformationVector)
            MATCH (v)-[:FROM_BIM]->(f:BusinessModel)
            MATCH (v)-[:TO_BIM]->(t:BusinessModel)
            OPTIONAL MATCH (v)-[r:IMPACTS]->(sc:Scalar)
            OPTIONAL MATCH (h:DisruptionHypothesis)-[:GENERATED_FROM]->(v)
            WITH v, f, t,
                 {tech_col} AS raw_tech,
                 coalesce(v.signal_strength, 0.0) AS signal,
                 max(coalesce(h.conviction_score, 0.0)) AS conviction,
                 collect({{
                     direction: r.direction,
                     strength:  r.impact_strength,
                     score:     r.impact_score
                 }}) AS impacts
            RETURN v.vector_id    AS vid,
                   f.name         AS from_name,
                   t.name         AS to_name,
                   f.bim_id       AS from_id,
                   t.bim_id       AS to_id,
                   coalesce(raw_tech, 0) AS raw_tech,
                   signal,
                   conviction,
                   impacts,
                   coalesce(v.tech_score_gnns, 0)      AS gnns,
                   coalesce(v.tech_score_kggen, 0)     AS kggen,
                   coalesce(v.tech_score_synthetic, 0) AS synth
        """)
        return result.data()


def scalar_alignment(impacts: list[dict]) -> float:
    """
    Fraction of strong/moderate impacts that agree on direction.
    1.0 = all pointing same way; 0.5 = split; 0.0 = all opposing.
    """
    strong = [i for i in impacts
              if i.get("strength") in ("strong", "moderate")
              and i.get("direction") in ("increases", "decreases")]
    if not strong:
        return 0.5

    dirs = Counter(i["direction"] for i in strong)
    dominant = max(dirs.values())
    return dominant / len(strong)


def compute_opportunity_score(row: dict) -> dict:
    signal     = float(row["signal"] or 0.0)
    raw_tech   = float(row["raw_tech"] or 0.0)
    conviction = float(row["conviction"] or 0.0)
    impacts    = [i for i in row["impacts"] if i.get("direction")]

    tech_norm  = min(raw_tech / TECH_SCORE_MAX, 1.0)
    sc_align   = scalar_alignment(impacts)

    opp_score = (
        signal     * 0.35 +
        tech_norm  * 0.35 +
        conviction * 0.20 +
        sc_align   * 0.10
    )

    return {
        "vector_id":         row["vid"],
        "from_name":         row["from_name"],
        "to_name":           row["to_name"],
        "from_id":           row["from_id"],
        "to_id":             row["to_id"],
        "opportunity_score": round(opp_score, 4),
        "signal_strength":   round(signal, 4),
        "tech_score_norm":   round(tech_norm, 4),
        "raw_tech":          int(raw_tech),
        "conviction":        round(conviction, 4),
        "scalar_alignment":  round(sc_align, 4),
        "gnns":              row["gnns"],
        "kggen":             row["kggen"],
        "synth":             row["synth"],
    }


def write_opportunity_scores(driver, scored: list[dict]) -> None:
    now = datetime.now(timezone.utc).isoformat()
    with driver.session() as s:
        for r in scored:
            s.run("""
                MATCH (v:TransformationVector {vector_id: $vid})
                SET v.opportunity_score  = $os,
                    v.scalar_alignment   = $sa,
                    v.opp_scored_at      = $now
            """,
                vid=r["vector_id"],
                os=r["opportunity_score"],
                sa=r["scalar_alignment"],
                now=now,
            )


def run_ranking(tech_filter: str = None, dry_run: bool = False) -> list[dict]:
    driver = get_driver()
    rows = fetch_all_vectors(driver, tech_filter=tech_filter)
    console.print(f"  Loaded {len(rows)} vectors")

    scored = [compute_opportunity_score(r) for r in rows]
    scored.sort(key=lambda x: x["opportunity_score"], reverse=True)

    if not dry_run:
        write_opportunity_scores(driver, scored)
        console.print(f"  Wrote opportunity_score to {len(scored)} vectors")

    driver.close()
    return scored


def display_top(scored: list[dict], n: int = 20) -> None:
    table = Table(title=f"Top {n} Opportunities", show_header=True)
    table.add_column("#", width=4)
    table.add_column("From → To", width=52)
    table.add_column("Opp", justify="right", width=7)
    table.add_column("Signal", justify="right", width=7)
    table.add_column("Tech", justify="right", width=5)
    table.add_column("Conv", justify="right", width=6)
    table.add_column("Align", justify="right", width=6)

    for i, r in enumerate(scored[:n], 1):
        opp = r["opportunity_score"]
        color = "green" if opp >= 0.30 else ("yellow" if opp >= 0.15 else "dim")
        table.add_row(
            str(i),
            f"{r['from_name'][:24]} → {r['to_name'][:24]}",
            f"[{color}]{opp:.4f}[/{color}]",
            f"{r['signal_strength']:.3f}",
            str(r["raw_tech"]),
            f"{r['conviction']:.2f}",
            f"{r['scalar_alignment']:.2f}",
        )
    console.print(table)


def main():
    parser = argparse.ArgumentParser(description="Opportunity ranker for TransformationVectors")
    parser.add_argument("--tech", choices=["gnns", "kggen", "synthetic"],
                        help="Rank by a specific technology score")
    parser.add_argument("--top", type=int, default=20, help="Show top N (default: 20)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Compute but do not write to graph")
    args = parser.parse_args()

    console.print(f"\n[bold]Opportunity Ranker[/bold]")
    if args.tech:
        console.print(f"  Tech filter: {args.tech}")
    console.print(f"  Mode: {'DRY RUN' if args.dry_run else 'LIVE'}\n")

    scored = run_ranking(tech_filter=args.tech, dry_run=args.dry_run)
    display_top(scored, n=args.top)

    top = scored[0] if scored else None
    if top:
        console.print(
            f"\n  [bold]#1:[/bold] {top['from_name']} → {top['to_name']} "
            f"(score={top['opportunity_score']:.4f})"
        )


if __name__ == "__main__":
    main()
