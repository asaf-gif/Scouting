"""
extraction/vector_activator.py — Technology → Vector Activation Scorer

Computes an activation score for each (Technology, TransformationVector) pair
by measuring scalar alignment: how well do the scalars the technology MOVES
match the scalars the vector IMPACTS, in the same direction?

This is Step 2 of the new hypothesis chain:
  Technology → (moves) Scalars → (activates) TransformationVectors → Hypothesis

Activation Score (0.0 – 1.0):
  - For each scalar in both Tech's MOVES_SCALAR and Vector's IMPACTS:
      aligned  (same direction) → +weight
      opposed  (opposite direction) → -weight
  - weight = score magnitude (1 for moderate/weak, 2 for strong)
  - score = sum(aligned weights) / (sum(aligned weights) + sum(opposed weights) + unmatched penalty)
  - Threshold: only write ACTIVATES if activation_score >= 0.35

Usage:
    from extraction.vector_activator import activate_vectors_for_tech

    result = activate_vectors_for_tech("TECH_001")

    # Or run for all technologies:
    from extraction.vector_activator import activate_all
    activate_all()
"""

import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
from neo4j import GraphDatabase
from rich.console import Console
from rich.table import Table

load_dotenv(override=True)
console = Console(width=200)

try:
    from core.error_log import log_error, capture_errors
    _LOG_AVAILABLE = True
except ImportError:
    _LOG_AVAILABLE = False
    def log_error(*a, **k): pass
    def capture_errors(context_keys=None):
        def decorator(fn): return fn
        return decorator

try:
    from core.editorial import get_constant as _get_constant
    ACTIVATION_THRESHOLD = _get_constant("activation", "ACTIVATION_THRESHOLD", 0.35)
except Exception:
    ACTIVATION_THRESHOLD = 0.35


def get_driver():
    return GraphDatabase.driver(
        os.getenv("NEO4J_URI"),
        auth=(os.getenv("NEO4J_USER", "neo4j"), os.getenv("NEO4J_PASSWORD")),
    )


def get_tech_scalar_fingerprint(driver, tech_id: str) -> dict[str, dict]:
    """
    Returns {scalar_id: {direction, strength, score}} for all MOVES_SCALAR rels.
    """
    with driver.session() as s:
        rows = s.run("""
            MATCH (t:Technology {tech_id: $tid})-[r:MOVES_SCALAR]->(sc:Scalar)
            RETURN sc.scalar_id AS sid, r.direction AS direction,
                   r.strength AS strength, r.score AS score
        """, tid=tech_id).data()
    return {r["sid"]: r for r in rows}


def get_all_vectors_with_impacts(driver) -> list[dict]:
    """
    Returns all vectors that have at least one IMPACTS relationship, with their
    full scalar impact profile.
    """
    with driver.session() as s:
        rows = s.run("""
            MATCH (v:TransformationVector)-[r:IMPACTS]->(sc:Scalar)
            WITH v, collect({
                sid: sc.scalar_id,
                direction: r.direction,
                strength: r.impact_strength,
                score: r.impact_score
            }) AS impacts
            WHERE size(impacts) > 0
            MATCH (v)-[:FROM_BIM]->(f:BusinessModel)
            MATCH (v)-[:TO_BIM]->(t:BusinessModel)
            RETURN v.vector_id AS vid,
                   f.name AS from_name,
                   t.name AS to_name,
                   impacts
        """).data()
    return rows


def compute_activation_score(
    tech_fingerprint: dict[str, dict],
    vector_impacts: list[dict],
) -> tuple[float, list[dict]]:
    """
    Compute how well a technology's scalar movements align with a vector's scalar impacts.

    Returns (activation_score, overlap_details).
    activation_score is in [0, 1].
    """
    if not tech_fingerprint or not vector_impacts:
        return 0.0, []

    vector_map = {imp["sid"]: imp for imp in vector_impacts}

    aligned_weight  = 0
    opposed_weight  = 0
    overlap_details = []

    for sid, tech_mv in tech_fingerprint.items():
        if sid not in vector_map:
            continue

        vec_imp = vector_map[sid]
        tech_dir = tech_mv.get("direction", "increases")
        vec_dir  = vec_imp.get("direction", "increases")

        # Weight = magnitude of tech's movement score
        weight = abs(tech_mv.get("score") or 1)

        if tech_dir == vec_dir:
            aligned_weight += weight
            overlap_details.append({
                "scalar_id": sid,
                "alignment": "aligned",
                "tech_direction": tech_dir,
                "vec_direction": vec_dir,
                "weight": weight,
            })
        else:
            opposed_weight += weight
            overlap_details.append({
                "scalar_id": sid,
                "alignment": "opposed",
                "tech_direction": tech_dir,
                "vec_direction": vec_dir,
                "weight": weight,
            })

    total = aligned_weight + opposed_weight
    if total == 0:
        return 0.0, []

    # Penalise small overlap: require at least 2 aligned scalars for a strong signal
    overlap_count = len([d for d in overlap_details if d["alignment"] == "aligned"])
    try:
        from core.editorial import get_constant as _gc
        _cov_base = _gc("activation", "COVERAGE_FACTOR_BASE", 3)
    except Exception:
        _cov_base = 3
    coverage_factor = min(1.0, overlap_count / _cov_base)  # reaches 1.0 at cov_base+ aligned scalars

    raw_score = aligned_weight / total
    activation_score = round(raw_score * coverage_factor, 3)

    return activation_score, overlap_details


def write_activation(
    driver,
    tech_id: str,
    vector_id: str,
    activation_score: float,
    overlap_details: list[dict],
    now: str,
) -> bool:
    """Write a (Technology)-[:ACTIVATES]->(TransformationVector) relationship."""
    aligned  = [d["scalar_id"] for d in overlap_details if d["alignment"] == "aligned"]
    opposed  = [d["scalar_id"] for d in overlap_details if d["alignment"] == "opposed"]

    with driver.session() as s:
        result = s.run("""
            MATCH (t:Technology {tech_id: $tid})
            MATCH (v:TransformationVector {vector_id: $vid})
            MERGE (t)-[r:ACTIVATES]->(v)
            SET r.activation_score  = $score,
                r.aligned_scalars   = $aligned,
                r.opposed_scalars   = $opposed,
                r.overlap_count     = $n_aligned,
                r.computed_by       = 'vector_activator',
                r.computed_at       = $now
            RETURN r
        """,
            tid=tech_id,
            vid=vector_id,
            score=activation_score,
            aligned=aligned,
            opposed=opposed,
            n_aligned=len(aligned),
            now=now,
        )
        return result.single() is not None


def delete_stale_activations(driver, tech_id: str):
    """Remove all existing ACTIVATES rels for this tech before recomputing."""
    with driver.session() as s:
        s.run("""
            MATCH (t:Technology {tech_id: $tid})-[r:ACTIVATES]->()
            DELETE r
        """, tid=tech_id)


@capture_errors(context_keys=["tech_id"])
def activate_vectors_for_tech(
    tech_id: str,
    threshold: float = ACTIVATION_THRESHOLD,
    dry_run: bool = False,
    top_n: int = 30,
) -> dict:
    """
    Compute activation scores for all vectors against a technology's scalar fingerprint.
    Writes ACTIVATES relationships for vectors above threshold.

    Returns:
    {
      "tech_id": str,
      "tech_name": str,
      "vectors_activated": int,
      "top_vectors": list of {vid, from_name, to_name, activation_score, aligned_scalars},
      "status": "activated" | "dry_run" | "no_fingerprint" | "error",
    }
    """
    console.print(f"\n[bold]Vector Activator[/bold] — {tech_id}")

    driver = get_driver()
    now = datetime.now(timezone.utc).isoformat()

    # Get tech name
    with driver.session() as s:
        tech_rec = s.run(
            "MATCH (t:Technology {tech_id: $id}) RETURN t.name AS name",
            id=tech_id
        ).single()
    tech_name = tech_rec["name"] if tech_rec else tech_id

    fingerprint = get_tech_scalar_fingerprint(driver, tech_id)
    if not fingerprint:
        console.print(f"  [yellow]No MOVES_SCALAR relationships found for {tech_id}. "
                      f"Run tech_scalar_classifier first.[/yellow]")
        driver.close()
        return {
            "tech_id": tech_id,
            "tech_name": tech_name,
            "vectors_activated": 0,
            "top_vectors": [],
            "status": "no_fingerprint",
        }

    console.print(f"  Technology: {tech_name}")
    console.print(f"  Scalar fingerprint: {len(fingerprint)} movements")

    vectors = get_all_vectors_with_impacts(driver)
    console.print(f"  Vectors to score: {len(vectors)}")

    scored = []
    for v in vectors:
        score, overlap = compute_activation_score(fingerprint, v["impacts"])
        if score >= threshold:
            scored.append({
                "vid":              v["vid"],
                "from_name":        v["from_name"],
                "to_name":          v["to_name"],
                "activation_score": score,
                "overlap":          overlap,
                "aligned_scalars":  [d["scalar_id"] for d in overlap if d["alignment"] == "aligned"],
                "opposed_scalars":  [d["scalar_id"] for d in overlap if d["alignment"] == "opposed"],
            })

    scored.sort(key=lambda x: x["activation_score"], reverse=True)
    top = scored[:top_n]

    written = 0
    if not dry_run:
        delete_stale_activations(driver, tech_id)
        for entry in scored:
            ok = write_activation(
                driver, tech_id, entry["vid"],
                entry["activation_score"], entry["overlap"], now
            )
            if ok:
                written += 1
        console.print(f"  Written: {written} ACTIVATES relationships (threshold={threshold})")

    driver.close()

    # Display top results
    table = Table(title=f"Activated Vectors — {tech_name} (top {len(top)})", show_header=True)
    table.add_column("Score", justify="right", width=7)
    table.add_column("Vector", width=35)
    table.add_column("From → To", width=60)
    table.add_column("Aligned Scalars", width=30)

    for entry in top:
        color = "green" if entry["activation_score"] >= 0.6 else (
            "yellow" if entry["activation_score"] >= 0.45 else "dim"
        )
        table.add_row(
            f"[{color}]{entry['activation_score']:.3f}[/{color}]",
            entry["vid"],
            f"{entry['from_name'][:28]} → {entry['to_name'][:28]}",
            ", ".join(entry["aligned_scalars"][:4]),
        )
    console.print(table)
    console.print(f"\n  Total vectors above threshold ({threshold}): {len(scored)}")

    return {
        "tech_id":           tech_id,
        "tech_name":         tech_name,
        "vectors_activated": written if not dry_run else len(scored),
        "top_vectors":       top,
        "status":            "dry_run" if dry_run else "activated",
    }


def activate_all(threshold: float = ACTIVATION_THRESHOLD, dry_run: bool = False) -> list[dict]:
    """Run vector activation for all technologies that have scalar fingerprints."""
    driver = get_driver()
    with driver.session() as s:
        techs = s.run("""
            MATCH (t:Technology)-[:MOVES_SCALAR]->()
            RETURN DISTINCT t.tech_id AS id, t.name AS name
            ORDER BY t.tech_id
        """).data()
    driver.close()

    if not techs:
        console.print("[yellow]No technologies with MOVES_SCALAR relationships found. "
                      "Run tech_scalar_classifier first.[/yellow]")
        return []

    results = []
    for t in techs:
        result = activate_vectors_for_tech(t["id"], threshold=threshold, dry_run=dry_run)
        results.append(result)

    total_activated = sum(r.get("vectors_activated", 0) for r in results)
    console.print(f"\n[bold]Done.[/bold] {len(results)} technologies, "
                  f"{total_activated} vector activations written.")
    return results


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Compute vector activation scores for technologies")
    parser.add_argument("tech_id", nargs="?", help="Tech ID (e.g. TECH_001) or omit for all")
    parser.add_argument("--threshold", type=float, default=ACTIVATION_THRESHOLD)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--top-n", type=int, default=30)
    args = parser.parse_args()

    if args.tech_id:
        activate_vectors_for_tech(args.tech_id, threshold=args.threshold,
                                  dry_run=args.dry_run, top_n=args.top_n)
    else:
        activate_all(threshold=args.threshold, dry_run=args.dry_run)
