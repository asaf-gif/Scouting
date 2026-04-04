"""
extraction/hypothesis_generator.py — DisruptionHypothesis Generator

Generates hypotheses following the new causal chain:
  Technology → (MOVES_SCALAR) → Scalars → (ACTIVATES) → TransformationVector → Hypothesis

For each Technology × TransformationVector pair above the activation threshold,
generates a DisruptionHypothesis that explains the causal path.

Usage:
    from extraction.hypothesis_generator import generate_hypotheses_for_tech

    results = generate_hypotheses_for_tech("TECH_001")

    # Or generate for a specific tech+vector:
    result = generate_hypothesis("TECH_001", "BIM_026", "BIM_027")
"""

import os
import sys
import json
import re
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import anthropic
from dotenv import load_dotenv
from neo4j import GraphDatabase
from rich.console import Console
from rich.panel import Panel

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

ACTIVATION_THRESHOLD = 0.35


def get_driver():
    return GraphDatabase.driver(
        os.getenv("NEO4J_URI"),
        auth=(os.getenv("NEO4J_USER", "neo4j"), os.getenv("NEO4J_PASSWORD")),
    )


def gather_hypothesis_context(driver, tech_id: str, from_bim: str, to_bim: str) -> dict:
    """Pull everything needed to generate a hypothesis for this tech+vector combination."""
    vector_id = f"VEC_{from_bim}_{to_bim}"

    with driver.session() as s:
        # Technology
        tech = s.run("""
            MATCH (t:Technology {tech_id: $tid})
            RETURN t.tech_id AS id, t.name AS name, t.short_name AS short_name,
                   t.description AS description, t.disruption_thesis AS thesis,
                   t.primary_scalar_driver AS primary_driver,
                   t.scalar_reasoning AS scalar_reasoning
        """, tid=tech_id).single()

        # Tech scalar fingerprint
        scalar_fingerprint = s.run("""
            MATCH (t:Technology {tech_id: $tid})-[r:MOVES_SCALAR]->(sc:Scalar)
            RETURN sc.scalar_id AS sid, sc.name AS name,
                   r.direction AS direction, r.strength AS strength,
                   r.score AS score, r.rationale AS rationale
            ORDER BY abs(r.score) DESC
        """, tid=tech_id).data()

        # Activation relationship (aligned/opposed scalars)
        activation = s.run("""
            MATCH (t:Technology {tech_id: $tid})-[r:ACTIVATES]->(v:TransformationVector {vector_id: $vid})
            RETURN r.activation_score AS activation_score,
                   r.aligned_scalars AS aligned_scalars,
                   r.opposed_scalars AS opposed_scalars,
                   r.overlap_count AS overlap_count
        """, tid=tech_id, vid=vector_id).single()

        # BM descriptions
        f_rec = s.run(
            "MATCH (n:BusinessModel {bim_id:$id}) RETURN n.name AS name, n.description AS desc",
            id=from_bim
        ).single()
        t_rec = s.run(
            "MATCH (n:BusinessModel {bim_id:$id}) RETURN n.name AS name, n.description AS desc",
            id=to_bim
        ).single()

        # Evidence nodes for this vector
        evidence = s.run("""
            MATCH (e:Evidence)-[:SUPPORTS]->(v:TransformationVector {vector_id:$vid})
            RETURN e.evidence_quote AS quote,
                   e.transition_summary AS summary,
                   e.source_url AS url,
                   e.source_type AS src_type,
                   e.companies_mentioned AS companies,
                   e.confidence AS conf
            ORDER BY e.confidence DESC
            LIMIT 5
        """, vid=vector_id).data()

        # Vector's scalar IMPACTS (the vector's own scalar profile)
        vector_impacts = s.run("""
            MATCH (v:TransformationVector {vector_id:$vid})-[r:IMPACTS]->(sc:Scalar)
            RETURN sc.scalar_id AS sid, sc.name AS name,
                   r.direction AS direction, r.impact_strength AS strength,
                   r.impact_score AS score
            ORDER BY abs(r.impact_score) DESC
        """, vid=vector_id).data()

    aligned_sids = set(activation["aligned_scalars"] if activation else [])

    return {
        "tech_id":            tech_id,
        "tech_name":          tech["name"] if tech else tech_id,
        "tech_desc":          (tech["description"] or "") if tech else "",
        "tech_thesis":        (tech["thesis"] or "") if tech else "",
        "tech_primary_driver": tech["primary_driver"] if tech else "",
        "scalar_fingerprint": scalar_fingerprint,
        "vector_id":          vector_id,
        "from_bim":           from_bim,
        "to_bim":             to_bim,
        "from_name":          f_rec["name"] if f_rec else from_bim,
        "from_desc":          f_rec["desc"] if f_rec else "",
        "to_name":            t_rec["name"] if t_rec else to_bim,
        "to_desc":            t_rec["desc"] if t_rec else "",
        "activation_score":   float(activation["activation_score"]) if activation else 0.0,
        "aligned_scalars":    list(aligned_sids),
        "opposed_scalars":    list(activation["opposed_scalars"]) if activation else [],
        "evidence":           evidence,
        "vector_impacts":     vector_impacts,
    }


def load_prompt() -> str:
    path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "prompts", "hypothesis_generation.txt",
    )
    with open(path) as f:
        return f.read()


def call_claude_generate(ctx: dict) -> dict:
    client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

    # Format scalar fingerprint — highlight aligned vs. non-aligned
    fingerprint_str = ""
    aligned_set = set(ctx["aligned_scalars"])
    for mv in ctx["scalar_fingerprint"]:
        tag = "✓ ALIGNED" if mv["sid"] in aligned_set else "  "
        fingerprint_str += (
            f"  {tag} {mv['sid']}: {mv['name'][:55]}\n"
            f"       {mv['direction']} ({mv['strength']}, score={mv['score']})\n"
            f"       {mv['rationale']}\n"
        )

    # Format evidence
    evidence_str = ""
    for i, e in enumerate(ctx["evidence"], 1):
        companies = ", ".join(e.get("companies") or [])
        evidence_str += (
            f"\nEvidence {i} (conf={e['conf']:.2f}):\n"
            f"  Quote: {e['quote']}\n"
            f"  Summary: {e['summary']}\n"
            f"  Companies: {companies or '—'}\n"
            f"  Source: {e['url']}\n"
        )

    user_msg = f"""TRIGGERING TECHNOLOGY:
ID: {ctx['tech_id']}
Name: {ctx['tech_name']}
Description: {ctx['tech_desc'][:400]}
Disruption Thesis: {ctx['tech_thesis'][:400] or '(not yet written)'}

SCALAR FINGERPRINT (scalars this technology moves):
{fingerprint_str or '  No scalar fingerprint yet.'}

ACTIVATED VECTOR: {ctx['vector_id']}
Activation Score: {ctx['activation_score']:.3f}
Aligned Scalars: {', '.join(ctx['aligned_scalars']) or 'none'}
Opposed Scalars: {', '.join(ctx['opposed_scalars']) or 'none'}

FROM: {ctx['from_name']}
{ctx['from_desc'][:400]}

TO: {ctx['to_name']}
{ctx['to_desc'][:400]}

EVIDENCE ({len(ctx['evidence'])} item(s)):
{evidence_str or '  No evidence nodes yet — synthesise from technology mechanism and scalar alignment.'}

Generate a DisruptionHypothesis JSON for this technology × vector combination.
{'Note: low evidence — set conviction_score ≤ 0.65 and reflect uncertainty.' if len(ctx['evidence']) == 0 else ''}
"""

    response = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=2500,
        system=load_prompt(),
        messages=[{"role": "user", "content": user_msg}],
    )
    raw = response.content[0].text.strip()
    obj = re.search(r"\{[\s\S]*\}", raw)
    if not obj:
        raise ValueError(f"Claude returned no JSON. Raw: {raw[:300]}")
    return json.loads(obj.group(0))


def write_hypothesis(driver, hypothesis: dict, tech_id: str, vector_id: str) -> str:
    """Write DisruptionHypothesis node and all relationships."""
    now = datetime.now(timezone.utc).isoformat()
    hyp_id = hypothesis.get("hypothesis_id", f"HYP_{tech_id}_{vector_id}")

    props = {
        "hypothesis_id":        hyp_id,
        "tech_id":              hypothesis.get("tech_id", tech_id),
        "tech_name":            hypothesis.get("tech_name", ""),
        "from_bim_id":          hypothesis.get("from_bim_id", ""),
        "to_bim_id":            hypothesis.get("to_bim_id", ""),
        "title":                hypothesis.get("title", ""),
        "thesis":               hypothesis.get("thesis", ""),
        "activation_score":     float(hypothesis.get("activation_score", 0.5)),
        "conviction_score":     float(hypothesis.get("conviction_score", 0.5)),
        "primary_scalar_driver": hypothesis.get("primary_scalar_driver", ""),
        "supporting_scalars":   hypothesis.get("supporting_scalars", []),
        "evidence_count":       int(hypothesis.get("evidence_count", 0)),
        "companies_exposed":    hypothesis.get("companies_exposed", []),
        "time_horizon":         hypothesis.get("time_horizon", "2-5 years"),
        "disruption_type":      hypothesis.get("disruption_type", "substitution"),
        "ai_technology_link":   hypothesis.get("ai_technology_link", tech_id),
        "counter_argument":     hypothesis.get("counter_argument", ""),
        "status":               "Hypothesis",
        "source":               "hypothesis_generator_v2",
        "created_at":           now,
        "updated_at":           now,
        "pending_human_review": True,
    }

    with driver.session() as s:
        s.run("MERGE (n:DisruptionHypothesis {hypothesis_id:$id}) SET n += $props",
              id=hyp_id, props=props)

        # Link to triggering technology
        s.run("""
            MATCH (h:DisruptionHypothesis {hypothesis_id:$hid})
            MATCH (t:Technology {tech_id:$tid})
            MERGE (h)-[r:TRIGGERED_BY]->(t)
            SET r.created_at = $now
        """, hid=hyp_id, tid=tech_id, now=now)

        # Link to vector
        s.run("""
            MATCH (h:DisruptionHypothesis {hypothesis_id:$hid})
            MATCH (v:TransformationVector {vector_id:$vid})
            MERGE (h)-[r:GENERATED_FROM]->(v)
            SET r.created_at = $now
        """, hid=hyp_id, vid=vector_id, now=now)

        # Link to BMs
        s.run("""
            MATCH (h:DisruptionHypothesis {hypothesis_id:$hid})
            MATCH (f:BusinessModel {bim_id:$fid})
            MATCH (t:BusinessModel {bim_id:$tid})
            MERGE (h)-[:TARGETS]->(f)
            MERGE (h)-[:PROPOSES]->(t)
        """, hid=hyp_id,
            fid=hypothesis.get("from_bim_id", ""),
            tid=hypothesis.get("to_bim_id", ""),
        )

    return hyp_id


@capture_errors(context_keys=["tech_id", "from_bim_id", "to_bim_id"])
def generate_hypothesis(
    tech_id: str,
    from_bim_id: str,
    to_bim_id: str,
    dry_run: bool = False,
) -> dict:
    """
    Generate a DisruptionHypothesis for a specific technology × vector combination.

    Returns:
    {
      "hypothesis_id": str,
      "tech_id": str,
      "vector_id": str,
      "activation_score": float,
      "conviction_score": float,
      "status": "written" | "dry_run" | "low_conviction_skipped" | "error",
      "hypothesis": dict,
    }
    """
    vector_id = f"VEC_{from_bim_id}_{to_bim_id}"
    console.print(f"\n[bold]Hypothesis Generator[/bold] — {tech_id} × {vector_id}")

    driver = get_driver()
    ctx = gather_hypothesis_context(driver, tech_id, from_bim_id, to_bim_id)

    console.print(f"  Tech: {ctx['tech_name']}")
    console.print(f"  From: {ctx['from_name']} → To: {ctx['to_name']}")
    console.print(f"  Activation score: {ctx['activation_score']:.3f}")
    console.print(f"  Aligned scalars: {', '.join(ctx['aligned_scalars']) or 'none'}")
    console.print(f"  Evidence nodes: {len(ctx['evidence'])}")
    console.print("  [dim]Calling Claude...[/dim]")

    try:
        hypothesis = call_claude_generate(ctx)
    except Exception as e:
        log_error("extraction.hypothesis_generator", "generate_hypothesis[call_claude]", e,
                  context={"tech_id": tech_id, "from_bim_id": from_bim_id, "to_bim_id": to_bim_id})
        driver.close()
        return {
            "hypothesis_id": None,
            "tech_id": tech_id,
            "vector_id": vector_id,
            "activation_score": ctx["activation_score"],
            "conviction_score": 0,
            "status": f"error: {e}",
            "hypothesis": {},
        }

    conviction = float(hypothesis.get("conviction_score", 0))

    hyp_id = None
    if conviction < 0.5:
        status = "low_conviction_skipped"
        console.print(f"  [yellow]Conviction too low ({conviction:.2f}) — hypothesis not written[/yellow]")
    elif dry_run:
        status = "dry_run"
        hyp_id = hypothesis.get("hypothesis_id", f"HYP_{tech_id}_{vector_id}")
    else:
        hyp_id = write_hypothesis(driver, hypothesis, tech_id, vector_id)
        status = "written"

    driver.close()

    console.print(Panel(
        f"[bold]Title:[/bold] {hypothesis.get('title', '')}\n\n"
        f"[bold]Thesis:[/bold]\n{hypothesis.get('thesis', '')}\n\n"
        f"[bold]Activation:[/bold] {ctx['activation_score']:.3f}  |  "
        f"[bold]Conviction:[/bold] {conviction:.2f}  |  "
        f"[bold]Type:[/bold] {hypothesis.get('disruption_type', '?')}  |  "
        f"[bold]Horizon:[/bold] {hypothesis.get('time_horizon', '?')}\n\n"
        f"[bold]Primary driver:[/bold] {hypothesis.get('primary_scalar_driver', '?')}\n"
        f"[bold]Triggered by:[/bold] {tech_id} — {ctx['tech_name']}\n\n"
        f"[dim]Counter: {hypothesis.get('counter_argument', '')}[/dim]",
        title=f"Hypothesis: {hyp_id or 'not written'}",
    ))

    return {
        "hypothesis_id":    hyp_id,
        "tech_id":          tech_id,
        "vector_id":        vector_id,
        "activation_score": ctx["activation_score"],
        "conviction_score": conviction,
        "status":           status,
        "hypothesis":       hypothesis,
    }


def generate_hypotheses_for_tech(
    tech_id: str,
    min_activation: float = ACTIVATION_THRESHOLD,
    dry_run: bool = False,
    limit: int = 20,
) -> list[dict]:
    """
    Generate hypotheses for all vectors activated by a technology above the threshold.
    Processes in descending activation score order.
    """
    driver = get_driver()

    # Get all activated vectors for this tech, sorted by activation score
    with driver.session() as s:
        activated = s.run("""
            MATCH (t:Technology {tech_id: $tid})-[r:ACTIVATES]->(v:TransformationVector)
            WHERE r.activation_score >= $threshold
            MATCH (v)-[:FROM_BIM]->(f:BusinessModel)
            MATCH (v)-[:TO_BIM]->(tb:BusinessModel)
            RETURN v.vector_id AS vid,
                   f.bim_id AS from_bim,
                   tb.bim_id AS to_bim,
                   f.name AS from_name,
                   tb.name AS to_name,
                   r.activation_score AS activation_score
            ORDER BY r.activation_score DESC
            LIMIT $limit
        """, tid=tech_id, threshold=min_activation, limit=limit).data()

    driver.close()

    if not activated:
        console.print(f"[yellow]No activated vectors found for {tech_id} "
                      f"(threshold={min_activation}). Run vector_activator first.[/yellow]")
        return []

    console.print(f"\n[bold]Generating hypotheses for {tech_id}[/bold] — "
                  f"{len(activated)} vectors to process")

    results = []
    for entry in activated:
        result = generate_hypothesis(
            tech_id,
            entry["from_bim"],
            entry["to_bim"],
            dry_run=dry_run,
        )
        results.append(result)

    written   = sum(1 for r in results if r["status"] == "written")
    skipped   = sum(1 for r in results if r["status"] == "low_conviction_skipped")
    errors    = sum(1 for r in results if "error" in r.get("status", ""))

    console.print(
        f"\n[bold]Done.[/bold] {written} written, {skipped} skipped (low conviction), "
        f"{errors} errors."
    )
    return results


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Generate disruption hypotheses")
    parser.add_argument("tech_id", help="Technology ID (e.g. TECH_001)")
    parser.add_argument("--from-bim", help="Specific FROM BIM ID")
    parser.add_argument("--to-bim", help="Specific TO BIM ID")
    parser.add_argument("--min-activation", type=float, default=ACTIVATION_THRESHOLD)
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if args.from_bim and args.to_bim:
        generate_hypothesis(args.tech_id, args.from_bim, args.to_bim, dry_run=args.dry_run)
    else:
        generate_hypotheses_for_tech(
            args.tech_id,
            min_activation=args.min_activation,
            dry_run=args.dry_run,
            limit=args.limit,
        )
