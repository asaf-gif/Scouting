"""
extraction/hypothesis_generator.py — DisruptionHypothesis Generator

Given a TransformationVector (from/to BIM pair), aggregates all linked
Evidence nodes and scalar classifications, then synthesises a structured
DisruptionHypothesis node.

Usage:
    from extraction.hypothesis_generator import generate_hypothesis

    result = generate_hypothesis("BIM_026", "BIM_027")
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


def get_driver():
    return GraphDatabase.driver(
        os.getenv("NEO4J_URI"),
        auth=(os.getenv("NEO4J_USER", "neo4j"), os.getenv("NEO4J_PASSWORD")),
    )


def gather_vector_context(driver, from_bim: str, to_bim: str) -> dict:
    """Pull everything we know about this vector from the graph."""
    vector_id = f"VEC_{from_bim}_{to_bim}"

    with driver.session() as s:
        # BM descriptions
        f_rec = s.run("MATCH (n:BusinessModel {bim_id:$id}) RETURN n.name AS name, n.description AS desc",
                      id=from_bim).single()
        t_rec = s.run("MATCH (n:BusinessModel {bim_id:$id}) RETURN n.name AS name, n.description AS desc",
                      id=to_bim).single()

        # Evidence nodes
        evidence = s.run("""
            MATCH (e:Evidence)-[:SUPPORTS]->(v:TransformationVector {vector_id:$vid})
            RETURN e.evidence_quote AS quote,
                   e.transition_summary AS summary,
                   e.source_url AS url,
                   e.source_type AS src_type,
                   e.companies_mentioned AS companies,
                   e.confidence AS conf,
                   e.scalars_activated_json AS scalars_json
            ORDER BY e.confidence DESC
        """, vid=vector_id).data()

        # Scalar impacts already in graph
        scalar_impacts = s.run("""
            MATCH (v:TransformationVector {vector_id:$vid})-[r:IMPACTS]->(sc:Scalar)
            RETURN sc.scalar_id AS sid, sc.name AS name,
                   r.direction AS dir, r.impact_strength AS strength,
                   r.impact_score AS score, r.rationale AS rationale
            ORDER BY abs(r.impact_score) DESC
        """, vid=vector_id).data()

        # Tech scores
        tech = s.run("""
            MATCH (v:TransformationVector {vector_id:$vid})
            RETURN v.tech_score_gnns AS gnns,
                   v.tech_score_kggen AS kggen,
                   v.tech_score_synthetic AS synth,
                   v.primary_scalar_driver AS primary_driver
        """, vid=vector_id).single()

    return {
        "vector_id":     vector_id,
        "from_bim":      from_bim,
        "to_bim":        to_bim,
        "from_name":     f_rec["name"] if f_rec else from_bim,
        "from_desc":     f_rec["desc"] if f_rec else "",
        "to_name":       t_rec["name"] if t_rec else to_bim,
        "to_desc":       t_rec["desc"] if t_rec else "",
        "evidence":      evidence,
        "scalar_impacts": scalar_impacts,
        "tech_scores":   dict(tech) if tech else {},
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

    # Format evidence
    evidence_str = ""
    for i, e in enumerate(ctx["evidence"], 1):
        companies = ", ".join(e.get("companies") or [])
        evidence_str += (
            f"\nEvidence {i} (conf={e['conf']:.2f}, type={e['src_type']}):\n"
            f"  Quote: {e['quote']}\n"
            f"  Summary: {e['summary']}\n"
            f"  Companies: {companies or '—'}\n"
            f"  Source: {e['url']}\n"
        )

    # Format scalars
    scalar_str = ""
    for sc in ctx["scalar_impacts"]:
        scalar_str += (
            f"  {sc['sid']}: {sc['name'][:60]} "
            f"[{sc['dir']}, {sc['strength']}, score={sc['score']}]\n"
            f"    {sc['rationale']}\n"
        )

    tech = ctx["tech_scores"]
    tech_str = (
        f"Tech scores: GNNs={tech.get('gnns','?')} "
        f"KGGen={tech.get('kggen','?')} "
        f"Synthetic={tech.get('synth','?')}"
    )

    user_msg = f"""VECTOR: {ctx['vector_id']}

FROM: {ctx['from_name']}
{ctx['from_desc'][:400]}

TO: {ctx['to_name']}
{ctx['to_desc'][:400]}

{tech_str}
Primary scalar driver (from graph): {tech.get('primary_driver', 'not yet classified')}

EVIDENCE ({len(ctx['evidence'])} item(s)):
{evidence_str or '  No evidence nodes yet — synthesise from BM descriptions and scalar data alone.'}

SCALAR IMPACTS ({len(ctx['scalar_impacts'])} classified):
{scalar_str or '  No scalar impacts classified yet.'}

Generate a DisruptionHypothesis JSON object for this vector.
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


def write_hypothesis(driver, hypothesis: dict, vector_id: str) -> str:
    """Write DisruptionHypothesis node and GENERATED_FROM relationship."""
    now = datetime.now(timezone.utc).isoformat()
    hyp_id = hypothesis.get("hypothesis_id", f"HYP_{vector_id}")

    props = {
        "hypothesis_id":        hyp_id,
        "from_bim_id":          hypothesis.get("from_bim_id", ""),
        "to_bim_id":            hypothesis.get("to_bim_id", ""),
        "title":                hypothesis.get("title", ""),
        "thesis":               hypothesis.get("thesis", ""),
        "conviction_score":     float(hypothesis.get("conviction_score", 0.5)),
        "primary_scalar_driver": hypothesis.get("primary_scalar_driver", ""),
        "supporting_scalars":   hypothesis.get("supporting_scalars", []),
        "evidence_count":       int(hypothesis.get("evidence_count", 0)),
        "companies_exposed":    hypothesis.get("companies_exposed", []),
        "time_horizon":         hypothesis.get("time_horizon", "2-5 years"),
        "disruption_type":      hypothesis.get("disruption_type", "substitution"),
        "ai_technology_link":   hypothesis.get("ai_technology_link"),
        "counter_argument":     hypothesis.get("counter_argument", ""),
        "status":               "Hypothesis",
        "source":               "hypothesis_generator",
        "created_at":           now,
        "updated_at":           now,
        "pending_human_review": True,
    }

    with driver.session() as s:
        s.run("MERGE (n:DisruptionHypothesis {hypothesis_id:$id}) SET n += $props",
              id=hyp_id, props=props)

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


def generate_hypothesis(
    from_bim_id: str,
    to_bim_id: str,
    dry_run: bool = False,
) -> dict:
    """
    Generate a DisruptionHypothesis for the given vector.

    Returns:
    {
      "hypothesis_id": str,
      "vector_id": str,
      "conviction_score": float,
      "status": "written" | "dry_run" | "low_conviction_skipped" | "error",
      "hypothesis": dict,
    }
    """
    vector_id = f"VEC_{from_bim_id}_{to_bim_id}"
    console.print(f"\n[bold]Hypothesis Generator[/bold] — {vector_id}")

    driver = get_driver()
    ctx = gather_vector_context(driver, from_bim_id, to_bim_id)

    console.print(f"  From: {ctx['from_name']}")
    console.print(f"  To:   {ctx['to_name']}")
    console.print(f"  Evidence nodes: {len(ctx['evidence'])}")
    console.print(f"  Scalar impacts: {len(ctx['scalar_impacts'])}")
    console.print("  [dim]Calling Claude...[/dim]")

    try:
        hypothesis = call_claude_generate(ctx)
    except Exception as e:
        driver.close()
        return {
            "hypothesis_id": None,
            "vector_id": vector_id,
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
        hyp_id = hypothesis.get("hypothesis_id", f"HYP_{vector_id}")
    else:
        hyp_id = write_hypothesis(driver, hypothesis, vector_id)
        status = "written"

    driver.close()

    # Display
    console.print(Panel(
        f"[bold]Title:[/bold] {hypothesis.get('title', '')}\n\n"
        f"[bold]Thesis:[/bold]\n{hypothesis.get('thesis', '')}\n\n"
        f"[bold]Conviction:[/bold] {conviction:.2f}  |  "
        f"[bold]Type:[/bold] {hypothesis.get('disruption_type', '?')}  |  "
        f"[bold]Horizon:[/bold] {hypothesis.get('time_horizon', '?')}\n\n"
        f"[bold]Primary driver:[/bold] {hypothesis.get('primary_scalar_driver', '?')}\n"
        f"[bold]AI link:[/bold] {hypothesis.get('ai_technology_link', 'none')}\n\n"
        f"[dim]Counter: {hypothesis.get('counter_argument', '')}[/dim]",
        title=f"Hypothesis: {hyp_id or 'not written'}",
    ))

    return {
        "hypothesis_id":   hyp_id,
        "vector_id":       vector_id,
        "conviction_score": conviction,
        "status":          status,
        "hypothesis":      hypothesis,
    }
