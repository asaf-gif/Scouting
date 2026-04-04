"""
research/validation_scorer.py — Validation Scorer (Part 20)

Combines deep research + counter-research evaluations into a final
validation_score and updates the hypothesis status.

validation_score = research_confidence × 0.60 + (1 - adversarial_confidence) × 0.40

Thresholds:
  ≥ 0.75 → status = 'Validated'
  0.50–0.74 → status = 'Hypothesis' (unchanged, needs more evidence)
  < 0.50 → status = 'Contested'

Usage:
    from research.validation_scorer import score_hypothesis
    result = score_hypothesis("HYP_BIM_026_BIM_027")
"""

import os
import sys
import json
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
from neo4j import GraphDatabase
from rich.console import Console
from rich.panel import Panel

load_dotenv(override=True)
console = Console(width=200)

VALIDATED_THRESHOLD = 0.75
CONTESTED_THRESHOLD = 0.50


def get_driver():
    return GraphDatabase.driver(
        os.getenv("NEO4J_URI"),
        auth=(os.getenv("NEO4J_USER", "neo4j"), os.getenv("NEO4J_PASSWORD")),
    )


def fetch_evaluations(driver, hypothesis_id: str) -> dict:
    with driver.session() as s:
        # Get both evaluation nodes
        evals = s.run("""
            MATCH (ev:Evaluation)-[:EVALUATES]->(h:DisruptionHypothesis {hypothesis_id:$hid})
            RETURN ev.evaluation_type AS etype,
                   ev.confidence AS conf,
                   ev.content_json AS content_json
        """, hid=hypothesis_id).data()

        # Get hypothesis fields
        h = s.run("""
            MATCH (h:DisruptionHypothesis {hypothesis_id:$hid})
            RETURN h.conviction_score AS conviction,
                   h.research_confidence AS research_conf,
                   h.counter_confidence AS counter_conf,
                   h.title AS title, h.status AS status
        """, hid=hypothesis_id).single()

    eval_map = {e["etype"]: e for e in evals}
    return {"hypothesis": dict(h) if h else {}, "evals": eval_map}


def compute_validation_score(research_conf: float, adversarial_conf: float,
                              conviction: float) -> dict:
    """
    validation_score = research_conf × 0.60 + (1 - adversarial_conf) × 0.40

    Also blends original conviction as a prior if research is missing.
    """
    has_research = research_conf > 0
    has_counter  = adversarial_conf > 0

    if has_research and has_counter:
        score = research_conf * 0.60 + (1 - adversarial_conf) * 0.40
    elif has_research:
        score = research_conf * 0.60 + conviction * 0.40
    elif has_counter:
        score = conviction * 0.60 + (1 - adversarial_conf) * 0.40
    else:
        score = conviction

    score = round(score, 4)

    if score >= VALIDATED_THRESHOLD:
        status = "Validated"
    elif score < CONTESTED_THRESHOLD:
        status = "Contested"
    else:
        status = "Hypothesis"

    return {
        "validation_score": score,
        "status":           status,
        "research_conf":    research_conf,
        "adversarial_conf": adversarial_conf,
        "conviction":       conviction,
        "has_research":     has_research,
        "has_counter":      has_counter,
    }


def write_validation(driver, hypothesis_id: str, scores: dict) -> None:
    now = datetime.now(timezone.utc).isoformat()
    with driver.session() as s:
        s.run("""
            MATCH (h:DisruptionHypothesis {hypothesis_id:$hid})
            SET h.validation_score    = $vs,
                h.status              = $status,
                h.validated_at        = $now,
                h.pending_human_review = CASE WHEN $status = 'Validated' THEN false ELSE h.pending_human_review END
        """,
            hid=hypothesis_id,
            vs=scores["validation_score"],
            status=scores["status"],
            now=now,
        )


def generate_research_brief_text(hypothesis_id: str, driver) -> str:
    """Generate a human-readable research brief from stored evaluation data."""
    with driver.session() as s:
        h = s.run("""
            MATCH (h:DisruptionHypothesis {hypothesis_id:$hid})
            OPTIONAL MATCH (f:BusinessModel {bim_id: h.from_bim_id})
            OPTIONAL MATCH (t:BusinessModel {bim_id: h.to_bim_id})
            RETURN h, f.name AS from_name, t.name AS to_name
        """, hid=hypothesis_id).single()

        evals = s.run("""
            MATCH (ev:Evaluation)-[:EVALUATES]->(h:DisruptionHypothesis {hypothesis_id:$hid})
            RETURN ev.evaluation_type AS etype, ev.content_json AS cj
        """, hid=hypothesis_id).data()

    if not h:
        return "Hypothesis not found."

    node = dict(h["h"])
    brief_lines = [
        f"# Research Brief: {node.get('title','')}",
        f"**Hypothesis ID:** {hypothesis_id}",
        f"**Transition:** {h['from_name']} → {h['to_name']}",
        f"**Conviction:** {node.get('conviction_score',0):.2f}  "
        f"| **Validation:** {node.get('validation_score','pending')}  "
        f"| **Status:** {node.get('status','')}",
        "",
        "## Thesis",
        node.get("thesis", ""),
        "",
        "## Counter-argument",
        node.get("counter_argument", ""),
        "",
    ]

    for ev in evals:
        try:
            content = json.loads(ev["cj"])
        except Exception:
            continue

        if ev["etype"] == "deep_research":
            brief_lines += [
                "## Supporting Research",
                content.get("research_summary", ""),
                "",
                "**Supporting evidence:**",
            ]
            for e in content.get("supporting_evidence", [])[:4]:
                brief_lines.append(f"- [{e.get('strength','')}] {e.get('claim','')} — *{e.get('source','')}*")
            brief_lines += [
                "",
                "**Refuting evidence:**",
            ]
            for e in content.get("refuting_evidence", [])[:3]:
                brief_lines.append(f"- [{e.get('strength','')}] {e.get('claim','')} — *{e.get('source','')}*")
            brief_lines += [
                "",
                f"**Time horizon:** {content.get('time_horizon_assessment','')} — {content.get('time_horizon_rationale','')}",
                "",
            ]

        elif ev["etype"] == "counter_research":
            brief_lines += [
                "## Adversarial Analysis",
                content.get("counter_thesis", ""),
                "",
                "**Structural barriers:**",
            ]
            for b in content.get("structural_barriers", [])[:4]:
                brief_lines.append(f"- [{b.get('severity','')}] **{b.get('barrier','')}**: {b.get('description','')}")
            brief_lines.append("")

    return "\n".join(brief_lines)


def score_hypothesis(hypothesis_id: str, dry_run: bool = False) -> dict:
    """
    Compute and write validation score for a hypothesis.

    Returns:
    {
      "hypothesis_id": str,
      "validation_score": float,
      "status": str,
      "brief_text": str,
    }
    """
    console.print(f"\n[bold]Validation Scorer[/bold] — {hypothesis_id}")

    driver = get_driver()
    data = fetch_evaluations(driver, hypothesis_id)

    hyp = data["hypothesis"]
    evals = data["evals"]

    conviction    = float(hyp.get("conviction") or 0.5)
    research_conf = float(hyp.get("research_conf") or
                          evals.get("deep_research", {}).get("conf") or 0.0)
    counter_conf  = float(hyp.get("counter_conf") or
                          evals.get("counter_research", {}).get("conf") or 0.0)

    scores = compute_validation_score(research_conf, counter_conf, conviction)

    console.print(f"  Title: {hyp.get('title','')}")
    console.print(f"  Conviction:     {conviction:.2f}")
    console.print(f"  Research conf:  {research_conf:.2f}")
    console.print(f"  Adversarial:    {counter_conf:.2f}")
    console.print(f"  → Validation score: [bold]{scores['validation_score']:.4f}[/bold]  "
                  f"Status: [bold]{scores['status']}[/bold]")

    brief_text = generate_research_brief_text(hypothesis_id, driver)

    if not dry_run:
        write_validation(driver, hypothesis_id, scores)

    driver.close()

    status_color = {"Validated": "green", "Contested": "red", "Hypothesis": "yellow"}
    color = status_color.get(scores["status"], "white")
    console.print(Panel(
        f"[bold]Validation score:[/bold] {scores['validation_score']:.4f}\n"
        f"[bold]Status:[/bold] [{color}]{scores['status']}[/{color}]\n\n"
        f"Research confidence:  {research_conf:.2f} (weight 0.60)\n"
        f"Adversarial confidence: {counter_conf:.2f} → inverted (weight 0.40)\n"
        f"Original conviction:  {conviction:.2f} (used as prior if research missing)",
        title="Validation Result",
    ))

    return {
        "hypothesis_id":    hypothesis_id,
        "validation_score": scores["validation_score"],
        "status":           scores["status"],
        "brief_text":       brief_text,
        "scores":           scores,
    }
