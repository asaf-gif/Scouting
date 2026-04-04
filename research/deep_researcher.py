"""
research/deep_researcher.py — Deep Research Agent (Part 18)
research/counter_researcher.py logic also lives here (Part 19)

Given a DisruptionHypothesis, runs targeted web searches and calls Claude
to produce a research brief and a counter-brief. Writes both as Evaluation
nodes linked to the hypothesis.

Usage:
    from research.deep_researcher import research_hypothesis
    result = research_hypothesis("HYP_BIM_026_BIM_027")

    from research.deep_researcher import counter_research_hypothesis
    result = counter_research_hypothesis("HYP_BIM_026_BIM_027")
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
from tavily import TavilyClient
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

load_dotenv(override=True)
console = Console(width=200)


def get_driver():
    return GraphDatabase.driver(
        os.getenv("NEO4J_URI"),
        auth=(os.getenv("NEO4J_USER", "neo4j"), os.getenv("NEO4J_PASSWORD")),
    )


def fetch_hypothesis(driver, hypothesis_id: str) -> dict:
    with driver.session() as s:
        rec = s.run("""
            MATCH (h:DisruptionHypothesis {hypothesis_id: $hid})
            OPTIONAL MATCH (f:BusinessModel {bim_id: h.from_bim_id})
            OPTIONAL MATCH (t:BusinessModel {bim_id: h.to_bim_id})
            OPTIONAL MATCH (e:Evidence)-[:SUPPORTS]->(v:TransformationVector)
                           <-[:GENERATED_FROM]-(h)
            RETURN h.hypothesis_id       AS hid,
                   h.title               AS title,
                   h.thesis              AS thesis,
                   h.counter_argument    AS counter,
                   h.conviction_score    AS conviction,
                   h.primary_scalar_driver AS primary_scalar,
                   h.companies_exposed   AS companies_exposed,
                   h.ai_technology_link  AS ai_link,
                   h.disruption_type     AS dtype,
                   f.name AS from_name, f.description AS from_desc,
                   t.name AS to_name,   t.description AS to_desc,
                   collect(DISTINCT e.evidence_quote)[..3] AS existing_quotes
        """, hid=hypothesis_id).single()
    return dict(rec) if rec else {}


def load_prompt(name: str) -> str:
    path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "prompts", name,
    )
    with open(path) as f:
        return f.read()


def run_research_queries(hypothesis: dict, mode: str = "support") -> str:
    """Run Tavily searches. mode='support' for corroborating, 'counter' for adversarial."""
    client = TavilyClient(api_key=os.getenv("TAVILY_API_KEY"))
    from_name = hypothesis.get("from_name", "")
    to_name   = hypothesis.get("to_name", "")
    title     = hypothesis.get("title", "")
    companies = hypothesis.get("companies_exposed") or []
    co_str    = " ".join(companies[:3])

    if mode == "support":
        queries = [
            f"{from_name} to {to_name} business model transition examples companies 2024 2025",
            f"{title} market evidence disruption {co_str}",
            f"{hypothesis.get('ai_link','')} enabling {to_name} business model",
        ]
    else:  # counter
        queries = [
            f"{from_name} business model resilient NOT disrupted reasons 2024 2025",
            f"{to_name} business model failure limitations barriers adoption",
            f"{from_name} companies successfully defending against {to_name} transition",
        ]

    sections = []
    for q in queries:
        if not q.strip():
            continue
        try:
            resp = client.search(q.strip(), max_results=5, include_answer=True)
            answer = resp.get("answer", "")
            block = f"Query: {q}\n"
            if answer:
                block += f"Summary: {answer}\n"
            for r in resp.get("results", []):
                block += f"- {r.get('title','')} ({r.get('url','')}): {r.get('content','')[:350]}\n"
            sections.append(block)
        except Exception as e:
            sections.append(f"Query: {q}\nSearch error: {e}\n")
    return "\n\n".join(sections)


def call_claude(system_prompt: str, user_message: str) -> dict:
    client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
    response = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=3000,
        system=system_prompt,
        messages=[{"role": "user", "content": user_message}],
    )
    raw = response.content[0].text.strip()
    obj = re.search(r"\{[\s\S]*\}", raw)
    if not obj:
        raise ValueError(f"No JSON in response: {raw[:300]}")
    return json.loads(obj.group(0))


def write_evaluation(driver, hypothesis_id: str, eval_type: str,
                     result: dict, confidence: float) -> str:
    now = datetime.now(timezone.utc).isoformat()
    eval_id = f"EVAL_{hypothesis_id}_{eval_type.upper()}"

    props = {
        "evaluation_id":   eval_id,
        "hypothesis_id":   hypothesis_id,
        "evaluation_type": eval_type,
        "content_json":    json.dumps(result),
        "confidence":      confidence,
        "evaluated_at":    now,
        "evaluated_by":    f"{eval_type}_agent",
        "status":          "Complete",
    }

    with driver.session() as s:
        s.run("MERGE (e:Evaluation {evaluation_id:$id}) SET e += $props",
              id=eval_id, props=props)
        s.run("""
            MATCH (ev:Evaluation {evaluation_id:$eid})
            MATCH (h:DisruptionHypothesis {hypothesis_id:$hid})
            MERGE (ev)-[r:EVALUATES]->(h)
            SET r.eval_type = $etype, r.created_at = $now
        """, eid=eval_id, hid=hypothesis_id, etype=eval_type, now=now)

    return eval_id


def research_hypothesis(hypothesis_id: str, dry_run: bool = False) -> dict:
    """Run deep supporting research on a hypothesis."""
    console.print(f"\n[bold]Deep Research Agent[/bold] — {hypothesis_id}")

    driver = get_driver()
    hypothesis = fetch_hypothesis(driver, hypothesis_id)
    if not hypothesis:
        driver.close()
        return {"status": "error", "message": f"Hypothesis {hypothesis_id} not found"}

    console.print(f"  Title: {hypothesis.get('title','')}")
    console.print("  [dim]Step 1/3 — Research queries...[/dim]")
    search_context = run_research_queries(hypothesis, mode="support")
    console.print(f"  Retrieved {len(search_context)} chars")

    console.print("  [dim]Step 2/3 — Calling Claude...[/dim]")
    existing_quotes = hypothesis.get("existing_quotes") or []
    user_msg = f"""
HYPOTHESIS: {hypothesis_id}
Title: {hypothesis.get('title','')}
Thesis: {hypothesis.get('thesis','')}
From: {hypothesis.get('from_name','')} → To: {hypothesis.get('to_name','')}
Primary scalar: {hypothesis.get('primary_scalar','')}
AI link: {hypothesis.get('ai_link','')}
Existing evidence quotes: {chr(10).join(existing_quotes)}

WEB RESEARCH RESULTS:
{search_context}

Produce the research brief JSON.
"""
    brief = call_claude(load_prompt("deep_research.txt"), user_msg)
    validation_confidence = float(brief.get("validation_confidence", 0.5))

    eval_id = None
    if not dry_run:
        console.print("  [dim]Step 3/3 — Writing Evaluation node...[/dim]")
        eval_id = write_evaluation(driver, hypothesis_id, "deep_research",
                                   brief, validation_confidence)
        # Update conviction on hypothesis
        with driver.session() as s:
            s.run("""
                MATCH (h:DisruptionHypothesis {hypothesis_id:$hid})
                SET h.research_confidence = $rc,
                    h.companies_actively_transitioning = $cos,
                    h.researched_at = $now
            """,
                hid=hypothesis_id,
                rc=validation_confidence,
                cos=brief.get("companies_actively_transitioning", []),
                now=datetime.now(timezone.utc).isoformat(),
            )

    driver.close()

    # Display
    sup = brief.get("supporting_evidence", [])
    ref = brief.get("refuting_evidence", [])
    console.print(Panel(
        f"[bold]Research summary:[/bold]\n{brief.get('research_summary','')}\n\n"
        f"[green]Supporting evidence:[/green] {len(sup)} item(s)\n" +
        "\n".join(f"  [{e['strength']}] {e['claim'][:100]}" for e in sup[:3]) +
        f"\n\n[red]Refuting evidence:[/red] {len(ref)} item(s)\n" +
        "\n".join(f"  [{e['strength']}] {e['claim'][:100]}" for e in ref[:3]) +
        f"\n\n[bold]Validation confidence:[/bold] {validation_confidence:.2f}  "
        f"|  Horizon: {brief.get('time_horizon_assessment','?')}",
        title=f"Research Brief: {eval_id or 'DRY RUN'}",
    ))

    return {
        "status": "written" if not dry_run else "dry_run",
        "evaluation_id": eval_id,
        "brief": brief,
        "validation_confidence": validation_confidence,
    }


def counter_research_hypothesis(hypothesis_id: str, dry_run: bool = False) -> dict:
    """Run adversarial counter-research on a hypothesis."""
    console.print(f"\n[bold]Counter Research Agent[/bold] — {hypothesis_id}")

    driver = get_driver()
    hypothesis = fetch_hypothesis(driver, hypothesis_id)
    if not hypothesis:
        driver.close()
        return {"status": "error", "message": f"Hypothesis {hypothesis_id} not found"}

    console.print(f"  Title: {hypothesis.get('title','')}")
    console.print("  [dim]Step 1/3 — Adversarial queries...[/dim]")
    search_context = run_research_queries(hypothesis, mode="counter")
    console.print(f"  Retrieved {len(search_context)} chars")

    console.print("  [dim]Step 2/3 — Calling Claude (devil's advocate)...[/dim]")
    user_msg = f"""
HYPOTHESIS TO CHALLENGE: {hypothesis_id}
Title: {hypothesis.get('title','')}
Thesis: {hypothesis.get('thesis','')}
From: {hypothesis.get('from_name','')} → To: {hypothesis.get('to_name','')}
Analyst's own counter: {hypothesis.get('counter','')}

ADVERSARIAL SEARCH RESULTS:
{search_context}

Find the strongest case AGAINST this hypothesis. Return counter_research JSON.
"""
    counter = call_claude(load_prompt("counter_research.txt"), user_msg)
    adversarial_confidence = float(counter.get("adversarial_confidence", 0.5))

    eval_id = None
    if not dry_run:
        console.print("  [dim]Step 3/3 — Writing Evaluation node...[/dim]")
        eval_id = write_evaluation(driver, hypothesis_id, "counter_research",
                                   counter, adversarial_confidence)
        with driver.session() as s:
            s.run("""
                MATCH (h:DisruptionHypothesis {hypothesis_id:$hid})
                SET h.counter_confidence = $cc,
                    h.counter_researched_at = $now
            """,
                hid=hypothesis_id,
                cc=adversarial_confidence,
                now=datetime.now(timezone.utc).isoformat(),
            )

    driver.close()

    barriers = counter.get("structural_barriers", [])
    console.print(Panel(
        f"[bold]Counter thesis:[/bold]\n{counter.get('counter_thesis','')}\n\n"
        f"[red]Structural barriers:[/red] {len(barriers)}\n" +
        "\n".join(f"  [{b['severity']}] {b['barrier']}: {b['description'][:80]}" for b in barriers[:3]) +
        f"\n\n[bold]Adversarial confidence:[/bold] {adversarial_confidence:.2f} "
        f"(higher = hypothesis more likely WRONG)",
        title=f"Counter Brief: {eval_id or 'DRY RUN'}",
    ))

    return {
        "status": "written" if not dry_run else "dry_run",
        "evaluation_id": eval_id,
        "counter": counter,
        "adversarial_confidence": adversarial_confidence,
    }
