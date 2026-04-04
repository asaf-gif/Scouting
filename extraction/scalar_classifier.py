"""
extraction/scalar_classifier.py — Scalar Classification Agent

Given a TransformationVector (by ID or from/to pair) and supporting
evidence text, classifies which of the 26 scalar conditions are activated
and at what strength. Writes IMPACTS relationships with scores.

Usage:
    from extraction.scalar_classifier import classify_vector_scalars

    result = classify_vector_scalars(
        from_bim_id="BIM_015",
        to_bim_id="BIM_027",
        evidence_text="...",
    )
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
from rich.table import Table

load_dotenv(override=True)
console = Console(width=200)

IMPACT_SCORE = {
    ("increases", "strong"):   2,
    ("increases", "moderate"): 1,
    ("neutral",   "weak"):     0,
    ("decreases", "moderate"): -1,
    ("decreases", "strong"):   -2,
}


def get_driver():
    return GraphDatabase.driver(
        os.getenv("NEO4J_URI"),
        auth=(os.getenv("NEO4J_USER", "neo4j"), os.getenv("NEO4J_PASSWORD")),
    )


def get_bm_descriptions(driver, from_bim: str, to_bim: str) -> tuple[str, str]:
    with driver.session() as s:
        f = s.run("MATCH (n:BusinessModel {bim_id:$id}) RETURN n.name AS name, coalesce(n.description,'') AS desc",
                  id=from_bim).single()
        t = s.run("MATCH (n:BusinessModel {bim_id:$id}) RETURN n.name AS name, coalesce(n.description,'') AS desc",
                  id=to_bim).single()
    from_desc = f"{f['name']}: {f['desc'][:300]}" if f else from_bim
    to_desc   = f"{t['name']}: {t['desc'][:300]}" if t else to_bim
    return from_desc, to_desc


def get_all_scalars(driver) -> list[dict]:
    with driver.session() as s:
        return s.run("""
            MATCH (n:Scalar)
            RETURN n.scalar_id AS id, n.name AS name
            ORDER BY n.scalar_id
        """).data()


def get_existing_impacts(driver, vector_id: str) -> list[str]:
    """Scalar IDs already linked via IMPACTS."""
    with driver.session() as s:
        result = s.run("""
            MATCH (v:TransformationVector {vector_id:$vid})-[:IMPACTS]-(sc:Scalar)
            RETURN sc.scalar_id AS sid
        """, vid=vector_id)
        return [r["sid"] for r in result]


def load_prompt() -> str:
    path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "prompts", "scalar_classification.txt",
    )
    with open(path) as f:
        return f.read()


def call_claude_classify(
    from_bim_id: str, to_bim_id: str,
    from_desc: str, to_desc: str,
    evidence_text: str,
    scalars: list[dict],
    already_classified: list[str],
) -> dict:
    client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

    scalar_list = "\n".join(f"- {s['id']}: {s['name'][:120]}" for s in scalars)
    already_str = ", ".join(already_classified) if already_classified else "none"
    vector_id = f"VEC_{from_bim_id}_{to_bim_id}"

    user_msg = f"""TRANSITION TO CLASSIFY:
FROM: {from_desc}
TO:   {to_desc}

EVIDENCE TEXT:
{evidence_text[:3000]}

SCALAR CONDITIONS:
{scalar_list}

ALREADY CLASSIFIED SCALARS (do not duplicate): {already_str}

Return a JSON object with the scalar_impacts for vector {vector_id}.
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
        return {}
    try:
        return json.loads(obj.group(0))
    except json.JSONDecodeError:
        return {}


def write_scalar_impacts(driver, vector_id: str, classification: dict) -> int:
    """Write IMPACTS relationships. Returns count written."""
    now = datetime.now(timezone.utc).isoformat()
    written = 0

    for impact in classification.get("scalar_impacts", []):
        sid      = impact.get("scalar_id", "")
        direction = impact.get("direction", "neutral")
        strength  = impact.get("impact_strength", "moderate")
        score     = IMPACT_SCORE.get((direction, strength), 0)
        rationale = impact.get("rationale", "")

        if not sid:
            continue

        with driver.session() as s:
            result = s.run("""
                MATCH (v:TransformationVector {vector_id: $vid})
                MATCH (sc:Scalar {scalar_id: $sid})
                MERGE (v)-[r:IMPACTS]->(sc)
                SET r.direction       = $dir,
                    r.impact_strength = $strength,
                    r.impact_score    = $score,
                    r.impact          = $score,
                    r.rationale       = $rationale,
                    r.classified_by   = 'scalar_classifier',
                    r.classified_at   = $now
                RETURN r
            """,
                vid=vector_id, sid=sid,
                dir=direction, strength=strength,
                score=score, rationale=rationale,
                now=now,
            )
            if result.single():
                written += 1

    # Store primary driver on the vector
    primary = classification.get("primary_driver")
    if primary:
        with driver.session() as s:
            s.run("""
                MATCH (v:TransformationVector {vector_id: $vid})
                SET v.primary_scalar_driver = $sid,
                    v.scalar_reasoning      = $reasoning
            """,
                vid=vector_id,
                sid=primary,
                reasoning=classification.get("reasoning", ""),
            )
    return written


def classify_vector_scalars(
    from_bim_id: str,
    to_bim_id: str,
    evidence_text: str = "",
    dry_run: bool = False,
) -> dict:
    """
    Classify scalar impacts for a specific vector.

    Returns:
    {
      "vector_id": str,
      "scalars_written": int,
      "primary_driver": str,
      "classification": dict (raw Claude output),
      "status": "classified" | "dry_run" | "error",
    }
    """
    vector_id = f"VEC_{from_bim_id}_{to_bim_id}"
    console.print(f"\n[bold]Scalar Classifier[/bold] — {vector_id}")

    driver = get_driver()
    from_desc, to_desc = get_bm_descriptions(driver, from_bim_id, to_bim_id)
    scalars   = get_all_scalars(driver)
    existing  = get_existing_impacts(driver, vector_id)

    console.print(f"  From: {from_desc[:80]}")
    console.print(f"  To:   {to_desc[:80]}")
    console.print(f"  Already classified: {len(existing)} scalar(s)")
    console.print("  [dim]Calling Claude...[/dim]")

    try:
        classification = call_claude_classify(
            from_bim_id, to_bim_id,
            from_desc, to_desc,
            evidence_text, scalars, existing,
        )
    except Exception as e:
        driver.close()
        return {"vector_id": vector_id, "scalars_written": 0,
                "primary_driver": None, "classification": {}, "status": f"error: {e}"}

    n_impacts = len(classification.get("scalar_impacts", []))
    console.print(f"  Classification: {n_impacts} scalar impact(s)")

    written = 0
    if not dry_run:
        written = write_scalar_impacts(driver, vector_id, classification)

    driver.close()

    # Display table
    table = Table(title=f"Scalar Impacts — {vector_id}", show_header=True)
    table.add_column("Scalar", width=10)
    table.add_column("Name", width=45)
    table.add_column("Direction", width=10)
    table.add_column("Strength", width=10)
    table.add_column("Score", justify="right", width=6)

    for imp in classification.get("scalar_impacts", []):
        score = IMPACT_SCORE.get(
            (imp.get("direction", "neutral"), imp.get("impact_strength", "moderate")), 0
        )
        color = "green" if score > 0 else ("red" if score < 0 else "dim")
        table.add_row(
            imp.get("scalar_id", ""),
            imp.get("scalar_name", "")[:44],
            imp.get("direction", ""),
            imp.get("impact_strength", ""),
            f"[{color}]{score:+d}[/{color}]",
        )
    console.print(table)

    primary = classification.get("primary_driver")
    console.print(f"\n  [bold]Primary driver:[/bold] {primary}")
    console.print(f"  [dim]{classification.get('reasoning', '')}[/dim]")

    return {
        "vector_id":      vector_id,
        "scalars_written": written,
        "primary_driver": primary,
        "classification": classification,
        "status":         "dry_run" if dry_run else "classified",
    }
