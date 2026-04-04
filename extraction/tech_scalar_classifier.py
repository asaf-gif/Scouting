"""
extraction/tech_scalar_classifier.py — Technology Scalar Fingerprint Classifier

Given a Technology node, classifies which of the 26 scalar conditions it moves
(increases / decreases) and at what strength. Writes MOVES_SCALAR relationships
from the Technology node to each affected Scalar node.

This is Step 1 of the new hypothesis chain:
  Technology → (moves) Scalars → (activates) TransformationVectors → Hypothesis

Usage:
    from extraction.tech_scalar_classifier import classify_tech_scalars

    result = classify_tech_scalars("TECH_001")
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

try:
    from core.error_log import log_error, capture_errors
    _LOG_AVAILABLE = True
except ImportError:
    _LOG_AVAILABLE = False
    def log_error(*a, **k): pass
    def capture_errors(context_keys=None):
        def decorator(fn): return fn
        return decorator

IMPACT_SCORE = {
    ("increases", "strong"):   2,
    ("increases", "moderate"): 1,
    ("increases", "weak"):     1,
    ("decreases", "weak"):    -1,
    ("decreases", "moderate"): -1,
    ("decreases", "strong"):   -2,
}


def get_driver():
    return GraphDatabase.driver(
        os.getenv("NEO4J_URI"),
        auth=(os.getenv("NEO4J_USER", "neo4j"), os.getenv("NEO4J_PASSWORD")),
    )


def get_technology(driver, tech_id: str) -> dict:
    with driver.session() as s:
        rec = s.run("""
            MATCH (t:Technology {tech_id: $id})
            RETURN t.tech_id AS id, t.name AS name, t.short_name AS short_name,
                   t.description AS description, t.disruption_thesis AS thesis,
                   t.category AS category, t.primary_use_cases AS use_cases,
                   t.maturity_level AS maturity
        """, id=tech_id).single()
        if not rec:
            raise ValueError(f"Technology {tech_id} not found")
        return dict(rec)


def get_all_scalars(driver) -> list[dict]:
    with driver.session() as s:
        return s.run("""
            MATCH (n:Scalar)
            RETURN n.scalar_id AS id, n.name AS name, n.description AS description
            ORDER BY n.scalar_id
        """).data()


def get_existing_movements(driver, tech_id: str) -> list[str]:
    """Scalar IDs already linked via MOVES_SCALAR."""
    with driver.session() as s:
        result = s.run("""
            MATCH (t:Technology {tech_id: $tid})-[:MOVES_SCALAR]->(sc:Scalar)
            RETURN sc.scalar_id AS sid
        """, tid=tech_id)
        return [r["sid"] for r in result]


def load_prompt() -> str:
    path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "prompts", "tech_scalar_classification.txt",
    )
    with open(path) as f:
        return f.read()


def call_claude_classify(tech: dict, scalars: list[dict]) -> dict:
    client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

    scalar_list = "\n".join(
        f"- {s['id']}: {s['name'][:120]}"
        for s in scalars
    )

    use_cases = ""
    if tech.get("use_cases"):
        use_cases = "\nPrimary use cases:\n" + "\n".join(
            f"  - {u}" for u in (tech["use_cases"] or [])
        )

    user_msg = f"""TECHNOLOGY TO CLASSIFY:
ID: {tech['id']}
Name: {tech['name']}
Category: {tech.get('category', 'unknown')}
Maturity: {tech.get('maturity', '?')}/100

Description:
{tech.get('description', '')}

Disruption Thesis:
{tech.get('thesis', '') or '(not yet written)'}
{use_cases}

SCALAR CONDITIONS (classify which ones this technology moves):
{scalar_list}

Return the JSON scalar fingerprint for technology {tech['id']}.
"""

    response = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=3000,
        system=load_prompt(),
        messages=[{"role": "user", "content": user_msg}],
    )
    raw = response.content[0].text.strip()
    obj = re.search(r"\{[\s\S]*\}", raw)
    if not obj:
        raise ValueError(f"Claude returned no JSON. Raw: {raw[:300]}")
    return json.loads(obj.group(0))


def write_scalar_movements(driver, tech_id: str, classification: dict) -> int:
    """Write MOVES_SCALAR relationships. Returns count written."""
    now = datetime.now(timezone.utc).isoformat()
    written = 0

    for movement in classification.get("scalar_movements", []):
        sid       = movement.get("scalar_id", "")
        direction = movement.get("direction", "increases")
        strength  = movement.get("strength", "moderate")
        score     = IMPACT_SCORE.get((direction, strength), 0)
        rationale = movement.get("rationale", "")

        if not sid:
            continue

        with driver.session() as s:
            result = s.run("""
                MATCH (t:Technology {tech_id: $tid})
                MATCH (sc:Scalar {scalar_id: $sid})
                MERGE (t)-[r:MOVES_SCALAR]->(sc)
                SET r.direction       = $dir,
                    r.strength        = $strength,
                    r.score           = $score,
                    r.rationale       = $rationale,
                    r.classified_by   = 'tech_scalar_classifier',
                    r.classified_at   = $now
                RETURN r
            """,
                tid=tech_id, sid=sid,
                dir=direction, strength=strength,
                score=score, rationale=rationale,
                now=now,
            )
            if result.single():
                written += 1

    # Store primary driver and reasoning on the Technology node
    primary = classification.get("primary_scalar_driver")
    reasoning = classification.get("reasoning", "")
    if primary:
        with driver.session() as s:
            s.run("""
                MATCH (t:Technology {tech_id: $tid})
                SET t.primary_scalar_driver   = $sid,
                    t.scalar_reasoning        = $reasoning,
                    t.scalars_classified_at   = $now
            """,
                tid=tech_id,
                sid=primary,
                reasoning=reasoning,
                now=now,
            )

    return written


@capture_errors(context_keys=["tech_id"])
def classify_tech_scalars(tech_id: str, dry_run: bool = False) -> dict:
    """
    Classify which scalars a technology moves.

    Returns:
    {
      "tech_id": str,
      "tech_name": str,
      "movements_written": int,
      "primary_driver": str,
      "classification": dict (raw Claude output),
      "status": "classified" | "dry_run" | "error",
    }
    """
    console.print(f"\n[bold]Tech Scalar Classifier[/bold] — {tech_id}")

    driver = get_driver()
    tech    = get_technology(driver, tech_id)
    scalars = get_all_scalars(driver)
    existing = get_existing_movements(driver, tech_id)

    console.print(f"  Technology: {tech['name']}")
    console.print(f"  Already classified: {len(existing)} scalar movement(s)")
    console.print("  [dim]Calling Claude...[/dim]")

    try:
        classification = call_claude_classify(tech, scalars)
    except Exception as e:
        log_error("extraction.tech_scalar_classifier", "classify_tech_scalars", e,
                  context={"tech_id": tech_id})
        driver.close()
        return {
            "tech_id": tech_id,
            "tech_name": tech.get("name", ""),
            "movements_written": 0,
            "primary_driver": None,
            "classification": {},
            "status": f"error: {e}",
        }

    n_movements = len(classification.get("scalar_movements", []))
    console.print(f"  Classified: {n_movements} scalar movement(s)")

    written = 0
    if not dry_run:
        written = write_scalar_movements(driver, tech_id, classification)
        console.print(f"  Written: {written} MOVES_SCALAR relationships")

    driver.close()

    # Display table
    table = Table(title=f"Scalar Fingerprint — {tech['name']}", show_header=True)
    table.add_column("Scalar", width=10)
    table.add_column("Name", width=55)
    table.add_column("Direction", width=10)
    table.add_column("Strength", width=10)
    table.add_column("Score", justify="right", width=6)
    table.add_column("Rationale", width=60)

    for mv in classification.get("scalar_movements", []):
        score = IMPACT_SCORE.get(
            (mv.get("direction", "increases"), mv.get("strength", "moderate")), 0
        )
        color = "green" if score > 0 else "red"
        table.add_row(
            mv.get("scalar_id", ""),
            mv.get("scalar_name", "")[:54],
            mv.get("direction", ""),
            mv.get("strength", ""),
            f"[{color}]{score:+d}[/{color}]",
            mv.get("rationale", "")[:59],
        )
    console.print(table)

    primary = classification.get("primary_scalar_driver")
    console.print(f"\n  [bold]Primary driver:[/bold] {primary}")
    console.print(f"  [dim]{classification.get('reasoning', '')}[/dim]")

    return {
        "tech_id":           tech_id,
        "tech_name":         tech.get("name", ""),
        "movements_written": written,
        "primary_driver":    primary,
        "classification":    classification,
        "status":            "dry_run" if dry_run else "classified",
    }


def classify_all_technologies(dry_run: bool = False) -> list[dict]:
    """Classify scalar fingerprints for all Technology nodes."""
    driver = get_driver()
    with driver.session() as s:
        techs = s.run(
            "MATCH (t:Technology) RETURN t.tech_id AS id ORDER BY t.tech_id"
        ).data()
    driver.close()

    results = []
    for t in techs:
        result = classify_tech_scalars(t["id"], dry_run=dry_run)
        results.append(result)

    console.print(f"\n[bold]Done.[/bold] Classified {len(results)} technologies.")
    return results


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Classify scalar fingerprints for technologies")
    parser.add_argument("tech_id", nargs="?", help="Tech ID (e.g. TECH_001) or omit for all")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if args.tech_id:
        classify_tech_scalars(args.tech_id, dry_run=args.dry_run)
    else:
        classify_all_technologies(dry_run=args.dry_run)
