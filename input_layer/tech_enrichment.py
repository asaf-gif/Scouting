"""
input_layer/tech_enrichment.py — Technology Enrichment Agent

Given a technology name, researches it via Tavily, calls Claude to classify
its maturity, scalar impacts, and disruption thesis, then writes the node
to Neo4j and creates INFLUENCES relationships to Scalar nodes.

Usage (as a module):
    from input_layer.tech_enrichment import enrich_technology
    result = enrich_technology("Retrieval-Augmented Generation")

Usage (as CLI via add_tech.py):
    python input_layer/add_tech.py --name "Retrieval-Augmented Generation"
    python input_layer/add_tech.py --name "RAG" --dry-run
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


def get_existing_techs(driver) -> list[dict]:
    with driver.session() as s:
        result = s.run("""
            MATCH (n:Technology)
            RETURN n.tech_id AS id, n.name AS name,
                   coalesce(n.short_name, '') AS short_name
            ORDER BY n.tech_id
        """)
        return result.data()


def get_all_scalars(driver) -> list[dict]:
    with driver.session() as s:
        result = s.run("""
            MATCH (n:Scalar)
            RETURN n.scalar_id AS id, n.name AS name
            ORDER BY n.scalar_id
        """)
        return result.data()


def next_tech_id(driver) -> str:
    with driver.session() as s:
        result = s.run("""
            MATCH (n:Technology)
            RETURN n.tech_id AS id ORDER BY n.tech_id DESC LIMIT 1
        """)
        rec = result.single()
    if not rec or not rec["id"]:
        return "TECH_001"
    num = int(rec["id"].split("_")[1]) + 1
    return f"TECH_{num:03d}"


def tech_exists(driver, name: str) -> dict | None:
    with driver.session() as s:
        result = s.run(
            """MATCH (n:Technology)
               WHERE toLower(n.name) = toLower($name)
                  OR toLower(n.short_name) = toLower($name)
                  OR toLower(n.name) CONTAINS toLower($name)
                  OR toLower($name) CONTAINS toLower(n.name)
               RETURN n LIMIT 1""",
            name=name,
        )
        rec = result.single()
    return dict(rec["n"]) if rec else None


def web_search(name: str) -> str:
    client = TavilyClient(api_key=os.getenv("TAVILY_API_KEY"))
    queries = [
        f'"{name}" technology how it works applications use cases AI',
        f'"{name}" maturity benchmarks adoption production deployments 2024 2025',
        f'"{name}" business model disruption impact companies using it',
    ]
    sections = []
    for q in queries:
        try:
            resp = client.search(q, max_results=4, include_answer=True)
            answer = resp.get("answer", "")
            results = resp.get("results", [])
            block = f"Query: {q}\n"
            if answer:
                block += f"Summary: {answer}\n"
            for r in results:
                block += f"- {r.get('title', '')} ({r.get('url', '')}): {r.get('content', '')[:300]}\n"
            sections.append(block)
        except Exception as e:
            sections.append(f"Query: {q}\nSearch error: {e}\n")
    return "\n\n".join(sections)


def load_system_prompt() -> str:
    prompt_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "prompts", "tech_enrichment.txt",
    )
    with open(prompt_path) as f:
        return f.read()


def call_claude(name: str, search_context: str, scalars: list[dict]) -> dict:
    client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

    scalar_list = "\n".join(
        f"- {s['id']}: {s['name'][:120]}"
        for s in scalars
    )

    user_message = f"""
TECHNOLOGY TO RESEARCH: {name}

WEB SEARCH RESULTS:
{search_context}

SCALAR CONDITIONS (classify impacts against these):
{scalar_list}

Return a single JSON object following the schema in your instructions.
"""

    response = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=3000,
        system=load_system_prompt(),
        messages=[{"role": "user", "content": user_message}],
    )

    raw = response.content[0].text.strip()
    json_match = re.search(r"\{[\s\S]*\}", raw)
    if not json_match:
        raise ValueError(f"Claude did not return valid JSON:\n{raw[:500]}")
    return json.loads(json_match.group(0))


def write_tech_to_graph(driver, tech_id: str, enriched: dict) -> None:
    now = datetime.now(timezone.utc).isoformat()

    props = {
        "tech_id":           tech_id,
        "name":              enriched.get("name", ""),
        "short_name":        enriched.get("short_name", ""),
        "category":          enriched.get("category", "AI/ML"),
        "description":       enriched.get("description", ""),
        "primary_use_cases": enriched.get("primary_use_cases", []),
        "key_players":       enriched.get("key_players", []),
        "maturity_level":    float(enriched.get("maturity_level", 50)),
        "maturity_rationale": enriched.get("maturity_rationale", ""),
        "maturity_source":   enriched.get("maturity_source", ""),
        "disruption_thesis": enriched.get("disruption_thesis", ""),
        "tracking_status":   enriched.get("tracking_status", "Active"),
        "confidence":        float(enriched.get("confidence", 0.8)),
        "source":            "manual_entry",
        "created_by":        "tech_enrichment_agent",
        "pending_human_review": True,
        "created_at":        now,
        "updated_at":        now,
    }

    scalar_impacts = enriched.get("scalar_impacts", [])

    with driver.session() as s:
        # Create Technology node
        s.run("MERGE (n:Technology {tech_id: $id}) SET n += $props",
              id=tech_id, props=props)

        # Create INFLUENCES relationships to Scalar nodes
        for impact in scalar_impacts:
            scalar_id = impact.get("scalar_id", "")
            if not scalar_id:
                continue
            s.run("""
                MATCH (t:Technology {tech_id: $tid})
                MATCH (sc:Scalar {scalar_id: $sid})
                MERGE (t)-[r:INFLUENCES]->(sc)
                SET r.direction       = $direction,
                    r.impact_strength = $strength,
                    r.rationale       = $rationale,
                    r.created_at      = $now
            """,
                tid=tech_id,
                sid=scalar_id,
                direction=impact.get("direction", "neutral"),
                strength=impact.get("impact_strength", "moderate"),
                rationale=impact.get("rationale", ""),
                now=now,
            )


def enrich_technology(name: str, dry_run: bool = False) -> dict:
    """
    Enrich a single technology.

    Returns:
    {
      "status": "created" | "duplicate_skipped" | "error",
      "tech_id": str or None,
      "enriched": dict,
      "message": str,
    }
    """
    console.print(f"\n[bold]Enriching technology:[/bold] {name}")

    driver = get_driver()

    # Duplicate check
    existing = tech_exists(driver, name)
    if existing:
        driver.close()
        console.print(f"  [yellow]↩ Skipped — already exists as {existing.get('tech_id')}[/yellow]")
        return {
            "status": "duplicate_skipped",
            "tech_id": existing.get("tech_id"),
            "enriched": existing,
            "message": f"Technology '{name}' already in graph as {existing.get('tech_id')}.",
        }

    console.print("  [dim]Step 1/3 — Web search...[/dim]")
    search_context = web_search(name)
    console.print(f"  Retrieved web context ({len(search_context)} chars)")

    scalars = get_all_scalars(driver)

    console.print("  [dim]Step 2/3 — Calling Claude...[/dim]")
    try:
        enriched = call_claude(name, search_context, scalars)
    except Exception as e:
        driver.close()
        return {"status": "error", "tech_id": None, "enriched": {}, "message": str(e)}

    console.print("  [dim]Step 3/3 — Writing to Neo4j...[/dim]")
    if not dry_run:
        tech_id = next_tech_id(driver)
        write_tech_to_graph(driver, tech_id, enriched)
    else:
        tech_id = "DRY_RUN"

    driver.close()

    scalar_impacts = enriched.get("scalar_impacts", [])

    # Display summary table
    table = Table(show_header=True, title=f"Technology created: {tech_id}")
    table.add_column("Field", style="bold", width=22)
    table.add_column("Value")
    table.add_row("ID", tech_id)
    table.add_row("Name", enriched.get("name", name))
    table.add_row("Short name", enriched.get("short_name", ""))
    table.add_row("Category", enriched.get("category", ""))
    table.add_row("Maturity", f"{enriched.get('maturity_level', '?')}/100 — {enriched.get('maturity_source', '')}")
    table.add_row("Key players", ", ".join(enriched.get("key_players", [])[:5]))
    table.add_row("Scalar impacts", str(len(scalar_impacts)))
    table.add_row("Description", enriched.get("description", "")[:120])
    console.print(table)

    console.print(Panel(
        enriched.get("disruption_thesis", ""),
        title="Disruption thesis",
    ))

    return {
        "status": "created",
        "tech_id": tech_id,
        "enriched": enriched,
        "message": f"Created {tech_id} — {len(scalar_impacts)} scalar impacts — pending human review.",
    }
