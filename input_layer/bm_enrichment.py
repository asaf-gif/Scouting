"""
input_layer/bm_enrichment.py — Business Model Enrichment Agent

Given a business model name, searches the web, calls Claude to fill all
schema fields, checks for duplicates, and writes the node to Neo4j.

Usage (as a module):
    from input_layer.bm_enrichment import enrich_business_model
    result = enrich_business_model("Embedded Finance")

Usage (as CLI via add_bm.py):
    python input_layer/add_bm.py --name 'Embedded Finance'
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

load_dotenv(override=True)
console = Console()

try:
    from core.editorial import get_constant as _gc
    SIMILARITY_THRESHOLD = _gc("duplicate_detection", "SIMILARITY_THRESHOLD", 0.85)
    REVIEW_THRESHOLD     = _gc("duplicate_detection", "REVIEW_THRESHOLD", 0.60)
except Exception:
    SIMILARITY_THRESHOLD = 0.85   # above this → treat as duplicate, block creation
    REVIEW_THRESHOLD     = 0.60   # above this → flag for human review even if not blocked


def get_driver():
    return GraphDatabase.driver(
        os.getenv("NEO4J_URI"),
        auth=(os.getenv("NEO4J_USER", "neo4j"), os.getenv("NEO4J_PASSWORD")),
    )


def get_existing_bms(driver) -> list[dict]:
    """Fetch name + description of all active BusinessModel nodes."""
    with driver.session() as s:
        result = s.run("""
            MATCH (n:BusinessModel)
            WHERE n.status <> 'Deprecated'
            RETURN n.bim_id AS id, n.name AS name,
                   coalesce(n.description, '') AS description
            ORDER BY n.bim_id
        """)
        return result.data()


def next_bim_id(driver) -> str:
    """Generate the next sequential BIM_XXX id."""
    with driver.session() as s:
        result = s.run("""
            MATCH (n:BusinessModel)
            RETURN n.bim_id AS id ORDER BY n.bim_id DESC LIMIT 1
        """)
        rec = result.single()
    if not rec:
        return "BIM_028"
    last = rec["id"]  # e.g. "BIM_027"
    num = int(last.split("_")[1]) + 1
    return f"BIM_{num:03d}"


def web_search(name: str) -> str:
    """Run two Tavily searches and return concatenated results as context."""
    client = TavilyClient(api_key=os.getenv("TAVILY_API_KEY"))
    queries = [
        f'"{name}" business model how it works revenue',
        f'"{name}" business model examples companies',
    ]
    sections = []
    for q in queries:
        try:
            resp = client.search(q, max_results=5, include_answer=True)
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
        "prompts", "bm_enrichment.txt",
    )
    with open(prompt_path) as f:
        return f.read()


def call_claude(name: str, search_context: str, existing_bms: list[dict]) -> dict:
    """Call Claude to enrich the BM and check for duplicates. Returns parsed JSON."""
    client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

    existing_summary = "\n".join(
        f"- {bm['id']}: {bm['name']} — {bm['description'][:120]}..."
        for bm in existing_bms
    )

    user_message = f"""
BUSINESS MODEL TO ENRICH: {name}

WEB SEARCH RESULTS:
{search_context}

EXISTING BUSINESS MODEL LIBRARY ({len(existing_bms)} models):
{existing_summary}

Return a single JSON object following the schema in your instructions.
Check carefully whether this model is already represented in the existing library.
"""

    response = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=2048,
        system=load_system_prompt(),
        messages=[{"role": "user", "content": user_message}],
    )

    raw = response.content[0].text.strip()

    # Extract JSON even if Claude wraps it in ```json ... ```
    json_match = re.search(r"\{[\s\S]*\}", raw)
    if not json_match:
        raise ValueError(f"Claude did not return valid JSON. Raw output:\n{raw[:500]}")

    return json.loads(json_match.group(0))


def write_bm_to_graph(driver, bim_id: str, enriched: dict, pending: bool) -> None:
    """Write the enriched BusinessModel node to Neo4j."""
    now = datetime.now(timezone.utc).isoformat()
    props = {
        "bim_id":              bim_id,
        "name":                enriched.get("name", ""),
        "description":         enriched.get("description", ""),
        "revenue_logic":       enriched.get("revenue_logic", ""),
        "key_dependencies":    enriched.get("key_dependencies", []),
        "typical_margins":     enriched.get("typical_margins", "Variable"),
        "scalars_most_affected": enriched.get("scalars_most_affected", []),
        "examples_json":       json.dumps(enriched.get("examples", [])),
        "status":              "Active",
        "version":             1,
        "source":              "manual_entry",
        "added_by":            "bm_enrichment_agent",
        "confidence":          0.85,
        "pending_human_review": pending,
        "created_at":          now,
        "updated_at":          now,
    }
    with driver.session() as s:
        s.run("""
            MERGE (n:BusinessModel {bim_id: $id})
            SET n += $props
        """, id=bim_id, props=props)


def enrich_business_model(name: str, dry_run: bool = False) -> dict:
    """
    Main enrichment function.

    Returns a result dict:
    {
      "status": "created" | "duplicate_blocked" | "similarity_flagged" | "error",
      "bim_id": str or None,
      "enriched": dict,       # the Claude output
      "message": str,
    }
    """
    console.print(f"\n[bold]Enriching business model:[/bold] {name}\n")

    driver = get_driver()

    # 1. Web search
    console.print("[dim]Step 1/3 — Web search...[/dim]")
    search_context = web_search(name)
    console.print(f"  Retrieved web context ({len(search_context)} chars)")

    # 2. Fetch existing BMs for duplicate check
    existing_bms = get_existing_bms(driver)
    console.print(f"  Comparing against {len(existing_bms)} existing business models")

    # 3. Call Claude
    console.print("[dim]Step 2/3 — Calling Claude...[/dim]")
    try:
        enriched = call_claude(name, search_context, existing_bms)
    except Exception as e:
        driver.close()
        return {"status": "error", "bim_id": None, "enriched": {}, "message": str(e)}

    # 4. Duplicate check
    similarity    = float(enriched.get("similarity_score", 0.0))
    duplicate_of  = enriched.get("is_duplicate_of")

    if similarity >= SIMILARITY_THRESHOLD and duplicate_of:
        console.print(Panel(
            f"[yellow]Duplicate detected[/yellow]\n"
            f"'{name}' is structurally identical to '[bold]{duplicate_of}[/bold]'\n"
            f"Similarity score: {similarity:.2f}\n"
            f"Rationale: {enriched.get('duplicate_rationale', '')}",
            title="⚠ Merge Proposed — Node NOT created",
        ))
        driver.close()
        return {
            "status": "duplicate_blocked",
            "bim_id": None,
            "enriched": enriched,
            "message": f"Duplicate of '{duplicate_of}' (similarity={similarity:.2f}). No node created.",
        }

    # 5. Write to graph
    console.print("[dim]Step 3/3 — Writing to Neo4j...[/dim]")
    pending = True  # always requires human review on manual entry
    flagged = similarity >= REVIEW_THRESHOLD

    if not dry_run:
        bim_id = next_bim_id(driver)
        write_bm_to_graph(driver, bim_id, enriched, pending)
    else:
        bim_id = "DRY_RUN"

    driver.close()

    # 6. Display result
    examples = enriched.get("examples", [])
    example_lines = "\n".join(
        f"  • {ex.get('company', '?')}: {ex.get('description', '')[:100]}"
        for ex in examples[:3]
    )

    status_note = ""
    if flagged:
        status_note = f"\n[yellow]⚑ Flagged for review — similarity {similarity:.2f} to '{duplicate_of or 'existing model'}'[/yellow]"

    console.print(Panel(
        f"[bold]ID:[/bold] {bim_id}\n"
        f"[bold]Name:[/bold] {enriched.get('name', name)}\n"
        f"[bold]Margins:[/bold] {enriched.get('typical_margins', '—')}\n\n"
        f"[bold]Description:[/bold]\n{enriched.get('description', '')}\n\n"
        f"[bold]Revenue logic:[/bold]\n{enriched.get('revenue_logic', '')}\n\n"
        f"[bold]Key dependencies:[/bold]\n" +
        "\n".join(f"  • {d}" for d in enriched.get("key_dependencies", [])) +
        f"\n\n[bold]Examples ({len(examples)} found):[/bold]\n{example_lines}"
        f"\n\n[green]✓ pending_human_review=True[/green]"
        f"{status_note}",
        title=f"BusinessModel created: {bim_id}",
    ))

    return {
        "status": "similarity_flagged" if flagged else "created",
        "bim_id": bim_id,
        "enriched": enriched,
        "message": f"Created {bim_id} — pending human review.",
    }
