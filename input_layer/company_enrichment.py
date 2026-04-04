"""
input_layer/company_enrichment.py — Company Enrichment Agent

Given a CSV of company names, researches each one via Tavily, calls Claude
to classify its business model and fill Company Registry fields, then writes
the node to Neo4j.

Usage (as a module):
    from input_layer.company_enrichment import enrich_company, enrich_companies_from_csv
    result = enrich_company("Palantir")

Usage (as CLI via add_companies.py):
    python input_layer/add_companies.py --csv data/sample_companies.csv
    python input_layer/add_companies.py --name "Palantir" --dry-run
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
console = Console()


def get_driver():
    return GraphDatabase.driver(
        os.getenv("NEO4J_URI"),
        auth=(os.getenv("NEO4J_USER", "neo4j"), os.getenv("NEO4J_PASSWORD")),
    )


def get_existing_bms(driver) -> list[dict]:
    """Fetch all active BusinessModel nodes for classification reference."""
    with driver.session() as s:
        result = s.run("""
            MATCH (n:BusinessModel)
            WHERE n.status <> 'Deprecated'
            RETURN n.bim_id AS id, n.name AS name,
                   coalesce(n.description, '') AS description
            ORDER BY n.bim_id
        """)
        return result.data()


def next_company_id(driver) -> str:
    """Generate the next sequential CO_XXX id."""
    with driver.session() as s:
        result = s.run("""
            MATCH (n:Company)
            RETURN n.company_id AS id ORDER BY n.company_id DESC LIMIT 1
        """)
        rec = result.single()
    if not rec or not rec["id"]:
        return "CO_001"
    last = rec["id"]  # e.g. "CO_007"
    num = int(last.split("_")[1]) + 1
    return f"CO_{num:03d}"


def company_exists(driver, name: str) -> dict | None:
    """Check if a company with this name already exists.

    Matches on: exact name, stored name contains search term, or search
    term contains stored name — catches 'Palantir' vs 'Palantir Technologies'.
    Returns node dict or None.
    """
    with driver.session() as s:
        result = s.run(
            """MATCH (n:Company)
               WHERE toLower(n.name) = toLower($name)
                  OR toLower(n.name) CONTAINS toLower($name)
                  OR toLower($name) CONTAINS toLower(n.name)
               RETURN n LIMIT 1""",
            name=name,
        )
        rec = result.single()
    return dict(rec["n"]) if rec else None


def web_search(name: str) -> str:
    """Run Tavily searches for company profile, funding, and business model."""
    client = TavilyClient(api_key=os.getenv("TAVILY_API_KEY"))
    queries = [
        f'"{name}" company business model revenue how it works',
        f'"{name}" funding valuation founded investors',
        # Signal 2: operational reality — how value is actually delivered,
        # not how revenue is reported. Targets delivery mechanism, staffing
        # model, customer implementation process, and analyst critiques that
        # cut through investor-relations framing.
        f'"{name}" how customers use it implementation deployment process '
        f'professional services consulting employees embedded',
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
        "prompts", "company_enrichment.txt",
    )
    with open(prompt_path) as f:
        return f.read()


def call_claude(name: str, search_context: str, existing_bms: list[dict]) -> dict:
    """Call Claude to classify and enrich the company. Returns parsed JSON."""
    client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

    bm_library = "\n".join(
        f"- {bm['id']}: {bm['name']} — {bm['description'][:300]}"
        for bm in existing_bms
    )

    user_message = f"""
COMPANY TO RESEARCH: {name}

WEB SEARCH RESULTS:
{search_context}

BUSINESS MODEL LIBRARY ({len(existing_bms)} models — classify against these):
{bm_library}

Return a single JSON object following the schema in your instructions.
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


def write_company_to_graph(driver, company_id: str, enriched: dict) -> None:
    """Write the enriched Company node to Neo4j and link to its BusinessModel."""
    now = datetime.now(timezone.utc).isoformat()

    props = {
        "company_id":          company_id,
        "name":                enriched.get("name", ""),
        "description":         enriched.get("description", ""),
        "website":             enriched.get("website"),
        "founded_year":        enriched.get("founded_year"),
        "hq_country":          enriched.get("hq_country"),
        "employee_count_band": enriched.get("employee_count_band"),
        "funding_stage":       enriched.get("funding_stage", "Unknown"),
        "total_funding_usd":   enriched.get("total_funding_usd"),
        "industries":          enriched.get("industries", []),
        "ai_involvement":      enriched.get("ai_involvement", "Unknown"),
        "ai_description":      enriched.get("ai_description"),
        "bm_confidence":       float(enriched.get("bm_confidence", 0.5)),
        "bm_rationale":        enriched.get("bm_rationale", ""),
        "data_sources":        enriched.get("data_sources", []),
        "source":              "manual_csv_upload",
        "added_by":            "company_enrichment_agent",
        "confidence":          float(enriched.get("bm_confidence", 0.5)),
        "pending_human_review": True,
        "created_at":          now,
        "updated_at":          now,
    }

    bim_id = enriched.get("current_bm_id")

    with driver.session() as s:
        # Create/update Company node
        s.run("""
            MERGE (c:Company {company_id: $id})
            SET c += $props
        """, id=company_id, props=props)

        # Link to BusinessModel if we have a valid BIM id
        if bim_id:
            s.run("""
                MATCH (c:Company {company_id: $co_id})
                MATCH (bm:BusinessModel {bim_id: $bim_id})
                MERGE (c)-[r:CURRENTLY_USES]->(bm)
                SET r.confidence = $conf,
                    r.rationale  = $rationale,
                    r.created_at = $now
            """,
                co_id=company_id,
                bim_id=bim_id,
                conf=float(enriched.get("bm_confidence", 0.5)),
                rationale=enriched.get("bm_rationale", ""),
                now=now,
            )


def enrich_company(name: str, dry_run: bool = False) -> dict:
    """
    Enrich a single company.

    Returns:
    {
      "status": "created" | "duplicate_skipped" | "error",
      "company_id": str or None,
      "enriched": dict,
      "message": str,
    }
    """
    console.print(f"\n[bold]Enriching company:[/bold] {name}")

    driver = get_driver()

    # Duplicate check
    existing = company_exists(driver, name)
    if existing:
        driver.close()
        console.print(f"  [yellow]↩ Skipped — already exists as {existing.get('company_id')}[/yellow]")
        return {
            "status": "duplicate_skipped",
            "company_id": existing.get("company_id"),
            "enriched": existing,
            "message": f"Company '{name}' already in graph as {existing.get('company_id')}.",
        }

    # Web search
    console.print("  [dim]Step 1/3 — Web search...[/dim]")
    search_context = web_search(name)
    console.print(f"  Retrieved web context ({len(search_context)} chars)")

    # Fetch existing BMs
    existing_bms = get_existing_bms(driver)
    console.print(f"  Classifying against {len(existing_bms)} business models")

    # Call Claude
    console.print("  [dim]Step 2/3 — Calling Claude...[/dim]")
    try:
        enriched = call_claude(name, search_context, existing_bms)
    except Exception as e:
        driver.close()
        return {"status": "error", "company_id": None, "enriched": {}, "message": str(e)}

    # Write to graph
    console.print("  [dim]Step 3/3 — Writing to Neo4j...[/dim]")
    if not dry_run:
        company_id = next_company_id(driver)
        write_company_to_graph(driver, company_id, enriched)
    else:
        company_id = "DRY_RUN"

    driver.close()

    # Display summary
    console.print(Panel(
        f"[bold]ID:[/bold] {company_id}\n"
        f"[bold]Name:[/bold] {enriched.get('name', name)}\n"
        f"[bold]BM:[/bold] {enriched.get('current_bm_id')} — {enriched.get('current_bm_name')} "
        f"(confidence={enriched.get('bm_confidence', '?')})\n"
        f"[bold]Funding:[/bold] {enriched.get('funding_stage', '?')} · "
        f"${enriched.get('total_funding_usd') or '?':,} total\n"
        f"[bold]AI:[/bold] {enriched.get('ai_involvement', '?')}\n"
        f"[bold]Industries:[/bold] {', '.join(enriched.get('industries', []))}\n\n"
        f"{enriched.get('description', '')}\n\n"
        f"[dim]{enriched.get('bm_rationale', '')}[/dim]",
        title=f"Company created: {company_id}",
    ))

    return {
        "status": "created",
        "company_id": company_id,
        "enriched": enriched,
        "message": f"Created {company_id} — pending human review.",
    }


def enrich_companies_from_csv(csv_path: str, dry_run: bool = False, limit: int = None) -> list[dict]:
    """
    Enrich all companies from a CSV file (must have a 'name' column).
    Returns a list of result dicts.
    """
    import csv

    results = []
    companies = []

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = row.get("name", "").strip()
            if name:
                companies.append(name)

    if limit:
        companies = companies[:limit]

    console.print(f"\n[bold]Company enrichment batch:[/bold] {len(companies)} companies from {csv_path}\n")

    created = skipped = errors = 0
    for i, name in enumerate(companies, 1):
        console.print(f"[dim]({i}/{len(companies)})[/dim] ", end="")
        try:
            result = enrich_company(name, dry_run=dry_run)
            results.append(result)
            if result["status"] == "created":
                created += 1
            elif result["status"] == "duplicate_skipped":
                skipped += 1
            else:
                errors += 1
        except Exception as e:
            console.print(f"  [red]Error processing {name}: {e}[/red]")
            results.append({"status": "error", "company_id": None, "enriched": {}, "message": str(e)})
            errors += 1

    # Summary table
    table = Table(title="\nBatch Summary", show_header=True)
    table.add_column("Metric", style="bold")
    table.add_column("Count", justify="right")
    table.add_row("Created", str(created))
    table.add_row("Skipped (duplicate)", str(skipped))
    table.add_row("Errors", str(errors))
    table.add_row("Total", str(len(companies)))
    console.print(table)

    return results
