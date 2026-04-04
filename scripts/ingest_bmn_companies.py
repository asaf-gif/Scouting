"""
scripts/ingest_bmn_companies.py

Two-phase BMN company ingestion:

Phase 1 — Company extraction:
  Fetches each BMN pattern page, uses Claude to extract company names
  mentioned as examples of that pattern.

Phase 2 — Business model transition extraction:
  For each unique company, searches the web for articles about their
  business model evolution/transformation, then runs those through the
  full extraction pipeline (vector → scalar → hypothesis).

This yields real, specific, evidence-backed transitions rather than
the abstract archetype descriptions on the pattern pages themselves.

Usage:
    python scripts/ingest_bmn_companies.py
    python scripts/ingest_bmn_companies.py --phase 1          # discover only
    python scripts/ingest_bmn_companies.py --phase 2          # extract only (uses cache)
    python scripts/ingest_bmn_companies.py --limit 20         # cap company count
    python scripts/ingest_bmn_companies.py --dry-run
    python scripts/ingest_bmn_companies.py --company "Netflix" # single company
"""

import os
import sys
import time
import json
import argparse

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv(override=True)

import anthropic
from tavily import TavilyClient
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn

from extraction.vector_extractor import fetch_url_content, extract_from_text
from extraction.scalar_classifier import classify_vector_scalars
from extraction.hypothesis_generator import generate_hypothesis
from neo4j import GraphDatabase

console = Console(width=200)

# Cache file for discovered companies (avoids re-fetching pattern pages)
COMPANY_CACHE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "bmn_companies_cache.json"
)

# All 60 BMN pattern names
PATTERN_NAMES = {
    1: "Add-on", 2: "Affiliation", 3: "Aikido", 4: "Auction", 5: "Barter",
    6: "Cash Machine", 7: "Cross Selling", 8: "Crowdfunding", 9: "Crowdsourcing",
    10: "Customer Loyalty", 11: "Digitization", 12: "Direct Selling",
    13: "E-commerce", 14: "Experience Selling", 15: "Flat Rate",
    16: "Fractional Ownership", 17: "Franchising", 18: "Freemium",
    19: "From Push-to-Pull", 20: "Guaranteed Availability", 21: "Hidden Revenue",
    22: "Ingredient Branding", 23: "Integrator", 24: "Layer Player",
    25: "Leverage Customer Data", 26: "License", 27: "Lock-in", 28: "Long Tail",
    29: "Make More Of It", 30: "Mass Customization", 31: "No Frills",
    32: "Open Business Model", 33: "Open Source", 34: "Orchestrator",
    35: "Pay Per Use", 36: "Pay What You Want", 37: "Peer-to-Peer",
    38: "Performance-based Contracting", 39: "Razor and Blade",
    40: "Rent Instead Of Buy", 41: "Revenue Sharing", 42: "Reverse Engineering",
    43: "Reverse Innovation", 44: "Robin Hood", 45: "Self-service",
    46: "Shop-in-shop", 47: "Solution Provider", 48: "Subscription",
    49: "Supermarket", 50: "Target the Poor", 51: "Trash-to-cash",
    52: "Two-sided Market", 53: "Ultimate Luxury", 54: "User Designed",
    55: "Whitelabel", 56: "Sensor as a Service", 57: "Virtualization",
    58: "Object Self-service", 59: "Object as Point-of-sale", 60: "Prosumer",
}


def get_driver():
    return GraphDatabase.driver(
        os.getenv("NEO4J_URI"),
        auth=(os.getenv("NEO4J_USER", "neo4j"), os.getenv("NEO4J_PASSWORD")),
    )


# ── Phase 1: Discover companies from BMN pattern pages ─────────────────────────

def extract_companies_from_text(text: str, pattern_name: str) -> list[str]:
    """Ask Claude to extract company names from a BMN pattern page."""
    client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
    try:
        response = client.messages.create(
            model="claude-haiku-4-5",
            max_tokens=500,
            messages=[{
                "role": "user",
                "content": (
                    f"From this text about the '{pattern_name}' business model pattern, "
                    f"extract all specific company names mentioned as real-world examples. "
                    f"Return ONLY a JSON array of company names, nothing else. "
                    f"Example: [\"Netflix\", \"Spotify\", \"Amazon\"]\n\n"
                    f"TEXT:\n{text[:3000]}"
                ),
            }],
        )
        raw = response.content[0].text.strip()
        import re
        arr = re.search(r"\[[\s\S]*?\]", raw)
        if arr:
            names = json.loads(arr.group(0))
            return [n.strip() for n in names if isinstance(n, str) and len(n) > 1]
    except Exception as e:
        console.print(f"  [yellow]Company extract error: {e}[/yellow]")
    return []


def discover_companies(start: int = 1, end: int = 60) -> dict[str, list[str]]:
    """
    Fetch all BMN pattern pages and extract company names.
    Returns dict: {company_name: [pattern_names_where_mentioned]}
    """
    # Load existing cache
    cache = {}
    if os.path.exists(COMPANY_CACHE):
        try:
            with open(COMPANY_CACHE) as f:
                cache = json.load(f)
            console.print(f"[dim]Loaded {len(cache)} companies from cache[/dim]")
        except Exception:
            pass

    company_to_patterns: dict[str, list[str]] = {}
    # Re-populate from cache
    for company, patterns in cache.items():
        company_to_patterns[company] = patterns

    # Fetch patterns not already in cache (check by pattern name)
    cached_patterns = set()
    for patterns in cache.values():
        cached_patterns.update(patterns)

    for pid in range(start, end + 1):
        pattern_name = PATTERN_NAMES.get(pid, f"Pattern {pid}")
        if pattern_name in cached_patterns:
            console.print(f"  [dim]#{pid} {pattern_name} — cached[/dim]")
            continue

        url = f"https://businessmodelnavigator.com/pattern?id={pid}"
        console.print(f"  Fetching #{pid} {pattern_name}...", end=" ")
        content = fetch_url_content(url)

        if not content or len(content) < 200:
            console.print("[yellow]too short[/yellow]")
            time.sleep(0.5)
            continue

        console.print(f"[dim]{len(content)} chars[/dim]", end=" → ")
        companies = extract_companies_from_text(content, pattern_name)
        console.print(f"[green]{len(companies)} companies[/green]")

        for company in companies:
            if company not in company_to_patterns:
                company_to_patterns[company] = []
            if pattern_name not in company_to_patterns[company]:
                company_to_patterns[company].append(pattern_name)

        time.sleep(0.8)

    # Save updated cache
    with open(COMPANY_CACHE, "w") as f:
        json.dump(company_to_patterns, f, indent=2)

    return company_to_patterns


# ── Phase 2: Extract transitions from company BM evolution articles ────────────

def search_company_bm_evolution(company: str, patterns: list[str]) -> list[dict]:
    """
    Search for web articles about this company's business model transformation.
    Returns list of {url, content, title} dicts.
    """
    client = TavilyClient(api_key=os.getenv("TAVILY_API_KEY"))
    pattern_hint = patterns[0] if patterns else "business model"

    queries = [
        f"{company} business model evolution transformation history",
        f"{company} revenue model change strategy shift",
        f"{company} {pattern_hint} business model case study",
    ]

    seen_urls = set()
    results = []

    for query in queries[:2]:  # 2 queries per company to manage API costs
        try:
            resp = client.search(query, max_results=3, include_raw_content=True)
            for r in resp.get("results", []):
                url = r.get("url", "")
                if url in seen_urls:
                    continue
                seen_urls.add(url)
                content = r.get("raw_content") or r.get("content", "")
                if content and len(content) > 300:
                    results.append({
                        "url": url,
                        "title": r.get("title", ""),
                        "content": content[:5000],
                    })
        except Exception as e:
            console.print(f"  [yellow]Search error for {company}: {e}[/yellow]")
        time.sleep(0.3)

    return results[:4]  # cap at 4 articles per company


def process_company(company: str, patterns: list[str], dry_run: bool = False) -> dict:
    """
    Full pipeline for one company:
    1. Search for BM evolution articles
    2. Extract transitions from combined text
    Returns summary dict.
    """
    console.print(f"\n  [bold]{company}[/bold] [dim]({', '.join(patterns[:2])})[/dim]")

    articles = search_company_bm_evolution(company, patterns)
    if not articles:
        console.print(f"  [yellow]No articles found[/yellow]")
        return {"company": company, "articles": 0, "transitions": 0}

    console.print(f"  [dim]{len(articles)} articles found[/dim]")

    # Build combined text with company context
    parts = [
        f"Company: {company}",
        f"Known for business model patterns: {', '.join(patterns)}",
        "",
    ]
    for art in articles:
        parts.append(f"Source: {art['title']} ({art['url']})")
        parts.append(art["content"])
        parts.append("---")

    combined = "\n".join(parts)

    # Extract transitions
    try:
        results = extract_from_text(
            combined,
            source_url=f"https://businessmodelnavigator.com/explore#{company.replace(' ','_')}",
            source_type="company_case_study",
            dry_run=dry_run,
        )
        written = [r for r in results if r.get("status") == "written"]
        console.print(
            f"  [green]{len(results)} transitions extracted, "
            f"{len(written)} written[/green]"
        )
        for r in written:
            console.print(
                f"    {r.get('from_bim_id','?')} → {r.get('to_bim_id','?')} "
                f"conf={r.get('confidence',0):.2f} — {r.get('evidence_quote','')[:80]}"
            )
        return {"company": company, "articles": len(articles), "transitions": len(written)}
    except Exception as e:
        console.print(f"  [red]Extraction error: {e}[/red]")
        return {"company": company, "articles": len(articles), "transitions": 0, "error": str(e)}


def classify_and_hypothesize(dry_run: bool = False) -> dict:
    """Classify scalars and generate hypotheses for new vectors."""
    driver = get_driver()

    with driver.session() as s:
        unclassified = s.run("""
            MATCH (v:TransformationVector)
            WHERE EXISTS { MATCH (e:Evidence)-[:SUPPORTS]->(v) }
            AND NOT EXISTS { MATCH (v)-[:IMPACTS]->() }
            MATCH (v)-[:FROM_BIM]->(f:BusinessModel)
            MATCH (v)-[:TO_BIM]->(t:BusinessModel)
            OPTIONAL MATCH (e:Evidence)-[:SUPPORTS]->(v)
            WITH v, f, t, collect(e.evidence_quote)[..3] AS quotes
            RETURN v.vector_id AS vid, f.bim_id AS from_id, t.bim_id AS to_id,
                   size([(e:Evidence)-[:SUPPORTS]->(v)|e]) AS ev_count, quotes
            ORDER BY ev_count DESC
        """).data()

    console.print(f"  [dim]{len(unclassified)} vectors need scalar classification[/dim]")
    scalars_written = 0
    for v in unclassified:
        evidence_text = " ".join([q for q in (v["quotes"] or []) if q])
        try:
            result = classify_vector_scalars(
                v["from_id"], v["to_id"],
                evidence_text=evidence_text,
                dry_run=dry_run,
            )
            scalars_written += result.get("scalars_written", 0)
        except Exception as e:
            console.print(f"  [yellow]Scalar error {v['vid']}: {e}[/yellow]")
        time.sleep(0.3)

    with driver.session() as s:
        unhypothesized = s.run("""
            MATCH (v:TransformationVector)
            WHERE EXISTS { MATCH (e:Evidence)-[:SUPPORTS]->(v) }
            AND EXISTS { MATCH (v)-[:IMPACTS]->() }
            AND NOT EXISTS { MATCH (h:DisruptionHypothesis)-[:GENERATED_FROM]->(v) }
            MATCH (v)-[:FROM_BIM]->(f:BusinessModel)
            MATCH (v)-[:TO_BIM]->(t:BusinessModel)
            WITH v, f, t,
                 size([(e:Evidence)-[:SUPPORTS]->(v)|e]) AS ev_count
            RETURN f.bim_id AS from_id, t.bim_id AS to_id, ev_count
            ORDER BY ev_count DESC
        """).data()

    driver.close()

    console.print(f"  [dim]{len(unhypothesized)} vectors ready for hypothesis generation[/dim]")
    hypotheses_written = 0
    for v in unhypothesized:
        try:
            result = generate_hypothesis(v["from_id"], v["to_id"], dry_run=dry_run)
            if result.get("status") == "written":
                hypotheses_written += 1
                console.print(
                    f"  [green]Hypothesis:[/green] {result.get('hypothesis_id')} "
                    f"(conviction={result.get('conviction', 0):.2f})"
                )
        except Exception as e:
            console.print(f"  [yellow]Hypothesis error {v['from_id']}->{v['to_id']}: {e}[/yellow]")
        time.sleep(0.3)

    return {
        "scalars_written": scalars_written,
        "hypotheses_written": hypotheses_written,
        "vectors_classified": len(unclassified),
        "vectors_hypothesized": len(unhypothesized),
    }


# ── Main ───────────────────────────────────────────────────────────────────────

def run(phase: int = 0, limit: int = 0, dry_run: bool = False,
        company_filter: str = None) -> dict:
    """
    Run the full two-phase ingestion.
    phase=0: both phases. phase=1: discover only. phase=2: extract only.
    """
    console.print(f"\n[bold]BMN Company Ingestion[/bold]")
    console.print(f"  Phase: {phase or 'both'}  |  Limit: {limit or 'all'}  |  "
                  f"Dry run: {dry_run}\n")

    # ── Phase 1 ──────────────────────────────────────────────────────────────
    if phase in (0, 1):
        console.print(f"[bold]Phase 1: Discover companies from BMN pattern pages[/bold]")
        company_map = discover_companies(start=1, end=60)
        console.print(f"\n  [green]Found {len(company_map)} unique companies across all patterns[/green]")
    else:
        # Load from cache for phase 2 only
        if os.path.exists(COMPANY_CACHE):
            with open(COMPANY_CACHE) as f:
                company_map = json.load(f)
            console.print(f"[dim]Loaded {len(company_map)} companies from cache[/dim]")
        else:
            console.print("[red]No cache found — run phase 1 first[/red]")
            return {}

    if phase == 1:
        # Just print the discovered companies
        console.print("\nDiscovered companies:")
        for co, pats in sorted(company_map.items()):
            console.print(f"  {co}: {', '.join(pats[:3])}")
        return {"companies_found": len(company_map)}

    # ── Phase 2 ──────────────────────────────────────────────────────────────
    console.print(f"\n[bold]Phase 2: Extract BM transitions from company articles[/bold]")

    # Filter if --company specified
    if company_filter:
        company_map = {k: v for k, v in company_map.items()
                       if company_filter.lower() in k.lower()}
        console.print(f"  Filtered to {len(company_map)} companies matching '{company_filter}'")

    # Prioritize companies mentioned in more patterns (more interesting transitions)
    companies_sorted = sorted(company_map.items(), key=lambda x: len(x[1]), reverse=True)

    # Apply limit
    if limit:
        companies_sorted = companies_sorted[:limit]

    console.print(f"  Processing {len(companies_sorted)} companies...\n")

    total_transitions = 0
    results_list = []

    for i, (company, patterns) in enumerate(companies_sorted, 1):
        console.print(f"[dim]({i}/{len(companies_sorted)})[/dim]", end=" ")
        result = process_company(company, patterns, dry_run=dry_run)
        results_list.append(result)
        total_transitions += result.get("transitions", 0)
        time.sleep(1.0)  # Polite crawl rate

    # ── Post-extraction ────────────────────────────────────────────────────────
    console.print(f"\n[bold]Post-extraction: scalar classification + hypothesis generation[/bold]")
    post = classify_and_hypothesize(dry_run=dry_run)

    # ── Analysis pipeline ─────────────────────────────────────────────────────
    if not dry_run and total_transitions > 0:
        console.print(f"\n[bold]Running analysis pipeline...[/bold]")
        from orchestrator.pipeline import run_pipeline
        run_pipeline(stages=["aggregate", "trends", "rank"], dry_run=False)

    summary = {
        "companies_processed": len(companies_sorted),
        "total_transitions": total_transitions,
        "scalars_written": post["scalars_written"],
        "hypotheses_written": post["hypotheses_written"],
        "company_results": results_list,
    }

    console.print(f"\n[bold]Ingestion complete:[/bold]")
    console.print(f"  Companies processed:  {summary['companies_processed']}")
    console.print(f"  Transitions written:  {summary['total_transitions']}")
    console.print(f"  Scalars classified:   {summary['scalars_written']}")
    console.print(f"  Hypotheses generated: {summary['hypotheses_written']}")

    # Top companies by transitions
    top = sorted(results_list, key=lambda x: x.get("transitions", 0), reverse=True)[:10]
    if any(r.get("transitions", 0) > 0 for r in top):
        console.print(f"\n  Top companies by transitions:")
        for r in top:
            if r.get("transitions", 0) > 0:
                console.print(f"    {r['company']}: {r['transitions']} transitions")

    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest BMN company case studies")
    parser.add_argument("--phase",    type=int, default=0,
                        help="1=discover only, 2=extract only, 0=both (default)")
    parser.add_argument("--limit",    type=int, default=0,
                        help="Max companies to process in phase 2 (0=all)")
    parser.add_argument("--dry-run",  action="store_true", help="No writes")
    parser.add_argument("--company",  default=None,
                        help="Filter to companies matching this name (substring)")
    args = parser.parse_args()

    run(
        phase=args.phase,
        limit=args.limit,
        dry_run=args.dry_run,
        company_filter=args.company,
    )
