"""
input_layer/bm_scanner.py — Internet Scanner for New Business Models

Searches the web for emerging business model patterns, extracts structured
candidates, deduplicates against the existing library, and enriches + queues
novel ones for human review.

Usage:
    # Run a full scan
    python input_layer/bm_scanner.py

    # Scan but don't write to graph (report only)
    python input_layer/bm_scanner.py --dry-run

    # Control how many candidates to enrich (default: 3)
    python input_layer/bm_scanner.py --enrich-limit 2

    # Save scan report to file
    python input_layer/bm_scanner.py --output scan_report.json
"""

import os
import sys
import json
import re
import argparse
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import anthropic
from dotenv import load_dotenv
from tavily import TavilyClient
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

from input_layer.bm_enrichment import (
    enrich_business_model,
    get_driver,
    get_existing_bms,
)

load_dotenv(override=True)
console = Console(width=200)

# Similarity threshold below which a candidate is considered novel enough to enrich
NOVELTY_THRESHOLD = 0.60


# ── Step 1: Web search for emerging business models ──────────────────────────

SCAN_QUERIES = [
    "emerging business models AI 2024 2025 new revenue model",
    "new business model patterns disruption startup monetisation 2025",
    "AI-native business model examples companies 2024 structural",
    "business model innovation examples B2B SaaS beyond subscription 2025",
]


def scan_web() -> str:
    """Run all scan queries and return combined context."""
    client = TavilyClient(api_key=os.getenv("TAVILY_API_KEY"))
    sections = []
    for q in SCAN_QUERIES:
        try:
            resp = client.search(q, max_results=5, include_answer=True)
            answer = resp.get("answer", "")
            results = resp.get("results", [])
            block = f"Query: {q}\n"
            if answer:
                block += f"Summary: {answer}\n"
            for r in results:
                block += (
                    f"- {r.get('title', '')} ({r.get('url', '')}): "
                    f"{r.get('content', '')[:400]}\n"
                )
            sections.append(block)
        except Exception as e:
            sections.append(f"Query: {q}\nSearch error: {e}\n")
    return "\n\n".join(sections)


# ── Step 2: Extract candidates with Claude ────────────────────────────────────

def load_scanner_prompt() -> str:
    path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "prompts", "bm_scanner.txt",
    )
    with open(path) as f:
        return f.read()


def extract_candidates(search_context: str, existing_bms: list[dict]) -> list[dict]:
    """Ask Claude to extract novel BM candidates from the search results."""
    client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

    existing_summary = "\n".join(
        f"- {bm['id']}: {bm['name']} — {bm['description'][:120]}..."
        for bm in existing_bms
    )

    user_message = f"""
WEB SEARCH RESULTS (scan for novel business model patterns):
{search_context}

EXISTING BUSINESS MODEL LIBRARY ({len(existing_bms)} models — do NOT propose variants of these):
{existing_summary}

Return a JSON array of novel candidates. If none are genuinely novel, return [].
"""

    response = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=3000,
        system=load_scanner_prompt(),
        messages=[{"role": "user", "content": user_message}],
    )

    raw = response.content[0].text.strip()

    # Extract JSON array
    arr_match = re.search(r"\[[\s\S]*\]", raw)
    if not arr_match:
        console.print(f"[yellow]Scanner returned no JSON array. Raw: {raw[:200]}[/yellow]")
        return []

    try:
        candidates = json.loads(arr_match.group(0))
        return [c for c in candidates if isinstance(c, dict)]
    except json.JSONDecodeError as e:
        console.print(f"[red]JSON parse error: {e}[/red]")
        return []


# ── Step 3: Filter by novelty threshold ──────────────────────────────────────

def filter_novel(candidates: list[dict]) -> tuple[list[dict], list[dict]]:
    """Split into (novel_candidates, filtered_out)."""
    novel, filtered = [], []
    for c in candidates:
        sim = float(c.get("similarity_to_closest", 1.0))
        if sim < NOVELTY_THRESHOLD:
            novel.append(c)
        else:
            filtered.append(c)
    return novel, filtered


# ── Step 4: Log scan run to graph ─────────────────────────────────────────────

def log_scan_to_graph(driver, scan_result: dict) -> None:
    """Store a CompressionLog node recording this scan run."""
    now = datetime.now(timezone.utc).isoformat()
    with driver.session() as s:
        s.run("""
            CREATE (n:CompressionLog {
                log_id:         $log_id,
                scan_type:      'bm_internet_scan',
                ran_at:         $now,
                candidates_found: $found,
                candidates_novel: $novel,
                candidates_enriched: $enriched,
                queries_run:    $queries,
                source:         'bm_scanner'
            })
        """,
            log_id=f"SCAN_{now[:10].replace('-','')}_{now[11:13]}{now[14:16]}",
            now=now,
            found=scan_result.get("total_candidates", 0),
            novel=scan_result.get("novel_count", 0),
            enriched=scan_result.get("enriched_count", 0),
            queries=len(SCAN_QUERIES),
        )


# ── Orchestrator ──────────────────────────────────────────────────────────────

def run_scan(dry_run: bool = False, enrich_limit: int = 3) -> dict:
    """
    Full scan pipeline.

    Returns:
    {
      "total_candidates": int,
      "novel_count": int,
      "enriched_count": int,
      "filtered_count": int,
      "candidates": [...],
      "enrich_results": [...],
    }
    """
    console.print("\n[bold]BM Internet Scanner[/bold]\n")
    console.print(f"  Queries: {len(SCAN_QUERIES)}")
    console.print(f"  Novelty threshold: similarity < {NOVELTY_THRESHOLD}")
    console.print(f"  Enrich limit: {enrich_limit}")
    console.print(f"  Mode: {'DRY RUN' if dry_run else 'LIVE'}\n")

    driver = get_driver()
    existing_bms = get_existing_bms(driver)

    # Step 1: Scan web
    console.print("[dim]Step 1/4 — Scanning web...[/dim]")
    search_context = scan_web()
    console.print(f"  Retrieved {len(search_context)} chars across {len(SCAN_QUERIES)} queries\n")

    # Step 2: Extract candidates
    console.print("[dim]Step 2/4 — Extracting candidates with Claude...[/dim]")
    candidates = extract_candidates(search_context, existing_bms)
    console.print(f"  Found {len(candidates)} candidate(s)\n")

    if not candidates:
        driver.close()
        console.print("[green]No novel candidates found in this scan.[/green]")
        return {
            "total_candidates": 0,
            "novel_count": 0,
            "enriched_count": 0,
            "filtered_count": 0,
            "candidates": [],
            "enrich_results": [],
        }

    # Step 3: Filter by novelty
    novel, filtered_out = filter_novel(candidates)
    console.print(f"[dim]Step 3/4 — Novelty filter:[/dim] {len(novel)} novel, {len(filtered_out)} filtered out\n")

    # Print candidate table
    table = Table(title="Candidates Extracted", show_header=True)
    table.add_column("Name", width=35)
    table.add_column("Closest existing", width=30)
    table.add_column("Sim", justify="right", width=6)
    table.add_column("Novel?", justify="center", width=8)
    table.add_column("Confidence", justify="right", width=10)

    for c in candidates:
        sim = float(c.get("similarity_to_closest", 1.0))
        is_novel = sim < NOVELTY_THRESHOLD
        table.add_row(
            c.get("name", ""),
            c.get("closest_existing", ""),
            f"{sim:.2f}",
            "[green]✓[/green]" if is_novel else "[red]✗[/red]",
            f"{c.get('confidence', '?')}",
        )
    console.print(table)
    console.print()

    # Step 4: Enrich novel candidates (up to limit)
    to_enrich = novel[:enrich_limit]
    console.print(f"[dim]Step 4/4 — Enriching {len(to_enrich)} novel candidate(s)...[/dim]\n")

    enrich_results = []
    for candidate in to_enrich:
        name = candidate.get("name", "")
        console.print(Panel(
            candidate.get("description", "") + "\n\n"
            f"[dim]Why novel:[/dim] {candidate.get('why_novel', '')}",
            title=f"Candidate: {name}",
        ))
        result = enrich_business_model(name, dry_run=dry_run)
        enrich_results.append({
            "candidate": candidate,
            "enrich_result": result,
        })

    driver.close()

    # Log to graph
    scan_result = {
        "total_candidates": len(candidates),
        "novel_count": len(novel),
        "enriched_count": sum(
            1 for r in enrich_results
            if r["enrich_result"]["status"] in ("created", "similarity_flagged")
        ),
        "filtered_count": len(filtered_out),
        "candidates": candidates,
        "enrich_results": enrich_results,
    }

    if not dry_run:
        try:
            driver2 = get_driver()
            log_scan_to_graph(driver2, scan_result)
            driver2.close()
        except Exception as e:
            console.print(f"[yellow]Warning: could not log scan to graph: {e}[/yellow]")

    # Final summary
    summary = Table(title="\nScan Summary", show_header=True)
    summary.add_column("Metric", style="bold")
    summary.add_column("Count", justify="right")
    summary.add_row("Candidates found", str(scan_result["total_candidates"]))
    summary.add_row("Novel (below threshold)", str(scan_result["novel_count"]))
    summary.add_row("Filtered out (too similar)", str(scan_result["filtered_count"]))
    summary.add_row("Enriched & queued", str(scan_result["enriched_count"]))
    console.print(summary)

    return scan_result


def main():
    parser = argparse.ArgumentParser(description="Internet scanner for new business models")
    parser.add_argument("--dry-run", action="store_true",
                        help="Extract and report candidates without writing to Neo4j")
    parser.add_argument("--enrich-limit", type=int, default=3,
                        help="Max candidates to fully enrich per scan (default: 3)")
    parser.add_argument("--output", help="Save scan report as JSON to this path")
    args = parser.parse_args()

    result = run_scan(dry_run=args.dry_run, enrich_limit=args.enrich_limit)

    if args.output:
        with open(args.output, "w") as f:
            # Scrub non-serialisable values
            json.dump(result, f, indent=2, default=str)
        console.print(f"\nReport saved to {args.output}")


if __name__ == "__main__":
    main()
