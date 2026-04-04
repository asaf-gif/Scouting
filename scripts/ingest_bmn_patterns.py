"""
scripts/ingest_bmn_patterns.py

Fetches all 60 Business Model Navigator pattern pages, extracts the
company "How they do it" descriptions, and feeds them through the
full extraction pipeline (vector extraction → scalar classification
→ hypothesis generation).

Processes patterns in batches to respect API rate limits.

Usage:
    python scripts/ingest_bmn_patterns.py
    python scripts/ingest_bmn_patterns.py --batch-size 5 --start 1 --end 60
    python scripts/ingest_bmn_patterns.py --dry-run
"""

import os
import sys
import time
import argparse

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv(override=True)

from extraction.vector_extractor import fetch_url_content, extract_from_text
from extraction.scalar_classifier import classify_vector_scalars
from extraction.hypothesis_generator import generate_hypothesis
from neo4j import GraphDatabase
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn

console = Console(width=200)

# All 60 BMN pattern names (for labelling)
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


def fetch_pattern_content(pattern_id: int) -> str:
    """Fetch text content from a BMN pattern page."""
    url = f"https://businessmodelnavigator.com/pattern?id={pattern_id}"
    content = fetch_url_content(url)
    return content or ""


def classify_and_hypothesize_new_vectors(dry_run: bool = False) -> dict:
    """
    After extraction, classify scalars and generate hypotheses for
    vectors that now have evidence but no scalar impacts or hypothesis.
    """
    driver = get_driver()

    # Vectors with evidence but no scalar impacts
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
            console.print(f"  [yellow]Scalar classify error {v['vid']}: {e}[/yellow]")
        time.sleep(0.3)

    # Vectors with evidence + impacts but no hypothesis
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


def run_ingestion(start: int = 1, end: int = 60, batch_size: int = 6,
                  dry_run: bool = False) -> dict:
    """
    Main ingestion loop:
    1. Fetch each pattern page
    2. Batch into groups, run extraction on each batch
    3. After all batches, classify scalars + generate hypotheses
    """
    pattern_ids = list(range(start, end + 1))
    total_batches = (len(pattern_ids) + batch_size - 1) // batch_size

    console.print(f"\n[bold]BMN Pattern Ingestion[/bold]")
    console.print(f"  Patterns: {start}–{end}  |  Batch size: {batch_size}  |  "
                  f"Dry run: {dry_run}\n")

    all_transitions = []
    fetch_errors = []

    # Process in batches
    for batch_num in range(total_batches):
        batch_ids = pattern_ids[batch_num * batch_size:(batch_num + 1) * batch_size]
        batch_texts = []

        console.print(f"[bold]Batch {batch_num + 1}/{total_batches}[/bold] "
                      f"— patterns {batch_ids[0]}–{batch_ids[-1]}")

        # Fetch each pattern page in this batch
        for pid in batch_ids:
            name = PATTERN_NAMES.get(pid, f"Pattern {pid}")
            console.print(f"  Fetching #{pid} {name}...", end=" ")
            content = fetch_pattern_content(pid)
            if content and len(content) > 200:
                # Label the content with the pattern name for context
                labelled = (
                    f"Business Model Pattern: {name}\n"
                    f"Pattern #{pid} from Business Model Navigator\n\n"
                    f"{content}"
                )
                batch_texts.append(labelled)
                console.print(f"[green]{len(content)} chars[/green]")
            else:
                fetch_errors.append(pid)
                console.print(f"[yellow]too short or failed[/yellow]")
            time.sleep(0.8)  # polite crawl rate

        if not batch_texts:
            console.print("  [yellow]No content fetched for this batch, skipping[/yellow]")
            continue

        # Combine batch text and run extraction
        combined = "\n\n---\n\n".join(batch_texts)
        source_label = f"BMN patterns {batch_ids[0]}-{batch_ids[-1]}"

        console.print(f"  Extracting from {len(batch_texts)} patterns "
                      f"({len(combined)} total chars)...")
        try:
            results = extract_from_text(
                combined,
                source_url=f"https://businessmodelnavigator.com/explore#{batch_ids[0]}-{batch_ids[-1]}",
                source_type="business_model_case_study",
                dry_run=dry_run,
            )
            written = [r for r in results if r.get("status") == "written"]
            all_transitions.extend(written)
            console.print(f"  [green]{len(results)} transitions extracted, "
                          f"{len(written)} written[/green]")
            for r in written[:5]:
                console.print(f"    {r.get('from_bim_id','?')} → {r.get('to_bim_id','?')} "
                               f"conf={r.get('confidence',0):.2f}")
        except Exception as e:
            console.print(f"  [red]Extraction error: {e}[/red]")

        # Small pause between batches
        if batch_num < total_batches - 1:
            console.print("  [dim]Pausing 2s between batches...[/dim]")
            time.sleep(2)

    # After all extractions, classify + hypothesize
    console.print(f"\n[bold]Post-extraction: scalar classification + hypothesis generation[/bold]")
    post = classify_and_hypothesize_new_vectors(dry_run=dry_run)

    # Run analysis pipeline
    if not dry_run:
        console.print(f"\n[bold]Running analysis pipeline...[/bold]")
        from orchestrator.pipeline import run_pipeline
        run_pipeline(stages=["aggregate", "trends", "rank"], dry_run=False)

    summary = {
        "patterns_fetched": len(pattern_ids) - len(fetch_errors),
        "fetch_errors": fetch_errors,
        "transitions_written": len(all_transitions),
        "scalars_written": post["scalars_written"],
        "hypotheses_written": post["hypotheses_written"],
    }

    console.print(f"\n[bold]Ingestion complete:[/bold]")
    console.print(f"  Patterns fetched:     {summary['patterns_fetched']}/{len(pattern_ids)}")
    console.print(f"  Transitions written:  {summary['transitions_written']}")
    console.print(f"  Scalars classified:   {summary['scalars_written']}")
    console.print(f"  Hypotheses generated: {summary['hypotheses_written']}")
    if fetch_errors:
        console.print(f"  Fetch errors:         {fetch_errors}")

    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest all BMN pattern case studies")
    parser.add_argument("--start",      type=int, default=1,  help="First pattern ID")
    parser.add_argument("--end",        type=int, default=60, help="Last pattern ID")
    parser.add_argument("--batch-size", type=int, default=6,  help="Patterns per extraction call")
    parser.add_argument("--dry-run",    action="store_true",  help="No writes")
    args = parser.parse_args()

    run_ingestion(
        start=args.start,
        end=args.end,
        batch_size=args.batch_size,
        dry_run=args.dry_run,
    )
