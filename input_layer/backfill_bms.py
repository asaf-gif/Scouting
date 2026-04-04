"""
input_layer/backfill_bms.py — Backfill descriptions for Excel-origin BusinessModel nodes

The 27 nodes migrated from the Excel prototype have bim_id, name, and status
but no description, revenue_logic, key_dependencies, etc.  This script enriches
each existing node in-place using the same web-search + Claude pipeline as
bm_enrichment.py, without creating new nodes or running duplicate detection.

Usage:
    # Backfill all 27 nodes (takes ~10-15 minutes — 27 Claude calls)
    python input_layer/backfill_bms.py

    # Backfill only nodes still missing descriptions
    python input_layer/backfill_bms.py --missing-only

    # Dry-run: enrich but do not write to Neo4j
    python input_layer/backfill_bms.py --dry-run --limit 3

    # Backfill a specific BIM id
    python input_layer/backfill_bms.py --id BIM_026
"""

import argparse
import os
import sys
import json
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
from rich.console import Console
from rich.table import Table
from rich.progress import track

load_dotenv(override=True)
console = Console()

# Reuse search + Claude call from bm_enrichment
from input_layer.bm_enrichment import get_driver, web_search, call_claude


def get_nodes_to_backfill(driver, missing_only: bool = True, target_id: str = None) -> list[dict]:
    """Return list of {id, name} dicts for nodes that need enrichment."""
    with driver.session() as s:
        if target_id:
            result = s.run(
                "MATCH (n:BusinessModel {bim_id: $id}) RETURN n.bim_id AS id, n.name AS name",
                id=target_id,
            )
        elif missing_only:
            result = s.run("""
                MATCH (n:BusinessModel)
                WHERE n.description IS NULL OR n.description = ''
                RETURN n.bim_id AS id, n.name AS name
                ORDER BY n.bim_id
            """)
        else:
            result = s.run("""
                MATCH (n:BusinessModel)
                RETURN n.bim_id AS id, n.name AS name
                ORDER BY n.bim_id
            """)
        return result.data()


def patch_bm_node(driver, bim_id: str, enriched: dict) -> None:
    """
    Update content fields on an existing BusinessModel node.
    Preserves: bim_id, status, version, source, created_at, added_by.
    Updates: description, revenue_logic, key_dependencies, typical_margins,
             examples_json, scalars_most_affected, updated_at.
    """
    now = datetime.now(timezone.utc).isoformat()
    props = {
        "description":           enriched.get("description", ""),
        "revenue_logic":         enriched.get("revenue_logic", ""),
        "key_dependencies":      enriched.get("key_dependencies", []),
        "typical_margins":       enriched.get("typical_margins", "Variable"),
        "scalars_most_affected": enriched.get("scalars_most_affected", []),
        "examples_json":         json.dumps(enriched.get("examples", [])),
        "updated_at":            now,
        "backfilled_at":         now,
    }
    with driver.session() as s:
        s.run("""
            MATCH (n:BusinessModel {bim_id: $id})
            SET n += $props
        """, id=bim_id, props=props)


def backfill_one(driver, bim_id: str, name: str, dry_run: bool = False) -> dict:
    """
    Enrich a single existing BM node.  Returns a result dict:
    { "bim_id", "name", "status": "patched"|"dry_run"|"error", "message" }
    """
    try:
        # Web research
        search_context = web_search(name)

        # Call Claude — pass empty existing list so no duplicate detection runs
        enriched = call_claude(name, search_context, existing_bms=[])

        if dry_run:
            return {
                "bim_id":  bim_id,
                "name":    name,
                "status":  "dry_run",
                "message": f"Would patch {bim_id} — margins={enriched.get('typical_margins')}",
                "enriched": enriched,
            }

        patch_bm_node(driver, bim_id, enriched)
        return {
            "bim_id":  bim_id,
            "name":    name,
            "status":  "patched",
            "message": f"Patched {bim_id} — {len(enriched.get('examples', []))} examples, "
                       f"margins={enriched.get('typical_margins')}",
            "enriched": enriched,
        }

    except Exception as e:
        return {
            "bim_id":  bim_id,
            "name":    name,
            "status":  "error",
            "message": str(e),
            "enriched": {},
        }


def main():
    parser = argparse.ArgumentParser(description="Backfill descriptions for BusinessModel nodes")
    parser.add_argument("--missing-only", action="store_true", default=True,
                        help="Only enrich nodes with no description (default: True)")
    parser.add_argument("--all", dest="missing_only", action="store_false",
                        help="Re-enrich all nodes, even those already described")
    parser.add_argument("--id", dest="target_id", help="Backfill a specific BIM id only")
    parser.add_argument("--dry-run", action="store_true", help="Enrich but do not write to Neo4j")
    parser.add_argument("--limit", type=int, help="Process only first N nodes")
    args = parser.parse_args()

    driver = get_driver()
    nodes = get_nodes_to_backfill(driver, missing_only=args.missing_only, target_id=args.target_id)

    if args.limit:
        nodes = nodes[:args.limit]

    if not nodes:
        console.print("[green]Nothing to backfill — all nodes already have descriptions.[/green]")
        driver.close()
        return

    mode = "[yellow]DRY RUN[/yellow]" if args.dry_run else "[green]LIVE[/green]"
    console.print(f"\n[bold]BM Backfill[/bold] — {len(nodes)} nodes · mode={mode}\n")
    console.print("  Each node = 2 Tavily searches + 1 Claude call (~15-20 s)\n")

    results = []
    patched = errors = 0

    for i, node in enumerate(nodes, 1):
        console.print(f"[dim]({i}/{len(nodes)})[/dim] [bold]{node['id']}[/bold] — {node['name']}")
        result = backfill_one(driver, node["id"], node["name"], dry_run=args.dry_run)
        results.append(result)

        if result["status"] in ("patched", "dry_run"):
            patched += 1
            console.print(f"  [green]✓[/green] {result['message']}")
        else:
            errors += 1
            console.print(f"  [red]✗[/red] {result['message']}")

    driver.close()

    # Summary
    table = Table(title="\nBackfill Summary", show_header=True)
    table.add_column("Metric", style="bold")
    table.add_column("Count", justify="right")
    table.add_row("Patched" if not args.dry_run else "Would patch", str(patched))
    table.add_row("Errors", str(errors))
    table.add_row("Total", str(len(nodes)))
    console.print(table)

    if errors:
        sys.exit(1)


if __name__ == "__main__":
    main()
