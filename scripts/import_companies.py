"""
scripts/import_companies.py — Company node importer

Reads a JSON array of company records and writes them to Neo4j as Company nodes,
with OPERATES_AS relationships to BusinessModel nodes, and auto-links to any
Evidence nodes that mention the company name.

Usage:
    venv/bin/python3 scripts/import_companies.py data/companies.json
    venv/bin/python3 scripts/import_companies.py data/companies.json --dry-run
"""

import os
import sys
import json
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
from neo4j import GraphDatabase
from rich.console import Console
from rich.table import Table

load_dotenv(override=True)
console = Console(width=200)


def get_driver():
    return GraphDatabase.driver(
        os.getenv("NEO4J_URI"),
        auth=(os.getenv("NEO4J_USER", "neo4j"), os.getenv("NEO4J_PASSWORD")),
    )


def next_company_id(driver) -> str:
    with driver.session() as s:
        row = s.run(
            "MATCH (c:Company) RETURN c.company_id AS id ORDER BY id DESC LIMIT 1"
        ).single()
        if not row or not row["id"]:
            return "COMP_001"
        try:
            n = int(row["id"].split("_")[1]) + 1
            return f"COMP_{n:03d}"
        except Exception:
            return f"COMP_001"


def import_company(driver, rec: dict, now: str, dry_run: bool = False) -> dict:
    """Import one company record. Returns status dict."""
    cid   = rec.get("company_id", "")
    name  = rec.get("name", "").strip()

    if not name:
        return {"company_id": cid, "name": name, "status": "skipped_no_name"}

    props = {
        "company_id":          cid,
        "name":                name,
        "ticker":              rec.get("ticker", ""),
        "description":         rec.get("description", ""),
        "primary_industry":    rec.get("primary_industry", ""),
        "secondary_industries": rec.get("secondary_industries", []),
        "hq_country":          rec.get("hq_country", "USA"),
        "employee_range":      rec.get("employee_range", ""),
        "revenue_range":       rec.get("revenue_range", ""),
        "created_at":          now,
        "updated_at":          now,
        "source":              "import_companies",
    }

    primary_bim   = rec.get("primary_bim_id", "")
    secondary_bims = rec.get("secondary_bim_ids", [])

    if dry_run:
        return {
            "company_id": cid, "name": name,
            "primary_bim": primary_bim,
            "secondary_bims": secondary_bims,
            "status": "dry_run",
        }

    with driver.session() as s:
        # Upsert company node
        s.run(
            "MERGE (c:Company {company_id: $cid}) SET c += $props",
            cid=cid, props=props
        )

        # Primary OPERATES_AS relationship
        if primary_bim:
            s.run("""
                MATCH (c:Company {company_id: $cid})
                MATCH (b:BusinessModel {bim_id: $bid})
                MERGE (c)-[r:OPERATES_AS]->(b)
                SET r.is_primary = true, r.created_at = $now
            """, cid=cid, bid=primary_bim, now=now)

        # Secondary OPERATES_AS relationships
        for bid in secondary_bims:
            if bid and bid != primary_bim:
                s.run("""
                    MATCH (c:Company {company_id: $cid})
                    MATCH (b:BusinessModel {bim_id: $bid})
                    MERGE (c)-[r:OPERATES_AS]->(b)
                    SET r.is_primary = false, r.created_at = $now
                """, cid=cid, bid=bid, now=now)

        # Auto-link to Evidence nodes that mention this company
        linked = s.run("""
            MATCH (e:Evidence)
            WHERE any(c IN e.companies_mentioned WHERE toLower(c) CONTAINS toLower($name))
            MATCH (c:Company {company_id: $cid})
            MERGE (c)-[r:HAS_EVIDENCE]->(e)
            SET r.auto_linked = true, r.created_at = $now
            RETURN count(r) AS n
        """, name=name, cid=cid, now=now).single()
        evidence_linked = linked["n"] if linked else 0

    return {
        "company_id":     cid,
        "name":           name,
        "primary_bim":    primary_bim,
        "secondary_bims": secondary_bims,
        "evidence_linked": evidence_linked,
        "status":         "written",
    }


def run_import(filepath: str, dry_run: bool = False):
    with open(filepath) as f:
        companies = json.load(f)

    console.print(f"\n[bold]Company Importer[/bold] — {len(companies)} records from {filepath}")
    driver = get_driver()
    now = datetime.now(timezone.utc).isoformat()

    results = []
    for rec in companies:
        result = import_company(driver, rec, now, dry_run=dry_run)
        results.append(result)

    driver.close()

    # Summary table
    table = Table(title=f"Import Results {'(DRY RUN)' if dry_run else ''}", show_header=True)
    table.add_column("ID", width=12)
    table.add_column("Name", width=35)
    table.add_column("Primary BIM", width=12)
    table.add_column("Evidence", justify="right", width=8)
    table.add_column("Status", width=10)

    for r in results:
        color = "green" if r["status"] == "written" else ("yellow" if r["status"] == "dry_run" else "red")
        table.add_row(
            r.get("company_id", ""),
            r.get("name", "")[:34],
            r.get("primary_bim", ""),
            str(r.get("evidence_linked", "—")),
            f"[{color}]{r['status']}[/{color}]",
        )
    console.print(table)

    written  = sum(1 for r in results if r["status"] == "written")
    dry      = sum(1 for r in results if r["status"] == "dry_run")
    evidence = sum(r.get("evidence_linked", 0) for r in results)
    console.print(f"\n[bold]Done.[/bold] {written or dry} companies {'written' if not dry_run else 'would be written'}, "
                  f"{evidence} evidence links auto-created.")
    return results


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("filepath", help="Path to JSON array of company records")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    run_import(args.filepath, dry_run=args.dry_run)
