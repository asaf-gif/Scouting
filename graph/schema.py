"""
graph/schema.py — Apply Neo4j schema (constraints + indexes) from schema.cypher

Usage:
    python graph/schema.py
    python graph/schema.py --drop   # drop all constraints and indexes first (reset)
"""

import os
import sys
import argparse

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
from neo4j import GraphDatabase
from rich.console import Console
from rich.table import Table

load_dotenv()
console = Console()


def get_driver():
    uri = os.getenv("NEO4J_URI")
    user = os.getenv("NEO4J_USER", "neo4j")
    password = os.getenv("NEO4J_PASSWORD")
    return GraphDatabase.driver(uri, auth=(user, password))


def drop_all(driver):
    """Drop all existing constraints and indexes (clean slate)."""
    console.print("[yellow]Dropping all existing constraints and indexes...[/yellow]")
    with driver.session() as session:
        # Drop constraints first (indexes are dropped automatically with them)
        constraints = session.run("SHOW CONSTRAINTS").data()
        for c in constraints:
            name = c.get("name")
            if name:
                session.run(f"DROP CONSTRAINT {name} IF EXISTS")
                console.print(f"  Dropped constraint: {name}")

        # Drop any remaining standalone indexes
        indexes = session.run("SHOW INDEXES").data()
        for idx in indexes:
            if idx.get("type") != "LOOKUP":  # never drop built-in lookup indexes
                name = idx.get("name")
                if name:
                    try:
                        session.run(f"DROP INDEX {name} IF EXISTS")
                        console.print(f"  Dropped index: {name}")
                    except Exception:
                        pass  # already dropped with constraint


def apply_schema(driver):
    """Read schema.cypher and execute each statement."""
    schema_path = os.path.join(os.path.dirname(__file__), "schema.cypher")
    with open(schema_path, "r") as f:
        raw = f.read()

    # Split on semicolons, skip comments and blanks
    statements = []
    for block in raw.split(";"):
        lines = [l for l in block.splitlines() if l.strip() and not l.strip().startswith("//")]
        stmt = " ".join(lines).strip()
        if stmt:
            statements.append(stmt)

    console.print(f"\n[bold]Applying {len(statements)} schema statements...[/bold]")
    ok = 0
    errors = []
    with driver.session() as session:
        for stmt in statements:
            try:
                session.run(stmt)
                ok += 1
            except Exception as e:
                errors.append((stmt[:60] + "...", str(e)))

    if errors:
        console.print(f"\n[red]Errors ({len(errors)}):[/red]")
        for s, e in errors:
            console.print(f"  [red]✗[/red] {s}\n    {e}")
    console.print(f"\n[green]✓ {ok} statements applied successfully[/green]")
    return len(errors) == 0


def verify_schema(driver):
    """Check that all expected constraints and indexes are present."""
    expected_constraints = [
        "bm_id_unique", "vector_id_unique", "scalar_id_unique",
        "company_id_unique", "industry_id_unique", "technology_id_unique",
        "hypothesis_id_unique", "evidence_id_unique", "evaluation_id_unique",
        "review_item_id_unique", "compression_log_id_unique",
    ]
    expected_indexes = [
        "bm_name", "bm_status", "company_name", "company_monitoring_status",
        "industry_name", "technology_name", "technology_tracking_status",
        "hypothesis_status", "hypothesis_confidence", "hypothesis_updated_at",
        "evidence_source_type", "evidence_status", "evaluation_alert_status",
        "review_item_status", "review_item_priority",
    ]

    with driver.session() as session:
        active_constraints = {r["name"] for r in session.run("SHOW CONSTRAINTS").data()}
        active_indexes = {r["name"] for r in session.run("SHOW INDEXES").data()}

    # Build report table
    table = Table(title="Schema Verification", show_lines=True)
    table.add_column("Type", style="bold")
    table.add_column("Name")
    table.add_column("Status")

    all_ok = True
    for name in expected_constraints:
        ok = name in active_constraints
        table.add_row("Constraint", name, "[green]✓ active[/green]" if ok else "[red]✗ missing[/red]")
        if not ok:
            all_ok = False

    for name in expected_indexes:
        ok = name in active_indexes
        table.add_row("Index", name, "[green]✓ active[/green]" if ok else "[red]✗ missing[/red]")
        if not ok:
            all_ok = False

    console.print(table)
    console.print(f"\nConstraints found: {len(active_constraints)}  |  Indexes found: {len(active_indexes)}")
    return all_ok


def test_duplicate_constraint(driver):
    """Verify that inserting a duplicate BusinessModel ID raises a ConstraintError."""
    from neo4j.exceptions import ConstraintError
    with driver.session() as session:
        # Clean up any leftover test node
        session.run("MATCH (n:BusinessModel {bim_id: '__test__'}) DELETE n")
        # Create first node
        session.run("CREATE (:BusinessModel {bim_id: '__test__', name: 'Test'})")
        try:
            # Attempt duplicate — should raise
            session.run("CREATE (:BusinessModel {bim_id: '__test__', name: 'Duplicate'})")
            session.run("MATCH (n:BusinessModel {bim_id: '__test__'}) DELETE n")
            return False  # should not reach here
        except ConstraintError:
            session.run("MATCH (n:BusinessModel {bim_id: '__test__'}) DELETE n")
            return True


def main():
    parser = argparse.ArgumentParser(description="Apply Neo4j schema for Systematic Problem Scouting")
    parser.add_argument("--drop", action="store_true", help="Drop all constraints/indexes before applying")
    args = parser.parse_args()

    driver = get_driver()

    if args.drop:
        drop_all(driver)

    apply_schema(driver)
    console.print()

    all_ok = verify_schema(driver)

    console.print("\n[bold]Testing duplicate constraint...[/bold]")
    dup_ok = test_duplicate_constraint(driver)
    if dup_ok:
        console.print("[green]✓ Duplicate ID constraint correctly raised ConstraintError[/green]")
    else:
        console.print("[red]✗ Duplicate ID was NOT rejected — constraint not working[/red]")

    driver.close()

    if all_ok and dup_ok:
        console.print("\n[bold green]Schema applied and verified. Ready for Part 3.[/bold green]")
    else:
        console.print("\n[bold red]Schema verification failed. Check errors above.[/bold red]")
        sys.exit(1)


if __name__ == "__main__":
    main()
