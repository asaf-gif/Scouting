"""
input_layer/add_companies.py — CLI for adding companies to the knowledge graph

Usage:
    # Single company
    python input_layer/add_companies.py --name "Palantir"
    python input_layer/add_companies.py --name "Snowflake" --dry-run

    # Batch from CSV (must have 'name' column)
    python input_layer/add_companies.py --csv data/sample_companies.csv
    python input_layer/add_companies.py --csv data/sample_companies.csv --limit 5
    python input_layer/add_companies.py --csv data/sample_companies.csv --dry-run
"""

import argparse
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from input_layer.company_enrichment import enrich_company, enrich_companies_from_csv


def main():
    parser = argparse.ArgumentParser(description="Add companies to the knowledge graph")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--name", help="Single company name to research and add")
    group.add_argument("--csv", help="Path to CSV file with 'name' column")
    parser.add_argument("--dry-run", action="store_true", help="Run without writing to Neo4j")
    parser.add_argument("--limit", type=int, help="Process only first N companies from CSV")
    args = parser.parse_args()

    if args.name:
        result = enrich_company(args.name, dry_run=args.dry_run)
        if result["status"] == "error":
            print(f"\nError: {result['message']}", file=sys.stderr)
            sys.exit(1)
    else:
        results = enrich_companies_from_csv(args.csv, dry_run=args.dry_run, limit=args.limit)
        errors = [r for r in results if r["status"] == "error"]
        if errors:
            print(f"\n{len(errors)} error(s) occurred.", file=sys.stderr)
            sys.exit(1)

    sys.exit(0)


if __name__ == "__main__":
    main()
