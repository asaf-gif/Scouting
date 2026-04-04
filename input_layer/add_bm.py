"""
input_layer/add_bm.py — CLI for adding a new Business Model

Usage:
    python input_layer/add_bm.py --name 'Embedded Finance'
    python input_layer/add_bm.py --name 'Subscription' --dry-run
"""

import argparse
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from input_layer.bm_enrichment import enrich_business_model


def main():
    parser = argparse.ArgumentParser(description="Add a new Business Model to the knowledge graph")
    parser.add_argument("--name", required=True, help="Business model name to research and add")
    parser.add_argument("--dry-run", action="store_true", help="Run enrichment without writing to Neo4j")
    args = parser.parse_args()

    result = enrich_business_model(args.name, dry_run=args.dry_run)

    if result["status"] == "error":
        print(f"\nError: {result['message']}", file=sys.stderr)
        sys.exit(1)

    sys.exit(0)


if __name__ == "__main__":
    main()
