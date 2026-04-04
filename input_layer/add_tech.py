"""
input_layer/add_tech.py — CLI for adding technologies to the knowledge graph

Usage:
    python input_layer/add_tech.py --name "Retrieval-Augmented Generation"
    python input_layer/add_tech.py --name "RAG" --dry-run
"""

import argparse
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from input_layer.tech_enrichment import enrich_technology


def main():
    parser = argparse.ArgumentParser(description="Add a technology to the knowledge graph")
    parser.add_argument("--name", required=True, help="Technology name to research and add")
    parser.add_argument("--dry-run", action="store_true", help="Run without writing to Neo4j")
    args = parser.parse_args()

    result = enrich_technology(args.name, dry_run=args.dry_run)

    if result["status"] == "error":
        print(f"\nError: {result['message']}", file=sys.stderr)
        sys.exit(1)

    sys.exit(0)


if __name__ == "__main__":
    main()
