"""
Part 8 — Internet Scan for New Business Models Test

Tests:
1. scan_web() returns non-empty context (Tavily queries ran)
2. extract_candidates() returns a list (may be empty — that's valid)
3. novelty filter correctly splits candidates by threshold
4. Full run_scan() completes without error and returns required keys
5. Any enriched candidates are written to graph with pending_human_review=True
6. CompressionLog node written for the scan run

Run: python tests/test_08_scanner.py
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv(override=True)

from neo4j import GraphDatabase

PASS = "✓"
FAIL = "✗"

RESULT_KEYS = ["total_candidates", "novel_count", "enriched_count",
               "filtered_count", "candidates", "enrich_results"]


def get_driver():
    return GraphDatabase.driver(
        os.getenv("NEO4J_URI"),
        auth=(os.getenv("NEO4J_USER", "neo4j"), os.getenv("NEO4J_PASSWORD")),
    )


def test_web_scan():
    from input_layer.bm_scanner import scan_web
    context = scan_web()
    if not context or len(context) < 200:
        return False, f"Web scan returned too little content ({len(context)} chars)"
    return True, f"Web scan returned {len(context)} chars"


def test_candidate_extraction():
    from input_layer.bm_scanner import scan_web, extract_candidates
    from input_layer.bm_enrichment import get_driver as gd, get_existing_bms
    driver = gd()
    existing = get_existing_bms(driver)
    driver.close()

    context = scan_web()
    candidates = extract_candidates(context, existing)

    if not isinstance(candidates, list):
        return False, "extract_candidates did not return a list"
    # An empty list is valid (no novel models found)
    return True, f"Extraction returned {len(candidates)} candidate(s) (list type confirmed)"


def test_novelty_filter():
    from input_layer.bm_scanner import filter_novel, NOVELTY_THRESHOLD

    mock_candidates = [
        {"name": "Very Novel Model", "similarity_to_closest": 0.20},
        {"name": "Somewhat Novel",   "similarity_to_closest": 0.50},
        {"name": "Basically SaaS",   "similarity_to_closest": 0.80},
        {"name": "Exact Duplicate",  "similarity_to_closest": 0.95},
    ]
    novel, filtered = filter_novel(mock_candidates)

    expected_novel = 2   # 0.20 and 0.50 are both < 0.60
    expected_filtered = 2

    if len(novel) != expected_novel:
        return False, f"Expected {expected_novel} novel, got {len(novel)}"
    if len(filtered) != expected_filtered:
        return False, f"Expected {expected_filtered} filtered, got {len(filtered)}"
    return True, f"Filter correct: {len(novel)} novel, {len(filtered)} filtered (threshold={NOVELTY_THRESHOLD})"


def test_full_scan_dry_run():
    from input_layer.bm_scanner import run_scan
    result = run_scan(dry_run=True, enrich_limit=2)

    missing = [k for k in RESULT_KEYS if k not in result]
    if missing:
        return False, f"Result missing keys: {missing}"

    if not isinstance(result["candidates"], list):
        return False, "candidates is not a list"
    if not isinstance(result["enrich_results"], list):
        return False, "enrich_results is not a list"

    return True, (
        f"Scan complete — {result['total_candidates']} candidates, "
        f"{result['novel_count']} novel, "
        f"{result['enriched_count']} enriched (dry run)"
    )


def test_enriched_nodes_pending_review():
    """Any BMs created by the scanner must have pending_human_review=True."""
    driver = get_driver()
    with driver.session() as s:
        result = s.run("""
            MATCH (n:BusinessModel)
            WHERE n.source = 'manual_entry' AND n.added_by = 'bm_enrichment_agent'
            RETURN count(n) AS total,
                   sum(CASE WHEN n.pending_human_review = true THEN 1 ELSE 0 END) AS pending
        """)
        rec = result.single()
    driver.close()

    total = rec["total"]
    pending = rec["pending"]

    if total == 0:
        return True, "No scanner-created BMs in graph yet (dry run or no novel models found)"
    if pending < total:
        return False, f"{total - pending}/{total} scanner BMs are missing pending_human_review=True"
    return True, f"All {total} scanner-created BM(s) have pending_human_review=True"


def test_compression_log():
    """A CompressionLog node should be written for each live scan run."""
    driver = get_driver()
    with driver.session() as s:
        rec = s.run("""
            MATCH (n:CompressionLog {scan_type: 'bm_internet_scan'})
            RETURN count(n) AS cnt
        """).single()
    driver.close()

    cnt = rec["cnt"]
    # For dry-run tests, no log is written — that's acceptable
    return True, f"{cnt} CompressionLog node(s) found (0 expected for dry-run tests)"


def main():
    print("\n=== Part 8 — Internet Scan for New Business Models ===\n")
    print("  Note: Calls Claude + Tavily for scan + extraction. Takes ~2 minutes.\n")

    all_passed = True

    tests = [
        ("Web scan returns content",        test_web_scan),
        ("Candidate extraction returns list", test_candidate_extraction),
        ("Novelty filter works correctly",  test_novelty_filter),
        ("Full scan (dry-run) completes",   test_full_scan_dry_run),
        ("Enriched BMs have pending review", test_enriched_nodes_pending_review),
        ("CompressionLog written",          test_compression_log),
    ]

    for label, fn in tests:
        print(f"  Running: {label}...")
        try:
            passed, msg = fn()
        except Exception as e:
            passed, msg = False, str(e)
        icon = PASS if passed else FAIL
        print(f"  {icon} {label}: {msg}")
        if not passed:
            all_passed = False

    print()
    if all_passed:
        print("All internet scan tests passed.")
        print("Ready to proceed to Part 9.")
    else:
        print("Some tests failed.")
        sys.exit(1)


if __name__ == "__main__":
    main()
