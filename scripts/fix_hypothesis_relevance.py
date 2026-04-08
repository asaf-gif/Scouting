#!/usr/bin/env python3
"""
Fix hypothesis relevance by filtering EXPOSED_TO relationships
to only companies in sectors relevant to each hypothesis's business model.
Also populates BIM_029 (Layer Player) with appropriate companies.
"""

import sys

# Parse .env manually
env = {}
with open('/Users/asafg/Claude code/scouting/.env') as f:
    for line in f:
        line = line.strip()
        if line and not line.startswith('#') and '=' in line:
            k, v = line.split('=', 1)
            env[k.strip()] = v.strip()

from neo4j import GraphDatabase

driver = GraphDatabase.driver(
    env['NEO4J_URI'],
    auth=(env['NEO4J_USER'], env['NEO4J_PASSWORD'])
)

# ============================================================
# SECTOR FILTER RULES per business model (from_bm id)
# ============================================================

BM_SECTOR_RULES = {
    # BIM_002 - Advertising
    'BIM_002': [
        'Communication Services',
        'Consumer Discretionary',
        'Information Technology',
    ],
    # BIM_004 - Marketplace
    'BIM_004': [
        'Consumer Discretionary',
        'Information Technology',
        'Communication Services',
        'Real Estate',
    ],
    # BIM_005 - SaaS / Enterprise Software
    'BIM_005': [
        'Information Technology',
        'Communication Services',
        'Financials',
        'Health Care',
    ],
    # BIM_006 - E-commerce / Retail
    'BIM_006': [
        'Consumer Discretionary',
        'Consumer Staples',
        'Information Technology',
    ],
    # BIM_009 - Direct Sales (B2B)
    'BIM_009': [
        'Information Technology',
        'Industrials',
        'Health Care',
        'Consumer Discretionary',
        'Communication Services',
        'Financials',
        'Energy',          # B2B energy services (keep some)
        'Consumer Staples', # some B2B sellers here
    ],
    # BIM_015 - Professional Services / Consulting
    'BIM_015': [
        'Information Technology',
        'Financials',
        'Health Care',
        'Industrials',
        'Communication Services',
    ],
    # BIM_026 - Project-Based Billing
    'BIM_026': [
        'Industrials',
        'Information Technology',
        'Financials',
        'Health Care',
    ],
    # BIM_037 - Transactional
    'BIM_037': [
        'Financials',
        'Information Technology',
    ],
    # BIM_001 - Subscription
    'BIM_001': [
        'Information Technology',
        'Communication Services',
        'Consumer Discretionary',
        'Health Care',
        'Financials',
    ],
    # BIM_003, BIM_010, BIM_011 - keep all (no filter)
    # Any other BM not listed = no filter applied
}


def get_all_hypotheses(session):
    result = session.run("""
        MATCH (h:DisruptionHypothesis)-[:TARGETS]->(fb:BusinessModel),
              (h)-[:PROPOSES]->(tb:BusinessModel)
        OPTIONAL MATCH (c:Company)-[:EXPOSED_TO]->(h)
        RETURN h.hypothesis_id as hid,
               h.title as title,
               fb.bim_id as from_bm_id,
               fb.name as from_bm,
               tb.name as to_bm,
               count(DISTINCT c) as company_count
        ORDER BY hid
    """)
    return [dict(r) for r in result]


def get_sector_breakdown(session, hid):
    result = session.run("""
        MATCH (c:Company)-[:EXPOSED_TO]->(h:DisruptionHypothesis {hypothesis_id: $hid})
        RETURN c.gics_sector as sector, count(*) as cnt
        ORDER BY cnt DESC
    """, hid=hid)
    return [(r['sector'], r['cnt']) for r in result]


def delete_irrelevant_companies(session, hid, allowed_sectors):
    result = session.run("""
        MATCH (c:Company)-[r:EXPOSED_TO]->(h:DisruptionHypothesis {hypothesis_id: $hid})
        WHERE NOT c.gics_sector IN $allowed_sectors
        DELETE r
        RETURN count(*) as deleted
    """, hid=hid, allowed_sectors=allowed_sectors)
    rec = result.single()
    return rec['deleted'] if rec else 0


def get_top_companies(session, hid, limit=5):
    result = session.run("""
        MATCH (c:Company)-[:EXPOSED_TO]->(h:DisruptionHypothesis {hypothesis_id: $hid})
        WHERE c.fortune_rank IS NOT NULL
        RETURN c.name as name, c.fortune_rank as rank, c.gics_sector as sector
        ORDER BY c.fortune_rank ASC
        LIMIT $limit
    """, hid=hid, limit=limit)
    return [dict(r) for r in result]


def get_remaining_count(session, hid):
    result = session.run("""
        MATCH (c:Company)-[:EXPOSED_TO]->(h:DisruptionHypothesis {hypothesis_id: $hid})
        RETURN count(DISTINCT c) as cnt
    """, hid=hid)
    rec = result.single()
    return rec['cnt'] if rec else 0


# ============================================================
# BIM_029 Layer Player population
# ============================================================

BIM_029_CANDIDATES = [
    'Fiserv',
    'Fidelity National Information Services',
    'FIS',
    'Jack Henry & Associates',
    'Broadridge Financial Solutions',
    'S&P Global',
    'MSCI',
    'Morningstar',
    'Intercontinental Exchange',
    'CME Group',
    'Nasdaq',
    'Verisk Analytics',
]


def check_bim_029_existing(session):
    result = session.run("""
        MATCH (c:Company)-[:OPERATES_AS]->(bm:BusinessModel {bm_id: 'BIM_029'})
        RETURN c.name as name, c.gics_sector as sector, c.fortune_rank as rank
        ORDER BY c.fortune_rank ASC
    """)
    return [dict(r) for r in result]


def find_candidates_in_db(session, names):
    """Find companies by approximate name match."""
    found = []
    for name in names:
        result = session.run("""
            MATCH (c:Company)
            WHERE toLower(c.name) CONTAINS toLower($name)
               OR toLower($name) CONTAINS toLower(c.name)
            RETURN c.name as name, c.company_id as cid, c.gics_sector as sector, c.fortune_rank as rank
            LIMIT 3
        """, name=name)
        rows = [dict(r) for r in result]
        if rows:
            found.append((name, rows))
    return found


def add_operates_as_bim029(session, company_id):
    result = session.run("""
        MATCH (c:Company {company_id: $cid})
        MATCH (bm:BusinessModel {bim_id: 'BIM_029'})
        MERGE (c)-[:OPERATES_AS]->(bm)
        RETURN c.name as name
    """, cid=company_id)
    records = list(result)
    return records[0]['name'] if records else None


# ============================================================
# MAIN
# ============================================================

def main():
    print("=" * 70)
    print("HYPOTHESIS RELEVANCE FILTER - Neo4j Scouting Database")
    print("=" * 70)

    with driver.session() as session:
        # Step 1: Get all hypotheses
        print("\n--- Step 1: Current hypothesis state ---\n")
        hypotheses = get_all_hypotheses(session)

        print(f"{'HID':<12} {'From BM':<12} {'Count':>7}  Title")
        print("-" * 70)
        for h in hypotheses:
            print(f"{h['hid']:<12} {h['from_bm_id'] or 'N/A':<12} {h['company_count']:>7}  {h['title'][:45]}")

        # Step 2: For each hypothesis, show sector breakdown then filter
        print("\n--- Step 2: Filtering by sector relevance ---\n")

        summary = []

        for h in hypotheses:
            hid = h['hid']
            from_bm_id = h['from_bm_id']
            before_count = h['company_count']

            # Determine if this BM has a sector filter
            allowed_sectors = BM_SECTOR_RULES.get(from_bm_id)

            if allowed_sectors is None:
                print(f"\n[{hid}] {h['title'][:55]}")
                print(f"  From BM: {from_bm_id} ({h['from_bm']}) -> No filter applied (BM not in rules)")
                print(f"  Companies: {before_count} -> {before_count} (unchanged)")
                summary.append({
                    'hid': hid,
                    'title': h['title'],
                    'from_bm': h['from_bm'],
                    'to_bm': h['to_bm'],
                    'before': before_count,
                    'after': before_count,
                    'deleted': 0,
                    'filtered': False,
                })
                continue

            # Show sector breakdown before filtering
            sectors_before = get_sector_breakdown(session, hid)
            print(f"\n[{hid}] {h['title'][:55]}")
            print(f"  From BM: {from_bm_id} ({h['from_bm']})")
            print(f"  Allowed sectors: {', '.join(allowed_sectors)}")
            print(f"  Sector breakdown BEFORE ({before_count} companies):")
            for sector, cnt in sectors_before:
                marker = "  OK" if sector in allowed_sectors else "  REMOVE"
                print(f"    {marker:8} {sector or 'None':<35} {cnt:>4}")

            # Delete irrelevant
            deleted = delete_irrelevant_companies(session, hid, allowed_sectors)
            after_count = get_remaining_count(session, hid)

            print(f"  Deleted {deleted} relationships -> {after_count} companies remain")

            summary.append({
                'hid': hid,
                'title': h['title'],
                'from_bm': h['from_bm'],
                'to_bm': h['to_bm'],
                'before': before_count,
                'after': after_count,
                'deleted': deleted,
                'filtered': True,
            })

        # Step 3: Verify remaining companies
        print("\n--- Step 3: Verification - top companies per hypothesis ---\n")

        for s in summary:
            hid = s['hid']
            top = get_top_companies(session, hid, limit=5)
            sectors_after = get_sector_breakdown(session, hid)

            print(f"\n[{hid}] {s['title'][:60]}")
            print(f"  {s['from_bm']} -> {s['to_bm']}")
            print(f"  Companies: {s['before']} -> {s['after']} (deleted {s['deleted']})")
            print(f"  Sector breakdown after:")
            for sector, cnt in sectors_after:
                print(f"    {sector or 'None':<35} {cnt:>4}")
            if top:
                print(f"  Top companies by Fortune rank:")
                for c in top:
                    print(f"    #{c['rank']:>4}  {c['name']:<40} [{c['sector']}]")
            else:
                print(f"  (No companies with fortune_rank)")

        # Step 4: BIM_029 Layer Player
        print("\n" + "=" * 70)
        print("BIM_029 LAYER PLAYER - Population")
        print("=" * 70)

        existing = check_bim_029_existing(session)
        print(f"\nExisting BIM_029 companies ({len(existing)}):")
        for c in existing:
            print(f"  {c['name']:<45} [{c['sector']}]  rank={c['rank']}")

        print(f"\nSearching for Layer Player candidates...")
        found_candidates = find_candidates_in_db(session, BIM_029_CANDIDATES)

        existing_names = {c['name'].lower() for c in existing}
        added = []

        for search_name, matches in found_candidates:
            print(f"\n  Search: '{search_name}'")
            for m in matches:
                print(f"    Found: {m['name']} (id={m['cid']}, sector={m['sector']}, rank={m['rank']})")

            # Pick the best match (first one, since we searched specifically)
            best = matches[0]
            if best['name'].lower() not in existing_names:
                name = add_operates_as_bim029(session, best['cid'])
                if name:
                    print(f"    -> Added OPERATES_AS BIM_029: {name}")
                    added.append(best)
                    existing_names.add(best['name'].lower())
            else:
                print(f"    -> Already in BIM_029: {best['name']}")

        print(f"\nAdded {len(added)} new companies to BIM_029:")
        for c in added:
            print(f"  {c['name']:<45} [{c['sector']}]")

        # Final BIM_029 state
        final_029 = check_bim_029_existing(session)
        print(f"\nFinal BIM_029 companies ({len(final_029)}):")
        for c in final_029:
            print(f"  {c['name']:<45} [{c['sector']}]  rank={c['rank']}")

        # Also expose BIM_029 companies to any BIM_029 hypotheses
        print("\nChecking for hypotheses targeting BIM_029...")
        h029_result = session.run("""
            MATCH (h:DisruptionHypothesis)-[:TARGETS]->(bm:BusinessModel {bm_id: 'BIM_029'})
            RETURN h.hypothesis_id as hid, h.title as title
        """)
        h029_list = [dict(r) for r in h029_result]
        if h029_list:
            for h in h029_list:
                print(f"  Hypothesis {h['hid']}: {h['title']}")
                # Add EXPOSED_TO for all BIM_029 companies that don't already have it
                exposed_result = session.run("""
                    MATCH (c:Company)-[:OPERATES_AS]->(bm:BusinessModel {bm_id: 'BIM_029'})
                    MATCH (h:DisruptionHypothesis {hypothesis_id: $hid})
                    MERGE (c)-[:EXPOSED_TO]->(h)
                    RETURN count(*) as cnt
                """, hid=h['hid'])
                rec = exposed_result.single()
                print(f"  -> Ensured EXPOSED_TO for {rec['cnt']} companies")
        else:
            print("  No hypotheses directly targeting BIM_029 found.")

        # ============================================================
        # FINAL SUMMARY
        # ============================================================
        print("\n" + "=" * 70)
        print("FINAL SUMMARY")
        print("=" * 70)
        print(f"\n{'HID':<12} {'Before':>7} {'After':>7} {'Deleted':>8}  Title")
        print("-" * 70)
        for s in summary:
            print(f"{s['hid']:<12} {s['before']:>7} {s['after']:>7} {s['deleted']:>8}  {s['title'][:40]}")

        print("\n--- Top companies per hypothesis (final state) ---\n")
        for s in summary:
            hid = s['hid']
            top = get_top_companies(session, hid, limit=5)
            print(f"\n[{hid}] {s['title'][:60]}")
            print(f"  {s['from_bm']} -> {s['to_bm']}")
            print(f"  Companies: {s['before']} -> {s['after']}")
            if top:
                for c in top:
                    print(f"    #{c['rank']:>4}  {c['name']:<40} [{c['sector']}]")
            else:
                # Fall back to any companies without fortune_rank
                result = session.run("""
                    MATCH (c:Company)-[:EXPOSED_TO]->(h:DisruptionHypothesis {hypothesis_id: $hid})
                    RETURN c.name as name, c.gics_sector as sector, c.fortune_rank as rank
                    LIMIT 5
                """, hid=hid)
                rows = [dict(r) for r in result]
                for c in rows:
                    print(f"    rank={c['rank']}  {c['name']:<40} [{c['sector']}]")
                if not rows:
                    print("    (No companies remaining)")

    driver.close()
    print("\nDone.")


if __name__ == '__main__':
    main()
