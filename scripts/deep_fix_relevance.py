#!/usr/bin/env python3
"""
Deep fix: company-hypothesis relevance in Neo4j scouting database.
Fixes all 7 problems found in the full audit, then rebuilds EXPOSED_TO from scratch.
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


def run_query(session, query, **params):
    """Run a query and return list of dicts."""
    result = session.run(query, **params)
    return [dict(r) for r in result]


def run_single(session, query, **params):
    """Run a query and return first record as dict."""
    result = session.run(query, **params)
    rec = result.single()
    return dict(rec) if rec else {}


# ============================================================
# PROBLEM 1: Create BIM_041, move hypothesis, populate
# ============================================================

def fix_problem_1(session):
    print("\n" + "=" * 80)
    print("PROBLEM 1: Split BIM_026 - Create BIM_041 (Research & Advisory)")
    print("=" * 80)

    # 1a. Update BIM_026 description
    print("\n--- 1a. Update BIM_026 description ---")
    session.run("""
        MATCH (bm:BusinessModel {bim_id: 'BIM_026'})
        SET bm.name = 'Project-Based (Engineering & Defense)',
            bm.description = 'Companies billing on per-project/contract basis for engineering, construction, or defense programs.'
    """)
    r = run_single(session, "MATCH (bm:BusinessModel {bim_id: 'BIM_026'}) RETURN bm.name as name, bm.description as desc")
    print(f"  BIM_026 updated: name='{r['name']}', desc='{r['desc']}'")

    # 1b. Create BIM_041
    print("\n--- 1b. Create BIM_041 ---")
    session.run("""
        MERGE (bm:BusinessModel {bim_id: 'BIM_041'})
        SET bm.name = 'Project-Based (Research & Advisory)',
            bm.description = 'Companies that sell custom research studies, bespoke advisory engagements, or project-scoped analytical work. Revenue is project-based but the output is information/insights, not physical goods.'
    """)
    r = run_single(session, "MATCH (bm:BusinessModel {bim_id: 'BIM_041'}) RETURN bm.name as name, bm.description as desc")
    print(f"  BIM_041 created: name='{r['name']}', desc='{r['desc']}'")

    # 1c. Move hypothesis HYP_TECH_003_BIM_026_BIM_005 from BIM_026 to BIM_041
    print("\n--- 1c. Move hypothesis HYP_TECH_003_BIM_026_BIM_005 to target BIM_041 ---")
    hyp_id = 'HYP_TECH_003_BIM_026_BIM_005'

    # Show current state
    before = run_query(session, """
        MATCH (h:DisruptionHypothesis {hypothesis_id: $hid})-[:TARGETS]->(bm:BusinessModel)
        RETURN bm.bim_id as bim_id, bm.name as name
    """, hid=hyp_id)
    print(f"  Before: targets {before}")

    # Delete old TARGETS, create new one
    session.run("""
        MATCH (h:DisruptionHypothesis {hypothesis_id: $hid})-[r:TARGETS]->(old:BusinessModel {bim_id: 'BIM_026'})
        DELETE r
    """, hid=hyp_id)
    session.run("""
        MATCH (h:DisruptionHypothesis {hypothesis_id: $hid})
        MATCH (new:BusinessModel {bim_id: 'BIM_041'})
        MERGE (h)-[:TARGETS]->(new)
    """, hid=hyp_id)

    # Update hypothesis properties
    session.run("""
        MATCH (h:DisruptionHypothesis {hypothesis_id: $hid})
        SET h.from_bim_id = 'BIM_041', h.from_bm_name = 'Project-Based (Research & Advisory)'
    """, hid=hyp_id)

    after = run_query(session, """
        MATCH (h:DisruptionHypothesis {hypothesis_id: $hid})-[:TARGETS]->(bm:BusinessModel)
        RETURN bm.bim_id as bim_id, bm.name as name
    """, hid=hyp_id)
    print(f"  After: targets {after}")

    # 1d. Populate BIM_041 with research/advisory companies
    print("\n--- 1d. Populate BIM_041 with research/advisory companies ---")
    search_names = [
        'nielsen', 'ims health', 'verisk', 'msci', 'morningstar', 'gartner',
        'booz allen', 'navigant', 'fti consulting', 'dun & bradstreet',
        'ihs markit', 'gallup', 'kantar', 'ipsos', 'forrester', 'comscore',
        'palantir', 's&p global', "moody", 'wolters kluwer', 'factset',
        'wood mackenzie', 'frost & sullivan'
    ]

    found = run_query(session, """
        MATCH (c:Company)
        WHERE any(term IN $names WHERE toLower(c.name) CONTAINS term)
        RETURN c.name as name, c.company_id as cid, c.fortune_rank as rank,
               c.gics_sector as sector, c.gics_industry_group as ig
        ORDER BY c.fortune_rank ASC
    """, names=search_names)

    print(f"  Found {len(found)} research/advisory candidates:")
    for c in found:
        print(f"    {c['name']:<45} rank={str(c['rank'] or 'N/A'):<6} sector={str(c['sector'] or 'N/A'):<30} ig={c['ig'] or 'N/A'}")

    # Add OPERATES_AS BIM_041 for each
    added_count = 0
    for c in found:
        r = run_single(session, """
            MATCH (c:Company {company_id: $cid})
            MATCH (bm:BusinessModel {bim_id: 'BIM_041'})
            MERGE (c)-[rel:OPERATES_AS]->(bm)
            RETURN c.name as name
        """, cid=c['cid'])
        if r:
            added_count += 1
            print(f"    + OPERATES_AS BIM_041: {r['name']}")

    print(f"  Total added to BIM_041: {added_count}")


# ============================================================
# PROBLEM 3: Move gaming companies from BIM_005 to BIM_006
# ============================================================

def fix_problem_3(session):
    print("\n" + "=" * 80)
    print("PROBLEM 3: Move gaming companies from BIM_005 (SaaS) to BIM_006 (E-commerce)")
    print("=" * 80)

    gaming_names = ['activision', 'electronic arts', 'take-two']

    for name_fragment in gaming_names:
        # Find the company
        companies = run_query(session, """
            MATCH (c:Company)-[:OPERATES_AS]->(bm:BusinessModel {bim_id: 'BIM_005'})
            WHERE toLower(c.name) CONTAINS $name
            RETURN c.name as name, c.company_id as cid, c.gics_industry_group as ig
        """, name=name_fragment)

        for c in companies:
            print(f"\n  Processing: {c['name']} (ig={c['ig']})")

            # Remove from BIM_005
            session.run("""
                MATCH (c:Company {company_id: $cid})-[r:OPERATES_AS]->(bm:BusinessModel {bim_id: 'BIM_005'})
                DELETE r
            """, cid=c['cid'])
            print(f"    - Removed OPERATES_AS BIM_005")

            # Add to BIM_006
            session.run("""
                MATCH (c:Company {company_id: $cid})
                MATCH (bm:BusinessModel {bim_id: 'BIM_006'})
                MERGE (c)-[:OPERATES_AS]->(bm)
            """, cid=c['cid'])
            print(f"    + Added OPERATES_AS BIM_006")

    # Verify BIM_005 remaining
    remaining = run_query(session, """
        MATCH (c:Company)-[:OPERATES_AS]->(bm:BusinessModel {bim_id: 'BIM_005'})
        RETURN c.name as name, c.gics_industry_group as ig
        ORDER BY c.name
    """)
    print(f"\n  BIM_005 remaining ({len(remaining)} companies):")
    for c in remaining:
        print(f"    {c['name']:<45} ig={c['ig']}")


# ============================================================
# PROBLEM 4: Fix BIM_006 "Diversified" companies
# ============================================================

def fix_problem_4(session):
    print("\n" + "=" * 80)
    print("PROBLEM 4: Fix BIM_006 (E-commerce/Retail) - Remove non-retail Diversified")
    print("=" * 80)

    # Get all Diversified in BIM_006
    diversified = run_query(session, """
        MATCH (c:Company)-[:OPERATES_AS]->(bm:BusinessModel {bim_id: 'BIM_006'})
        WHERE c.gics_industry_group = 'Diversified'
        RETURN c.name as name, c.company_id as cid, c.fortune_rank as rank
        ORDER BY c.name
    """)

    print(f"\n  Diversified companies in BIM_006 ({len(diversified)}):")
    for c in diversified:
        print(f"    {c['name']:<45} rank={c['rank']}")

    # Keep these e-commerce companies
    keep_names_lower = ['1-800-flowers']

    for c in diversified:
        name_lower = c['name'].lower()
        is_keep = any(k in name_lower for k in keep_names_lower)

        if is_keep:
            print(f"\n  KEEP in BIM_006: {c['name']}")
        else:
            print(f"\n  MOVE {c['name']}: BIM_006 -> BIM_009 (Direct Sales)")
            # Remove from BIM_006
            session.run("""
                MATCH (c:Company {company_id: $cid})-[r:OPERATES_AS]->(bm:BusinessModel {bim_id: 'BIM_006'})
                DELETE r
            """, cid=c['cid'])
            print(f"    - Removed OPERATES_AS BIM_006")

            # Add to BIM_009
            session.run("""
                MATCH (c:Company {company_id: $cid})
                MATCH (bm:BusinessModel {bim_id: 'BIM_009'})
                MERGE (c)-[:OPERATES_AS]->(bm)
            """, cid=c['cid'])
            print(f"    + Added OPERATES_AS BIM_009")


# ============================================================
# PROBLEM 5: Fix BIM_016 (Bundling) - Remove non-bundling companies
# ============================================================

def fix_problem_5(session):
    print("\n" + "=" * 80)
    print("PROBLEM 5: Fix BIM_016 (Bundling) - Remove non-bundling companies")
    print("=" * 80)

    # Telecom bundling names to KEEP
    keep_names_lower = ['at&t', 'verizon', 'comcast', 'charter', 't-mobile', 'dish', 'altice',
                        'frontier', 'lumen', 'cox', 'directv', 'spectrum']

    # Get all non-telecom companies (Diversified group specifically)
    non_bundling = run_query(session, """
        MATCH (c:Company)-[:OPERATES_AS]->(bm:BusinessModel {bim_id: 'BIM_016'})
        WHERE c.gics_industry_group = 'Diversified'
        RETURN c.name as name, c.company_id as cid, c.fortune_rank as rank
        ORDER BY c.name
    """)

    print(f"\n  Diversified companies in BIM_016 ({len(non_bundling)}):")
    for c in non_bundling:
        name_lower = c['name'].lower()
        is_telecom = any(k in name_lower for k in keep_names_lower)
        if is_telecom:
            print(f"    KEEP: {c['name']:<45} rank={c['rank']}")
        else:
            print(f"    REMOVE: {c['name']:<45} rank={c['rank']}")
            session.run("""
                MATCH (c:Company {company_id: $cid})-[r:OPERATES_AS]->(bm:BusinessModel {bim_id: 'BIM_016'})
                DELETE r
            """, cid=c['cid'])
            print(f"      - Removed OPERATES_AS BIM_016")

    # Also check all companies in BIM_016 by industry group
    all_bim016 = run_query(session, """
        MATCH (c:Company)-[:OPERATES_AS]->(bm:BusinessModel {bim_id: 'BIM_016'})
        RETURN c.name as name, c.company_id as cid, c.gics_industry_group as ig, c.fortune_rank as rank
        ORDER BY c.gics_industry_group, c.name
    """)
    print(f"\n  BIM_016 after cleanup ({len(all_bim016)} companies):")
    for c in all_bim016:
        print(f"    {c['name']:<45} ig={c['ig']:<35} rank={c['rank']}")


# ============================================================
# PROBLEM 6: Fix BIM_015 (Professional Services)
# ============================================================

def fix_problem_6(session):
    print("\n" + "=" * 80)
    print("PROBLEM 6: Fix BIM_015 (Professional Services) - Remove non-consulting")
    print("=" * 80)

    # Names to REMOVE from BIM_015
    remove_names_lower = [
        'first financial bankshares',
        'enhabit',
        'healthstream',
        'healthcare services group',
        'matthews international',
        'superior group',
        'crawford & company',
        'arc document solutions',
    ]

    all_bim015 = run_query(session, """
        MATCH (c:Company)-[:OPERATES_AS]->(bm:BusinessModel {bim_id: 'BIM_015'})
        RETURN c.name as name, c.company_id as cid, c.gics_industry_group as ig, c.fortune_rank as rank
        ORDER BY c.name
    """)

    print(f"\n  Current BIM_015 companies ({len(all_bim015)}):")
    for c in all_bim015:
        name_lower = c['name'].lower()
        should_remove = any(rn in name_lower for rn in remove_names_lower)
        marker = "REMOVE" if should_remove else "KEEP"
        print(f"    {marker:<8} {c['name']:<45} ig={c['ig']:<30} rank={c['rank']}")

        if should_remove:
            session.run("""
                MATCH (c:Company {company_id: $cid})-[r:OPERATES_AS]->(bm:BusinessModel {bim_id: 'BIM_015'})
                DELETE r
            """, cid=c['cid'])
            print(f"             - Removed OPERATES_AS BIM_015")

    # Show final state
    final = run_query(session, """
        MATCH (c:Company)-[:OPERATES_AS]->(bm:BusinessModel {bim_id: 'BIM_015'})
        RETURN c.name as name, c.gics_industry_group as ig
        ORDER BY c.name
    """)
    print(f"\n  BIM_015 after cleanup ({len(final)} companies):")
    for c in final:
        print(f"    {c['name']:<45} ig={c['ig']}")


# ============================================================
# STEP 9: Delete ALL EXPOSED_TO and rebuild from scratch
# ============================================================

def rebuild_exposed_to(session):
    print("\n" + "=" * 80)
    print("STEP 9: DELETE ALL EXPOSED_TO AND REBUILD FROM SCRATCH")
    print("=" * 80)

    # Count before
    before = run_single(session, "MATCH ()-[r:EXPOSED_TO]->() RETURN count(r) as cnt")
    print(f"\n  EXPOSED_TO relationships before: {before['cnt']}")

    # Delete all
    session.run("MATCH ()-[r:EXPOSED_TO]->() DELETE r")
    after_delete = run_single(session, "MATCH ()-[r:EXPOSED_TO]->() RETURN count(r) as cnt")
    print(f"  EXPOSED_TO relationships after delete: {after_delete['cnt']}")

    # Rebuild: company exposed to hypothesis if it OPERATES_AS the hypothesis's from-BM (TARGETS)
    result = run_single(session, """
        MATCH (h:DisruptionHypothesis)-[:TARGETS]->(fb:BusinessModel)
        MATCH (c:Company)-[:OPERATES_AS]->(fb)
        MERGE (c)-[:EXPOSED_TO]->(h)
        RETURN count(*) as created
    """)
    print(f"  EXPOSED_TO relationships rebuilt: {result['created']}")

    # Show per-hypothesis counts
    counts = run_query(session, """
        MATCH (c:Company)-[:EXPOSED_TO]->(h:DisruptionHypothesis)
        RETURN h.hypothesis_id as hid, count(c) as cnt
        ORDER BY hid
    """)
    print(f"\n  Per-hypothesis counts after rebuild:")
    for c in counts:
        print(f"    {c['hid']:<45} {c['cnt']:>5} companies")


# ============================================================
# PROBLEM 2: Filter BIM_009 EXPOSED_TO by industry group
# ============================================================

def fix_problem_2(session):
    print("\n" + "=" * 80)
    print("PROBLEM 2: Filter BIM_009 EXPOSED_TO - Remove commodity/utility/food/retail")
    print("=" * 80)

    remove_industry_groups = [
        'Oil, Gas & Consumable Fuels',
        'Multi-Utilities',
        'Electric Utilities',
        'Chemicals',
        'Metals & Mining',
        'Containers & Packaging',
        'Paper & Forest Products',
        'Construction Materials',
        'Food, Beverage & Tobacco',
        'Household & Personal Products',
        'Consumer Services',
        'Retailing',
    ]

    bim009_hypotheses = [
        'HYP_TECH_001_BIM_009_BIM_004',
        'HYP_TECH_001_BIM_009_BIM_006',
        'HYP_TECH_003_BIM_009_BIM_012',
    ]

    for hid in bim009_hypotheses:
        # Show before
        before = run_single(session, """
            MATCH (c:Company)-[:EXPOSED_TO]->(h:DisruptionHypothesis {hypothesis_id: $hid})
            RETURN count(c) as cnt
        """, hid=hid)
        print(f"\n  [{hid}]")
        print(f"    Before: {before['cnt']} companies")

        # Show what we're removing
        to_remove = run_query(session, """
            MATCH (c:Company)-[:EXPOSED_TO]->(h:DisruptionHypothesis {hypothesis_id: $hid})
            WHERE c.gics_industry_group IN $remove_igs
            RETURN c.name as name, c.gics_industry_group as ig, c.fortune_rank as rank
            ORDER BY c.gics_industry_group, c.name
        """, hid=hid, remove_igs=remove_industry_groups)

        print(f"    Removing {len(to_remove)} companies:")
        for c in to_remove:
            print(f"      {c['name']:<45} ig={c['ig']:<35} rank={c['rank']}")

        # Delete
        deleted = run_single(session, """
            MATCH (c:Company)-[r:EXPOSED_TO]->(h:DisruptionHypothesis {hypothesis_id: $hid})
            WHERE c.gics_industry_group IN $remove_igs
            DELETE r
            RETURN count(r) as deleted
        """, hid=hid, remove_igs=remove_industry_groups)
        print(f"    Deleted: {deleted['deleted']} relationships")

        after = run_single(session, """
            MATCH (c:Company)-[:EXPOSED_TO]->(h:DisruptionHypothesis {hypothesis_id: $hid})
            RETURN count(c) as cnt
        """, hid=hid)
        print(f"    After: {after['cnt']} companies")


# ============================================================
# PROBLEM 7: Filter BIM_037 research hypotheses EXPOSED_TO
# ============================================================

def fix_problem_7(session):
    print("\n" + "=" * 80)
    print("PROBLEM 7: Filter BIM_037 research hypothesis EXPOSED_TO")
    print("=" * 80)

    remove_industry_groups = [
        'Hotels, Restaurants & Leisure',
        'Airlines',
        'Trading Companies & Distributors',
        'Air Freight & Logistics',
        'Health Care Equipment & Services',
        'Transportation Infrastructure',
        'Machinery',
        'Automobiles & Components',
        'Food & Staples Retailing',
    ]

    research_hypotheses = [
        'HYP_TECH_002_BIM_006_BIM_004',
        'HYP_TECH_003_BIM_006_BIM_010',
    ]

    for hid in research_hypotheses:
        before = run_single(session, """
            MATCH (c:Company)-[:EXPOSED_TO]->(h:DisruptionHypothesis {hypothesis_id: $hid})
            RETURN count(c) as cnt
        """, hid=hid)
        print(f"\n  [{hid}]")
        print(f"    Before: {before['cnt']} companies")

        # Show what we're removing
        to_remove = run_query(session, """
            MATCH (c:Company)-[:EXPOSED_TO]->(h:DisruptionHypothesis {hypothesis_id: $hid})
            WHERE c.gics_industry_group IN $remove_igs
            RETURN c.name as name, c.gics_industry_group as ig, c.fortune_rank as rank
            ORDER BY c.gics_industry_group, c.name
        """, hid=hid, remove_igs=remove_industry_groups)

        print(f"    Removing {len(to_remove)} companies:")
        for c in to_remove:
            print(f"      {c['name']:<45} ig={c['ig']:<35} rank={c['rank']}")

        # Delete
        deleted = run_single(session, """
            MATCH (c:Company)-[r:EXPOSED_TO]->(h:DisruptionHypothesis {hypothesis_id: $hid})
            WHERE c.gics_industry_group IN $remove_igs
            DELETE r
            RETURN count(r) as deleted
        """, hid=hid, remove_igs=remove_industry_groups)
        print(f"    Deleted: {deleted['deleted']} relationships")

        after = run_single(session, """
            MATCH (c:Company)-[:EXPOSED_TO]->(h:DisruptionHypothesis {hypothesis_id: $hid})
            RETURN count(c) as cnt
        """, hid=hid)
        print(f"    After: {after['cnt']} companies")


# ============================================================
# FINAL SUMMARY TABLE
# ============================================================

def print_final_summary(session):
    print("\n" + "=" * 80)
    print("FINAL SUMMARY TABLE")
    print("=" * 80)

    hypotheses = run_query(session, """
        MATCH (h:DisruptionHypothesis)-[:TARGETS]->(fb:BusinessModel),
              (h)-[:PROPOSES]->(tb:BusinessModel)
        OPTIONAL MATCH (c:Company)-[:EXPOSED_TO]->(h)
        RETURN h.hypothesis_id as hid,
               h.title as title,
               fb.bim_id as from_bm_id,
               fb.name as from_bm,
               tb.bim_id as to_bm_id,
               tb.name as to_bm,
               count(DISTINCT c) as company_count
        ORDER BY hid
    """)

    print(f"\n{'HYPOTHESIS':<45} {'FROM_BM':<35} {'TO_BM':<35} {'COMPANIES':>10}")
    print("-" * 130)
    for h in hypotheses:
        from_label = f"{h['from_bm_id']} ({h['from_bm'][:25]})"
        to_label = f"{h['to_bm_id']} ({h['to_bm'][:25]})"
        print(f"{h['hid']:<45} {from_label:<35} {to_label:<35} {h['company_count']:>10}")

    print("\n--- Top 5 companies per hypothesis ---\n")
    for h in hypotheses:
        hid = h['hid']
        top = run_query(session, """
            MATCH (c:Company)-[:EXPOSED_TO]->(h:DisruptionHypothesis {hypothesis_id: $hid})
            RETURN c.name as name, c.fortune_rank as rank, c.gics_sector as sector,
                   c.gics_industry_group as ig
            ORDER BY CASE WHEN c.fortune_rank IS NOT NULL THEN c.fortune_rank ELSE 99999 END ASC
            LIMIT 5
        """, hid=hid)

        print(f"[{hid}] ({h['company_count']} companies)")
        print(f"  {h['from_bm']} -> {h['to_bm']}")
        if top:
            for c in top:
                print(f"    #{str(c['rank'] or 'N/A'):>5}  {c['name']:<45} [{c['sector']}] ig={c['ig']}")
        else:
            print(f"    (No companies)")
        print()

    # Also show BM membership counts
    print("\n--- Business Model membership counts ---\n")
    bm_counts = run_query(session, """
        MATCH (c:Company)-[:OPERATES_AS]->(bm:BusinessModel)
        RETURN bm.bim_id as bim_id, bm.name as name, count(c) as cnt
        ORDER BY bm.bim_id
    """)
    for bm in bm_counts:
        print(f"  {bm['bim_id']:<10} {bm['name']:<45} {bm['cnt']:>5} companies")


# ============================================================
# MAIN
# ============================================================

def main():
    print("=" * 80)
    print("DEEP FIX: Company-Hypothesis Relevance - Neo4j Scouting Database")
    print("=" * 80)

    with driver.session() as session:
        # Show initial state
        print("\n--- INITIAL STATE ---")
        initial = run_query(session, """
            MATCH (h:DisruptionHypothesis)-[:TARGETS]->(fb:BusinessModel)
            OPTIONAL MATCH (c:Company)-[:EXPOSED_TO]->(h)
            RETURN h.hypothesis_id as hid, fb.bim_id as from_bm, count(DISTINCT c) as cnt
            ORDER BY hid
        """)
        for h in initial:
            print(f"  {h['hid']:<45} from={h['from_bm']:<10} companies={h['cnt']}")

        # Execute all fixes in order
        fix_problem_1(session)   # Create BIM_041, move hypothesis, populate
        fix_problem_3(session)   # Move gaming from BIM_005 to BIM_006
        fix_problem_4(session)   # Fix BIM_006 Diversified
        fix_problem_5(session)   # Fix BIM_016 non-bundling
        fix_problem_6(session)   # Fix BIM_015 non-consulting

        # Step 9: Delete ALL EXPOSED_TO and rebuild from corrected OPERATES_AS
        rebuild_exposed_to(session)

        # Re-apply industry group filters (since rebuild adds all companies in the BM)
        fix_problem_2(session)   # Filter BIM_009 hypotheses
        fix_problem_7(session)   # Filter BIM_037 research hypotheses

        # Final summary
        print_final_summary(session)

    driver.close()
    print("\nDone. All fixes applied.")


if __name__ == '__main__':
    main()
