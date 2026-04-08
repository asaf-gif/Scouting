#!/usr/bin/env python3
"""
Comprehensive script to fix company-hypothesis relevance in Neo4j database.
Steps:
1. Audit current state
2. Remove duplicate company nodes
3. Create new Business Model nodes (BIM_038, BIM_039, BIM_040)
4. Reclassify companies from BIM_015
5. Fix wrong BIM assignments for major tech/internet companies
6. Add companies to underpopulated BMs
7. Rebuild EXPOSED_TO relationships
8. Report
"""

from neo4j import GraphDatabase

# Parse credentials from .env file
env = {}
with open('/Users/asafg/Claude code/scouting/.env') as f:
    for line in f:
        line = line.strip()
        if line and not line.startswith('#') and '=' in line:
            k, v = line.split('=', 1)
            env[k.strip()] = v.strip()

NEO4J_URI = env['NEO4J_URI']
NEO4J_USER = env['NEO4J_USER']
NEO4J_PASSWORD = env['NEO4J_PASSWORD']

print(f"Connecting to Neo4j at {NEO4J_URI} as {NEO4J_USER}")
driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

def run_query(session, cypher, params=None, description=""):
    try:
        result = session.run(cypher, params or {})
        records = list(result)
        return records
    except Exception as e:
        print(f"  ERROR in '{description}': {e}")
        return []

# ============================================================
# STEP 1: Audit current state
# ============================================================
print("\n" + "="*60)
print("STEP 1: AUDIT CURRENT STATE")
print("="*60)

with driver.session() as session:
    print("\n--- Business Models and company counts ---")
    records = run_query(session, """
        MATCH (c:Company)-[:OPERATES_AS]->(bm:BusinessModel)
        RETURN bm.bim_id as bim_id, bm.name as bm_name, count(c) as company_count
        ORDER BY bim_id
    """, description="BM company counts")
    for r in records:
        print(f"  {r['bim_id']:10s} | {str(r['bm_name']):50s} | {r['company_count']:4d} companies")

    print("\n--- All hypotheses with TARGETS/PROPOSES BMs ---")
    records = run_query(session, """
        MATCH (h:DisruptionHypothesis)-[:TARGETS]->(fb:BusinessModel), (h)-[:PROPOSES]->(tb:BusinessModel)
        RETURN h.hypothesis_id as hid, h.title as title, fb.bim_id as from_bm, fb.name as from_bm_name,
               tb.bim_id as to_bm, tb.name as to_bm_name
        ORDER BY hid
    """, description="Hypotheses")
    for r in records:
        print(f"  {r['hid']:10s} | FROM {r['from_bm']}:{r['from_bm_name']:30s} -> TO {r['to_bm']}:{r['to_bm_name']}")
        print(f"             Title: {r['title']}")

    print("\n--- Count of EXPOSED_TO relationships ---")
    records = run_query(session, "MATCH ()-[r:EXPOSED_TO]->() RETURN count(r) as cnt", description="EXPOSED_TO count")
    exposed_before = records[0]['cnt'] if records else 0
    print(f"  Total EXPOSED_TO relationships: {exposed_before}")

# ============================================================
# STEP 2: Remove duplicate company nodes
# ============================================================
print("\n" + "="*60)
print("STEP 2: REMOVE DUPLICATE COMPANY NODES")
print("="*60)

duplicates_removed = 0

with driver.session() as session:
    print("\n--- Finding duplicate companies ---")
    records = run_query(session, """
        MATCH (c:Company)
        WITH toLower(trim(c.name)) as name, collect(c) as nodes, count(*) as cnt
        WHERE cnt > 1
        RETURN name, [n IN nodes | {id: n.company_id, rank: n.fortune_rank}] as info
        ORDER BY name
    """, description="Find duplicates")

    print(f"  Found {len(records)} duplicate groups")
    for r in records:
        print(f"  '{r['name']}': {r['info']}")

    # Process each duplicate group
    for r in records:
        name = r['name']
        info = r['info']

        # Sort by fortune_rank ascending (lower rank = better/more prominent), None goes last
        def sort_key(x):
            rank = x.get('rank')
            if rank is None:
                return 99999
            try:
                return int(rank)
            except (ValueError, TypeError):
                return 99999

        sorted_info = sorted(info, key=sort_key)
        keep_id = sorted_info[0]['id']
        del_ids = [x['id'] for x in sorted_info[1:]]

        print(f"\n  Processing '{name}': KEEP {keep_id} (rank {sorted_info[0].get('rank')}), DELETE {del_ids}")

        for del_id in del_ids:
            try:
                # Transfer OPERATES_AS relationships
                result = session.run("""
                    MATCH (keep:Company {company_id: $keep_id}), (del:Company {company_id: $del_id})
                    OPTIONAL MATCH (del)-[:OPERATES_AS]->(bm:BusinessModel)
                    WHERE NOT (keep)-[:OPERATES_AS]->(bm)
                    WITH keep, del, collect(bm) as bms
                    FOREACH (bm IN bms | MERGE (keep)-[:OPERATES_AS]->(bm))
                    RETURN count(bms) as transferred_bms
                """, keep_id=keep_id, del_id=del_id)
                result_list = list(result)
                transferred_bms = result_list[0]['transferred_bms'] if result_list else 0

                # Transfer EXPOSED_TO relationships
                result2 = session.run("""
                    MATCH (keep:Company {company_id: $keep_id}), (del:Company {company_id: $del_id})
                    OPTIONAL MATCH (del)-[:EXPOSED_TO]->(h:DisruptionHypothesis)
                    WHERE NOT (keep)-[:EXPOSED_TO]->(h)
                    WITH keep, del, collect(h) as hyps
                    FOREACH (h IN hyps | MERGE (keep)-[:EXPOSED_TO]->(h))
                    RETURN count(hyps) as transferred_hyps
                """, keep_id=keep_id, del_id=del_id)
                result2_list = list(result2)
                transferred_hyps = result2_list[0]['transferred_hyps'] if result2_list else 0

                # Delete the duplicate
                del_result = session.run("""
                    MATCH (del:Company {company_id: $del_id})
                    DETACH DELETE del
                    RETURN count(del) as deleted
                """, del_id=del_id)
                list(del_result)  # consume

                print(f"    Deleted {del_id} (transferred {transferred_bms} BM rels, {transferred_hyps} hypothesis rels)")
                duplicates_removed += 1
            except Exception as e:
                print(f"    ERROR deleting {del_id}: {e}")

print(f"\n  Total duplicates removed: {duplicates_removed}")

# ============================================================
# STEP 3: Create new Business Model nodes
# ============================================================
print("\n" + "="*60)
print("STEP 3: CREATE NEW BUSINESS MODEL NODES")
print("="*60)

new_bms = [
    {
        'bim_id': 'BIM_038',
        'name': 'Healthcare Services',
        'description': 'Companies that deliver clinical care, hospital operations, or long-term care services. Revenue comes from patient volume and payer reimbursements (insurance, Medicare, Medicaid). Distinct from Professional Services in that the core product is physical care delivery, not advisory.'
    },
    {
        'bim_id': 'BIM_039',
        'name': 'IT Services & Outsourcing',
        'description': 'Companies that provide technology infrastructure, managed services, or business-process outsourcing. Revenue from long-term service contracts and government/enterprise IT programs. Distinct from SaaS in that revenue comes from labor/project delivery rather than software licenses.'
    },
    {
        'bim_id': 'BIM_040',
        'name': 'Facilities & Workforce Services',
        'description': 'Companies providing outsourced human-capital management, facility management, uniform rental, security, or payroll processing. Revenue from service contracts. Distinct from Professional Services in that the core output is operational labor delivery, not advisory expertise.'
    }
]

with driver.session() as session:
    for bm in new_bms:
        try:
            result = session.run("""
                MERGE (bm:BusinessModel {bim_id: $bim_id})
                SET bm.name = $name, bm.description = $description
                RETURN bm.bim_id as bim_id, bm.name as name
            """, bim_id=bm['bim_id'], name=bm['name'], description=bm['description'])
            records = list(result)
            print(f"  Created/updated: {records[0]['bim_id']} - {records[0]['name']}")
        except Exception as e:
            print(f"  ERROR creating {bm['bim_id']}: {e}")

# ============================================================
# STEP 4: Reclassify companies from BIM_015
# ============================================================
print("\n" + "="*60)
print("STEP 4: RECLASSIFY COMPANIES FROM BIM_015")
print("="*60)

with driver.session() as session:
    print("\n--- Current BIM_015 companies ---")
    records = run_query(session, """
        MATCH (c:Company)-[:OPERATES_AS]->(bm:BusinessModel {bim_id: 'BIM_015'})
        RETURN c.name as name, c.company_id as cid
        ORDER BY c.name
    """, description="BIM_015 companies")
    print(f"  Found {len(records)} companies in BIM_015:")
    for r in records:
        print(f"    {r['cid']:12s} | {r['name']}")

# Companies to move to BIM_038 (Healthcare Services)
healthcare_patterns = [
    'hca healthcare', 'tenet healthcare', 'community health systems',
    'universal health services', 'select medical', 'iasis healthcare',
    'kindred healthcare', 'brookdale senior living', 'healthsouth',
    'encompass health', 'davita'
]

# Companies to move to BIM_039 (IT Services & Outsourcing)
it_services_patterns = [
    'hewlett packard enterprise', 'dxc technology', 'cdw',
    'cognizant technology', 'leidos', 'saic', 'science applications',
    'caci international', 'unisys'
]

# Companies to move to BIM_040 (Facilities & Workforce Services)
facilities_patterns = [
    'aramark', 'automatic data processing', 'adp', 'cintas',
    'abm industries', "brink's", 'brinks', 'paychex', 'insperity',
    'trinet group', 'adt', 'servicemaster', 'convergys',
    'west corporation', 'iron mountain'
]

reclassified = []

with driver.session() as session:
    print("\n--- Moving to BIM_038 (Healthcare Services) ---")
    for pattern in healthcare_patterns:
        try:
            result = session.run("""
                MATCH (c:Company)-[r:OPERATES_AS]->(old_bm:BusinessModel {bim_id: 'BIM_015'})
                WHERE toLower(c.name) CONTAINS $name_pattern
                MATCH (new_bm:BusinessModel {bim_id: 'BIM_038'})
                DELETE r
                MERGE (c)-[:OPERATES_AS]->(new_bm)
                RETURN c.name as moved
            """, name_pattern=pattern)
            moved = [rec['moved'] for rec in result]
            if moved:
                print(f"  Moved to BIM_038: {moved}")
                reclassified.extend([(m, 'BIM_015', 'BIM_038') for m in moved])
        except Exception as e:
            print(f"  ERROR moving '{pattern}' to BIM_038: {e}")

    print("\n--- Moving to BIM_039 (IT Services & Outsourcing) ---")
    for pattern in it_services_patterns:
        try:
            result = session.run("""
                MATCH (c:Company)-[r:OPERATES_AS]->(old_bm:BusinessModel {bim_id: 'BIM_015'})
                WHERE toLower(c.name) CONTAINS $name_pattern
                MATCH (new_bm:BusinessModel {bim_id: 'BIM_039'})
                DELETE r
                MERGE (c)-[:OPERATES_AS]->(new_bm)
                RETURN c.name as moved
            """, name_pattern=pattern)
            moved = [rec['moved'] for rec in result]
            if moved:
                print(f"  Moved to BIM_039: {moved}")
                reclassified.extend([(m, 'BIM_015', 'BIM_039') for m in moved])
        except Exception as e:
            print(f"  ERROR moving '{pattern}' to BIM_039: {e}")

    print("\n--- Moving to BIM_040 (Facilities & Workforce Services) ---")
    for pattern in facilities_patterns:
        try:
            result = session.run("""
                MATCH (c:Company)-[r:OPERATES_AS]->(old_bm:BusinessModel {bim_id: 'BIM_015'})
                WHERE toLower(c.name) CONTAINS $name_pattern
                MATCH (new_bm:BusinessModel {bim_id: 'BIM_040'})
                DELETE r
                MERGE (c)-[:OPERATES_AS]->(new_bm)
                RETURN c.name as moved
            """, name_pattern=pattern)
            moved = [rec['moved'] for rec in result]
            if moved:
                print(f"  Moved to BIM_040: {moved}")
                reclassified.extend([(m, 'BIM_015', 'BIM_040') for m in moved])
        except Exception as e:
            print(f"  ERROR moving '{pattern}' to BIM_040: {e}")

    print("\n--- BIM_015 remaining companies ---")
    records = run_query(session, """
        MATCH (c:Company)-[:OPERATES_AS]->(bm:BusinessModel {bim_id: 'BIM_015'})
        RETURN c.name as name
        ORDER BY c.name
    """, description="Remaining BIM_015")
    print(f"  {len(records)} companies remain in BIM_015:")
    for r in records:
        print(f"    {r['name']}")

print(f"\n  Total reclassified: {len(reclassified)}")

# ============================================================
# STEP 5: Fix wrong BIM assignments for major tech/internet companies
# ============================================================
print("\n" + "="*60)
print("STEP 5: FIX WRONG BIM ASSIGNMENTS FOR TECH/INTERNET COMPANIES")
print("="*60)

fixes = [
    ('alphabet', 'BIM_002'),  # Advertising-Based
    ('google', 'BIM_002'),    # Advertising-Based
    ('meta platforms', 'BIM_002'),  # Advertising-Based
    ('facebook', 'BIM_002'),  # Advertising-Based (legacy name)
    ('netflix', 'BIM_001'),   # Subscription
    ('booking holdings', 'BIM_004'),  # Marketplace
    ('expedia', 'BIM_004'),   # Marketplace
]

wrong_assignments_fixed = []

with driver.session() as session:
    print("\n--- Checking current assignments for major tech companies ---")
    records = run_query(session, """
        MATCH (c:Company)-[:OPERATES_AS]->(bm:BusinessModel)
        WHERE toLower(c.name) CONTAINS 'alphabet'
           OR toLower(c.name) CONTAINS 'meta platforms'
           OR toLower(c.name) = 'netflix'
           OR toLower(c.name) CONTAINS 'booking holdings'
           OR toLower(c.name) CONTAINS 'expedia'
           OR toLower(c.name) CONTAINS 'facebook'
        RETURN c.name as name, c.company_id as cid, bm.bim_id as bim_id, bm.name as bm_name
        ORDER BY c.name
    """, description="Tech company current BMs")
    for r in records:
        print(f"  {r['cid']:12s} | {r['name']:35s} | {r['bim_id']:10s} | {r['bm_name']}")

    print("\n--- Fixing assignments ---")
    for name_pattern, target_bim in fixes:
        try:
            # Check if BM exists
            bm_check = session.run("MATCH (bm:BusinessModel {bim_id: $bim_id}) RETURN bm.name as name", bim_id=target_bim)
            bm_list = list(bm_check)
            if not bm_list:
                print(f"  SKIP: {target_bim} does not exist in DB")
                continue

            # First check what the company currently has
            check = session.run("""
                MATCH (c:Company)-[:OPERATES_AS]->(bm:BusinessModel)
                WHERE toLower(c.name) CONTAINS $name_pattern
                RETURN c.name as name, c.company_id as cid, bm.bim_id as bim_id
            """, name_pattern=name_pattern)
            check_list = list(check)

            for rec in check_list:
                current_bim = rec['bim_id']
                if current_bim == target_bim:
                    print(f"  OK (already correct): {rec['name']} -> {target_bim}")
                    continue

                # Remove wrong assignment and add correct one
                fix_result = session.run("""
                    MATCH (c:Company {company_id: $cid})-[r:OPERATES_AS]->(old_bm:BusinessModel {bim_id: $old_bim})
                    MATCH (new_bm:BusinessModel {bim_id: $new_bim})
                    DELETE r
                    MERGE (c)-[:OPERATES_AS]->(new_bm)
                    RETURN c.name as name
                """, cid=rec['cid'], old_bim=current_bim, new_bim=target_bim)
                fix_list = list(fix_result)
                if fix_list:
                    print(f"  Fixed: {fix_list[0]['name']} from {current_bim} -> {target_bim}")
                    wrong_assignments_fixed.append((fix_list[0]['name'], current_bim, target_bim))

        except Exception as e:
            print(f"  ERROR fixing '{name_pattern}': {e}")

print(f"\n  Total wrong assignments fixed: {len(wrong_assignments_fixed)}")

# ============================================================
# STEP 6: Add companies to underpopulated BMs
# ============================================================
print("\n" + "="*60)
print("STEP 6: ADD COMPANIES TO UNDERPOPULATED BMs")
print("="*60)

with driver.session() as session:
    print("\n--- Current BIM_004 (Marketplace) companies ---")
    records = run_query(session, """
        MATCH (c:Company)-[:OPERATES_AS]->(bm:BusinessModel {bim_id: 'BIM_004'})
        RETURN c.name as name ORDER BY c.name
    """, description="BIM_004 companies")
    print(f"  BIM_004 has {len(records)} companies:")
    for r in records:
        print(f"    {r['name']}")

    print("\n--- Adding companies to BIM_004 (Marketplace) ---")
    marketplace_patterns = ['ebay', 'booking holdings', 'expedia', 'costar group', 'realtor.com']
    for pattern in marketplace_patterns:
        try:
            result = session.run("""
                MATCH (c:Company)
                WHERE toLower(c.name) CONTAINS $name_pattern
                MATCH (bm:BusinessModel {bim_id: 'BIM_004'})
                MERGE (c)-[:OPERATES_AS]->(bm)
                RETURN c.name as name
            """, name_pattern=pattern)
            added = [rec['name'] for rec in result]
            if added:
                print(f"  Added/confirmed in BIM_004: {added}")
        except Exception as e:
            print(f"  ERROR adding '{pattern}' to BIM_004: {e}")

    print("\n--- Current BIM_012 (Licensing) companies ---")
    records = run_query(session, """
        MATCH (c:Company)-[:OPERATES_AS]->(bm:BusinessModel {bim_id: 'BIM_012'})
        RETURN c.name as name ORDER BY c.name
    """, description="BIM_012 companies")
    print(f"  BIM_012 has {len(records)} companies:")
    for r in records:
        print(f"    {r['name']}")

    print("\n--- Adding companies to BIM_012 (Licensing) ---")
    licensing_patterns = ['qualcomm']
    for pattern in licensing_patterns:
        try:
            result = session.run("""
                MATCH (c:Company)
                WHERE toLower(c.name) CONTAINS $name_pattern
                MATCH (bm:BusinessModel {bim_id: 'BIM_012'})
                MERGE (c)-[:OPERATES_AS]->(bm)
                RETURN c.name as name
            """, name_pattern=pattern)
            added = [rec['name'] for rec in result]
            if added:
                print(f"  Added/confirmed in BIM_012: {added}")
        except Exception as e:
            print(f"  ERROR adding '{pattern}' to BIM_012: {e}")

    print("\n--- Current BIM_016 (Bundling) companies ---")
    records = run_query(session, """
        MATCH (c:Company)-[:OPERATES_AS]->(bm:BusinessModel {bim_id: 'BIM_016'})
        RETURN c.name as name ORDER BY c.name
    """, description="BIM_016 companies")
    print(f"  BIM_016 has {len(records)} companies:")
    for r in records:
        print(f"    {r['name']}")

    print("\n--- Adding companies to BIM_016 (Bundling) ---")
    bundling_patterns = ['comcast', 'charter communications', 'at&t', 'verizon']
    for pattern in bundling_patterns:
        try:
            result = session.run("""
                MATCH (c:Company)
                WHERE toLower(c.name) CONTAINS $name_pattern
                MATCH (bm:BusinessModel {bim_id: 'BIM_016'})
                MERGE (c)-[:OPERATES_AS]->(bm)
                RETURN c.name as name
            """, name_pattern=pattern)
            added = [rec['name'] for rec in result]
            if added:
                print(f"  Added/confirmed in BIM_016: {added}")
        except Exception as e:
            print(f"  ERROR adding '{pattern}' to BIM_016: {e}")

# ============================================================
# STEP 7: Rebuild EXPOSED_TO relationships
# ============================================================
print("\n" + "="*60)
print("STEP 7: REBUILD EXPOSED_TO RELATIONSHIPS")
print("="*60)

with driver.session() as session:
    print("\n--- Deleting all existing EXPOSED_TO relationships ---")
    try:
        result = session.run("MATCH ()-[r:EXPOSED_TO]->() DELETE r RETURN count(r) as deleted")
        # Note: count(r) after DELETE returns 0 in Neo4j, so we just confirm it ran
        list(result)
        print("  Deleted all EXPOSED_TO relationships")
    except Exception as e:
        print(f"  ERROR deleting EXPOSED_TO: {e}")

    print("\n--- Rebuilding EXPOSED_TO based on BusinessModel targeting ---")
    try:
        result = session.run("""
            MATCH (h:DisruptionHypothesis)-[:TARGETS]->(fb:BusinessModel)
            MATCH (c:Company)-[:OPERATES_AS]->(fb)
            MERGE (c)-[:EXPOSED_TO]->(h)
            RETURN count(*) as created
        """)
        records = list(result)
        exposed_after = records[0]['created'] if records else 0
        print(f"  Created {exposed_after} EXPOSED_TO relationships")
    except Exception as e:
        print(f"  ERROR rebuilding EXPOSED_TO: {e}")
        exposed_after = 0

    # Verify actual count
    print("\n--- Verifying EXPOSED_TO count ---")
    records = run_query(session, "MATCH ()-[r:EXPOSED_TO]->() RETURN count(r) as cnt", description="EXPOSED_TO verify")
    actual_after = records[0]['cnt'] if records else 0
    print(f"  Actual EXPOSED_TO relationships in DB: {actual_after}")

# ============================================================
# STEP 8: Report
# ============================================================
print("\n" + "="*60)
print("STEP 8: COMPREHENSIVE REPORT")
print("="*60)

with driver.session() as session:
    print("\n=== SUMMARY ===")
    print(f"\n1. Duplicates removed: {duplicates_removed}")

    print("\n2. New Business Models created:")
    for bm in new_bms:
        records = run_query(session, """
            MATCH (c:Company)-[:OPERATES_AS]->(bm:BusinessModel {bim_id: $bim_id})
            RETURN count(c) as cnt
        """, params={'bim_id': bm['bim_id']}, description=f"Count {bm['bim_id']}")
        cnt = records[0]['cnt'] if records else 0
        print(f"   {bm['bim_id']}: {bm['name']} - {cnt} companies")

    print("\n3. Companies reclassified from BIM_015:")
    if reclassified:
        from_counts = {}
        for name, old_bm, new_bm in reclassified:
            from_counts[new_bm] = from_counts.get(new_bm, [])
            from_counts[new_bm].append(name)
        for target_bm, names in sorted(from_counts.items()):
            print(f"   To {target_bm}: {names}")
    else:
        print("   None reclassified (companies may not have been in BIM_015)")

    print("\n4. Wrong assignments fixed:")
    if wrong_assignments_fixed:
        for name, old_bm, new_bm in wrong_assignments_fixed:
            print(f"   {name}: {old_bm} -> {new_bm}")
    else:
        print("   None fixed (assignments may have already been correct)")

    print(f"\n5. EXPOSED_TO relationships:")
    print(f"   Before rebuild: {exposed_before}")
    records = run_query(session, "MATCH ()-[r:EXPOSED_TO]->() RETURN count(r) as cnt", description="Final EXPOSED_TO count")
    final_after = records[0]['cnt'] if records else 0
    print(f"   After rebuild: {final_after}")

    print("\n6. Companies per hypothesis after rebuild:")
    records = run_query(session, """
        MATCH (c:Company)-[:EXPOSED_TO]->(h:DisruptionHypothesis)
        RETURN h.hypothesis_id as hid, h.title as title, count(c) as company_count
        ORDER BY hid
    """, description="Companies per hypothesis")
    for r in records:
        print(f"   {r['hid']:10s} | {r['company_count']:4d} companies | {r['title']}")

    print("\n7. Final BM distribution:")
    records = run_query(session, """
        MATCH (c:Company)-[:OPERATES_AS]->(bm:BusinessModel)
        RETURN bm.bim_id as bim_id, bm.name as bm_name, count(c) as company_count
        ORDER BY bim_id
    """, description="Final BM counts")
    for r in records:
        print(f"   {r['bim_id']:10s} | {str(r['bm_name']):50s} | {r['company_count']:4d} companies")

print("\n" + "="*60)
print("SCRIPT COMPLETE")
print("="*60)

driver.close()
