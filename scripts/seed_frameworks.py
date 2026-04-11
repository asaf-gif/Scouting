"""
scripts/seed_frameworks.py

Seeds InvestmentFramework nodes, FrameworkConcept nodes, GROUNDED_IN relationships
on existing DisruptionHypothesis nodes, and HypothesisGap nodes into Neo4j.

Usage:
    python scripts/seed_frameworks.py
"""

import os
import sys

# Parse .env manually
env = {}
with open('/Users/asafg/Claude code/scouting/.env') as f:
    for line in f:
        line = line.strip()
        if line and not line.startswith('#') and '=' in line:
            k, v = line.split('=', 1)
            env[k.strip()] = v.strip()

os.environ.update(env)

from neo4j import GraphDatabase

# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------

def get_driver():
    return GraphDatabase.driver(
        os.environ["NEO4J_URI"],
        auth=(os.environ["NEO4J_USER"], os.environ["NEO4J_PASSWORD"]),
    )


# ---------------------------------------------------------------------------
# Framework definitions
# ---------------------------------------------------------------------------

FRAMEWORKS = [
    {
        "framework_id": "FW_001",
        "name": "APIs to the Real World",
        "summary": (
            "As AI agents become the dominant interface, value concentrates at the two ends of "
            "the stack: frontware (user-facing intent capture) and endware (real-world execution). "
            "Middleware without a defensible anchor to either end commoditizes. The strategic "
            "opportunity is in building programmable, reliable interfaces between digital intent "
            "and physical-world outcomes."
        ),
        "full_text": (
            "AI is introducing a new interface paradigm: agents. Instead of traditional software "
            "workflows, users increasingly define goals in natural language, and agents execute "
            "against them. This shifts the primary interface from UI-driven interaction to "
            "outcome-driven orchestration. We can think of this layer as \"frontware\"—the system "
            "that directly interfaces with humans, interprets intent, and is accountable for "
            "delivering results.\n\n"
            "This new interface has implications for the rest of the stack.\n\n"
            "To fulfill goals, agents require access to inputs—data, capabilities, physical "
            "execution, and services. These inputs form a new layer we can call \"endware\": the "
            "components that connect digital intent to real-world outcomes. Endware includes "
            "everything from APIs to databases, to physical infrastructure, to human labor. It is "
            "where execution actually happens.\n\n"
            "Between these layers sits what we traditionally think of as \"middleware\"—the "
            "orchestration, logic, and tooling that connects inputs to outputs. However, in an "
            "agent-driven world, middleware is likely to be structurally compressed. Agents "
            "themselves can dynamically generate and manage workflows, reducing the need for "
            "static middleware products. As a result, middleware without defensible access—either "
            "to unique endware or privileged frontware distribution—will increasingly commoditize.\n\n"
            "The strategic value, therefore, shifts toward control of scarce interfaces: frontware "
            "(owning user relationships and intent capture) and endware (owning execution "
            "capabilities, especially where they touch the physical world).\n\n"
            "A particularly important category of endware is the interface between the digital "
            "and analog worlds. Wherever software needs to act on reality, it must rely on systems "
            "that can translate digital instructions into physical outcomes. Examples include human "
            "labor as an API (e.g., Fiverr) and logistics as an API.\n\n"
            "However, the challenge is not just access—it is operability. The real world is messy, "
            "stochastic, and failure-prone. For agents to reliably use endware, new tooling is "
            "required: decomposing high-level goals into executable tasks, planning and sequencing "
            "actions dynamically, maintaining state and adapting to changing conditions, handling "
            "failures and escalating when needed.\n\n"
            "As agents become the dominant interface, value accrues to those who control either "
            "side of the stack—the user-facing frontware or the execution-layer endware. "
            "Middleware, unless anchored in one of these, risks becoming transient and commoditized."
        ),
        "version": 1,
        "created_at": "2026-04-11",
        "last_updated": "2026-04-11",
    },
    {
        "framework_id": "FW_002",
        "name": "Complex Simulation Engines",
        "summary": (
            "AI transforms prediction from a narrow analytical function into a general-purpose "
            "capability. By combining global knowledge with proprietary data, AI can simulate "
            "complex futures—across markets, organizations, and human behavior. This enables more "
            "accurate risk pricing, better capital allocation, and simulation of counterfactual "
            "outcomes. Value accrues to those who own simulation layers in high-stakes domains."
        ),
        "full_text": (
            "AI's most powerful capability is not just automation—it is prediction. Modern systems "
            "can ingest vast amounts of data and uncover non-linear relationships that were "
            "previously inaccessible, enabling increasingly accurate forecasts of complex systems.\n\n"
            "With transformer-based models, AI can effectively internalize a large portion of the "
            "accessible corpus of human knowledge—across domains, formats, and contexts—and combine "
            "it with proprietary, domain-specific data. This fusion creates a new class of systems: "
            "generalized simulation engines that can reason about the future with both breadth "
            "(global knowledge) and depth (specific context).\n\n"
            "Improved predictive capability manifests economically in two primary ways: (1) Risk "
            "Pricing and Risk Taking — if you can assess risk more accurately than others, you can "
            "systematically take it; this underpins insurance, credit/lending, and financial "
            "markets. (2) Capturing Future Value Creation — if you can predict where value will "
            "emerge, you can position ahead of it through investing, building products, or "
            "allocating resources toward emerging markets or behaviors.\n\n"
            "What is emerging is not just better forecasting, but the ability to simulate "
            "counterfactual futures—to ask not just what will happen, but what would happen if. "
            "This is particularly powerful in domains driven by human and organizational behavior.\n\n"
            "Human Behavior Simulation: A natural direction is the creation of synthetic humans—"
            "models that replicate decision-making patterns at the individual or cohort level. "
            "These systems act as partial simulations, capturing specific slices of behavior with "
            "high accuracy: modeling attention, predicting conversion or churn, optimizing "
            "onboarding flows.\n\n"
            "Organizational Simulation: Organizations are complex, adaptive systems. AI enables "
            "modeling of Potential LTV, company simulators, and counterfactual evaluation—"
            "understanding not just performance, but missed performance and latent opportunity.\n\n"
            "The strategic opportunity lies in owning these simulation layers in high-value "
            "domains. Those who can most accurately model the future will be best positioned to "
            "price risk, allocate capital, and capture the next wave of value creation."
        ),
        "version": 1,
        "created_at": "2026-04-11",
        "last_updated": "2026-04-11",
    },
    {
        "framework_id": "FW_003",
        "name": "Structuring Information into Models",
        "summary": (
            "LLMs unlock access to knowledge but without structure their reasoning is incomplete. "
            "Structure—as graphs, ontologies, or domain models—allows causal reasoning rather than "
            "correlation, gap detection, and reliable decision-making. The opportunity is "
            "identifying high-value domains where structure is latent but not formalized, then "
            "building systems that make it explicit and computable. Graphs combined with GNNs and "
            "knowledge graph generation (KGGen) are the key primitive."
        ),
        "full_text": (
            "Transformers are a breakthrough in how we process information. They can ingest vast "
            "amounts of unstructured data, reason across it in parallel, and generate insights "
            "that were previously inaccessible. However, they have a fundamental limitation: they "
            "do not inherently impose structure.\n\n"
            "Structure is effectively mental models of a domain. They define entities, "
            "relationships, constraints, and flows. With structure you can process information "
            "more efficiently, identify gaps—what is missing or unknown, and reason causally, "
            "not just correlationally. Without structure, you lose a critical capability: you "
            "don't know what you don't know.\n\n"
            "The key shift is not replacing LLMs, but anchoring them to explicit structures. "
            "These structures already exist in the world—in the tacit knowledge of domain experts, "
            "in the operating models of mature organizations, in specialized datasets. The "
            "opportunity is to encode these mental models into formal structures and connect them "
            "to LLMs, transforming them from general-purpose text processors into domain-aware "
            "systems.\n\n"
            "Graphs are one of the most promising abstractions for structuring information. Graphs "
            "allow representation of entities (nodes), relationships (edges), directionality, "
            "dependencies, and flows. When combined with machine learning, Graph Neural Networks "
            "(GNNs) enable learning over structured relationships, and knowledge graph generation "
            "(KGGen) allows LLMs to translate unstructured text into structured representations.\n\n"
            "Emerging applications include: Economic Flow Mapping (structuring markets as graphs "
            "of supply, demand, and transformation), Qualitative Research at Scale (mapping "
            "unstructured qualitative inputs into structured representations), and Legal and "
            "Regulatory Mapping (encoding legal systems into graph-based models to identify "
            "inconsistencies, trace implications, and surface arbitrage opportunities).\n\n"
            "The strategic opportunity lies in identifying high-value domains where structure is "
            "latent but not yet formalized—and building the systems that make it explicit, "
            "computable, and actionable."
        ),
        "version": 1,
        "created_at": "2026-04-11",
        "last_updated": "2026-04-11",
    },
    {
        "framework_id": "FW_004",
        "name": "Understanding Business Processes",
        "summary": (
            "If AI can decompose, model, and manipulate business processes, then companies "
            "themselves become programmable abstractions. This enables automatic software "
            "migration, on-the-fly tool construction, and process-aware optimization. Beyond "
            "workflow automation, AI can create programmable trust layers that verify performance "
            "and compliance without exposing sensitive data—displacing traditional audit and "
            "consulting functions. The end state is self-optimizing organizations."
        ),
        "full_text": (
            "One of the most important—and underexplored—frontiers in AI is the ability to "
            "understand and operate on business processes. Businesses are complex, adaptive "
            "systems: they involve people with different incentives and behaviors, operate across "
            "fragmented tools and legacy systems, and are shaped by real-world uncertainty, "
            "exceptions, and non-linear dynamics. Business processes are rarely clean or fully "
            "specified—they are partially structured, partially emergent, and often only "
            "understood tacitly by the people operating them.\n\n"
            "The key unlock is this: if AI can decompose, model, and manipulate business "
            "processes, then companies themselves become programmable abstractions. This requires: "
            "breaking down workflows into discrete steps and dependencies, understanding intent "
            "behind actions, mapping how information and decisions flow across an organization, "
            "and identifying bottlenecks, redundancies, and failure points.\n\n"
            "In an AI-native world, software becomes fluid and generated on demand: automatic "
            "software migration, on-the-fly tool construction, and process-aware optimization. "
            "The operating system of a company is no longer static—it is continuously rewritten "
            "based on goals and context.\n\n"
            "A deeper opportunity lies in roles that require contextual understanding of a "
            "business spanning multiple systems and datasets. One compelling example is the role "
            "of a third-party auditor. An AI system embedded within a company's processes could "
            "continuously monitor and interpret operational data, generate verifiable attestations "
            "about performance and compliance, and provide external stakeholders with assurance "
            "without exposing underlying sensitive data. This introduces programmable trust layers "
            "that sit on top of business processes.\n\n"
            "As AI gains the ability to understand and intervene in processes, companies shift "
            "from static entities to adaptive systems: processes are continuously measured and "
            "improved, decisions are informed by real-time understanding of operations, and "
            "organizational knowledge becomes explicit and computable. Over time this could lead "
            "to businesses that are partially or fully self-optimizing.\n\n"
            "The strategic opportunity lies in building systems that can sit inside businesses, "
            "learn how they operate, and progressively take over higher-order functions—from "
            "tooling and workflows to governance and trust."
        ),
        "version": 1,
        "created_at": "2026-04-11",
        "last_updated": "2026-04-11",
    },
]


# ---------------------------------------------------------------------------
# Framework concept definitions
# ---------------------------------------------------------------------------

CONCEPTS = [
    # FW_001
    {
        "concept_id": "FC_001_01",
        "name": "Frontware",
        "definition": "User-facing agent layer that captures intent and is accountable for results. Controlled by players like OpenAI, Google, Meta.",
        "framework_id": "FW_001",
    },
    {
        "concept_id": "FC_001_02",
        "name": "Endware",
        "definition": "Execution-layer components that connect digital intent to real-world outcomes, including APIs, physical infrastructure, and human labor.",
        "framework_id": "FW_001",
    },
    {
        "concept_id": "FC_001_03",
        "name": "Middleware Compression",
        "definition": "The structural commoditization of middleware products as AI agents dynamically generate workflows, eliminating the need for static orchestration layers.",
        "framework_id": "FW_001",
    },
    {
        "concept_id": "FC_001_04",
        "name": "Real-World Operability",
        "definition": "The challenge of making endware reliable: decomposing goals into tasks, handling failure, maintaining state in non-deterministic environments.",
        "framework_id": "FW_001",
    },
    # FW_002
    {
        "concept_id": "FC_002_01",
        "name": "Generalized Simulation Engine",
        "definition": "AI systems combining global knowledge with proprietary data to reason about the future with both breadth and depth.",
        "framework_id": "FW_002",
    },
    {
        "concept_id": "FC_002_02",
        "name": "Risk Repricing",
        "definition": "Using more accurate prediction to systematically price and take risk better than incumbents in insurance, credit, and financial markets.",
        "framework_id": "FW_002",
    },
    {
        "concept_id": "FC_002_03",
        "name": "Counterfactual Simulation",
        "definition": "The ability to simulate what would happen under different conditions—enabling evaluation of alternatives, not just forecasting.",
        "framework_id": "FW_002",
    },
    {
        "concept_id": "FC_002_04",
        "name": "Synthetic Humans",
        "definition": "AI models that replicate individual or cohort decision-making patterns, enabling behavioral prediction without real human panels.",
        "framework_id": "FW_002",
    },
    {
        "concept_id": "FC_002_05",
        "name": "Organizational Simulation",
        "definition": "Modeling how decisions (pricing, hiring, product) propagate through a business, surfacing latent opportunity and missed performance.",
        "framework_id": "FW_002",
    },
    # FW_003
    {
        "concept_id": "FC_003_01",
        "name": "Structural Grounding",
        "definition": "Anchoring LLMs to explicit domain models (graphs, ontologies) to enable causal reasoning rather than pure correlation.",
        "framework_id": "FW_003",
    },
    {
        "concept_id": "FC_003_02",
        "name": "Knowledge Graph",
        "definition": "Graph-based representation of domain knowledge: entities as nodes, relationships as edges, enabling gap detection and causal tracing.",
        "framework_id": "FW_003",
    },
    {
        "concept_id": "FC_003_03",
        "name": "Latent Structure",
        "definition": "Structure that exists in expert knowledge or operating models but has not yet been formalized or made computable.",
        "framework_id": "FW_003",
    },
    {
        "concept_id": "FC_003_04",
        "name": "GNN + KGGen Stack",
        "definition": "The combination of Graph Neural Networks and knowledge graph generation tools that translates unstructured text into structured, learnable representations.",
        "framework_id": "FW_003",
    },
    # FW_004
    {
        "concept_id": "FC_004_01",
        "name": "Process Decomposition",
        "definition": "Breaking business workflows into discrete, dependency-mapped steps that AI can understand, sequence, and optimize.",
        "framework_id": "FW_004",
    },
    {
        "concept_id": "FC_004_02",
        "name": "Programmable Company",
        "definition": "A business whose operating model is sufficiently understood and modeled that it can be reconfigured, optimized, or extended by AI in real time.",
        "framework_id": "FW_004",
    },
    {
        "concept_id": "FC_004_03",
        "name": "Programmable Trust Layer",
        "definition": "AI-embedded audit system that generates verifiable attestations about performance and compliance without exposing sensitive underlying data.",
        "framework_id": "FW_004",
    },
    {
        "concept_id": "FC_004_04",
        "name": "Self-Optimizing Organization",
        "definition": "A business where the gap between strategy and execution is continuously minimized by AI operating across the full stack.",
        "framework_id": "FW_004",
    },
]


# ---------------------------------------------------------------------------
# Hypothesis → Framework grounding mappings
# Based on hypothesis_id patterns found in the database
# ---------------------------------------------------------------------------

GROUNDING_RULES = [
    # FW_001 grounds: middleware compression hypotheses
    # BIM_009 → BIM_004 (direct sales = middleware being compressed)
    ("HYP_TECH_001_BIM_009_BIM_004", "FW_001"),
    # BIM_005 → any (SaaS middleware compression)
    ("HYP_TECH_001_BIM_005_BIM_004", "FW_001"),
    ("HYP_TECH_002_BIM_005_BIM_024", "FW_001"),
    ("HYP_TECH_003_BIM_005_BIM_003", "FW_001"),
    # BIM_016 → BIM_004 (bundling = middleware catalog compressed into platform)
    ("HYP_TECH_001_BIM_016_BIM_004", "FW_001"),
    # BIM_029 → BIM_004 (layer player endware becoming marketplace)
    ("HYP_TECH_001_BIM_029_BIM_004", "FW_001"),
    ("HYP_TECH_002_BIM_029_BIM_004", "FW_001"),
    ("HYP_TECH_002_BIM_029_BIM_016", "FW_001"),

    # FW_002 grounds: simulation engine hypotheses
    # HYP_TECH_003_* (Synthetic Audiences = human behavior simulation)
    ("HYP_TECH_003_BIM_005_BIM_003", "FW_002"),
    ("HYP_TECH_003_BIM_006_BIM_010", "FW_002"),
    ("HYP_TECH_003_BIM_006_BIM_034", "FW_002"),
    ("HYP_TECH_003_BIM_009_BIM_012", "FW_002"),
    ("HYP_TECH_003_BIM_012_BIM_001", "FW_002"),
    ("HYP_TECH_003_BIM_012_BIM_005", "FW_002"),
    ("HYP_TECH_003_BIM_015_BIM_010", "FW_002"),
    ("HYP_TECH_003_BIM_015_BIM_016", "FW_002"),
    ("HYP_TECH_003_BIM_016_BIM_001", "FW_002"),
    ("HYP_TECH_003_BIM_026_BIM_005", "FW_002"),
    # HYP_TECH_002_* (KGGen = knowledge simulation)
    ("HYP_TECH_002_BIM_005_BIM_024", "FW_002"),
    ("HYP_TECH_002_BIM_006_BIM_004", "FW_002"),
    ("HYP_TECH_002_BIM_029_BIM_004", "FW_002"),
    ("HYP_TECH_002_BIM_029_BIM_016", "FW_002"),
    # BIM_037 hypotheses (transactional → knowledge platform = prediction value)
    ("HYP_TECH_002_BIM_006_BIM_004", "FW_002"),
    ("HYP_TECH_003_BIM_006_BIM_010", "FW_002"),

    # FW_003 grounds: structuring information hypotheses
    # HYP_TECH_001_* (GNNs = graph-based structure)
    ("HYP_TECH_001_BIM_002_BIM_006", "FW_003"),
    ("HYP_TECH_001_BIM_004_BIM_006", "FW_003"),
    ("HYP_TECH_001_BIM_004_BIM_017", "FW_003"),
    ("HYP_TECH_001_BIM_005_BIM_004", "FW_003"),
    ("HYP_TECH_001_BIM_006_BIM_004", "FW_003"),
    ("HYP_TECH_001_BIM_009_BIM_004", "FW_003"),
    ("HYP_TECH_001_BIM_009_BIM_006", "FW_003"),
    ("HYP_TECH_001_BIM_012_BIM_004", "FW_003"),
    ("HYP_TECH_001_BIM_016_BIM_004", "FW_003"),
    ("HYP_TECH_001_BIM_029_BIM_004", "FW_003"),
    # HYP_TECH_002_* (KGGen = knowledge graph generation)
    ("HYP_TECH_002_BIM_005_BIM_024", "FW_003"),
    ("HYP_TECH_002_BIM_006_BIM_004", "FW_003"),
    ("HYP_TECH_002_BIM_029_BIM_004", "FW_003"),
    ("HYP_TECH_002_BIM_029_BIM_016", "FW_003"),
    # BIM_015 hypotheses (professional services data → structured knowledge)
    ("HYP_TECH_003_BIM_015_BIM_010", "FW_003"),
    ("HYP_TECH_003_BIM_015_BIM_016", "FW_003"),

    # FW_004 grounds: business process intelligence
    # BIM_015 → BIM_010 and BIM_016 (consulting → metered API = process platformization)
    ("HYP_TECH_003_BIM_015_BIM_010", "FW_004"),
    ("HYP_TECH_003_BIM_015_BIM_016", "FW_004"),
    # BIM_041 → BIM_005 (research projects → SaaS = process automation)
    ("HYP_TECH_003_BIM_026_BIM_005", "FW_004"),
]


# ---------------------------------------------------------------------------
# Hypothesis gap definitions
# ---------------------------------------------------------------------------

GAPS = [
    {
        "gap_id": "GAP_001",
        "name": "Simulation engines reprice insurance",
        "description": (
            "AI simulation engines can model risk at the individual level more accurately than "
            "actuarial tables, enabling new entrants to systematically undercut and reprice the "
            "insurance market. This disruption path moves from traditional risk pooling models "
            "toward AI-native, dynamic risk pricing platforms."
        ),
        "implied_by_framework": "FW_002",
        "from_bm_implied": "BIM_020",
        "to_bm_implied": "BIM_044",
        "status": "open",
    },
    {
        "gap_id": "GAP_002",
        "name": "AI agents compress SaaS middleware to endware",
        "description": (
            "As AI agents orchestrate workflows dynamically, static SaaS middleware products "
            "lose their differentiation and compress toward commodity endware. The surviving "
            "value lies in the execution layer (endware) that agents call, not the middleware "
            "that previously connected them."
        ),
        "implied_by_framework": "FW_001",
        "from_bm_implied": "BIM_005",
        "to_bm_implied": "BIM_029",
        "status": "open",
    },
    {
        "gap_id": "GAP_003",
        "name": "Programmable trust displaces audit/advisory consulting",
        "description": (
            "AI systems embedded in business operations can generate continuous, verifiable "
            "attestations about performance and compliance, replacing the periodic, expensive, "
            "and opaque services provided by traditional audit and advisory consulting firms. "
            "This shifts the model from project-based engagements to programmable trust layers."
        ),
        "implied_by_framework": "FW_004",
        "from_bm_implied": "BIM_015",
        "to_bm_implied": "BIM_043",
        "status": "open",
    },
    {
        "gap_id": "GAP_004",
        "name": "KGGen structures legal/regulatory knowledge",
        "description": (
            "Knowledge graph generation (KGGen) can transform the fragmented, unstructured "
            "corpus of legal and regulatory text into computable graph models—enabling automated "
            "compliance checking, regulatory arbitrage detection, and legal reasoning at scale. "
            "This shifts legal knowledge from project-based work to structured SaaS."
        ),
        "implied_by_framework": "FW_003",
        "from_bm_implied": "BIM_026",
        "to_bm_implied": "BIM_005",
        "status": "open",
    },
    {
        "gap_id": "GAP_005",
        "name": "Human-as-API endware emerges from gig platforms",
        "description": (
            "Gig platforms (direct sales model) evolve into programmable human-labor APIs that "
            "agents can call for physical-world execution tasks. This transforms direct sales "
            "from human-managed workflows into agent-accessible endware, becoming a critical "
            "layer in the AI execution stack."
        ),
        "implied_by_framework": "FW_001",
        "from_bm_implied": "BIM_009",
        "to_bm_implied": "BIM_042",
        "status": "open",
    },
    {
        "gap_id": "GAP_006",
        "name": "Organizational simulation disrupts management consulting",
        "description": (
            "AI systems that can model how decisions propagate through organizations—across "
            "pricing, hiring, product, and operations—can replace the high-cost, slow, and "
            "qualitative work of management consulting. This shifts from episodic professional "
            "services engagements to continuous organizational intelligence platforms."
        ),
        "implied_by_framework": "FW_002",
        "from_bm_implied": "BIM_015",
        "to_bm_implied": "BIM_044",
        "status": "open",
    },
]

# GAP_006 is also implied by FW_004
GAP_ADDITIONAL_FRAMEWORKS = {
    "GAP_006": ["FW_004"],
}


# ---------------------------------------------------------------------------
# Seeding functions
# ---------------------------------------------------------------------------

def seed_frameworks(session):
    print("\n=== Step 1: Creating InvestmentFramework nodes ===")
    for fw in FRAMEWORKS:
        session.run(
            """
            MERGE (fw:InvestmentFramework {framework_id: $framework_id})
            SET fw.name         = $name,
                fw.summary      = $summary,
                fw.full_text    = $full_text,
                fw.version      = $version,
                fw.created_at   = $created_at,
                fw.last_updated = $last_updated
            """,
            **fw,
        )
        print(f"  Created/updated: {fw['framework_id']} — {fw['name']}")
    print(f"  Total frameworks: {len(FRAMEWORKS)}")


def seed_concepts(session):
    print("\n=== Step 2: Creating FrameworkConcept nodes and HAS_CONCEPT relationships ===")
    for c in CONCEPTS:
        session.run(
            """
            MERGE (c:FrameworkConcept {concept_id: $concept_id})
            SET c.name         = $name,
                c.definition   = $definition,
                c.framework_id = $framework_id
            WITH c
            MATCH (fw:InvestmentFramework {framework_id: $framework_id})
            MERGE (fw)-[:HAS_CONCEPT]->(c)
            """,
            **c,
        )
        print(f"  Created concept: {c['concept_id']} ({c['name']}) → {c['framework_id']}")

    # Report counts per framework
    for fw_id in ["FW_001", "FW_002", "FW_003", "FW_004"]:
        count = sum(1 for c in CONCEPTS if c["framework_id"] == fw_id)
        print(f"  {fw_id} concept count: {count}")


def seed_grounding(session):
    print("\n=== Step 3: Creating GROUNDED_IN relationships (hypothesis → framework) ===")

    # Deduplicate
    seen = set()
    unique_rules = []
    for hid, fwid in GROUNDING_RULES:
        key = (hid, fwid)
        if key not in seen:
            seen.add(key)
            unique_rules.append((hid, fwid))

    links_created = 0
    links_skipped = 0

    for hid, fwid in unique_rules:
        result = session.run(
            """
            MATCH (h:DisruptionHypothesis {hypothesis_id: $hid})
            MATCH (fw:InvestmentFramework {framework_id: $fwid})
            MERGE (h)-[r:GROUNDED_IN]->(fw)
            RETURN h.hypothesis_id AS hid, fw.name AS fw_name,
                   (CASE WHEN r IS NOT NULL THEN 1 ELSE 0 END) AS created
            """,
            hid=hid,
            fwid=fwid,
        ).single()

        if result:
            links_created += 1
            print(f"  Linked: {result['hid']} → {result['fw_name']}")
        else:
            links_skipped += 1
            print(f"  SKIPPED (not found): {hid} → {fwid}")

    print(f"\n  Total links created: {links_created} | Skipped (missing nodes): {links_skipped}")
    return links_created


def seed_gaps(session):
    print("\n=== Step 4: Creating HypothesisGap nodes and IMPLIED_BY relationships ===")
    for gap in GAPS:
        session.run(
            """
            MERGE (g:HypothesisGap {gap_id: $gap_id})
            SET g.name                  = $name,
                g.description           = $description,
                g.implied_by_framework  = $implied_by_framework,
                g.from_bm_implied       = $from_bm_implied,
                g.to_bm_implied         = $to_bm_implied,
                g.status                = $status
            WITH g
            MATCH (fw:InvestmentFramework {framework_id: $implied_by_framework})
            MERGE (g)-[:IMPLIED_BY]->(fw)
            """,
            **gap,
        )
        print(f"  Created gap: {gap['gap_id']} — {gap['name']}")

    # Additional framework links for multi-framework gaps
    for gap_id, fw_ids in GAP_ADDITIONAL_FRAMEWORKS.items():
        for fw_id in fw_ids:
            session.run(
                """
                MATCH (g:HypothesisGap {gap_id: $gap_id})
                MATCH (fw:InvestmentFramework {framework_id: $fw_id})
                MERGE (g)-[:IMPLIED_BY]->(fw)
                """,
                gap_id=gap_id,
                fw_id=fw_id,
            )
            print(f"  Additional link: {gap_id} → {fw_id}")

    print(f"\n  Total gaps created: {len(GAPS)}")


def verify_and_report(session):
    print("\n=== Step 5: Verification Report ===")

    # 1. Framework nodes with concept counts
    print("\n--- Framework nodes and concept counts ---")
    rows = session.run(
        """
        MATCH (fw:InvestmentFramework)
        OPTIONAL MATCH (fw)-[:HAS_CONCEPT]->(c:FrameworkConcept)
        RETURN fw.framework_id AS fw_id, fw.name AS name,
               count(c) AS concept_count
        ORDER BY fw_id
        """
    ).data()
    for r in rows:
        print(f"  {r['fw_id']} | {r['name']} | concepts: {r['concept_count']}")

    # 2. Hypothesis → framework links
    print("\n--- Hypothesis → Framework links (GROUNDED_IN) ---")
    rows = session.run(
        """
        MATCH (h:DisruptionHypothesis)-[:GROUNDED_IN]->(fw:InvestmentFramework)
        RETURN h.hypothesis_id AS hypothesis_id, fw.name AS framework_name
        ORDER BY fw.framework_id, h.hypothesis_id
        """
    ).data()
    for r in rows:
        print(f"  {r['hypothesis_id']} → {r['framework_name']}")
    print(f"  Total links: {len(rows)}")

    # 3. Gap nodes
    print("\n--- HypothesisGap nodes ---")
    rows = session.run(
        """
        MATCH (g:HypothesisGap)
        OPTIONAL MATCH (g)-[:IMPLIED_BY]->(fw:InvestmentFramework)
        RETURN g.gap_id AS gap_id, g.name AS name,
               collect(fw.framework_id) AS frameworks,
               g.status AS status
        ORDER BY gap_id
        """
    ).data()
    for r in rows:
        print(f"  {r['gap_id']} | {r['name']} | implied_by: {r['frameworks']} | status: {r['status']}")
    print(f"  Total gaps: {len(rows)}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("Connecting to Neo4j...")
    driver = get_driver()

    with driver.session() as session:
        seed_frameworks(session)
        seed_concepts(session)
        seed_grounding(session)
        seed_gaps(session)
        verify_and_report(session)

    driver.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
