"""
core/agent.py — Disruption Scout Agent

A Claude-powered conversational agent that can:
  - Explain and discuss hypotheses, companies, BMs, technologies, scalars
  - Query the Neo4j graph for context
  - Read and update prompts and logic constants (with full changelog)
  - Reason about why the system behaves the way it does

Uses Anthropic tool-use (function calling). The agent loop runs synchronously
so Streamlit can display intermediate tool results inline.
"""

import json
import os
import sys

import anthropic

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv(override=True)

# ── Anthropic client ──────────────────────────────────────────────────────────
_client = None

def get_client() -> anthropic.Anthropic:
    global _client
    if _client is None:
        _client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
    return _client


# ── Neo4j helper (imported lazily to avoid circular deps) ─────────────────────
def _run_query(cypher: str, **params):
    from neo4j import GraphDatabase
    driver = GraphDatabase.driver(
        os.getenv("NEO4J_URI"),
        auth=(os.getenv("NEO4J_USER", "neo4j"), os.getenv("NEO4J_PASSWORD")),
    )
    with driver.session() as s:
        result = s.run(cypher, **params).data()
    driver.close()
    return result


# ── System prompt ─────────────────────────────────────────────────────────────
SYSTEM_PROMPT = """You are the Disruption Scout Agent — an expert analyst embedded in a
business-model disruption tracking platform built on Neo4j + Claude.

YOUR ROLE
You help the operator understand, validate, and improve the system's findings.
You can:
  • Explain any hypothesis, company, BM, technology, or scalar in plain language
  • Pull live data from the graph to ground your analysis in real evidence
  • Identify weaknesses or gaps in a hypothesis and propose improvements
  • Read the prompts used by each pipeline stage and reason about their design
  • Propose and apply changes to prompts or logic constants, always logging your rationale
  • Write new nodes and relationships to the graph (BMs, scalars, vectors, hypotheses,
    framework links, gaps) — always with a rationale, always logged

SYSTEM ARCHITECTURE
The pipeline flows: Technology → (tech_scalar_classifier) → MOVES_SCALAR
→ (vector_activator, threshold=0.35) → ACTIVATES TransformationVector
→ (hypothesis_generator) → DisruptionHypothesis

NODE TYPES (full schema)
  BusinessModel (bim_id, name, description)
    — a way companies make money. IDs follow BIM_001…BIM_041+
  TransformationVector (vid, name, description, signal_strength)
    — a real-world transition between two BMs
  Scalar (scalar_id, name, description)
    — a structural condition that drives BM transitions
  Technology (tech_id, name, description, maturity)
    — a technology that moves scalars
  DisruptionHypothesis (hypothesis_id, title, thesis, status, conviction_score)
    — a prediction that a technology disrupts a transition
  Company (company_id, name, fortune_rank, gics_sector, gics_industry_group)
    — a company classified against BMs
  Evidence (evidence_id, text, source)
    — a real-world data point supporting a vector
  InvestmentFramework (framework_id, name, summary, full_text, version, last_updated)
    — a first-principles investment thesis. IDs: FW_001…FW_004+
  FrameworkConcept (concept_id, name, definition, framework_id)
    — a key concept within an InvestmentFramework
  HypothesisGap (gap_id, name, description, status, from_bm_implied, to_bm_implied)
    — a disruption implied by a framework but not yet built as a hypothesis

KEY RELATIONSHIPS
  (Tech)-[:MOVES_SCALAR]->(Scalar)           props: direction, strength, score, rationale
  (Tech)-[:ACTIVATES]->(Vector)              props: activation_score
  (Vector)-[:IMPACTS]->(Scalar)
  (Vector)-[:FROM_BIM]->(BM)
  (Vector)-[:TO_BIM]->(BM)
  (Hypothesis)-[:GENERATED_FROM]->(Vector)
  (Hypothesis)-[:TRIGGERED_BY]->(Tech)
  (Hypothesis)-[:TARGETS]->(BM)             — the disrupted BM
  (Hypothesis)-[:PROPOSES]->(BM)            — the new BM
  (Hypothesis)-[:GROUNDED_IN]->(InvestmentFramework)
  (Company)-[:EXPOSED_TO]->(Hypothesis)
  (Company)-[:OPERATES_AS]->(BM)
  (InvestmentFramework)-[:HAS_CONCEPT]->(FrameworkConcept)
  (HypothesisGap)-[:IMPLIED_BY]->(InvestmentFramework)

WRITE TOOL GUIDELINES
Use write_graph to:
  • Create new BusinessModel nodes (use next available BIM_0XX id — check first)
  • Create new Scalar nodes (use next available SCL_XXX id — check first)
  • Create new Technology nodes (use next available TECH_XXX id — check first)
  • Link technologies to scalars (MOVES_SCALAR)
  • Create TransformationVectors and link to BMs
  • Create or update DisruptionHypotheses
  • Link hypotheses to frameworks (GROUNDED_IN)
  • Update HypothesisGap status
  • Add FrameworkConcepts to frameworks
  • Update any node property (e.g. h.status, fw.summary)

Safety rules for write_graph:
  • Never DROP databases, indexes, or constraints
  • Never delete Company nodes or EXPOSED_TO relationships in bulk without explicit user confirmation
  • Always use MERGE (not CREATE) when creating BM/Scalar/Tech nodes to avoid duplicates
  • Always provide a rationale — it is logged to the changelog
  • Before creating a new BIM/SCL/TECH ID, query_graph to find the highest existing ID first

PROMPTS USED (all in /prompts/*.txt):
  hypothesis_generation       — synthesises DisruptionHypothesis from tech + vector + scalar context
  scalar_classification       — classifies scalar impacts on a TransformationVector
  tech_scalar_classification  — maps how a technology moves each scalar
  vector_extraction           — extracts TransformationVectors from raw text evidence
  company_enrichment          — classifies a company against the BM library
  tech_enrichment             — enriches Technology nodes with maturity + scalar hints
  bm_enrichment               — enriches BM candidates, detects duplicates
  bm_scanner                  — scans internet for novel BM patterns

LOGIC CONSTANTS (all editable via update_logic_constant):
  activation.ACTIVATION_THRESHOLD        = 0.35
  activation.COVERAGE_FACTOR_BASE        = 3
  signal_weights.evidence_weight         = 0.40
  signal_weights.scalar_coverage_weight  = 0.30
  signal_weights.scalar_magnitude_weight = 0.20
  signal_weights.conviction_weight       = 0.10
  duplicate_detection.SIMILARITY_THRESHOLD = 0.85
  duplicate_detection.REVIEW_THRESHOLD     = 0.60

STYLE
  • Be concise but precise. Use bullet points when listing multiple items.
  • When writing to the graph, confirm what was created/updated before finishing.
  • Ask clarifying questions before making destructive changes.
  • Show your reasoning when analysing a hypothesis.
  • If you spot a flaw in a prompt or threshold, propose a fix with justification.
"""


# ── Tool definitions ──────────────────────────────────────────────────────────
TOOLS = [
    {
        "name": "get_hypothesis",
        "description": (
            "Fetch full details about a disruption hypothesis from the graph, "
            "including its thesis, conviction score, linked technology, vector, "
            "companies exposed, and scalar chain."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "hyp_id": {
                    "type": "string",
                    "description": "Hypothesis ID, e.g. 'HYP_001'",
                }
            },
            "required": ["hyp_id"],
        },
    },
    {
        "name": "search_hypotheses",
        "description": "Search hypotheses by keyword in title, thesis, or company name.",
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "Search term",
                },
                "limit": {
                    "type": "integer",
                    "description": "Max results to return (default 10)",
                    "default": 10,
                },
            },
            "required": ["query"],
        },
    },
    {
        "name": "query_graph",
        "description": (
            "Run a READ-ONLY Cypher query against the Neo4j graph. "
            "Use this to fetch any data not covered by other tools — "
            "e.g. scalar trends, company BM classification, vector evidence. "
            "Do NOT use WRITE operations (CREATE, SET, MERGE, DELETE)."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "cypher": {
                    "type": "string",
                    "description": "A read-only Cypher query",
                },
                "params": {
                    "type": "object",
                    "description": "Optional query parameters as a JSON object",
                },
            },
            "required": ["cypher"],
        },
    },
    {
        "name": "write_graph",
        "description": (
            "Run a WRITE Cypher query against the Neo4j graph. "
            "Use this to create or update nodes and relationships — new BMs, scalars, "
            "technologies, vectors, hypotheses, framework links, gap nodes, or any "
            "property update. All writes are logged to the editorial changelog. "
            "Always use MERGE (not CREATE) for entity nodes to avoid duplicates. "
            "Never DROP databases or constraints. "
            "A rationale is required."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "cypher": {
                    "type": "string",
                    "description": "A Cypher write statement (MERGE, CREATE, SET, DELETE, REMOVE)",
                },
                "params": {
                    "type": "object",
                    "description": "Optional query parameters as a JSON object",
                },
                "rationale": {
                    "type": "string",
                    "description": "Why this change is being made — stored in the changelog",
                },
                "description": {
                    "type": "string",
                    "description": "Short human-readable summary of what this write does, e.g. 'Create BIM_042 Human-as-API'",
                },
            },
            "required": ["cypher", "rationale", "description"],
        },
    },
    {
        "name": "read_prompt",
        "description": "Read the current content of a pipeline prompt file.",
        "input_schema": {
            "type": "object",
            "properties": {
                "prompt_id": {
                    "type": "string",
                    "description": (
                        "Prompt identifier: hypothesis_generation, scalar_classification, "
                        "tech_scalar_classification, vector_extraction, company_enrichment, "
                        "tech_enrichment, bm_enrichment, bm_scanner, "
                        "deep_research, counter_research"
                    ),
                }
            },
            "required": ["prompt_id"],
        },
    },
    {
        "name": "update_prompt",
        "description": (
            "Update the content of a pipeline prompt file. "
            "Always explain your rationale — it is logged to the editorial changelog. "
            "Only call this after discussing the change with the user."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "prompt_id": {
                    "type": "string",
                    "description": "Prompt identifier (same options as read_prompt)",
                },
                "new_content": {
                    "type": "string",
                    "description": "The full new prompt text",
                },
                "rationale": {
                    "type": "string",
                    "description": "Why this change was made — stored in the changelog",
                },
            },
            "required": ["prompt_id", "new_content", "rationale"],
        },
    },
    {
        "name": "get_logic_constants",
        "description": "Read all current logic constants and thresholds from logic_config.json.",
        "input_schema": {
            "type": "object",
            "properties": {},
        },
    },
    {
        "name": "update_logic_constant",
        "description": (
            "Update a single logic constant in logic_config.json. "
            "Changes take effect on the next pipeline run. "
            "Always provide a clear rationale — it is logged to the editorial changelog."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "category": {
                    "type": "string",
                    "description": "Category: activation, signal_weights, duplicate_detection, trends, impact_scoring",
                },
                "key": {
                    "type": "string",
                    "description": "Constant name, e.g. ACTIVATION_THRESHOLD",
                },
                "new_value": {
                    "description": "New value (number or object for map types)",
                },
                "rationale": {
                    "type": "string",
                    "description": "Why this value was chosen",
                },
            },
            "required": ["category", "key", "new_value", "rationale"],
        },
    },
    {
        "name": "list_editorial_changes",
        "description": "Show recent changes to prompts or logic constants from the editorial changelog.",
        "input_schema": {
            "type": "object",
            "properties": {
                "n": {
                    "type": "integer",
                    "description": "Number of recent changes to show (default 20)",
                    "default": 20,
                },
                "change_type": {
                    "type": "string",
                    "description": "Filter: 'prompt_edit', 'logic_edit', or omit for all",
                },
            },
        },
    },
]


# ── Tool execution ────────────────────────────────────────────────────────────
def execute_tool(name: str, inputs: dict) -> str:
    """Execute a tool call and return the result as a string."""
    try:
        if name == "get_hypothesis":
            return _tool_get_hypothesis(inputs["hyp_id"])

        elif name == "search_hypotheses":
            return _tool_search_hypotheses(inputs["query"], inputs.get("limit", 10))

        elif name == "query_graph":
            # Safety: block write operations
            cypher = inputs["cypher"]
            forbidden = ["CREATE ", "SET ", "MERGE ", "DELETE ", "REMOVE ", "DROP "]
            if any(kw in cypher.upper() for kw in forbidden):
                return "ERROR: Write operations are not permitted via query_graph."
            params = inputs.get("params") or {}
            rows = _run_query(cypher, **params)
            if not rows:
                return "Query returned no results."
            return json.dumps(rows[:50], default=str, indent=2)

        elif name == "write_graph":
            cypher     = inputs["cypher"]
            rationale  = inputs["rationale"]
            description = inputs.get("description", "graph write")
            params     = inputs.get("params") or {}

            # Hard block genuinely destructive DDL
            blocked = ["DROP DATABASE", "DROP CONSTRAINT", "DROP INDEX"]
            if any(kw in cypher.upper() for kw in blocked):
                return "ERROR: Destructive DDL (DROP DATABASE/CONSTRAINT/INDEX) is not permitted."

            # Execute
            rows = _run_query(cypher, **params)
            row_count = len(rows) if rows else 0

            # Log to editorial changelog
            try:
                from core.editorial import append_changelog
                append_changelog({
                    "change_type": "graph_write",
                    "item_name":   description,
                    "field":       "cypher",
                    "old_value":   None,
                    "new_value":   cypher[:300],
                    "rationale":   rationale,
                    "source":      "agent",
                })
            except Exception:
                pass  # Don't fail the write if logging fails

            summary = f"✅ Write executed: {description}\n"
            summary += f"   Rows returned: {row_count}\n"
            if rows:
                summary += "   Result preview:\n"
                summary += json.dumps(rows[:10], default=str, indent=2)
            return summary

        elif name == "read_prompt":
            from core.editorial import read_prompt
            content = read_prompt(inputs["prompt_id"])
            return content

        elif name == "update_prompt":
            from core.editorial import write_prompt
            write_prompt(
                inputs["prompt_id"],
                inputs["new_content"],
                inputs["rationale"],
                editor="agent",
            )
            return f"✅ Prompt '{inputs['prompt_id']}' updated and logged to editorial changelog."

        elif name == "get_logic_constants":
            from core.editorial import load_logic_config
            cfg = load_logic_config()
            # Return a clean summary (values only, with descriptions)
            out = {}
            for cat, constants in cfg.items():
                if cat.startswith("_"):
                    continue
                out[cat] = {}
                for key, meta in constants.items():
                    out[cat][key] = {
                        "value": meta.get("value"),
                        "description": meta.get("description", "")[:120],
                    }
            return json.dumps(out, indent=2)

        elif name == "update_logic_constant":
            from core.editorial import update_constant
            update_constant(
                inputs["category"],
                inputs["key"],
                inputs["new_value"],
                inputs["rationale"],
                editor="agent",
            )
            return (
                f"✅ `{inputs['category']}.{inputs['key']}` updated to "
                f"`{inputs['new_value']}` and logged to editorial changelog."
            )

        elif name == "list_editorial_changes":
            from core.editorial import load_changelog
            entries = load_changelog(
                n=inputs.get("n", 20),
                change_type=inputs.get("change_type"),
            )
            if not entries:
                return "No editorial changes logged yet."
            lines = []
            for e in entries:
                ts  = e.get("timestamp", "")[:19].replace("T", " ")
                src = "UI" if e.get("source") == "manual_ui" else ("Agent" if e.get("source") == "agent" else "Code")
                lines.append(
                    f"{ts} [{src}] {e.get('change_type','')} — {e.get('item_name','')} "
                    f"→ {str(e.get('new_value',''))[:60]} | {e.get('rationale','')[:80]}"
                )
            return "\n".join(lines)

        else:
            return f"ERROR: Unknown tool '{name}'"

    except Exception as exc:
        return f"ERROR executing {name}: {exc}"


# ── Individual tool implementations ───────────────────────────────────────────

def _tool_get_hypothesis(hyp_id: str) -> str:
    rows = _run_query("""
        MATCH (h:DisruptionHypothesis {hypothesis_id: $hid})
        OPTIONAL MATCH (h)-[:TRIGGERED_BY]->(t:Technology)
        OPTIONAL MATCH (h)-[:GENERATED_FROM]->(v:TransformationVector)
        OPTIONAL MATCH (v)-[:FROM_BIM]->(fb:BusinessModel)
        OPTIONAL MATCH (v)-[:TO_BIM]->(tb:BusinessModel)
        OPTIONAL MATCH (c:Company)-[:EXPOSED_TO]->(h)
        RETURN h.hypothesis_id      AS hyp_id,
               h.title              AS title,
               h.thesis             AS thesis,
               h.counter_argument   AS counter,
               h.conviction_score   AS conviction,
               h.disruption_type    AS dtype,
               h.time_horizon       AS horizon,
               h.status             AS status,
               h.primary_scalar     AS primary_scalar,
               h.supporting_scalars AS supporting_scalars,
               t.name               AS tech_name,
               t.tech_id            AS tech_id,
               v.vector_id          AS vector_id,
               v.signal_strength    AS signal,
               fb.name              AS from_bm,
               tb.name              AS to_bm,
               collect(DISTINCT c.name) AS exposed_companies
        LIMIT 1
    """, hid=hyp_id)

    if not rows:
        return f"No hypothesis found with ID '{hyp_id}'."

    h = rows[0]
    # Fetch scalar names
    scalar_ids = [h.get("primary_scalar")] + (h.get("supporting_scalars") or [])
    scalar_ids = [s for s in scalar_ids if s]
    scalar_names = {}
    if scalar_ids:
        srows = _run_query(
            "MATCH (s:Scalar) WHERE s.scalar_id IN $ids RETURN s.scalar_id AS id, s.name AS name",
            ids=scalar_ids,
        )
        scalar_names = {r["id"]: r["name"] for r in (srows or [])}

    lines = [
        f"HYPOTHESIS: {h.get('hyp_id')}",
        f"Title:      {h.get('title')}",
        f"Status:     {h.get('status')}  |  Conviction: {h.get('conviction') or 0:.2f}",
        f"Type:       {h.get('dtype')}  |  Horizon: {h.get('horizon')}",
        f"",
        f"Transition: {h.get('from_bm','?')} → {h.get('to_bm','?')}",
        f"Technology: {h.get('tech_name','?')} ({h.get('tech_id','')})",
        f"Vector:     {h.get('vector_id','?')}  (signal={h.get('signal') or 0:.3f})",
        f"",
        f"Primary scalar: {h.get('primary_scalar','')} — {scalar_names.get(h.get('primary_scalar',''),'')}"
    ]
    sup = [f"  {s} — {scalar_names.get(s,'')}" for s in (h.get("supporting_scalars") or []) if s]
    if sup:
        lines.append("Supporting scalars:")
        lines.extend(sup[:6])
    lines += [
        f"",
        f"THESIS:\n{h.get('thesis','')}",
        f"",
        f"COUNTER:\n{h.get('counter','')}",
        f"",
        f"Companies exposed ({len(h.get('exposed_companies') or [])}): "
        + ", ".join((h.get("exposed_companies") or [])[:10]),
    ]
    return "\n".join(lines)


def _tool_search_hypotheses(query: str, limit: int = 10) -> str:
    q = query.lower()
    rows = _run_query("""
        MATCH (h:DisruptionHypothesis)
        OPTIONAL MATCH (h)-[:TRIGGERED_BY]->(t:Technology)
        OPTIONAL MATCH (h)-[:GENERATED_FROM]->(v:TransformationVector)
        OPTIONAL MATCH (v)-[:FROM_BIM]->(fb:BusinessModel)
        OPTIONAL MATCH (v)-[:TO_BIM]->(tb:BusinessModel)
        WHERE toLower(h.title) CONTAINS $q
           OR toLower(h.thesis) CONTAINS $q
           OR toLower(coalesce(fb.name,'')) CONTAINS $q
           OR toLower(coalesce(tb.name,'')) CONTAINS $q
           OR toLower(coalesce(t.name,'')) CONTAINS $q
        RETURN h.hypothesis_id AS hyp_id,
               h.title AS title,
               h.conviction_score AS conviction,
               h.status AS status,
               t.name AS tech_name,
               fb.name AS from_bm,
               tb.name AS to_bm
        ORDER BY h.conviction_score DESC
        LIMIT $limit
    """, q=q, limit=limit)

    if not rows:
        return f"No hypotheses found matching '{query}'."

    lines = [f"Found {len(rows)} hypothesis(es) matching '{query}':\n"]
    for r in rows:
        lines.append(
            f"  {r['hyp_id']}  [{r.get('status','')}]  conviction={r.get('conviction') or 0:.2f}\n"
            f"  Title: {r.get('title','')}\n"
            f"  Tech: {r.get('tech_name','?')}  |  {r.get('from_bm','?')} → {r.get('to_bm','?')}\n"
        )
    return "\n".join(lines)


# ── Agent loop ────────────────────────────────────────────────────────────────
def run_agent_turn(
    messages: list,
    on_tool_start=None,
    on_tool_end=None,
) -> tuple[str, list]:
    """
    Run one turn of the agent loop (may involve multiple tool calls).

    Args:
        messages:      Full conversation history in Anthropic format.
        on_tool_start: Optional callback(tool_name, tool_input) called before each tool.
        on_tool_end:   Optional callback(tool_name, result) called after each tool.

    Returns:
        (final_text, updated_messages)
    """
    client = get_client()

    while True:
        response = client.messages.create(
            model="claude-sonnet-4-5",
            max_tokens=4096,
            system=SYSTEM_PROMPT,
            tools=TOOLS,
            messages=messages,
        )

        # Collect any text content
        text_blocks = [b.text for b in response.content if hasattr(b, "text")]
        tool_blocks = [b for b in response.content if b.type == "tool_use"]

        if not tool_blocks:
            # No more tool calls — we're done
            final_text = "\n".join(text_blocks) if text_blocks else ""
            messages = messages + [{"role": "assistant", "content": response.content}]
            return final_text, messages

        # Execute tool calls
        tool_results = []
        for tb in tool_blocks:
            if on_tool_start:
                on_tool_start(tb.name, tb.input)
            result = execute_tool(tb.name, tb.input)
            if on_tool_end:
                on_tool_end(tb.name, result)
            tool_results.append({
                "type": "tool_result",
                "tool_use_id": tb.id,
                "content": result,
            })

        # Append assistant turn + tool results and loop
        messages = messages + [
            {"role": "assistant", "content": response.content},
            {"role": "user",      "content": tool_results},
        ]
