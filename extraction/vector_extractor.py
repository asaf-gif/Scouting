"""
extraction/vector_extractor.py — TransformationVector Extraction Agent

Given a URL or raw text, extracts evidence of real-world business model
transitions, writes Evidence nodes to Neo4j, and links them to the
matching TransformationVector (or creates a new one if not in library).

Usage:
    from extraction.vector_extractor import extract_from_text, extract_from_url

    results = extract_from_url("https://techcrunch.com/...")
    results = extract_from_text("OpenAI is shifting from API-only...")
"""

import os
import sys
import json
import re
import hashlib
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import anthropic
from dotenv import load_dotenv
from neo4j import GraphDatabase
from tavily import TavilyClient
from rich.console import Console
from rich.table import Table

load_dotenv(override=True)
console = Console(width=200)


def get_driver():
    return GraphDatabase.driver(
        os.getenv("NEO4J_URI"),
        auth=(os.getenv("NEO4J_USER", "neo4j"), os.getenv("NEO4J_PASSWORD")),
    )


def get_bm_library(driver) -> list[dict]:
    with driver.session() as s:
        return s.run("""
            MATCH (n:BusinessModel) WHERE n.status <> 'Deprecated'
            RETURN n.bim_id AS id, n.name AS name,
                   left(coalesce(n.description, ''), 200) AS description
            ORDER BY n.bim_id
        """).data()


def get_scalar_library(driver) -> list[dict]:
    with driver.session() as s:
        return s.run("""
            MATCH (n:Scalar)
            RETURN n.scalar_id AS id, n.name AS name
            ORDER BY n.scalar_id
        """).data()


def fetch_url_content(url: str) -> str:
    """Fetch URL content via Tavily extract."""
    client = TavilyClient(api_key=os.getenv("TAVILY_API_KEY"))
    try:
        resp = client.extract(urls=[url])
        results = resp.get("results", [])
        if results:
            return results[0].get("raw_content", "")[:6000]
    except Exception:
        pass
    # Fallback: search for the URL content
    try:
        resp = client.search(url, max_results=3)
        parts = []
        for r in resp.get("results", []):
            parts.append(r.get("content", "")[:800])
        return "\n\n".join(parts)
    except Exception as e:
        return f"Could not fetch URL: {e}"


def load_prompt(name: str) -> str:
    path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "prompts", name,
    )
    with open(path) as f:
        return f.read()


def call_claude_extract(text: str, bm_library: list, scalar_library: list) -> list[dict]:
    """Call Claude to extract transitions from text. Returns list of transition dicts."""
    client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

    bm_summary = "\n".join(
        f"- {b['id']}: {b['name']} — {b['description'][:150]}"
        for b in bm_library
    )
    scalar_summary = "\n".join(
        f"- {s['id']}: {s['name'][:100]}"
        for s in scalar_library
    )

    user_msg = f"""SOURCE TEXT:
{text[:5000]}

BUSINESS MODEL LIBRARY:
{bm_summary}

SCALAR CONDITIONS:
{scalar_summary}

Extract all genuine business model transitions evidenced in the text. Return a JSON array.
"""
    response = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=3000,
        system=load_prompt("vector_extraction.txt"),
        messages=[{"role": "user", "content": user_msg}],
    )
    raw = response.content[0].text.strip()
    arr = re.search(r"\[[\s\S]*\]", raw)
    if not arr:
        return []
    try:
        return json.loads(arr.group(0))
    except json.JSONDecodeError:
        return []


def evidence_id(from_bim: str, to_bim: str, quote: str) -> str:
    """Deterministic Evidence node ID based on vector + quote hash."""
    h = hashlib.md5(quote.encode()).hexdigest()[:8]
    return f"EVD_{from_bim}_{to_bim}_{h}"


def get_or_create_vector(driver, from_bim: str, to_bim: str) -> str:
    """Return the vector_id for this pair, creating a new node if needed."""
    vector_id = f"VEC_{from_bim}_{to_bim}"
    with driver.session() as s:
        existing = s.run(
            "MATCH (v:TransformationVector {vector_id: $id}) RETURN v.vector_id AS id",
            id=vector_id,
        ).single()
        if existing:
            return vector_id

        # Create new vector not in original Excel set
        now = datetime.now(timezone.utc).isoformat()
        s.run("""
            MATCH (f:BusinessModel {bim_id: $from_id})
            MATCH (t:BusinessModel {bim_id: $to_id})
            CREATE (v:TransformationVector {
                vector_id:  $vid,
                source:     'extraction_agent',
                created_at: $now,
                created_by: 'vector_extractor',
                confidence: 0.7,
                version:    1
            })
            CREATE (v)-[:FROM_BIM]->(f)
            CREATE (v)-[:TO_BIM]->(t)
            CREATE (f)-[:HAS_TRANSITION]->(v)
        """,
            from_id=from_bim,
            to_id=to_bim,
            vid=vector_id,
            now=now,
        )
    return vector_id


def write_evidence(driver, vector_id: str, transition: dict,
                   source_url: str, source_type: str) -> str:
    """Write an Evidence node and SUPPORTS relationship."""
    now = datetime.now(timezone.utc).isoformat()
    from_bim = transition.get("from_bim_id", "")
    to_bim   = transition.get("to_bim_id", "")
    quote    = transition.get("evidence_quote", "")
    evd_id   = evidence_id(from_bim, to_bim, quote)

    scalars_json = json.dumps(transition.get("scalars_activated", []))

    with driver.session() as s:
        s.run("""
            MERGE (e:Evidence {evidence_id: $evd_id})
            SET e.source_url        = $url,
                e.source_type       = $src_type,
                e.evidence_quote    = $quote,
                e.transition_summary = $summary,
                e.companies_mentioned = $companies,
                e.scalars_activated_json = $scalars_json,
                e.confidence        = $conf,
                e.extracted_at      = $now,
                e.created_by        = 'vector_extractor'
        """,
            evd_id=evd_id,
            url=source_url,
            src_type=source_type,
            quote=quote,
            summary=transition.get("transition_summary", ""),
            companies=transition.get("companies_mentioned", []),
            scalars_json=scalars_json,
            conf=float(transition.get("confidence", 0.7)),
            now=now,
        )
        # SUPPORTS relationship
        s.run("""
            MATCH (e:Evidence {evidence_id: $evd_id})
            MATCH (v:TransformationVector {vector_id: $vid})
            MERGE (e)-[r:SUPPORTS]->(v)
            SET r.confidence = $conf, r.created_at = $now
        """,
            evd_id=evd_id,
            vid=vector_id,
            conf=float(transition.get("confidence", 0.7)),
            now=now,
        )

        # Link scalars via ACTIVATES
        for sc in transition.get("scalars_activated", []):
            sid = sc.get("scalar_id", "")
            if sid:
                s.run("""
                    MATCH (e:Evidence {evidence_id: $evd_id})
                    MATCH (sc:Scalar {scalar_id: $sid})
                    MERGE (e)-[r:ACTIVATES]->(sc)
                    SET r.direction = $dir, r.created_at = $now
                """,
                    evd_id=evd_id,
                    sid=sid,
                    dir=sc.get("direction", "increases"),
                    now=now,
                )
    return evd_id


def extract_from_text(
    text: str,
    source_url: str = "manual_input",
    source_type: str = "manual_input",
    dry_run: bool = False,
) -> list[dict]:
    """
    Main extraction function. Returns list of result dicts:
    [{
      "vector_id": str,
      "evidence_id": str,
      "from_bim": str,
      "to_bim": str,
      "confidence": float,
      "status": "written" | "dry_run" | "skipped_low_confidence",
    }]
    """
    driver = get_driver()
    bm_library     = get_bm_library(driver)
    scalar_library = get_scalar_library(driver)

    console.print(f"\n[bold]Vector Extractor[/bold] — source: {source_url[:80]}")
    console.print(f"  [dim]Text length: {len(text)} chars[/dim]")
    console.print("  [dim]Calling Claude to extract transitions...[/dim]")

    transitions = call_claude_extract(text, bm_library, scalar_library)
    console.print(f"  Extracted {len(transitions)} transition(s)")

    results = []
    for t in transitions:
        from_bim = t.get("from_bim_id", "")
        to_bim   = t.get("to_bim_id", "")
        conf     = float(t.get("confidence", 0))

        if conf < 0.5 or not from_bim or not to_bim or from_bim == to_bim:
            results.append({
                "vector_id": f"VEC_{from_bim}_{to_bim}",
                "evidence_id": None,
                "from_bim": from_bim,
                "to_bim": to_bim,
                "confidence": conf,
                "status": "skipped_low_confidence",
            })
            continue

        vector_id = f"VEC_{from_bim}_{to_bim}"
        evd_id = None

        if not dry_run:
            vector_id = get_or_create_vector(driver, from_bim, to_bim)
            evd_id    = write_evidence(driver, vector_id, t, source_url, source_type)
            status = "written"
        else:
            evd_id = evidence_id(from_bim, to_bim, t.get("evidence_quote", ""))
            status = "dry_run"

        results.append({
            "vector_id":  vector_id,
            "evidence_id": evd_id,
            "from_bim":   from_bim,
            "to_bim":     to_bim,
            "confidence": conf,
            "transition": t,
            "status":     status,
        })

    driver.close()

    # Display results table
    if results:
        table = Table(title="Extracted Transitions", show_header=True)
        table.add_column("Vector", width=35)
        table.add_column("From → To", width=55)
        table.add_column("Conf", justify="right", width=6)
        table.add_column("Scalars", justify="right", width=8)
        table.add_column("Status", width=12)

        for r in results:
            t_data = r.get("transition", {})
            from_name = t_data.get("from_bim_name", r["from_bim"])
            to_name   = t_data.get("to_bim_name",   r["to_bim"])
            n_scalars = len(t_data.get("scalars_activated", []))
            table.add_row(
                r["vector_id"],
                f"{from_name[:25]} → {to_name[:25]}",
                f"{r['confidence']:.2f}",
                str(n_scalars),
                r["status"],
            )
        console.print(table)

    return results


def extract_from_url(url: str, dry_run: bool = False) -> list[dict]:
    """Fetch a URL and run extraction on its content."""
    console.print(f"\n[dim]Fetching: {url}[/dim]")
    content = fetch_url_content(url)
    if not content or len(content) < 100:
        console.print(f"[yellow]Warning: could not fetch meaningful content from {url}[/yellow]")
        return []
    return extract_from_text(content, source_url=url, source_type="web_article", dry_run=dry_run)
