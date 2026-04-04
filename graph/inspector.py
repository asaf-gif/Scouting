"""
graph/inspector.py — Graph Inspector CLI

Query and inspect the Knowledge Graph during development.
Every agent output can be verified with this tool.

Usage:
    python graph/inspector.py bm --list
    python graph/inspector.py bm --id 'Project Based'
    python graph/inspector.py vector --from 'Project Based' --to 'Selling Knowledge Databases'
    python graph/inspector.py top-transitions --tech kggen --limit 10
    python graph/inspector.py hypothesis --status candidate
    python graph/inspector.py company --id ipsos
    python graph/inspector.py review-queue --type new_bm_candidate
    python graph/inspector.py export --entity companies --output review.csv
"""

import os
import sys
import csv
import argparse

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
from neo4j import GraphDatabase
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.columns import Columns
from rich import box

load_dotenv()
# Force wide output so column names aren't truncated in subprocesses / narrow TTYs
console = Console(width=200)

TECH_FIELD = {
    "gnns":      "tech_score_gnns",
    "kggen":     "tech_score_kggen",
    "synthetic": "tech_score_synthetic",
    "gnn":       "tech_score_gnns",
    "synth":     "tech_score_synthetic",
}

TECH_LABEL = {
    "gnns":      "GNNs",
    "kggen":     "KGGen",
    "synthetic": "Synthetic Audiences",
    "gnn":       "GNNs",
    "synth":     "Synthetic Audiences",
}


def get_driver():
    return GraphDatabase.driver(
        os.getenv("NEO4J_URI"),
        auth=(os.getenv("NEO4J_USER", "neo4j"), os.getenv("NEO4J_PASSWORD")),
    )


def score_color(score) -> str:
    if score is None:
        return "[dim]—[/dim]"
    s = int(score)
    if s >= 10:
        return f"[bold green]+{s}[/bold green]"
    if s >= 5:
        return f"[green]+{s}[/green]"
    if s > 0:
        return f"[cyan]+{s}[/cyan]"
    if s == 0:
        return f"[dim]0[/dim]"
    if s >= -5:
        return f"[yellow]{s}[/yellow]"
    return f"[red]{s}[/red]"


def conf_color(score) -> str:
    if score is None:
        return "[dim]?[/dim]"
    s = float(score)
    if s >= 70:
        return f"[bold green]{s:.0f}[/bold green]"
    if s >= 40:
        return f"[yellow]{s:.0f}[/yellow]"
    return f"[dim]{s:.0f}[/dim]"


# ─────────────────────────────────────────────
# BUSINESS MODELS
# ─────────────────────────────────────────────

def cmd_bm(args, driver):
    if args.list:
        with driver.session() as s:
            result = s.run("""
                MATCH (n:BusinessModel)
                RETURN n.bim_id AS id, n.name AS name, n.status AS status,
                       n.typical_margins AS margins, n.source AS source
                ORDER BY n.bim_id
            """)
            rows = result.data()

        t = Table(title=f"Business Model Library ({len(rows)} models)",
                  box=box.SIMPLE_HEAVY, show_lines=False)
        t.add_column("ID", style="dim", width=10)
        t.add_column("Name", style="bold")
        t.add_column("Status", width=10)
        t.add_column("Margins", width=10)
        t.add_column("Source", style="dim", width=16)

        for r in rows:
            status_style = "green" if r["status"] == "Active" else "yellow"
            t.add_row(
                r["id"] or "—",
                r["name"] or "—",
                f"[{status_style}]{r['status'] or '—'}[/{status_style}]",
                r["margins"] or "—",
                r["source"] or "—",
            )
        console.print(t)
        return

    if args.id:
        name_or_id = args.id.strip()
        with driver.session() as s:
            result = s.run("""
                MATCH (n:BusinessModel)
                WHERE n.bim_id = $q OR toLower(n.name) CONTAINS toLower($q)
                RETURN n
                LIMIT 1
            """, q=name_or_id)
            rec = result.single()

        if not rec:
            console.print(f"[red]No BusinessModel found matching '{name_or_id}'[/red]")
            return

        node = dict(rec["n"])

        # Properties panel
        lines = [f"[bold]{k}:[/bold] {v}" for k, v in sorted(node.items())]
        console.print(Panel("\n".join(lines), title=f"BusinessModel: {node.get('name', '?')}"))

        # Outgoing transitions
        with driver.session() as s:
            out = s.run("""
                MATCH (f:BusinessModel)-[:HAS_TRANSITION]->(t:BusinessModel)
                WHERE f.bim_id = $id OR toLower(f.name) CONTAINS toLower($id)
                OPTIONAL MATCH (v:TransformationVector)-[:FROM_BIM]->(f), (v)-[:TO_BIM]->(t)
                RETURN t.name AS to_name, v.evidence_quality AS quality,
                       v.tech_score_kggen AS kggen, v.tech_score_gnns AS gnns,
                       v.tech_score_synthetic AS synth
                ORDER BY kggen DESC
                LIMIT 10
            """, id=name_or_id).data()

        if out:
            ot = Table(title="Top 10 outgoing transitions (by KGGen score)",
                       box=box.SIMPLE_HEAVY, show_lines=False)
            ot.add_column("→ To", style="bold")
            ot.add_column("KGGen", justify="right", width=8)
            ot.add_column("GNNs", justify="right", width=8)
            ot.add_column("Synth", justify="right", width=8)
            ot.add_column("Quality", width=12)
            for r in out:
                ot.add_row(
                    r["to_name"] or "—",
                    score_color(r["kggen"]),
                    score_color(r["gnns"]),
                    score_color(r["synth"]),
                    r["quality"] or "—",
                )
            console.print(ot)


# ─────────────────────────────────────────────
# VECTORS
# ─────────────────────────────────────────────

def cmd_vector(args, driver):
    with driver.session() as s:
        result = s.run("""
            MATCH (v:TransformationVector)-[:FROM_BIM]->(f:BusinessModel),
                  (v)-[:TO_BIM]->(t:BusinessModel)
            WHERE toLower(f.name) CONTAINS toLower($from_q)
              AND toLower(t.name) CONTAINS toLower($to_q)
            RETURN v, f.name AS from_name, t.name AS to_name
            LIMIT 1
        """, from_q=args.from_bm.strip(), to_q=args.to_bm.strip())
        rec = result.single()

    if not rec:
        console.print(f"[red]No vector found: '{args.from_bm}' → '{args.to_bm}'[/red]")
        return

    v = dict(rec["v"])
    from_name = rec["from_name"]
    to_name   = rec["to_name"]

    lines = [
        f"[bold]vector_id:[/bold] {v.get('vector_id', '—')}",
        f"[bold]FROM:[/bold]      {from_name}",
        f"[bold]TO:[/bold]        {to_name}",
        f"[bold]Quality:[/bold]   {v.get('evidence_quality', '—')}",
        f"[bold]KGGen:[/bold]     {v.get('tech_score_kggen', 0)}",
        f"[bold]GNNs:[/bold]      {v.get('tech_score_gnns', 0)}",
        f"[bold]Synth:[/bold]     {v.get('tech_score_synthetic', 0)}",
        "",
        f"[bold]Example:[/bold]",
        v.get("example_text", "[dim]None[/dim]"),
    ]
    console.print(Panel("\n".join(str(l) for l in lines),
                        title=f"Vector: {from_name} → {to_name}"))


# ─────────────────────────────────────────────
# TOP TRANSITIONS
# ─────────────────────────────────────────────

def cmd_top_transitions(args, driver):
    tech = args.tech.lower()
    field = TECH_FIELD.get(tech)
    if not field:
        console.print(f"[red]Unknown tech '{args.tech}'. Use: gnns, kggen, synthetic[/red]")
        return

    limit = args.limit
    tech_label = TECH_LABEL.get(tech, tech)

    with driver.session() as s:
        result = s.run(f"""
            MATCH (v:TransformationVector)-[:FROM_BIM]->(f:BusinessModel),
                  (v)-[:TO_BIM]->(t:BusinessModel)
            WHERE v.{field} IS NOT NULL
            RETURN f.name AS from_name, t.name AS to_name,
                   v.{field} AS score,
                   v.tech_score_gnns AS gnns,
                   v.tech_score_kggen AS kggen,
                   v.tech_score_synthetic AS synth,
                   v.evidence_quality AS quality,
                   v.vector_id AS vid
            ORDER BY v.{field} DESC
            LIMIT $limit
        """, limit=limit)
        rows = result.data()

    t = Table(title=f"Top {limit} transitions by {tech_label} score",
              box=box.SIMPLE_HEAVY, show_lines=False)
    t.add_column("#", width=4, justify="right", style="dim")
    t.add_column("FROM", style="bold", min_width=20)
    t.add_column("→ TO", min_width=20)
    t.add_column("Score", justify="right", width=8)
    t.add_column("GNNs", justify="right", width=7)
    t.add_column("KGGen", justify="right", width=7)
    t.add_column("Synth", justify="right", width=7)
    t.add_column("Quality", width=12)

    for i, r in enumerate(rows, 1):
        t.add_row(
            str(i),
            r["from_name"] or "—",
            r["to_name"] or "—",
            score_color(r["score"]),
            score_color(r["gnns"]),
            score_color(r["kggen"]),
            score_color(r["synth"]),
            r["quality"] or "—",
        )
    console.print(t)

    if args.output:
        with open(args.output, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=[
                "rank", "from", "to", "score", "gnns", "kggen", "synthetic", "quality", "vector_id"
            ])
            writer.writeheader()
            for i, r in enumerate(rows, 1):
                writer.writerow({
                    "rank": i, "from": r["from_name"], "to": r["to_name"],
                    "score": r["score"], "gnns": r["gnns"],
                    "kggen": r["kggen"], "synthetic": r["synth"],
                    "quality": r["quality"], "vector_id": r["vid"],
                })
        console.print(f"\n[green]Exported to {args.output}[/green]")


# ─────────────────────────────────────────────
# HYPOTHESIS
# ─────────────────────────────────────────────

def cmd_hypothesis(args, driver):
    with driver.session() as s:
        if args.status:
            result = s.run("""
                MATCH (h:DisruptionHypothesis)
                WHERE toLower(h.status) = toLower($status)
                OPTIONAL MATCH (h)-[:TARGETS]->(c:Company)
                OPTIONAL MATCH (h)-[:PREDICTS]->(v:TransformationVector)-[:FROM_BIM]->(f:BusinessModel),
                               (v)-[:TO_BIM]->(t:BusinessModel)
                RETURN h.hyp_id AS id, h.status AS status,
                       h.confidence_score AS confidence, h.confidence_gate AS gate,
                       c.name AS company, f.name AS from_bm, t.name AS to_bm,
                       h.created_at AS created
                ORDER BY h.confidence_score DESC
                LIMIT 50
            """, status=args.status)
        elif args.gate:
            result = s.run("""
                MATCH (h:DisruptionHypothesis)
                WHERE toLower(h.confidence_gate) = toLower($gate)
                OPTIONAL MATCH (h)-[:TARGETS]->(c:Company)
                OPTIONAL MATCH (h)-[:PREDICTS]->(v:TransformationVector)-[:FROM_BIM]->(f:BusinessModel),
                               (v)-[:TO_BIM]->(t:BusinessModel)
                RETURN h.hyp_id AS id, h.status AS status,
                       h.confidence_score AS confidence, h.confidence_gate AS gate,
                       c.name AS company, f.name AS from_bm, t.name AS to_bm,
                       h.created_at AS created
                ORDER BY h.confidence_score DESC
                LIMIT 50
            """, gate=args.gate)
        else:
            result = s.run("""
                MATCH (h:DisruptionHypothesis)
                OPTIONAL MATCH (h)-[:TARGETS]->(c:Company)
                OPTIONAL MATCH (h)-[:PREDICTS]->(v:TransformationVector)-[:FROM_BIM]->(f:BusinessModel),
                               (v)-[:TO_BIM]->(t:BusinessModel)
                RETURN h.hyp_id AS id, h.status AS status,
                       h.confidence_score AS confidence, h.confidence_gate AS gate,
                       c.name AS company, f.name AS from_bm, t.name AS to_bm,
                       h.created_at AS created
                ORDER BY h.confidence_score DESC
                LIMIT 50
            """)
        rows = result.data()

    if not rows:
        console.print("[yellow]No hypotheses found.[/yellow]")
        return

    t = Table(title=f"Hypotheses ({len(rows)} shown)", box=box.SIMPLE_HEAVY, show_lines=False)
    t.add_column("ID", style="dim", width=12)
    t.add_column("Company", style="bold", min_width=15)
    t.add_column("FROM → TO", min_width=30)
    t.add_column("Conf", justify="right", width=6)
    t.add_column("Gate", width=12)
    t.add_column("Status", width=12)

    for r in rows:
        transition = f"{r['from_bm'] or '?'} → {r['to_bm'] or '?'}"
        gate = r["gate"] or "—"
        gate_style = {"deep_research": "green", "review": "yellow", "passive": "dim"}.get(gate, "")
        t.add_row(
            r["id"] or "—",
            r["company"] or "—",
            transition,
            conf_color(r["confidence"]),
            f"[{gate_style}]{gate}[/{gate_style}]" if gate_style else gate,
            r["status"] or "—",
        )
    console.print(t)


# ─────────────────────────────────────────────
# COMPANY
# ─────────────────────────────────────────────

def cmd_company(args, driver):
    with driver.session() as s:
        result = s.run("""
            MATCH (c:Company)
            WHERE toLower(c.company_id) CONTAINS toLower($q)
               OR toLower(c.name) CONTAINS toLower($q)
            OPTIONAL MATCH (c)-[:CLASSIFIES]->(bm:BusinessModel)
            RETURN c, collect(bm.name) AS bms
            LIMIT 1
        """, q=args.id.strip())
        rec = result.single()

    if not rec:
        console.print(f"[red]No company found matching '{args.id}'[/red]")
        return

    node = dict(rec["c"])
    bms  = rec["bms"]

    lines = [f"[bold]{k}:[/bold] {v}" for k, v in sorted(node.items())]
    if bms:
        lines.append(f"[bold]business_models:[/bold] {', '.join(bms)}")
    console.print(Panel("\n".join(lines), title=f"Company: {node.get('name', '?')}"))


# ─────────────────────────────────────────────
# REVIEW QUEUE
# ─────────────────────────────────────────────

def cmd_review_queue(args, driver):
    with driver.session() as s:
        if args.type:
            result = s.run("""
                MATCH (q:HumanReviewItem)
                WHERE toLower(q.item_type) CONTAINS toLower($t)
                  AND q.status <> 'Completed'
                RETURN q.queue_id AS id, q.item_type AS type,
                       q.priority_score AS priority, q.status AS status,
                       q.suggested_action AS action, q.created_at AS created
                ORDER BY q.priority_score DESC
                LIMIT 50
            """, t=args.type)
        else:
            result = s.run("""
                MATCH (q:HumanReviewItem)
                WHERE q.status <> 'Completed'
                RETURN q.queue_id AS id, q.item_type AS type,
                       q.priority_score AS priority, q.status AS status,
                       q.suggested_action AS action, q.created_at AS created
                ORDER BY q.priority_score DESC
                LIMIT 50
            """)
        rows = result.data()

    if not rows:
        console.print("[yellow]Review queue is empty.[/yellow]")
        return

    t = Table(title=f"Human Review Queue ({len(rows)} items)", box=box.SIMPLE_HEAVY)
    t.add_column("ID", style="dim", width=14)
    t.add_column("Type", width=22)
    t.add_column("Priority", justify="right", width=9)
    t.add_column("Status", width=12)
    t.add_column("Suggested Action", min_width=20)

    for r in rows:
        t.add_row(
            r["id"] or "—",
            r["type"] or "—",
            f"{r['priority']:.0f}" if r["priority"] else "—",
            r["status"] or "—",
            r["action"] or "—",
        )
    console.print(t)


# ─────────────────────────────────────────────
# EXPORT
# ─────────────────────────────────────────────

def cmd_export(args, driver):
    entity = args.entity.lower()
    output = args.output

    queries = {
        "companies": (
            "MATCH (c:Company) RETURN c.company_id AS id, c.name AS name, "
            "c.monitoring_status AS status, c.last_updated AS updated",
            ["id", "name", "status", "updated"],
        ),
        "bms": (
            "MATCH (n:BusinessModel) RETURN n.bim_id AS id, n.name AS name, "
            "n.status AS status, n.source AS source ORDER BY n.bim_id",
            ["id", "name", "status", "source"],
        ),
        "vectors": (
            "MATCH (v:TransformationVector)-[:FROM_BIM]->(f:BusinessModel), "
            "(v)-[:TO_BIM]->(t:BusinessModel) "
            "RETURN v.vector_id AS id, f.name AS from_name, t.name AS to_name, "
            "v.tech_score_kggen AS kggen, v.tech_score_gnns AS gnns, "
            "v.tech_score_synthetic AS synth, v.evidence_quality AS quality "
            "ORDER BY kggen DESC",
            ["id", "from_name", "to_name", "kggen", "gnns", "synth", "quality"],
        ),
        "hypotheses": (
            "MATCH (h:DisruptionHypothesis) "
            "OPTIONAL MATCH (h)-[:TARGETS]->(c:Company) "
            "RETURN h.hyp_id AS id, h.status AS status, h.confidence_score AS confidence, "
            "h.confidence_gate AS gate, c.name AS company ORDER BY confidence DESC",
            ["id", "status", "confidence", "gate", "company"],
        ),
        "scalars": (
            "MATCH (s:Scalar) RETURN s.scalar_id AS id, s.code AS code, "
            "s.name AS name, s.group AS group ORDER BY s.code",
            ["id", "code", "name", "group"],
        ),
    }

    if entity not in queries:
        console.print(f"[red]Unknown entity '{entity}'. Choose: {', '.join(queries.keys())}[/red]")
        return

    query, fields = queries[entity]
    with driver.session() as s:
        rows = s.run(query).data()

    with open(output, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

    console.print(f"[green]✓ Exported {len(rows)} {entity} to {output}[/green]")


# ─────────────────────────────────────────────
# ARGUMENT PARSER
# ─────────────────────────────────────────────

def build_parser():
    parser = argparse.ArgumentParser(
        prog="python graph/inspector.py",
        description="Systematic Problem Scouting — Graph Inspector CLI",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # bm
    p_bm = sub.add_parser("bm", help="Query BusinessModel nodes")
    g = p_bm.add_mutually_exclusive_group(required=True)
    g.add_argument("--list", action="store_true", help="List all business models")
    g.add_argument("--id", metavar="NAME_OR_ID", help="Show a specific business model")

    # vector
    p_vec = sub.add_parser("vector", help="Show a transition vector")
    p_vec.add_argument("--from", dest="from_bm", required=True, metavar="FROM_BM")
    p_vec.add_argument("--to",   dest="to_bm",   required=True, metavar="TO_BM")

    # top-transitions
    p_top = sub.add_parser("top-transitions", help="Ranked transitions by technology")
    p_top.add_argument("--tech",  required=True, choices=["gnns", "kggen", "synthetic", "gnn", "synth"])
    p_top.add_argument("--limit", type=int, default=10)
    p_top.add_argument("--output", metavar="FILE.CSV", help="Also export to CSV")

    # hypothesis
    p_hyp = sub.add_parser("hypothesis", help="Query DisruptionHypothesis nodes")
    p_hyp.add_argument("--status", metavar="STATUS")
    p_hyp.add_argument("--gate",   metavar="GATE")

    # company
    p_co = sub.add_parser("company", help="Show a Company node")
    p_co.add_argument("--id", required=True, metavar="NAME_OR_ID")

    # review-queue
    p_rq = sub.add_parser("review-queue", help="Show pending Human Review Queue items")
    p_rq.add_argument("--type", metavar="ITEM_TYPE")

    # export
    p_exp = sub.add_parser("export", help="Export entities to CSV")
    p_exp.add_argument("--entity", required=True,
                       choices=["companies", "bms", "vectors", "hypotheses", "scalars"])
    p_exp.add_argument("--output", required=True, metavar="FILE.CSV")


    return parser


# Module-level command registry (used by tests and orchestrator)
COMMANDS = {
    "bm":               cmd_bm,
    "vector":           cmd_vector,
    "top-transitions":  cmd_top_transitions,
    "hypothesis":       cmd_hypothesis,
    "company":          cmd_company,
    "review-queue":     cmd_review_queue,
    "export":           cmd_export,
}


def main():
    parser = build_parser()
    args = parser.parse_args()
    driver = get_driver()

    dispatch = COMMANDS

    fn = dispatch.get(args.command)
    if fn:
        fn(args, driver)
    else:
        parser.print_help()

    driver.close()


if __name__ == "__main__":
    main()
