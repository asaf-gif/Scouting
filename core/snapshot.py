"""
core/snapshot.py — Graph state snapshots

Takes a lightweight JSON snapshot of the Neo4j graph before major operations
so you can see what the data looked like before any pipeline run changed it.

Usage:
    from core.snapshot import take_snapshot, list_snapshots

    take_snapshot(label="pre_ingestion")
    snapshots = list_snapshots()
"""

import os
import json
from datetime import datetime, timezone

SNAPSHOT_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data", "snapshots"
)


def _get_driver():
    from dotenv import load_dotenv
    load_dotenv(override=True)
    from neo4j import GraphDatabase
    return GraphDatabase.driver(
        os.getenv("NEO4J_URI"),
        auth=(os.getenv("NEO4J_USER", "neo4j"), os.getenv("NEO4J_PASSWORD")),
    )


def take_snapshot(label: str = "") -> dict:
    """
    Query Neo4j for key graph stats and write a JSON snapshot file.

    Returns the snapshot dict, or {} on error.
    """
    try:
        driver = _get_driver()
        with driver.session() as s:
            # Node counts
            node_counts = {
                r["label"]: r["cnt"]
                for r in s.run("""
                    MATCH (n)
                    RETURN labels(n)[0] AS label, count(n) AS cnt
                    ORDER BY cnt DESC
                """).data()
            }

            # Relationship counts
            rel_counts = {
                r["rel_type"]: r["cnt"]
                for r in s.run("""
                    MATCH ()-[r]->()
                    RETURN type(r) AS rel_type, count(r) AS cnt
                    ORDER BY cnt DESC
                """).data()
            }

            # Hypothesis breakdown
            hyp_stats = s.run("""
                MATCH (h:DisruptionHypothesis)
                RETURN
                    count(h)                                             AS total,
                    count(h.validation_score)                            AS scored,
                    count(CASE WHEN h.status = 'Validated' THEN 1 END)  AS validated,
                    count(CASE WHEN h.status = 'Contested' THEN 1 END)  AS contested,
                    count(CASE WHEN h.status = 'Hypothesis' THEN 1 END) AS hypothesis_status,
                    avg(h.conviction_score)                              AS avg_conviction
            """).single()

            # Top 5 hypotheses by conviction
            top_hyps = s.run("""
                MATCH (h:DisruptionHypothesis)
                RETURN h.hypothesis_id AS id, h.title AS title,
                       h.conviction_score AS conviction, h.status AS status
                ORDER BY conviction DESC LIMIT 5
            """).data()

            # Top 5 vectors by opportunity score
            top_vectors = s.run("""
                MATCH (v:TransformationVector)-[:FROM_BIM]->(f:BusinessModel)
                MATCH (v)-[:TO_BIM]->(t:BusinessModel)
                WHERE v.opportunity_score IS NOT NULL
                RETURN v.vector_id AS vid,
                       f.name AS from_bm, t.name AS to_bm,
                       v.opportunity_score AS opp,
                       v.signal_strength AS signal
                ORDER BY opp DESC LIMIT 5
            """).data()

        driver.close()

        now = datetime.now(timezone.utc)
        snapshot = {
            "timestamp":   now.isoformat(),
            "label":       label,
            "node_counts": node_counts,
            "rel_counts":  rel_counts,
            "hypotheses":  dict(hyp_stats) if hyp_stats else {},
            "top_hypotheses":  top_hyps,
            "top_vectors":     top_vectors,
        }

        # Write file
        os.makedirs(SNAPSHOT_DIR, exist_ok=True)
        filename = f"{now.strftime('%Y-%m-%dT%H-%M')}_{label or 'snapshot'}.json"
        filepath = os.path.join(SNAPSHOT_DIR, filename)
        with open(filepath, "w") as f:
            json.dump(snapshot, f, indent=2, default=str)

        return snapshot

    except Exception as e:
        return {"error": str(e)}


def list_snapshots(n: int = 20) -> list:
    """
    Return the most recent n snapshots as a list of summary dicts.
    """
    if not os.path.exists(SNAPSHOT_DIR):
        return []

    files = sorted(
        [f for f in os.listdir(SNAPSHOT_DIR) if f.endswith(".json")],
        reverse=True
    )[:n]

    result = []
    for fname in files:
        path = os.path.join(SNAPSHOT_DIR, fname)
        try:
            with open(path) as f:
                data = json.load(f)
            nc = data.get("node_counts", {})
            result.append({
                "filename":    fname,
                "timestamp":   data.get("timestamp", "")[:19],
                "label":       data.get("label", ""),
                "total_nodes": sum(nc.values()),
                "hypotheses":  nc.get("DisruptionHypothesis", 0),
                "vectors":     nc.get("TransformationVector", 0),
                "evidence":    nc.get("Evidence", 0),
                "filepath":    path,
                "full_data":   data,
            })
        except Exception:
            pass

    return result
