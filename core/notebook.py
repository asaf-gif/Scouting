"""
core/notebook.py — Research Notebook

Persistent note-taking attached to disruption hypotheses.
Notes survive hypothesis rejection and cross-link via the shared
TransformationVector, so thoughts on the same transition surface
across all hypotheses that share it.

Node: ResearchNote
  note_id          — UUID (unique)
  hyp_id           — hypothesis this was written about
  vector_id        — transformation vector (for cross-linking)
  note_type        — idea | writeup | agent_convo | observation | question
  title            — short descriptive title
  content          — full markdown text
  created_at       — ISO timestamp
  updated_at       — ISO timestamp
  source           — "user" | "agent"
  tags             — list[str]
  hyp_title        — denormalised hypothesis title (survives rejection)
  hyp_status_at    — snapshot of hypothesis status when note was created
  from_bm_name     — denormalised from-BM name (for display)
  to_bm_name       — denormalised to-BM name (for display)

Relationships:
  (ResearchNote)-[:ATTACHED_TO]->(:DisruptionHypothesis)
  (ResearchNote)-[:REFERENCES_VECTOR]->(:TransformationVector)
"""

import os
import sys
import uuid
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv
load_dotenv(override=True)

from neo4j import GraphDatabase

NOTE_TYPES = ["idea", "writeup", "agent_convo", "observation", "question"]
NOTE_TYPE_ICONS = {
    "idea":       "💡",
    "writeup":    "📝",
    "agent_convo": "🤖",
    "observation": "👁",
    "question":   "❓",
}


# ── Driver ────────────────────────────────────────────────────────────────────
def _get_driver():
    return GraphDatabase.driver(
        os.getenv("NEO4J_URI"),
        auth=(os.getenv("NEO4J_USER", "neo4j"), os.getenv("NEO4J_PASSWORD")),
    )


def _run(cypher: str, **params):
    driver = _get_driver()
    with driver.session() as s:
        result = s.run(cypher, **params).data()
    driver.close()
    return result


# ── Schema init ───────────────────────────────────────────────────────────────
def ensure_schema():
    """Create constraint + index for ResearchNote if they don't exist."""
    _run("""
        CREATE CONSTRAINT research_note_id_unique IF NOT EXISTS
          FOR (n:ResearchNote) REQUIRE n.note_id IS UNIQUE
    """)
    _run("""
        CREATE INDEX research_note_hyp_id IF NOT EXISTS
          FOR (n:ResearchNote) ON (n.hyp_id)
    """)
    _run("""
        CREATE INDEX research_note_vector_id IF NOT EXISTS
          FOR (n:ResearchNote) ON (n.vector_id)
    """)
    _run("""
        CREATE INDEX research_note_created_at IF NOT EXISTS
          FOR (n:ResearchNote) ON (n.created_at)
    """)


# ── CRUD ──────────────────────────────────────────────────────────────────────
def create_note(
    hyp_id: str,
    title: str,
    content: str,
    note_type: str = "idea",
    source: str = "user",
    tags: list = None,
) -> str:
    """
    Create a ResearchNote attached to a hypothesis.
    Automatically resolves vector_id and BM names from the hypothesis.
    Returns the new note_id.
    """
    note_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc).isoformat()

    # Resolve hypothesis context for denormalisation
    ctx = _run("""
        MATCH (h:DisruptionHypothesis {hypothesis_id: $hid})
        OPTIONAL MATCH (h)-[:GENERATED_FROM]->(v:TransformationVector)
        OPTIONAL MATCH (v)-[:FROM_BIM]->(fb:BusinessModel)
        OPTIONAL MATCH (v)-[:TO_BIM]->(tb:BusinessModel)
        RETURN h.title AS hyp_title,
               h.status AS hyp_status,
               v.vector_id AS vector_id,
               fb.name AS from_bm,
               tb.name AS to_bm
        LIMIT 1
    """, hid=hyp_id)

    hyp_title  = (ctx[0]["hyp_title"]  if ctx else "") or ""
    hyp_status = (ctx[0]["hyp_status"] if ctx else "") or "Unknown"
    vector_id  = (ctx[0]["vector_id"]  if ctx else "") or ""
    from_bm    = (ctx[0]["from_bm"]    if ctx else "") or ""
    to_bm      = (ctx[0]["to_bm"]      if ctx else "") or ""

    _run("""
        CREATE (n:ResearchNote {
          note_id:       $note_id,
          hyp_id:        $hyp_id,
          vector_id:     $vector_id,
          note_type:     $note_type,
          title:         $title,
          content:       $content,
          created_at:    $now,
          updated_at:    $now,
          source:        $source,
          tags:          $tags,
          hyp_title:     $hyp_title,
          hyp_status_at: $hyp_status,
          from_bm_name:  $from_bm,
          to_bm_name:    $to_bm
        })
        WITH n
        MATCH (h:DisruptionHypothesis {hypothesis_id: $hyp_id})
        CREATE (n)-[:ATTACHED_TO]->(h)
    """,
        note_id=note_id, hyp_id=hyp_id, vector_id=vector_id,
        note_type=note_type, title=title, content=content,
        now=now, source=source, tags=tags or [],
        hyp_title=hyp_title, hyp_status=hyp_status,
        from_bm=from_bm, to_bm=to_bm,
    )

    # Also link to the TransformationVector if one exists
    if vector_id:
        _run("""
            MATCH (n:ResearchNote {note_id: $nid})
            MATCH (v:TransformationVector {vector_id: $vid})
            MERGE (n)-[:REFERENCES_VECTOR]->(v)
        """, nid=note_id, vid=vector_id)

    return note_id


def get_notes_for_hypothesis(hyp_id: str) -> list:
    """All notes attached to a specific hypothesis, newest first."""
    return _run("""
        MATCH (n:ResearchNote {hyp_id: $hid})
        RETURN n.note_id    AS note_id,
               n.title      AS title,
               n.content    AS content,
               n.note_type  AS note_type,
               n.source     AS source,
               n.tags       AS tags,
               n.created_at AS created_at,
               n.updated_at AS updated_at,
               n.from_bm_name AS from_bm,
               n.to_bm_name   AS to_bm
        ORDER BY n.created_at DESC
    """, hid=hyp_id)


def get_related_notes(vector_id: str, exclude_hyp_id: str = None) -> list:
    """
    Notes on other hypotheses that share the same TransformationVector.
    Used to surface prior thinking when the same transition reappears.
    """
    if not vector_id:
        return []
    rows = _run("""
        MATCH (n:ResearchNote)-[:REFERENCES_VECTOR]->(v:TransformationVector {vector_id: $vid})
        WHERE n.hyp_id <> $excl
        OPTIONAL MATCH (h:DisruptionHypothesis {hypothesis_id: n.hyp_id})
        RETURN n.note_id    AS note_id,
               n.title      AS title,
               n.content    AS content,
               n.note_type  AS note_type,
               n.source     AS source,
               n.created_at AS created_at,
               n.hyp_id     AS hyp_id,
               n.hyp_title  AS hyp_title,
               n.hyp_status_at AS hyp_status_at,
               coalesce(h.status, n.hyp_status_at) AS current_hyp_status
        ORDER BY n.created_at DESC
        LIMIT 20
    """, vid=vector_id, excl=exclude_hyp_id or "")
    return rows


def update_note(note_id: str, title: str = None, content: str = None, tags: list = None) -> None:
    """Update a note's editable fields."""
    now = datetime.now(timezone.utc).isoformat()
    sets = ["n.updated_at = $now"]
    params = {"note_id": note_id, "now": now}
    if title is not None:
        sets.append("n.title = $title")
        params["title"] = title
    if content is not None:
        sets.append("n.content = $content")
        params["content"] = content
    if tags is not None:
        sets.append("n.tags = $tags")
        params["tags"] = tags
    _run(f"MATCH (n:ResearchNote {{note_id: $note_id}}) SET {', '.join(sets)}", **params)


def delete_note(note_id: str) -> None:
    """Permanently delete a note."""
    _run("MATCH (n:ResearchNote {note_id: $nid}) DETACH DELETE n", nid=note_id)


def search_notes(query: str, limit: int = 30) -> list:
    """Full-text search across all notes by title, content, or tags."""
    q = query.lower()
    return _run("""
        MATCH (n:ResearchNote)
        WHERE toLower(n.title)   CONTAINS $q
           OR toLower(n.content) CONTAINS $q
           OR any(tag IN n.tags WHERE toLower(tag) CONTAINS $q)
        OPTIONAL MATCH (h:DisruptionHypothesis {hypothesis_id: n.hyp_id})
        RETURN n.note_id    AS note_id,
               n.title      AS title,
               n.content    AS content,
               n.note_type  AS note_type,
               n.source     AS source,
               n.created_at AS created_at,
               n.hyp_id     AS hyp_id,
               n.hyp_title  AS hyp_title,
               n.from_bm_name AS from_bm,
               n.to_bm_name   AS to_bm,
               n.tags         AS tags,
               coalesce(h.status, n.hyp_status_at) AS hyp_status
        ORDER BY n.created_at DESC
        LIMIT $limit
    """, q=q, limit=limit)


def get_all_notes(limit: int = 200, note_type: str = None, source: str = None) -> list:
    """All notes across all hypotheses, for the global Notebook page."""
    filters = []
    params = {"limit": limit}
    if note_type:
        filters.append("n.note_type = $note_type")
        params["note_type"] = note_type
    if source:
        filters.append("n.source = $source")
        params["source"] = source
    where = ("WHERE " + " AND ".join(filters)) if filters else ""
    return _run(f"""
        MATCH (n:ResearchNote)
        {where}
        OPTIONAL MATCH (h:DisruptionHypothesis {{hypothesis_id: n.hyp_id}})
        RETURN n.note_id    AS note_id,
               n.title      AS title,
               n.content    AS content,
               n.note_type  AS note_type,
               n.source     AS source,
               n.created_at AS created_at,
               n.hyp_id     AS hyp_id,
               n.hyp_title  AS hyp_title,
               n.from_bm_name AS from_bm,
               n.to_bm_name   AS to_bm,
               n.tags         AS tags,
               coalesce(h.status, n.hyp_status_at) AS hyp_status
        ORDER BY n.created_at DESC
        LIMIT $limit
    """, **params)


def get_note(note_id: str) -> dict:
    """Fetch a single note by ID."""
    rows = _run("""
        MATCH (n:ResearchNote {note_id: $nid})
        RETURN n.note_id    AS note_id,
               n.title      AS title,
               n.content    AS content,
               n.note_type  AS note_type,
               n.source     AS source,
               n.created_at AS created_at,
               n.updated_at AS updated_at,
               n.hyp_id     AS hyp_id,
               n.hyp_title  AS hyp_title,
               n.from_bm_name AS from_bm,
               n.to_bm_name   AS to_bm,
               n.tags         AS tags,
               n.vector_id    AS vector_id
    """, nid=note_id)
    return rows[0] if rows else {}
