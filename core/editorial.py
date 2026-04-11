"""
core/editorial.py — Editorial layer for prompt and logic management.

Responsibilities:
  - Read / write prompt .txt files in /prompts/
  - Read / write config/logic_config.json
  - Append / read data/editorial_changelog.jsonl
  - Detect drift between current file state and last-recorded changelog values
    (called once per app session on startup)

No Streamlit imports — pure Python, fully testable independently.
"""

import hashlib
import json
import os
from datetime import datetime, timezone
from typing import Any

# ── Path resolution ────────────────────────────────────────────────────────────
REPO_ROOT      = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROMPTS_DIR    = os.path.join(REPO_ROOT, "prompts")
CONFIG_PATH    = os.path.join(REPO_ROOT, "config", "logic_config.json")
# SHARED_DATA_PATH env var points to a shared Google Drive folder (or similar).
# Falls back to the local ./data/ directory when not set.
_DATA_ROOT     = os.getenv("SHARED_DATA_PATH", os.path.join(REPO_ROOT, "data"))
CHANGELOG_PATH = os.path.join(_DATA_ROOT, "editorial_changelog.jsonl")


# ── Prompt registry ───────────────────────────────────────────────────────────
# Single source of truth: prompt filename → metadata shown in the UI.
PROMPT_REGISTRY = [
    {
        "id":          "hypothesis_generation",
        "filename":    "hypothesis_generation.txt",
        "name":        "Hypothesis Generation",
        "description": "Synthesises DisruptionHypothesis nodes from a technology's scalar fingerprint and the vectors it activates. Returns JSON with title, thesis, conviction_score, disruption_type, time_horizon, counter_argument.",
        "stage":       "extraction",
        "used_in":     "extraction/hypothesis_generator.py",
    },
    {
        "id":          "scalar_classification",
        "filename":    "scalar_classification.txt",
        "name":        "Scalar Classification",
        "description": "Classifies which of the 26 structural scalars are impacted by a business model transition and at what direction/strength. Writes IMPACTS relationships on TransformationVector nodes.",
        "stage":       "extraction",
        "used_in":     "extraction/scalar_classifier.py",
    },
    {
        "id":          "tech_scalar_classification",
        "filename":    "tech_scalar_classification.txt",
        "name":        "Tech → Scalar Classification",
        "description": "Maps how a Technology moves each scalar (direction + strength), producing MOVES_SCALAR relationships. The tech scalar fingerprint then drives vector activation scoring.",
        "stage":       "extraction",
        "used_in":     "extraction/tech_scalar_classifier.py",
    },
    {
        "id":          "vector_extraction",
        "filename":    "vector_extraction.txt",
        "name":        "Vector Extraction",
        "description": "Extracts TransformationVector candidates from raw text evidence. Identifies from/to BIM IDs, confidence score, scalars activated, and companies mentioned.",
        "stage":       "extraction",
        "used_in":     "extraction/vector_extractor.py",
    },
    {
        "id":          "company_enrichment",
        "filename":    "company_enrichment.txt",
        "name":        "Company Enrichment",
        "description": "Classifies a company's current and target business model from web search context. Returns funding_stage, current_bm_id, bm_confidence, ai_involvement, and a description.",
        "stage":       "input_layer",
        "used_in":     "input_layer/company_enrichment.py",
    },
    {
        "id":          "tech_enrichment",
        "filename":    "tech_enrichment.txt",
        "name":        "Tech Enrichment",
        "description": "Enriches a Technology node with category, maturity_level, scalar_impacts, and a disruption_thesis from web search context.",
        "stage":       "input_layer",
        "used_in":     "input_layer/tech_enrichment.py",
    },
    {
        "id":          "bm_enrichment",
        "filename":    "bm_enrichment.txt",
        "name":        "BM Enrichment",
        "description": "Enriches a new Business Model candidate with description, revenue_logic, key_dependencies, examples, and a similarity_score against existing BMs to detect duplicates.",
        "stage":       "input_layer",
        "used_in":     "input_layer/bm_enrichment.py",
    },
    {
        "id":          "bm_scanner",
        "filename":    "bm_scanner.txt",
        "name":        "BM Scanner",
        "description": "Scans internet search results for novel business model patterns not yet in the library. Returns candidates with name, description, why_novel, and similarity_to_closest.",
        "stage":       "input_layer",
        "used_in":     "input_layer/bm_scanner.py",
    },
    {
        "id":          "deep_research",
        "filename":    "deep_research.txt",
        "name":        "Deep Research",
        "description": "In-depth validation prompt for a specific vector or hypothesis. Returns supporting/refuting evidence, market signals, and validation_confidence. Not yet fully integrated into the main pipeline.",
        "stage":       "research",
        "used_in":     "N/A — not yet fully integrated",
    },
    {
        "id":          "counter_research",
        "filename":    "counter_research.txt",
        "name":        "Counter Research",
        "description": "Generates adversarial counter-arguments to an existing hypothesis. Returns counter_thesis, structural_barriers, incumbents_defending, adversarial_confidence. Not yet fully integrated.",
        "stage":       "research",
        "used_in":     "N/A — not yet fully integrated",
    },
]


# ── Prompt I/O ────────────────────────────────────────────────────────────────

def list_prompts() -> list:
    """Return registry augmented with last_file_modified from file mtime."""
    result = []
    for p in PROMPT_REGISTRY:
        path = os.path.join(PROMPTS_DIR, p["filename"])
        mtime = None
        if os.path.exists(path):
            mtime = datetime.fromtimestamp(
                os.path.getmtime(path), tz=timezone.utc
            ).strftime("%Y-%m-%d %H:%M UTC")
        result.append({**p, "last_file_modified": mtime})
    return result


def read_prompt(prompt_id: str) -> str:
    """Read the full text content of a prompt file."""
    p = _get_prompt_meta(prompt_id)
    path = os.path.join(PROMPTS_DIR, p["filename"])
    with open(path) as f:
        return f.read()


def write_prompt(prompt_id: str, new_content: str, rationale: str,
                 editor: str = "manual_ui", session_id: str = None) -> None:
    """
    Write new content to a prompt file and log the change.
    Raises ValueError if rationale is empty.
    """
    if not rationale.strip():
        raise ValueError("Rationale is required for prompt edits.")
    p = _get_prompt_meta(prompt_id)
    path = os.path.join(PROMPTS_DIR, p["filename"])
    old_content = ""
    if os.path.exists(path):
        with open(path) as f:
            old_content = f.read()
    with open(path, "w") as f:
        f.write(new_content)
    _append_changelog({
        "change_type": "prompt_edit",
        "item_id":     prompt_id,
        "item_name":   p["name"],
        "field":       "content",
        "old_value":   old_content,
        "new_value":   new_content,
        "rationale":   rationale.strip(),
        "source":      editor,
        "editor":      editor,
        "session_id":  session_id,
    })


def file_hash(prompt_id: str) -> str:
    """SHA-256 hex digest of a prompt file's current content."""
    p = _get_prompt_meta(prompt_id)
    path = os.path.join(PROMPTS_DIR, p["filename"])
    if not os.path.exists(path):
        return ""
    with open(path, "rb") as f:
        return hashlib.sha256(f.read()).hexdigest()


# ── Logic config I/O ──────────────────────────────────────────────────────────

def load_logic_config() -> dict:
    """
    Load and return the full logic_config.json.
    Returns {} if the file does not exist yet.
    This function is also imported by pipeline files via get_constant().
    """
    if not os.path.exists(CONFIG_PATH):
        return {}
    with open(CONFIG_PATH) as f:
        return json.load(f)


def get_constant(category: str, key: str, default: Any = None) -> Any:
    """
    Convenience helper for pipeline files.
    Returns the .value field for a given category/key, or default if missing.

    Usage in pipeline files:
        from core.editorial import get_constant
        ACTIVATION_THRESHOLD = get_constant("activation", "ACTIVATION_THRESHOLD", 0.35)
    """
    cfg = load_logic_config()
    try:
        return cfg[category][key]["value"]
    except (KeyError, TypeError):
        return default


def update_constant(category: str, key: str, new_value: Any,
                    rationale: str, editor: str = "manual_ui",
                    session_id: str = None) -> None:
    """
    Update a single constant's value in logic_config.json and log the change.
    Does not touch description / formula / used_in fields.
    Raises ValueError if rationale is empty.
    """
    if not rationale.strip():
        raise ValueError("Rationale is required for logic edits.")
    cfg = load_logic_config()
    old_value = cfg.get(category, {}).get(key, {}).get("value", None)
    cfg[category][key]["value"] = new_value
    cfg["_last_updated"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(CONFIG_PATH), exist_ok=True)
    with open(CONFIG_PATH, "w") as f:
        json.dump(cfg, f, indent=2)
    _append_changelog({
        "change_type": "logic_edit",
        "item_id":     f"{category}.{key}",
        "item_name":   key,
        "field":       "value",
        "old_value":   json.dumps(old_value),
        "new_value":   json.dumps(new_value),
        "rationale":   rationale.strip(),
        "source":      editor,
        "editor":      editor,
        "session_id":  session_id,
    })


# ── Changelog I/O ─────────────────────────────────────────────────────────────

def _append_changelog(fields: dict) -> None:
    """Internal: append a single entry to the editorial changelog."""
    entry = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        **fields,
    }
    os.makedirs(os.path.dirname(CHANGELOG_PATH), exist_ok=True)
    with open(CHANGELOG_PATH, "a") as f:
        f.write(json.dumps(entry) + "\n")


def append_changelog(fields: dict) -> None:
    """Public: append a custom entry to the editorial changelog (e.g. from agent graph writes)."""
    _append_changelog(fields)


def load_changelog(n: int = None, change_type: str = None,
                   source: str = None, item_id: str = None) -> list:
    """
    Load editorial changelog entries, newest first.
    Optional filters: change_type, source, item_id.
    n limits the result count (after filtering).
    """
    if not os.path.exists(CHANGELOG_PATH):
        return []
    entries = []
    with open(CHANGELOG_PATH) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                e = json.loads(line)
                if change_type and e.get("change_type") != change_type:
                    continue
                if source and e.get("source") != source:
                    continue
                if item_id and e.get("item_id") != item_id:
                    continue
                entries.append(e)
            except Exception:
                pass
    entries.reverse()  # newest first
    if n:
        entries = entries[:n]
    return entries


# ── Code-change drift detection ───────────────────────────────────────────────

def detect_and_log_drift() -> list:
    """
    Compare the current state of prompts and logic_config.json against the
    last-recorded values in the editorial changelog.

    On first call (no changelog entries): writes a baseline entry for every
    prompt and every logic constant (source='code_edit', editor='system').

    On subsequent calls: if any prompt file content or config value differs
    from the last logged value, writes a new 'code_edit' entry and returns
    a list of drift items so the UI can show a warning banner.

    Called once per Streamlit app session via a session_state guard.
    """
    drift = []

    # ── Prompt drift ──────────────────────────────────────────────────────────
    for p in PROMPT_REGISTRY:
        pid = p["id"]
        current_hash = file_hash(pid)

        history = load_changelog(item_id=pid, change_type="prompt_edit", n=1)

        if not history:
            # First time: log baseline
            current_content = ""
            path = os.path.join(PROMPTS_DIR, p["filename"])
            if os.path.exists(path):
                with open(path) as f:
                    current_content = f.read()
            _append_changelog({
                "change_type": "prompt_edit",
                "item_id":     pid,
                "item_name":   p["name"],
                "field":       "content",
                "old_value":   "",
                "new_value":   current_content,
                "rationale":   "Initial baseline snapshot on first Editorial page load.",
                "source":      "code_edit",
                "editor":      "system",
                "session_id":  None,
            })
        else:
            last_logged_content = history[0].get("new_value", "")
            last_hash = hashlib.sha256(last_logged_content.encode()).hexdigest()

            if current_hash and current_hash != last_hash:
                current_content = ""
                path = os.path.join(PROMPTS_DIR, p["filename"])
                if os.path.exists(path):
                    with open(path) as f:
                        current_content = f.read()
                _append_changelog({
                    "change_type": "prompt_edit",
                    "item_id":     pid,
                    "item_name":   p["name"],
                    "field":       "content",
                    "old_value":   last_logged_content,
                    "new_value":   current_content,
                    "rationale":   "File changed outside the Editorial UI (detected on startup).",
                    "source":      "code_edit",
                    "editor":      "system",
                    "session_id":  None,
                })
                drift.append({"type": "prompt", "id": pid, "name": p["name"]})

    # ── Logic drift ───────────────────────────────────────────────────────────
    cfg = load_logic_config()
    for category, constants in cfg.items():
        if category.startswith("_"):
            continue
        if not isinstance(constants, dict):
            continue
        for key, meta in constants.items():
            current_value = meta.get("value")
            item_id = f"{category}.{key}"
            history = load_changelog(item_id=item_id, change_type="logic_edit", n=1)

            if not history:
                # First time: log baseline
                _append_changelog({
                    "change_type": "logic_edit",
                    "item_id":     item_id,
                    "item_name":   key,
                    "field":       "value",
                    "old_value":   "",
                    "new_value":   json.dumps(current_value),
                    "rationale":   "Initial baseline snapshot on first Editorial page load.",
                    "source":      "code_edit",
                    "editor":      "system",
                    "session_id":  None,
                })
            else:
                try:
                    last_value = json.loads(history[0].get("new_value", "null"))
                except Exception:
                    last_value = None

                if current_value != last_value:
                    _append_changelog({
                        "change_type": "logic_edit",
                        "item_id":     item_id,
                        "item_name":   key,
                        "field":       "value",
                        "old_value":   history[0].get("new_value", ""),
                        "new_value":   json.dumps(current_value),
                        "rationale":   "Value changed in logic_config.json outside the Editorial UI.",
                        "source":      "code_edit",
                        "editor":      "system",
                        "session_id":  None,
                    })
                    drift.append({"type": "logic", "id": item_id, "name": key})

    return drift


# ── Private helpers ────────────────────────────────────────────────────────────

def _get_prompt_meta(prompt_id: str) -> dict:
    for p in PROMPT_REGISTRY:
        if p["id"] == prompt_id:
            return p
    raise KeyError(f"Unknown prompt_id: {prompt_id!r}")
