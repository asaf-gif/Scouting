"""
graph/migrate_utils.py — Helpers for reading Excel sheets

Shared parsing utilities used by migrate.py and future migration scripts.
"""

import re

# Unicode codepoints — defined explicitly so they always match Excel values
_UP   = "\u2191"   # ↑  UPWARDS ARROW
_DOWN = "\u2193"   # ↓  DOWNWARDS ARROW
_DASH = "\u2014"   # —  EM DASH

# Maps impact notation symbols → numeric scores (+2 to -2)
IMPACT_MAP = {
    _UP + _UP: 2,
    _UP:       1,
    "~":       0,
    _DASH:     0,
    "-":       0,
    "":        0,
    "None":    0,
    _DOWN:    -1,
    _DOWN + _DOWN: -2,
}

IMPACT_LABEL = {
    2:  "High",
    1:  "Medium",
    0:  "Neutral",
    -1: "Low negative",
    -2: "High negative",
}


def parse_impact(raw) -> tuple[int, str]:
    """Convert an Excel cell value to (score, label). Returns (0, 'Neutral') if unrecognised.

    Handles cells that contain just a symbol ('↑↑') as well as cells where
    the symbol is followed by additional description text ('↑↑ KGGen enables...').
    Extraction order: try exact match first, then extract leading symbol characters.
    """
    if raw is None:
        return 0, "Neutral"
    val = str(raw).strip()

    # 1. Exact match (fast path — symbol-only cells)
    if val in IMPACT_MAP:
        score = IMPACT_MAP[val]
        return score, IMPACT_LABEL[score]

    # 2. Extract leading symbol — handles cells with trailing description text
    # Match one or two arrows, tilde, em-dash, or hyphen at the start
    pattern = rf"^({re.escape(_UP*2)}|{re.escape(_UP)}|{re.escape(_DOWN*2)}|{re.escape(_DOWN)}|~|{re.escape(_DASH)}|-)"
    m = re.match(pattern, val)
    if m:
        score = IMPACT_MAP.get(m.group(1), 0)
        return score, IMPACT_LABEL[score]

    return 0, "Neutral"


def is_scalar_id(value) -> bool:
    """Return True if value looks like a scalar ID: one letter followed by digits (e.g. A1, E7)."""
    if not value or not isinstance(value, str):
        return False
    v = value.strip()
    return bool(re.match(r"^[A-Za-z]\d+$", v))


def is_section_header(value) -> bool:
    """Return True if the cell looks like a section header (all-caps string with no digits)."""
    if not value or not isinstance(value, str):
        return False
    v = value.strip()
    return v == v.upper() and len(v) > 3 and not any(c.isdigit() for c in v)


def bim_id_from_index(i: int) -> str:
    """Generate BIM_001, BIM_002, ... from a 1-based index."""
    return f"BIM_{i:03d}"


def scalar_id_from_code(code: str) -> str:
    """Convert a raw scalar code (e.g. 'A1') to a stable ID (e.g. 'SCL_A1')."""
    return f"SCL_{code.strip().upper()}"


def tech_id_from_index(i: int) -> str:
    """Generate TECH_001, TECH_002, ... from a 1-based index."""
    return f"TECH_{i:03d}"
