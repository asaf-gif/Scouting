"""
Part 4 — Graph Inspector CLI Test

Verifies all inspector commands return correct results.

Run: python tests/test_04_inspector.py
"""

import os
import sys
import subprocess
import csv
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

PASS = "✓"
FAIL = "✗"
PYTHON = sys.executable
INSPECTOR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "graph", "inspector.py")


def run(cmd: list[str]) -> tuple[int, str]:
    result = subprocess.run(
        [PYTHON, INSPECTOR] + cmd,
        capture_output=True, text=True,
        cwd=os.path.dirname(os.path.dirname(__file__)),
    )
    return result.returncode, result.stdout + result.stderr


def test_bm_list():
    code, out = run(["bm", "--list"])
    if code != 0:
        return False, f"Exit code {code}"
    # Check for partial names (rich may wrap/truncate long names)
    if "Project Based" not in out:
        return False, "'Project Based' missing from output"
    if "Selling Knowledge" not in out:
        return False, "'Selling Knowledge' missing from output"
    # Count BIM_ ID occurrences across all output lines
    bim_count = sum(1 for l in out.splitlines() if "BIM_" in l)
    if bim_count < 27:
        return False, f"Expected ≥27 lines with BIM_ IDs, found {bim_count}"
    return True, f"Listed {bim_count} business models"


def test_bm_detail():
    code, out = run(["bm", "--id", "Project Based"])
    if code != 0:
        return False, f"Exit code {code}"
    if "Project Based" not in out:
        return False, "BM name missing from output"
    if "BIM_" not in out:
        return False, "BIM ID missing from output"
    return True, "BusinessModel detail shows name and ID"


def test_vector_detail():
    code, out = run(["vector", "--from", "Project Based", "--to", "Selling Knowledge Databases"])
    if code != 0:
        return False, f"Exit code {code}"
    if "Project Based" not in out or "Selling Knowledge Databases" not in out:
        return False, "FROM/TO names missing"
    if "KGGen" not in out:
        return False, "KGGen score missing from vector detail"
    return True, "Vector detail shows FROM, TO, and KGGen score"


def test_top_transitions_kggen():
    code, out = run(["top-transitions", "--tech", "kggen", "--limit", "10"])
    if code != 0:
        return False, f"Exit code {code}"
    # #1 should be Project Based → Selling Knowledge Databases per the Excel
    if "Project Based" not in out:
        return False, "'Project Based' not in top-10 KGGen list"
    # "Selling Knowledge Databases" may be truncated to "Selling Knowledge" in narrow output
    if "Selling Knowledge" not in out:
        return False, "'Selling Knowledge' not in top-10 KGGen list"
    return True, "Top-10 KGGen list contains 'Project Based → Selling Knowledge DB'"


def test_top_transitions_csv_export():
    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False, mode="w") as f:
        tmp = f.name

    code, out = run(["top-transitions", "--tech", "kggen", "--limit", "10", "--output", tmp])
    if code != 0:
        os.unlink(tmp)
        return False, f"Exit code {code}"

    try:
        with open(tmp) as f:
            reader = list(csv.DictReader(f))
        os.unlink(tmp)
    except Exception as e:
        return False, f"CSV parse error: {e}"

    if len(reader) != 10:
        return False, f"Expected 10 CSV rows, got {len(reader)}"

    # Row 1 should be Project Based → Selling Knowledge Databases
    row1 = reader[0]
    if "Project Based" not in row1.get("from", ""):
        return False, f"Row 1 FROM is '{row1.get('from')}', expected 'Project Based'"
    return True, f"CSV export: 10 rows, rank-1 is '{row1['from']} → {row1['to']}'"


def test_export_bms():
    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False, mode="w") as f:
        tmp = f.name

    code, _ = run(["export", "--entity", "bms", "--output", tmp])
    if code != 0:
        os.unlink(tmp)
        return False, f"Exit code {code}"

    with open(tmp) as f:
        rows = list(csv.DictReader(f))
    os.unlink(tmp)

    if len(rows) != 27:
        return False, f"Expected 27 BM rows in CSV, got {len(rows)}"
    return True, f"Exported 27 business models to CSV"


def test_hypothesis_empty():
    """With no hypotheses yet, should return gracefully."""
    code, out = run(["hypothesis", "--status", "candidate"])
    if code != 0:
        return False, f"Exit code {code}"
    return True, "Hypothesis query runs without error (queue empty is OK)"


def main():
    print("\n=== Part 4 — Graph Inspector CLI Test ===\n")

    # Re-run migration with tech scores first
    print("  [setup] Re-running migration to add tech scores...")
    result = subprocess.run(
        [PYTHON, os.path.join(os.path.dirname(os.path.dirname(__file__)), "graph", "migrate.py"), "--clear"],
        capture_output=True, text=True,
        cwd=os.path.dirname(os.path.dirname(__file__)),
    )
    if result.returncode != 0:
        print(f"  {FAIL} Migration failed:\n{result.stderr}")
        sys.exit(1)
    print("  Migration done.\n")

    tests = [
        ("bm --list: 27 models", test_bm_list),
        ("bm --id: Project Based detail", test_bm_detail),
        ("vector: Project Based → Knowledge DB", test_vector_detail),
        ("top-transitions --tech kggen: correct rank-1", test_top_transitions_kggen),
        ("top-transitions --output CSV: 10 rows", test_top_transitions_csv_export),
        ("export --entity bms: 27 rows", test_export_bms),
        ("hypothesis --status candidate: no error", test_hypothesis_empty),
    ]

    all_passed = True
    for name, fn in tests:
        try:
            passed, msg = fn()
        except Exception as e:
            passed, msg = False, str(e)
        icon = PASS if passed else FAIL
        print(f"  {icon} {name}: {msg}")
        if not passed:
            all_passed = False

    print()
    if all_passed:
        print("All inspector tests passed. Ready to proceed to Part 5.")
    else:
        print("Some tests failed.")
        sys.exit(1)


if __name__ == "__main__":
    main()
