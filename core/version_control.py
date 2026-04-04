"""
core/version_control.py — Git-based code version control

Auto-commits Python files after successful pipeline runs.
Provides commit history and code restore for the UI.

Requires: GitPython 3.1.46 (already in venv)
Requires: git repository initialised at project root (done via `git init`)
"""

import os
from datetime import datetime, timezone

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _get_repo():
    """Return a GitPython Repo object, or None if not initialised."""
    try:
        import git
        return git.Repo(REPO_ROOT)
    except Exception:
        return None


def git_commit_if_changed(message: str = None) -> dict:
    """
    Stage all *.py and *.txt files and commit if anything has changed.
    Never commits .env, data/, venv/, or __pycache__.

    Returns:
        {"committed": bool, "sha": str, "message": str, "error": str|None}
    """
    repo = _get_repo()
    if repo is None:
        return {"committed": False, "sha": "", "message": "", "error": "No git repo found"}

    try:
        # Stage only tracked file types (gitignore handles exclusions)
        repo.git.add("--all", "--", "*.py", "*.txt", "*.md", "*.sh", "requirements.txt")

        if not repo.index.diff("HEAD"):
            return {"committed": False, "sha": "", "message": "No changes to commit", "error": None}

        if not message:
            message = f"Auto-commit {datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M')}Z"

        commit = repo.index.commit(message)
        return {
            "committed": True,
            "sha":       commit.hexsha[:8],
            "message":   message,
            "error":     None,
        }
    except Exception as e:
        return {"committed": False, "sha": "", "message": "", "error": str(e)}


def get_recent_commits(n: int = 15) -> list:
    """
    Return the last n commits as a list of dicts.
    Returns [] if no repo or no commits.
    """
    repo = _get_repo()
    if repo is None:
        return []
    try:
        commits = []
        for c in repo.iter_commits(max_count=n):
            commits.append({
                "sha":       c.hexsha[:8],
                "full_sha":  c.hexsha,
                "message":   c.message.strip()[:120],
                "timestamp": datetime.fromtimestamp(
                    c.committed_date, tz=timezone.utc
                ).strftime("%Y-%m-%d %H:%M UTC"),
                "author":    c.author.name or "",
            })
        return commits
    except Exception:
        return []


def git_restore(sha: str) -> dict:
    """
    Restore all *.py files to the state at a given commit SHA.
    This does NOT affect data files, .env, or anything in .gitignore.

    Returns:
        {"restored": bool, "sha": str, "error": str|None}
    """
    repo = _get_repo()
    if repo is None:
        return {"restored": False, "sha": sha, "error": "No git repo found"}

    try:
        # Validate SHA exists
        commit = repo.commit(sha)
        # Checkout only .py files from that commit
        repo.git.checkout(sha, "--", "*.py")
        return {"restored": True, "sha": sha[:8], "error": None}
    except Exception as e:
        return {"restored": False, "sha": sha, "error": str(e)}


def current_sha() -> str:
    """Return the current HEAD commit SHA (short), or 'unknown'."""
    repo = _get_repo()
    if repo is None:
        return "unknown"
    try:
        return repo.head.commit.hexsha[:8]
    except Exception:
        return "unknown"
