"""
core/error_log.py — Persistent structured error logging

Every error in the system writes a JSONL record to data/error_log.jsonl.
This survives terminal restarts and is surfaced in the Pipeline Monitor UI.

Usage:
    from core.error_log import log_error, capture_errors

    # Direct call
    except Exception as e:
        log_error("extraction.vector_extractor", "extract_from_url", e,
                  context={"url": url})

    # Decorator — logs and re-raises automatically
    @capture_errors(context_keys=["from_bim_id", "to_bim_id"])
    def classify_vector_scalars(from_bim_id, to_bim_id, ...):
        ...
"""

import os
import json
import traceback
import functools
from datetime import datetime, timezone

try:
    from rich.console import Console
    _console = Console(width=200)
except ImportError:
    _console = None

LOG_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data", "error_log.jsonl"
)


def log_error(module: str, func: str, error: Exception,
              context: dict = None) -> dict:
    """
    Write a structured error record to data/error_log.jsonl.
    Also prints to terminal (red) so existing console output is unchanged.

    Returns the record dict.
    """
    record = {
        "timestamp":  datetime.now(timezone.utc).isoformat(),
        "module":     module,
        "function":   func,
        "error_type": type(error).__name__,
        "message":    str(error),
        "traceback":  traceback.format_exc(),
        "context":    context or {},
    }

    # Write to file
    try:
        os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
        with open(LOG_PATH, "a") as f:
            f.write(json.dumps(record) + "\n")
    except Exception:
        pass  # Never let logging crash the caller

    # Also print to terminal
    ctx_str = f" context={context}" if context else ""
    msg = f"  [red][{module}.{func}] {type(error).__name__}: {error}{ctx_str}[/red]"
    if _console:
        _console.print(msg)

    return record


def capture_errors(context_keys: list = None):
    """
    Decorator factory. Wraps a function: logs any Exception then re-raises it.

    context_keys: list of argument names whose values to include in the log context.

    Example:
        @capture_errors(["from_bim_id", "to_bim_id"])
        def my_func(from_bim_id, to_bim_id, other_arg):
            ...
    """
    if context_keys is None:
        context_keys = []

    def decorator(fn):
        module = fn.__module__ or "unknown"
        func   = fn.__qualname__

        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            try:
                return fn(*args, **kwargs)
            except Exception as e:
                # Build context from named arguments
                import inspect
                sig    = inspect.signature(fn)
                params = list(sig.parameters.keys())
                ctx    = {}
                for key in context_keys:
                    if key in kwargs:
                        ctx[key] = str(kwargs[key])[:200]
                    elif key in params:
                        idx = params.index(key)
                        if idx < len(args):
                            ctx[key] = str(args[idx])[:200]
                log_error(module, func, e, context=ctx)
                raise

        return wrapper
    return decorator


def read_recent(n: int = 50) -> list:
    """Return the last n error records from the log file, newest first."""
    if not os.path.exists(LOG_PATH):
        return []
    records = []
    try:
        with open(LOG_PATH) as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        records.append(json.loads(line))
                    except Exception:
                        pass
    except Exception:
        return []
    return list(reversed(records[-n:]))


def clear_log():
    """Truncate the error log file."""
    try:
        open(LOG_PATH, "w").close()
    except Exception:
        pass


def error_count_last_24h() -> int:
    """Return count of errors logged in the last 24 hours."""
    from datetime import timedelta
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
    records = read_recent(500)
    return sum(1 for r in records if r.get("timestamp", "") >= cutoff)
