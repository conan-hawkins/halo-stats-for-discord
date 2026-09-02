"""Guards the package layering that the src/jobs extraction established.

src/database used to import src/api from five of its backfill scripts while
src/api/client.py imported src.database.cache, so the two packages depended on
each other. Nothing broke, because the rate_limiters imports were function-local
and so never circular at import time, but it meant neither package could be read
or reasoned about on its own.

Moving the hand-run scripts to src/jobs removed the back edge. These tests are
what stop it coming back the next time a backfill is written in a hurry: the
allowed direction is jobs -> api -> database -> config, and a new import that
points the wrong way fails here rather than silently re-tangling the packages.
"""
import ast
from pathlib import Path

import pytest

SRC = Path(__file__).resolve().parent.parent / "src"


def _imported_packages(path):
    """Return the src.<package> names a module imports, function-local included."""
    # utf-8-sig, not utf-8: src/api/client.py starts with a BOM. Python's own
    # tokenizer strips it on import, but ast.parse rejects it as a non-printable.
    tree = ast.parse(path.read_text(encoding="utf-8-sig"), filename=str(path))
    found = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            module = node.module or ""
        elif isinstance(node, ast.Import):
            module = node.names[0].name
        else:
            continue
        parts = module.split(".")
        if len(parts) >= 2 and parts[0] == "src":
            found.add(parts[1])
    return found


def _modules_in(package):
    return sorted((SRC / package).rglob("*.py"))


@pytest.mark.parametrize("path", _modules_in("database"), ids=lambda p: p.name)
def test_database_does_not_import_api(path):
    """src/database is a leaf: it may know about config, never about api.

    This is the edge that made the graph cyclic. A backfill that needs the Halo
    API belongs in src/jobs, which is allowed to import both.
    """
    assert "api" not in _imported_packages(path), (
        f"{path.name} imports src.api, which puts the api/database cycle back. "
        "A script that needs both belongs in src/jobs/."
    )


@pytest.mark.parametrize("package", ["database", "api", "auth", "config", "graph", "web", "bot"])
def test_nothing_imports_jobs(package):
    """src/jobs holds entry points, not a library surface.

    Every module there is run by hand via `python -m src.jobs.<name>`. If the
    running bot ever needs one of these code paths, the shared part should move
    into src/database or src/api rather than the bot importing a backfill.
    """
    offenders = [p.name for p in _modules_in(package) if "jobs" in _imported_packages(p)]
    assert not offenders, (
        f"{package} imports src.jobs from: {', '.join(offenders)}. "
        "Jobs are hand-run scripts; promote the shared logic instead."
    )


def test_jobs_package_exports_nothing():
    """An empty __init__ is what keeps jobs from drifting back into a library.

    Re-exporting a backfill here would make `from src.jobs import x` cheap, and
    the next step after that is the bot doing it.
    """
    tree = ast.parse((SRC / "jobs" / "__init__.py").read_text(encoding="utf-8-sig"))
    statements = [n for n in tree.body if not isinstance(n, ast.Expr)]
    assert not statements, "src/jobs/__init__.py should stay a docstring only."
