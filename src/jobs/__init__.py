"""One-off and hand-run jobs: backfills, migrations and reconstructions.

Nothing here is imported by the running bot. Each module is a script with its
own ``__main__`` block, invoked by hand against the live database:

    python -m src.jobs.<module> --help

Deliberately empty of re-exports. These are entry points, not a library
surface, and keeping ``__init__`` bare is what stops them drifting back into
being imported as one. Jobs may depend on both ``src.database`` and
``src.api``; neither of those may depend on this package (see
tests/test_layering.py).
"""
