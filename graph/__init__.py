"""
Cross-repo knowledge graph generator for Harmonate.

Usage:
    python -m graph                    # full rebuild → shared-context/graph/
    python -m graph --repo pgp_service # single repo only
    python -m graph --dry-run          # print graph to stdout
    python -m graph --validate         # check config paths
"""
from .cli import main

__all__ = ["main"]
