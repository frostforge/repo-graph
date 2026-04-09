import argparse
import pprint
import sys
from pathlib import Path

from .config import load_config
from .extractors.dbt import DbtExtractor
from .extractors.python import PythonExtractor
from .extractors.manifest import ManifestExtractor
from .graph import build_graph
from .render import render

EXTRACTOR_MAP = {
    "dbt": DbtExtractor,
    "python": PythonExtractor,
}


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(
        description="Cross-repo knowledge graph generator for Harmonate."
    )
    parser.add_argument("--repo", metavar="NAME",
                        help="Rebuild only this repo (by name in config.json)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print graph to stdout instead of writing files")
    parser.add_argument("--validate", action="store_true",
                        help="Validate config paths only, then exit")
    parser.add_argument("--config", metavar="PATH",
                        help="Path to config.json (default: repo-graph/config.json)")
    args = parser.parse_args(argv)

    config = load_config(args.config)

    if args.validate:
        _validate(config)
        return

    repos = [r for r in config["repos"] if r.get("enabled", True)]
    if args.repo:
        repos = [r for r in repos if r["name"] == args.repo]
        if not repos:
            print(f"ERROR: repo '{args.repo}' not found or not enabled in config", file=sys.stderr)
            sys.exit(1)

    results = []
    for repo in repos:
        repo_type = repo.get("type", "")
        extractor_cls = EXTRACTOR_MAP.get(repo_type)
        if not extractor_cls:
            print(f"WARNING: no extractor for type '{repo_type}' (repo: {repo['name']}) — skipping",
                  file=sys.stderr)
            continue
        repo_path = Path(repo["path"])
        if not repo_path.exists():
            print(f"WARNING: path not found: {repo_path} (repo: {repo['name']}) — skipping",
                  file=sys.stderr)
            continue
        print(f"  extracting {repo['name']} ({repo_type}) ...")
        results.append(extractor_cls(repo).extract())

    # Always run manifest extractor against shared-context
    shared_ctx = config.get("shared_context_path", "C:/git/shared-context")
    manifest_repo_cfg = {"name": "shared-context", "path": shared_ctx}
    print(f"  extracting contracts from {shared_ctx} ...")
    results.append(ManifestExtractor(manifest_repo_cfg, shared_ctx).extract())

    graph = build_graph(results)
    node_count = len(graph["nodes"])
    edge_count = len(graph["edges"])

    if args.dry_run:
        pprint.pprint(graph)
        print(f"\n--- {node_count} nodes, {edge_count} edges ---", file=sys.stderr)
        return

    output_dir = Path(shared_ctx) / "graph"
    print(f"  writing to {output_dir} ...")
    render(graph, output_dir)
    print(f"Done. {node_count} nodes, {edge_count} edges -> {output_dir}")


def _validate(config: dict) -> None:
    ok = True
    print("Validating config.json paths:")
    for repo in config.get("repos", []):
        exists = Path(repo["path"]).exists()
        status = "OK     " if exists else "MISSING"
        if not exists:
            ok = False
        enabled = "" if repo.get("enabled", True) else " (disabled)"
        print(f"  [{status}] {repo['name']} ({repo['type']}) -> {repo['path']}{enabled}")

    shared = config.get("shared_context_path", "")
    exists = Path(shared).exists() if shared else False
    status = "OK     " if exists else "MISSING"
    if not exists:
        ok = False
    print(f"  [{status}] shared-context -> {shared}")

    if ok:
        print("All paths valid.")
    else:
        print("One or more paths are missing.", file=sys.stderr)
        sys.exit(1)
