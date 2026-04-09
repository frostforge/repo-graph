from pathlib import Path
from typing import Any, Dict, List

TYPE_LABELS = {
    "service": "Service",
    "dbt_model": "dbt model",
    "dbt_source": "dbt source",
    "s3_path": "S3 path",
    "sqs_queue": "SQS queue",
    "snowflake_table": "Snowflake table",
    "contract": "Contract",
}

# Sort order for GRAPH_INDEX
TYPE_ORDER = ["service", "contract", "dbt_model", "dbt_source", "s3_path", "sqs_queue", "snowflake_table"]


def render(graph: Dict[str, Any], output_dir: Path) -> None:
    """Write all graph markdown files to output_dir."""
    output_dir.mkdir(parents=True, exist_ok=True)
    nodes = graph["nodes"]
    edges = graph["edges"]
    _write_index(nodes, edges, output_dir)
    _write_services(nodes, edges, output_dir)
    _write_dbt(nodes, edges, output_dir)
    _write_s3(nodes, edges, output_dir)
    _write_contracts(nodes, edges, output_dir)


# ---------------------------------------------------------------------------
# GRAPH_INDEX.md
# ---------------------------------------------------------------------------
def _write_index(nodes: dict, edges: list, out: Path) -> None:
    def sort_key(nid):
        nd = nodes[nid]
        t = nd.get("type", "")
        order = TYPE_ORDER.index(t) if t in TYPE_ORDER else 99
        return (order, nid)

    lines = [
        "# Graph Index",
        "",
        "Full node inventory. Load service-specific files as needed.",
        "",
        "| Node ID | Type | Repo | Description |",
        "|---------|------|------|-------------|",
    ]
    for nid in sorted(nodes, key=sort_key):
        nd = nodes[nid]
        label = TYPE_LABELS.get(nd.get("type", ""), nd.get("type", ""))
        desc = (nd.get("description") or "")[:80]
        lines.append(f"| `{nid}` | {label} | {nd.get('repo', '')} | {desc} |")

    lines += [
        "",
        f"**Total:** {len(nodes)} nodes, {len(edges)} edges",
        "",
        "## Files in this directory",
        "",
        "| File | Contents | Load when |",
        "|------|----------|-----------|",
        "| `GRAPH_INDEX.md` | This file — full node table | Every session |",
        "| `GRAPH_SERVICES.md` | Per-service reads/writes/queues | Session involves a specific service |",
        "| `GRAPH_DBT.md` | dbt models, sources, tenant-variant flags | Session involves dbt or Snowflake |",
        "| `GRAPH_S3.md` | S3 paths, writers, readers, contracts | Session involves ingestion pipelines |",
        "| `GRAPH_CONTRACTS.md` | Contract nodes cross-referenced to artifacts | Session involves interface changes |",
    ]

    (out / "GRAPH_INDEX.md").write_text("\n".join(lines), encoding="utf-8")


# ---------------------------------------------------------------------------
# GRAPH_SERVICES.md
# ---------------------------------------------------------------------------
def _write_services(nodes: dict, edges: list, out: Path) -> None:
    services = {nid: nd for nid, nd in nodes.items() if nd.get("type") == "service"}
    lines = ["# Service Graph", ""]

    for svc_id in sorted(services):
        svc = services[svc_id]
        repo = svc.get("repo", svc_id)
        lines += [f"## {repo}", ""]

        out_edges = [e for e in edges if e["from"] == svc_id]
        writes = sorted({e["to"] for e in out_edges if e["type"] == "writes_to"})
        reads = sorted({e["to"] for e in out_edges if e["type"] == "reads_from"})
        queues = sorted({t for t in reads if t.startswith("sqs_queue:")})
        reads = [t for t in reads if not t.startswith("sqs_queue:")]

        if writes:
            lines.append("**Writes to:**")
            lines.extend(f"- `{t}`" for t in writes)
        if reads:
            lines.append("**Reads from:**")
            lines.extend(f"- `{t}`" for t in reads)
        if queues:
            lines.append("**Queues:**")
            lines.extend(f"- `{t}`" for t in queues)
        if not writes and not reads and not queues:
            lines.append("_(no extracted dependencies)_")
        lines.append("")

    (out / "GRAPH_SERVICES.md").write_text("\n".join(lines), encoding="utf-8")


# ---------------------------------------------------------------------------
# GRAPH_DBT.md
# ---------------------------------------------------------------------------
def _write_dbt(nodes: dict, edges: list, out: Path) -> None:
    dbt_models = {nid: nd for nid, nd in nodes.items() if nd.get("type") == "dbt_model"}
    dbt_sources = {nid: nd for nid, nd in nodes.items() if nd.get("type") == "dbt_source"}

    lines = ["# dbt Graph", ""]

    # Sources table
    lines += ["## Sources", "", "| Source ID | Schema | Description |", "|-----------|--------|-------------|"]
    for sid in sorted(dbt_sources):
        sd = dbt_sources[sid]
        schema = sd.get("schema", "")
        desc = (sd.get("description") or "")[:70]
        lines.append(f"| `{sid}` | {schema} | {desc} |")
    lines.append("")

    # Models table
    lines += [
        "## Models",
        "",
        "| Model | File | Tenant-variant | Depends on |",
        "|-------|------|----------------|------------|",
    ]
    for mid in sorted(dbt_models):
        md = dbt_models[mid]
        deps = sorted({e["to"] for e in edges if e["from"] == mid and e["type"] == "depends_on"})
        dep_str = ", ".join(f"`{d}`" for d in deps) if deps else "—"
        tv = "yes" if md.get("tenant_variant") else ""
        file_path = md.get("file", "")
        lines.append(f"| `{mid}` | {file_path} | {tv} | {dep_str} |")
    lines.append("")

    (out / "GRAPH_DBT.md").write_text("\n".join(lines), encoding="utf-8")


# ---------------------------------------------------------------------------
# GRAPH_S3.md
# ---------------------------------------------------------------------------
def _write_s3(nodes: dict, edges: list, out: Path) -> None:
    s3_nodes = {nid: nd for nid, nd in nodes.items() if nd.get("type") == "s3_path"}
    lines = [
        "# S3 Path Graph",
        "",
        "| S3 Path | Writers | Readers | Contract |",
        "|---------|---------|---------|----------|",
    ]

    for s3_id in sorted(s3_nodes):
        writers = sorted({e["from"] for e in edges if e["to"] == s3_id and e["type"] == "writes_to"})
        readers = sorted({e["from"] for e in edges if e["to"] == s3_id and e["type"] == "reads_from"})
        contracts = sorted({e["from"] for e in edges if e["to"] == s3_id and e["type"] == "implements"})
        w = ", ".join(f"`{x}`" for x in writers) or "—"
        r = ", ".join(f"`{x}`" for x in readers) or "—"
        c = ", ".join(f"`{x}`" for x in contracts) or "—"
        lines.append(f"| `{s3_id}` | {w} | {r} | {c} |")
    lines.append("")

    (out / "GRAPH_S3.md").write_text("\n".join(lines), encoding="utf-8")


# ---------------------------------------------------------------------------
# GRAPH_CONTRACTS.md
# ---------------------------------------------------------------------------
def _write_contracts(nodes: dict, edges: list, out: Path) -> None:
    contract_nodes = {nid: nd for nid, nd in nodes.items() if nd.get("type") == "contract"}
    lines = ["# Contracts Graph", ""]

    for cid in sorted(contract_nodes):
        cd = contract_nodes[cid]
        lines += [f"## {cd.get('description', cid)}", f"Source: `{cd.get('file', '')}`", ""]

        artifacts = [(e["to"], e["type"]) for e in edges if e["from"] == cid]
        if artifacts:
            lines += ["| Artifact | Relationship |", "|----------|-------------|"]
            for art, etype in sorted(artifacts):
                lines.append(f"| `{art}` | {etype} |")
        else:
            lines.append("_(no cross-references extracted)_")

        implementors = sorted({e["from"] for e in edges if e["to"] == cid and e["type"] == "implements"})
        if implementors:
            lines += ["", "**Implemented by:**"]
            lines.extend(f"- `{imp}`" for imp in implementors)

        lines.append("")

    (out / "GRAPH_CONTRACTS.md").write_text("\n".join(lines), encoding="utf-8")
