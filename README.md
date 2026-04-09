# repo-graph

Cross-repo knowledge graph generator for the Harmonate workspace. Reads repos on disk via static analysis and writes a set of compact markdown files into `shared-context/graph/` so Claude Code sessions can orient themselves without scanning every file in every repo.

## Problem

Claude Code has no native understanding of relationships between repos. Each session starts cold with respect to how a Fargate service, a dbt model, an S3 path, and a Snowflake table relate to each other. The existing `shared-context/` contracts document interfaces, but not the full dependency graph. The dbt `manifest.json` is also insufficient because it reflects a single tenant instantiation and never represents the full multi-tenant package.

## What it does

Walks each registered repo, extracts cross-service relationships (S3 paths, SQS queues, Snowflake tables, dbt `ref()`/`source()` calls, shared-context contracts), assembles them into an in-memory node/edge graph, and renders five markdown files into `shared-context/graph/`:

| Output file | Contents | When to load |
|---|---|---|
| `GRAPH_INDEX.md` | All nodes: type, repo, brief description | Every session — acts as the TOC |
| `GRAPH_SERVICES.md` | Per-service: reads, writes, queues | Sessions involving a specific service |
| `GRAPH_DBT.md` | dbt model dependencies, sources, tenant-variant flags | Sessions involving dbt or Snowflake |
| `GRAPH_S3.md` | S3 paths, which services read/write each, contract linkage | Sessions involving ingestion pipelines |
| `GRAPH_CONTRACTS.md` | Contract nodes cross-referenced to implementing services | Sessions involving interface changes |

`GRAPH_INDEX.md` is kept small (a single table) so it can always be included in initial context without token pressure. The other files are loaded on demand.

## Usage

```bash
# Full rebuild
python -m graph

# Rebuild a single repo only
python -m graph --repo bai_file_parser

# Dry run (print graph to stdout, don't write files)
python -m graph --dry-run

# Validate config paths only, then exit
python -m graph --validate

# Use an alternate config file
python -m graph --config /path/to/config.json
```

## Repo structure

```
repo-graph/
├── graph/
│   ├── config.py          # repo registry loader and settings
│   ├── graph.py           # assembles unified node/edge model in memory
│   ├── render.py          # writes markdown output files
│   ├── cli.py             # entry point (also __main__.py)
│   └── extractors/
│       ├── base.py        # abstract extractor interface
│       ├── dbt.py         # ref(), source(), schema.yml parsing
│       ├── python.py      # boto3/S3 paths, SQS queues, Snowflake refs
│       └── manifest.py    # shared-context contract parsing
├── config.json            # repo registry (paths, types, enabled flags)
├── requirements.txt       # PyYAML only
└── README.md
```

## config.json

The registry is the only extensibility mechanism. Adding a new repo requires one JSON entry — no code changes.

```json
{
  "shared_context_path": "C:/git/shared-context",
  "tenant_variable_name": "v_tenant",
  "repos": [
    {
      "name": "snowflake",
      "path": "C:/git/snowflake",
      "type": "dbt",
      "enabled": true
    },
    {
      "name": "bai_file_parser",
      "path": "C:/git/bai_file_parser",
      "type": "python",
      "enabled": true
    }
  ]
}
```

Set `"enabled": false` to exclude a repo without removing it. Repo `type` is either `"dbt"` or `"python"`.

Current registered repos: `snowflake` (dbt), `bai_file_parser`, `nap_file_parser`, `pms_parser`, `pgp_service`, `chargeback-letter-service`, `api-extractor-service` (all python).

## Node and edge types

### Nodes
| Type | Examples |
|---|---|
| `dbt_model` | `stg_cash_transactions`, `fct_fund_positions` |
| `dbt_source` | `raw.wells_fargo.transactions` |
| `s3_path` | `s3://harmonate-raw/wells-fargo/statements/` |
| `sqs_queue` | `chargeback-input-queue` |
| `snowflake_table` | `HARMONATE_PRD.FINANCE.FUND_POSITIONS` |
| `service` | `bai_file_parser`, `pgp_service` |
| `contract` | S3 manifest contract, raw ingestion contract |

### Edges
| Type | Meaning |
|---|---|
| `reads_from` | service or dbt source consumes this path/table |
| `writes_to` | service produces output to this path/table |
| `depends_on` | dbt model `ref()` dependency |
| `implements` | service implements this shared-context contract |
| `tenant_variant` | relationship varies per `v_tenant` (flagged, not resolved) |

## Adding a new repo or extractor

1. Add an entry to `config.json` with `type` set to `dbt` or `python`.
2. If the repo uses a language not covered by existing extractors (e.g. Node.js), add a new extractor in `graph/extractors/` that inherits from `base.py` and register it in `cli.py`'s `EXTRACTOR_MAP`.
3. Re-run `python -m graph`. Output files are fully regenerated on each run.

No changes to `render.py`, `graph.py`, or the CLI are needed for new repos of existing types.

## Constraints

- **No dbt runtime.** Static analysis only — no `dbt compile`, no live profile.
- **No graph libraries.** Dict-based in-memory model, markdown output. Dependency is PyYAML only.
- **No LLM calls.** All extraction is deterministic regex + YAML parsing. Output is stable and suitable for git diff.
- **Idempotent.** Re-running always produces the same output given the same input. Safe to run as a post-commit hook on any member repo.
- **Tenant variance flagged, never resolved.** Models and paths that vary by `v_tenant` are marked — the graph represents the template, not any single tenant's instantiation.

## Installation

```bash
pip install -r requirements.txt
```

PyYAML is the only non-stdlib dependency.
