import re
from pathlib import Path
from typing import Any, Dict

from .base import BaseExtractor

# Explicit s3:// paths in backtick code spans
S3_BACKTICK_RE = re.compile(r"`(s3://[^`\s]+)`")

# Snowflake DB.SCHEMA.TABLE in backticks
SF_TABLE_RE = re.compile(r"`([A-Z][A-Z0-9_]+\.[A-Z][A-Z0-9_]+\.[A-Z][A-Z0-9_]+)`")

# Field names in table rows (heuristic: | `field_name` | type |)
FIELD_ROW_RE = re.compile(r"\|\s*`([a-z_]+)`\s*\|")


class ManifestExtractor(BaseExtractor):
    """Extracts contract nodes from shared-context markdown contract files.

    Parses s3-manifest-contract.md and raw-ingestion-contract.md.
    Emits contract nodes and cross-references to s3_path / snowflake_table nodes
    that appear inside those files.
    """

    def __init__(self, repo_config: Dict[str, Any], shared_context_path: str):
        super().__init__(repo_config)
        self.shared_context_path = Path(shared_context_path)

    def extract(self) -> Dict[str, Any]:
        nodes: Dict[str, Any] = {}
        edges = []

        for md_file in sorted(self.shared_context_path.glob("*.md")):
            # Skip graph output files and the plan file
            name = md_file.name
            if name.startswith("GRAPH_") or name.startswith("PLAN_") or name == "MEMORY.md":
                continue

            contract_id = f"contract:{md_file.stem}"
            nodes[contract_id] = {
                "type": "contract",
                "repo": "shared-context",
                "file": name,
                "description": md_file.stem.replace("-", " ").replace("_", " "),
            }

            content = md_file.read_text(encoding="utf-8", errors="ignore")

            # Cross-ref S3 paths
            for m in S3_BACKTICK_RE.finditer(content):
                path_str = m.group(1).rstrip("/")
                node_id = f"s3_path:{path_str}"
                if node_id not in nodes:
                    nodes[node_id] = {
                        "type": "s3_path",
                        "repo": "shared-context",
                        "file": name,
                        "description": path_str,
                    }
                edges.append({"from": contract_id, "to": node_id, "type": "implements"})

            # Cross-ref Snowflake tables
            for m in SF_TABLE_RE.finditer(content):
                ref = m.group(1)
                node_id = f"snowflake_table:{ref}"
                if node_id not in nodes:
                    nodes[node_id] = {
                        "type": "snowflake_table",
                        "repo": "shared-context",
                        "file": name,
                        "description": ref,
                    }
                edges.append({"from": contract_id, "to": node_id, "type": "implements"})

        return {"nodes": nodes, "edges": edges}
