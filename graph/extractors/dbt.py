import re
from pathlib import Path
from typing import Any, Dict

try:
    import yaml
    _YAML_AVAILABLE = True
except ImportError:
    _YAML_AVAILABLE = False

from .base import BaseExtractor

# Jinja ref/source/var patterns
REF_RE = re.compile(r"""\{\{\s*ref\(\s*['"](\w+)['"]\s*\)\s*\}\}""")
SOURCE_RE = re.compile(r"""\{\{\s*source\(\s*['"](\w+)['"]\s*,\s*['"](\w+)['"]\s*\)\s*\}\}""")
TENANT_VAR_RE = re.compile(r"""var\(\s*['"]v_tenant['"]\s*\)""")


class DbtExtractor(BaseExtractor):
    """Extracts nodes and edges from a dbt project (snowflake repo).

    Walks DBT/models/ for .sql files. Parses ref(), source(), var('v_tenant').
    Parses schema.yml files for source definitions.
    """

    def extract(self) -> Dict[str, Any]:
        nodes: Dict[str, Any] = {}
        edges = []

        # dbt models may be at DBT/models/ (snowflake repo) or models/ (generic)
        models_dir = Path(self.path) / "DBT" / "models"
        if not models_dir.exists():
            models_dir = Path(self.path) / "models"
        if not models_dir.exists():
            return {"nodes": nodes, "edges": edges}

        # Parse schema.yml source definitions first
        for source_id, source_node in self._parse_sources(models_dir).items():
            nodes[source_id] = source_node

        # Parse every .sql model file
        for sql_file in sorted(models_dir.rglob("*.sql")):
            rel = str(sql_file.relative_to(Path(self.path))).replace("\\", "/")
            content = sql_file.read_text(encoding="utf-8", errors="ignore")

            model_name = sql_file.stem
            node_id = f"dbt_model:{model_name}"
            tenant_variant = bool(TENANT_VAR_RE.search(content))

            nodes[node_id] = {
                "type": "dbt_model",
                "repo": self.repo,
                "file": rel,
                "tenant_variant": tenant_variant,
                "description": "",
            }

            # ref() → depends_on edge
            for m in REF_RE.finditer(content):
                edges.append({
                    "from": node_id,
                    "to": f"dbt_model:{m.group(1)}",
                    "type": "depends_on",
                })

            # source() → reads_from edge
            for m in SOURCE_RE.finditer(content):
                src_key = f"dbt_source:{m.group(1)}.{m.group(2)}"
                edges.append({
                    "from": node_id,
                    "to": src_key,
                    "type": "reads_from",
                })
                if src_key not in nodes:
                    nodes[src_key] = {
                        "type": "dbt_source",
                        "repo": self.repo,
                        "file": "",
                        "description": f"{m.group(1)}.{m.group(2)}",
                    }

        return {"nodes": nodes, "edges": edges}

    def _parse_sources(self, models_dir: Path) -> Dict[str, Any]:
        sources: Dict[str, Any] = {}
        if not _YAML_AVAILABLE:
            return sources

        for yml_file in sorted(models_dir.rglob("schema.yml")):
            try:
                data = yaml.safe_load(yml_file.read_text(encoding="utf-8", errors="ignore"))
            except Exception:
                continue
            if not isinstance(data, dict) or "sources" not in data:
                continue

            rel = str(yml_file.relative_to(yml_file.parents[len(yml_file.parts) - len(Path(self.path).parts) - 1])).replace("\\", "/")

            for src in data.get("sources", []) or []:
                src_name = src.get("name", "")
                schema = src.get("schema", "")
                for table in src.get("tables", []) or []:
                    tbl_name = table.get("name", "")
                    node_id = f"dbt_source:{src_name}.{tbl_name}"
                    sources[node_id] = {
                        "type": "dbt_source",
                        "repo": self.repo,
                        "file": str(yml_file.relative_to(Path(self.path))).replace("\\", "/"),
                        "schema": schema,
                        "description": table.get("description", f"{src_name}.{tbl_name}"),
                    }
        return sources
