import re
from pathlib import Path
from typing import Any, Dict

from .base import BaseExtractor

# S3 literal: 's3://bucket/path/...' — stops at quote, no template vars
S3_LITERAL_RE = re.compile(r"""['"]s3://([a-zA-Z0-9._-]+)(/[^'"{}\s]*)?\s*['"]""")

# S3 f-string: f's3://{bucket}/prefix/...' — captures full template as-is
S3_FSTRING_RE = re.compile(r"""f['"]s3://([^'"]+)['"]""")

# boto3 method calls — used to determine edge direction
BOTO3_WRITE_OPS = {"put_object", "upload_file", "upload_fileobj", "copy_object", "copy"}
BOTO3_READ_OPS = {"get_object", "download_file", "download_fileobj", "head_object", "list_objects", "list_objects_v2"}
BOTO3_METHOD_RE = re.compile(r"""\.(\w+)\s*\(""")

# SQS: queue names that look like '<name>-queue' or similar
SQS_NAME_RE = re.compile(r"""['"]([a-zA-Z0-9_-]+-(?:queue|Queue)[a-zA-Z0-9_-]*)['"]""")

# boto3 Bucket= kwarg with a literal bucket name (no env var)
# Catches: s3.get_object(Bucket='sftp-harmonate-drop', ...) etc.
BOTO3_BUCKET_KWARG_RE = re.compile(r"""Bucket\s*=\s*['"]([a-zA-Z0-9._-]+)['"]""")

# Module-level bucket name constants: SOURCE_S3_BUCKET = 'bucket-name'
# Also: os.environ["SOURCE_S3_BUCKET"] = 'bucket-name'
BUCKET_CONST_RE = re.compile(r"""BUCKET\w*['"\]]?\s*=\s*['"]([a-zA-Z0-9._-]{3,})['"]""")

# Snowflake fully-qualified table refs: DB.SCHEMA.TABLE (all caps/underscores)
SF_TABLE_RE = re.compile(r"""['"]([A-Z][A-Z0-9_]+\.[A-Z][A-Z0-9_]+\.[A-Z][A-Z0-9_]+)['"]""")

SKIP_DIRS = {"__pycache__", ".git", "node_modules", ".venv", "venv", "env", "dist", "build",
             "target", "logs", "dbt_packages", ".tox", ".mypy_cache"}


class PythonExtractor(BaseExtractor):
    """Extracts nodes and edges from a Python service repo.

    Walks all .py files and extracts:
    - S3 path literals and f-string prefixes
    - boto3 put/get calls → writes_to / reads_from direction
    - SQS queue name references
    - Snowflake fully-qualified table references
    """

    def extract(self) -> Dict[str, Any]:
        nodes: Dict[str, Any] = {}
        edges = []

        repo_node_id = f"service:{self.repo}"
        nodes[repo_node_id] = {
            "type": "service",
            "repo": self.repo,
            "file": "",
            "description": f"Service: {self.repo}",
        }

        for py_file in self._py_files():
            rel = str(py_file.relative_to(Path(self.path))).replace("\\", "/")
            try:
                content = py_file.read_text(encoding="utf-8", errors="ignore")
            except Exception:
                continue
            self._extract_s3(content, rel, repo_node_id, nodes, edges)
            self._extract_sqs(content, rel, repo_node_id, nodes, edges)
            self._extract_snowflake(content, rel, repo_node_id, nodes, edges)

        return {"nodes": nodes, "edges": edges}

    # ------------------------------------------------------------------
    def _py_files(self):
        for p in sorted(Path(self.path).rglob("*.py")):
            if any(skip in p.parts for skip in SKIP_DIRS):
                continue
            yield p

    def _extract_s3(self, content: str, rel: str, repo_node_id: str,
                    nodes: dict, edges: list):
        # Determine boto3 operation direction for this file
        methods = BOTO3_METHOD_RE.findall(content)
        writes = any(m in BOTO3_WRITE_OPS for m in methods)
        reads = any(m in BOTO3_READ_OPS for m in methods)
        # If no boto3 ops at all, skip S3 edges (likely a config/test file)
        has_boto = writes or reads or "boto3" in content

        seen: set = set()

        def add_s3(path_str: str):
            if not path_str or path_str in seen:
                return
            seen.add(path_str)
            node_id = f"s3_path:{path_str}"
            if node_id not in nodes:
                nodes[node_id] = {
                    "type": "s3_path",
                    "repo": self.repo,
                    "file": rel,
                    "description": path_str,
                }
            if not has_boto:
                return
            if writes:
                edges.append({"from": repo_node_id, "to": node_id, "type": "writes_to"})
            if reads:
                edges.append({"from": repo_node_id, "to": node_id, "type": "reads_from"})
            if not writes and not reads:
                edges.append({"from": repo_node_id, "to": node_id, "type": "reads_from"})

        for m in S3_LITERAL_RE.finditer(content):
            bucket = m.group(1)
            key = (m.group(2) or "").rstrip("/")
            add_s3(f"s3://{bucket}{key}" if key else f"s3://{bucket}")

        for m in S3_FSTRING_RE.finditer(content):
            raw = m.group(1)
            # Normalize dynamic segments to {*}
            normalized = re.sub(r"\{[^}]+\}", "{*}", raw).rstrip("/")
            add_s3(f"s3://{normalized}")

        # boto3 Bucket= kwarg with a literal bucket name (no s3:// prefix in code)
        for m in BOTO3_BUCKET_KWARG_RE.finditer(content):
            add_s3(f"s3://{m.group(1)}")

        # Module-level bucket constants: SOURCE_S3_BUCKET = 'my-bucket'
        for m in BUCKET_CONST_RE.finditer(content):
            add_s3(f"s3://{m.group(1)}")

    def _extract_sqs(self, content: str, rel: str, repo_node_id: str,
                     nodes: dict, edges: list):
        seen: set = set()
        for m in SQS_NAME_RE.finditer(content):
            name = m.group(1)
            if name in seen:
                continue
            seen.add(name)
            node_id = f"sqs_queue:{name}"
            if node_id not in nodes:
                nodes[node_id] = {
                    "type": "sqs_queue",
                    "repo": self.repo,
                    "file": rel,
                    "description": name,
                }
            edges.append({"from": repo_node_id, "to": node_id, "type": "reads_from"})

    def _extract_snowflake(self, content: str, rel: str, repo_node_id: str,
                           nodes: dict, edges: list):
        seen: set = set()
        for m in SF_TABLE_RE.finditer(content):
            ref = m.group(1)
            if ref in seen:
                continue
            seen.add(ref)
            node_id = f"snowflake_table:{ref}"
            if node_id not in nodes:
                nodes[node_id] = {
                    "type": "snowflake_table",
                    "repo": self.repo,
                    "file": rel,
                    "description": ref,
                }
            edges.append({"from": repo_node_id, "to": node_id, "type": "reads_from"})
