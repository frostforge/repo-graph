from abc import ABC, abstractmethod
from typing import Any, Dict


class BaseExtractor(ABC):
    """Abstract base for all repo extractors.

    Each extractor is responsible for one repo type. They all return the same shape:
    {"nodes": {node_id: {...}}, "edges": [{from, to, type}]}
    """

    def __init__(self, repo_config: Dict[str, Any]):
        self.repo = repo_config["name"]
        self.path = repo_config["path"]

    @abstractmethod
    def extract(self) -> Dict[str, Any]:
        """
        Returns:
            {
              "nodes": {node_id: {"type", "repo", "file", "description", ...}},
              "edges": [{"from": node_id, "to": node_id, "type": edge_type}]
            }

        Node types: dbt_model, dbt_source, s3_path, sqs_queue, snowflake_table, service, contract
        Edge types: reads_from, writes_to, depends_on, implements, tenant_variant
        """
        ...
