from typing import Any, Dict, List


def build_graph(extraction_results: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Merge all extractor results into a single in-memory node/edge model.

    Nodes: first writer wins (earlier extractors take precedence on conflict).
    Edges: deduplicated by (from, to, type) triple.
    """
    all_nodes: Dict[str, Any] = {}
    all_edges: List[Dict[str, Any]] = []

    for result in extraction_results:
        for node_id, node_data in result.get("nodes", {}).items():
            if node_id not in all_nodes:
                all_nodes[node_id] = node_data
        all_edges.extend(result.get("edges", []))

    # Deduplicate edges
    seen: set = set()
    unique_edges = []
    for edge in all_edges:
        key = (edge["from"], edge["to"], edge["type"])
        if key not in seen:
            seen.add(key)
            unique_edges.append(edge)

    return {"nodes": all_nodes, "edges": unique_edges}
