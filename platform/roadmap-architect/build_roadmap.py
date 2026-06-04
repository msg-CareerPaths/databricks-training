"""Generate the roadmap-architect graph JSON for the Databricks DE Associate path.

Output mirrors the schema used by `msg-CareerPaths/roadmap-architect`
(`apps/roadmap-architect/src/assets/roadmaps/<uuid>.json`): a react-flow graph with one
**container** node (the course, linking the repo `readme.md`) holding one **child** node
per chapter (linking the raw chapter markdown), with the mandatory chapters chained by edges.

Run:  python platform/roadmap-architect/build_roadmap.py
It writes `<roadmap-id>.json` next to this script and prints the id to register in
`roadmap-architect/.../src/assets/roadmaps.json`.

IDs are deterministic (uuid5) so re-running is stable.
"""
from __future__ import annotations

import json
import pathlib
import uuid

NS = uuid.UUID("00000000-0000-0000-0000-00000000d8ba")  # fixed namespace → stable ids
RAW = "https://raw.githubusercontent.com/msg-CareerPaths/databricks-training/main/"


def uid(name: str) -> str:
    return str(uuid.uuid5(NS, "databricks-training/" + name))


MANDATORY = [
    ("Introduction", "chapters/000-introduction.md"),
    ("Platform Foundations", "chapters/100-platform-foundations.md"),
    ("Generate & Land", "chapters/200-generate-and-land.md"),
    ("Bronze Ingestion", "chapters/300-bronze-ingestion.md"),
    ("Silver Transform", "chapters/400-silver-transform.md"),
    ("Gold Modeling", "chapters/500-gold-modeling.md"),
    ("Declarative Pipelines", "chapters/600-declarative-pipelines.md"),
    ("Lakeflow Jobs", "chapters/700-lakeflow-jobs.md"),
    ("CI/CD & Bundles", "chapters/800-cicd-bundles.md"),
    ("Troubleshooting & Optimization", "chapters/900-troubleshooting-optimization.md"),
    ("Governance & Security", "chapters/950-governance-security.md"),
    ("Dashboards & Readiness", "chapters/990-dashboards-and-readiness.md"),
]
OPTIONAL = [
    ("Lakeflow Connect", "chapters/opt-100-lakeflow-connect.md"),
    ("Streaming Deep-Dive", "chapters/opt-200-streaming-deep-dive.md"),
    ("Liquid Clustering", "chapters/opt-300-performance-liquid-clustering.md"),
    ("GitHub Actions CI", "chapters/opt-400-cicd-github-actions.md"),
]

NODE_W, NODE_H, STEP = 240, 43, 82
CONTAINER_ID = uid("container")


def child(label: str, path: str, path_type: str, x: int, y: int) -> dict:
    return {
        "id": uid(path),
        "position": {"x": x, "y": y},
        "type": "RoadmapNode",
        "zIndex": 11,
        "style": {"zIndex": 11},
        "data": {"pathType": path_type, "isContainer": False, "label": label,
                 "isReadonly": False, "url": RAW + path},
        "extent": "parent",
        "parentNode": CONTAINER_ID,
        "width": NODE_W, "height": NODE_H,
        "selected": False,
        "positionAbsolute": {"x": x, "y": y},  # container is at (0,0) → absolute == relative
        "dragging": False,
    }


def build() -> dict:
    nodes = [child(l, p, "Main path", 60, 80 + i * STEP) for i, (l, p) in enumerate(MANDATORY)]
    nodes += [child(l, p, "Optional path", 520, 80 + j * STEP) for j, (l, p) in enumerate(OPTIONAL)]

    width = 520 + NODE_W + 60
    height = 80 + len(MANDATORY) * STEP + 20
    container = {
        "id": CONTAINER_ID,
        "position": {"x": 0, "y": 0},
        "type": "RoadmapNode",
        "zIndex": 10,
        "style": {"zIndex": 10, "height": height, "width": width},
        "data": {"pathType": "Main path", "isContainer": True, "label": "Databricks DE Associate",
                 "isReadonly": False, "url": RAW + "readme.md"},
        "width": width, "height": height,
        "selected": False,
        "positionAbsolute": {"x": 0, "y": 0},
        "dragging": False,
    }

    mand_ids = [uid(p) for _, p in MANDATORY]
    edges = [
        {"source": a, "sourceHandle": "output-handle",
         "target": b, "targetHandle": "input-handle-top",
         "id": f"reactflow__edge-{a}output-handle-{b}input-handle-top"}
        for a, b in zip(mand_ids, mand_ids[1:])
    ]

    return {
        "nodes": [container] + nodes,
        "edges": edges,
        "id": uid("roadmap"),
        "tags": ["databricks", "data-engineering"],
        "title": "Databricks Data Engineer Associate",
        "targetAudience": "Associate & IT Consultants",
    }


if __name__ == "__main__":
    roadmap = build()
    here = pathlib.Path(__file__).resolve().parent
    fp = here / f"{roadmap['id']}.json"
    fp.write_text(json.dumps(roadmap, indent=2) + "\n")
    print("roadmap_id:", roadmap["id"])
    print("wrote:", fp.relative_to(here.parent.parent))
    print("nodes:", len(roadmap["nodes"]), "edges:", len(roadmap["edges"]))
