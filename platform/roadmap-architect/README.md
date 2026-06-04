# Roadmap for the msg-CareerPaths platform

This folder holds a ready-to-drop **roadmap graph** for the company career-path platform
(`msg-CareerPaths/roadmap-architect` → published by `msg-CareerPaths/roadmap`). It is **not**
ingested from here — it's a deliverable you copy into the `roadmap-architect` repo via a PR.

## How the platform works (why this file exists)
- `roadmap-architect` is the editor app; each career path is a **react-flow graph** stored as
  `apps/roadmap-architect/src/assets/roadmaps/<uuid>.json` and registered in the index
  `apps/roadmap-architect/src/assets/roadmaps.json` (a flat list of UUIDs).
- A graph has one **container** node per course (links the repo `readme.md`) holding one **child**
  node per chapter. Each node's `data.url` is the **raw GitHub URL** of that markdown file; the
  published `roadmap` site fetches and renders it. Mandatory chapters are chained by edges
  (`pathType: "Main path"`); optional ones are `"Optional path"`.
- There is **no CI in the training repo** — the platform pulls content by URL. (See the chapter
  links in `12388307-60d3-58ac-ad02-bac14269d286.json` — they point at this repo's
  `chapters/*.md` on `main`.)

## Files
- `12388307-60d3-58ac-ad02-bac14269d286.json` — the Databricks DE Associate roadmap (1 container +
  12 mandatory + 4 optional chapter nodes, mandatory path chained by edges).
- `build_roadmap.py` — regenerates it (deterministic ids). Edit the `MANDATORY` / `OPTIONAL`
  lists and re-run to adjust labels, add chapters, or change order.

## Install it (PR against `msg-CareerPaths/roadmap-architect`)
1. Copy `12388307-60d3-58ac-ad02-bac14269d286.json` into
   `apps/roadmap-architect/src/assets/roadmaps/`.
2. Add the id to the registry `apps/roadmap-architect/src/assets/roadmaps.json`:
   ```json
   "12388307-60d3-58ac-ad02-bac14269d286"
   ```
3. Open a PR. roadmap-architect's own CI deploys it to the `roadmap` site.
4. (Optional) open the file in the architect editor to fine-tune node positions — the layout here
   is a clean starting grid; the viewer renders nodes by position.

## Prerequisite
The node URLs point at `raw.githubusercontent.com/msg-CareerPaths/databricks-training/main/...`, so
this repo must be hosted at **`msg-CareerPaths/databricks-training`** with the content on **`main`**
for the chapters + diagrams to resolve in the roadmap viewer.
