# M7 · CI/CD — Git Folders & Declarative Automation Bundles

> **Exam domain 5 — Implementing CI/CD.** Objectives: manage the code workflow in the
> workspace with **Databricks Git Folders** (branches, commits, PRs); use **Automation
> Bundle variables and overrides** for environment-specific config while promoting **one
> codebase** across **dev/test/prod**; **deploy Declarative Automation Bundles** to package,
> configure, and promote **Lakeflow Jobs** + **Lakeflow Spark Declarative Pipelines**; and
> drive it all with the **Databricks CLI**.

## 1 · Concept / theory

### The two halves of CI/CD on Databricks
**Continuous Integration** = your *code* lives in Git and is reviewed/merged.
**Continuous Deployment** = that code, plus the *resources* it needs (jobs, pipelines), is
**deployed** to a target environment by an automated tool. On Databricks those halves are:

- **Databricks Git Folders** (formerly *Repos*) — a Git working copy *inside* the
  workspace. You author notebooks/SQL in the UI, then **create/switch branches**, **commit
  & push**, and **open a pull request** through the provider (GitHub/GitLab/Azure DevOps).
  Git Folders is the *interactive* author-and-review surface.
- **Declarative Automation Bundles** (formerly *Databricks Asset Bundles*, "DABs") — an
  **infrastructure-as-code** packaging of a project: source code **plus** declarative
  resource definitions (jobs, pipelines, schemas), described in YAML and **promoted across
  environments** by the **Databricks CLI**. Bundles are the *automated deployment* surface.

> **Canonical exam answer:** *"DABs define resources **and** code, versioned in Git, and
> promoted across environments via CI/CD."* When a question asks how to package a job +
> pipeline once and ship the same code to dev/test/prod, the answer is **a Declarative
> Automation Bundle**, not copy-pasting notebooks or clicking through each workspace.

### Git Folders workflow (UI)
A Git Folder is bound to a remote repo + branch. The typical loop:
1. **Branch** off `main` (`feature/silver-scd2`) from the Git dialog — never edit `main`.
2. Edit notebooks/files; the Git panel shows a **diff**.
3. **Commit & push** with a message; this pushes to your branch on the remote.
4. **Create a pull request** (the dialog deep-links to the provider) → review → merge.
5. CI (e.g. a GitHub Action) then runs `databricks bundle deploy` to a target.

Git Folders cover the *"manage code in the workspace UI"* objective; bundles cover
*"promote across environments."* They compose: the **same repo** holds your `databricks.yml`.

### Anatomy of `databricks.yml`
A bundle is rooted at a `databricks.yml` (the **bundle root**). Top-level keys:

- **`bundle:`** — name + metadata identifying the project.
- **`variables:`** — named, typed, **defaultable** inputs (e.g. `catalog`, `notifications`).
  Reference them anywhere as `${var.<name>}`. Override per target or on the CLI.
- **`include:`** — globs pulling in resource files, conventionally **`resources/*.yml`**, so
  each job/pipeline lives in its own file instead of one giant document.
- **`targets:`** — the **environments** (dev/test/prod). Each sets a `workspace.host`, a
  deploy **`mode`**, optional **`run_as`**, and **variable overrides**. One codebase → many
  targets is the whole point.

### Deploy modes & `run_as`
- **`mode: development`** (dev target) — prefixes deployed resources with your username,
  **pauses** schedules, and tags everything `dev` so concurrent learners don't collide. This
  is where the per-user sandbox comes from: we pass `${workspace.current_user.short_name}`
  into a `DEV_SCHEMA_PREFIX` so two people write to `insurance.jane_bronze` vs
  `insurance.john_bronze` (see `src/common/config.py`).
- **`mode: production`** (prod target) — **no** name-mangling, schedules **run as
  configured**, with stricter validation. Deploy here only from CI on `main`.
- **`run_as`** — the identity a deployed job/pipeline executes as (a user or a **service
  principal**). Prod should run as a service principal, not a person.

### The CLI is the engine
- `databricks bundle validate` — parse + type-check the bundle, resolve variables/targets,
  surface errors **before** touching the workspace. Run this in CI on every PR.
- `databricks bundle deploy --target <t>` — upload code + **create/update** the jobs &
  pipelines defined in `resources/` in that target's workspace.
- `databricks bundle run <resource> --target <t>` — trigger a deployed job/pipeline.
- `databricks bundle destroy --target <t>` — tear the deployed resources back down.

## 2 · Worked code

**`databricks.yml`** (bundle root — variables + include + dev/test/prod targets):
```yaml
bundle:
  name: insurance-lakehouse

variables:
  catalog:
    description: Target Unity Catalog.
    default: insurance
  pipeline_dev_mode:           # Lakeflow pipeline "development" toggle
    default: true

include:
  - resources/*.yml            # resources/insurance_ingest.job.yml, insurance_pipeline.pipeline.yml

targets:
  dev:
    mode: development          # per-user prefix, schedules paused, dev tags
    default: true
    workspace:
      host: https://<your-workspace>.cloud.databricks.com
    variables:
      # isolate each learner's schemas: insurance.<short_name>_bronze, ...
      dev_schema_prefix: ${workspace.current_user.short_name}

  test:
    mode: development          # like prod-shape but safe; CI runs integration tests here
    workspace:
      host: https://<test-workspace>.cloud.databricks.com
    variables:
      pipeline_dev_mode: false

  prod:
    mode: production           # no name-mangling, schedules live, strict validation
    workspace:
      host: https://<prod-workspace>.cloud.databricks.com
    run_as:
      service_principal_name: ${var.prod_sp}   # run jobs as an SP, not a person
    variables:
      pipeline_dev_mode: false
```

**`resources/insurance_ingest.job.yml`** (a **Lakeflow Job**, parameterised by variables):
```yaml
resources:
  jobs:
    insurance_ingest:
      name: insurance-ingest-${bundle.target}      # dev/test/prod in the name
      tasks:
        - task_key: bronze_autoloader
          notebook_task:
            notebook_path: ../src/bronze/load_reference.py
            base_parameters:
              catalog: ${var.catalog}
              dev_schema_prefix: ${var.dev_schema_prefix}   # wired to config.py
```

**`resources/insurance_pipeline.pipeline.yml`** (a **Lakeflow Spark Declarative Pipeline**):
```yaml
resources:
  pipelines:
    insurance_medallion:
      name: insurance-medallion-${bundle.target}
      catalog: ${var.catalog}
      development: ${var.pipeline_dev_mode}     # true in dev, false in test/prod
      libraries:
        - glob:
            include: ../src/silver/**            # pipeline source code from the same repo
```

**CLI — the promote-one-codebase flow:**
```bash
databricks auth login --host https://<your-workspace>.cloud.databricks.com  # OAuth profile
databricks bundle validate                       # parse + resolve (defaults to dev)
databricks bundle deploy  --target dev           # per-user sandbox; schedules paused
databricks bundle run     insurance_ingest --target dev
databricks bundle deploy  --target prod          # same code; run from CI on main only
# override a variable at deploy time:
databricks bundle deploy  --target test --var="catalog=insurance_test"
```

## 3 · Best practices & pitfalls

- **One codebase, many targets.** Never fork notebooks per environment — change behaviour
  through **variables + target overrides**, not copies. That's literally what bundles exist
  for (and the canonical exam answer).
- **Always `validate` before `deploy`**, and run it in CI on every PR — it catches bad
  variable refs and resource typos without mutating any workspace.
- **`mode: development` ≠ optional.** It pauses schedules and prefixes resources so shared
  dev workspaces don't trample each other; the per-user `${workspace.current_user.short_name}`
  prefix is how this repo's `config.py` keeps learners isolated.
- **Deploy prod from CI, not your laptop**, and use **`run_as` a service principal** so prod
  jobs don't depend on one person's account.
- **Resource files are includes.** Keep one job/pipeline per `resources/*.yml`; reference it
  by its **resource key** (`insurance_ingest`), which is *not* the same as its display
  `name`.
- **Git Folders is for authoring/review, bundles are for deployment** — don't try to
  "promote" by copying a Git Folder between workspaces; deploy the bundle.
- **`destroy` is destructive** — it removes the deployed jobs/pipelines (and dev data with
  development-mode prefixes). Scope it to a target deliberately.
- On **Free Edition** you have one serverless workspace, so dev/test/prod are taught
  conceptually; you still `validate`/`deploy --target dev` for real here.

## 4 · Exam focus

**Objectives:** use **Git Folders** to branch/commit/push/PR in the workspace UI; use
**bundle variables and overrides** for environment-specific config across **dev/test/prod**;
**deploy bundles** to package/configure/promote **Lakeflow Jobs + Declarative Pipelines**;
use the **Databricks CLI** to **validate/deploy/manage** bundles.

**Practice questions**
1. *You have one job + one pipeline and must ship the identical code to dev, test, and prod
   with only the catalog and schedule differing per environment. What do you use?*
   **A.** A **Declarative Automation Bundle**: define the job and pipeline once in
   `resources/`, parameterise with **`variables`**, and set per-environment values under
   **`targets`** (dev/test/prod). *DABs define resources + code, versioned in Git, promoted
   across environments via CI/CD* — copying notebooks per workspace is the wrong answer.
2. *Which CLI command parses a bundle and resolves its variables/targets **without** changing
   any workspace, and should run on every PR?* **A.** `databricks bundle validate`. (`deploy`
   mutates the target; `run` triggers a resource; `destroy` removes resources.)
3. *In a shared dev workspace, two engineers deploy the same bundle and must not collide; the
   target should also pause schedules. Which target setting achieves this?*
   **A.** **`mode: development`**, which prefixes resources per user and pauses schedules;
   pair it with `${workspace.current_user.short_name}` to isolate each person's UC schemas.
   (`mode: production` would name-mangle nothing and run schedules live.)

## 5 · References
- **Databricks Asset Bundles / Declarative Automation Bundles** — `databricks.yml` schema:
  `bundle`, `variables`, `include`, `targets`, deploy **modes** (development/production),
  `run_as`, variable **overrides**
- **Databricks CLI** — `bundle validate | deploy | run | destroy`, `--target`, `--var`
- **Databricks Git Folders** — branches, commit/push, pull requests; Git provider setup
- **Lakeflow Jobs** and **Lakeflow Spark Declarative Pipelines** as bundle resources
- CI/CD for Databricks (GitHub Actions / Azure DevOps running `bundle deploy`)
