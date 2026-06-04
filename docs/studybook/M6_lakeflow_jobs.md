# M6 · Orchestration with Lakeflow Jobs

> **Exam domain 4 — Working with Lakeflow Jobs.** This chapter covers the objectives the
> exam names: building a **DAG-based task graph** from **common task types** (notebook, SQL
> query, dashboard, pipeline) wired by **dependencies**; **control flow** — **retries** and
> **conditional tasks** (`run_if` branching, **If/else** condition task, **for-each** loop);
> and **schedules / triggers** (**cron schedule**, **file arrival**, **table update**) plus
> when to pick **time-based vs data-driven** triggers. We orchestrate the project's
> `land → pipeline → DQ-test → publish` flow. ("Lakeflow Jobs" is the current name for what
> older docs call **Workflows / Jobs**.)

## 1 · Concept / theory

### A Job is a DAG of tasks
A **Lakeflow Job** is the orchestrator. It runs one or more **tasks**, and the
**`depends_on`** edges between tasks form a **DAG** (directed acyclic graph — no cycles).
Databricks reads the graph and runs tasks **in topological order**, parallelizing any with
no unmet dependency. A task with two parents waits for **both**; that fan-in point is where
**conditional execution** lives (below). One Job = one schedulable, monitorable unit with a
single run history, retries, parameters, and notifications.

### Common task types (the ones the exam lists)
| `task_key` type | What it runs | In our DAG |
|---|---|---|
| **`notebook_task`** | A workspace/Git notebook (any language) | DQ-test notebook, notify, land |
| **`pipeline_task`** | A **Lakeflow Declarative Pipeline** (the M5 pipeline) by `pipeline_id` | the medallion refresh |
| **`sql_task`** | A saved **SQL query**, **file**, **alert**, or **dashboard** on a SQL warehouse | (alt. way to refresh the dashboard) |
| **`dashboard_task`** | Refreshes a **Lakeview/AI-BI dashboard** and snapshots it | publish step |
| also: `spark_jar_task`, `python_wheel_task`, `run_job_task` (call another Job), `condition_task`, `for_each_task` |

Each task picks **compute** (serverless or a job cluster) and can take **parameters**.

### Control flow #1 — retries
Tasks fail (a flaky read, a transient cluster issue). Per task you set:
- **`max_retries`** — how many times to re-run the task on failure (`-1` = unlimited).
- **`min_retry_interval_millis`** — minimum wait between attempts (back-off).
- **`retry_on_timeout`** — also retry if the task hit its `timeout_seconds`.

Retries are **task-scoped** — only the failed task re-runs, not the whole DAG. Use them for
**transient** faults, not logic errors (retrying a bad DQ result just fails 3× slower).

### Control flow #2 — conditional tasks (`run_if`, If/else, for-each)
Three distinct mechanisms — keep them straight for the exam:

1. **`run_if`** on a task decides whether that task runs based on the **status of its
   dependencies**. Values: **`ALL_SUCCESS`** (default), **`ALL_DONE`** (run regardless),
   **`AT_LEAST_ONE_FAILED`**, `NONE_FAILED`, `ALL_FAILED`, `AT_LEAST_ONE_SUCCESS`. This is
   how you **branch**: two downstream tasks share the same parent, one with
   `ALL_SUCCESS`, the other with `AT_LEAST_ONE_FAILED`. Exactly one runs; the skipped one is
   marked **Skipped** (not failed), so the run's overall status stays clean if you want.
2. **If/else `condition_task`** evaluates a **boolean expression** over **task values /
   parameters** (e.g. `{{tasks.dq_test.values.failed_rows}} == 0`) and exposes two outcome
   branches (`true` / `false`) that downstream tasks depend on via `run_if`. Use this to
   branch on **data/values**, not just task status.
3. **`for_each_task`** **loops** a nested task over an array of inputs, with a
   **`concurrency`** limit. The array can be literal or come from an upstream **task value**
   (`{{tasks.list_sources.values.sources}}`). Each iteration gets `{{input}}`. Perfect for
   "run this notebook once per source/region."

**Task values** glue these together: `dbutils.jobs.taskValues.set(key=..., value=...)` in
one task, then reference `{{tasks.<key>.values.<k>}}` downstream.

### Schedules & triggers (time-based vs data-driven)
A Job runs when **triggered**. The exam wants you to choose the right trigger:
- **Scheduled (cron)** — *time-based*. **Quartz cron** expression + timezone, e.g. nightly.
  Fits **predictable, periodic** batch (our nightly medallion refresh). Simple, but runs
  even if no new data arrived (wasted compute) and lags if data is early.
- **File arrival** — *data-driven*. Watches a **storage location / UC Volume path**; fires
  when **new files** land. Fits **"run as soon as the upstream drops a file"** with
  irregular timing — exactly our landing Volume. Polls periodically; has a min interval.
- **Table update** — *data-driven*. Fires when one or more **Delta tables are updated** (new
  commit). Fits **chaining off another pipeline's output** without a second schedule — e.g.
  refresh a downstream Job when `silver.fact_claims` changes.
- **Continuous** — keep a streaming Job always running (a different trigger mode).
- **Manual / `run_job_task`** — on demand, or invoked by another Job.

**Rule of thumb:** if the cadence is **clock-driven and predictable → cron**; if it's
**"whenever new data appears" → file arrival (new files) or table update (new commits)**.
Data-driven triggers avoid empty runs and cut latency between arrival and processing.

> **Free Edition:** FE is **serverless** and **can run scheduled serverless Jobs** —
> cron, file-arrival, and table-update triggers all work on serverless compute, so the whole
> M6 DAG runs on FE within the budget. (Use `availableNow` ingestion from M2 so each run is a
> bounded batch.)

## 2 · Worked code

### The project DAG (task graph)
```
land_delta ──> run_pipeline ──> dq_test ─┬─(ALL_SUCCESS)──────> refresh_dashboard
   (notebook)   (pipeline)    (notebook) │
                                         └─(AT_LEAST_ONE_FAILED)─> notify_and_stop
```
`dq_test` is the fan-in/branch point: on clean DQ the run **publishes the dashboard**; on a
DQ failure it **notifies and stops** (publish is Skipped).

### Job definition (YAML — bundle `resources/insurance_ingest.job.yml`)
This is the same task graph expressed as a **Lakeflow Jobs** resource; in M7 it lives in the
Automation Bundle as `resources/insurance_ingest.job.yml` and deploys via the CLI.
```yaml
resources:
  jobs:
    insurance_ingest:
      name: insurance_ingest
      # --- trigger: data-driven, fire when new files land in the Volume ---
      trigger:
        pause_status: UNPAUSED
        file_arrival:
          url: /Volumes/insurance/landing/raw/
          min_time_between_triggers_seconds: 300   # poll/debounce window
      # (alternative time-based trigger — nightly cron — shown commented:)
      # schedule:
      #   quartz_cron_expression: "0 0 2 * * ?"     # 02:00 every day
      #   timezone_id: "Europe/Berlin"
      #   pause_status: UNPAUSED
      parameters:
        - { name: env, default: dev }
      email_notifications:
        on_failure: [you@example.com]

      tasks:
        # 1) land/upload the delta batch into the landing Volume (batch-incremental)
        - task_key: land_delta
          notebook_task:
            notebook_path: ../src/bronze/land_batch.py
            base_parameters: { batch: "{{job.parameters.env}}" }
          max_retries: 2
          min_retry_interval_millis: 30000          # back off 30s between attempts
          retry_on_timeout: true
          timeout_seconds: 1200

        # 2) run the M5 Lakeflow Declarative Pipeline (bronze->silver->gold + EXPECTATIONS)
        - task_key: run_pipeline
          depends_on: [{ task_key: land_delta }]
          pipeline_task:
            pipeline_id: ${resources.pipelines.insurance_medallion.id}
            full_refresh: false
          max_retries: 1

        # 3) DQ-test notebook — sets a task value `failed_rows`; fails if defects remain
        - task_key: dq_test
          depends_on: [{ task_key: run_pipeline }]
          notebook_task:
            notebook_path: ../tests/dq_checks.py
          max_retries: 0                              # logic check: don't retry failures

        # 4a) BRANCH A — DQ clean: refresh the AI/BI dashboard (publish)
        - task_key: refresh_dashboard
          depends_on: [{ task_key: dq_test }]
          run_if: ALL_SUCCESS                         # default: only if dq_test passed
          dashboard_task:
            dashboard_id: ${resources.dashboards.exec_loss_premium.id}
            warehouse_id: ${var.warehouse_id}

        # 4b) BRANCH B — DQ failed: notify + stop (publish is Skipped)
        - task_key: notify_and_stop
          depends_on: [{ task_key: dq_test }]
          run_if: AT_LEAST_ONE_FAILED                 # only when an upstream failed
          notebook_task:
            notebook_path: ../src/ops/notify.py
            base_parameters: { failed_rows: "{{tasks.dq_test.values.failed_rows}}" }
```

### If/else condition task + a for-each loop (JSON fragment)
The same Job can branch on a **value** (not just task status) and **loop** over the sources.
Shown as the Jobs JSON these tasks compile to:
```json
{"tasks": [
  {"task_key": "dq_gate",
   "depends_on": [{"task_key": "dq_test"}],
   "condition_task": {"op": "EQUAL_TO",
                      "left": "{{tasks.dq_test.values.failed_rows}}",
                      "right": "0"}},
  {"task_key": "publish",
   "depends_on": [{"task_key": "dq_gate", "outcome": "true"}],
   "dashboard_task": {"dashboard_id": "…", "warehouse_id": "…"}},

  {"task_key": "smoke_each_source",
   "for_each_task": {
     "concurrency": 3,
     "inputs": "[\"customers\",\"policies\",\"claims\",\"payments\",\"telematics\"]",
     "task": {"task_key": "smoke_one",
              "notebook_task": {"notebook_path": "../tests/smoke_table.py",
                                "base_parameters": {"source": "{{input}}"}}}}}
]}
```
`outcome: "true"` / `"false"` are the two branches of the condition task; `{{input}}` is the
current loop element. `inputs` can also be a JSON array produced upstream via
`dbutils.jobs.taskValues.set(key="sources", value=[...])` and referenced as
`{{tasks.<key>.values.sources}}`.

### The DQ-test notebook task (PySpark)
A tiny task that **publishes a task value** the branch/condition reads, and **fails the task**
when defects remain so `run_if`/the condition can route the DAG:
```python
# tests/dq_checks.py  — a notebook_task
from pyspark.sql import functions as F
from src.common import config

spark = config.get_spark()

# example gate: no orphan claims and fraud_flag fully normalized in silver
claims = spark.table(config.table("silver", "fact_claims"))
bad = claims.filter(
    F.col("policy_id").isNull() | (~F.col("fraud_flag").isin(True, False))
).count()

# expose the count to downstream tasks (run_if branch / condition_task)
dbutils.jobs.taskValues.set(key="failed_rows", value=int(bad))   # noqa: F821
print(f"DQ failed_rows = {bad}")

if bad > 0:                       # fail the task → AT_LEAST_ONE_FAILED branch fires
    raise ValueError(f"DQ gate failed: {bad} bad rows in fact_claims")
```

## 3 · Best practices & pitfalls
- **One Job, many tasks** — don't split a pipeline into many single-task Jobs; you lose the
  shared DAG, run history, and conditional flow. Use **`run_job_task`** only to compose
  *independent* Jobs.
- **Retries are for transient faults.** Set `max_retries` + `min_retry_interval_millis` on
  flaky I/O tasks (`land_delta`), and `max_retries: 0` on **logic checks** (`dq_test`) — you
  don't want a real DQ failure retried into a pass-by-luck.
- **`run_if` skips, it doesn't fail.** A task whose `run_if` isn't met is **Skipped**; the
  branch you *don't* take won't redden the run. To make a DQ failure actually fail the Job,
  let `dq_test` raise (its failure is real); the success branch then auto-skips.
- **Branch on status with `run_if`, on values with a `condition_task`.** Mixing them up is a
  classic exam trap. `ALL_SUCCESS` vs `AT_LEAST_ONE_FAILED` is status; `failed_rows == 0` is
  a value (condition task).
- **`pipeline_task` by id, not by copying logic.** Reference the M5 pipeline; set
  `full_refresh: false` for incremental nightly runs, `true` only when you intend a rebuild.
- **Pick the trigger to match cadence.** Predictable nightly → **cron**; "as files land" →
  **file arrival** on the Volume; "when an upstream table commits" → **table update**. Don't
  cron-poll for data that arrives irregularly (empty runs) or you'll add latency.
- **File-arrival has a debounce** (`min_time_between_triggers_seconds`) — it's *near*-real-
  time, not instant; size it so a multi-file drop triggers **one** run, not many.
- **Parameterize, don't hardcode.** Job/task parameters (`{{job.parameters.env}}`) + the
  bundle's `DEV_SCHEMA_PREFIX` keep one Job definition working across dev/test/prod (M7).
- **Free Edition**: schedules and triggers run on **serverless** — use `availableNow` batches
  so each triggered run is bounded and cheap.

## 4 · Exam focus
**Objectives:** implement **control flow** — **retries** and **conditional tasks**
(branching/looping); configure **common task types** (notebook, SQL, dashboard, pipeline) and
their **dependencies** on the **DAG task graph**; implement **schedules** with **trigger
types** (scheduled, file arrival, table update); choose **time-based vs data-driven** triggers.

**Practice questions**
1. *A nightly Job must refresh a dashboard **only when** the upstream DQ task passes, and
   **page on-call** when it fails — without the failure branch turning the run red on the
   success path. How do you wire the two downstream tasks?* **A.** Give both the **same
   parent** (`dq_test`) and set `run_if`: the dashboard task **`ALL_SUCCESS`**, the notify
   task **`AT_LEAST_ONE_FAILED`**. Exactly one runs; the other is **Skipped**. (Let `dq_test`
   *raise* on failure so its status drives the branch.)
2. *Files land in `/Volumes/insurance/landing/raw/` at unpredictable times and you want
   processing to start **as soon as** they arrive, not on a clock. Which trigger?* **A.** A
   **file-arrival** trigger on that Volume path (data-driven). A **cron schedule** would
   either run empty or lag; a **table-update** trigger fires on *Delta commits*, not raw file
   drops. (On FE this runs on serverless.)
3. *A task reads from a flaky external endpoint and occasionally times out, but a DQ
   validation task must **never** be retried. Configure each.* **A.** On the flaky task set
   **`max_retries`** (e.g. 2) with **`min_retry_interval_millis`** back-off and
   **`retry_on_timeout: true`**; on the DQ task set **`max_retries: 0`** (a real data failure
   shouldn't be retried into a false pass). Retries are **task-scoped** — only that task
   re-runs.

*(Bonus discriminators: a **`for_each_task`** loops a nested task over an array with a
**`concurrency`** cap; an **If/else `condition_task`** branches on a **boolean over task
values** and exposes `true`/`false` outcomes — use it to gate on `failed_rows == 0`, whereas
`run_if` gates on **task status**.)*

## 5 · References
- **Lakeflow Jobs** — create a Job, the **task graph / `depends_on`**, task types
  (notebook, **SQL**, **pipeline**, **dashboard**, run-job, condition, for-each)
- **Control flow** — **retries** (`max_retries`, `min_retry_interval_millis`,
  `retry_on_timeout`), **`run_if`** dependency conditions, **If/else condition task**,
  **for-each** task, **task values** (`dbutils.jobs.taskValues`)
- **Triggers & schedules** — **Quartz cron** schedule, **file-arrival** trigger,
  **table-update** trigger, continuous; choosing **time-based vs data-driven**
- **Databricks Asset Bundles** — `resources/*.job.yml` job definitions (referenced here,
  built in **M7**); the **M5** Declarative Pipeline the `pipeline_task` runs
