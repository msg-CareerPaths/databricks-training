# 7. Orchestration with Lakeflow Jobs [3-4 hours]

_Milestone M6 · Exam domain 4: Working with Lakeflow Jobs_

**Goal:**
- Orchestrate the end-to-end flow as a scheduled **Lakeflow Jobs** DAG with retries, a
  **conditional** branch, and the right **trigger**.

## Mandatory Materials:

**Videos:**
- Databricks Academy — *Deploy Workloads with Lakeflow Jobs*

**Reading:**
 - [Studybook M6 — Lakeflow Jobs](https://github.com/msg-CareerPaths/databricks-training/blob/main/docs/studybook/M6_lakeflow_jobs.md)
 - [Lakeflow Jobs docs](https://docs.databricks.com/aws/en/jobs/configure-job)
 - Stub: [resources/insurance_ingest.job.yml](https://github.com/msg-CareerPaths/databricks-training/blob/main/resources/insurance_ingest.job.yml)

## Insurance Lakehouse:
 > 1. Complete the job in `resources/insurance_ingest.job.yml`: tasks
 >    `land_delta → run_pipeline → dq_tests → check_dq → publish_dashboard / notify_dq_failure`.
 > 2. Add **retries** (`max_retries`, `min_retry_interval_millis`) to the brittle tasks.
 > 3. Use a **condition task** so the DAG branches on the DQ result (PASS → publish, FAIL → notify).
 > 4. Pick a **trigger**: file-arrival (new telematics) vs a cron schedule vs table-update — and
 >    justify the choice.
 >
 > **Acceptance:** the DAG runs; the conditional branch routes on DQ pass/fail; failures email
 > you; retries are configured; you can explain time-based vs data-driven triggers.

## Further Resources:
- [Task types & dependencies](https://docs.databricks.com/aws/en/jobs/) · [Trigger types](https://docs.databricks.com/aws/en/jobs/triggers.html)
