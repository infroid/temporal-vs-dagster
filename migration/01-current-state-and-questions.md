# 01 — Current state, open questions and working assumptions

> **Key takeaways**
> - Confirmed from the team: Dagster on Kubernetes via the Helm chart, job-specific resource allocation, ~50 jobs (≈1 per source), Meltano or dlt into Snowflake `RAW`, dbt runs after each ingestion job into `CLEAN` and then `SEMANTIC`.
> - 25 open questions (Q1–Q25) decide which POCs are needed and how big they are.
> - Until answers arrive, the plan uses the assumptions **A1–A12** below. Each POC lists which assumptions it depends on.

## 1. What is confirmed

| Area | Fact (from the team) |
|---|---|
| Orchestrator | Dagster, deployed with the Dagster Helm chart on Kubernetes |
| Resource control | Job-specific resource allocation (CPU/memory per job) |
| Scale | 50+ jobs, roughly one per source |
| Ingestion tools | Meltano or dlt (mix) |
| Landing | Snowflake `RAW` database |
| Transform | dbt runs when a job's ingestion finishes; writes `CLEAN`, then `SEMANTIC` (joins, filters, complex logic) |
| Direction | Move from Dagster to Temporal |

### How Dagster does per-job resources today (for reference)

- `K8sRunLauncher` takes `run_k8s_config` with `container_config`, `pod_spec_config`, `pod_template_spec_metadata`, `job_spec_config`, `job_metadata` ([dagster-k8s](https://dagster.io/docs/integrations/libraries/k8s/dagster-k8s)).
- Tag-based config on a `@job` "will only apply to the run level"; per-step config must be set on each `@op` ([dagster-k8s](https://dagster.io/docs/integrations/libraries/k8s/dagster-k8s)).
- `k8s_job_executor` "launches steps as Kubernetes Jobs" ([dagster-k8s](https://dagster.io/docs/integrations/libraries/k8s/dagster-k8s)).

Net effect: each run (or step) gets its **own pod** with its **own requests/limits**. Temporal has no equivalent launcher (see [02](02-feature-mapping.md) and U01).

## 2. Open questions

Asked in the project thread on 2026-11-11. **Impact** says which POC or decision the answer changes.

| Q | Question | Impact |
|---|---|---|
| Q1 | Why leave Dagster; which pain points must Temporal fix? | Success criteria |
| Q2 | Self-hosted Temporal on same K8s (Helm) or Temporal Cloud? Persistence DB? Elasticsearch? | POC-11 scope |
| Q3 | Python-only workers? | All POCs |
| Q4 | Timeline; can both run side by side? | [05](05-migration-plan.md) |
| Q5 | Definition of "100% smooth" (no dupes, SLAs, alerts) | Pass criteria |
| Q6 | Dagster version; assets vs ops; `dagster-dbt` / `dagster-dlt` / `dagster-meltano` / subprocess? | POC-05 |
| Q7 | Run launcher / executor; per-job tags; node selectors, tolerations, service accounts, secrets, volumes? | POC-01 |
| Q8 | Cron schedules vs sensors vs declarative automation; counts | POC-07 (conditional) |
| Q9 | Partitions / backfills: frequency and size | POC-07 (conditional) |
| Q10 | Concurrency controls: run queue, tag limits, pools | POC-06 |
| Q11 | Run retries / op retry policies | POC-02 |
| Q12 | Asset checks, freshness policies; do dbt test failures block downstream? | POC-04, POC-08 |
| Q13 | Alerting channel and mechanism | POC-08 |
| Q14 | Who uses the Dagster UI and for what | POC-09 |
| Q15 | Code locations / images; secrets source | POC-01 |
| Q16 | Meltano vs dlt split; Meltano state backend; dlt state location | POC-02, POC-03 |
| Q17 | Source kinds; largest run (time, size) | POC-01, POC-02 |
| Q18 | Shared API rate limits / tokens across jobs | POC-06 |
| Q19 | dbt Core or dbt Cloud; version | POC-04 |
| Q20 | dbt selection after a source finishes (`source:x+` or full) | POC-04, POC-05 |
| Q21 | Do `SEMANTIC` models wait for **all** upstream sources? | POC-05 |
| Q22 | What happens when two jobs finish together and both trigger dbt? | POC-04 |
| Q23 | Metrics/logs stack; CI/CD | POC-08, POC-10 |
| Q24 | SSO / RBAC needed on UI? | POC-11 |
| Q25 | Migrate run history or clean start? | [05](05-migration-plan.md) |

## 3. Working assumptions (until answered)

| A | Assumption | Replaces |
|---|---|---|
| A1 | Self-hosted Temporal OSS on the same Kubernetes platform, PostgreSQL persistence, SQL visibility | Q2 |
| A2 | Workers in Python (`temporalio`) | Q3 |
| A3 | Dagster and Temporal can run side by side for several weeks | Q4 |
| A4 | "Smooth" = no data loss, no duplicate rows in `RAW`, freshness no worse than today, same alert coverage | Q5 |
| A5 | Jobs run Meltano/dlt in their own pod with per-job requests/limits; some use per-job secrets | Q7, Q15 |
| A6 | Mostly cron schedules; a few sensors | Q8 |
| A7 | Few or no partitioned assets | Q9 |
| A8 | Some concurrency limits exist (e.g. cap on Snowflake loads) | Q10 |
| A9 | dbt Core 1.x, run in-cluster; dagster-dbt assets | Q6, Q19 |
| A10 | After source X loads, its downstream models run; `SEMANTIC` models that join sources depend on Dagster's asset graph to decide when to run | Q20, Q21 |
| A11 | Alerts go to Slack (or similar) via Dagster sensors | Q13 |
| A12 | Clean start for run history; no history migration | Q25 |
