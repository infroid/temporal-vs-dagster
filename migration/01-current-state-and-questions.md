# 01 — Current state, answers and follow-up questions

> **Key takeaways**
> - The team answered Q1–Q25 on 2026-11-12. The working assumptions (A1–A12) of the first draft are replaced by these answers.
> - Four answers change the plan most: **(1)** Temporal is a **shared company cluster** with **no persistent DB yet** (RDS can be requested); **(2)** **>75 % of jobs use partitions and backfills**; **(3)** many jobs **share API rate limits/tokens**; **(4)** dbt builds **all downstream models per source, with no wait-for-all-sources rule**.
> - Follow-up questions F1–F8 (section 4) refine the partition, concurrency and shared-cluster POCs.

## 1. Confirmed setup (today, Dagster)

| Area | Fact (from the team) |
|---|---|
| Scale | 50+ jobs, about one per source |
| Dagster | Latest version; uses **jobs, ops and assets**; integrations `dagster-dbt`, `dagster-dlt`, `dagster-meltano` |
| Code layout | **1 code location, 1 image** |
| Run launch | `K8sRunLauncher` (Helm chart); per-job resources via `dagster-k8s/config` tags |
| Triggers | Dagster schedules; **a sensor starts dbt after an ingestion job finishes** |
| Partitions | **> 75 % of jobs** are partitioned and use backfills |
| Concurrency | Run queue limit + tag concurrency limits |
| Retries | Run retries |
| Quality | Dagster freshness checks + dbt tests |
| Alerting | Slack and PagerDuty on failure |
| UI use | See failed jobs, re-run, backfill |
| Secrets | AWS Secrets Manager → Kubernetes Secrets → env vars in pods |
| Ingestion | Mostly Meltano; 10–15 of 50 use dlt or plain Python; dlt state in Snowflake (default) |
| Sources | APIs, databases, CDC, files (S3/SFTP), SaaS |
| Rate limits | Many sources share API rate limits and tokens across jobs |
| dbt | dbt Core, latest version compatible with Dagster (same virtualenv) |
| dbt trigger rule | When a source finishes, **all its downstream models** build. No waiting for other sources |
| dbt overlap | Two sources finishing together **both** trigger their downstream dbt runs |
| Logs | Sumo Logic, run by another team; we must emit good logs |

How `K8sRunLauncher` per-job config works: it accepts `container_config`, `pod_spec_config`, `pod_template_spec_metadata`, `job_spec_config`, `job_metadata`; tag config on a `@job` "will only apply to the run level" ([dagster-k8s](https://dagster.io/docs/integrations/libraries/k8s/dagster-k8s)). So each run gets its own pod with its own requests/limits.

## 2. Target (Temporal)

| Area | Fact (from the team) | Impact |
|---|---|---|
| Cluster | **One company-wide Temporal on Kubernetes**, meant to replace all orchestrators | We are a tenant, not the operator. Platform POC becomes a readiness check of the shared cluster (POC-11) |
| Persistence | **No persistent DB today**; RDS can be requested | **Blocker.** "A Temporal Service's only required dependency for basic operation is the Persistence database"; supported: PostgreSQL 13.18/14.15/15.10/16.6, MySQL 5.7/8.0 (8.0.19+), Cassandra; SQLite "meant only for development and testing, not production usage" ([Persistence](https://docs.temporal.io/temporal-service/persistence)). See U15 |
| Languages | Python now; maybe others later | Temporal has 8 SDKs ([Understanding Temporal](https://docs.temporal.io/evaluate/understanding-temporal)) |
| Side by side | Allowed | Cutover by cluster with rollback ([05](05-migration-plan.md)) |
| Auth | No SSO/RBAC needed | Dropped from POC-11 |
| Run history | Clean start is fine; migrate only if easy | Not easy: no official Dagster → Temporal import found (*absence*, [matrix](../comparison-matrix.md) D12). Option: export Dagster run table to Snowflake for reference |
| "Smooth" | "Fully planned; no surprises or blockers during implementation" | Every POC has explicit pass/fail; Wave A covers every High risk |

## 3. Answers to the original questions

| Q | Answer | Q | Answer |
|---|---|---|---|
| Q1 | Company standardises on one Temporal | Q14 | Failures, re-run, backfill |
| Q2 | Shared cluster on K8s; no DB yet; RDS possible | Q15 | 1 code location/image; AWS SM → K8s Secrets → env |
| Q3 | Python (others possible) | Q16 | Mostly Meltano; 10–15 dlt/Python; dlt state in Snowflake |
| Q4 | Side by side OK | Q17 | All source types |
| Q5 | No surprises in implementation | Q18 | Many shared rate limits/tokens |
| Q6 | Latest; jobs + ops + assets; all 3 integrations | Q19 | dbt Core latest (Dagster-compatible) |
| Q7 | `K8sRunLauncher`, per-job tags | Q20 | All downstream models |
| Q8 | Schedules + dbt-after-job sensor | Q21 | No wait-for-all |
| Q9 | Partitions in > 75 % of jobs; backfills | Q22 | Both trigger dbt |
| Q10 | Run queue limit + tag limits | Q23 | Sumo Logic (other team); we log well |
| Q11 | Run retries | Q24 | No SSO/RBAC |
| Q12 | Freshness checks + dbt tests | Q25 | Clean start OK |
| Q13 | Slack + PagerDuty | | |

## 4. Follow-up questions (asked 2026-11-12)

| F | Question | Changes |
|---|---|---|
| F1 | Shared Temporal: which server version; who operates it; is it the dev server or the Helm chart; will we get our own Namespace? | POC-11; Worker Versioning needs server ≥ v1.29.1 ([Worker Versioning](https://docs.temporal.io/production-deployment/worker-deployments/worker-versioning)) |
| F2 | Can the platform team set dynamic config for our Namespace (e.g. `matching.enableFairness`) and add custom Search Attributes? Which visibility store (SQL or Elasticsearch)? | POC-06, POC-09; SQL visibility allows 10 Keyword attributes per Namespace ([Search Attributes](https://docs.temporal.io/search-attribute)) |
| F3 | Partition types used: daily/hourly time windows, static lists, dynamic, multi-dimensional? | POC-07 |
| F4 | How does a partition reach Meltano/dlt today (e.g. start/end date env vars, `--state-id` per partition, full refresh of a window)? | POC-07, POC-03 |
| F5 | Typical and largest backfill (number of partitions, parallel runs)? | POC-07 |
| F6 | Tag concurrency limits: which keys (per API token, per Snowflake warehouse, per source) and values? | POC-06 |
| F7 | Freshness checks: which assets, what thresholds, who gets paged? | POC-08 |
| F8 | Are there ops-only jobs that are not ELT (e.g. Python steps, file moves, exports)? | POC-02 scope |

## Sources

- [S125] [dagster-k8s](https://dagster.io/docs/integrations/libraries/k8s/dagster-k8s)
- [S3] [Persistence](https://docs.temporal.io/temporal-service/persistence)
- [S1] [Understanding Temporal](https://docs.temporal.io/evaluate/understanding-temporal)
- [S135] [Worker Versioning](https://docs.temporal.io/production-deployment/worker-deployments/worker-versioning)
- [S38] [Search Attributes](https://docs.temporal.io/search-attribute)
