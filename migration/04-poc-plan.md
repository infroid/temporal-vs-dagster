# 04 — POC plan

> **Key takeaways**
> - Re-scoped on 2026-11-12 with the team's answers ([01](01-current-state-and-questions.md)). 10 POCs. Each answers **one** question from the [unknowns register](03-unknowns-register.md) and nothing already documented.
> - **Step 0 (prerequisite):** POC-11a, a Namespace on the shared Temporal cluster **backed by a persistent DB** (RDS). Nothing else can start without it.
> - **Wave A (blocking, ~5–6 engineer-weeks):** POC-01 resources, POC-02 ELT in an Activity, POC-03 state continuity, POC-04 dbt coordinator, POC-06 shared rate limits, POC-07 partitions & backfills, POC-11b shared-cluster readiness. If any fails, revisit the decision to migrate.
> - **Wave B (hardening, ~1.5 engineer-weeks, can overlap):** POC-08 alerting + freshness + logs, POC-09 on-call tasks, POC-10 safe deploys.
> - **Dropped:** POC-05 (multi-source gating). The team has no wait-for-all rule (Q21), so each source simply builds `source:<x>+`; covered by POC-04.
> - Effort numbers are **estimates** for one engineer, not facts.

## 1. Order and dependencies

```mermaid
flowchart LR
  RDS["RDS request<br/>(platform team)"] --> P11a["POC-11a<br/>Namespace on shared cluster<br/>+ persistent DB"]
  subgraph A["Wave A: go / no-go"]
    P11b["POC-11b<br/>Shared-cluster readiness"]
    P01["POC-01<br/>Per-job resources"]
    P02["POC-02<br/>ELT in Activity"]
    P03["POC-03<br/>State continuity"]
    P04["POC-04<br/>dbt coordinator"]
    P06["POC-06<br/>Shared rate limits"]
    P07["POC-07<br/>Partitions & backfills"]
  end
  P11a --> P11b
  P11a --> P01
  P11a --> P02
  P11a --> P04
  P02 --> P03
  P03 --> P07
  P01 --> P06
  P06 --> P07
  A --> GATE{"Go / no-go"}
  subgraph B["Wave B: hardening"]
    P08["POC-08<br/>Alerts, freshness, logs"]
    P09["POC-09<br/>On-call tasks"]
    P10["POC-10<br/>Safe deploys"]
  end
  GATE --> B
```

## 2. Shared POC setup

- **Temporal:** the **shared company cluster**, our own non-prod Namespace (POC-11a). We do not install our own server. If the platform team cannot give us a persistent-DB-backed Namespace in time, a temporary dev install (official Helm chart, PostgreSQL) unblocks Wave A **except** POC-11b, which must run on the shared cluster.
- **Sources:** pick 4 real sources that cover the mix:
  - **S-big:** the largest/longest Meltano job.
  - **S-dlt:** a dlt job using `merge` or incremental loading.
  - **S-part:** a partitioned job with a typical backfill (F3–F5).
  - **S-token-pair:** two sources that share one API token or rate limit (F6).
- **Snowflake:** a POC database cloned from `RAW`/`CLEAN`/`SEMANTIC` (zero-copy clone, if allowed) so tests never touch prod.
- **Evidence:** every POC stores logs, screenshots and a one-page result in `migration/poc-results/POC-xx.md` (to be created per POC).

## 3. POC cards

### POC-11 — Shared Temporal cluster readiness (U15)

| Item | Detail |
|---|---|
| Question | Can the company's shared Temporal cluster run our production loads safely, and what must the platform team add first? |
| Not tested | That Temporal needs a persistence DB and which versions are supported (documented, [Persistence](https://docs.temporal.io/temporal-service/persistence)) |
| 11a (prerequisite) | Platform team provisions RDS (PostgreSQL 13.18/14.15/15.10/16.6 or MySQL 8.0.19+) and gives us a non-prod Namespace. Check: start a Workflow, restart every server pod, the Workflow is still there |
| 11b steps | 1. Record server version, chart/install method, shard count, visibility store (SQL or Elasticsearch) (F1, F2). 2. Ask for, and verify, Namespace retention, our custom Search Attributes, and `matching.enableFairness` for our Namespace. 3. Kill each server service pod and fail over RDS while 10 POC Workflows run. 4. Load test: 50 concurrent ingests + backfill burst from POC-07; watch our latency while another tenant is busy (noisy neighbour). 5. Agree backup/restore and upgrade process with the platform team |
| Pass | No Workflow lost or stuck after each failure · server ≥ v1.29.1 (needed for POC-10) or an upgrade date is agreed · our Search Attributes and dynamic config are applied · Namespace rate limits do not throttle our peak load · written ownership: who is paged when the cluster is down |
| Fail → | No DB: blocker, migration cannot start. Version, config or limits missing: list them as platform asks with dates; Wave A continues on a dev install |
| Effort | 0.5 day (11a, after RDS exists) + 3 days (11b) |
| Depends | Q2, F1, F2 |

### POC-01 — Per-job resource isolation (U01, U02, U16)

| Item | Detail |
|---|---|
| Question | Which pattern keeps today's per-job CPU/memory (and secret) isolation: **(a)** worker pools per resource class, or **(b)** an Activity that launches one K8s Job per run? |
| Not tested | Temporal scheduling and retries (documented) |
| Setup | (a) 3 Worker Deployments (`small`, `medium`, `large`), one Task Queue each, autoscaled on backlog (KEDA or HPA via Worker Controller). (b) One light Worker whose Activity creates a K8s Job with per-source requests/limits and service account, then polls and heartbeats until the Job ends |
| Steps | 1. Run S-big + 5 small jobs at once on each pattern. 2. Force an OOM in S-big. 3. Scale from 0 → N and back with a 2-hour Activity in flight. 4. From a `small` pod, try to read S-big's secret. 5. Ask the platform team whether KEDA / Worker Controller may be installed (U02) |
| Pass | OOM in one job does **not** fail any other job · peak memory of each job stays within its configured limit · no Activity is killed by scale-down · scale-up starts work within an agreed time · a job cannot read another job's secrets, or the exposure is no wider than today (one image, env vars) and accepted in writing |
| Fail → | If (a) fails isolation and (b) fails reliability (orphan Jobs, lost heartbeats), Temporal cannot match today's model; escalate |
| Effort | 4–5 days |
| Depends | Q7, Q15, F1 |

### POC-02 — ELT tool inside an Activity: cancel, crash, retry (U03, U04)

| Item | Detail |
|---|---|
| Question | When the Activity is cancelled, times out, or its worker dies mid-load, does the Meltano/dlt process stop, and does the retry leave `RAW` without duplicates or gaps? |
| Not tested | That Activities retry and need heartbeats to see cancel (documented) |
| Setup | Activity runs `meltano run …` or the dlt pipeline as a **subprocess**, heartbeats every N s, kills the process group on cancel. Sync Activity with a thread pool `activity_executor`, or async with non-blocking subprocess API |
| Steps | For S-big and S-dlt: 1. Cancel mid-load. 2. Kill the worker pod mid-load. 3. Let Start-To-Close expire. 4. Inject a Snowflake error after 50 % of rows. After each, let the Retry Policy run to completion |
| Pass | No orphan process or open Snowflake query after cancel/kill · after retry, `RAW` matches a clean single run on row count, primary-key uniqueness (no duplicates), the set of primary keys, and row values (hash per row) · incremental bookmark advances exactly once · cancel is seen within 2× heartbeat interval |
| Fail → | Per-source write disposition must change (e.g. append → merge) before that source migrates; record per source |
| Effort | 3–4 days |
| Depends | Q11, Q16 |

### POC-03 — Incremental state continuity from Dagster (U05)

| Item | Detail |
|---|---|
| Question | Can a Temporal-run job pick up exactly where the last Dagster run stopped, without a full re-sync, and is a run blocked if both orchestrators fire at once? |
| Not tested | That dlt restores state from the destination, and Meltano's backend list and lock defaults (documented) |
| Setup | **Isolated copy** of today's Meltano state backend, with the **same** state IDs (including per-partition IDs, if any, F4), used by both a POC Dagster and the POC Temporal (never the prod backend, so prod bookmarks cannot move). dlt: **same** pipeline name + dataset as today, against the cloned Snowflake DB (dlt state lives in the destination, so the clone isolates it) |
| Steps | 1. Run S-big, S-dlt and one partition of S-part once in Dagster. 2. Run them once in Temporal. 3. Compare bookmarks and loaded row ranges. 4. Trigger Dagster and Temporal for the same source within seconds |
| Pass | Step 2 loads only rows newer than step 1 · no gap or overlap in loaded ranges · step 4 results in one load, or a clean lock error on the second, never two writers |
| Fail → | Cutover must include a one-time state copy or controlled re-sync for that source |
| Effort | 2 days |
| Depends | Q16, F4 |

### POC-04 — dbt coordinator: overlap, failure, retry (U06, U08; closes U07, U10)

| Item | Detail |
|---|---|
| Question | When many sources finish close together, does the dbt trigger keep runs correct and freshness at least as good as today? |
| Not tested | `source:<x>+` selection and dbt's skip-on-test-failure (documented, [node selection](https://docs.getdbt.com/reference/node-selection/methods), [dbt build](https://docs.getdbt.com/reference/commands/build)) |
| Setup | Each ingest Workflow, on success, starts dbt for `dbt build --select source:<x>+` as a subprocess Activity. Two options: **(a) parallel**, one dbt subprocess per finished source (today's behaviour, Q22); **(b) coordinator**, one `DbtCoordinatorWorkflow` via Signal-With-Start that runs non-overlapping selections in parallel and queues overlapping ones. Persist `target/` (at least `run_results.json`) to object storage after each run |
| Steps | 1. Send 50 "source loaded" events in 60 s (simulated). 2. Send duplicates. 3. Two sources sharing a downstream model finish within 1 s. 4. Fail one model, fix it, re-run with `dbt retry` on a different pod. 5. (b) only: force Continue-As-New mid-stream |
| Pass | No two dbt processes build the same model at the same time, or (a) shows no wrong results and the team accepts it in writing · each event builds its models exactly once · `dbt retry` on a new pod resumes from the failure · lag (load done → model built) ≤ today's Dagster lag for the same sources · (b) no events lost across Continue-As-New |
| Fail → | (a) fails: use (b). (b) too slow: per-model lock instead of per-selection; re-test |
| Effort | 4 days |
| Depends | Q19, Q20, Q22 |

### POC-06 — Shared API rate limits and run-queue caps (U09)

| Item | Detail |
|---|---|
| Question | Can we enforce today's tag concurrency limits (e.g. "max N jobs per API token") and the global run-queue limit across all workers, even while autoscaling? |
| Not tested | Task Queue rate limit is per second and queue-wide (documented) |
| Setup | Use the real keys and values from F6. Compare: (a) one Task Queue per shared token with fixed replicas × slots; (b) a semaphore Workflow per key (acquire/release via Update or Signal); (c) Fairness keys with `matching.enableFairness=true` (only if POC-11b confirms we can set it) |
| Steps | 1. Fire 30 jobs at once with cap N=5 on one token (S-token-pair + clones). 2. Scale workers 1 → 6 during the run. 3. Kill a worker holding a permit. 4. Start the same source from a Schedule and manually at the same time (source-wide lock, [02](02-feature-mapping.md) row 2). 5. Run a 100-partition backfill of S-part and check the cap still holds |
| Pass | Concurrent count per key never exceeds N · global running count never exceeds the run-queue limit · never two runs of one source at once · a killed holder's permit is released within heartbeat timeout · no source starves for longer than the agreed time |
| Fail → | Fall back to (a) with one queue per token and accept lower utilisation |
| Effort | 3 days |
| Depends | Q10, Q18, F2, F6 |

### POC-07 — Partitions and backfills (U11)

| Item | Detail |
|---|---|
| Question | Can we run partitioned loads and backfills (> 75 % of jobs) on Temporal, track per-partition status, and keep them consistent with incremental state? |
| Not tested | Schedule backfill of **schedule times** (documented, [CLI schedule](https://docs.temporal.io/cli/command-reference/schedule)); history size limits (documented) |
| Setup | `BackfillWorkflow(source, partition_keys or range)` starts one child `IngestWorkflow` per partition with Workflow ID `ingest-<source>-<partition>`, limited by POC-06 caps; Continue-As-New every N children. Partition key passed to Meltano/dlt the same way as today (F4). Per-partition status in Search Attributes (`source`, `partition`, `status`) or a small Snowflake status table |
| Steps | 1. Scheduled run of S-part creates exactly today's partition key. 2. Backfill the largest real range from F5 (or 365 daily partitions). 3. Fail 5 partitions on purpose; re-run only those. 4. Start the same partition twice. 5. Static/dynamic or multi-dimensional partitions, if F3 says we use them. 6. Check the incremental bookmark after a backfill of old partitions |
| Pass | Every partition key runs once and lands correct rows · on-call lists failed/missing partitions of a source in one query · re-run of only failed partitions works · a duplicate start is rejected · no Workflow hits history limits · backfill does not move the live incremental bookmark backwards (or the per-partition state model from F4 is reproduced) |
| Fail → | Keep partitioned sources on Dagster until a partition library is built and passes; this blocks most of the migration |
| Effort | 5 days |
| Depends | Q9, F3, F4, F5 |

### POC-08 — Alerting, freshness and logs (U12, U17)

| Item | Detail |
|---|---|
| Question | Can we alert to Slack and PagerDuty on the same events as today, replace Dagster freshness checks, and emit logs Sumo Logic can use? |
| Setup | Server + SDK metrics to Prometheus; alert rules routed to Slack and PagerDuty; a final "on failure" Activity that posts source, run link and error; a freshness check (Schedule + Activity that reads `max(loaded_at)` per table against the F7 thresholds); structured JSON logs with `source`, `workflow_id`, `run_id`, `partition` |
| Steps | Trigger: failed run, stuck run (no heartbeat), missed schedule (worker pool down), stale `RAW` table, failed dbt test. Replay one Workflow and check log lines |
| Pass | Each case alerts within agreed time with source name and run link · PagerDuty only for the cases that page today · no alert storm on a 10-source outage · freshness alerts match F7 thresholds · logs have the fields above and no duplicate lines on replay (or duplicates are marked) |
| Effort | 3 days |
| Depends | Q12, Q13, Q23, F7 |

### POC-09 — On-call tasks without the Dagster UI (U13)

| Item | Detail |
|---|---|
| Question | Can on-call do today's three tasks (Q14) as fast and safely as in Dagster? |
| Tasks to time | 1. Find which jobs failed in the last 24 h. 2. Re-run a failed job (whole run, or only the failed step). 3. Backfill partitions X–Y for source S (uses POC-07) |
| Tools | Temporal UI, `temporal` CLI (`schedule trigger`, Workflow Reset, `activity reset`), Search Attributes, a small backfill CLI/script |
| Pass | Each task doable in ≤ 2 min by someone not on the POC team, using a 1-page runbook |
| Effort | 1.5 days |
| Depends | Q14, POC-07 |

### POC-10 — Safe deploys during long loads (U14)

| Item | Detail |
|---|---|
| Question | On the shared cluster, does Worker Versioning (Pinned) + Worker Controller let us ship new code while a multi-hour load or backfill runs? |
| Steps | Start a 2-hour ingest and a backfill on v1; deploy v2 with a changed Workflow; start a new run |
| Pass | v1 runs finish on v1 pods with no non-determinism error · new run uses v2 · v1 pods are removed after drain · replay test of v1 histories runs in CI |
| Fail → | Use Workflow patching (`patched()`) and replay tests instead of Worker Versioning |
| Effort | 2–3 days |
| Depends | F1; server ≥ v1.29.1, Python SDK ≥ 1.11 |

## 4. Go / no-go after Wave A

| Result | Decision |
|---|---|
| All Wave A POCs pass | Proceed to Wave B and build ([05](05-migration-plan.md)) |
| No persistent DB on the shared cluster (POC-11a) | **Stop.** Nothing can start until RDS is provisioned |
| POC-11b finds missing version/config/limits | Proceed only with dated platform commitments |
| POC-01 fails both patterns | Stop; per-job isolation is a hard requirement |
| POC-07 fails | Stop for partitioned jobs (> 75 %); migrating only the rest is not worth a second orchestrator |
| POC-06 fails all options | Proceed only with one queue per shared token and accepted lower throughput |
| POC-02/03 fail for some sources only | Proceed; fix write disposition / state per source before it migrates |
| POC-04 fails both options | Proceed only with an agreed simpler dbt schedule and written freshness trade-off |

## 5. Effort summary (estimates)

| Wave | POCs | Engineer-days |
|---|---|---|
| Step 0 | 11a | 0.5 + RDS lead time (platform team) |
| A | 11b, 01, 02, 03, 04, 06, 07 | 3 + 5 + 4 + 2 + 4 + 3 + 5 ≈ **26** |
| B | 08, 09, 10 | 3 + 1.5 + 3 ≈ **7.5** |

## Sources

Facts used in "Not tested" rows and setups are cited in [02](02-feature-mapping.md) and [03](03-unknowns-register.md). Key pages:

- [S3] [Persistence](https://docs.temporal.io/temporal-service/persistence)
- [S118] [Temporal Worker Controller](https://docs.temporal.io/production-deployment/worker-deployments/kubernetes-controller)
- [S119] [KEDA Temporal scaler](https://keda.sh/docs/2.21/scalers/temporal/) — *KEDA project docs*
- [S123] [Python SDK sync vs async Activities](https://docs.temporal.io/develop/python/best-practices/python-sdk-sync-vs-async)
- [S128] [Cancellation - Python SDK](https://docs.temporal.io/develop/python/workflows/cancellation)
- [S127] [dlt state](https://dlthub.com/docs/general-usage/state)
- [S122] [Meltano state backends](https://discovery.meltano.com/concepts/state_backends)
- [S120] [dbt programmatic invocations](https://docs.getdbt.com/reference/programmatic-invocations)
- [S137] [dbt build](https://docs.getdbt.com/reference/commands/build)
- [S140] [dbt node selection methods](https://docs.getdbt.com/reference/node-selection/methods)
- [S133] [dbt retry](https://docs.getdbt.com/reference/commands/retry)
- [S129] [Signal with Start pattern](https://docs.temporal.io/design-patterns/signal-with-start)
- [S124] [Task Queue Priority and Fairness](https://docs.temporal.io/develop/task-queue-priority-fairness)
- [S130] [Workflow Id and Run Id](https://docs.temporal.io/workflow-execution/workflowid-runid)
- [S131] [Temporal CLI schedule command reference](https://docs.temporal.io/cli/command-reference/schedule)
- [S11] [Self-hosted defaults](https://docs.temporal.io/self-hosted-guide/defaults)
- [S38] [Search Attributes](https://docs.temporal.io/search-attribute)
- [S139] [Reset an Activity](https://docs.temporal.io/activity-operations/reset)
- [S135] [Worker Versioning](https://docs.temporal.io/production-deployment/worker-deployments/worker-versioning)
- [S126] [temporalio/helm-charts](https://github.com/temporalio/helm-charts)
- [S132] [Monitor Temporal Platform metrics](https://docs.temporal.io/self-hosted-guide/monitoring)
