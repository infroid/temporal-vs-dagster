# 02 — Feature mapping: Dagster → Temporal

> **Key takeaways**
> - 9 of 22 capabilities map to documented Temporal features and need **no POC**.
> - 13 need a POC, because Temporal has no built-in equivalent (data awareness, per-run pods, alerting) or because the behaviour depends on our tools (Meltano, dlt, dbt, Snowflake).
> - The target design is: **one ingest Workflow per source**, **ELT in an Activity**, and **one dbt coordinator Workflow** that decides what dbt runs and when.

## 1. Target design (proposed, to be validated by POCs)

```mermaid
flowchart TB
  SCH["Temporal Schedule per source<br/>(cron, overlap=Skip)"] --> IW
  subgraph IW["IngestWorkflow(source)"]
    direction TB
    A1["Activity: run_elt(source)<br/>Meltano or dlt subprocess<br/>heartbeat every N s"] --> A2["Activity: post-load checks<br/>(row counts, freshness)"]
  end
  IW -->|"Signal-With-Start:<br/>source_loaded(source, run_id)"| DC
  subgraph DC["DbtCoordinatorWorkflow (single, long-lived)"]
    direction TB
    Q["Queue of loaded sources"] --> G["Gate: which models are ready?<br/>(dbt manifest: source → models)"]
    G --> B["Activity: dbt build --select ...<br/>(one at a time or non-overlapping)"]
  end
  B --> SNOW[("Snowflake<br/>CLEAN / SEMANTIC")]
  A1 --> RAW[("Snowflake RAW")]
  IW -. failure .-> AL["Alert (POC-08)"]
  DC -. failure .-> AL
```

- **Why a single coordinator:** dbt does not support parallel invocations in one process ([dbt programmatic invocations](https://docs.getdbt.com/reference/programmatic-invocations)), and two overlapping `dbt build` runs on the same models "could lead to unpredictable results" (same page). One coordinator serialises or de-overlaps dbt work.
- **Why Signal-With-Start:** it "starts a Workflow when Signaling it if it does not already exist. If already running, it receives the Signal directly", atomically ([Signal with Start pattern](https://docs.temporal.io/design-patterns/signal-with-start)).
- The coordinator is an *entity Workflow*; it needs Continue-As-New to stay under history limits (51,200 events or 10K Signals) and must de-duplicate Signals, which "can be delivered more than once" ([Signal with Start pattern](https://docs.temporal.io/design-patterns/signal-with-start)).

## 2. Mapping table

Status: **Known** = documented, no POC. **Unknown** = POC needed (ID in [03](03-unknowns-register.md)).

| # | Dagster capability (today) | Temporal approach | Status | Evidence / gap |
|---|---|---|---|---|
| 1 | Cron schedule per job | Temporal Schedule (cron/calendar/interval), overlap policies, catch-up, pause | **Known** | [Schedule](https://docs.temporal.io/schedule); CLI `create`, `toggle`, `trigger`, `backfill` ([CLI schedule](https://docs.temporal.io/cli/command-reference/schedule)) |
| 2 | "Don't start a second run of the same job" | Workflow ID = source name; one open execution per ID; Conflict Policy `Fail` (default) or `UseExisting` | **Known** | "at most one Workflow Execution with a given ID running at any point in time" ([Workflow Id and Run Id](https://docs.temporal.io/workflow-execution/workflowid-runid)) |
| 3 | Op retry policy | Activity Retry Policy (default unlimited; we must set `maximum_attempts`) | **Known** | [Retry Policies](https://docs.temporal.io/encyclopedia/retry-policies) |
| 4 | Run timeouts / run monitoring | Activity Start-To-Close + Heartbeat timeouts | **Known** | [Detecting Activity failures](https://docs.temporal.io/encyclopedia/detecting-activity-failures.md) |
| 5 | Large data never passes through orchestrator | Pass IDs only; payload limit 2 MB default | **Known** | [Self-hosted defaults](https://docs.temporal.io/self-hosted-guide/defaults), [Troubleshoot payload size limit](https://docs.temporal.io/troubleshooting/blob-size-limit-error) |
| 6 | Unit tests | Activity env, time skipping, replay tests | **Known** | [Testing - Python SDK](https://docs.temporal.io/develop/python/testing-suite) |
| 7 | Metrics endpoint | Server + SDK Prometheus metrics; community Grafana dashboards | **Known** | [Monitor Temporal Platform metrics](https://docs.temporal.io/self-hosted-guide/monitoring) |
| 8 | Heavy imports in pipeline code | Keep Meltano/dlt/dbt imports in Activities; sandbox reloads non-passthrough modules per run | **Known** | [Python SDK sandbox](https://docs.temporal.io/develop/python/best-practices/python-sdk-sandbox) |
| 9 | dbt test failure skips downstream | Same: inside `dbt build`, a failing upstream test makes downstream `SKIP` | **Known** | [dbt build](https://docs.getdbt.com/reference/commands/build) |
| 10 | **Per-job pod with own CPU/memory** (`K8sRunLauncher`) | (a) Worker pools per resource class, one Task Queue each; or (b) Activity launches a K8s Job per run | **Unknown** | No Temporal doc covers per-run pods; Worker Controller scales per Worker Deployment Version, not per Workflow ([Worker Controller](https://docs.temporal.io/production-deployment/worker-deployments/kubernetes-controller)) → **U01, U02, U16** |
| 11 | Run ELT tool, stop it on cancel | Activity runs subprocess; must heartbeat to receive cancel | **Unknown** | "If the Heartbeat is not invoked, the Activity cannot receive a cancellation request" ([Cancellation - Python SDK](https://docs.temporal.io/develop/python/workflows/cancellation)); child-process kill not documented → **U03** |
| 12 | Retry after partial load | Activities "may be executed multiple times" | **Unknown** | [Activity Definition](https://docs.temporal.io/activity-definition); effect on Meltano/dlt + Snowflake not documented → **U04** |
| 13 | Incremental state (bookmarks) | Keep Meltano state backend / dlt destination state | **Unknown** | dlt restores state from destination by pipeline name + destination + dataset ([dlt state](https://dlthub.com/docs/general-usage/state)); Meltano state backends and locks ([Meltano state backends](https://discovery.meltano.com/concepts/state_backends)); continuity from Dagster-era runs untested → **U05** |
| 14 | dbt after each ingestion | Coordinator Workflow + dbt subprocess Activity | **Unknown** | dbt in-process parallelism unsupported ([dbt programmatic invocations](https://docs.getdbt.com/reference/programmatic-invocations)); behaviour under many near-simultaneous loads untested → **U06, U08** |
| 15 | `SEMANTIC` models wait for several sources (asset graph / declarative automation) | Gate in coordinator using dbt manifest | **Unknown** | Temporal has no lineage or upstream-aware triggers (*inference*, [comparison matrix](../comparison-matrix.md) D4/D5) → **U07** |
| 16 | Concurrency pools / run queue | Worker slots, Task Queue rate limit, Fairness keys, or a semaphore Workflow | **Unknown** | Rate limit is per-second, not a concurrency cap ([Downstream Rate Limiting](https://docs.temporal.io/design-patterns/downstream-rate-limiting)); Fairness off by default on self-hosted (`matching.enableFairness`) ([Priority and Fairness](https://docs.temporal.io/develop/task-queue-priority-fairness)) → **U09** |
| 17 | Sensors (file arrival, run status) | Polling Workflow on a Schedule with cursor | **Unknown** (conditional on Q8) | No sensor primitive (*inference*) → **U10** |
| 18 | Partitions + backfills | Partition key as Workflow input; Schedule backfill for time ranges | **Unknown** (conditional on Q9) | Schedule backfill exists ([Schedule](https://docs.temporal.io/schedule)); no partition status tracking (*inference*) → **U11** |
| 19 | Failure alerts (sensors / Slack) | Metrics alerts and/or failure-handler Activity | **Unknown** | Server docs recommend no alerts; "configure Grafana Alerts" ([Monitor Temporal Platform metrics](https://docs.temporal.io/self-hosted-guide/monitoring)) → **U12** |
| 20 | UI: asset catalog, lineage, re-execute from failure, launchpad | Temporal UI (executions), Search Attributes, Workflow Reset, Activity Reset; dbt docs for lineage | **Unknown** | SQL visibility allows only 10 Keyword custom Search Attributes per Namespace ([Search Attributes](https://docs.temporal.io/search-attribute)); Reset only to Workflow Task events ([Events and Event History](https://docs.temporal.io/workflow-execution/event)) → **U13** |
| 21 | Code deploys without breaking runs | Worker Versioning (Pinned) + Worker Controller | **Unknown** | Versioning needs self-hosted server ≥ v1.29.1, Python SDK ≥ 1.11 ([Worker Versioning](https://docs.temporal.io/production-deployment/worker-deployments/worker-versioning)); GA blog does not say OSS is covered ([GA blog](https://temporal.io/blog/ga-worker-versioning-public-preview-upgrade-on-continue-as-new)) → **U14** |
| 22 | Platform run by us (Helm) | Temporal Helm chart + external DB | **Unknown** | Chart README does not say "production-ready"; "You must provide persistence" ([helm-charts](https://github.com/temporalio/helm-charts)); History shard count fixed at creation ([Temporal Server](https://docs.temporal.io/temporal-service/temporal-server)) → **U15** |

## 3. Things Temporal will **not** give us (accept or build)

| Lost capability | Replacement | Decision owner |
|---|---|---|
| Asset catalog and cross-tool lineage | dbt docs/lineage for dbt models; source → model map from `manifest.json` | Team (Q14) |
| Partition status grid | Our own table or Search Attributes (if Q9 says partitions are used) | Team |
| Dagster run history | None; keep Dagster DB read-only for audit until retention passes | Team (Q25) |

## Sources

- [S10] [Schedule](https://docs.temporal.io/schedule)
- [S130] [Workflow Id and Run Id](https://docs.temporal.io/workflow-execution/workflowid-runid)
- [S7] [Retry Policies](https://docs.temporal.io/encyclopedia/retry-policies)
- [S8] [Detecting Activity failures](https://docs.temporal.io/encyclopedia/detecting-activity-failures.md)
- [S11] [Self-hosted defaults](https://docs.temporal.io/self-hosted-guide/defaults)
- [S17] [Troubleshoot payload size limit](https://docs.temporal.io/troubleshooting/blob-size-limit-error)
- [S41] [Testing - Python SDK](https://docs.temporal.io/develop/python/testing-suite)
- [S132] [Monitor Temporal Platform metrics](https://docs.temporal.io/self-hosted-guide/monitoring)
- [S134] [Temporal Python SDK sandbox](https://docs.temporal.io/develop/python/best-practices/python-sdk-sandbox)
- [S137] [dbt build](https://docs.getdbt.com/reference/commands/build)
- [S118] [Temporal Worker Controller](https://docs.temporal.io/production-deployment/worker-deployments/kubernetes-controller)
- [S128] [Cancellation - Python SDK](https://docs.temporal.io/develop/python/workflows/cancellation)
- [S6] [Activity Definition](https://docs.temporal.io/activity-definition)
- [S127] [dlt state](https://dlthub.com/docs/general-usage/state)
- [S122] [Meltano state backends](https://discovery.meltano.com/concepts/state_backends)
- [S120] [dbt programmatic invocations](https://docs.getdbt.com/reference/programmatic-invocations)
- [S129] [Signal with Start pattern](https://docs.temporal.io/design-patterns/signal-with-start)
- [S24] [Downstream Rate Limiting](https://docs.temporal.io/design-patterns/downstream-rate-limiting)
- [S124] [Task Queue Priority and Fairness](https://docs.temporal.io/develop/task-queue-priority-fairness)
- [S38] [Search Attributes](https://docs.temporal.io/search-attribute)
- [S138] [Events and Event History](https://docs.temporal.io/workflow-execution/event)
- [S135] [Worker Versioning](https://docs.temporal.io/production-deployment/worker-deployments/worker-versioning)
- [S136] [Announcing GA for Worker Versioning](https://temporal.io/blog/ga-worker-versioning-public-preview-upgrade-on-continue-as-new) — *vendor blog*
- [S126] [temporalio/helm-charts](https://github.com/temporalio/helm-charts)
- [S2] [Temporal Server](https://docs.temporal.io/temporal-service/temporal-server)
- [S131] [Temporal CLI schedule command reference](https://docs.temporal.io/cli/command-reference/schedule)
- [S125] [dagster-k8s](https://dagster.io/docs/integrations/libraries/k8s/dagster-k8s)
