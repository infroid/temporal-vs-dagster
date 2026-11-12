# Dagster → Temporal migration plan

Plan to move ~50 ingestion jobs (Meltano or dlt → Snowflake `RAW`, then dbt → `CLEAN` → `SEMANTIC`) from self-hosted Dagster on Kubernetes to the company's **shared Temporal cluster**.

| | |
|---|---|
| As of | 2026-11-12. All team questions (Q1–Q25, F1–F11) answered |
| Status | Plan ready. **Next step:** platform PR that registers our non-prod Namespace (Step 0, POC-11a) |
| Top risk | Partitions and backfills (U11): > 75 % of jobs, every partition type down to 5-minute, backfills of ~2 years |
| Then | Per-job resource isolation (U01), shared API rate limits (U09), ELT state and duplicates under retries (U04, U05) |
| Builds on | [1-comparison.md](1-comparison.md). Citations `[S#]` link to [sources.md](sources.md) |
| Estimates | Effort and timeline numbers are **estimates**, not facts. Risk ratings are **judgement** |

**Status labels used below**

| Label | Meaning |
|---|---|
| **Known** | Stated in official docs we fetched; cited. No POC |
| **Unknown** | Not in docs, or depends on our setup. Needs a POC |
| *Inference* | Our reasoning from docs, not a doc statement. Treated as Unknown when it carries risk |
| Q#, F# | Team answer that drives a decision ([§2.3](#23-team-answers)) |

## Contents

1. [Big picture](#1-big-picture)
2. [Current state and target](#2-current-state-and-target)
3. [Target design](#3-target-design)
4. [Feature mapping](#4-feature-mapping)
5. [Unknowns register](#5-unknowns-register)
6. [POC plan](#6-poc-plan)
7. [Migration plan](#7-migration-plan)

---

## 1. Big picture

```mermaid
flowchart LR
  subgraph Today["Today: Dagster on K8s"]
    DS[Schedules + partitions] --> DR[K8sRunLauncher<br/>1 pod per run]
    DR --> ELT1[Meltano / dlt]
    ELT1 --> RAW1[(RAW)]
    RAW1 --> DBT1[dbt via sensor<br/>source:x+]
    DBT1 --> CLEAN1[(CLEAN)] --> SEM1[(SEMANTIC)]
  end
  subgraph Target["Target: shared Temporal cluster + our workers"]
    TS[Temporal Schedules /<br/>backfill Workflow] --> WF[Ingest Workflow<br/>per source + partition]
    WF --> ACT[Activity: run Meltano / dlt<br/>in worker pod or K8s Job]
    ACT --> RAW2[(RAW)]
    WF -->|on success| DC[dbt trigger<br/>or coordinator]
    DC --> DBT2[Activity: dbt build<br/>subprocess]
    DBT2 --> CLEAN2[(CLEAN)] --> SEM2[(SEMANTIC)]
  end
  Today -. cut over by dependency cluster .-> Target
```

```mermaid
flowchart LR
  S0["Step 0<br/>Namespace PR<br/>POC-11a"] --> WA["Wave A POCs<br/>~28 eng-days<br/>go / no-go"]
  WA --> WB["Wave B POCs<br/>~8 eng-days"]
  WB --> F["Foundation<br/>3–4 wks"]
  F --> P["Pilot<br/>1 cluster, 2 wks"]
  P --> W["Waves<br/>~10 sources / wk"]
  W --> D["Decommission<br/>Dagster off"]
```

---

## 2. Current state and target

### 2.1 Today (Dagster)

| Area | Fact (from the team) |
|---|---|
| Scale | 50+ jobs, about one per source |
| Dagster | Latest version; **jobs, ops and assets**; `dagster-dbt`, `dagster-dlt`, `dagster-meltano` |
| Code layout | 1 code location, 1 image |
| Run launch | `K8sRunLauncher` (Helm chart); per-job resources via `dagster-k8s/config` tags |
| Triggers | Schedules; **one sensor**: starts dbt after an ingestion job finishes |
| Partitions | **> 75 % of jobs**; all types: daily, hourly, **5-minute**, static, dynamic, multi-dimensional |
| Partition input | Through **Launchpad run config**, with defaults |
| Backfills | **~2 years** of past data; usually **several partitions per run** (Launchpad date range); depends on the job |
| Concurrency | Run queue limit + "many" tag concurrency limits |
| Retries | Run retries |
| Quality | Freshness checks on **almost all assets** (Slack on failure) + dbt tests |
| Alerting | Slack and PagerDuty on failure |
| UI use | See failed jobs, re-run, backfill |
| Secrets | AWS Secrets Manager → Kubernetes Secrets → env vars in pods |
| Ingestion | Mostly Meltano; 10–15 of 50 use dlt or plain Python; dlt state in Snowflake (default) |
| Other jobs | Some non-ELT ops jobs |
| Sources | APIs, databases, CDC, files (S3/SFTP), SaaS |
| Rate limits | Many sources share API rate limits and tokens |
| dbt | dbt Core, latest Dagster-compatible version (same virtualenv) |
| dbt trigger rule | When a source finishes, **all its downstream models** build; no waiting for other sources; two sources finishing together **both** trigger dbt |
| Logs | Sumo Logic, run by another team; we must emit good logs |

`K8sRunLauncher` accepts `container_config`, `pod_spec_config`, `pod_template_spec_metadata`, `job_spec_config`, `job_metadata`; tag config on a `@job` "will only apply to the run level" [S125](sources.md#s125). Each run gets its own pod with its own requests/limits.

### 2.2 Target (shared Temporal cluster)

Cluster facts come from the team's review of the internal platform repository. They are the team's findings, not checked by us. Internal repo, team and tenant names are left out of this public repo.

| Area | Fact | Impact |
|---|---|---|
| Ownership | One company-wide Temporal on Kubernetes, meant to replace all orchestrators; another team operates it | We are a tenant. Platform POC is a readiness check (POC-11) |
| Version | Server **1.32.2**, Web UI 2.56.1, upstream Helm chart 1.8.0 (vendored) | Worker Versioning needs ≥ v1.29.1: met [S135](sources.md#s135) |
| Persistence | Default store **and** visibility on **managed durable MySQL** | Blocker closed. The default store holds tasks, mutable Workflow state, the event history and Namespace metadata; it is "the only required dependency for basic operation"; visibility only enables "show all running Workflow Executions" [S3](sources.md#s3) |
| Visibility | **SQL on MySQL 8**, own DB; no Elasticsearch/OpenSearch; S3 only for archival | 10 Keyword custom Search Attributes per Namespace [S38](sources.md#s38) |
| Auth | Server build patched with a **custom JWT authorizer**; it denies unregistered Namespaces | Our Workers need an allowed token issuer |
| Worker token | Managed by another team; each pod gets a working token at a fixed file path | POC-11a checks a long-lived Worker survives token rotation |
| Namespace | One per consumer service, added by a PR to the platform values files (namespace + token issuer); a namespace-manager job creates it and enables archival | Step 0 = Namespace PR |
| Dynamic config | **Not self-service**: one cluster-wide ConfigMap (empty today); changes by platform PR, cluster-wide unless scoped by Namespace | Fairness (`matching.enableFairness`) needs a platform PR |
| Search Attributes | Yes, per Namespace, registered by a Namespace admin with `temporal operator search-attribute create`; not declarative | Register once in POC-11b |
| Languages | Python now; maybe others later | 8 SDKs [S1](sources.md#s1) |
| Side by side | Allowed | Cutover by cluster with rollback ([§7](#7-migration-plan)) |
| UI auth | No SSO/RBAC needed for our use | Out of scope |
| Run history | Clean start is fine | No official Dagster → Temporal import (*absence*); optional export of the Dagster run table to Snowflake |
| Goal | "Fully planned; no surprises or blockers during implementation" | Every POC has pass/fail; Wave A covers every High risk |

### 2.3 Team answers

| ID | Answer | Drives |
|---|---|---|
| Q1 | Company standardises on one Temporal | Shared cluster |
| Q2 | Shared cluster on K8s (persistence confirmed in F9) | POC-11 |
| Q3 | Python (others possible) | SDK choice |
| Q4 | Side by side is OK | Cutover by cluster |
| Q5 | No surprises in implementation | Pass/fail per POC |
| Q6 | Latest Dagster; jobs + ops + assets; dbt, dlt, Meltano integrations | Scope |
| Q7 | `K8sRunLauncher`, per-job tags | POC-01 |
| Q8 | Schedules + one dbt-after-job sensor | U10 closed |
| Q9 | Partitions in > 75 % of jobs; backfills | POC-07 |
| Q10 | Run queue limit + tag limits | POC-06 |
| Q11 | Run retries | POC-02 |
| Q12 | Freshness checks + dbt tests | POC-08 |
| Q13 | Slack + PagerDuty | POC-08 |
| Q14 | UI used for failures, re-run, backfill | POC-09 |
| Q15 | 1 code location/image; AWS SM → K8s Secrets → env | U16 |
| Q16 | Mostly Meltano; 10–15 dlt/Python; dlt state in Snowflake | POC-02, POC-03 |
| Q17 | All source types | POC source mix |
| Q18 | Many shared rate limits/tokens | POC-06 |
| Q19 | dbt Core latest (Dagster-compatible) | POC-04 |
| Q20 | Build all downstream models of a source | `source:<x>+` |
| Q21 | No wait-for-all | U07 closed |
| Q22 | Both overlapping sources trigger dbt | POC-04 |
| Q23 | Logs to Sumo Logic (other team) | POC-08 |
| Q24 | No SSO/RBAC | Out of scope |
| Q25 | Clean start on run history | No history import |
| F1 | Server 1.32.2, chart 1.8.0, custom JWT authorizer, Namespace by PR | POC-10 version check passes; POC-11a |
| F2 | SQL visibility on MySQL 8; dynamic config by platform PR; Search Attributes by admin CLI | POC-06 option c, POC-11b |
| F3 | All partition types, down to 5-minute | POC-07 covers each type |
| F4 | Partition reaches the job via Launchpad run config + defaults | Typed Workflow input (POC-07, POC-09) |
| F5 | Backfills of ~2 years | Batching (POC-07) |
| F6 | "Many" tag concurrency limits | Export them as POC-06 input |
| F7 | Freshness on almost all assets; Slack on failure | POC-08 |
| F8 | Some non-ELT ops jobs | POC-02 includes one; inventory in P0 |
| F9 | Default persistence store is managed durable MySQL | U15 blocker closed |
| F10 | Worker token managed by another team, mounted at a file path | POC-11a token-rotation check |
| F11 | Backfill input depends on the job; usually a range per run | P0 inventory of range vs single-partition jobs; POC-07 batches |

**Partition scale (arithmetic):** 2 years ≈ 730 daily, 17,520 hourly, **210,240 five-minute** partitions. One child Workflow per partition does not fit: 2,000 pending children per Workflow, ≤ 500 recommended [S11](sources.md#s11). Partitions must be **batched**.

---

## 3. Target design

Proposed; validated by the POCs.

```mermaid
flowchart TB
  SCH["Temporal Schedule per source<br/>(cron, overlap=Skip)"] --> IW
  BF["BackfillWorkflow<br/>(partition range, POC-07)"] --> IW
  subgraph IW["IngestWorkflow(source, partition)"]
    direction TB
    A1["Activity: run_elt(source)<br/>Meltano or dlt subprocess<br/>heartbeat every N s"] --> A2["Activity: post-load checks<br/>(row counts, freshness)"]
  end
  IW -->|"Signal-With-Start:<br/>source_loaded(source, run_id)"| DC
  subgraph DC["DbtCoordinatorWorkflow (single, long-lived)"]
    direction TB
    Q["Queue of loaded sources"] --> G["Select: source:x+<br/>(all downstream models, no waiting)"]
    G --> B["Activity: dbt build --select source:x+<br/>(parallel if selections do not overlap)"]
  end
  B --> SNOW[("Snowflake<br/>CLEAN / SEMANTIC")]
  A1 --> RAW[("Snowflake RAW")]
  IW -. failure .-> AL["Alert (POC-08)"]
  DC -. failure .-> AL
```

| Design point | Why (cited) |
|---|---|
| One `IngestWorkflow` per source (and partition batch); ELT tool runs as a subprocess in an Activity | Temporal runs code only in our Workers [S1](sources.md#s1); keep heavy imports in Activities, the sandbox reloads non-passthrough modules per run [S134](sources.md#s134) |
| dbt builds `source:<x>+` after each load | Selects models that read source `x` and their descendants [S140](sources.md#s140); no wait-for-all (Q21) |
| Maybe a coordinator | dbt does not support parallel invocations in one process; overlapping `dbt build` on the same models "could lead to unpredictable results" [S120](sources.md#s120). POC-04 compares the coordinator with today's parallel behaviour |
| Signal-With-Start | "starts a Workflow when Signaling it if it does not already exist. If already running, it receives the Signal directly", atomically [S129](sources.md#s129) |
| Coordinator housekeeping | Entity Workflow: needs Continue-As-New to stay under history limits (51,200 events or 10K Signals) and must de-duplicate Signals, which "can be delivered more than once" [S129](sources.md#s129) |

---

## 4. Feature mapping

**Known** = documented, no POC. **Unknown** = POC needed (U-ID in [§5](#5-unknowns-register)).

| # | Dagster capability (today) | Temporal approach | Status | Evidence / gap |
|---|---|---|---|---|
| 1 | Cron schedule per job | Schedule (cron/calendar/interval), overlap policies, catch-up, pause | **Known** | [S10](sources.md#s10); CLI `create`, `toggle`, `trigger`, `backfill` [S131](sources.md#s131) |
| 2 | Don't start a second run of the same job | Scheduled runs: overlap `Skip`. Other start paths (manual, backfill): a source-wide lock every path takes, e.g. a fixed per-source lock Workflow ID with Conflict Policy `Fail` | **Known** (build) | "at most one Workflow Execution with a given ID running at any point in time" [S130](sources.md#s130); but a Schedule appends "the Action's timestamp … to the Workflow Id" [S10](sources.md#s10), so IDs alone do not exclude runs from different paths. Tested in POC-06. `Skip` drops an overlapping action, leaving a partition unloaded: a scheduled gap check reads the partition status table and backfills it (POC-07) |
| 3 | Op retry policy | Activity Retry Policy (default unlimited; set `maximum_attempts`) | **Known** | [S7](sources.md#s7) |
| 4 | Run timeouts / run monitoring | Start-To-Close caps one attempt; Heartbeat Timeout detects stalls; Schedule-To-Close caps the total across retries | **Known** | [S8](sources.md#s8) [S115](sources.md#s115) |
| 5 | Large data never passes through the orchestrator | Pass IDs only; payload limit 2 MB default | **Known** | [S11](sources.md#s11) [S17](sources.md#s17) |
| 6 | Unit tests | Activity env, time skipping, replay tests | **Known** | [S41](sources.md#s41) |
| 7 | Metrics endpoint | Server + SDK Prometheus metrics; community Grafana dashboards | **Known** | [S132](sources.md#s132) |
| 8 | Heavy imports in pipeline code | Keep Meltano/dlt/dbt imports in Activities | **Known** | [S134](sources.md#s134) |
| 9 | dbt test failure skips downstream | Same: inside `dbt build`, a failing upstream test makes downstream `SKIP` | **Known** | [S137](sources.md#s137) |
| 10 | **Per-job pod with own CPU/memory** | (a) Worker pools per resource class, one Task Queue each; or (b) Activity launches a K8s Job per run | **Unknown** | No Temporal doc covers per-run pods; Worker Controller scales per Worker Deployment Version, not per Workflow [S118](sources.md#s118) → U01, U02, U16 |
| 11 | Run ELT tool, stop it on cancel | Subprocess in an Activity; must heartbeat to receive cancel | **Unknown** | "If the Heartbeat is not invoked, the Activity cannot receive a cancellation request" [S128](sources.md#s128); child-process kill not documented → U03 |
| 12 | Retry after partial load | Activities "may be executed multiple times" | **Unknown** | [S6](sources.md#s6); effect on Meltano/dlt + Snowflake not documented → U04 |
| 13 | Incremental state (bookmarks) | Keep Meltano state backend / dlt destination state | **Unknown** | dlt restores state by pipeline name + destination + dataset [S127](sources.md#s127); Meltano backends and locks [S122](sources.md#s122); continuity from Dagster-era runs untested → U05 |
| 14 | dbt after each ingestion | Coordinator Workflow + dbt subprocess Activity | **Unknown** | dbt in-process parallelism unsupported [S120](sources.md#s120); behaviour under many near-simultaneous loads untested → U06, U08 |
| 15 | Build all downstream models of a loaded source | `dbt build --select source:<x>+` | **Known** | [S140](sources.md#s140); U07 closed |
| 16 | Run queue limit + tag concurrency limits | Worker slots, Task Queue rate limit, Fairness keys, or a semaphore Workflow | **Unknown** | Rate limit is per second, not a concurrency cap [S24](sources.md#s24); Fairness off by default on self-hosted (`matching.enableFairness`) [S124](sources.md#s124) → U09 |
| 17 | Sensor: start dbt after ingestion (the only sensor) | Ingest Workflow starts dbt (or signals the coordinator) on success | **Known** | [S129](sources.md#s129); U10 closed |
| 18 | Partitions + backfills | Partition library + Launchpad config as typed Workflow input; `BackfillWorkflow` runs **batches** as children; status in a Snowflake table | **Unknown (High)** | Schedule backfill only re-runs schedule times [S131](sources.md#s131); no partition status tracking (*inference*) → U11 |
| 19 | Slack + PagerDuty; freshness checks | Metrics alerts, failure-handler Activity, scheduled freshness check | **Unknown** | Server docs recommend no alerts; "configure Grafana Alerts" [S132](sources.md#s132) → U12, U17 |
| 20 | UI: see failures, re-run, backfill | Temporal UI, Search Attributes, Workflow Reset, Activity Reset; dbt docs for lineage | **Unknown** | SQL visibility: 10 Keyword Search Attributes per Namespace [S38](sources.md#s38); Reset only to Workflow Task events [S138](sources.md#s138) → U13 |
| 21 | Code deploys without breaking runs | Worker Versioning (Pinned) + Worker Controller | **Unknown** | Needs server ≥ v1.29.1 (cluster: 1.32.2), Python SDK ≥ 1.11 [S135](sources.md#s135); GA blog does not say OSS is covered [S136](sources.md#s136) → U14 |
| 22 | Platform (today: our Dagster Helm install) | Shared cluster, our own Namespace via platform PR, JWT token for Workers | **Unknown** | Persistence is "the only required dependency" [S3](sources.md#s3); durable MySQL confirmed (F9); HA, limits, paging open → U15 |

**What Temporal will not give us (accept or build)**

| Lost capability | Replacement | Owner |
|---|---|---|
| Asset catalog and cross-tool lineage | dbt docs/lineage (team uses the UI only for failures, re-run, backfill) | Team |
| Partition status grid and Launchpad | Snowflake status table + small launch/backfill CLI (POC-07, POC-09) | Team |
| Freshness checks (almost all assets) | `dbt source freshness` on a Schedule + Slack alert (POC-08) | Team |
| Dagster run history | Clean start; optional export of the run table to Snowflake; Dagster DB read-only until retention passes | Team |

---

## 5. Unknowns register

```mermaid
quadrantChart
  title Open unknowns: likelihood of a problem vs impact
  x-axis Low likelihood --> High likelihood
  y-axis Low impact --> High impact
  quadrant-1 Must pass before cutover
  quadrant-2 Prove early
  quadrant-3 Verify during build
  quadrant-4 Watch
  U11: [0.9, 0.95]
  U15: [0.55, 0.9]
  U01: [0.75, 0.85]
  U09: [0.7, 0.75]
  U04: [0.6, 0.9]
  U05: [0.55, 0.9]
  U06: [0.6, 0.7]
  U12: [0.6, 0.6]
  U03: [0.55, 0.55]
  U13: [0.6, 0.45]
  U02: [0.5, 0.45]
  U08: [0.45, 0.5]
  U14: [0.2, 0.55]
  U18: [0.5, 0.5]
  U16: [0.2, 0.4]
  U17: [0.3, 0.3]
```

| Risk | Unknowns |
|---|---|
| **High** (must pass before any cutover) | U11, U15, U01, U09, U04, U05, U06 |
| Medium | U12, U03, U13, U02, U08, U18 |
| Low | U14, U16, U17 |
| Closed | U07, U10, SSO/RBAC |

### 5.1 Open unknowns

| ID | Question | What docs say | Gap | Risk | POC | Ref |
|---|---|---|---|---|---|---|
| **U11** | Can we run partitioned jobs and backfills without Dagster's partition model? | Dagster tracks partitions and backfill status; default one run per partition [S64](sources.md#s64) [S65](sources.md#s65). Temporal Schedule backfill only re-runs **schedule action times** [S131](sources.md#s131). Per Workflow: 2,000 pending children, ≤ 500 recommended; history error at 51,200 events or 50 MB [S11](sources.md#s11) | No partition concept, no missing/failed view, no range launcher; static, dynamic and multi-dimensional types (*inference*). ~210,240 five-minute partitions over 2 years, so batching is required. Some jobs may accept only one partition per run (F11) | **High (top)** | POC-07 | Q9, F3–F5, F11 |
| **U15** | Is the shared cluster fit for our production loads? | "only required dependency … is the Persistence database"; supported versions [S3](sources.md#s3). Shard count cannot change after creation [S2](sources.md#s2) | Persistence durable (F9); token provided (F10). **Open:** HA, backups, noisy-neighbour limits, who is paged | **High** | POC-11 | Q2, F1, F2, F9, F10 |
| **U01** | Can we keep per-job CPU/memory isolation without a per-run pod? | `K8sRunLauncher`: own pod and config per run [S125](sources.md#s125). Temporal runs code only in our Workers [S1](sources.md#s1). Worker Controller scales per Worker Deployment Version [S118](sources.md#s118). Resource-based tuner targets "can still be exceeded" [S121](sources.md#s121) | No documented "one pod per Activity". Shared pods: one OOM can kill every Activity in the pod (*inference*). Launching a K8s Job from an Activity is not documented | **High** | POC-01 | Q7 |
| **U09** | Can we enforce shared API token limits and run-queue limits, like tag concurrency limits? | Task Queue rate limit is per second, queue-wide; slots "are not throughput limits" [S24](sources.md#s24). Fairness needs `matching.enableFairness=true` on self-hosted [S124](sources.md#s124) | No built-in concurrency cap per key. Worker slots × replicas change with autoscaling (*inference*). Fairness needs a platform PR (F2). "Many" limits exist (F6) | **High** | POC-06 | Q10, Q18, F6 |
| **U04** | Does a retry after a partial load create duplicates or gaps in `RAW`? | Activities "may be executed multiple times"; make writes idempotent [S6](sources.md#s6) | Depends on each Meltano target / dlt write disposition and when state commits | **High** | POC-02 | Q11, Q16 |
| **U05** | Can Temporal-run jobs continue from Dagster-era state without a full re-sync? | dlt restores `_dlt_pipeline_state` when pipeline name, destination and dataset match [S127](sources.md#s127). Meltano backends: systemdb (default), local, S3, Azure, GCS, Snowflake; lock defaults 10 s / 1 s [S122](sources.md#s122) | Whether `dagster-meltano` state IDs (incl. per-partition) can be reproduced exactly (F4) | **High** | POC-03 | Q16, F4 |
| **U06** | Do overlapping dbt runs (two sources finishing together) stay correct? | No parallel invocations in one process; concurrent `run`/`build` on the same models "could lead to unpredictable results"; separate processes are the supported way to parallelise [S120](sources.md#s120) | Keep today's parallel behaviour, or serialise overlapping selections; freshness impact of each | **High** | POC-04 | Q20, Q22 |
| **U12** | Can we alert like today: Slack + PagerDuty on failure, plus freshness? | Prometheus metrics, no recommended alerts [S132](sources.md#s132). No freshness checks in Temporal (*inference*); `dbt source freshness` "exits with a nonzero exit code" when stale [S141](sources.md#s141) | Per-source alert with link; freshness on almost all assets with Slack (F7); stuck/missed runs; logs to Sumo Logic | Medium | POC-08 | Q12, Q13, F7 |
| **U03** | Do cancel, timeout and worker crash cleanly stop the Meltano/dlt subprocess? | Cancel needs Heartbeat + Heartbeat Timeout [S128](sources.md#s128). Sync Activities need an `activity_executor` [S123](sources.md#s123) | Killing the child process and its Snowflake session is our code | Medium | POC-02 | Q11 |
| **U13** | Can on-call see failures, re-run and backfill without the Dagster UI? | Temporal UI lists executions [S31](sources.md#s31); Reset from a Workflow Task event [S138](sources.md#s138); `temporal activity reset` [S139](sources.md#s139); 10 Keyword Search Attributes on SQL visibility [S38](sources.md#s38) | Speed and safety of today's three tasks, especially "backfill partitions X–Y for source S" and starting a run with edited Launchpad-style config | Medium | POC-09 | Q14 |
| **U02** | Do worker pools scale from backlog reliably, and may we install the scaler? | KEDA Temporal scaler: backlog-based, community-maintained, scale-to-zero activation "may not be reliable" [S119](sources.md#s119). Worker Controller supports HPA/KEDA [S118](sources.md#s118) | Scale latency; safe scale-down with hours-long Activities; cluster operators need platform approval | Medium | POC-01 | F1 |
| **U08** | Do dbt failures and retries behave well inside an Activity? | `dbt build` skips downstream on test failure [S137](sources.md#s137); `dbt retry` reads `run_results.json` [S133](sources.md#s133); exit codes 0/1/2 [S120](sources.md#s120) | `run_results.json` must survive pod changes; which exit codes to retry | Medium | POC-04 | Q12, Q19 |
| **U18** | Do non-ELT ops jobs and Launchpad run config port cleanly? | Activities run any code in our Workers [S1](sources.md#s1) | Inventory unknown; run config + defaults must become typed Workflow input | Medium | POC-02, POC-07 | F4, F8 |
| **U14** | Can we deploy while multi-hour loads run? | Pinned Workflows finish on one version; server ≥ v1.29.1, Python SDK ≥ 1.11 [S135](sources.md#s135); GA blog does not say OSS is covered [S136](sources.md#s136) | Version is fine (1.32.2). Open: Worker Controller approval (U02); whether the custom authorizer allows the versioning APIs | Low | POC-10 | F1 |
| **U16** | Do shared worker pods weaken secret isolation? | Today: one image, secrets as env vars (Q15). No Temporal doc on per-Activity secrets (*inference*) | Likely same exposure as today; confirm the POC-01 pattern does not widen it | Low | POC-01 | Q15 |
| **U17** | Do logs reach Sumo Logic with source and run IDs, without replay duplicates? | `workflow.logger` and SDK logging [S32](sources.md#s32); replay de-duplication not verified ([gaps TG23](3-evidence-and-gaps.md#3-temporal-gaps)) | Log fields and duplicates on replay | Low | POC-08 | Q23 |

### 5.2 Closed

| ID | Was | Closed by | Now |
|---|---|---|---|
| U07 | How to gate `SEMANTIC` models that join several sources | Q21: no wait-for-all | `dbt build --select source:<x>+` [S140](sources.md#s140); part of POC-04 |
| U10 | Replacing sensors | Q8: the only sensor starts dbt after a job | Ingest Workflow signals the dbt coordinator; part of POC-04 |
| — | SSO / RBAC on the UI | Q24: not needed | Out of scope |
| — | Durable persistence on the shared cluster | F9: managed durable MySQL | Kept as a POC-11a restart check |

---

## 6. POC plan

Each POC answers **one** question from [§5](#5-unknowns-register) and nothing already documented. 10 POCs. POC-05 (multi-source gating) was dropped: no wait-for-all rule (Q21); covered by POC-04.

### 6.1 Order and dependencies

```mermaid
flowchart LR
  NSPR["Namespace PR + token issuer<br/>(platform team)"] --> P11a["POC-11a<br/>Namespace on shared cluster<br/>+ durable persistence check"]
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

### 6.2 Summary

| POC | Question | Unknowns | Wave | Effort (days) |
|---|---|---|---|---|
| 11a | Our Namespace works on the shared cluster; Worker auth and persistence hold | U15 | Step 0 | 1 + platform lead time |
| 11b | Shared cluster is fit for production loads | U15 | A | 3 |
| 01 | Per-job resource and secret isolation | U01, U02, U16 | A | 4–5 |
| 02 | ELT in an Activity: cancel, crash, retry | U03, U04, U18 | A | 3–4 |
| 03 | State continuity from Dagster | U05 | A | 2 |
| 04 | dbt trigger/coordinator: overlap, failure, retry | U06, U08 | A | 4 |
| 06 | Shared API rate limits and run-queue caps | U09 | A | 3 |
| 07 | Partitions and backfills | U11, U18 | A | 7 |
| 08 | Alerting, freshness, logs | U12, U17 | B | 3 |
| 09 | On-call tasks without the Dagster UI | U13 | B | 2 |
| 10 | Safe deploys during long loads | U14 | B | 2–3 |
| | **Totals** | | Step 0 ≈ 1; A ≈ **28** (3 + 5 + 4 + 2 + 4 + 3 + 7); B ≈ **8** (3 + 2 + 3) | |

### 6.3 Shared setup

| Item | Setup |
|---|---|
| Temporal | The shared cluster, our own non-prod Namespace (POC-11a). If the Namespace is late, a temporary dev install (official Helm chart [S126](sources.md#s126), same server version) unblocks Wave A **except** POC-11b |
| Search Attributes | Registered once by a Namespace admin (admin CLI). Budget: 10 Keyword on SQL visibility [S38](sources.md#s38). Proposed: `source`, `tool`, `resource_class`, `partition_key`, `backfill_id`, `status` |
| Sources | **S-big** (largest/longest Meltano job); **S-dlt** (dlt with `merge` or incremental); **S-part-5m** (5-minute partitions); **S-part-multi** (multi-dimensional or dynamic); **S-ops** (non-ELT ops job); **S-token-pair** (two sources sharing an API token) |
| Snowflake | POC database cloned from `RAW`/`CLEAN`/`SEMANTIC` (zero-copy clone, if allowed); tests never touch prod |
| Evidence | Logs, screenshots and a one-page result per POC in `docs/poc-results/POC-xx.md` |

### 6.4 POC cards

#### POC-11 — Shared cluster readiness (U15)

| Item | Detail |
|---|---|
| Question | Can the shared cluster run our production loads safely, and what must the platform team add first? |
| Not tested | That Temporal needs a persistence DB and which versions are supported [S3](sources.md#s3) |
| 11a steps | 1. PR to the platform values files registering our non-prod Namespace and token issuer. 2. Worker reads the token from the provided file path; check it keeps working after the token in the file changes. 3. Start a Workflow, restart every server pod, check the Workflow is still there |
| 11b steps | 1. Record shard count and Namespace retention. 2. Register our Search Attributes. 3. If POC-06 needs it: PR for `matching.enableFairness` scoped to our Namespace. 4. Kill each server service pod while 10 POC Workflows run; ask the platform team to fail over the DB. 5. Load test: 50 concurrent ingests + the POC-07 backfill burst; watch our latency while another tenant is busy, and theirs while ours runs. 6. Agree backup/restore, upgrade and paging |
| Pass | No Workflow lost or stuck after each failure · token refresh works for 24 h without restart · Search Attributes registered · Namespace rate limits do not throttle our peak · our backfill burst does not slow other tenants · written ownership of who is paged when the cluster is down |
| Fail → | List missing config/limits as platform asks with dates; Wave A continues on a dev install |
| Depends | Q2, F1, F2, F9, F10 |

#### POC-01 — Per-job resource isolation (U01, U02, U16)

| Item | Detail |
|---|---|
| Question | Which pattern keeps per-job CPU/memory (and secret) isolation: **(a)** worker pools per resource class, or **(b)** an Activity that launches one K8s Job per run? |
| Not tested | Temporal scheduling and retries (documented) |
| Setup | (a) 3 Worker Deployments (`small`, `medium`, `large`), one Task Queue each, autoscaled on backlog (KEDA or HPA via Worker Controller). (b) One light Worker whose Activity creates a K8s Job with per-source requests/limits and service account, then polls and heartbeats until it ends |
| Steps | 1. Run S-big + 5 small jobs at once on each pattern. 2. Force an OOM in S-big. 3. Scale 0 → N and back with a 2-hour Activity in flight. 4. From a `small` pod, try to read S-big's secret. 5. Ask whether KEDA / Worker Controller may be installed (U02) |
| Pass | OOM in one job fails no other job · each job's peak memory stays within its limit · no Activity killed by scale-down · scale-up starts work within an agreed time · a job cannot read another job's secrets, or exposure is no wider than today and accepted in writing |
| Fail → | If (a) fails isolation and (b) fails reliability (orphan Jobs, lost heartbeats), Temporal cannot match today's model; escalate |
| Depends | Q7, Q15, F1 |

#### POC-02 — ELT tool inside an Activity (U03, U04, U18)

| Item | Detail |
|---|---|
| Question | When the Activity is cancelled, times out, or its worker dies mid-load, does the Meltano/dlt process stop, and does the retry leave `RAW` without duplicates or gaps? |
| Not tested | That Activities retry and need heartbeats to see cancel (documented) |
| Setup | Activity runs `meltano run …` or the dlt pipeline as a **subprocess**, heartbeats every N s, kills the process group on cancel. Sync Activity with a thread-pool `activity_executor`, or async with a non-blocking subprocess API |
| Steps | For S-big, S-dlt and S-ops: 1. Cancel mid-load. 2. Kill the worker pod mid-load. 3. Let Start-To-Close expire. 4. Inject a Snowflake error after 50 % of rows. After each, let the Retry Policy finish |
| Pass | No orphan process or open Snowflake query · after retry, `RAW` matches a clean single run on row count, primary-key uniqueness, the set of primary keys and row values (hash per row) · bookmark advances exactly once · cancel seen within 2× heartbeat interval |
| Fail → | Change that source's write disposition (e.g. append → merge) before it migrates; record per source |
| Depends | Q11, Q16, F8 |

#### POC-03 — State continuity from Dagster (U05)

| Item | Detail |
|---|---|
| Question | Can a Temporal-run job continue exactly where the last Dagster run stopped, and is a run blocked if both orchestrators fire at once? |
| Not tested | dlt destination-state restore; Meltano backend list and lock defaults (documented) |
| Setup | **Isolated copy** of today's Meltano state backend with the **same** state IDs (incl. per-partition IDs), used by a POC Dagster and the POC Temporal; never the prod backend. dlt: same pipeline name + dataset, against the cloned Snowflake DB (dlt state lives in the destination, so the clone isolates it) |
| Steps | 1. Run S-big, S-dlt and one S-part-5m partition once in Dagster. 2. Run them once in Temporal. 3. Compare bookmarks and loaded row ranges. 4. Trigger Dagster and Temporal for one source within seconds |
| Pass | Step 2 loads only rows newer than step 1 · no gap or overlap · step 4 gives one load, or a clean lock error on the second, never two writers |
| Fail → | Cutover for that source includes a one-time state copy or controlled re-sync |
| Depends | Q16, F4 |

#### POC-04 — dbt trigger and coordinator (U06, U08)

| Item | Detail |
|---|---|
| Question | When many sources finish close together, does the dbt trigger keep runs correct and freshness at least as good as today? |
| Not tested | `source:<x>+` selection [S140](sources.md#s140) and skip-on-test-failure [S137](sources.md#s137) (documented) |
| Setup | Each ingest Workflow, on success, starts `dbt build --select source:<x>+` as a subprocess Activity. **(a) parallel:** one dbt subprocess per finished source (today's behaviour). **(b) coordinator:** one `DbtCoordinatorWorkflow` via Signal-With-Start runs non-overlapping selections in parallel and queues overlapping ones. Persist `target/` (at least `run_results.json`) to object storage after each run |
| Steps | 1. 50 "source loaded" events in 60 s. 2. Duplicates. 3. Two sources sharing a downstream model finish within 1 s. 4. Fail one model, fix it, `dbt retry` on another pod. 5. (b) only: force Continue-As-New mid-stream |
| Pass | No two dbt processes build the same model at once, or (a) shows no wrong results and the team accepts it in writing · each event builds its models exactly once · `dbt retry` on a new pod resumes from the failure · lag (load done → model built) ≤ today's · (b) no events lost across Continue-As-New |
| Fail → | (a) fails: use (b). (b) too slow: per-model lock instead of per-selection; re-test |
| Depends | Q19, Q20, Q22 |

#### POC-06 — Shared API rate limits and run-queue caps (U09)

| Item | Detail |
|---|---|
| Question | Can we enforce today's tag concurrency limits (e.g. "max N jobs per API token") and the global run-queue limit across all workers, even while autoscaling? |
| Not tested | Task Queue rate limit is per second and queue-wide (documented) |
| Setup | Input: export of the real `tag_concurrency_limits` and run-queue limit. Compare: (a) one Task Queue per shared token with fixed replicas × slots; (b) a semaphore Workflow per key (acquire/release via Update or Signal); (c) Fairness keys with `matching.enableFairness=true` (platform PR; only if granted) |
| Steps | 1. Fire 30 jobs at once with cap N=5 on one token (S-token-pair + clones). 2. Scale workers 1 → 6 during the run. 3. Kill a worker holding a permit. 4. Start one source from a Schedule and manually at once (source-wide lock, [§4](#4-feature-mapping) row 2). 5. Backfill S-part-5m while its 5-minute schedule keeps firing |
| Pass | Concurrency per key never exceeds N · global running count never exceeds the run-queue limit · never two runs of one source at once · a killed holder's permit is released within heartbeat timeout · no source starves longer than the agreed time |
| Fail → | Pick the option that passes every criterion (lower throughput is fine if caps hold). If none passes, token-sharing sources stay on Dagster |
| Depends | Q10, Q18, F2, F6 |

#### POC-07 — Partitions and backfills (U11, U18)

| Item | Detail |
|---|---|
| Question | Can we run partitioned loads and backfills (all types, down to 5-minute, ~2-year ranges) on Temporal, track per-partition status, and keep them consistent with incremental state? |
| Not tested | Schedule backfill of schedule times [S131](sources.md#s131); per-Workflow limits [S11](sources.md#s11) (documented) |
| Setup | **Partition library** in Python: one `PartitionDef` per type (time window with cron, static list, dynamic list stored in Snowflake, multi-dimensional = product of two). `BackfillWorkflow(source, range, batch_size)` splits the range into **batches** (e.g. 1 day of 5-minute partitions = 288), runs at most K child `IngestWorkflow`s at a time (K under POC-06 caps, ≤ 500), and Continues-As-New every N batches. Child Workflow ID `ingest-<source>-<batch_start>-<batch_end>`. For a job that accepts only **one partition per run** (P0 inventory), the batch child runs its partitions one after another, one Activity each, so pending children stay ≤ K (*inference*). Backfill runs use **their own state ID**, never the live one. Run config: Launchpad config + defaults become one typed input dataclass, defaults in code. Status: one row per partition in a Snowflake table; Search Attributes `partition_key`/`backfill_id` per batch |
| Steps | 1. Scheduled run of each type (S-part-5m, S-part-multi, a daily, a static, a dynamic) creates today's partition key and run config. 2. Backfill the largest real range (2 years) for S-part-5m on the cloned DB, one range per child run as in Launchpad. 3. Repeat a shorter backfill for one single-partition-only job. 4. Fail 5 batches on purpose; re-run only failed partitions. 5. Start the same batch twice. 6. Add a dynamic partition key and backfill it. 7. After a backfill of old partitions, run a live load and compare it with a clean live run over the same window. 8. Compare runtime and Snowflake cost with Dagster's range-per-run backfills. 9. Two backfills with overlapping ranges, and a backfill over partitions the live schedule also loads. 10. Make one 5-minute run last over 5 minutes so `Skip` drops the next action; run the gap check |
| Pass | Every partition key loads once with correct rows (POC-02 checks) · on-call lists failed/missing partitions of a source in one query · re-run of only failed partitions works · duplicate start rejected · no Workflow exceeds 10,240 events (warn level) · 2-year backfill finishes in an agreed time without starving live schedules · after a backfill, the live bookmark equals the value from a clean live run over the same window, and the next live run loads the same rows as that clean run · overlapping ranges never load one partition concurrently; a re-load replaces rows and the status table shows the latest run · the partition skipped by `Skip` is found and loaded by the gap check |
| Fail → | Partitioned sources stay on Dagster until the library passes; this blocks most of the migration |
| Depends | Q9, F3, F4, F5, F11 |

#### POC-08 — Alerting, freshness, logs (U12, U17)

| Item | Detail |
|---|---|
| Question | Can we alert to Slack and PagerDuty on the same events as today, replace Dagster freshness checks, and emit logs Sumo Logic can use? |
| Setup | Server + SDK metrics to Prometheus; alert rules to Slack and PagerDuty; a final "on failure" Activity posting source, run link and error; freshness for almost all assets: port each Dagster threshold to dbt source `freshness` config, run `dbt source freshness` on a Schedule [S141](sources.md#s141), plus `max(loaded_at)` checks for assets dbt does not cover; structured JSON logs with `source`, `workflow_id`, `run_id`, `partition` |
| Steps | Trigger: failed run, stuck run (no heartbeat), missed schedule (worker pool down), stale `RAW` table, failed dbt test. Replay one Workflow and check log lines |
| Pass | Each case alerts within agreed time with source and run link · PagerDuty only for cases that page today; freshness to Slack · no alert storm on a 10-source outage · every Dagster freshness check has an equivalent with the same threshold · logs have the fields above and no duplicate lines on replay (or duplicates are marked) |
| Depends | Q12, Q13, Q23, F7 |

#### POC-09 — On-call tasks without the Dagster UI (U13)

| Item | Detail |
|---|---|
| Question | Can on-call do today's tasks as fast and safely as in Dagster? |
| Tasks to time | 1. Find failed jobs in the last 24 h. 2. Re-run a failed job (whole run, or only the failed step). 3. Backfill partitions X–Y for source S (uses POC-07). 4. Start a run with edited config, like Launchpad |
| Tools | Temporal UI, `temporal` CLI (`workflow start --input`, `schedule trigger`, Workflow Reset, `activity reset`), Search Attributes, a small backfill/launch CLI that fills config defaults |
| Pass | Each task ≤ 2 min for someone not on the POC team, using a 1-page runbook |
| Depends | Q14, F4, POC-07 |

#### POC-10 — Safe deploys during long loads (U14)

| Item | Detail |
|---|---|
| Question | Does Worker Versioning (Pinned) + Worker Controller let us ship new code while a multi-hour load or backfill runs? |
| Steps | Start a 2-hour ingest and a backfill on v1; deploy v2 with a changed Workflow; start a new run |
| Pass | v1 runs finish on v1 pods with no non-determinism error · new run uses v2 · v1 pods removed after drain · replay test of v1 histories runs in CI |
| Fail → | Use Workflow patching (`patched()`) and replay tests instead |
| Depends | Server ≥ v1.29.1 (1.32.2), Python SDK ≥ 1.11; Worker Controller approval (U02) |

### 6.5 Go / no-go after Wave A

| Result | Decision |
|---|---|
| All Wave A POCs pass | Proceed to Wave B and build |
| POC-11b finds missing config/limits | Proceed only with dated platform commitments |
| POC-01 fails both patterns | **Stop**; per-job isolation is a hard requirement |
| POC-07 fails | **Stop** for partitioned jobs (> 75 %); migrating only the rest is not worth a second orchestrator |
| POC-06: no option passes every criterion | **Stop** for sources that share a token or need the run-queue limit; they stay on Dagster |
| POC-02/03 fail for some sources only | Proceed; fix write disposition / state per source before it migrates |
| POC-04 fails both options | Proceed only with an agreed simpler dbt schedule and a written freshness trade-off |

---

## 7. Migration plan

### 7.1 Phases

```mermaid
flowchart LR
  P0["P0 Namespace PR<br/>(1 wk + platform lead time)"] --> P1["P1 POCs<br/>Wave A → go/no-go<br/>Wave B<br/>(6–8 wks)"]
  P1 --> P2["P2 Foundation<br/>prod platform, libraries,<br/>alerts, runbooks<br/>(3–4 wks)"]
  P2 --> P3["P3 Pilot<br/>1 cluster, 2–3 sources<br/>(2 wks)"]
  P3 --> P4["P4 Waves<br/>remaining clusters<br/>(~10 sources / wk)"]
  P4 --> P5["P5 Decommission<br/>Dagster read-only → off<br/>(2 wks)"]
```

| Phase | Output | Exit criteria |
|---|---|---|
| P0 Prepare | Namespace PR merged; exports of Dagster tag concurrency limits, freshness checks, partition definitions and Launchpad defaults; list of non-ELT ops jobs; **backfill input per job** (accepts a partition range, or only one partition per run) | POC scope frozen; POC-11a Namespace available |
| P1 POCs | `docs/poc-results/POC-xx.md` per POC | Wave A all pass ([§6.5](#65-go--no-go-after-wave-a)); Wave B pass or risk accepted in writing |
| P2 Foundation | Prod Namespace (POC-11b agreements); worker images; shared library: `IngestWorkflow`, `BackfillWorkflow`, `run_elt` Activity, dbt trigger (POC-04 option); concurrency control (POC-06); Search Attributes; Slack/PagerDuty alerts and freshness checks; replay tests in CI; runbook | Failure drills pass; runbook reviewed by on-call |
| P3 Pilot | 1 dependency cluster on Temporal | 2 weeks with no duplicates, no missed runs, freshness ≤ today |
| P4 Waves | All clusters moved | Per-source checklist ([§7.3](#73-per-source-cutover-runbook)) done for every source |
| P5 Decommission | Dagster schedules removed; optional run-table export to Snowflake; Dagster DB read-only until retention passes | 30 days with no rollback; then shut down |

**Critical path starts outside our team:** the Namespace comes by platform PR, so start it in P0.

### 7.2 Cutover unit: dependency cluster

Each finished source builds all its downstream models (Q20, Q21). If source A (Dagster) and source B (Temporal) both feed `SEMANTIC.model_x`, both orchestrators would run dbt on it, and neither can stop the overlap. Overlapping dbt runs on the same models "could lead to unpredictable results" [S120](sources.md#s120). Moving the whole cluster keeps one owner per model. Sources that share an API token should also move together, or the token's cap must be split between Dagster and Temporal during the transition (*inference*).

```mermaid
flowchart LR
  subgraph C1["Cluster 1 (move together)"]
    A[source A] --> m1[CLEAN.a]
    B[source B] --> m2[CLEAN.b]
    m1 --> s1[SEMANTIC.x]
    m2 --> s1
  end
  subgraph C2["Cluster 2"]
    C[source C] --> m3[CLEAN.c] --> s2[SEMANTIC.y]
  end
```

| Step | How to build clusters |
|---|---|
| 1 | Parse dbt `manifest.json`: for each model, list upstream sources |
| 2 | Union-find: sources that share any downstream model, or share an API token (F6), go in one cluster |
| 3 | Order: smallest and least critical first; the cluster with S-big after the pilot proves POC-01 in prod |
| 4 | Too-big cluster (e.g. a "core" model joins 30 sources): split it. During the transition the shared model and its descendants are built **only** by one scheduled dbt job, and both orchestrators' per-source builds exclude them (`dbt build --select source:<x>+ --exclude <shared_model>+`). State this in the wave plan. When the last source of the split moves: **pause the shared scheduled dbt job, wait until it has no run in flight**, then remove the exclusion |

### 7.3 Per-source cutover runbook

```mermaid
stateDiagram-v2
  [*] --> Ready: checklist green
  Ready --> DagsterPaused: pause Dagster schedule/sensor
  DagsterPaused --> Drained: no Dagster run in flight
  Drained --> TemporalLive: unpause Temporal Schedule
  TemporalLive --> Verified: first run checks pass
  Verified --> Stable: N runs clean
  TemporalLive --> RolledBack: check fails
  Verified --> RolledBack: incident
  RolledBack --> Ready: fix, retry
  Stable --> [*]
```

**Before (checklist per source)**

| Check | Proven by |
|---|---|
| Write disposition reviewed for retry safety | POC-02 |
| State backend / pipeline name identical to Dagster's | POC-03 |
| Resource class or K8s Job spec matches today's requests/limits | POC-01 |
| Partition definition and partition → Meltano/dlt mapping identical to Dagster's | POC-07 |
| Concurrency key (API token, run queue) assigned | POC-06 |
| No Dagster backfill in progress for the cluster: finish it in Dagster, or cancel and re-run later with `BackfillWorkflow` | — |
| Secrets mounted only where needed | U16 |
| Slack/PagerDuty alerts, freshness check and log fields cover this source | POC-08 |
| Temporal Schedule created **paused**, same cron and time zone as Dagster | — |

**Cutover (whole cluster at once)**

| # | Step |
|---|---|
| 1 | Pause the Dagster schedules/sensors for **every** source in the cluster, and the Dagster dbt trigger for the cluster's models |
| 2 | Wait until no Dagster run is in flight for any of them |
| 3 | Record each source's last completed schedule time, and list ingests that finished after the Dagster dbt trigger was paused (their dbt work is pending) |
| 4 | Hand the cluster's dbt trigger to Temporal, then unpause the cluster's Temporal Schedules |
| 5 | Reconcile missed work: every schedule time after the recorded last completed time up to the unpause (incl. times skipped while Dagster was paused and draining) runs with `temporal schedule backfill` (or `trigger` if only one). Backfill runs actions that "would have run during a specified time interval" [S131](sources.md#s131). For partitioned sources, check that every partition Dagster marked missing or failed is backfilled or listed for the owner |
| 6 | Run the pending dbt work from step 3 once (`dbt build --select source:<x>+` per listed source, plus `--exclude <shared_model>+` while a split is active). Send it **through the same dbt path live loads use**, so it cannot overlap a live build: with the coordinator (POC-04 option b) it is queued like any load; with option (a), finish it before step 4 unpauses the Schedules |
| 7 | Verify each source before marking the cluster done |

**Per-source checks:** row counts vs last Dagster run, primary-key uniqueness in `RAW`, bookmark advanced, no missed schedule times, dbt models built.

**Rollback (any time, whole cluster)**

| # | Step |
|---|---|
| 1 | Pause the Temporal Schedules for **every** source in the cluster; record the schedule time and partitions of every running ingest or backfill Workflow, then cancel them |
| 2 | Wait until all of them are closed and their ELT subprocesses or K8s Jobs and Snowflake queries have stopped. Cancellation is only a request the Workflow and Activity must handle [S128](sources.md#s128); if one does not close in time, terminate it and confirm the process is gone |
| 3 | Turn off the Temporal dbt trigger for the cluster's models. **Cancel any dbt selections queued in the `DbtCoordinatorWorkflow`** (option b). Wait for any running dbt build to finish |
| 4 | Re-enable the Dagster schedules/sensors and the Dagster dbt trigger for the cluster. State is in the same backend, so runs continue from the last good bookmark (if POC-03 passed) |
| 5 | Reconcile in Dagster: the work recorded in step 1, plus schedule times missed during rollback (as in cutover step 5). Run dbt for any source whose load completed in Temporal but whose dbt build did not (this covers selections cancelled in step 3) |

### 7.4 Parallel-run guardrails

- Never let both orchestrators schedule the same source at once. One table records the owner of each source (`dagster` / `temporal`).
- dbt for a cluster is triggered only by the orchestrator that owns the cluster.
- Optional shadow run (if proof is needed before cutover): Temporal loads pilot sources into a separate `RAW_SHADOW` database for one week; compare row counts and checksums with `RAW`. Shadow runs use a **separate** Meltano state backend (or state IDs) and a separate dlt dataset, so they never move prod bookmarks. Cost: double source API load; check shared token limits first.

### 7.5 Main risks and mitigations

| Risk | Mitigation | Linked |
|---|---|---|
| Lost per-job isolation causes noisy-neighbour failures | Pattern chosen in POC-01; resource class per source in config | U01 |
| Duplicates after retries | Merge/upsert write dispositions; uniqueness checks after each load | U04 |
| dbt freshness worse than today | Coordinator batching tuned in POC-04; target lag in alerts | U06 |
| On-call slower without Dagster UI | Runbook + Search Attributes; dbt docs for lineage | U13 |
| Shared cluster down or degraded | Durable persistence (F9); failure drills, backups and on-call ownership agreed (POC-11) | U15 |
| Our 2-year backfills slow other tenants | Batched backfills with caps; joint load test (POC-07, POC-11b) | U11, U15 |
| Partitioned jobs cannot be tracked or backfilled | `BackfillWorkflow` + status table proven in POC-07 before any partitioned source moves | U11 |
| Backfill moves the live bookmark | Backfills use their own state ID; POC-07 compares the next live run with a clean run | U05, U11 |
| Shared API token over-used during transition | Move token-sharing sources together; split caps if not possible (POC-06) | U09 |
