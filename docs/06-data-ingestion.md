# 06 — Data ingestion (core document)

> **Key takeaways**
> - **Dagster** ships first-party integrations for **dlt, Sling, Airbyte, Fivetran** (and dbt downstream) that model each table/stream as an asset; Meltano is community-maintained; `dagster-embedded-elt` is deprecated ([Dagster integrations index](https://dagster.io/docs/integrations/libraries), [Embedded ELT (deprecated)](https://dagster.io/docs/integrations/libraries/embedded-elt)).
> - **Temporal** ships **no data connectors** (none found); it gives durable primitives: retries, heartbeat checkpoints, batch patterns, queue-wide rate limits ([Long-Running Activity pattern](https://docs.temporal.io/design-patterns/long-running-activity), [Batch Processing Patterns](https://docs.temporal.io/design-patterns/batch-processing-patterns), [Downstream Rate Limiting](https://docs.temporal.io/design-patterns/downstream-rate-limiting)).
> - **Large data never goes through Temporal payloads**: 2 MB default limit; pass references or use **External Storage** (Public Preview, SDK-side Data Converter feature; self-hosted support not explicitly stated) ([Troubleshoot payload size limit](https://docs.temporal.io/troubleshooting/blob-size-limit-error), [External Storage](https://docs.temporal.io/external-storage)).
> - Both are **at-least-once** at the work-unit level: Temporal Activities "may be executed multiple times" ([Activity Definition](https://docs.temporal.io/activity-definition)); Dagster dedupes *runs* with sensor run keys ([Sensors](https://dagster.io/docs/guides/automate/sensors)). Writes must be idempotent in both.
> - *Judgement:* Dagster wins on connector-based ELT and partition backfills; Temporal wins on custom, high-fan-out, rate-limited or long-running extraction.

## 1. Integrations

| Tool | Dagster OSS | Temporal OSS |
|---|---|---|
| **dlt** | "Dagster supported", Component (`DltLoadCollectionComponent` in `defs.yaml`); Pythonic: multi-asset, each dlt resource → asset, `DagsterDltResource` ([dlt integration](https://dagster.io/docs/integrations/libraries/dlt), [dlt integration (Pythonic)](https://dagster.io/docs/integrations/libraries/dlt/dlt-pythonic)) | No official integration found; call dlt inside an Activity (*inference*) |
| **Sling** | "Dagster supported", Component (`SlingReplicationCollectionComponent`, `connections` + `replications`); example uses `mode: full-refresh` ([Sling integration](https://dagster.io/docs/integrations/libraries/sling)). Pythonic guide lists "full-refresh, incremental, and snapshot" and passes the replication config to the Sling CLI ([Sling (Pythonic)](https://dagster.io/docs/integrations/libraries/sling/sling-pythonic)). Sling modes: `full-refresh` (default), `incremental`, `truncate`, `snapshot`, `backfill`, `definition-only`, `change-capture` ([Sling replication modes](https://docs.slingdata.io/concepts/replication/modes)) | None found |
| **Airbyte** | "Dagster supported"; `AirbyteWorkspaceComponent` for Cloud and OSS/Self-Managed; one asset per stream; caches workspace metadata ([Airbyte integration](https://dagster.io/docs/integrations/libraries/airbyte), [Airbyte component](https://dagster.io/docs/integrations/libraries/airbyte/airbyte-component)). **Triggers syncs**: "Materializing these assets will trigger a sync of the Airbyte connection"; `AirbyteCloudWorkspace` (Cloud) and `AirbyteWorkspace` (OSS/self-managed) both "launch syncs and monitor their progress"; no observe-only option documented ([dagster-airbyte library reference](https://dagster.io/docs/integrations/libraries/airbyte/dagster-airbyte)) | None found. Airbyte has a case study on temporal.io (*vendor case study, not fetched*) ([Airbyte case study](https://web.temporal.io/case-studies/airbyte-case-study)) |
| **Fivetran** | "Dagster supported"; `FivetranAccountComponent`; triggering mode (sets connector schedule to "manual" unless `disable_schedule_on_trigger: false`) or observe-only (`polling_sensor: true`); `retry_on_reschedule` ([Fivetran integration](https://dagster.io/docs/integrations/libraries/fivetran)) | None found |
| **Meltano** | "Community supported" ([Meltano integration](https://dagster.io/docs/integrations/libraries/meltano)) | None found |
| **Embedded ELT** | Deprecated → `dagster-sling` and `dagster-dlt` ([Embedded ELT (deprecated)](https://dagster.io/docs/integrations/libraries/embedded-elt)) | — |
| **dbt** (downstream) | "Dagster supported"; `DbtProjectComponent`; each model is an asset; link via `meta.dagster.asset_key` ([dbt integration](https://dagster.io/docs/integrations/libraries/dbt)) | None found |
| **External compute** | Pipes: AWS, Azure ML, GCP Dataproc, Spark/Databricks Connect, Kubernetes, PySpark, Scala/Spark, JavaScript ([External pipelines (Pipes) integrations](https://dagster.io/docs/integrations/external-pipelines)) | Any SDK language in Workers: 8 SDKs ([Understanding Temporal](https://docs.temporal.io/evaluate/understanding-temporal)) |
| **Object-store offload** | IO managers (details **Not verified**) | External Storage: S3 drivers (Go, Java, Python, TypeScript), GCS (Go, TypeScript), Azure Blob listed, custom drivers ([External Storage](https://docs.temporal.io/external-storage)) |

- No official Temporal–Dagster integration exists ([Dagster integrations index](https://dagster.io/docs/integrations/libraries)); see [doc 12](12-decision-guide.md).
- No official CDC/streaming positioning was found for either. Temporal's "Workflow Streams" targets live interactivity for agents, content **not verified** ([Workflow Streams](https://docs.temporal.io/workflow-streams)).

## 2. Patterns per scenario

Scenario IDs come from [the plan](00-plan.md). Each pattern below is *the author's synthesis of documented primitives*, not a vendor-published reference architecture unless cited.

### S1 — API polling (REST/SaaS)

**Temporal**

- Schedule with overlap `Skip` starts a Workflow each interval ([Schedule](https://docs.temporal.io/schedule)).
- One Activity per page (or a long Activity that heartbeats the page cursor) ([Long-Running Activity pattern](https://docs.temporal.io/design-patterns/long-running-activity)).
- Batch Iterator: "Processes one page of records per Workflow run and continues as new with the next page offset" ([Batch Processing Patterns](https://docs.temporal.io/design-patterns/batch-processing-patterns)).
- Queue-wide rate limit protects the API ([Downstream Rate Limiting](https://docs.temporal.io/design-patterns/downstream-rate-limiting)).

```mermaid
flowchart LR
    SCH["Schedule<br/>overlap Skip"] --> WF["Polling Workflow"]
    WF --> ACT["Activity: fetch page<br/>retry with backoff"]
    ACT -->|"page ref"| ST[("Object store / staging table")]
    ACT -->|"next cursor"| WF
    WF -->|"history getting large"| CAN["Continue-As-New<br/>carry cursor"]
    CAN --> WF
    WF --> LD["Activity: load to warehouse<br/>idempotent upsert"]
```

**Dagster**

- dlt asset (each dlt resource → asset) on a schedule or `on_cron` automation ([dlt integration (Pythonic)](https://dagster.io/docs/integrations/libraries/dlt/dlt-pythonic), [Declarative Automation](https://dagster.io/docs/guides/automate/declarative-automation)).
- dlt manages its own state; with partitions, use concurrency limits ([dlt integration (Pythonic)](https://dagster.io/docs/integrations/libraries/dlt/dlt-pythonic)).
- Concurrency pools protect the shared API across runs ([Managing concurrency](https://dagster.io/docs/guides/operate/managing-concurrency)).

```mermaid
flowchart LR
    TR["Schedule or AutomationCondition on_cron"] --> RUN["Run"]
    RUN --> DLT["dlt asset<br/>DagsterDltResource"]
    DLT -->|"dlt state + cursor"| API["Source API"]
    DLT --> WH[("Warehouse table asset")]
    POOL["Concurrency pool"] -.->|"limits parallel runs"| DLT
    WH --> DBT["dbt models via source asset key"]
```

### S2 — File arrival in S3

**Temporal**

- No built-in bucket trigger found. Options (*inference*): a Schedule-driven listing Workflow, or an external notifier that starts a Workflow or sends a Signal.
- One Child Workflow per file with a deterministic ID gives per-file tracking (Rapidflare used deterministic child IDs) ([How Rapidflare built a million+ document ingestion pipeline](https://temporal.io/blog/how-rapidflare-built-a-million-document-ingestion-pipeline-for-agents-on-temporal), *vendor blog*).
- Retry policy on file-processing Workflows is one of the few documented reasons to retry Workflows ([Retry Policies](https://docs.temporal.io/encyclopedia/retry-policies)).

```mermaid
flowchart LR
    SCH["Schedule"] --> LW["Lister Workflow"]
    LW --> LA["Activity: list new keys"]
    LA -->|"keys only"| LW
    LW --> C1["Child Workflow id=file key"]
    LW --> C2["Child Workflow id=file key"]
    C1 --> P1["Activity: load file<br/>heartbeat offset"]
    C2 --> P2["Activity: load file<br/>heartbeat offset"]
```

**Dagster**

- Sensor lists the bucket, stores the last-seen modification time in a **cursor**, and emits one run per file with a **run key** ([Sensors](https://dagster.io/docs/guides/automate/sensors)).
- Dynamic partitions: the sensor returns `SensorResult` with `dynamic_partitions_requests` plus one run per new file, to "incrementally load new partitions instead of reading the entire bucket" ([Dynamic partitioning](https://dagster.io/blog/dynamic-partitioning), *vendor blog*); the same pattern (`SensorResult` with `RunRequest(partition_key=...)` and `build_add_request`) is in the [Partitions API reference](https://dagster.io/docs/api/dagster/partitions).
- S3 helper: `dagster-aws` ships `dagster_aws.s3.sensor.get_objects(bucket, prefix, since_key, since_last_modified)`; the older `get_s3_keys` is marked deprecated ("Use get_objects instead"). Found in the package source only; not listed in the [dagster-aws API reference](https://dagster.io/docs/api/libraries/dagster-aws) ([dagster-aws 0.29.29 (PyPI)](https://pypi.org/project/dagster-aws/0.29.29/)). Treat as an unstable helper.
- Default sensor interval is 30 s when `minimum_interval_seconds` is not set (`DEFAULT_SENSOR_DAEMON_INTERVAL = 30` in package source; not stated in the docs) ([dagster 1.13.29 (PyPI)](https://pypi.org/project/dagster/1.13.29/)).

```mermaid
flowchart LR
    SEN["Sensor<br/>minimum_interval_seconds"] -->|"list since cursor"| B[("S3 bucket")]
    SEN -->|"dynamic_partitions_requests"| DP["Dynamic partitions<br/>one per file"]
    SEN -->|"run_key = file key"| R1["Run: partition file_a"]
    SEN -->|"run_key = file key"| R2["Run: partition file_b"]
    R1 --> A["Asset partition materialized"]
    R2 --> A
```

### S3 — Database incremental / CDC-style batch

**Temporal**

- Long-running Activity pages through rows by high-watermark and heartbeats the last key; a retry resumes from it ([Long-Running Activity pattern](https://docs.temporal.io/design-patterns/long-running-activity)).
- Writes use an idempotency key, for example Workflow Run ID + Activity ID ([Activity Definition](https://docs.temporal.io/activity-definition)).

```mermaid
sequenceDiagram
    participant WF as Workflow
    participant ACT as Activity: incremental_copy
    participant SRC as Source DB
    participant DST as Warehouse
    WF->>ACT: start with watermark W
    loop each page
        ACT->>SRC: select rows where key above last
        ACT->>DST: MERGE page
        ACT->>WF: heartbeat last key
    end
    ACT-->>WF: new watermark
    WF->>WF: store watermark, maybe Continue-As-New
```

**Dagster**

- Sling replication (Component) or dlt SQL source as assets ([Sling integration](https://dagster.io/docs/integrations/libraries/sling), [dlt integration (Pythonic)](https://dagster.io/docs/integrations/libraries/dlt/dlt-pythonic)).
- For dlt SQL sources set `defer_table_reflect=True`, else the daemon queries the DB about every minute to refresh definitions ([dlt integration (Pythonic)](https://dagster.io/docs/integrations/libraries/dlt/dlt-pythonic)).
- Sling incremental: Sling's own docs define `incremental` (merge/append; strategies: new-data upsert needs `primary_key` + `update_key`, full-data upsert needs `primary_key`, append-only needs `update_key`), `backfill` (re-load an `update_key` range) and `change-capture` (reads the source transaction log) ([Sling replication modes](https://docs.slingdata.io/concepts/replication/modes)).
- The Dagster Sling pages show only `full-refresh` in examples; the Pythonic guide lists incremental as supported and passes the replication config to the Sling CLI ([Sling (Pythonic)](https://dagster.io/docs/integrations/libraries/sling/sling-pythonic)). No Dagster example of `incremental` or `change-capture` was found.
- Time-partitioned asset per day/hour lets a window be re-run by re-materializing the partition ([Backfilling data](https://dagster.io/docs/guides/build/partitions-and-backfills/backfilling-data)).

```mermaid
flowchart LR
    T["Schedule / on_cron"] --> SL["Sling or dlt asset<br/>per table"]
    SL -->|"read changes"| SDB[("Source DB")]
    SL --> TBL[("Target table asset<br/>daily partition")]
    TBL --> CHK["Asset check<br/>row count, freshness"]
```

### S4 — SaaS connector sync (Fivetran, Airbyte)

**Temporal**

- No connector integration. *Inference:* an Activity calls the vendor's API to trigger a sync and polls for completion, with heartbeats; Temporal tracks nothing about the resulting tables.

```mermaid
flowchart LR
    WF["Workflow"] --> TA["Activity: trigger sync<br/>vendor API"]
    TA --> PA["Activity: poll status<br/>heartbeat, retry"]
    PA --> NX["Activity: run downstream step"]
```

**Dagster**

- `FivetranAccountComponent`: trigger syncs, or observe with `polling_sensor: true` to see sync history and freshness without triggering ([Fivetran integration](https://dagster.io/docs/integrations/libraries/fivetran)).
- `AirbyteWorkspaceComponent`: one asset per stream; materializing an asset **triggers** the Airbyte sync, for both Airbyte Cloud and OSS/self-managed (OSS needs REST and Configuration API URLs) ([dagster-airbyte library reference](https://dagster.io/docs/integrations/libraries/airbyte/dagster-airbyte), [Airbyte component](https://dagster.io/docs/integrations/libraries/airbyte/airbyte-component)). No observe-only mode (like Fivetran's `polling_sensor`) is documented.
- dbt models link to these assets through `meta.dagster.asset_key` ([dbt integration](https://dagster.io/docs/integrations/libraries/dbt)).

```mermaid
flowchart LR
    FC["FivetranAccountComponent"] --> FA["Assets: schema/table"]
    AC["AirbyteWorkspaceComponent"] --> AA["Assets: one per stream"]
    FA --> DBT["DbtProjectComponent models"]
    AA --> DBT
    PS["polling_sensor: true"] -.->|"observe only"| FA
    AA -.->|"materialize = trigger sync"| AB["Airbyte Cloud or OSS"]
```

### S5 — Large backfill

**Temporal**

- Patterns: Fan-Out with Child Workflows, Batch Iterator, Sliding Window ("fixed-size window of concurrent child Workflows"), MapReduce Tree ([Batch Processing Patterns](https://docs.temporal.io/design-patterns/batch-processing-patterns)).
- Stay well under history limits: "51,200 events max (aim for a few thousand)" ([Batch Processing Patterns](https://docs.temporal.io/design-patterns/batch-processing-patterns)); ≤2,000 pending Activities/Children, ≤500 recommended ([Self-hosted defaults](https://docs.temporal.io/self-hosted-guide/defaults)).
- Rapidflare: sliding window of N children, parent keeps cursor + in-flight IDs + counters, Continue-As-New at page boundaries, `ParentClosePolicy.ABANDON`; tested on 1,000,000 documents ([How Rapidflare built a million+ document ingestion pipeline](https://temporal.io/blog/how-rapidflare-built-a-million-document-ingestion-pipeline-for-agents-on-temporal), *vendor blog*).
- Schedule backfill re-runs past *schedule times*, in parallel (`AllowAll`) or sequence (`BufferAll`) ([Schedule](https://docs.temporal.io/schedule)).

```mermaid
flowchart TB
    P["Parent Workflow<br/>cursor, in-flight IDs, counters"] -->|"keep N in flight"| C1["Child 1"]
    P --> C2["Child 2"]
    P --> CN["Child N"]
    C1 -->|"done"| P
    P -->|"page boundary"| CAN["Continue-As-New"]
    CAN --> P
```

**Dagster**

- Backfill partitions from the asset page or lineage graph; default one run per partition, or `multi_run` / `single_run` ([Backfilling data](https://dagster.io/docs/guides/build/partitions-and-backfills/backfilling-data), [Partition backfill strategies](https://dagster.io/docs/examples/best-practices/partition-backfill-strategies)).
- Run queue (`max_concurrent_runs`, default 10 per the config schema in package source) and pools limit pressure on sources ([Managing concurrency](https://dagster.io/docs/guides/operate/managing-concurrency), [dagster 1.13.29 (PyPI)](https://pypi.org/project/dagster/1.13.29/)).

```mermaid
flowchart LR
    UI["Materialize: select 365 partitions"] --> POL{"BackfillPolicy"}
    POL -->|"default"| M["365 runs"]
    POL -->|"multi_run"| B["Batched runs"]
    POL -->|"single_run"| S["1 run, key range"]
    M --> Q["Run queue + pools"]
    B --> Q
    S --> Q
```

## 3. Large payload handling

| Topic | Temporal OSS | Dagster OSS |
|---|---|---|
| Hard limit | Blob warn 512 KB, error 2 MB; gRPC 4 MB; history 50 MB ([Self-hosted defaults](https://docs.temporal.io/self-hosted-guide/defaults)) | No payload limit found in notes; data moves through IO managers or external systems |
| Configurable | Payload limit configurable on self-hosted, default 2 MB ([Troubleshoot payload size limit](https://docs.temporal.io/troubleshooting/blob-size-limit-error)) | — |
| Recommended fix | Pass references; External Storage (claim check, offloads ≥256 KiB by default, Public Preview, may change before GA); codecs for compression only delay the problem ([Troubleshoot payload size limit](https://docs.temporal.io/troubleshooting/blob-size-limit-error), [External Storage](https://docs.temporal.io/external-storage)) | Write data to the warehouse/object store inside the asset; Pipes streams only logs and metadata back ([Dagster Pipes (guide)](https://dagster.io/docs/guides/build/external-pipelines)) |
| Self-hosted External Storage | SDK-side: configured on the client/Worker Data Converter; clients and Workers upload/download, history keeps a reference ([External Storage](https://docs.temporal.io/external-storage), [External Storage - Python SDK](https://docs.temporal.io/develop/python/data-handling/external-storage)). No page says it is Cloud-only; no page states a minimum server or SDK version. Self-hosted support is *likely* (*inference*) but **not explicitly stated** | — |
| Sensor tick size | — | Ticks requesting runs over the size limit fail explicitly (1.13.26) ([dagster-io/dagster releases](https://github.com/dagster-io/dagster/releases)) |

## 4. Idempotency and delivery semantics

| Topic | Temporal | Dagster |
|---|---|---|
| Execution semantics | "Activity may be executed multiple times and may even partially complete more than once"; observed as completed exactly once ([Activity Definition](https://docs.temporal.io/activity-definition)) | Steps re-run on retry; run retries create new runs ([Op retries](https://dagster.io/docs/guides/build/ops/op-retries), [Run retries](https://dagster.io/docs/deployment/execution/run-retries)); official delivery semantics **Not verified** |
| Dedupe of triggers | Overlap policy on Schedules ([Schedule](https://docs.temporal.io/schedule)); *inference*: deterministic Workflow IDs | Sensor run keys skip duplicates ([Sensors](https://dagster.io/docs/guides/automate/sensors)) |
| Idempotency keys | Enforced by the called service; Workflow Run ID + Activity ID suggested ([Activity Definition](https://docs.temporal.io/activity-definition)) | Partition-scoped overwrite is the usual approach (*inference*; not stated in fetched docs) |
| Guidance | "Activities that perform writes should be idempotent" ([Activity Definition](https://docs.temporal.io/activity-definition)) | No official "partitioned writes must be idempotent" statement found |

*Judgement:* in both tools use MERGE/upsert or deterministic object keys. Temporal makes the at-least-once nature explicit; Dagster hides it behind run-level dedupe, which is easy to over-trust.

## 5. Rate limiting and concurrency

| Mechanism | Temporal | Dagster |
|---|---|---|
| Global throughput cap | Task-queue rate limit (`max_task_queue_activities_per_second` in Python) enforced by Matching "for the entire queue regardless of how many Workers"; 2 Workers × 5 = 5 ([Downstream Rate Limiting](https://docs.temporal.io/design-patterns/downstream-rate-limiting)) | Concurrency pools across runs protect "shared databases and APIs" ([Managing concurrency](https://dagster.io/docs/guides/operate/managing-concurrency)) |
| Per-process cap | `MaxWorkerActivitiesPerSecond` (multiplies with Workers) ([Downstream Rate Limiting](https://docs.temporal.io/design-patterns/downstream-rate-limiting)) | Run executor limits within a run ([Managing concurrency](https://dagster.io/docs/guides/operate/managing-concurrency)) |
| Concurrency slots | `MaxConcurrentActivityExecutionSize`; "not throughput limits" ([Downstream Rate Limiting](https://docs.temporal.io/design-patterns/downstream-rate-limiting)) | `max_concurrent_runs` in run queue; `default_op_concurrency_limit` ([Managing concurrency](https://dagster.io/docs/guides/operate/managing-concurrency), [dagster.yaml reference](https://dagster.io/docs/deployment/oss/dagster-yaml)) |
| Ordering | Task Queue Priority & Fairness GA 2026-05-05 ([Temporal changelog](https://temporal.io/changelog)) | With pools, "Dagster+ does **not** guarantee strict FIFO" (page states this for Dagster+; OSS not stated); use QueuedRunCoordinator priority rules ([Run coordinators](https://dagster.io/docs/deployment/execution/run-coordinators)) |
| Gotcha | Different Worker values: last poller wins ([Downstream Rate Limiting](https://docs.temporal.io/design-patterns/downstream-rate-limiting)) | Concurrency limits are "recommended" for partitioned dlt assets because dlt manages state ([dlt integration (Pythonic)](https://dagster.io/docs/integrations/libraries/dlt/dlt-pythonic)) |

## 6. Backfills compared

| | Temporal | Dagster |
|---|---|---|
| Unit | Schedule action time or your own work items | Asset partition |
| Built-in UI action | Schedule backfill ([Schedule](https://docs.temporal.io/schedule)) | Materialize partitions from asset page/lineage ([Backfilling data](https://dagster.io/docs/guides/build/partitions-and-backfills/backfilling-data)) |
| Fault isolation | Per Child Workflow / Activity (*inference*) | Per run; `single_run` retries all ([Partition backfill strategies](https://dagster.io/docs/examples/best-practices/partition-backfill-strategies)) |
| Status tracking | Your own (Search Attributes help) ([Observability - Python SDK](https://docs.temporal.io/develop/python/platform/observability)) | Partition fill status tracked ([Backfilling data](https://dagster.io/docs/guides/build/partitions-and-backfills/backfilling-data)) |

## 7. Illustrative code sketches

> **Illustrative only.** Short sketches, not tested programs. API names were checked against the official docs or package source cited under each block. `read_page`, `merge_into_warehouse` and `list_new_objects` stand for your own code.

### Temporal (Python SDK): resumable page loader

```python
# ILLUSTRATIVE SKETCH - simplified, not tested.
from datetime import timedelta
from temporalio import activity, workflow
from temporalio.common import RetryPolicy

@activity.defn
async def copy_table(table: str, start_key: str) -> str:
    details = activity.info().heartbeat_details      # last heartbeat, seen on retry
    last_key = details[0] if details else start_key
    while page := await read_page(table, after=last_key):   # your code
        await merge_into_warehouse(page)                     # idempotent MERGE
        last_key = page.last_key
        activity.heartbeat(last_key)                         # checkpoint
    return last_key                                          # small value, not data

@workflow.defn
class IngestTables:
    @workflow.run
    async def run(self, tables: list[str], marks: dict[str, str]) -> dict[str, str]:
        for t in tables:
            marks[t] = await workflow.execute_activity(
                copy_table, args=[t, marks[t]],
                start_to_close_timeout=timedelta(hours=2),
                heartbeat_timeout=timedelta(minutes=1),   # 2-3x heartbeat interval
                retry_policy=RetryPolicy(
                    maximum_attempts=10,
                    non_retryable_error_types=["SourceAuthError"],
                ),
            )
            workflow.logger.info("loaded %s", t)
        return marks

# Worker(client, task_queue="ingest", workflows=[IngestTables],
#        activities=[copy_table], max_task_queue_activities_per_second=5)
```

API sources: `@workflow.defn`, `@workflow.run`, `@activity.defn`, `workflow.execute_activity`, `start_to_close_timeout`, `heartbeat_timeout`, `RetryPolicy` ([Activity Timeouts - Python SDK](https://docs.temporal.io/develop/python/activities/timeouts)); `activity.heartbeat`, `activity.info().heartbeat_details`, 2–3× heartbeat timeout ([Long-Running Activity pattern](https://docs.temporal.io/design-patterns/long-running-activity)); `max_task_queue_activities_per_second` ([Downstream Rate Limiting](https://docs.temporal.io/design-patterns/downstream-rate-limiting)); `args=`, `non_retryable_error_types`, `Worker(...)` keywords ([temporalio 1.36.0 (PyPI)](https://pypi.org/project/temporalio/1.36.0/), package source).

### Dagster: file-arrival sensor with dynamic partitions

```python
# ILLUSTRATIVE SKETCH - simplified, not tested.
import dagster as dg

files = dg.DynamicPartitionsDefinition(name="files")

@dg.asset(partitions_def=files)
def raw_file(context: dg.AssetExecutionContext) -> None:
    load_file(context.partition_key)              # your code; key = S3 object key

@dg.sensor(target=raw_file, minimum_interval_seconds=30)
def new_files_sensor(context: dg.SensorEvaluationContext):
    since = float(context.cursor or 0)            # cursor persists between ticks
    new = list_new_objects(since)                 # your code (e.g. boto3 listing)
    return dg.SensorResult(
        dynamic_partitions_requests=[files.build_add_request([o.key for o in new])],
        run_requests=[dg.RunRequest(run_key=o.key, partition_key=o.key) for o in new],
        cursor=str(max([o.modified for o in new], default=since)),
    )   # Dagster skips any run whose run_key was used before
```

API sources: `@sensor(target=..., minimum_interval_seconds=...)`, `SensorResult(run_requests, cursor, dynamic_partitions_requests)`, `RunRequest(run_key, partition_key)`, `context.cursor` ([Schedules and sensors API reference](https://dagster.io/docs/api/dagster/schedules-sensors)); `DynamicPartitionsDefinition(name)`, `build_add_request` ([Partitions API reference](https://dagster.io/docs/api/dagster/partitions)); `@asset(partitions_def=...)` ([Assets API reference](https://dagster.io/docs/api/dagster/assets)); `AssetExecutionContext.partition_key` ([Execution API reference](https://docs.dagster.io/api/dagster/execution)); run-key dedupe ([Sensors](https://dagster.io/docs/guides/automate/sensors)).

### Dagster: connector components (`defs.yaml` fragments)

```yaml
# ILLUSTRATIVE FRAGMENTS - one component per defs.yaml.
# Sling
type: dagster_sling.SlingReplicationCollectionComponent
attributes:
  connections:
    DUCKDB:
      type: duckdb
      instance: /tmp/my_project.duckdb
  replications:
    - path: ./replication.yaml   # Sling config; set mode (e.g. incremental) here
---
# Fivetran
type: dagster_fivetran.FivetranAccountComponent
attributes:
  workspace:
    account_id: "{{ env.FIVETRAN_ACCOUNT_ID }}"
    api_key: "{{ env.FIVETRAN_API_KEY }}"
    api_secret: "{{ env.FIVETRAN_API_SECRET }}"
    disable_schedule_on_trigger: false   # keep Fivetran's own schedule
    retry_on_reschedule: false
  polling_sensor: true                   # observe syncs without triggering
```

API sources: Sling keys ([Sling integration](https://dagster.io/docs/integrations/libraries/sling)); `mode` values ([Sling replication modes](https://docs.slingdata.io/concepts/replication/modes)); Fivetran keys and their placement ([Fivetran integration](https://dagster.io/docs/integrations/libraries/fivetran)).

## 8. Scenario scorecard

*Judgement, using the 0–3 rubric in [the plan](00-plan.md#8-rating-rubric).*

| Scenario | Temporal OSS | Dagster OSS | Why |
|---|---|---|---|
| S1 API polling | 3 | 2 | Temporal: per-page retries, heartbeats, queue-wide rate limit. Dagster: dlt asset + pools, coarser retries |
| S2 File arrival | 2 | 3 | Dagster: sensor cursor + run key + dynamic partitions. Temporal: no bucket trigger; build lister |
| S3 DB incremental | 2 | 2 | Temporal: resumable checkpoints. Dagster: Sling/dlt assets + partitions; Sling has incremental/CDC modes, but no Dagster example of them |
| S4 SaaS connectors | 1 | 3 | Dagster first-party Fivetran/Airbyte/dlt/Sling. Temporal: custom API calls |
| S5 Large backfill | 2 | 3 | Dagster: partition backfills with policies. Temporal: strong batch patterns but you build data tracking and respect history limits |

## Sources

- [S1] [Understanding Temporal](https://docs.temporal.io/evaluate/understanding-temporal)
- [S6] [Activity Definition](https://docs.temporal.io/activity-definition)
- [S7] [Retry Policies](https://docs.temporal.io/encyclopedia/retry-policies)
- [S10] [Schedule](https://docs.temporal.io/schedule)
- [S11] [Self-hosted defaults](https://docs.temporal.io/self-hosted-guide/defaults)
- [S17] [Troubleshoot payload size limit](https://docs.temporal.io/troubleshooting/blob-size-limit-error)
- [S18] [External Storage](https://docs.temporal.io/external-storage)
- [S20] [Long-Running Activity pattern](https://docs.temporal.io/design-patterns/long-running-activity)
- [S21] [Batch Processing Patterns](https://docs.temporal.io/design-patterns/batch-processing-patterns)
- [S24] [Downstream Rate Limiting](https://docs.temporal.io/design-patterns/downstream-rate-limiting)
- [S26] [Workflow Streams](https://docs.temporal.io/workflow-streams) — *not fetched*
- [S28] [How Rapidflare built a million+ document ingestion pipeline](https://temporal.io/blog/how-rapidflare-built-a-million-document-ingestion-pipeline-for-agents-on-temporal) — *vendor blog, customer guest post*
- [S29] [Airbyte case study](https://web.temporal.io/case-studies/airbyte-case-study) — *vendor case study, not fetched*
- [S32] [Observability - Python SDK](https://docs.temporal.io/develop/python/platform/observability)
- [S50] [Temporal changelog](https://temporal.io/changelog)
- [S53] [dagster.yaml reference](https://dagster.io/docs/deployment/oss/dagster-yaml)
- [S55] [Run coordinators](https://dagster.io/docs/deployment/execution/run-coordinators)
- [S58] [Op retries](https://dagster.io/docs/guides/build/ops/op-retries)
- [S59] [Run retries](https://dagster.io/docs/deployment/execution/run-retries)
- [S60] [Declarative Automation](https://dagster.io/docs/guides/automate/declarative-automation)
- [S61] [Sensors](https://dagster.io/docs/guides/automate/sensors)
- [S63] [Managing concurrency](https://dagster.io/docs/guides/operate/managing-concurrency)
- [S64] [Backfilling data](https://dagster.io/docs/guides/build/partitions-and-backfills/backfilling-data)
- [S65] [Partition backfill strategies](https://dagster.io/docs/examples/best-practices/partition-backfill-strategies)
- [S66] [Dagster Pipes (guide)](https://dagster.io/docs/guides/build/external-pipelines)
- [S67] [External pipelines (Pipes) integrations](https://dagster.io/docs/integrations/external-pipelines)
- [S68] [Dagster integrations index](https://dagster.io/docs/integrations/libraries)
- [S69] [dlt integration](https://dagster.io/docs/integrations/libraries/dlt)
- [S70] [dlt integration (Pythonic)](https://dagster.io/docs/integrations/libraries/dlt/dlt-pythonic)
- [S71] [Sling integration](https://dagster.io/docs/integrations/libraries/sling)
- [S72] [Embedded ELT (deprecated)](https://dagster.io/docs/integrations/libraries/embedded-elt)
- [S73] [Airbyte integration](https://dagster.io/docs/integrations/libraries/airbyte)
- [S74] [Airbyte component](https://dagster.io/docs/integrations/libraries/airbyte/airbyte-component)
- [S75] [Fivetran integration](https://dagster.io/docs/integrations/libraries/fivetran)
- [S76] [Meltano integration](https://dagster.io/docs/integrations/libraries/meltano)
- [S77] [dbt integration](https://dagster.io/docs/integrations/libraries/dbt)
- [S84] [Execution API reference](https://docs.dagster.io/api/dagster/execution)
- [S94] [dagster-io/dagster releases](https://github.com/dagster-io/dagster/releases) — *GitHub HTML scraped*
- [S101] [Dynamic partitioning](https://dagster.io/blog/dynamic-partitioning) — *vendor blog*
- [S103] [dagster-airbyte library reference](https://dagster.io/docs/integrations/libraries/airbyte/dagster-airbyte)
- [S104] [Sling (Pythonic)](https://dagster.io/docs/integrations/libraries/sling/sling-pythonic)
- [S105] [Sling replication modes](https://docs.slingdata.io/concepts/replication/modes) — *Sling's official docs*
- [S106] [External Storage - Python SDK](https://docs.temporal.io/develop/python/data-handling/external-storage)
- [S108] [Schedules and sensors API reference](https://dagster.io/docs/api/dagster/schedules-sensors)
- [S109] [Partitions API reference](https://dagster.io/docs/api/dagster/partitions)
- [S110] [Assets API reference](https://dagster.io/docs/api/dagster/assets)
- [S112] [dagster 1.13.29 (PyPI)](https://pypi.org/project/dagster/1.13.29/) — *package source*
- [S113] [dagster-aws 0.29.29 (PyPI)](https://pypi.org/project/dagster-aws/0.29.29/) — *package source*
- [S114] [dagster-aws API reference](https://dagster.io/docs/api/libraries/dagster-aws)
- [S115] [Activity Timeouts - Python SDK](https://docs.temporal.io/develop/python/activities/timeouts)
- [S116] [temporalio 1.36.0 (PyPI)](https://pypi.org/project/temporalio/1.36.0/) — *package source*

Full numbered list: [references/sources.md](../references/sources.md)
