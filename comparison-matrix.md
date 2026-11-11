# Comparison matrix

> **Key takeaways**
> - One row per dimension from [the plan](docs/00-plan.md); each fact cell carries its own citation.
> - The **Rating** columns use the 0–3 rubric (3 = built in, 0 = not in OSS or not verified) and are **judgements**, not facts.
> - Dagster leads on data awareness, connectors and ramp-up; Temporal leads on durability, language reach, rate limiting and OSS security.
> - "Not verified" cells point to [doc 13](docs/13-unverified-and-open-questions.md).

As of 2026-11-11. Temporal Server v1.32.1; Dagster 1.13.29.

## Facts

| Dimension | Temporal OSS | Dagster OSS |
|---|---|---|
| **D1 Core abstraction** | Deterministic Workflows + side-effecting Activities ([Workflow Definition](https://docs.temporal.io/workflow-definition.md)) | Software-defined assets; ops/jobs/graphs below ([dagster-io/dagster (GitHub)](https://github.com/dagster-io/dagster)) |
| D1 Positioning | Durable Execution, runs "whether that takes a second or a year" ([Understanding Temporal](https://docs.temporal.io/evaluate/understanding-temporal)) | Data orchestrator, "data-asset aware" ([Why We Built Dagster](https://dagster.io/blog/dagster-the-data-orchestrator)) |
| **D2 Control plane** | Frontend, History, Matching, internal Worker ([Temporal Server](https://docs.temporal.io/temporal-service/temporal-server)) | Webserver, daemon, code-location servers ([OSS deployment architecture](https://dagster.io/docs/deployment/oss/oss-deployment-architecture)) |
| D2 Database | Cassandra / PostgreSQL / MySQL; SQLite dev only ([Persistence](https://docs.temporal.io/temporal-service/persistence)) | SQLite / PostgreSQL / MySQL ([dagster.yaml reference](https://dagster.io/docs/deployment/oss/dagster-yaml)) |
| D2 Extra store | Elasticsearch or SQL visibility ([Persistence](https://docs.temporal.io/temporal-service/persistence)) | Compute logs: local, S3, GCS, Azure ([dagster.yaml reference](https://dagster.io/docs/deployment/oss/dagster-yaml)) |
| D2 Where user code runs | Your Workers only ([Understanding Temporal](https://docs.temporal.io/evaluate/understanding-temporal)) | Code servers, run workers, step processes ([OSS deployment architecture](https://dagster.io/docs/deployment/oss/oss-deployment-architecture)) |
| D2 Scheduler HA | Services run multiple instances ([Temporal Server](https://docs.temporal.io/temporal-service/temporal-server)) | Single daemon, no replicas ([OSS deployment architecture](https://dagster.io/docs/deployment/oss/oss-deployment-architecture)) |
| **D3 Recovery unit** | Event History replay + Activity heartbeat checkpoint ([Workflow Definition](https://docs.temporal.io/workflow-definition.md), [Long-Running Activity pattern](https://docs.temporal.io/design-patterns/long-running-activity)) | Step retry, run retry, re-execute from failure ([Op retries](https://dagster.io/docs/guides/build/ops/op-retries), [Run retries](https://dagster.io/docs/deployment/execution/run-retries)) |
| D3 Default step retry | Unlimited, 1 s ×2.0 to 100 s ([Retry Policies](https://docs.temporal.io/encyclopedia/retry-policies)) | Only when a `RetryPolicy` is attached ([Op retries](https://dagster.io/docs/guides/build/ops/op-retries)) |
| D3 Default whole-unit retry | Workflows: none ([Retry Policies](https://docs.temporal.io/encyclopedia/retry-policies)) | Run retries opt-in in OSS ([Run retries](https://dagster.io/docs/deployment/execution/run-retries)) |
| D3 Timeouts | Start-To-Close or Schedule-To-Close required; heartbeat timeout ([Detecting Activity failures](https://docs.temporal.io/encyclopedia/detecting-activity-failures.md)) | `run_monitoring` start/cancel 180 s (opt-in) ([Instance configuration](https://dagster.io/docs/deployment/oss/oss-instance-configuration)) |
| D3 Code-change constraint | Determinism; Worker Versioning or patching ([Safely deploying changes to Workflow code](https://docs.temporal.io/develop/safe-deployments)) | None documented; reload code location ([Dagster webserver and UI](https://dagster.io/docs/guides/operate/webserver)) |
| **D4 Time scheduling** | Schedules: interval/calendar/cron, jitter, 1-year catch-up ([Schedule](https://docs.temporal.io/schedule)) | Schedules + `on_cron` automation in daemon ([OSS deployment architecture](https://dagster.io/docs/deployment/oss/oss-deployment-architecture), [Declarative Automation](https://dagster.io/docs/guides/automate/declarative-automation)) |
| D4 Overlap control | 6 policies, default Skip ([Schedule](https://docs.temporal.io/schedule)) | Run queue + pools ([Managing concurrency](https://dagster.io/docs/guides/operate/managing-concurrency)) |
| D4 Event triggers | Signals/Updates, 2,000 pending Signals ([Workflow Execution limits](https://docs.temporal.io/workflow-execution/limits)) | Sensors with cursor + run key; 60 s timeout ([Sensors](https://dagster.io/docs/guides/automate/sensors), [Troubleshooting sensor timeouts](https://dagster.io/docs/deployment/troubleshooting/sensor-timeouts)) |
| D4 Upstream-aware | Not built in (*inference*) | `eager`, `on_missing` conditions ([Declarative Automation](https://dagster.io/docs/guides/automate/declarative-automation)) |
| **D5 Lineage** | None found (*inference*) | Global asset lineage ([Dagster webserver and UI](https://dagster.io/docs/guides/operate/webserver)) |
| D5 Partitions | None (*inference*) | Partitioned assets ([Backfilling data](https://dagster.io/docs/guides/build/partitions-and-backfills/backfilling-data)) |
| D5 Backfill unit | Schedule action times ([Schedule](https://docs.temporal.io/schedule)) | Partitions; per-partition, multi-run, single-run ([Partition backfill strategies](https://dagster.io/docs/examples/best-practices/partition-backfill-strategies)) |
| D5 Data quality | None built in | Asset checks, optional `blocking=True` ([Testing assets with asset checks](https://dagster.io/docs/guides/test/asset-checks)) |
| **D6 Connectors** | None official found; user Activities (*inference*) | dlt, Sling, Airbyte, Fivetran (supported); Meltano (community) ([Dagster integrations index](https://dagster.io/docs/integrations/libraries)) |
| D6 Downstream dbt | None | `DbtProjectComponent`, source → asset key links ([dbt integration](https://dagster.io/docs/integrations/libraries/dbt)) |
| D6 Large payloads | 2 MB default; references or External Storage (Public Preview) ([Troubleshoot payload size limit](https://docs.temporal.io/troubleshooting/blob-size-limit-error), [External Storage](https://docs.temporal.io/external-storage)) | Data written by assets; Pipes returns logs/metadata only ([Dagster Pipes (guide)](https://dagster.io/docs/guides/build/external-pipelines)) |
| D6 Idempotency | Activities may run multiple times; idempotency key = Run ID + Activity ID ([Activity Definition](https://docs.temporal.io/activity-definition)) | Sensor run keys dedupe runs ([Sensors](https://dagster.io/docs/guides/automate/sensors)) |
| D6 Rate limiting | Queue-global `max_task_queue_activities_per_second` ([Downstream Rate Limiting](https://docs.temporal.io/design-patterns/downstream-rate-limiting)) | Concurrency pools for shared APIs ([Managing concurrency](https://dagster.io/docs/guides/operate/managing-concurrency)) |
| D6 Batch fan-out | Child Workflows, Batch Iterator, Sliding Window, MapReduce ([Batch Processing Patterns](https://docs.temporal.io/design-patterns/batch-processing-patterns)) | Backfill policies; executors ([Partition backfill strategies](https://dagster.io/docs/examples/best-practices/partition-backfill-strategies), [Run executors](https://dagster.io/docs/guides/operate/run-executors)) |
| **D7 UI focus** | Executions, Event History, call stack ([Temporal Web UI](https://docs.temporal.io/web-ui)) | Assets, runs, lineage, backfills ([Dagster webserver and UI](https://dagster.io/docs/guides/operate/webserver)) |
| D7 Metrics / tracing | Prometheus + OpenTelemetry in SDKs ([Observability - Python SDK](https://docs.temporal.io/develop/python/platform/observability)) | Not verified |
| D7 Logs | `workflow.logger` ([Observability - Python SDK](https://docs.temporal.io/develop/python/platform/observability)) | Event log + compute logs ([Instance configuration](https://dagster.io/docs/deployment/oss/oss-instance-configuration)) |
| D7 Alerting | Bring your own (*inference*) | Dagster+ only ([Observe](https://dagster.io/docs/guides/observe)) |
| D7 Search | List Filter on Search Attributes ([Observability - Python SDK](https://docs.temporal.io/develop/python/platform/observability)) | Catalog filters; richer ones Dagster+ Pro ([Dagster webserver and UI](https://dagster.io/docs/guides/operate/webserver)) |
| **D8 Unit testing** | `ActivityEnvironment`, mocked Activities ([Testing - Python SDK](https://docs.temporal.io/develop/python/testing-suite)) | Direct invocation, `build_asset_context()` ([Unit testing assets and ops](https://dagster.io/docs/guides/test/unit-testing-assets-and-ops)) |
| D8 Special testing | Time skipping, replay in CI ([Testing - Python SDK](https://docs.temporal.io/develop/python/testing-suite)) | Asset checks at runtime ([Testing assets with asset checks](https://dagster.io/docs/guides/test/asset-checks)) |
| D8 Local dev | `temporal server start-dev`, UI 8233 ([Temporal CLI server command reference](https://docs.temporal.io/cli/server)) | `dg dev` ([Running Dagster locally](https://dagster.io/docs/deployment/oss/deployment-options/running-dagster-locally)) |
| D8 Languages | 8 SDKs ([Understanding Temporal](https://docs.temporal.io/evaluate/understanding-temporal)) | Python; Pipes for others ([Dagster Pipes (guide)](https://dagster.io/docs/guides/build/external-pipelines)) |
| **D9 Install paths** | Dev server, Compose, binaries, embedded Go, Helm ([Deploying a Temporal Service](https://docs.temporal.io/self-hosted-guide/deployment)) | Local, Compose, Helm, ECS, GCP ([Deploying to Kubernetes](https://dagster.io/docs/deployment/oss/deployment-options/kubernetes/deploying-to-kubernetes)) |
| D9 Fixed-at-creation | History Shards, 1–128K ([Temporal Server](https://docs.temporal.io/temporal-service/temporal-server)) | None found |
| D9 Payload limit | 512 KB warn / 2 MB error ([Self-hosted defaults](https://docs.temporal.io/self-hosted-guide/defaults)) | Not applicable |
| D9 History limit | 10,240 / 51,200 events; 10 / 50 MB ([Self-hosted defaults](https://docs.temporal.io/self-hosted-guide/defaults)) | Not applicable |
| D9 Pending ops | 2,000 each (≤500 recommended) ([Self-hosted defaults](https://docs.temporal.io/self-hosted-guide/defaults)) | `max_concurrent_runs`, pools ([Managing concurrency](https://dagster.io/docs/guides/operate/managing-concurrency)) |
| **D10 Default posture** | Allows every request ([Temporal Platform security features](https://docs.temporal.io/self-hosted-guide/security)) | No auth in OSS ([GitHub Discussion #23851 (maintainer answer)](https://github.com/dagster-io/dagster/discussions/23851)) |
| D10 AuthN / AuthZ | mTLS, JWT ClaimMapper, Authorizer, UI SSO ([Temporal Platform security features](https://docs.temporal.io/self-hosted-guide/security)) | Dagster+ only ([GitHub Discussion #23851 (maintainer answer)](https://github.com/dagster-io/dagster/discussions/23851)) |
| D10 Payload encryption | Payload Codec ([Temporal Platform security features](https://docs.temporal.io/self-hosted-guide/security)) | No equivalent found (*inference*) |
| **D11 Licence** | MIT ([temporalio/temporal (GitHub)](https://github.com/temporalio/temporal)) | Apache-2.0 ([PyPI dagster JSON](https://pypi.org/pypi/dagster/json)) |
| D11 Stars (scraped) | ~23.7k ([temporalio/temporal (GitHub)](https://github.com/temporalio/temporal)) | ~16.1k ([dagster-io/dagster (GitHub)](https://github.com/dagster-io/dagster)) |
| D11 Ownership | Temporal Technologies ([temporalio/temporal (GitHub)](https://github.com/temporalio/temporal)) | Being acquired by Prefect ([Prefect is acquiring Dagster](https://dagster.io/blog/prefect-is-acquiring-dagster)) |
| **D12 Interop** | No official Dagster integration (*absence*) | No Temporal integration listed ([Dagster integrations index](https://dagster.io/docs/integrations/libraries)) |

## Ratings (judgement, 0–3)

| Dimension | Temporal OSS | Dagster OSS | Note |
|---|---|---|---|
| D1 Fit of abstraction for ingestion | 2 | 3 | Assets match "tables to keep fresh" |
| D2 Operational simplicity | 1 | 2 | 4 services + DB + shards vs 3 services + DB, but single daemon |
| D3 Durability | 3 | 2 | Mid-step resume vs step-level recovery |
| D4 Scheduling and triggers | 3 | 3 | Time scheduling vs data-aware triggers |
| D5 Data awareness | 0 | 3 | Lineage, partitions, checks |
| D6 Ingestion: connectors | 1 | 3 | First-party ELT components |
| D6 Ingestion: custom/high-volume | 3 | 2 | Rate limits, sliding window, heartbeats |
| D7 Observability in OSS | 2 | 2 | Metrics/tracing vs lineage/catalog; alerts missing in both |
| D8 Testing and DX | 2 | 3 | Determinism and versioning overhead |
| D9 Scale headroom | 3 | 2 | Sharded server vs single daemon |
| D10 Security in OSS | 2 | 0 | Opt-in controls vs none built in |
| D11 Licensing and stewardship | 3 | 2 | Both permissive; ownership change for Dagster |
| D12 Interoperability | 0 | 0 | No official integration |

## Sources

- [S1] [Understanding Temporal](https://docs.temporal.io/evaluate/understanding-temporal)
- [S2] [Temporal Server](https://docs.temporal.io/temporal-service/temporal-server)
- [S3] [Persistence](https://docs.temporal.io/temporal-service/persistence)
- [S5] [Workflow Definition](https://docs.temporal.io/workflow-definition.md)
- [S6] [Activity Definition](https://docs.temporal.io/activity-definition)
- [S7] [Retry Policies](https://docs.temporal.io/encyclopedia/retry-policies)
- [S8] [Detecting Activity failures](https://docs.temporal.io/encyclopedia/detecting-activity-failures.md)
- [S10] [Schedule](https://docs.temporal.io/schedule)
- [S11] [Self-hosted defaults](https://docs.temporal.io/self-hosted-guide/defaults)
- [S12] [Workflow Execution limits](https://docs.temporal.io/workflow-execution/limits)
- [S14] [Deploying a Temporal Service](https://docs.temporal.io/self-hosted-guide/deployment)
- [S17] [Troubleshoot payload size limit](https://docs.temporal.io/troubleshooting/blob-size-limit-error)
- [S18] [External Storage](https://docs.temporal.io/external-storage)
- [S20] [Long-Running Activity pattern](https://docs.temporal.io/design-patterns/long-running-activity)
- [S21] [Batch Processing Patterns](https://docs.temporal.io/design-patterns/batch-processing-patterns)
- [S24] [Downstream Rate Limiting](https://docs.temporal.io/design-patterns/downstream-rate-limiting)
- [S31] [Temporal Web UI](https://docs.temporal.io/web-ui)
- [S32] [Observability - Python SDK](https://docs.temporal.io/develop/python/platform/observability)
- [S39] [Temporal CLI server command reference](https://docs.temporal.io/cli/server)
- [S41] [Testing - Python SDK](https://docs.temporal.io/develop/python/testing-suite)
- [S44] [Safely deploying changes to Workflow code](https://docs.temporal.io/develop/safe-deployments)
- [S45] [Temporal Platform security features](https://docs.temporal.io/self-hosted-guide/security)
- [S48] [temporalio/temporal (GitHub)](https://github.com/temporalio/temporal) — *GitHub HTML scraped*
- [S52] [OSS deployment architecture](https://dagster.io/docs/deployment/oss/oss-deployment-architecture)
- [S53] [dagster.yaml reference](https://dagster.io/docs/deployment/oss/dagster-yaml)
- [S54] [Instance configuration](https://dagster.io/docs/deployment/oss/oss-instance-configuration)
- [S56] [Run executors](https://dagster.io/docs/guides/operate/run-executors)
- [S58] [Op retries](https://dagster.io/docs/guides/build/ops/op-retries)
- [S59] [Run retries](https://dagster.io/docs/deployment/execution/run-retries)
- [S60] [Declarative Automation](https://dagster.io/docs/guides/automate/declarative-automation)
- [S61] [Sensors](https://dagster.io/docs/guides/automate/sensors)
- [S62] [Troubleshooting sensor timeouts](https://dagster.io/docs/deployment/troubleshooting/sensor-timeouts)
- [S63] [Managing concurrency](https://dagster.io/docs/guides/operate/managing-concurrency)
- [S64] [Backfilling data](https://dagster.io/docs/guides/build/partitions-and-backfills/backfilling-data)
- [S65] [Partition backfill strategies](https://dagster.io/docs/examples/best-practices/partition-backfill-strategies)
- [S66] [Dagster Pipes (guide)](https://dagster.io/docs/guides/build/external-pipelines)
- [S68] [Dagster integrations index](https://dagster.io/docs/integrations/libraries)
- [S77] [dbt integration](https://dagster.io/docs/integrations/libraries/dbt)
- [S78] [Dagster webserver and UI](https://dagster.io/docs/guides/operate/webserver)
- [S79] [Observe](https://dagster.io/docs/guides/observe)
- [S82] [Unit testing assets and ops](https://dagster.io/docs/guides/test/unit-testing-assets-and-ops)
- [S83] [Testing assets with asset checks](https://dagster.io/docs/guides/test/asset-checks)
- [S86] [Running Dagster locally](https://dagster.io/docs/deployment/oss/deployment-options/running-dagster-locally)
- [S88] [Deploying to Kubernetes](https://dagster.io/docs/deployment/oss/deployment-options/kubernetes/deploying-to-kubernetes)
- [S89] [GitHub Discussion #23851 (maintainer answer)](https://github.com/dagster-io/dagster/discussions/23851)
- [S93] [dagster-io/dagster (GitHub)](https://github.com/dagster-io/dagster) — *GitHub HTML scraped*
- [S95] [PyPI dagster JSON](https://pypi.org/pypi/dagster/json)
- [S96] [Prefect is acquiring Dagster](https://dagster.io/blog/prefect-is-acquiring-dagster) — *vendor blog*
- [S100] [Why We Built Dagster](https://dagster.io/blog/dagster-the-data-orchestrator) — *vendor blog*

Full numbered list: [references/sources.md](references/sources.md)
