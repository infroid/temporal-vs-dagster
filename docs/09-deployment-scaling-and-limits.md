# 09 — Deployment, scaling and limits

> **Key takeaways**
> - **Temporal OSS** install paths: CLI dev server, Docker Compose, release binaries + systemd, embedded Go package, Helm charts ([Deploying a Temporal Service](https://docs.temporal.io/self-hosted-guide/deployment)).
> - **Dagster OSS** install paths: local, Docker Compose, Kubernetes (`dagster/dagster` Helm chart), AWS (ECS), GCP ([Deploying to Kubernetes](https://dagster.io/docs/deployment/oss/deployment-options/kubernetes/deploying-to-kubernetes)).
> - Temporal scales through History Shards (**fixed at cluster creation**; run successfully with 1–128K) and horizontally scaled Workers ([Temporal Server](https://docs.temporal.io/temporal-service/temporal-server), [Task Queue performance](https://docs.temporal.io/develop/worker-performance/task-queues)).
> - Dagster scales through run launchers/executors (process, Docker, Kubernetes, Celery) and concurrency limits; the daemon is a **single replica** ([OSS deployment architecture](https://dagster.io/docs/deployment/oss/oss-deployment-architecture), [Run executors](https://dagster.io/docs/guides/operate/run-executors)).
> - Temporal's per-execution limits (2 MB payload, 51,200 events/50 MB history, 2,000 pending ops) shape ingestion design ([Self-hosted defaults](https://docs.temporal.io/self-hosted-guide/defaults)).

## 1. Deployment options

| | Temporal OSS | Dagster OSS |
|---|---|---|
| Dev | `temporal server start-dev`, UI at localhost:8233 ([temporalio/temporal (GitHub)](https://github.com/temporalio/temporal)) | `dg dev` ([Running Dagster locally](https://dagster.io/docs/deployment/oss/deployment-options/running-dagster-locally)) |
| Compose | `temporalio/samples-server` `compose/`: PostgreSQL + Elasticsearch, gRPC 7233, UI 8080 ([Deploying a Temporal Service](https://docs.temporal.io/self-hosted-guide/deployment)) | Docker Compose listed ([Deploying to Kubernetes](https://dagster.io/docs/deployment/oss/deployment-options/kubernetes/deploying-to-kubernetes)) |
| Kubernetes | `temporalio/helm-charts`; own DB/Elasticsearch; images 1.30+ need chart 0.73.1+; schema updates via `temporalio/admin-tools` are the operator's job ([Deploying a Temporal Service](https://docs.temporal.io/self-hosted-guide/deployment)) | `dagster/dagster` chart, new chart per Dagster version; deploys daemon, webserver (guide port-forwards local 8080 to pod port 80), user-code gRPC servers, in-cluster PostgreSQL ([Deploying to Kubernetes](https://dagster.io/docs/deployment/oss/deployment-options/kubernetes/deploying-to-kubernetes)) |
| Other | Release binaries + systemd; embed as Go package ([Deploying a Temporal Service](https://docs.temporal.io/self-hosted-guide/deployment)) | AWS (ECS), GCP ([Deploying to Kubernetes](https://dagster.io/docs/deployment/oss/deployment-options/kubernetes/deploying-to-kubernetes)) |
| K8s operator tooling | Temporal Worker Controller GA 2026-05-04 ([Temporal changelog](https://temporal.io/changelog)) | ECS: cross-account discovery (1.13.24), resumable run workers (1.13.25) ([dagster-io/dagster releases](https://github.com/dagster-io/dagster/releases)) |
| "Production-ready" statement for Helm | None found | None found in notes |
| Network exposure | Must not be reachable from the public internet ([Deploying a Temporal Service](https://docs.temporal.io/self-hosted-guide/deployment)) | See [doc 10](10-security.md) |

## 2. Scaling knobs

### Temporal

| Knob | Fact |
|---|---|
| History Shards | Unit of concurrency; one persistence partition each; set before DB integration and "cannot be changed"; has "operated successfully using anywhere from 1 to 128K" shards, no theoretical limit ([Temporal Server](https://docs.temporal.io/temporal-service/temporal-server)) |
| History processes | 1 to number of shards; start ~1 per 500 shards ([Temporal Server](https://docs.temporal.io/temporal-service/temporal-server)) |
| Workers | ≥2 per Task Queue; poll success >90% (>95% high-volume); raise slots (`maxConcurrentActivityExecutionSize`) or pollers; watch `ApproximateBacklogCount` ([Task Queue performance](https://docs.temporal.io/develop/worker-performance/task-queues)) |
| Rate limits | `maxTaskQueueActivitiesPerSecond` (server-enforced), `maxWorkerActivitiesPerSecond` ([Task Queue performance](https://docs.temporal.io/develop/worker-performance/task-queues)) |
| Concurrent executions | No limit apart from per-execution history ([Workflow Execution limits](https://docs.temporal.io/workflow-execution/limits)) |
| Task queue partitions | Default count and dynamic-config keys **Not verified** |

### Dagster

| Knob | Fact |
|---|---|
| Run queue | `QueuedRunCoordinator` with `max_concurrent_runs`: default **10** (`-1` = no limit, `0` = launch nothing) per config schema in package source; docs show examples 15/25 and 10 but no default ([dagster 1.13.29 (PyPI)](https://pypi.org/project/dagster/1.13.29/), [dagster.yaml reference](https://dagster.io/docs/deployment/oss/dagster-yaml), [Managing concurrency](https://dagster.io/docs/guides/operate/managing-concurrency), [Run queue limits](https://dagster.io/docs/guides/operate/managing-concurrency/run-queue-limits)) |
| Pools | Cross-run limits for shared DBs/APIs, e.g. `pools: default_limit: 3` ([Managing concurrency](https://dagster.io/docs/guides/operate/managing-concurrency)) |
| Op concurrency | `concurrency.default_op_concurrency_limit` (example 10) ([dagster.yaml reference](https://dagster.io/docs/deployment/oss/dagster-yaml)) |
| Run isolation | `DockerRunLauncher` (container per run), `K8sRunLauncher` (K8s job per run) ([dagster.yaml reference](https://dagster.io/docs/deployment/oss/dagster-yaml)) |
| Step isolation | `multiprocess_executor` with configurable parallelism; `k8s_job_executor` pod per step; `celery_executor` ([Run executors](https://dagster.io/docs/guides/operate/run-executors)) |
| Sensor throughput | `DAGSTER_GRPC_MAX_WORKERS`, `sensors.use_threads`, `num_workers` ([Troubleshooting sensor timeouts](https://dagster.io/docs/deployment/troubleshooting/sensor-timeouts)) |
| Daemon | One instance; replicas not supported ([OSS deployment architecture](https://dagster.io/docs/deployment/oss/oss-deployment-architecture)) |

## 3. Limits table (exact numbers)

### Temporal OSS self-hosted defaults

| Limit | Warn | Error / max | Source |
|---|---|---|---|
| Blob (each payload: argument or result) | 512 KB | 2 MB (`ErrBlobSizeExceedsLimit`) | [Self-hosted defaults](https://docs.temporal.io/self-hosted-guide/defaults) |
| gRPC received message | — | 4 MB | [Self-hosted defaults](https://docs.temporal.io/self-hosted-guide/defaults) |
| Event batch transaction (`DefaultTransactionSizeLimit`) | — | 4 MB | [Self-hosted defaults](https://docs.temporal.io/self-hosted-guide/defaults) |
| Event History events | 10,240 | 51,200 (execution terminated) | [Self-hosted defaults](https://docs.temporal.io/self-hosted-guide/defaults), [Workflow Execution limits](https://docs.temporal.io/workflow-execution/limits) |
| Event History size | 10 MB | 50 MB (execution terminated) | [Self-hosted defaults](https://docs.temporal.io/self-hosted-guide/defaults), [Workflow Execution limits](https://docs.temporal.io/workflow-execution/limits) |
| Pending Activities per execution | — | 2,000 (≤500 recommended) | [Self-hosted defaults](https://docs.temporal.io/self-hosted-guide/defaults) |
| Pending Child Workflows per execution | — | 2,000 | [Self-hosted defaults](https://docs.temporal.io/self-hosted-guide/defaults), [Workflow Execution limits](https://docs.temporal.io/workflow-execution/limits) |
| Pending external Signals per execution | — | 2,000 | [Self-hosted defaults](https://docs.temporal.io/self-hosted-guide/defaults), [Workflow Execution limits](https://docs.temporal.io/workflow-execution/limits) |
| Pending cancel requests per execution | — | 2,000 | [Self-hosted defaults](https://docs.temporal.io/self-hosted-guide/defaults) |
| Updates per execution | — | 10 in-flight; 2,000 total | [Self-hosted defaults](https://docs.temporal.io/self-hosted-guide/defaults) |
| Incomplete Nexus Operations per execution | — | 30 | [Workflow Execution limits](https://docs.temporal.io/workflow-execution/limits) |
| Identifier length (Workflow Id, Type, Task Queue) | — | 1,000 chars (sample SQL config: 255) | [Self-hosted defaults](https://docs.temporal.io/self-hosted-guide/defaults) |
| Batch jobs | — | 1 at a time | [Self-hosted defaults](https://docs.temporal.io/self-hosted-guide/defaults) |
| Schedule last completion result | — | 2 MiB (not passed if within 1 KiB of limit) | [Schedule](https://docs.temporal.io/schedule) |
| Schedule catch-up window | — | default 1 year, min 10 s | [Schedule](https://docs.temporal.io/schedule) |
| History Shards | — | Fixed at creation; 1–128K used in practice (not a hard cap) | [Temporal Server](https://docs.temporal.io/temporal-service/temporal-server) |
| Workflow Task retry interval | — | up to 10 min | [Retry Policies](https://docs.temporal.io/encyclopedia/retry-policies) |
| Activity default max retry interval | — | 100 s | [Retry Policies](https://docs.temporal.io/encyclopedia/retry-policies) |

When a pending-operation limit is hit, the Commands fail ([Self-hosted defaults](https://docs.temporal.io/self-hosted-guide/defaults)) and the Workflow Task fails and is retried ([Workflow Execution limits](https://docs.temporal.io/workflow-execution/limits)).

### Temporal Cloud (contrast only)

| Limit | Value | Source |
|---|---|---|
| Payload / gRPC / history transaction | 2 MB / 4 MB / 4 MB, not configurable | [Temporal Cloud limits](https://docs.temporal.io/cloud/limits) |
| History | 51,200 events / 50 MB, not configurable | [Temporal Cloud limits](https://docs.temporal.io/cloud/limits) |
| Actions per second | 500 APS default per namespace | [Temporal Cloud limits](https://docs.temporal.io/cloud/limits) |
| Schedules / Visibility API | 10 req/s / 30 calls/s | [Temporal Cloud limits](https://docs.temporal.io/cloud/limits) |
| Retention | 30 days default, 1–90 days | [Temporal Cloud limits](https://docs.temporal.io/cloud/limits) |
| Signals per execution | 10,000 | [Temporal Cloud limits](https://docs.temporal.io/cloud/limits) |

### Dagster OSS

| Limit / default | Value | Source |
|---|---|---|
| Sensor evaluation timeout | 60 s default (`DAGSTER_SENSOR_GRPC_TIMEOUT_SECONDS`) | [Troubleshooting sensor timeouts](https://dagster.io/docs/deployment/troubleshooting/sensor-timeouts) |
| Sensor evaluation guideline | >3 min means a sensor "may not be the correct tool" | [Troubleshooting sensor timeouts](https://dagster.io/docs/deployment/troubleshooting/sensor-timeouts) |
| Daemon replicas | 1 (replicas not supported) | [OSS deployment architecture](https://dagster.io/docs/deployment/oss/oss-deployment-architecture) |
| Code-location server replicas | 1 per code location | [OSS deployment architecture](https://dagster.io/docs/deployment/oss/oss-deployment-architecture) |
| Run monitoring start / cancel timeout | 180 s / 180 s | [Instance configuration](https://dagster.io/docs/deployment/oss/oss-instance-configuration) |
| Run monitoring resume attempts | 3 | [Instance configuration](https://dagster.io/docs/deployment/oss/oss-instance-configuration) |
| Run monitoring poll interval | 120 s | [Instance configuration](https://dagster.io/docs/deployment/oss/oss-instance-configuration) |
| Free slots after run end | 300 s | [Instance configuration](https://dagster.io/docs/deployment/oss/oss-instance-configuration) |
| Sensor tick request size | Over-limit ticks fail with explicit error (1.13.26); numeric limit **Not verified** | [dagster-io/dagster releases](https://github.com/dagster-io/dagster/releases) |
| Default `max_concurrent_runs` (`QueuedRunCoordinator`) | 10; `-1` disables the limit (config schema, not in docs pages) | [dagster 1.13.29 (PyPI)](https://pypi.org/project/dagster/1.13.29/) |
| Default sensor interval | 30 s when `minimum_interval_seconds` is unset (`DEFAULT_SENSOR_DAEMON_INTERVAL = 30`; API docs list no default) | [dagster 1.13.29 (PyPI)](https://pypi.org/project/dagster/1.13.29/), [Schedules and sensors API reference](https://dagster.io/docs/api/dagster/schedules-sensors) |
| Run queue dequeue interval | 5 s fallback in `QueuedRunCoordinator` (package source) | [dagster 1.13.29 (PyPI)](https://pypi.org/project/dagster/1.13.29/) |

*Judgement:* Temporal's limits are per *execution*, so they push you to split work into many small executions. Dagster's limits are mostly about *throughput of the control plane* (sensor time, single daemon), which pushes you to keep sensors light and heavy work in runs.

## Sources

- [S2] [Temporal Server](https://docs.temporal.io/temporal-service/temporal-server)
- [S7] [Retry Policies](https://docs.temporal.io/encyclopedia/retry-policies)
- [S10] [Schedule](https://docs.temporal.io/schedule)
- [S11] [Self-hosted defaults](https://docs.temporal.io/self-hosted-guide/defaults)
- [S12] [Workflow Execution limits](https://docs.temporal.io/workflow-execution/limits)
- [S13] [Temporal Cloud limits](https://docs.temporal.io/cloud/limits)
- [S14] [Deploying a Temporal Service](https://docs.temporal.io/self-hosted-guide/deployment)
- [S15] [Task Queue performance](https://docs.temporal.io/develop/worker-performance/task-queues)
- [S48] [temporalio/temporal (GitHub)](https://github.com/temporalio/temporal) — *GitHub HTML scraped*
- [S50] [Temporal changelog](https://temporal.io/changelog)
- [S52] [OSS deployment architecture](https://dagster.io/docs/deployment/oss/oss-deployment-architecture)
- [S53] [dagster.yaml reference](https://dagster.io/docs/deployment/oss/dagster-yaml)
- [S54] [Instance configuration](https://dagster.io/docs/deployment/oss/oss-instance-configuration)
- [S56] [Run executors](https://dagster.io/docs/guides/operate/run-executors)
- [S62] [Troubleshooting sensor timeouts](https://dagster.io/docs/deployment/troubleshooting/sensor-timeouts)
- [S63] [Managing concurrency](https://dagster.io/docs/guides/operate/managing-concurrency)
- [S86] [Running Dagster locally](https://dagster.io/docs/deployment/oss/deployment-options/running-dagster-locally)
- [S88] [Deploying to Kubernetes](https://dagster.io/docs/deployment/oss/deployment-options/kubernetes/deploying-to-kubernetes)
- [S94] [dagster-io/dagster releases](https://github.com/dagster-io/dagster/releases) — *GitHub HTML scraped*
- [S108] [Schedules and sensors API reference](https://dagster.io/docs/api/dagster/schedules-sensors)
- [S111] [Run queue limits](https://dagster.io/docs/guides/operate/managing-concurrency/run-queue-limits)
- [S112] [dagster 1.13.29 (PyPI)](https://pypi.org/project/dagster/1.13.29/) — *package source*

Full numbered list: [references/sources.md](../references/sources.md)
