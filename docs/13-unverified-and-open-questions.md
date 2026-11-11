# 13 — Unverified claims, conflicts and open questions

> **Key takeaways**
> - This page lists every claim the research notes flag as **unverified**, **conflicting**, **third-party** or **not fetched**.
> - None of these items is used as a fact elsewhere; where a doc mentions them, it says **Not verified**.
> - A verification pass on 2026-11-11 closed the Airbyte, Sling, sensor-interval, `max_concurrent_runs` and S3-helper gaps (see the [Verification log](#8-verification-log-2026-11-11)).
> - Biggest open items for ingestion: explicit official statement that External Storage works with self-hosted Temporal (and minimum versions); Dagster examples of Sling `incremental`/`change-capture` mode.
> - No official Temporal–Dagster comparison or integration exists; the only comparison found is an AI-generated forum answer ([How does Dagster compare to Flyte and Temporal (community forum)](https://discuss.dagster.io/t/22772686/how-does-dagster-compare-to-flyte-and-temporal)).

Status legend: **U** = unverified (missing from fetched pages), **C** = conflicting sources, **3P** = third-party only, **NF** = page found but not fetched, **I** = inference only.

## 1. Research-method caveats

| # | Item | Status |
|---|---|---|
| M1 | Page text was extracted with a summarisation fetch tool; verbatim quotes should be spot-checked | Caveat |
| M2 | GitHub API returned HTTP 403; stars, forks and releases are scraped from HTML; contributor counts unavailable for both repos | U |
| M3 | `docs.dagster.io/*` 302-redirects to `dagster.io/docs/*`; a few cited `docs.dagster.io` URLs were not fetched ([Alerts (Dagster+)](https://docs.dagster.io/guides/observe/alerts), [Insights (Dagster+)](https://docs.dagster.io/guides/observe/insights), [Ops API reference](https://docs.dagster.io/api/dagster/ops), [Dagster CLI API reference](https://docs.dagster.io/api/clis/cli)) | NF |

## 2. Conflicts between sources

| # | Conflict | Sources | How handled |
|---|---|---|---|
| C1 | Dagster latest version, re-checked 2026-11-11: **PyPI** `info.version` = 1.13.29, uploaded 2026-11-05 (1.13.28 on 2026-10-30); **GitHub Releases** marks 1.13.28 / 0.29.28 (30 Oct) "Latest", no 1.13.29 release entry; **docs version picker** shows "Latest (1.12.8)" | [PyPI dagster JSON](https://pypi.org/pypi/dagster/json), [dagster-io/dagster releases](https://github.com/dagster-io/dagster/releases), [OSS deployment architecture](https://dagster.io/docs/deployment/oss/oss-deployment-architecture) | Still conflicting. PyPI is the version of record (it is what `pip` installs). Why GitHub and docs lag is **not stated** anywhere |
| C2 | Dagster asset catalog: Observe page labels it Dagster+; webserver page describes an OSS catalog | [Observe](https://dagster.io/docs/guides/observe), [Dagster webserver and UI](https://dagster.io/docs/guides/operate/webserver) | Read as basic OSS catalog + enhanced Dagster+ catalog (**I**) |
| C3 | `docker_executor` described as using a Kubernetes pod on the executors page | [Run executors](https://dagster.io/docs/guides/operate/run-executors) | Likely docs error; `dagster-docker` page not checked |
| C4 | Dagster default compute log manager: one note says no fetched page names the default; another quotes "Used by default, the LocalComputeLogManager" | [dagster.yaml reference](https://dagster.io/docs/deployment/oss/dagster-yaml), [Instance configuration](https://dagster.io/docs/deployment/oss/oss-instance-configuration) | Resolved: Local is default per [Instance configuration](https://dagster.io/docs/deployment/oss/oss-instance-configuration) |
| C5 | `run_monitoring` marked unverified in one note; its defaults are quoted from the instance configuration page in another | [Instance configuration](https://dagster.io/docs/deployment/oss/oss-instance-configuration) | Resolved: treated as verified |
| C6 | Rapidflare post listed as "not fetched" in one note but summarised in detail in another | [How Rapidflare built a million+ document ingestion pipeline](https://temporal.io/blog/how-rapidflare-built-a-million-document-ingestion-pipeline-for-agents-on-temporal) | Used, labelled *vendor blog* |
| C7 | Temporal release dates on GitHub omit the year; 2026 is inferred | [temporalio/temporal releases](https://github.com/temporalio/temporal/releases) | Year shown as inferred |
| C8 | Temporal Signal cap: Cloud states 10,000 per execution; self-hosted defaults list only 2,000 *pending* Signals and no total cap | [Temporal Cloud limits](https://docs.temporal.io/cloud/limits), [Self-hosted defaults](https://docs.temporal.io/self-hosted-guide/defaults) | Self-hosted total cap **U** |

## 3. Temporal — unverified or not fetched

| # | Item | Status |
|---|---|---|
| T1 | OpenSearch/Elasticsearch version support matrix for visibility | U |
| T2 | Scaling characteristics of the internal Worker service | U |
| T3 | Per-SDK sandbox rules (threads, I/O) for Workflow code | U |
| T4 | Workflow Execution/Run/Task timeout defaults, including the widely cited 10 s Workflow Task timeout | U |
| T5 | Semantics of Signals, Queries, Updates and Continue-As-New (only limits verified) | NF |
| T6 | Numeric limits for `MaxCallbacksPerWorkflow` and memo size | U |
| T7 | External Storage on self-hosted: it is SDK-side (Data Converter; clients/Workers move the data, history keeps a reference) and no page says Cloud-only; the changelog tags Cloud, Server, Go SDK, Python SDK. But no page **explicitly** says self-hosted is supported, and no minimum server/SDK version is stated ([External Storage](https://docs.temporal.io/external-storage), [External Storage - Python SDK](https://docs.temporal.io/develop/python/data-handling/external-storage), [External Storage - Go SDK](https://docs.temporal.io/develop/go/data-handling/external-storage), [External Storage Public Preview (changelog entry)](https://temporal.io/changelog/external-storage-public-preview)) | U (narrowed) |
| T8 | Official per-SDK maturity table; Ruby SDK GA status; current SDK version numbers | U |
| T9 | Ruby SDK Public Preview (May 1, 2025) — search-result title only ([Ruby SDK Public Preview (changelog entry)](https://temporal.io/change-log/ruby-sdk-public-preview)) | NF |
| T10 | SDK licences other than Python (Python `temporalio` 1.36.0 is MIT per PyPI ([temporalio 1.36.0 (PyPI)](https://pypi.org/project/temporalio/1.36.0/))) | U (partial) |
| T11 | Whether the Helm chart is "production-ready" (no statement either way) | U |
| T12 | Task queue partition default count and `matching.numTaskqueueReadPartitions` / `matching.numTaskqueueWritePartitions` keys (forum-only) | U / 3P |
| T13 | Temporal Server contributor count | U |
| T14 | Any official ETL/batch docs page (only design patterns and blogs found) | U |
| T15 | Official data-source connectors — none found; conclusion is from absence, not an official statement | I |
| T16 | Exact names of `temporalio/samples-*` batch samples (only an unofficial mirror found) | U / 3P |
| T17 | Official "polling external APIs" guidance | NF |
| T18 | Dedicated Continue-As-New docs page | NF |
| T19 | Batch Iterator and Sliding Window dedicated pages ([Batch Iterator](https://docs.temporal.io/design-patterns/batch-iterator), [Sliding Window](https://docs.temporal.io/design-patterns/sliding-window)) | NF |
| T20 | Rate-limit guide with separate Task Queues ([Rate-limit downstream APIs with separate Task Queues](https://docs.temporal.io/guides/rate-limit-downstream-apis)) | NF |
| T21 | Any official CDC statement; Workflow Streams content ([Workflow Streams](https://docs.temporal.io/workflow-streams), [Workflow Streams: live interactivity for agents](https://temporal.io/blog/workflow-streams-live-interactivity-agents-other-applications)) | U / NF |
| T22 | Server-side metrics reference and Prometheus config ([Temporal Service configuration reference](https://docs.temporal.io/references/configuration)) | NF |
| T23 | Whether `workflow.logger` suppresses duplicate logs during replay | U |
| T24 | Limits on number of custom Search Attributes ([Search Attributes](https://docs.temporal.io/search-attribute)) | U / NF |
| T25 | GA vs preview status of Worker Versioning in self-hosted OSS; Build-ID vs Worker Deployments model | U |
| T26 | Batch operations in the Web UI | U |
| T27 | Activity timeouts blog ([Understanding the 4 types of Activity timeouts](https://temporal.io/blog/activity-timeouts)) | NF |
| T28 | Airbyte case study on temporal.io ([Airbyte case study](https://web.temporal.io/case-studies/airbyte-case-study)) | NF |
| T29 | Other SDK observability and testing guides ([Observability - Go SDK](https://docs.temporal.io/develop/go/observability), [Observability - Java SDK](https://docs.temporal.io/develop/java/observability), [Observability - TypeScript SDK](https://docs.temporal.io/develop/typescript/platform/observability), [Observability - .NET SDK](https://docs.temporal.io/develop/dotnet/platform/observability), [Observability - Ruby SDK](https://docs.temporal.io/develop/ruby/platform/observability), [Testing - TypeScript SDK](https://docs.temporal.io/develop/typescript/testing-suite), [Pre-production testing best practices](https://docs.temporal.io/best-practices/pre-production-testing), [Run a development server](https://docs.temporal.io/develop/run-a-development-server)) | NF |
| T30 | Codec and encryption deep-dive pages ([Codecs and Encryption](https://docs.temporal.io/production-deployment/data-encryption), [Codec Server](https://docs.temporal.io/codec-server)) | NF |
| T31 | "At-least-once" is not the literal wording of the Activity Definition page (it says Activities "may be executed multiple times"); the phrase appears in a guest post ([The foundation: Why Temporal for a data pipeline?](https://temporal.io/blog/the-foundation-why-temporal-for-a-data-pipeline)) | I / vendor blog |
| T32 | Temporal does not provide row-level auditing — guest post only ([The foundation: Why Temporal for a data pipeline?](https://temporal.io/blog/the-foundation-why-temporal-for-a-data-pipeline)) | Vendor blog |
| T34 | Temporal Code Exchange not searched for a Dagster bridge | U |

## 4. Dagster — unverified or not fetched

| # | Item | Status |
|---|---|---|
| D1 | Claim that the Helm chart enables `QueuedRunCoordinator` (OSS default `DefaultRunCoordinator` is confirmed by [Instance configuration](https://dagster.io/docs/deployment/oss/oss-instance-configuration) and [Run coordinators](https://dagster.io/docs/deployment/execution/run-coordinators)) | U (partial) |
| D2 | Resources (`ConfigurableResource`), IO managers, `@asset_check` decorator, `FreshnessPolicy` (reworked in 1.11) | U |
| D3 | Components and `dg` reached GA with Dagster 1.11 in mid-2025 | U |
| D4 | `AutomationCondition` replaced `AutoMaterializePolicy` | U |
| D5 | Static and multi partition definitions from primary docs (`DynamicPartitionsDefinition` and its `SensorResult` example are now confirmed in [Partitions API reference](https://dagster.io/docs/api/dagster/partitions)) | U (partial) |
| D7 | Run status sensors and multi-asset sensors | NF |
| D8 | "Re-execute from failure" UI page | NF |
| D10 | Default schedule timeout; `DAGSTER_GRPC_TIMEOUT_SECONDS` and `DAGSTER_SCHEDULE_GRPC_TIMEOUT_SECONDS` (default `max_concurrent_runs` = 10 now verified from package source) | U |
| D11 | Pipes clients for Rust, Java, EMR, Lambda, ECS, Glue; Pipes on Snowflake | U |
| D12 | Full OSS vs Dagster+ feature matrix (SSO, catalog, insights, cost tracking) — partly confirmed by [Observe](https://dagster.io/docs/guides/observe) and [GitHub Discussion #23851 (maintainer answer)](https://github.com/dagster-io/dagster/discussions/23851) | U (partial) |
| D13 | Contributor count (~500+ commonly cited) | U |
| D14 | Exact close date of the Prefect acquisition | U |
| D16 | A Dagster-published example of Sling `incremental` or `change-capture` mode (Sling's own docs define the modes; Dagster pages show only `full-refresh`) ([Sling replication modes](https://docs.slingdata.io/concepts/replication/modes), [Sling (Pythonic)](https://dagster.io/docs/integrations/libraries/sling/sling-pythonic)) | U (narrowed) |
| D17 | Any numeric integration maturity metric (only "Dagster supported" vs "Community supported" tags) | U |
| D18 | Official statement that partitioned writes must be idempotent | U |
| D19 | S3 helper `dagster_aws.s3.sensor.get_objects` (and deprecated `get_s3_keys`) exists in package source but is **not in the API reference**; support status unclear ([dagster-aws 0.29.29 (PyPI)](https://pypi.org/project/dagster-aws/0.29.29/), [dagster-aws API reference](https://dagster.io/docs/api/libraries/dagster-aws)) | U (exists, undocumented) |
| D20 | Official delivery semantics for asset materializations | U |
| D21 | Dagster customer case study on ingestion | U |
| D22 | Official "Dagster is not a streaming engine" statement (forum blocked by robots.txt) | U |
| D23 | Launchpad, run timeline/Gantt view, OSS asset health in the UI | U |
| D24 | Prometheus metrics endpoint or native OpenTelemetry tracing in OSS | U |
| D25 | Run-failure sensors as an OSS alerting mechanism | U / I |
| D26 | `build_op_context()` examples ([Ops API reference](https://docs.dagster.io/api/dagster/ops)); `materialize()` now confirmed in [Execution API reference](https://docs.dagster.io/api/dagster/execution) | NF |
| D27 | Asset check severity levels | U |
| D28 | Legacy `dagster dev` command, default port 3000, `dg` vs `dagster` CLI relationship ([Dagster CLI API reference](https://docs.dagster.io/api/clis/cli)) | U / NF |
| D29 | Branch deployments as Dagster+ only (only branch-deployment *concurrency limits* are confirmed Dagster+) | U |
| D30 | Whether feature request #2219 (OSS auth/RBAC) is still open | U |
| D31 | Official guidance for securing the webserver behind a proxy | U |
| D32 | Dagster+ RBAC docs page ([Dagster+ RBAC user roles and permissions (docs source)](https://github.com/dagster-io/dagster/blob/master/docs/docs/deployment/dagster-plus/authentication-and-access-control/rbac/user-roles-permissions.md)) | NF |
| D33 | Dagster+ Alerts and Insights pages ([Alerts (Dagster+)](https://docs.dagster.io/guides/observe/alerts), [Insights (Dagster+)](https://docs.dagster.io/guides/observe/insights)) | NF |
| D34 | Dagster vs Airflow comparison pages in site navigation | NF |
| D36 | Secrets management for source credentials (either tool) | U |

## 5. Third-party items

| # | Item | Source | Use in this repo |
|---|---|---|---|
| P1 | AI-generated answer comparing Dagster, Flyte and Temporal | [How does Dagster compare to Flyte and Temporal (community forum)](https://discuss.dagster.io/t/22772686/how-does-dagster-compare-to-flyte-and-temporal) | Mentioned only to show no official comparison exists |
| P2 | Community auth/RBAC add-ons for Dagster OSS | [maltzsama/dagster-authkit](https://github.com/maltzsama/dagster-authkit), [stianthaulow/dagster-auth](https://github.com/stianthaulow/dagster-auth) | Mentioned; not evaluated |
| P3 | Press coverage of the Prefect–Dagster deal | [Prefect expands open source AI stack with Dagster acquisition](https://opensourceforu.com/2026/07/prefect-expands-open-source-ai-stack-with-dagster-acquisition/), [Prefect to acquire Dagster Labs](https://techstrong.ai/features/prefect-to-acquire-dagster-labs-to-drive-ai-workflow-automation/) | Not fetched; not used for facts |
| P4 | Search results for "Dagster Temporal integration" returned only third-party Dagster triggers (Kestra, Paradime, Hightouch, Airbyte) | — (no URLs recorded) | Supports "no official integration" |
| P5 | Community forum threads on Temporal task-queue partitions | — (no URLs recorded) | Not used |

## 6. Vendor-blog claims used (labelled in the docs)

| Claim | Source |
|---|---|
| Model each pipeline step as an Activity; heartbeats for resumable streaming ingestion; pass IDs, keep data in Postgres/Redis | [The foundation: Why Temporal for a data pipeline?](https://temporal.io/blog/the-foundation-why-temporal-for-a-data-pipeline) |
| 1M-document pipeline: child per document, sliding window, Continue-As-New, `ParentClosePolicy.ABANDON`, `ScheduleOverlapPolicy.SKIP`, 20+ source types | [How Rapidflare built a million+ document ingestion pipeline](https://temporal.io/blog/how-rapidflare-built-a-million-document-ingestion-pipeline-for-agents-on-temporal) |
| Dynamic partitions file-sensor pattern | [Dynamic partitioning](https://dagster.io/blog/dynamic-partitioning) |
| Dagster positioning vs "orchestrators and workflow engines" | [Why We Built Dagster](https://dagster.io/blog/dagster-the-data-orchestrator) |
| Prefect acquisition statements | [Prefect is acquiring Dagster](https://dagster.io/blog/prefect-is-acquiring-dagster), [Dagster Customer FAQ](https://dagster.io/dagster-customer-faq) |

## 7. Open questions worth a follow-up spike

1. Will Temporal state explicitly that External Storage works with a self-hosted server, and from which server/SDK versions? A test on a self-hosted dev server would settle it. (T7)
2. Does Sling `incremental` / `change-capture` work unchanged through the Dagster Sling component? A small spike would settle it. (D16)
3. What is the 2027 roadmap for Dagster OSS under Prefect? (D14)
4. Is there a maintained community bridge between Temporal and Dagster? (T34)

## 8. Verification log (2026-11-11)

Each row: claim → page opened → result. "Confirmed" means the page matches the repo text; "Corrected" means the repo text was changed.

**Gaps closed**

| Claim | URL | Result |
|---|---|---|
| Airbyte component triggers or observes | [dagster-airbyte library reference](https://dagster.io/docs/integrations/libraries/airbyte/dagster-airbyte), [Airbyte component](https://dagster.io/docs/integrations/libraries/airbyte/airbyte-component) | Confirmed: "Materializing these assets will trigger a sync"; `AirbyteCloudWorkspace` and `AirbyteWorkspace` (OSS) both launch and monitor syncs; no observe-only option documented |
| Sling incremental modes | [Sling replication modes](https://docs.slingdata.io/concepts/replication/modes), [Sling (Pythonic)](https://dagster.io/docs/integrations/libraries/sling/sling-pythonic) | Confirmed: `full-refresh` (default), `incremental`, `truncate`, `snapshot`, `backfill`, `definition-only`, `change-capture`; Dagster lists incremental support but shows only `full-refresh` |
| External Storage on self-hosted | [External Storage](https://docs.temporal.io/external-storage), [External Storage - Python SDK](https://docs.temporal.io/develop/python/data-handling/external-storage), [External Storage - Go SDK](https://docs.temporal.io/develop/go/data-handling/external-storage), [changelog entry](https://temporal.io/changelog/external-storage-public-preview) | Partly: SDK-side, not stated as Cloud-only, no version requirements; explicit self-hosted support not stated (T7 kept) |
| Default sensor interval | [Schedules and sensors API reference](https://dagster.io/docs/api/dagster/schedules-sensors), [Dagster daemon](https://dagster.io/docs/deployment/execution/dagster-daemon), [dagster 1.13.29 (PyPI)](https://pypi.org/project/dagster/1.13.29/) | Confirmed from source only: `DEFAULT_SENSOR_DAEMON_INTERVAL = 30`; docs give no default |
| Default `max_concurrent_runs` | [Run queue limits](https://dagster.io/docs/guides/operate/managing-concurrency/run-queue-limits), [dagster.yaml reference](https://dagster.io/docs/deployment/oss/dagster-yaml), [dagster 1.13.29 (PyPI)](https://pypi.org/project/dagster/1.13.29/) | Confirmed from source only: "Defaults to 10. Set to -1 to disable the limit"; docs show examples only |
| S3 sensor helper | [dagster-aws API reference](https://dagster.io/docs/api/libraries/dagster-aws), [dagster-aws 0.29.29 (PyPI)](https://pypi.org/project/dagster-aws/0.29.29/) | Exists in source (`get_objects`; `get_s3_keys` deprecated); not in API reference |
| Dagster latest version | [PyPI dagster JSON](https://pypi.org/pypi/dagster/json), [dagster-io/dagster releases](https://github.com/dagster-io/dagster/releases), [OSS deployment architecture](https://dagster.io/docs/deployment/oss/oss-deployment-architecture) | Re-checked: PyPI 1.13.29 (2026-11-05); GitHub "Latest" 1.13.28 (30 Oct); docs picker 1.12.8 (C1) |
| Code-sketch API names | [Activity Timeouts - Python SDK](https://docs.temporal.io/develop/python/activities/timeouts), [Long-Running Activity pattern](https://docs.temporal.io/design-patterns/long-running-activity), [Schedules and sensors API reference](https://dagster.io/docs/api/dagster/schedules-sensors), [Partitions API reference](https://dagster.io/docs/api/dagster/partitions), [Assets API reference](https://dagster.io/docs/api/dagster/assets), [Execution API reference](https://docs.dagster.io/api/dagster/execution), [temporalio 1.36.0 (PyPI)](https://pypi.org/project/temporalio/1.36.0/) | Confirmed; placeholders in doc 06 replaced |

**Citation spot-checks**

| Claim | URL | Result |
|---|---|---|
| Blob 512 KB/2 MB, gRPC 4 MB, transaction 4 MB, history 10,240/51,200 events and 10/50 MB | [Self-hosted defaults](https://docs.temporal.io/self-hosted-guide/defaults) | Confirmed |
| 2,000 pending ops, ≤500 recommended; Updates 10 in-flight/2,000 total; IDs 1,000 chars (SQL sample 255); 1 batch job | [Self-hosted defaults](https://docs.temporal.io/self-hosted-guide/defaults) | Confirmed |
| On pending-limit hit "the Workflow Task fails and is retried" cited to Self-hosted defaults | [Self-hosted defaults](https://docs.temporal.io/self-hosted-guide/defaults), [Workflow Execution limits](https://docs.temporal.io/workflow-execution/limits) | Corrected: defaults page says Commands fail; Task fail/retry is on the limits page |
| No concurrent-execution limit; 30 incomplete Nexus ops | [Workflow Execution limits](https://docs.temporal.io/workflow-execution/limits) | Confirmed |
| Cloud: 2 MB/4 MB/4 MB, 500 APS, 10 schedule req/s, 30 Visibility calls/s, retention 30 d (1–90), 10,000 Signals | [Temporal Cloud limits](https://docs.temporal.io/cloud/limits) | Confirmed |
| Activity retry defaults 1 s, 2.0, 100 s, unlimited; no Workflow retry by default; Workflow Task backoff up to 10 min | [Retry Policies](https://docs.temporal.io/encyclopedia/retry-policies) | Confirmed |
| Workflow retries "generally discouraged" | [Retry Policies](https://docs.temporal.io/encyclopedia/retry-policies) | Corrected (doc 03): page says "not recommended" |
| Overlap policies (default Skip), catch-up 1 year/min 10 s, 2 MiB last result, backfill AllowAll/BufferAll | [Schedule](https://docs.temporal.io/schedule) | Confirmed |
| History Shards "range 1–128K" | [Temporal Server](https://docs.temporal.io/temporal-service/temporal-server) | Corrected (docs 09, matrix): 1–128K is the range "operated successfully", not a hard limit; count cannot change |
| ≥2 Workers per queue, poll success >90%/>95%, setting names | [Task Queue performance](https://docs.temporal.io/develop/worker-performance/task-queues) | Confirmed |
| Queue-wide rate limit, 2 Workers × 5 = 5, last poller wins | [Downstream Rate Limiting](https://docs.temporal.io/design-patterns/downstream-rate-limiting) | Confirmed |
| Activity timeout defaults; heartbeat throttle 0.8×, 30 s, 60 s | [Detecting Activity failures](https://docs.temporal.io/encyclopedia/detecting-activity-failures.md) | Confirmed |
| Heartbeat details persisted for retries; timeout 2–3× interval | [Long-Running Activity pattern](https://docs.temporal.io/design-patterns/long-running-activity) | Confirmed |
| "may be executed multiple times…"; writes idempotent; Run ID + Activity ID key | [Activity Definition](https://docs.temporal.io/activity-definition) | Confirmed |
| Batch Iterator / Sliding Window quotes; "51,200 events max (aim for a few thousand)" | [Batch Processing Patterns](https://docs.temporal.io/design-patterns/batch-processing-patterns) | Confirmed |
| 2 MB default, configurable self-hosted; claim check; codec only delays | [Troubleshoot payload size limit](https://docs.temporal.io/troubleshooting/blob-size-limit-error) | Confirmed |
| External Storage Public Preview, 256 KiB threshold, S3 (Go/Java/Python/TS), GCS (Go/TS) | [External Storage](https://docs.temporal.io/external-storage) | Confirmed |
| Deployment options incl. systemd, Compose ports 7233/8080, chart 0.73.1+ for 1.30+, not public-internet | [Deploying a Temporal Service](https://docs.temporal.io/self-hosted-guide/deployment) | Confirmed |
| Temporal MIT, 23.7k stars/2.0k forks, Cadence fork, `start-dev` UI 8233 | [temporalio/temporal (GitHub)](https://github.com/temporalio/temporal) | Confirmed |
| v1.32.1 "Latest" (08 Oct); v1.31.4, v1.30.8 (12 Oct) | [temporalio/temporal releases](https://github.com/temporalio/temporal/releases) | Confirmed |
| Changelog dates (Rust SDK 09-04, Standalone Activities 09-15, Priority & Fairness 05-05, External Storage 05-14, Worker Controller 05-04) | [Temporal changelog](https://temporal.io/changelog) | Confirmed |
| Dagster Apache-2.0, 16.1k stars/2.3k forks | [dagster-io/dagster (GitHub)](https://github.com/dagster-io/dagster) | Confirmed |
| Prefect acquisition quotes | [Prefect is acquiring Dagster](https://dagster.io/blog/prefect-is-acquiring-dagster), [Dagster Customer FAQ](https://dagster.io/dagster-customer-faq) | Confirmed, but two quotes are from the FAQ, not the blog; attribution clarified in doc 11; close date still not given ("in the coming weeks") |
| Sensor timeout 60 s, `DAGSTER_SENSOR_GRPC_TIMEOUT_SECONDS`, >3 min guideline | [Troubleshooting sensor timeouts](https://dagster.io/docs/deployment/troubleshooting/sensor-timeouts) | Confirmed |
| Daemon replicas not supported; 1 replica per code location | [OSS deployment architecture](https://dagster.io/docs/deployment/oss/oss-deployment-architecture) | Confirmed |
| Pools `default_limit: 3`, `max_concurrent_runs: 10` example | [Managing concurrency](https://dagster.io/docs/guides/operate/managing-concurrency) | Confirmed |
| Not strict FIFO with pools | [Run coordinators](https://dagster.io/docs/deployment/execution/run-coordinators) | Clarified (doc 06): page states it for Dagster+ |
| Helm chart deploys webserver on 8080 | [Deploying to Kubernetes](https://dagster.io/docs/deployment/oss/deployment-options/kubernetes/deploying-to-kubernetes) | Corrected (docs 02, 09): 8080 is the local port-forward to pod port 80 |
| Run retries: `FROM_FAILURE` default, `retry_on_asset_or_op_failure` 1.6.7, on by default in Dagster+ | [Run retries](https://dagster.io/docs/deployment/execution/run-retries) | Confirmed |
| `RetryPolicy` params; "every exception triggers a retry" | [Op retries](https://dagster.io/docs/guides/build/ops/op-retries) | Confirmed |
| run_monitoring 180/180/3/120/300 | [Instance configuration](https://dagster.io/docs/deployment/oss/oss-instance-configuration) | Confirmed |
| Sensors: run-key dedupe quote, "not the exact frequency" | [Sensors](https://dagster.io/docs/guides/automate/sensors) | Confirmed |
| dlt: `defer_table_reflect`, ~1 min refresh, concurrency limits | [dlt integration (Pythonic)](https://dagster.io/docs/integrations/libraries/dlt/dlt-pythonic) | Confirmed; limits are "recommended" (wording softened in doc 06) |
| Backfill: one run per partition default; `multi_run`, `single_run` | [Partition backfill strategies](https://dagster.io/docs/examples/best-practices/partition-backfill-strategies) | Confirmed |
| `dagster-embedded-elt` deprecated → `dagster-sling`, `dagster-dlt` | [Embedded ELT (deprecated)](https://dagster.io/docs/integrations/libraries/embedded-elt) | Confirmed |
| Fivetran component keys and default "manual" schedule | [Fivetran integration](https://dagster.io/docs/integrations/libraries/fivetran) | Confirmed; YAML sketch fixed to real key placement |

Method notes: GitHub source pages were rate-limited and `curl` to dagster.io was blocked, so Dagster defaults were read from the official PyPI wheels (`dagster` 1.13.29, `dagster-aws` 0.29.29). Package source can change between releases; re-check after upgrades.

## Sources

- [S2] [Temporal Server](https://docs.temporal.io/temporal-service/temporal-server)
- [S6] [Activity Definition](https://docs.temporal.io/activity-definition)
- [S7] [Retry Policies](https://docs.temporal.io/encyclopedia/retry-policies)
- [S8] [Detecting Activity failures](https://docs.temporal.io/encyclopedia/detecting-activity-failures.md)
- [S9] [Understanding the 4 types of Activity timeouts](https://temporal.io/blog/activity-timeouts) — *vendor blog, not fetched*
- [S10] [Schedule](https://docs.temporal.io/schedule)
- [S11] [Self-hosted defaults](https://docs.temporal.io/self-hosted-guide/defaults)
- [S12] [Workflow Execution limits](https://docs.temporal.io/workflow-execution/limits)
- [S13] [Temporal Cloud limits](https://docs.temporal.io/cloud/limits)
- [S14] [Deploying a Temporal Service](https://docs.temporal.io/self-hosted-guide/deployment)
- [S15] [Task Queue performance](https://docs.temporal.io/develop/worker-performance/task-queues)
- [S16] [Temporal Service configuration reference](https://docs.temporal.io/references/configuration) — *not fetched*
- [S17] [Troubleshoot payload size limit](https://docs.temporal.io/troubleshooting/blob-size-limit-error)
- [S18] [External Storage](https://docs.temporal.io/external-storage)
- [S19] [External Storage Public Preview (changelog entry)](https://temporal.io/changelog/external-storage-public-preview)
- [S20] [Long-Running Activity pattern](https://docs.temporal.io/design-patterns/long-running-activity)
- [S21] [Batch Processing Patterns](https://docs.temporal.io/design-patterns/batch-processing-patterns)
- [S22] [Batch Iterator](https://docs.temporal.io/design-patterns/batch-iterator) — *not fetched*
- [S23] [Sliding Window](https://docs.temporal.io/design-patterns/sliding-window) — *not fetched*
- [S24] [Downstream Rate Limiting](https://docs.temporal.io/design-patterns/downstream-rate-limiting)
- [S25] [Rate-limit downstream APIs with separate Task Queues](https://docs.temporal.io/guides/rate-limit-downstream-apis) — *not fetched*
- [S26] [Workflow Streams](https://docs.temporal.io/workflow-streams) — *not fetched*
- [S27] [The foundation: Why Temporal for a data pipeline?](https://temporal.io/blog/the-foundation-why-temporal-for-a-data-pipeline) — *vendor blog, guest post*
- [S28] [How Rapidflare built a million+ document ingestion pipeline](https://temporal.io/blog/how-rapidflare-built-a-million-document-ingestion-pipeline-for-agents-on-temporal) — *vendor blog, customer guest post*
- [S29] [Airbyte case study](https://web.temporal.io/case-studies/airbyte-case-study) — *vendor case study, not fetched*
- [S30] [Workflow Streams: live interactivity for agents](https://temporal.io/blog/workflow-streams-live-interactivity-agents-other-applications) — *vendor blog, not fetched*
- [S33] [Observability - Go SDK](https://docs.temporal.io/develop/go/observability) — *not fetched*
- [S34] [Observability - Java SDK](https://docs.temporal.io/develop/java/observability) — *not fetched*
- [S35] [Observability - TypeScript SDK](https://docs.temporal.io/develop/typescript/platform/observability) — *not fetched*
- [S36] [Observability - .NET SDK](https://docs.temporal.io/develop/dotnet/platform/observability) — *not fetched*
- [S37] [Observability - Ruby SDK](https://docs.temporal.io/develop/ruby/platform/observability) — *not fetched*
- [S38] [Search Attributes](https://docs.temporal.io/search-attribute) — *not fetched*
- [S40] [Run a development server](https://docs.temporal.io/develop/run-a-development-server) — *not fetched*
- [S42] [Testing - TypeScript SDK](https://docs.temporal.io/develop/typescript/testing-suite) — *not fetched*
- [S43] [Pre-production testing best practices](https://docs.temporal.io/best-practices/pre-production-testing) — *not fetched*
- [S46] [Codecs and Encryption](https://docs.temporal.io/production-deployment/data-encryption) — *not fetched*
- [S47] [Codec Server](https://docs.temporal.io/codec-server) — *not fetched*
- [S48] [temporalio/temporal (GitHub)](https://github.com/temporalio/temporal) — *GitHub HTML scraped*
- [S49] [temporalio/temporal releases](https://github.com/temporalio/temporal/releases) — *GitHub HTML scraped*
- [S50] [Temporal changelog](https://temporal.io/changelog)
- [S51] [Ruby SDK Public Preview (changelog entry)](https://temporal.io/change-log/ruby-sdk-public-preview) — *search-result title only, not fetched*
- [S52] [OSS deployment architecture](https://dagster.io/docs/deployment/oss/oss-deployment-architecture)
- [S53] [dagster.yaml reference](https://dagster.io/docs/deployment/oss/dagster-yaml)
- [S54] [Instance configuration](https://dagster.io/docs/deployment/oss/oss-instance-configuration)
- [S55] [Run coordinators](https://dagster.io/docs/deployment/execution/run-coordinators)
- [S56] [Run executors](https://dagster.io/docs/guides/operate/run-executors)
- [S58] [Op retries](https://dagster.io/docs/guides/build/ops/op-retries)
- [S59] [Run retries](https://dagster.io/docs/deployment/execution/run-retries)
- [S61] [Sensors](https://dagster.io/docs/guides/automate/sensors)
- [S62] [Troubleshooting sensor timeouts](https://dagster.io/docs/deployment/troubleshooting/sensor-timeouts)
- [S63] [Managing concurrency](https://dagster.io/docs/guides/operate/managing-concurrency)
- [S65] [Partition backfill strategies](https://dagster.io/docs/examples/best-practices/partition-backfill-strategies)
- [S70] [dlt integration (Pythonic)](https://dagster.io/docs/integrations/libraries/dlt/dlt-pythonic)
- [S72] [Embedded ELT (deprecated)](https://dagster.io/docs/integrations/libraries/embedded-elt)
- [S74] [Airbyte component](https://dagster.io/docs/integrations/libraries/airbyte/airbyte-component)
- [S75] [Fivetran integration](https://dagster.io/docs/integrations/libraries/fivetran)
- [S78] [Dagster webserver and UI](https://dagster.io/docs/guides/operate/webserver)
- [S79] [Observe](https://dagster.io/docs/guides/observe)
- [S80] [Alerts (Dagster+)](https://docs.dagster.io/guides/observe/alerts) — *not fetched*
- [S81] [Insights (Dagster+)](https://docs.dagster.io/guides/observe/insights) — *not fetched*
- [S84] [Execution API reference](https://docs.dagster.io/api/dagster/execution)
- [S85] [Ops API reference](https://docs.dagster.io/api/dagster/ops) — *not fetched*
- [S87] [Dagster CLI API reference](https://docs.dagster.io/api/clis/cli) — *not fetched*
- [S88] [Deploying to Kubernetes](https://dagster.io/docs/deployment/oss/deployment-options/kubernetes/deploying-to-kubernetes)
- [S89] [GitHub Discussion #23851 (maintainer answer)](https://github.com/dagster-io/dagster/discussions/23851)
- [S90] [Dagster+ RBAC user roles and permissions (docs source)](https://github.com/dagster-io/dagster/blob/master/docs/docs/deployment/dagster-plus/authentication-and-access-control/rbac/user-roles-permissions.md) — *not fetched*
- [S91] [maltzsama/dagster-authkit](https://github.com/maltzsama/dagster-authkit) — *third-party, not evaluated*
- [S92] [stianthaulow/dagster-auth](https://github.com/stianthaulow/dagster-auth) — *third-party, not evaluated*
- [S93] [dagster-io/dagster (GitHub)](https://github.com/dagster-io/dagster) — *GitHub HTML scraped*
- [S94] [dagster-io/dagster releases](https://github.com/dagster-io/dagster/releases) — *GitHub HTML scraped*
- [S95] [PyPI dagster JSON](https://pypi.org/pypi/dagster/json)
- [S96] [Prefect is acquiring Dagster](https://dagster.io/blog/prefect-is-acquiring-dagster) — *vendor blog*
- [S97] [Dagster Customer FAQ](https://dagster.io/dagster-customer-faq) — *vendor FAQ*
- [S98] [Prefect expands open source AI stack with Dagster acquisition](https://opensourceforu.com/2026/07/prefect-expands-open-source-ai-stack-with-dagster-acquisition/) — *third-party press, not fetched*
- [S99] [Prefect to acquire Dagster Labs](https://techstrong.ai/features/prefect-to-acquire-dagster-labs-to-drive-ai-workflow-automation/) — *third-party press, not fetched*
- [S100] [Why We Built Dagster](https://dagster.io/blog/dagster-the-data-orchestrator) — *vendor blog*
- [S101] [Dynamic partitioning](https://dagster.io/blog/dynamic-partitioning) — *vendor blog*
- [S102] [How does Dagster compare to Flyte and Temporal (community forum)](https://discuss.dagster.io/t/22772686/how-does-dagster-compare-to-flyte-and-temporal) — *third-party, AI-generated answer, not authoritative*
- [S103] [dagster-airbyte library reference](https://dagster.io/docs/integrations/libraries/airbyte/dagster-airbyte)
- [S104] [Sling (Pythonic)](https://dagster.io/docs/integrations/libraries/sling/sling-pythonic)
- [S105] [Sling replication modes](https://docs.slingdata.io/concepts/replication/modes) — *Sling's official docs*
- [S106] [External Storage - Python SDK](https://docs.temporal.io/develop/python/data-handling/external-storage)
- [S107] [External Storage - Go SDK](https://docs.temporal.io/develop/go/data-handling/external-storage)
- [S108] [Schedules and sensors API reference](https://dagster.io/docs/api/dagster/schedules-sensors)
- [S109] [Partitions API reference](https://dagster.io/docs/api/dagster/partitions)
- [S110] [Assets API reference](https://dagster.io/docs/api/dagster/assets)
- [S111] [Run queue limits](https://dagster.io/docs/guides/operate/managing-concurrency/run-queue-limits)
- [S112] [dagster 1.13.29 (PyPI)](https://pypi.org/project/dagster/1.13.29/) — *package source*
- [S113] [dagster-aws 0.29.29 (PyPI)](https://pypi.org/project/dagster-aws/0.29.29/) — *package source*
- [S114] [dagster-aws API reference](https://dagster.io/docs/api/libraries/dagster-aws)
- [S115] [Activity Timeouts - Python SDK](https://docs.temporal.io/develop/python/activities/timeouts)
- [S116] [temporalio 1.36.0 (PyPI)](https://pypi.org/project/temporalio/1.36.0/) — *package source*
- [S117] [Dagster daemon](https://dagster.io/docs/deployment/execution/dagster-daemon)

Full numbered list: [references/sources.md](../references/sources.md)
