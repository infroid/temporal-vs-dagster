# 13 — Unverified claims, conflicts and open questions

> **Key takeaways**
> - This page lists every claim the research notes flag as **unverified**, **conflicting**, **third-party** or **not fetched**.
> - None of these items is used as a fact elsewhere; where a doc mentions them, it says **Not verified**.
> - Biggest open items for ingestion: Sling incremental modes, Airbyte trigger-vs-observe, External Storage on self-hosted Temporal, Dagster defaults (sensor interval, `max_concurrent_runs`).
> - No official Temporal–Dagster comparison or integration exists; the only comparison found is an AI-generated forum answer ([How does Dagster compare to Flyte and Temporal (community forum)](https://discuss.dagster.io/t/22772686/how-does-dagster-compare-to-flyte-and-temporal)).

Status legend: **U** = unverified (missing from fetched pages), **C** = conflicting sources, **3P** = third-party only, **NF** = page found but not fetched, **I** = inference only.

## 1. Research-method caveats

| # | Item | Status |
|---|---|---|
| M1 | Page text was extracted with a summarisation fetch tool; verbatim quotes should be spot-checked | Caveat |
| M2 | GitHub API returned HTTP 403; stars, forks and releases are scraped from HTML; contributor counts unavailable for both repos | U |
| M3 | `docs.dagster.io/*` 302-redirects to `dagster.io/docs/*`; a few cited `docs.dagster.io` URLs were not fetched ([Alerts (Dagster+)](https://docs.dagster.io/guides/observe/alerts), [Insights (Dagster+)](https://docs.dagster.io/guides/observe/insights), [Execution API reference](https://docs.dagster.io/api/dagster/execution), [Ops API reference](https://docs.dagster.io/api/dagster/ops), [Dagster CLI API reference](https://docs.dagster.io/api/clis/cli)) | NF |

## 2. Conflicts between sources

| # | Conflict | Sources | How handled |
|---|---|---|---|
| C1 | Dagster latest version: PyPI 1.13.29 vs GitHub "Latest" 1.13.28 vs docs version picker "Latest (1.12.8)" | [PyPI dagster JSON](https://pypi.org/pypi/dagster/json), [dagster-io/dagster releases](https://github.com/dagster-io/dagster/releases), [OSS deployment architecture](https://dagster.io/docs/deployment/oss/oss-deployment-architecture) | PyPI used as the version of record; GitHub page likely lagged; docs picker likely stale |
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
| T7 | Whether External Storage works with self-hosted servers | U ([External Storage Public Preview (changelog entry)](https://temporal.io/changelog/external-storage-public-preview) NF) |
| T8 | Official per-SDK maturity table; Ruby SDK GA status; current SDK version numbers | U |
| T9 | Ruby SDK Public Preview (May 1, 2025) — search-result title only ([Ruby SDK Public Preview (changelog entry)](https://temporal.io/change-log/ruby-sdk-public-preview)) | NF |
| T10 | SDK repository licences (historically MIT) | U |
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
| T33 | Temporal SDK API names for Activity/Workflow definitions, heartbeating and starting Workflows were not captured; code sketches use placeholders | U |
| T34 | Temporal Code Exchange not searched for a Dagster bridge | U |

## 4. Dagster — unverified or not fetched

| # | Item | Status |
|---|---|---|
| D1 | Default run coordinator in OSS; claim that Helm enables `QueuedRunCoordinator` | U |
| D2 | Resources (`ConfigurableResource`), IO managers, `@asset_check` decorator, `FreshnessPolicy` (reworked in 1.11) | U |
| D3 | Components and `dg` reached GA with Dagster 1.11 in mid-2025 | U |
| D4 | `AutomationCondition` replaced `AutoMaterializePolicy` | U |
| D5 | Partition definition types beyond `DailyPartitionsDefinition` (static, dynamic, multi) from primary docs; dynamic partitions shown only in a vendor blog ([Dynamic partitioning](https://dagster.io/blog/dynamic-partitioning)) | U / vendor blog |
| D6 | `max_partitions_per_run` for multi-run backfills | U |
| D7 | Run status sensors and multi-asset sensors | NF |
| D8 | "Re-execute from failure" UI page | NF |
| D9 | Default sensor evaluation interval of 30 s | U |
| D10 | Default `max_concurrent_runs`; default schedule timeout; `DAGSTER_GRPC_TIMEOUT_SECONDS` and `DAGSTER_SCHEDULE_GRPC_TIMEOUT_SECONDS` | U |
| D11 | Pipes clients for Rust, Java, EMR, Lambda, ECS, Glue; Pipes on Snowflake | U |
| D12 | Full OSS vs Dagster+ feature matrix (SSO, catalog, insights, cost tracking) — partly confirmed by [Observe](https://dagster.io/docs/guides/observe) and [GitHub Discussion #23851 (maintainer answer)](https://github.com/dagster-io/dagster/discussions/23851) | U (partial) |
| D13 | Contributor count (~500+ commonly cited) | U |
| D14 | Exact close date of the Prefect acquisition | U |
| D15 | Whether `AirbyteWorkspaceComponent` triggers syncs or only observes | U |
| D16 | Sling incremental replication modes in the Dagster integration | U |
| D17 | Any numeric integration maturity metric (only "Dagster supported" vs "Community supported" tags) | U |
| D18 | Official statement that partitioned writes must be idempotent | U |
| D19 | Dedicated S3 sensor helper (e.g. `get_s3_keys` in `dagster-aws`) | U |
| D20 | Official delivery semantics for asset materializations | U |
| D21 | Dagster customer case study on ingestion | U |
| D22 | Official "Dagster is not a streaming engine" statement (forum blocked by robots.txt) | U |
| D23 | Launchpad, run timeline/Gantt view, OSS asset health in the UI | U |
| D24 | Prometheus metrics endpoint or native OpenTelemetry tracing in OSS | U |
| D25 | Run-failure sensors as an OSS alerting mechanism | U / I |
| D26 | `materialize()` and `build_op_context()` examples ([Execution API reference](https://docs.dagster.io/api/dagster/execution), [Ops API reference](https://docs.dagster.io/api/dagster/ops)) | NF |
| D27 | Asset check severity levels | U |
| D28 | Legacy `dagster dev` command, default port 3000, `dg` vs `dagster` CLI relationship ([Dagster CLI API reference](https://docs.dagster.io/api/clis/cli)) | U / NF |
| D29 | Branch deployments as Dagster+ only (only branch-deployment *concurrency limits* are confirmed Dagster+) | U |
| D30 | Whether feature request #2219 (OSS auth/RBAC) is still open | U |
| D31 | Official guidance for securing the webserver behind a proxy | U |
| D32 | Dagster+ RBAC docs page ([Dagster+ RBAC user roles and permissions (docs source)](https://github.com/dagster-io/dagster/blob/master/docs/docs/deployment/dagster-plus/authentication-and-access-control/rbac/user-roles-permissions.md)) | NF |
| D33 | Dagster+ Alerts and Insights pages ([Alerts (Dagster+)](https://docs.dagster.io/guides/observe/alerts), [Insights (Dagster+)](https://docs.dagster.io/guides/observe/insights)) | NF |
| D34 | Dagster vs Airflow comparison pages in site navigation | NF |
| D35 | Dagster API names `@asset` and the sensor run-request class are not captured in the notes; sketches use placeholders | U |
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

1. Does Temporal External Storage work against a self-hosted server, and with which SDK versions? (T7)
2. Does the Dagster Sling component support incremental modes, and does the Airbyte component trigger syncs? (D15, D16)
3. What are Dagster OSS defaults for sensor interval and `max_concurrent_runs`? (D9, D10)
4. What is the 2027 roadmap for Dagster OSS under Prefect? (D14)
5. Is there a maintained community bridge between Temporal and Dagster? (T34)

## Sources

- [S9] [Understanding the 4 types of Activity timeouts](https://temporal.io/blog/activity-timeouts) — *vendor blog, not fetched*
- [S11] [Self-hosted defaults](https://docs.temporal.io/self-hosted-guide/defaults)
- [S13] [Temporal Cloud limits](https://docs.temporal.io/cloud/limits)
- [S16] [Temporal Service configuration reference](https://docs.temporal.io/references/configuration) — *not fetched*
- [S19] [External Storage Public Preview (changelog entry)](https://temporal.io/changelog/external-storage-public-preview) — *not fetched*
- [S22] [Batch Iterator](https://docs.temporal.io/design-patterns/batch-iterator) — *not fetched*
- [S23] [Sliding Window](https://docs.temporal.io/design-patterns/sliding-window) — *not fetched*
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
- [S49] [temporalio/temporal releases](https://github.com/temporalio/temporal/releases) — *GitHub HTML scraped*
- [S51] [Ruby SDK Public Preview (changelog entry)](https://temporal.io/change-log/ruby-sdk-public-preview) — *search-result title only, not fetched*
- [S52] [OSS deployment architecture](https://dagster.io/docs/deployment/oss/oss-deployment-architecture)
- [S53] [dagster.yaml reference](https://dagster.io/docs/deployment/oss/dagster-yaml)
- [S54] [Instance configuration](https://dagster.io/docs/deployment/oss/oss-instance-configuration)
- [S56] [Run executors](https://dagster.io/docs/guides/operate/run-executors)
- [S78] [Dagster webserver and UI](https://dagster.io/docs/guides/operate/webserver)
- [S79] [Observe](https://dagster.io/docs/guides/observe)
- [S80] [Alerts (Dagster+)](https://docs.dagster.io/guides/observe/alerts) — *not fetched*
- [S81] [Insights (Dagster+)](https://docs.dagster.io/guides/observe/insights) — *not fetched*
- [S84] [Execution API reference](https://docs.dagster.io/api/dagster/execution) — *not fetched*
- [S85] [Ops API reference](https://docs.dagster.io/api/dagster/ops) — *not fetched*
- [S87] [Dagster CLI API reference](https://docs.dagster.io/api/clis/cli) — *not fetched*
- [S89] [GitHub Discussion #23851 (maintainer answer)](https://github.com/dagster-io/dagster/discussions/23851)
- [S90] [Dagster+ RBAC user roles and permissions (docs source)](https://github.com/dagster-io/dagster/blob/master/docs/docs/deployment/dagster-plus/authentication-and-access-control/rbac/user-roles-permissions.md) — *not fetched*
- [S91] [maltzsama/dagster-authkit](https://github.com/maltzsama/dagster-authkit) — *third-party, not evaluated*
- [S92] [stianthaulow/dagster-auth](https://github.com/stianthaulow/dagster-auth) — *third-party, not evaluated*
- [S94] [dagster-io/dagster releases](https://github.com/dagster-io/dagster/releases) — *GitHub HTML scraped*
- [S95] [PyPI dagster JSON](https://pypi.org/pypi/dagster/json)
- [S96] [Prefect is acquiring Dagster](https://dagster.io/blog/prefect-is-acquiring-dagster) — *vendor blog*
- [S97] [Dagster Customer FAQ](https://dagster.io/dagster-customer-faq) — *vendor FAQ*
- [S98] [Prefect expands open source AI stack with Dagster acquisition](https://opensourceforu.com/2026/07/prefect-expands-open-source-ai-stack-with-dagster-acquisition/) — *third-party press, not fetched*
- [S99] [Prefect to acquire Dagster Labs](https://techstrong.ai/features/prefect-to-acquire-dagster-labs-to-drive-ai-workflow-automation/) — *third-party press, not fetched*
- [S100] [Why We Built Dagster](https://dagster.io/blog/dagster-the-data-orchestrator) — *vendor blog*
- [S101] [Dynamic partitioning](https://dagster.io/blog/dynamic-partitioning) — *vendor blog*
- [S102] [How does Dagster compare to Flyte and Temporal (community forum)](https://discuss.dagster.io/t/22772686/how-does-dagster-compare-to-flyte-and-temporal) — *third-party, AI-generated answer, not authoritative*

Full numbered list: [references/sources.md](../references/sources.md)
