# Evidence, gaps and verification log

Every claim the research could **not** confirm, every source conflict, and the verification log. None of these items is used as a fact elsewhere; where [1-comparison.md](1-comparison.md) or [2-migration-plan.md](2-migration-plan.md) mention one, they say **Not verified**.

| Status | Meaning |
|---|---|
| **U** | Unverified (missing from fetched pages) |
| **C** | Conflicting sources |
| **3P** | Third-party only |
| **NF** | Page found but not fetched |
| **I** | Inference only |

**Biggest open items for ingestion:** an explicit official statement that External Storage works with self-hosted Temporal (and minimum versions) (TG7); a Dagster example of Sling `incremental`/`change-capture` (DG16). No official Temporal–Dagster comparison or integration was found in the searches recorded here (P1, P4); the only comparison found is an AI-generated forum answer [S102](sources.md#s102).

## Contents

1. [Research-method caveats](#1-research-method-caveats)
2. [Conflicts between sources](#2-conflicts-between-sources)
3. [Temporal gaps](#3-temporal-gaps)
4. [Dagster gaps](#4-dagster-gaps)
5. [Third-party and vendor-blog items](#5-third-party-and-vendor-blog-items)
6. [Open questions for a spike](#6-open-questions-for-a-spike)
7. [Verification log (2026-11-11)](#7-verification-log-2026-11-11)
8. [Verification log (2026-11-12): Dagster features in Temporal](#8-verification-log-2026-11-12-dagster-features-in-temporal)
9. [Verification log (2026-11-12): effort estimates](#9-verification-log-2026-11-12-effort-estimates)

## 1. Research-method caveats

| # | Item | Status |
|---|---|---|
| M1 | Page text was extracted with a summarisation fetch tool; spot-check verbatim quotes | Caveat |
| M2 | GitHub API returned HTTP 403; stars, forks, releases scraped from HTML; contributor counts unavailable | U |
| M3 | `docs.dagster.io/*` 302-redirects to `dagster.io/docs/*`; a few cited `docs.dagster.io` URLs were not fetched [S80](sources.md#s80) [S81](sources.md#s81) [S85](sources.md#s85) [S87](sources.md#s87) | NF |
| M4 | GitHub source pages were rate-limited and `curl` to dagster.io was blocked, so Dagster defaults were read from the official PyPI wheels (`dagster` 1.13.29, `dagster-aws` 0.29.29) [S112](sources.md#s112) [S113](sources.md#s113). Package source can change; re-check after upgrades | Caveat |
| M5 | Shared-cluster facts in the migration plan come from the team's review of an internal repo, not checked by us | Caveat |

## 2. Conflicts between sources

| # | Conflict | Sources | How handled |
|---|---|---|---|
| C1 | Dagster latest version (re-checked 2026-11-11): PyPI `info.version` = 1.13.29 (2026-11-05; 1.13.28 on 2026-10-30); GitHub Releases marks 1.13.28 / 0.29.28 (30 Oct) "Latest", no 1.13.29 entry; docs picker shows "Latest (1.12.8)" | [S95](sources.md#s95) [S94](sources.md#s94) [S52](sources.md#s52) | Still conflicting. PyPI is the version of record (what `pip` installs). Why GitHub and docs lag is **not stated** |
| C2 | Asset catalog: Observe page labels it Dagster+; webserver page describes an OSS catalog | [S79](sources.md#s79) [S78](sources.md#s78) | Read as basic OSS catalog + enhanced Dagster+ (**I**) |
| C3 | `docker_executor` described as using a Kubernetes pod on the executors page | [S56](sources.md#s56) | Likely docs error; `dagster-docker` page not checked |
| C4 | Default compute log manager: one note says no page names it; another quotes "Used by default, the LocalComputeLogManager" | [S53](sources.md#s53) [S54](sources.md#s54) | Resolved: Local is default per [S54](sources.md#s54) |
| C5 | `run_monitoring` marked unverified in one note; defaults quoted from the instance page in another | [S54](sources.md#s54) | Resolved: verified |
| C6 | Rapidflare post "not fetched" in one note, summarised in another | [S28](sources.md#s28) | Used, labelled *vendor blog* |
| C7 | Temporal release dates on GitHub omit the year | [S49](sources.md#s49) | 2026 shown as inferred |
| C8 | Temporal Signal cap: Cloud states 10,000 per execution; self-hosted lists only 2,000 *pending* and no total cap | [S13](sources.md#s13) [S11](sources.md#s11) | Self-hosted total cap **U** |

## 3. Temporal gaps

| # | Item | Status |
|---|---|---|
| TG1 | OpenSearch/Elasticsearch version matrix for visibility | U |
| TG2 | Scaling characteristics of the internal Worker service | U |
| TG3 | Per-SDK sandbox rules (threads, I/O) for Workflow code | U |
| TG4 | Workflow Execution/Run/Task timeout defaults, incl. the widely cited 10 s Workflow Task timeout | U |
| TG5 | Semantics of Signals, Queries, Updates and Continue-As-New (only limits verified) | NF |
| TG6 | Numeric limits for `MaxCallbacksPerWorkflow` and memo size | U |
| TG7 | External Storage on self-hosted: SDK-side (Data Converter; clients/Workers move data, history keeps a reference); no page says Cloud-only; changelog tags Cloud, Server, Go SDK, Python SDK. But no page **explicitly** says self-hosted is supported, and no minimum server/SDK version is stated [S18](sources.md#s18) [S106](sources.md#s106) [S107](sources.md#s107) [S19](sources.md#s19) | U (narrowed) |
| TG8 | Official per-SDK maturity table; Ruby SDK GA status; current SDK versions | U |
| TG9 | Ruby SDK Public Preview (May 1, 2025): search-result title only [S51](sources.md#s51) | NF |
| TG10 | SDK licences other than Python (`temporalio` 1.36.0 is MIT [S116](sources.md#s116)) | U (partial) |
| TG11 | Whether the Helm chart is "production-ready" (no statement either way) | U |
| TG12 | Task queue partition default count; `matching.numTaskqueueReadPartitions` / `matching.numTaskqueueWritePartitions` (forum only) | U / 3P |
| TG13 | Server contributor count | U |
| TG14 | Any official ETL/batch docs page (only design patterns and blogs found) | U |
| TG15 | Official data-source connectors: none found; conclusion from absence | I |
| TG16 | Exact names of `temporalio/samples-*` batch samples (only an unofficial mirror found) | U / 3P |
| TG17 | Official "polling external APIs" guidance | NF |
| TG18 | Dedicated Continue-As-New page | NF |
| TG19 | Batch Iterator and Sliding Window pages [S22](sources.md#s22) [S23](sources.md#s23) | NF |
| TG20 | Rate-limit guide with separate Task Queues [S25](sources.md#s25) | NF |
| TG21 | Any official CDC statement; Workflow Streams content [S26](sources.md#s26) [S30](sources.md#s30) | U / NF |
| TG22 | Server-side metrics reference and Prometheus config [S16](sources.md#s16) | NF |
| TG23 | Whether `workflow.logger` suppresses duplicate logs during replay | U |
| TG24 | Limit on number of custom Search Attributes (now answered for SQL visibility: 10 Keyword per Namespace [S38](sources.md#s38); Elasticsearch limits not checked) | U (partial) |
| TG25 | GA vs preview of Worker Versioning in self-hosted OSS; Build-ID vs Worker Deployments model ([S136](sources.md#s136) does not say OSS is covered) | U |
| TG26 | Batch operations in the Web UI | U |
| TG27 | Activity timeouts blog [S9](sources.md#s9) | NF |
| TG28 | Airbyte case study on temporal.io [S29](sources.md#s29) | NF |
| TG29 | Other SDK observability and testing guides [S33](sources.md#s33) [S34](sources.md#s34) [S35](sources.md#s35) [S36](sources.md#s36) [S37](sources.md#s37) [S42](sources.md#s42) [S43](sources.md#s43) [S40](sources.md#s40) | NF |
| TG30 | Codec and encryption deep-dives [S46](sources.md#s46) [S47](sources.md#s47) | NF |
| TG31 | "At-least-once" is not the literal wording of the Activity Definition page (it says "may be executed multiple times"); the phrase is in a guest post [S27](sources.md#s27) | I / vendor blog |
| TG32 | No row-level auditing: guest post only [S27](sources.md#s27) | Vendor blog |
| TG33 | Temporal Code Exchange not searched for a Dagster bridge | U |

## 4. Dagster gaps

| # | Item | Status |
|---|---|---|
| DG1 | Helm chart enables `QueuedRunCoordinator` (OSS default `DefaultRunCoordinator` confirmed by [S54](sources.md#s54) [S55](sources.md#s55)) | U (partial) |
| DG2 | Resources (`ConfigurableResource`), IO managers, `@asset_check`, `FreshnessPolicy` (reworked in 1.11) | U |
| DG3 | Components and `dg` reached GA with Dagster 1.11 in mid-2025 | U |
| DG4 | `AutomationCondition` replaced `AutoMaterializePolicy` | U |
| DG5 | Static and multi partition definitions from primary docs (`DynamicPartitionsDefinition` and its `SensorResult` example confirmed in [S109](sources.md#s109)) | U (partial) |
| DG7 | Run status sensors and multi-asset sensors | NF |
| DG8 | "Re-execute from failure" UI page | NF |
| DG10 | Default schedule timeout; `DAGSTER_GRPC_TIMEOUT_SECONDS`, `DAGSTER_SCHEDULE_GRPC_TIMEOUT_SECONDS` | U |
| DG11 | Pipes clients for Rust, Java, EMR, Lambda, ECS, Glue; Pipes on Snowflake | U |
| DG12 | Full OSS vs Dagster+ feature matrix (SSO, catalog, insights, cost); partly confirmed by [S79](sources.md#s79) [S89](sources.md#s89) | U (partial) |
| DG13 | Contributor count (~500+ commonly cited) | U |
| DG14 | Exact close date of the Prefect acquisition | U |
| DG16 | A Dagster example of Sling `incremental` or `change-capture` (Sling defines the modes; Dagster shows only `full-refresh`) [S105](sources.md#s105) [S104](sources.md#s104) | U (narrowed) |
| DG17 | Any numeric integration maturity metric (only "Dagster supported" vs "Community supported") | U |
| DG18 | Official statement that partitioned writes must be idempotent | U |
| DG19 | `dagster_aws.s3.sensor.get_objects` (and deprecated `get_s3_keys`) in package source but **not in the API reference** [S113](sources.md#s113) [S114](sources.md#s114) | U (exists, undocumented) |
| DG20 | Official delivery semantics for asset materializations | U |
| DG21 | Dagster customer case study on ingestion | U |
| DG22 | Official "Dagster is not a streaming engine" statement (forum blocked by robots.txt) | U |
| DG23 | Launchpad, run timeline/Gantt view, OSS asset health in the UI | U |
| DG24 | Prometheus metrics endpoint or native OpenTelemetry in OSS | U |
| DG25 | Run-failure sensors as an OSS alerting mechanism | U / I |
| DG26 | `build_op_context()` examples [S85](sources.md#s85) (`materialize()` confirmed in [S84](sources.md#s84)) | NF |
| DG27 | Asset check severity levels | U |
| DG28 | Legacy `dagster dev`, default port 3000, `dg` vs `dagster` CLI relationship [S87](sources.md#s87) | U / NF |
| DG29 | Branch deployments as Dagster+ only (only their *concurrency limits* are confirmed Dagster+) | U |
| DG30 | Whether feature request #2219 (OSS auth/RBAC) is still open | U |
| DG31 | Official guidance for securing the webserver behind a proxy | U |
| DG32 | Dagster+ RBAC page [S90](sources.md#s90) | NF |
| DG33 | Dagster+ Alerts and Insights pages [S80](sources.md#s80) [S81](sources.md#s81) | NF |
| DG34 | Dagster vs Airflow comparison pages | NF |
| DG36 | Secrets management for source credentials (either tool) | U |


## 5. Third-party and vendor-blog items

**Third-party**

| # | Item | Source | Use |
|---|---|---|---|
| P1 | AI-generated answer comparing Dagster, Flyte, Temporal | [S102](sources.md#s102) | Only to show no official comparison was found |
| P2 | Community auth/RBAC add-ons for Dagster OSS | [S91](sources.md#s91) [S92](sources.md#s92) | Mentioned; not evaluated |
| P3 | Press coverage of the Prefect–Dagster deal | [S98](sources.md#s98) [S99](sources.md#s99) | Not fetched; not used for facts |
| P4 | Search for "Dagster Temporal integration" returned only third-party Dagster triggers (Kestra, Paradime, Hightouch, Airbyte) | — (no URLs recorded) | Supports "no official integration" |
| P5 | Forum threads on Temporal task-queue partitions | — (no URLs recorded) | Not used |

**Vendor-blog claims used (labelled in the docs)**

| Claim | Source |
|---|---|
| Model each step as an Activity; heartbeats for resumable streaming ingestion; pass IDs, keep data in Postgres/Redis | [S27](sources.md#s27) |
| 1M-document pipeline: child per document, sliding window, Continue-As-New, `ParentClosePolicy.ABANDON`, `ScheduleOverlapPolicy.SKIP`, 20+ source types | [S28](sources.md#s28) |
| Dynamic partitions file-sensor pattern | [S101](sources.md#s101) |
| Dagster positioning vs "orchestrators and workflow engines" | [S100](sources.md#s100) |
| Prefect acquisition statements | [S96](sources.md#s96) [S97](sources.md#s97) |
| Worker Versioning GA (does not say OSS is covered) | [S136](sources.md#s136) |

## 6. Open questions for a spike

| # | Question | Settles |
|---|---|---|
| 1 | Does External Storage work with a self-hosted server, and from which server/SDK versions? Test on a self-hosted dev server | TG7 |
| 2 | Does Sling `incremental` / `change-capture` work unchanged through the Dagster Sling component? | DG16 |
| 3 | What is the 2027 roadmap for Dagster OSS under Prefect? | DG14 |
| 4 | Is there a maintained community bridge between Temporal and Dagster? | TG33 |

Migration-specific unknowns live in the [unknowns register](2-migration-plan.md#5-unknowns-register).

## 7. Verification log (2026-11-11)

"Confirmed" = page matches the repo text; "Corrected" = repo text was changed.

**Gaps closed**

| Claim | Pages | Result |
|---|---|---|
| Airbyte component triggers or observes | [S103](sources.md#s103) [S74](sources.md#s74) | Confirmed: "Materializing these assets will trigger a sync of the Airbyte connection"; `AirbyteCloudWorkspace` and `AirbyteWorkspace` both launch and monitor syncs; no observe-only option |
| Sling incremental modes | [S105](sources.md#s105) [S104](sources.md#s104) | Confirmed: `full-refresh` (default), `incremental`, `truncate`, `snapshot`, `backfill`, `definition-only`, `change-capture`; Dagster lists incremental support but shows only `full-refresh` |
| External Storage on self-hosted | [S18](sources.md#s18) [S106](sources.md#s106) [S107](sources.md#s107) [S19](sources.md#s19) | Partly: SDK-side, not stated as Cloud-only, no version requirements; explicit self-hosted support not stated (TG7 kept) |
| Default sensor interval | [S108](sources.md#s108) [S117](sources.md#s117) [S112](sources.md#s112) | Confirmed from source only: `DEFAULT_SENSOR_DAEMON_INTERVAL = 30`; docs give no default |
| Default `max_concurrent_runs` | [S111](sources.md#s111) [S53](sources.md#s53) [S112](sources.md#s112) | Confirmed from source only: "Defaults to 10. Set to -1 to disable the limit"; docs show examples only |
| S3 sensor helper | [S114](sources.md#s114) [S113](sources.md#s113) | Exists in source (`get_objects`; `get_s3_keys` deprecated); not in API reference |
| Dagster latest version | [S95](sources.md#s95) [S94](sources.md#s94) [S52](sources.md#s52) | PyPI 1.13.29 (2026-11-05); GitHub "Latest" 1.13.28 (30 Oct); docs picker 1.12.8 (C1) |
| Code-sketch API names | [S115](sources.md#s115) [S20](sources.md#s20) [S108](sources.md#s108) [S109](sources.md#s109) [S110](sources.md#s110) [S84](sources.md#s84) [S116](sources.md#s116) | Confirmed; placeholders replaced |

**Citation spot-checks**

| Claim | Page | Result |
|---|---|---|
| Blob 512 KB/2 MB, gRPC 4 MB, transaction 4 MB, history 10,240/51,200 events and 10/50 MB | [S11](sources.md#s11) | Confirmed |
| 2,000 pending ops, ≤500 recommended; Updates 10 in-flight/2,000 total; IDs 1,000 chars (SQL sample 255); 1 batch job | [S11](sources.md#s11) | Confirmed |
| On pending-limit hit "the Workflow Task fails and is retried" | [S11](sources.md#s11) [S12](sources.md#s12) | Corrected: defaults page says Commands fail; Task fail/retry is on the limits page |
| No concurrent-execution limit; 30 incomplete Nexus ops | [S12](sources.md#s12) | Confirmed |
| Cloud: 2 MB/4 MB/4 MB, 500 APS, 10 schedule req/s, 30 Visibility calls/s, retention 30 d (1–90), 10,000 Signals | [S13](sources.md#s13) | Confirmed |
| Activity retry defaults 1 s, 2.0, 100 s, unlimited; no Workflow retry by default; Workflow Task backoff up to 10 min | [S7](sources.md#s7) | Confirmed |
| Workflow retries "generally discouraged" | [S7](sources.md#s7) | Corrected: page says "not recommended" |
| Overlap policies (default Skip), catch-up 1 year/min 10 s, 2 MiB last result, backfill AllowAll/BufferAll | [S10](sources.md#s10) | Confirmed |
| History Shards "range 1–128K" | [S2](sources.md#s2) | Corrected: 1–128K is the range "operated successfully", not a hard limit; count cannot change |
| ≥2 Workers per queue, poll success >90%/>95%, setting names | [S15](sources.md#s15) | Confirmed |
| Queue-wide rate limit, 2 Workers × 5 = 5, last poller wins | [S24](sources.md#s24) | Confirmed |
| Activity timeout defaults; heartbeat throttle 0.8×, 30 s, 60 s | [S8](sources.md#s8) | Confirmed |
| Heartbeat details persisted for retries; timeout 2–3× interval | [S20](sources.md#s20) | Confirmed |
| "may be executed multiple times…"; writes idempotent; Run ID + Activity ID key | [S6](sources.md#s6) | Confirmed |
| Batch Iterator / Sliding Window quotes; "51,200 events max (aim for a few thousand)" | [S21](sources.md#s21) | Confirmed |
| 2 MB default, configurable self-hosted; claim check; codec only delays | [S17](sources.md#s17) | Confirmed |
| External Storage Public Preview, 256 KiB threshold, S3 (Go/Java/Python/TS), GCS (Go/TS) | [S18](sources.md#s18) | Confirmed |
| Deployment options incl. systemd, Compose ports 7233/8080, chart 0.73.1+ for 1.30+, not public-internet | [S14](sources.md#s14) | Confirmed |
| Temporal MIT, 23.7k stars/2.0k forks, Cadence fork, `start-dev` UI 8233 | [S48](sources.md#s48) | Confirmed |
| v1.32.1 "Latest" (08 Oct); v1.31.4, v1.30.8 (12 Oct) | [S49](sources.md#s49) | Confirmed |
| Changelog dates (Rust SDK 09-04, Standalone Activities 09-15, Priority & Fairness 05-05, External Storage 05-14, Worker Controller 05-04) | [S50](sources.md#s50) | Confirmed |
| Dagster Apache-2.0, 16.1k stars/2.3k forks | [S93](sources.md#s93) | Confirmed |
| Prefect acquisition quotes | [S96](sources.md#s96) [S97](sources.md#s97) | Confirmed; two quotes are from the FAQ, attribution clarified; close date not given ("in the coming weeks") |
| Sensor timeout 60 s, `DAGSTER_SENSOR_GRPC_TIMEOUT_SECONDS`, >3 min guideline | [S62](sources.md#s62) | Confirmed |
| Daemon replicas not supported; 1 replica per code location | [S52](sources.md#s52) | Confirmed |
| Pools `default_limit: 3`, `max_concurrent_runs: 10` example | [S63](sources.md#s63) | Confirmed |
| Not strict FIFO with pools | [S55](sources.md#s55) | Clarified: page states it for Dagster+ |
| Helm chart deploys webserver on 8080 | [S88](sources.md#s88) | Corrected: 8080 is the local port-forward to pod port 80 |
| Run retries: `FROM_FAILURE` default, `retry_on_asset_or_op_failure` 1.6.7, on by default in Dagster+ | [S59](sources.md#s59) | Confirmed |
| `RetryPolicy` params; "every exception triggers a retry" | [S58](sources.md#s58) | Confirmed |
| run_monitoring 180/180/3/120/300 | [S54](sources.md#s54) | Confirmed |
| Sensors: run-key dedupe quote, "not the exact frequency" | [S61](sources.md#s61) | Confirmed |
| dlt: `defer_table_reflect`, ~1 min refresh, concurrency limits | [S70](sources.md#s70) | Confirmed; limits are "recommended" (wording softened) |
| Backfill: one run per partition default; `multi_run`, `single_run` | [S65](sources.md#s65) | Confirmed |
| `dagster-embedded-elt` deprecated → `dagster-sling`, `dagster-dlt` | [S72](sources.md#s72) | Confirmed |
| Fivetran component keys and default "manual" schedule | [S75](sources.md#s75) | Confirmed; YAML sketch fixed to real key placement |

## 8. Verification log (2026-11-12): Dagster features in Temporal

| Claim | Pages | Result |
|---|---|---|
| Temporal docs have no partition or lineage page | [S144](sources.md#s144) | Confirmed by absence in the docs index; conclusion from absence |
| No Temporal integration for OpenLineage | [S146](sources.md#s146) [S147](sources.md#s147); web search | Not found (*absence*); OpenLineage integrations page body did not load |
| Bulk `cancel`/`terminate`/`signal`/`delete` by `--query`; batch reset only to `FirstWorkflowTask`, `LastWorkflowTask`, `BuildId` | [S142](sources.md#s142) | Confirmed. Batch actions in the Web UI still not documented (TG26) |
| Web UI Start Workflow form ("single JSON value"); "Start Workflow Like This One" copies only the first argument | [S31](sources.md#s31) | Confirmed |
| Retention Period: CLI default 3 days; min 1 hour local / 1 day global; no max on ≥ 1.18 | [S2](sources.md#s2) | Confirmed |
| SQL visibility: 10 Keyword; 3 each of Bool, Datetime, Double, Int, KeywordList, Text | [S38](sources.md#s38) | Confirmed |
| Schedule backfill: `AllowAll` parallel, `BufferAll` sequential; `TemporalScheduledStartTime` added to runs | [S10](sources.md#s10) | Confirmed. Page also says `BufferAll` backfills are subject to "the Catchup Window and internal buffer limits" but manual triggers and backfills are not restricted by it: unclear, check in POC-07 |
| Fairness: per-key rate limit `fairness-key-rps-limit-default`; Priority and Fairness change dispatch order, not concurrency | [S124](sources.md#s124) | Confirmed (wording paraphrased by fetch tool) |
| Dagster named dynamic partitions need `dynamic_partitions_store` (`DagsterInstance`) | [S109](sources.md#s109) | Confirmed |
| Dagster REST `report_asset_materialization` with `partition`; works on OSS webserver | [S152](sources.md#s152) | Confirmed; partitioned external assets not documented in [S153](sources.md#s153) |
| Dagster freshness policies: opt-in preview in OSS; alerts docs labelled Dagster+ | [S154](sources.md#s154) | Confirmed; external-asset support not stated |
| Snowflake lineage: Enterprise+, COPY/CTAS/INSERT…SELECT/MERGE, 1 year | [S151](sources.md#s151) | Confirmed |
| `dbt-ol` drop-in for `dbt`, Snowflake supported | [S147](sources.md#s147) | Confirmed |
| Kubernetes Job definition | [S156](sources.md#s156) | Confirmed (one line only; other Job fields not checked) |

## 9. Verification log (2026-11-12): effort estimates

The effort numbers in [§8 of the plan](2-migration-plan.md#8-effort-and-timeline-estimates) are **estimates** and the LLM factors are **judgement**. Only the study results below were checked.

| Claim | Pages | Result |
|---|---|---|
| Copilot group "completed the task 55.8% faster" (HTTP server in JavaScript) | [S158](sources.md#s158) | Confirmed in abstract; sample size not in abstract |
| 96 Google engineers; effect ~21 %; "our confidence interval is large" | [S159](sources.md#s159) | Confirmed in abstract |
| 16 experienced developers, 246 issues; 19 % slower with AI; developers believed ~20 % faster | [S160](sources.md#s160) | Confirmed. METR itself now marks the result out of date |
| Late-2025 data unreliable (selection effects); METR believes developers "are more sped up from AI tools now"; "only very weak evidence" | [S161](sources.md#s161) | Confirmed (**3P**). Late-2025 point estimates not used: sign convention unclear in the fetched text |
| AI adoption: est. −1.5 % delivery throughput, −7.2 % delivery stability | [S162](sources.md#s162) | Confirmed (vendor blog summarising the report) |
| These results transfer to data-pipeline migrations | — | **I** (inference): no study of this kind of work was found |
