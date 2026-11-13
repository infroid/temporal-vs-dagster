# Sources

Every `[S#]` citation in this repo resolves to one row below. Official docs and GitHub come first; other kinds are labelled.

| Label | Meaning |
|---|---|
| (none) | Official page, fetched and used |
| *not fetched* | URL found in search; content not verified; never sole support for a fact |
| *vendor blog* / *vendor FAQ* | Hosted by the vendor, not normative docs |
| *third-party* | Not from Temporal or Dagster |
| *package source* / *GitHub HTML scraped* | Read from the PyPI wheel or rendered GitHub page (GitHub API was blocked) |

`docs.dagster.io/*` URLs redirect (HTTP 302) to `dagster.io/docs/*`. Fetched 2026-11-11 (S1–S117) and 2026-11-11/12 for the migration plan (S118–S141). Fetched 2026-11-12 for the Dagster-feature options (S142–S157) and the effort estimates (S158–S162).

## Temporal

### Temporal: Concepts and architecture

| # | Source | Note |
|---|---|---|
| <a id="s1"></a>S1 | [Understanding Temporal](https://docs.temporal.io/evaluate/understanding-temporal) |  |
| <a id="s2"></a>S2 | [Temporal Server](https://docs.temporal.io/temporal-service/temporal-server) |  |
| <a id="s3"></a>S3 | [Persistence](https://docs.temporal.io/temporal-service/persistence) |  |
| <a id="s4"></a>S4 | [Self-hosted Visibility](https://docs.temporal.io/self-hosted-guide/visibility) | *not fetched* |

### Temporal: Execution model and durability

| # | Source | Note |
|---|---|---|
| <a id="s5"></a>S5 | [Workflow Definition](https://docs.temporal.io/workflow-definition.md) |  |
| <a id="s6"></a>S6 | [Activity Definition](https://docs.temporal.io/activity-definition) |  |
| <a id="s7"></a>S7 | [Retry Policies](https://docs.temporal.io/encyclopedia/retry-policies) |  |
| <a id="s8"></a>S8 | [Detecting Activity failures](https://docs.temporal.io/encyclopedia/detecting-activity-failures.md) |  |
| <a id="s9"></a>S9 | [Understanding the 4 types of Activity timeouts](https://temporal.io/blog/activity-timeouts) | *vendor blog, not fetched* |

### Temporal: Scheduling and triggers

| # | Source | Note |
|---|---|---|
| <a id="s10"></a>S10 | [Schedule](https://docs.temporal.io/schedule) |  |

### Temporal: Limits, deployment and scaling

| # | Source | Note |
|---|---|---|
| <a id="s11"></a>S11 | [Self-hosted defaults](https://docs.temporal.io/self-hosted-guide/defaults) |  |
| <a id="s12"></a>S12 | [Workflow Execution limits](https://docs.temporal.io/workflow-execution/limits) |  |
| <a id="s13"></a>S13 | [Temporal Cloud limits](https://docs.temporal.io/cloud/limits) |  |
| <a id="s14"></a>S14 | [Deploying a Temporal Service](https://docs.temporal.io/self-hosted-guide/deployment) |  |
| <a id="s15"></a>S15 | [Task Queue performance](https://docs.temporal.io/develop/worker-performance/task-queues) |  |
| <a id="s16"></a>S16 | [Temporal Service configuration reference](https://docs.temporal.io/references/configuration) | *not fetched* |

### Temporal: Ingestion patterns

| # | Source | Note |
|---|---|---|
| <a id="s17"></a>S17 | [Troubleshoot payload size limit](https://docs.temporal.io/troubleshooting/blob-size-limit-error) |  |
| <a id="s18"></a>S18 | [External Storage](https://docs.temporal.io/external-storage) |  |
| <a id="s19"></a>S19 | [External Storage Public Preview (changelog entry)](https://temporal.io/changelog/external-storage-public-preview) |  |
| <a id="s20"></a>S20 | [Long-Running Activity pattern](https://docs.temporal.io/design-patterns/long-running-activity) |  |
| <a id="s21"></a>S21 | [Batch Processing Patterns](https://docs.temporal.io/design-patterns/batch-processing-patterns) |  |
| <a id="s22"></a>S22 | [Batch Iterator](https://docs.temporal.io/design-patterns/batch-iterator) | *not fetched* |
| <a id="s23"></a>S23 | [Sliding Window](https://docs.temporal.io/design-patterns/sliding-window) | *not fetched* |
| <a id="s24"></a>S24 | [Downstream Rate Limiting](https://docs.temporal.io/design-patterns/downstream-rate-limiting) |  |
| <a id="s25"></a>S25 | [Rate-limit downstream APIs with separate Task Queues](https://docs.temporal.io/guides/rate-limit-downstream-apis) | *not fetched* |
| <a id="s26"></a>S26 | [Workflow Streams](https://docs.temporal.io/workflow-streams) | *not fetched* |

### Temporal: Blogs and case studies

| # | Source | Note |
|---|---|---|
| <a id="s27"></a>S27 | [The foundation: Why Temporal for a data pipeline?](https://temporal.io/blog/the-foundation-why-temporal-for-a-data-pipeline) | *vendor blog, guest post* |
| <a id="s28"></a>S28 | [How Rapidflare built a million+ document ingestion pipeline](https://temporal.io/blog/how-rapidflare-built-a-million-document-ingestion-pipeline-for-agents-on-temporal) | *vendor blog, customer guest post* |
| <a id="s29"></a>S29 | [Airbyte case study](https://web.temporal.io/case-studies/airbyte-case-study) | *vendor case study, not fetched* |
| <a id="s30"></a>S30 | [Workflow Streams: live interactivity for agents](https://temporal.io/blog/workflow-streams-live-interactivity-agents-other-applications) | *vendor blog, not fetched* |

### Temporal: Observability, testing and DX

| # | Source | Note |
|---|---|---|
| <a id="s31"></a>S31 | [Temporal Web UI](https://docs.temporal.io/web-ui) |  |
| <a id="s32"></a>S32 | [Observability - Python SDK](https://docs.temporal.io/develop/python/platform/observability) |  |
| <a id="s33"></a>S33 | [Observability - Go SDK](https://docs.temporal.io/develop/go/observability) | *not fetched* |
| <a id="s34"></a>S34 | [Observability - Java SDK](https://docs.temporal.io/develop/java/observability) | *not fetched* |
| <a id="s35"></a>S35 | [Observability - TypeScript SDK](https://docs.temporal.io/develop/typescript/platform/observability) | *not fetched* |
| <a id="s36"></a>S36 | [Observability - .NET SDK](https://docs.temporal.io/develop/dotnet/platform/observability) | *not fetched* |
| <a id="s37"></a>S37 | [Observability - Ruby SDK](https://docs.temporal.io/develop/ruby/platform/observability) | *not fetched* |
| <a id="s38"></a>S38 | [Search Attributes](https://docs.temporal.io/search-attribute) | Fetched 2026-11-11 for the migration plan |
| <a id="s39"></a>S39 | [Temporal CLI server command reference](https://docs.temporal.io/cli/server) |  |
| <a id="s40"></a>S40 | [Run a development server](https://docs.temporal.io/develop/run-a-development-server) | *not fetched* |
| <a id="s41"></a>S41 | [Testing - Python SDK](https://docs.temporal.io/develop/python/testing-suite) |  |
| <a id="s42"></a>S42 | [Testing - TypeScript SDK](https://docs.temporal.io/develop/typescript/testing-suite) | *not fetched* |
| <a id="s43"></a>S43 | [Pre-production testing best practices](https://docs.temporal.io/best-practices/pre-production-testing) | *not fetched* |
| <a id="s44"></a>S44 | [Safely deploying changes to Workflow code](https://docs.temporal.io/develop/safe-deployments) |  |

### Temporal: Security

| # | Source | Note |
|---|---|---|
| <a id="s45"></a>S45 | [Temporal Platform security features](https://docs.temporal.io/self-hosted-guide/security) |  |
| <a id="s46"></a>S46 | [Codecs and Encryption](https://docs.temporal.io/production-deployment/data-encryption) | *not fetched* |
| <a id="s47"></a>S47 | [Codec Server](https://docs.temporal.io/codec-server) | *not fetched* |

### Temporal: Licensing, releases and community

| # | Source | Note |
|---|---|---|
| <a id="s48"></a>S48 | [temporalio/temporal (GitHub)](https://github.com/temporalio/temporal) | *GitHub HTML scraped* |
| <a id="s49"></a>S49 | [temporalio/temporal releases](https://github.com/temporalio/temporal/releases) | *GitHub HTML scraped* |
| <a id="s50"></a>S50 | [Temporal changelog](https://temporal.io/changelog) |  |
| <a id="s51"></a>S51 | [Ruby SDK Public Preview (changelog entry)](https://temporal.io/change-log/ruby-sdk-public-preview) | *search-result title only, not fetched* |

## Dagster

### Dagster: Concepts and architecture

| # | Source | Note |
|---|---|---|
| <a id="s52"></a>S52 | [OSS deployment architecture](https://dagster.io/docs/deployment/oss/oss-deployment-architecture) |  |
| <a id="s53"></a>S53 | [dagster.yaml reference](https://dagster.io/docs/deployment/oss/dagster-yaml) |  |
| <a id="s54"></a>S54 | [Instance configuration](https://dagster.io/docs/deployment/oss/oss-instance-configuration) |  |
| <a id="s55"></a>S55 | [Run coordinators](https://dagster.io/docs/deployment/execution/run-coordinators) |  |
| <a id="s56"></a>S56 | [Run executors](https://dagster.io/docs/guides/operate/run-executors) |  |

### Dagster: Programming model and automation

| # | Source | Note |
|---|---|---|
| <a id="s57"></a>S57 | [Components](https://dagster.io/docs/guides/build/components) |  |
| <a id="s58"></a>S58 | [Op retries](https://dagster.io/docs/guides/build/ops/op-retries) |  |
| <a id="s59"></a>S59 | [Run retries](https://dagster.io/docs/deployment/execution/run-retries) |  |
| <a id="s60"></a>S60 | [Declarative Automation](https://dagster.io/docs/guides/automate/declarative-automation) |  |
| <a id="s61"></a>S61 | [Sensors](https://dagster.io/docs/guides/automate/sensors) |  |
| <a id="s62"></a>S62 | [Troubleshooting sensor timeouts](https://dagster.io/docs/deployment/troubleshooting/sensor-timeouts) |  |
| <a id="s63"></a>S63 | [Managing concurrency](https://dagster.io/docs/guides/operate/managing-concurrency) |  |

### Dagster: Partitions and backfills

| # | Source | Note |
|---|---|---|
| <a id="s64"></a>S64 | [Backfilling data](https://dagster.io/docs/guides/build/partitions-and-backfills/backfilling-data) |  |
| <a id="s65"></a>S65 | [Partition backfill strategies](https://dagster.io/docs/examples/best-practices/partition-backfill-strategies) |  |

### Dagster: Integrations and ingestion

| # | Source | Note |
|---|---|---|
| <a id="s66"></a>S66 | [Dagster Pipes (guide)](https://dagster.io/docs/guides/build/external-pipelines) |  |
| <a id="s67"></a>S67 | [External pipelines (Pipes) integrations](https://dagster.io/docs/integrations/external-pipelines) |  |
| <a id="s68"></a>S68 | [Dagster integrations index](https://dagster.io/docs/integrations/libraries) |  |
| <a id="s69"></a>S69 | [dlt integration](https://dagster.io/docs/integrations/libraries/dlt) |  |
| <a id="s70"></a>S70 | [dlt integration (Pythonic)](https://dagster.io/docs/integrations/libraries/dlt/dlt-pythonic) |  |
| <a id="s71"></a>S71 | [Sling integration](https://dagster.io/docs/integrations/libraries/sling) |  |
| <a id="s72"></a>S72 | [Embedded ELT (deprecated)](https://dagster.io/docs/integrations/libraries/embedded-elt) |  |
| <a id="s73"></a>S73 | [Airbyte integration](https://dagster.io/docs/integrations/libraries/airbyte) |  |
| <a id="s74"></a>S74 | [Airbyte component](https://dagster.io/docs/integrations/libraries/airbyte/airbyte-component) |  |
| <a id="s75"></a>S75 | [Fivetran integration](https://dagster.io/docs/integrations/libraries/fivetran) |  |
| <a id="s76"></a>S76 | [Meltano integration](https://dagster.io/docs/integrations/libraries/meltano) |  |
| <a id="s77"></a>S77 | [dbt integration](https://dagster.io/docs/integrations/libraries/dbt) |  |

### Dagster: Observability, testing and DX

| # | Source | Note |
|---|---|---|
| <a id="s78"></a>S78 | [Dagster webserver and UI](https://dagster.io/docs/guides/operate/webserver) |  |
| <a id="s79"></a>S79 | [Observe](https://dagster.io/docs/guides/observe) |  |
| <a id="s80"></a>S80 | [Alerts (Dagster+)](https://docs.dagster.io/guides/observe/alerts) | *not fetched* |
| <a id="s81"></a>S81 | [Insights (Dagster+)](https://docs.dagster.io/guides/observe/insights) | *not fetched* |
| <a id="s82"></a>S82 | [Unit testing assets and ops](https://dagster.io/docs/guides/test/unit-testing-assets-and-ops) |  |
| <a id="s83"></a>S83 | [Testing assets with asset checks](https://dagster.io/docs/guides/test/asset-checks) |  |
| <a id="s84"></a>S84 | [Execution API reference](https://docs.dagster.io/api/dagster/execution) | *fetched 2026-11-11 via redirect to dagster.io/docs/api/dagster/execution* |
| <a id="s85"></a>S85 | [Ops API reference](https://docs.dagster.io/api/dagster/ops) | *not fetched* |
| <a id="s86"></a>S86 | [Running Dagster locally](https://dagster.io/docs/deployment/oss/deployment-options/running-dagster-locally) |  |
| <a id="s87"></a>S87 | [Dagster CLI API reference](https://docs.dagster.io/api/clis/cli) | *not fetched* |

### Dagster: Deployment

| # | Source | Note |
|---|---|---|
| <a id="s88"></a>S88 | [Deploying to Kubernetes](https://dagster.io/docs/deployment/oss/deployment-options/kubernetes/deploying-to-kubernetes) |  |

### Dagster: Security

| # | Source | Note |
|---|---|---|
| <a id="s89"></a>S89 | [GitHub Discussion #23851 (maintainer answer)](https://github.com/dagster-io/dagster/discussions/23851) |  |
| <a id="s90"></a>S90 | [Dagster+ RBAC user roles and permissions (docs source)](https://github.com/dagster-io/dagster/blob/master/docs/docs/deployment/dagster-plus/authentication-and-access-control/rbac/user-roles-permissions.md) | *not fetched* |
| <a id="s91"></a>S91 | [maltzsama/dagster-authkit](https://github.com/maltzsama/dagster-authkit) | *third-party, not evaluated* |
| <a id="s92"></a>S92 | [stianthaulow/dagster-auth](https://github.com/stianthaulow/dagster-auth) | *third-party, not evaluated* |

### Dagster: Licensing, releases and community

| # | Source | Note |
|---|---|---|
| <a id="s93"></a>S93 | [dagster-io/dagster (GitHub)](https://github.com/dagster-io/dagster) | *GitHub HTML scraped* |
| <a id="s94"></a>S94 | [dagster-io/dagster releases](https://github.com/dagster-io/dagster/releases) | *GitHub HTML scraped* |
| <a id="s95"></a>S95 | [PyPI dagster JSON](https://pypi.org/pypi/dagster/json) |  |
| <a id="s96"></a>S96 | [Prefect is acquiring Dagster](https://dagster.io/blog/prefect-is-acquiring-dagster) | *vendor blog* |
| <a id="s97"></a>S97 | [Dagster Customer FAQ](https://dagster.io/dagster-customer-faq) | *vendor FAQ* |
| <a id="s98"></a>S98 | [Prefect expands open source AI stack with Dagster acquisition](https://opensourceforu.com/2026/07/prefect-expands-open-source-ai-stack-with-dagster-acquisition/) | *third-party press, not fetched* |
| <a id="s99"></a>S99 | [Prefect to acquire Dagster Labs](https://techstrong.ai/features/prefect-to-acquire-dagster-labs-to-drive-ai-workflow-automation/) | *third-party press, not fetched* |

### Dagster: Blogs and positioning

| # | Source | Note |
|---|---|---|
| <a id="s100"></a>S100 | [Why We Built Dagster](https://dagster.io/blog/dagster-the-data-orchestrator) | *vendor blog* |
| <a id="s101"></a>S101 | [Dynamic partitioning](https://dagster.io/blog/dynamic-partitioning) | *vendor blog* |
| <a id="s102"></a>S102 | [How does Dagster compare to Flyte and Temporal (community forum)](https://discuss.dagster.io/t/22772686/how-does-dagster-compare-to-flyte-and-temporal) | *third-party, AI-generated answer, not authoritative* |

### Verification pass (2026-11-11)

| # | Source | Note |
|---|---|---|
| <a id="s103"></a>S103 | [dagster-airbyte library reference](https://dagster.io/docs/integrations/libraries/airbyte/dagster-airbyte) |  |
| <a id="s104"></a>S104 | [Sling (Pythonic)](https://dagster.io/docs/integrations/libraries/sling/sling-pythonic) |  |
| <a id="s105"></a>S105 | [Sling replication modes](https://docs.slingdata.io/concepts/replication/modes) | *Sling's official docs (third-party to Dagster)* |
| <a id="s106"></a>S106 | [External Storage - Python SDK](https://docs.temporal.io/develop/python/data-handling/external-storage) |  |
| <a id="s107"></a>S107 | [External Storage - Go SDK](https://docs.temporal.io/develop/go/data-handling/external-storage) |  |
| <a id="s108"></a>S108 | [Schedules and sensors API reference](https://dagster.io/docs/api/dagster/schedules-sensors) |  |
| <a id="s109"></a>S109 | [Partitions API reference](https://dagster.io/docs/api/dagster/partitions) |  |
| <a id="s110"></a>S110 | [Assets API reference](https://dagster.io/docs/api/dagster/assets) |  |
| <a id="s111"></a>S111 | [Run queue limits](https://dagster.io/docs/guides/operate/managing-concurrency/run-queue-limits) |  |
| <a id="s112"></a>S112 | [dagster 1.13.29 (PyPI)](https://pypi.org/project/dagster/1.13.29/) | *package source inspected: `_core/definitions/sensor_definition.py`, `_core/instance/config.py`* |
| <a id="s113"></a>S113 | [dagster-aws 0.29.29 (PyPI)](https://pypi.org/project/dagster-aws/0.29.29/) | *package source inspected: `dagster_aws/s3/sensor.py`* |
| <a id="s114"></a>S114 | [dagster-aws API reference](https://dagster.io/docs/api/libraries/dagster-aws) |  |
| <a id="s115"></a>S115 | [Activity Timeouts - Python SDK](https://docs.temporal.io/develop/python/activities/timeouts) |  |
| <a id="s116"></a>S116 | [temporalio 1.36.0 (PyPI)](https://pypi.org/project/temporalio/1.36.0/) | *package metadata and source inspected* |
| <a id="s117"></a>S117 | [Dagster daemon](https://dagster.io/docs/deployment/execution/dagster-daemon) |  |

### Migration plan (2026-11-11/12)

| # | Source | Note |
|---|---|---|
| <a id="s118"></a>S118 | [Temporal Worker Controller](https://docs.temporal.io/production-deployment/worker-deployments/kubernetes-controller) |  |
| <a id="s119"></a>S119 | [KEDA Temporal scaler](https://keda.sh/docs/2.21/scalers/temporal/) | *KEDA project docs; scaler maintained by the community* |
| <a id="s120"></a>S120 | [dbt programmatic invocations](https://docs.getdbt.com/reference/programmatic-invocations) | *dbt docs; page shows v1 and v2 notes* |
| <a id="s121"></a>S121 | [Worker performance](https://docs.temporal.io/develop/worker-performance) |  |
| <a id="s122"></a>S122 | [Meltano state backends](https://discovery.meltano.com/concepts/state_backends) | *Meltano docs* |
| <a id="s123"></a>S123 | [Python SDK sync vs async Activities](https://docs.temporal.io/develop/python/best-practices/python-sdk-sync-vs-async) |  |
| <a id="s124"></a>S124 | [Task Queue Priority and Fairness](https://docs.temporal.io/develop/task-queue-priority-fairness) |  |
| <a id="s125"></a>S125 | [dagster-k8s](https://dagster.io/docs/integrations/libraries/k8s/dagster-k8s) |  |
| <a id="s126"></a>S126 | [temporalio/helm-charts (README)](https://github.com/temporalio/helm-charts) | *GitHub HTML* |
| <a id="s127"></a>S127 | [dlt state](https://dlthub.com/docs/general-usage/state) | *dlt docs* |
| <a id="s128"></a>S128 | [Cancellation - Python SDK](https://docs.temporal.io/develop/python/workflows/cancellation) |  |
| <a id="s129"></a>S129 | [Signal with Start pattern](https://docs.temporal.io/design-patterns/signal-with-start) |  |
| <a id="s130"></a>S130 | [Workflow Id and Run Id](https://docs.temporal.io/workflow-execution/workflowid-runid) |  |
| <a id="s131"></a>S131 | [Temporal CLI schedule command reference](https://docs.temporal.io/cli/command-reference/schedule) |  |
| <a id="s132"></a>S132 | [Monitor Temporal Platform metrics](https://docs.temporal.io/self-hosted-guide/monitoring) |  |
| <a id="s133"></a>S133 | [dbt retry](https://docs.getdbt.com/reference/commands/retry) | *dbt docs* |
| <a id="s134"></a>S134 | [Temporal Python SDK sandbox](https://docs.temporal.io/develop/python/best-practices/python-sdk-sandbox) |  |
| <a id="s135"></a>S135 | [Worker Versioning](https://docs.temporal.io/production-deployment/worker-deployments/worker-versioning) |  |
| <a id="s136"></a>S136 | [Announcing GA for Worker Versioning and Public Preview for Upgrade on Continue-as-New](https://temporal.io/blog/ga-worker-versioning-public-preview-upgrade-on-continue-as-new) | *vendor blog, 2026-03-30* |
| <a id="s137"></a>S137 | [dbt build](https://docs.getdbt.com/reference/commands/build) | *dbt docs* |
| <a id="s138"></a>S138 | [Events and Event History](https://docs.temporal.io/workflow-execution/event) |  |
| <a id="s139"></a>S139 | [Reset an Activity](https://docs.temporal.io/activity-operations/reset) |  |
| <a id="s140"></a>S140 | [dbt node selection methods](https://docs.getdbt.com/reference/node-selection/methods) | *dbt docs* |
| <a id="s141"></a>S141 | [dbt source command](https://docs.getdbt.com/reference/commands/source) | *dbt docs* |

### Dagster features in Temporal (2026-11-12)

| # | Source | Note |
|---|---|---|
| <a id="s142"></a>S142 | [Temporal CLI workflow command reference](https://docs.temporal.io/cli/workflow) |  |
| <a id="s143"></a>S143 | [Temporal CLI batch command reference](https://docs.temporal.io/cli/batch) |  |
| <a id="s144"></a>S144 | [Temporal docs index (llms.txt)](https://docs.temporal.io/llms.txt) | *used to check absence: no page on partitions or lineage* |
| <a id="s145"></a>S145 | [samples-go: mutex](https://pkg.go.dev/github.com/temporalio/samples-go/mutex) | *official sample repo; page shows module v1.3.0 (2021)* |
| <a id="s146"></a>S146 | [OpenLineage Python client: usage](https://openlineage.io/docs/client/python/usage) | *OpenLineage docs* |
| <a id="s147"></a>S147 | [OpenLineage dbt integration](https://openlineage.io/docs/integrations/dbt) | *OpenLineage docs, v1.55.0* |
| <a id="s148"></a>S148 | [Marquez](https://marquezproject.ai/) | *project site* |
| <a id="s149"></a>S149 | [OpenMetadata OpenLineage connector](https://docs.open-metadata.org/latest/connectors/pipeline/openlineage) | *OpenMetadata docs* |
| <a id="s150"></a>S150 | [DataHub dbt source](https://docs.datahub.com/docs/generated/ingestion/sources/dbt) | *DataHub docs* |
| <a id="s151"></a>S151 | [Snowflake data lineage](https://docs.snowflake.com/en/user-guide/ui-snowsight-lineage) | *Snowflake docs* |
| <a id="s152"></a>S152 | [Dagster REST API](https://dagster.io/docs/api/rest-api) |  |
| <a id="s153"></a>S153 | [Dagster external assets](https://dagster.io/docs/guides/build/assets/external-assets) |  |
| <a id="s154"></a>S154 | [Dagster asset freshness policies](https://dagster.io/docs/guides/observe/asset-freshness-policies) |  |
| <a id="s155"></a>S155 | [dbt docs command](https://docs.getdbt.com/reference/commands/cmd-docs) | *dbt docs* |
| <a id="s156"></a>S156 | [Kubernetes Jobs](https://kubernetes.io/docs/concepts/workloads/controllers/job/) | *Kubernetes docs* |
| <a id="s157"></a>S157 | [Activity basics - Python SDK](https://docs.temporal.io/develop/python/activities/basics) |  |

### Effort estimates: LLM-assisted coding (2026-11-12)

| # | Source | Note |
|---|---|---|
| <a id="s158"></a>S158 | [The Impact of AI on Developer Productivity: Evidence from GitHub Copilot](https://arxiv.org/abs/2302.06590) | *third-party, research paper (2023); abstract read* |
| <a id="s159"></a>S159 | [How much does AI impact development speed? An enterprise-based randomized controlled trial](https://arxiv.org/abs/2410.12944) | *third-party, research paper (Google, 2024); abstract read* |
| <a id="s160"></a>S160 | [Measuring the Impact of Early-2025 AI on Experienced Open-Source Developer Productivity](https://metr.org/blog/2025-07-10-early-2025-ai-experienced-os-dev-study/) | *third-party, research blog (METR, 2025-07-10)* |
| <a id="s161"></a>S161 | [We are Changing our Developer Productivity Experiment Design](https://metr.org/blog/2026-02-24-uplift-update/) | *third-party, research blog (METR, 2026-02-24)* |
| <a id="s162"></a>S162 | [Announcing the 2024 DORA report](https://cloud.google.com/blog/products/devops-sre/announcing-the-2024-dora-report) | *vendor blog (Google Cloud, 2024)* |
