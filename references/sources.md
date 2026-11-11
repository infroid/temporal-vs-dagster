# Sources

> **Key takeaways**
> - Every citation in this repo resolves to one numbered entry below (S1, S2, ...).
> - Official docs and GitHub pages come first; vendor blogs, third-party pages and pages not fetched are labelled in *italics*.
> - `docs.dagster.io/*` URLs redirect (HTTP 302) to `dagster.io/docs/*`; the URLs below are the pages that served the content, except the few marked *not fetched*.
> - GitHub API access was blocked during research; GitHub facts come from scraped HTML pages.

As of 2026-11-11. Label legend: no label = official page, fetched and used; *not fetched* = URL found in search, content not verified; *vendor blog* = hosted by the vendor but not normative docs; *third-party* = not from Temporal or Dagster.

## Temporal


### Temporal: Concepts and architecture

1. **[S1]** [Understanding Temporal](https://docs.temporal.io/evaluate/understanding-temporal)
2. **[S2]** [Temporal Server](https://docs.temporal.io/temporal-service/temporal-server)
3. **[S3]** [Persistence](https://docs.temporal.io/temporal-service/persistence)
4. **[S4]** [Self-hosted Visibility](https://docs.temporal.io/self-hosted-guide/visibility) — *not fetched*

### Temporal: Execution model and durability

5. **[S5]** [Workflow Definition](https://docs.temporal.io/workflow-definition.md)
6. **[S6]** [Activity Definition](https://docs.temporal.io/activity-definition)
7. **[S7]** [Retry Policies](https://docs.temporal.io/encyclopedia/retry-policies)
8. **[S8]** [Detecting Activity failures](https://docs.temporal.io/encyclopedia/detecting-activity-failures.md)
9. **[S9]** [Understanding the 4 types of Activity timeouts](https://temporal.io/blog/activity-timeouts) — *vendor blog, not fetched*

### Temporal: Scheduling and triggers

10. **[S10]** [Schedule](https://docs.temporal.io/schedule)

### Temporal: Limits, deployment and scaling

11. **[S11]** [Self-hosted defaults](https://docs.temporal.io/self-hosted-guide/defaults)
12. **[S12]** [Workflow Execution limits](https://docs.temporal.io/workflow-execution/limits)
13. **[S13]** [Temporal Cloud limits](https://docs.temporal.io/cloud/limits)
14. **[S14]** [Deploying a Temporal Service](https://docs.temporal.io/self-hosted-guide/deployment)
15. **[S15]** [Task Queue performance](https://docs.temporal.io/develop/worker-performance/task-queues)
16. **[S16]** [Temporal Service configuration reference](https://docs.temporal.io/references/configuration) — *not fetched*

### Temporal: Ingestion patterns

17. **[S17]** [Troubleshoot payload size limit](https://docs.temporal.io/troubleshooting/blob-size-limit-error)
18. **[S18]** [External Storage](https://docs.temporal.io/external-storage)
19. **[S19]** [External Storage Public Preview (changelog entry)](https://temporal.io/changelog/external-storage-public-preview) — *not fetched*
20. **[S20]** [Long-Running Activity pattern](https://docs.temporal.io/design-patterns/long-running-activity)
21. **[S21]** [Batch Processing Patterns](https://docs.temporal.io/design-patterns/batch-processing-patterns)
22. **[S22]** [Batch Iterator](https://docs.temporal.io/design-patterns/batch-iterator) — *not fetched*
23. **[S23]** [Sliding Window](https://docs.temporal.io/design-patterns/sliding-window) — *not fetched*
24. **[S24]** [Downstream Rate Limiting](https://docs.temporal.io/design-patterns/downstream-rate-limiting)
25. **[S25]** [Rate-limit downstream APIs with separate Task Queues](https://docs.temporal.io/guides/rate-limit-downstream-apis) — *not fetched*
26. **[S26]** [Workflow Streams](https://docs.temporal.io/workflow-streams) — *not fetched*

### Temporal: Blogs and case studies

27. **[S27]** [The foundation: Why Temporal for a data pipeline?](https://temporal.io/blog/the-foundation-why-temporal-for-a-data-pipeline) — *vendor blog, guest post*
28. **[S28]** [How Rapidflare built a million+ document ingestion pipeline](https://temporal.io/blog/how-rapidflare-built-a-million-document-ingestion-pipeline-for-agents-on-temporal) — *vendor blog, customer guest post*
29. **[S29]** [Airbyte case study](https://web.temporal.io/case-studies/airbyte-case-study) — *vendor case study, not fetched*
30. **[S30]** [Workflow Streams: live interactivity for agents](https://temporal.io/blog/workflow-streams-live-interactivity-agents-other-applications) — *vendor blog, not fetched*

### Temporal: Observability, testing and DX

31. **[S31]** [Temporal Web UI](https://docs.temporal.io/web-ui)
32. **[S32]** [Observability - Python SDK](https://docs.temporal.io/develop/python/platform/observability)
33. **[S33]** [Observability - Go SDK](https://docs.temporal.io/develop/go/observability) — *not fetched*
34. **[S34]** [Observability - Java SDK](https://docs.temporal.io/develop/java/observability) — *not fetched*
35. **[S35]** [Observability - TypeScript SDK](https://docs.temporal.io/develop/typescript/platform/observability) — *not fetched*
36. **[S36]** [Observability - .NET SDK](https://docs.temporal.io/develop/dotnet/platform/observability) — *not fetched*
37. **[S37]** [Observability - Ruby SDK](https://docs.temporal.io/develop/ruby/platform/observability) — *not fetched*
38. **[S38]** [Search Attributes](https://docs.temporal.io/search-attribute) — *not fetched*
39. **[S39]** [Temporal CLI server command reference](https://docs.temporal.io/cli/server)
40. **[S40]** [Run a development server](https://docs.temporal.io/develop/run-a-development-server) — *not fetched*
41. **[S41]** [Testing - Python SDK](https://docs.temporal.io/develop/python/testing-suite)
42. **[S42]** [Testing - TypeScript SDK](https://docs.temporal.io/develop/typescript/testing-suite) — *not fetched*
43. **[S43]** [Pre-production testing best practices](https://docs.temporal.io/best-practices/pre-production-testing) — *not fetched*
44. **[S44]** [Safely deploying changes to Workflow code](https://docs.temporal.io/develop/safe-deployments)

### Temporal: Security

45. **[S45]** [Temporal Platform security features](https://docs.temporal.io/self-hosted-guide/security)
46. **[S46]** [Codecs and Encryption](https://docs.temporal.io/production-deployment/data-encryption) — *not fetched*
47. **[S47]** [Codec Server](https://docs.temporal.io/codec-server) — *not fetched*

### Temporal: Licensing, releases and community

48. **[S48]** [temporalio/temporal (GitHub)](https://github.com/temporalio/temporal) — *GitHub HTML scraped*
49. **[S49]** [temporalio/temporal releases](https://github.com/temporalio/temporal/releases) — *GitHub HTML scraped*
50. **[S50]** [Temporal changelog](https://temporal.io/changelog)
51. **[S51]** [Ruby SDK Public Preview (changelog entry)](https://temporal.io/change-log/ruby-sdk-public-preview) — *search-result title only, not fetched*

## Dagster


### Dagster: Concepts and architecture

52. **[S52]** [OSS deployment architecture](https://dagster.io/docs/deployment/oss/oss-deployment-architecture)
53. **[S53]** [dagster.yaml reference](https://dagster.io/docs/deployment/oss/dagster-yaml)
54. **[S54]** [Instance configuration](https://dagster.io/docs/deployment/oss/oss-instance-configuration)
55. **[S55]** [Run coordinators](https://dagster.io/docs/deployment/execution/run-coordinators)
56. **[S56]** [Run executors](https://dagster.io/docs/guides/operate/run-executors)

### Dagster: Programming model and automation

57. **[S57]** [Components](https://dagster.io/docs/guides/build/components)
58. **[S58]** [Op retries](https://dagster.io/docs/guides/build/ops/op-retries)
59. **[S59]** [Run retries](https://dagster.io/docs/deployment/execution/run-retries)
60. **[S60]** [Declarative Automation](https://dagster.io/docs/guides/automate/declarative-automation)
61. **[S61]** [Sensors](https://dagster.io/docs/guides/automate/sensors)
62. **[S62]** [Troubleshooting sensor timeouts](https://dagster.io/docs/deployment/troubleshooting/sensor-timeouts)
63. **[S63]** [Managing concurrency](https://dagster.io/docs/guides/operate/managing-concurrency)

### Dagster: Partitions and backfills

64. **[S64]** [Backfilling data](https://dagster.io/docs/guides/build/partitions-and-backfills/backfilling-data)
65. **[S65]** [Partition backfill strategies](https://dagster.io/docs/examples/best-practices/partition-backfill-strategies)

### Dagster: Integrations and ingestion

66. **[S66]** [Dagster Pipes (guide)](https://dagster.io/docs/guides/build/external-pipelines)
67. **[S67]** [External pipelines (Pipes) integrations](https://dagster.io/docs/integrations/external-pipelines)
68. **[S68]** [Dagster integrations index](https://dagster.io/docs/integrations/libraries)
69. **[S69]** [dlt integration](https://dagster.io/docs/integrations/libraries/dlt)
70. **[S70]** [dlt integration (Pythonic)](https://dagster.io/docs/integrations/libraries/dlt/dlt-pythonic)
71. **[S71]** [Sling integration](https://dagster.io/docs/integrations/libraries/sling)
72. **[S72]** [Embedded ELT (deprecated)](https://dagster.io/docs/integrations/libraries/embedded-elt)
73. **[S73]** [Airbyte integration](https://dagster.io/docs/integrations/libraries/airbyte)
74. **[S74]** [Airbyte component](https://dagster.io/docs/integrations/libraries/airbyte/airbyte-component)
75. **[S75]** [Fivetran integration](https://dagster.io/docs/integrations/libraries/fivetran)
76. **[S76]** [Meltano integration](https://dagster.io/docs/integrations/libraries/meltano)
77. **[S77]** [dbt integration](https://dagster.io/docs/integrations/libraries/dbt)

### Dagster: Observability, testing and DX

78. **[S78]** [Dagster webserver and UI](https://dagster.io/docs/guides/operate/webserver)
79. **[S79]** [Observe](https://dagster.io/docs/guides/observe)
80. **[S80]** [Alerts (Dagster+)](https://docs.dagster.io/guides/observe/alerts) — *not fetched*
81. **[S81]** [Insights (Dagster+)](https://docs.dagster.io/guides/observe/insights) — *not fetched*
82. **[S82]** [Unit testing assets and ops](https://dagster.io/docs/guides/test/unit-testing-assets-and-ops)
83. **[S83]** [Testing assets with asset checks](https://dagster.io/docs/guides/test/asset-checks)
84. **[S84]** [Execution API reference](https://docs.dagster.io/api/dagster/execution) — *not fetched*
85. **[S85]** [Ops API reference](https://docs.dagster.io/api/dagster/ops) — *not fetched*
86. **[S86]** [Running Dagster locally](https://dagster.io/docs/deployment/oss/deployment-options/running-dagster-locally)
87. **[S87]** [Dagster CLI API reference](https://docs.dagster.io/api/clis/cli) — *not fetched*

### Dagster: Deployment

88. **[S88]** [Deploying to Kubernetes](https://dagster.io/docs/deployment/oss/deployment-options/kubernetes/deploying-to-kubernetes)

### Dagster: Security

89. **[S89]** [GitHub Discussion #23851 (maintainer answer)](https://github.com/dagster-io/dagster/discussions/23851)
90. **[S90]** [Dagster+ RBAC user roles and permissions (docs source)](https://github.com/dagster-io/dagster/blob/master/docs/docs/deployment/dagster-plus/authentication-and-access-control/rbac/user-roles-permissions.md) — *not fetched*
91. **[S91]** [maltzsama/dagster-authkit](https://github.com/maltzsama/dagster-authkit) — *third-party, not evaluated*
92. **[S92]** [stianthaulow/dagster-auth](https://github.com/stianthaulow/dagster-auth) — *third-party, not evaluated*

### Dagster: Licensing, releases and community

93. **[S93]** [dagster-io/dagster (GitHub)](https://github.com/dagster-io/dagster) — *GitHub HTML scraped*
94. **[S94]** [dagster-io/dagster releases](https://github.com/dagster-io/dagster/releases) — *GitHub HTML scraped*
95. **[S95]** [PyPI dagster JSON](https://pypi.org/pypi/dagster/json)
96. **[S96]** [Prefect is acquiring Dagster](https://dagster.io/blog/prefect-is-acquiring-dagster) — *vendor blog*
97. **[S97]** [Dagster Customer FAQ](https://dagster.io/dagster-customer-faq) — *vendor FAQ*
98. **[S98]** [Prefect expands open source AI stack with Dagster acquisition](https://opensourceforu.com/2026/07/prefect-expands-open-source-ai-stack-with-dagster-acquisition/) — *third-party press, not fetched*
99. **[S99]** [Prefect to acquire Dagster Labs](https://techstrong.ai/features/prefect-to-acquire-dagster-labs-to-drive-ai-workflow-automation/) — *third-party press, not fetched*

### Dagster: Blogs and positioning

100. **[S100]** [Why We Built Dagster](https://dagster.io/blog/dagster-the-data-orchestrator) — *vendor blog*
101. **[S101]** [Dynamic partitioning](https://dagster.io/blog/dynamic-partitioning) — *vendor blog*
102. **[S102]** [How does Dagster compare to Flyte and Temporal (community forum)](https://discuss.dagster.io/t/22772686/how-does-dagster-compare-to-flyte-and-temporal) — *third-party, AI-generated answer, not authoritative*

