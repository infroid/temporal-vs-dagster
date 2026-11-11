# 11 — Licensing and community

> **Key takeaways**
> - **Temporal Server: MIT**, ~23.7k stars, ~2.0k forks; latest server release **v1.32.1** (Oct 8) ([temporalio/temporal (GitHub)](https://github.com/temporalio/temporal), [temporalio/temporal releases](https://github.com/temporalio/temporal/releases)).
> - **Dagster: Apache-2.0**, ~16.1k stars, ~2.3k forks; latest **1.13.29** on PyPI (2026-11-05), roughly weekly-to-biweekly patches ([dagster-io/dagster (GitHub)](https://github.com/dagster-io/dagster), [PyPI dagster JSON](https://pypi.org/pypi/dagster/json)).
> - **Prefect announced it is acquiring Dagster Labs** (2026-07-13); OSS to be "actively developed", "no plans to merge Dagster into Prefect" ([Prefect is acquiring Dagster](https://dagster.io/blog/prefect-is-acquiring-dagster), [Dagster Customer FAQ](https://dagster.io/dagster-customer-faq)).
> - Temporal originated as a fork of Uber's Cadence by Cadence's creators ([temporalio/temporal (GitHub)](https://github.com/temporalio/temporal)).

## 1. Facts

| | Temporal | Dagster |
|---|---|---|
| Licence | MIT (server repo) ([temporalio/temporal (GitHub)](https://github.com/temporalio/temporal)) | Apache-2.0 ([dagster-io/dagster (GitHub)](https://github.com/dagster-io/dagster), [PyPI dagster JSON](https://pypi.org/pypi/dagster/json)) |
| SDK licences | **Not verified** | Libraries versioned in parallel as 0.29.x ([dagster-io/dagster releases](https://github.com/dagster-io/dagster/releases)); licences per library **Not verified** |
| Stars / forks (2026-11-11, scraped) | ~23.7k / ~2.0k ([temporalio/temporal (GitHub)](https://github.com/temporalio/temporal)) | ~16.1k / ~2.3k ([dagster-io/dagster (GitHub)](https://github.com/dagster-io/dagster)) |
| Contributors | **Not verified** | **Not verified** |
| Latest release | v1.32.1 (Oct 8; year inferred 2026); patches v1.31.4, v1.30.8 on Oct 12 ([temporalio/temporal releases](https://github.com/temporalio/temporal/releases)) | 1.13.29 (PyPI, 2026-11-05) ([PyPI dagster JSON](https://pypi.org/pypi/dagster/json)); GitHub marked 1.13.28 "Latest" at fetch time ([dagster-io/dagster releases](https://github.com/dagster-io/dagster/releases)) |
| Release cadence | Server patch releases across 3 minor lines ([temporalio/temporal releases](https://github.com/temporalio/temporal/releases)) | 1.13.24 → 1.13.29 between 2026-09-21 and 2026-11-05 ([PyPI dagster JSON](https://pypi.org/pypi/dagster/json)) |
| Maintainer | Temporal Technologies ([temporalio/temporal (GitHub)](https://github.com/temporalio/temporal)) | Dagster Labs, being acquired by Prefect ([Prefect is acquiring Dagster](https://dagster.io/blog/prefect-is-acquiring-dagster)) |
| Origin | Fork of Uber's Cadence ([temporalio/temporal (GitHub)](https://github.com/temporalio/temporal)) | — |
| Recent milestones | Rust SDK GA 2026-09-04; Standalone Activities GA 2026-09-15; Priority & Fairness GA 2026-05-05; External Storage preview 2026-05-14 ([Temporal changelog](https://temporal.io/changelog)) | ECS resumable run workers (1.13.25) ([dagster-io/dagster releases](https://github.com/dagster-io/dagster/releases)) |

## 2. Dagster acquisition details

From the vendor's own posts ([Prefect is acquiring Dagster](https://dagster.io/blog/prefect-is-acquiring-dagster), [Dagster Customer FAQ](https://dagster.io/dagster-customer-faq), *vendor blog/FAQ*):

- "Prefect is acquiring Dagster Labs"; about 40 staff join Prefect.
- Prefect acquires "Dagster's product, codebase, and customer relationships".
- "The open-source project will be actively developed, not just maintained."
- "We have no plans to merge Dagster into Prefect." Commercial offering "continues to operate as it does today".
- Founder Nick Schrock is "moving on from the project and company".
- At fetch time, dagster.io showed a "Dagster is now part of Prefect" banner ([Why We Built Dagster](https://dagster.io/blog/dagster-the-data-orchestrator)). Exact close date: **Not verified**.
- Third-party coverage exists but was not fetched ([Prefect expands open source AI stack with Dagster acquisition](https://opensourceforu.com/2026/07/prefect-expands-open-source-ai-stack-with-dagster-acquisition/), [Prefect to acquire Dagster Labs](https://techstrong.ai/features/prefect-to-acquire-dagster-labs-to-drive-ai-workflow-automation/)).

## 3. Risk view

*Judgement:*

- Both licences are permissive and allow forking; neither is source-available or copyleft.
- Dagster carries **ownership-change risk** in 2026: roadmap and governance under Prefect are not yet visible. The Apache-2.0 licence limits the downside.
- Temporal carries **single-vendor risk** (one company drives the server and SDKs), which is typical for commercial OSS.
- Neither number of stars nor release cadence says much about long-term fit; treat them as weak signals.

## Sources

- [S48] [temporalio/temporal (GitHub)](https://github.com/temporalio/temporal) — *GitHub HTML scraped*
- [S49] [temporalio/temporal releases](https://github.com/temporalio/temporal/releases) — *GitHub HTML scraped*
- [S50] [Temporal changelog](https://temporal.io/changelog)
- [S93] [dagster-io/dagster (GitHub)](https://github.com/dagster-io/dagster) — *GitHub HTML scraped*
- [S94] [dagster-io/dagster releases](https://github.com/dagster-io/dagster/releases) — *GitHub HTML scraped*
- [S95] [PyPI dagster JSON](https://pypi.org/pypi/dagster/json)
- [S96] [Prefect is acquiring Dagster](https://dagster.io/blog/prefect-is-acquiring-dagster) — *vendor blog*
- [S97] [Dagster Customer FAQ](https://dagster.io/dagster-customer-faq) — *vendor FAQ*
- [S98] [Prefect expands open source AI stack with Dagster acquisition](https://opensourceforu.com/2026/07/prefect-expands-open-source-ai-stack-with-dagster-acquisition/) — *third-party press, not fetched*
- [S99] [Prefect to acquire Dagster Labs](https://techstrong.ai/features/prefect-to-acquire-dagster-labs-to-drive-ai-workflow-automation/) — *third-party press, not fetched*
- [S100] [Why We Built Dagster](https://dagster.io/blog/dagster-the-data-orchestrator) — *vendor blog*

Full numbered list: [references/sources.md](../references/sources.md)
