# Dagster → Temporal migration plan

> **Key takeaways**
> - This folder plans the move of ~50 ingestion jobs (Meltano or dlt → Snowflake `RAW`, then dbt → `CLEAN` → `SEMANTIC`) from self-hosted Dagster on Kubernetes to self-hosted Temporal.
> - It separates what is **already verified** (no POC needed) from what is **unknown** (POC needed). Each unknown maps to exactly one POC with pass/fail criteria.
> - The three highest risks are: per-job resource isolation (Temporal has no per-run pod), dbt triggering for models that join several sources (Temporal has no asset graph), and ELT tool state/duplicates under Temporal's retry model.
> - Several items depend on answers from the team. They are marked **Q#** and listed in [01](01-current-state-and-questions.md). Until answered, the plan uses the assumptions listed there.

As of 2026-11-11. Builds on the comparison in [`docs/`](../docs/) and [`comparison-matrix.md`](../comparison-matrix.md). Sources use the same numbering as [`references/sources.md`](../references/sources.md) (S118 onward were added for this plan).

## Reading order

| # | Doc | What it answers |
|---|---|---|
| 01 | [Current state and questions](01-current-state-and-questions.md) | What we know about today's setup; open questions; working assumptions |
| 02 | [Feature mapping](02-feature-mapping.md) | Each Dagster feature in use → Temporal equivalent → **Known** or **Unknown** |
| 03 | [Unknowns register](03-unknowns-register.md) | Every unknown or "might not work" item, with risk and the POC that closes it |
| 04 | [POC plan](04-poc-plan.md) | One card per POC: question, scope, steps, pass/fail, effort |
| 05 | [Migration plan](05-migration-plan.md) | Phases, cutover by dependency cluster, per-source runbook, rollback |

## Big picture

```mermaid
flowchart LR
  subgraph Today["Today: Dagster on K8s"]
    DS[Schedules / sensors] --> DR[K8sRunLauncher<br/>1 pod per run]
    DR --> ELT1[Meltano / dlt]
    ELT1 --> RAW1[(RAW)]
    RAW1 --> DBT1[dbt via asset graph]
    DBT1 --> CLEAN1[(CLEAN)] --> SEM1[(SEMANTIC)]
  end
  subgraph Target["Target: Temporal on K8s"]
    TS[Temporal Schedules] --> WF[Ingest Workflow<br/>per source]
    WF --> ACT[Activity: run Meltano / dlt<br/>in worker pod or K8s Job]
    ACT --> RAW2[(RAW)]
    WF -->|signal| DC[dbt coordinator Workflow]
    DC --> DBT2[Activity: dbt build<br/>subprocess]
    DBT2 --> CLEAN2[(CLEAN)] --> SEM2[(SEMANTIC)]
  end
  Today -. cut over by dependency cluster .-> Target
```

## Status legend used in this folder

| Label | Meaning |
|---|---|
| **Known** | Stated in official docs we fetched; cited inline. No POC. |
| **Unknown** | Not stated in docs, or depends on how our setup behaves. Needs a POC. |
| **Inference** | Our reasoning from docs, not a doc statement. Treated as unknown when it carries risk. |
| **Q#** | Needs an answer from the team (see [01](01-current-state-and-questions.md)). |
