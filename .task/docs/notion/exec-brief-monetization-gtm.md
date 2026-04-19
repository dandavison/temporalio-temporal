---
title: "Standalone Activities — Exec Brief (Monetization + GTM)"
notion_url: "https://www.notion.so/3118fc56773880ca91edf884f8d621b9"
last_edited: "2026-04-12T17:28:40.327Z"
page_type: strategy
---

**One-liner**: Standalone Activities lets customers run a single Temporal Activity as a first-class, durable job (no wrapper Workflow), with Temporal's reliability, fairness, and lifecycle controls — at a lower action cost than single-Activity Workflows.

TLDR Recommendation: Charge 1 Action Per Standalone Activity

| Milestone | Target | Notes |
|---|---|---|
| Pre-release | Late Feb 2026 | Go + Python |
| Public Preview | May 2026 | Billable actions begin |
| GA | H2 2026 | Broader platform + GTM readiness |

---
## Feature Objective/Goals
- PRD (see prd-standalone-activities-for-durable-job-processing.md)
- GTM Hub: Standalone Activities

## Feature summary + value proposition

| Question | Answer |
|---|---|
| What is this? | Temporal Activities can run on their own (not just as steps inside a Workflow). This enables Temporal for single-step jobs like notifications, webhooks, file processing, and data sync. |
| What does it do? | Durably persists jobs, automatically retries, and fairly schedules under load. Includes lifecycle controls (pause, cancel, reset) and visibility across UI, CLI, SDK, and API. No infrastructure to build or manage. |
| Why does it matter? | Teams stop losing jobs in production, avoid building and operating job infrastructure, and get one platform for single-step jobs today and multi-step orchestration tomorrow (reuse the same Activity code). |
| Why now? | Pre-release late Feb 2026 (Go + Python). Also building a pipeline of early adopters with a goal of a customer on stage at Replay. |

#### Why are we building this?
- Opens an adjacent buyer persona: backend engineers and platform teams running jobs with Celery, Faktory, Sidekiq, SQS, or custom systems.
- Many high-volume use cases are not economic on Temporal today with full Workflows.
- This is a strong onramp: Jobs → upgrade to full workflow orchestration later.
#### Desired long-term outcomes
- Become the default platform for "all async backend things" across jobs + orchestration.
- Win competitive takeouts from legacy job queues at similar price points.
- Reduce fragmentation for customers that currently need multiple tools for async workloads.
#### Value add vs. table stakes
- **Category call:** Premium Feature (large need, high desirability).
	- Many teams *need* reliable job execution.
	- The differentiated value is in fairness, lifecycle controls, and managed durability at scale.
---
## Key Context
**THE PLAY: GO FIND LEGACY JOB QUEUES TO TAKE OUT**
*Differentiate at the same price point.*

**Customer signal on "fewer tools"**
> "At Block there's a **strong desire to move to fewer tools** and if Temporal can't solve all async use cases, then detractors start positioning it as a niche solution — and not THE answer."
— Nick Esposito, Block

> "We could simplify our tools to have Temporal for Workflows and Jobs (Standalone Activities) instead of adding a new job processing framework."

> "A two birds one stone situation. Currently for rails we use Sidekiq + Redis and we already have to migrate off of Sidekiq."
— Josh Dunigan, Justworks

---
## Addressable Market
- Market sizing: **Global TAM (async task processing): ~$7.5B/year (2025) with 16% CAGR**
#### Who is the target market/segment?

| Segment | Description |
|---|---|
| Core | Teams with an existing job queue (Celery, Faktory, Sidekiq, SQS, custom systems) |
| Startups | Simple async workloads, want reliability without building infra |
| Mid-market | Scaling job volume, want fairness + operational simplicity |
| Enterprise | Ultra-high-volume background processing, multi-tenant concerns, operational rigor |

#### Internal demand (existing customers)
- Evidence of interest and takeout targets: Interested customers: 21 and growing

#### External demand (broader market)
- Large incumbent ecosystem of OSS + cloud primitives where teams assemble reliability themselves.
- Adjacent market: job/task queues with clear expansion potential beyond current "workflow-first" buyers.

---
## Value and Willingness to Pay
#### What use cases does this unlock?

| Use case cluster | Examples | Value delivered |
|---|---|---|
| Platform & Infra Ops | CI/CD automation, background jobs, search indexing, analytics, cron | Durability + retries without building a job platform |
| Notifications & Comms | Email, push, alerts, customer messaging | Guaranteed delivery and operational visibility |
| Integrations & Data Sync | Webhooks, unreliable APIs, partner sync | Durable execution semantics with lifecycle control |
| Automation | Single reliable function execution, handlers | Simple jobs now, reuse code for workflows later |

#### Willingness to pay and demand: want vs. need
- Strong early WTP signal:
	- Rippling: `... 15 dollars per 1,000,000 action ... very reasonable ...`
- Demand characterized as a **need**, especially for larger enterprise customers (reliability + cost + consolidation of tools).
#### Build-it-yourself cost (customer alternative)
- Build your own reliable Job Queue from scratch
	- Scope: Standalone Activities + full Activity semantics + heartbeats/checkpointing (single region)
	- Build effort: ~80–115 eng-months
	- One-time software build: ~$1.5M–$2.2M (excludes infra + SRE)
	- Timeline: ~10–14 months with 8 engineers (or longer with fewer)

---
## Competitive Environment
### Standalone Activities vs. Celery & Faktory (feature-level)

| Dimension | Temporal | Celery (popular job queue) | Faktory (from Sidekiq creator) |
|---|---|---|---|
| Job primitive | Standalone Activity | Task | Job |
| Durability | ✅ Highly reliable: Cloud WAL + DB | ⚠️ Job loss possible (Redis in-memory) | ⚠️ Job loss possible (customer observed with Enterprise) |
| Ops overhead | ✅ Fully-managed Cloud service (or OSS) | ❌ Dedicated SRE, complex | ❌ Tier 0 service with ongoing maintenance |
| Scalability | ✅ High-throughput | ⚠️ Head of line blocking | ❌ Single central server, manual partitioning |
| Scheduling | ✅ Priority & Fairness (anti-starvation) | ⚠️ Priority only, starvation risk | ⚠️ Priority only via queue ordering |
| Deduplication | ✅ Explicit conflict & reuse policies | ⚠️ Narrow, conditional | ❌ App-level only |
| Long-running jobs | ✅ Supported with optional checkpoints | ⚠️ Hard to run mixed jobs fairly | ⚠️ NO durable checkpointing |
| Lifecycle controls | ✅ Full control (cancel, pause, reset, …) | ⚠️ Limited (revoke, inspect) | ⚠️ Limited (kill, retry) |
| Results & errors | ✅ Rich visibility | ⚠️ Optional result backend | ⚠️ Limited UI per central server |
| Manual completion | ✅ Native (token, ID) | ❌ Custom patterns | ❌ Custom patterns |
| Upgrade to Workflows | ✅ Reuse Activities as Workflow steps | ❌ Separate tool | ❌ Separate tool |

#### Build it yourself comparison (300M jobs/day)

| Solution (Config) | Infra cost (year) | Ops cost (year) | Total (year) | Total (day) |
|---|---|---|---|---|
| Temporal Standalone Activities | $0 | $0 | $2.33M | $6.37k |
| Celery – RabbitMQ (Broker) + PostgreSQL (Results) | $500k – $785k | $1.35M – $2.1M | $1.85M – $2.9M | $5.1k – $8.0k |
| Celery – SQS (Broker) + DynamoDB (Results) | $470k – $815k | $1.35M – $2.05M | $1.82M – $2.86M | $5.0k – $7.8k |
| Celery – Redis (Broker) + Redis (Results) | $400k – $635k | $1.35M – $2.1M | $1.75M – $2.7M | $4.8k – $7.4k |
| Faktory Enterprise (Standard Config, RocksDB) | $500k – $830k | $1.0M – $1.75M | $1.5M – $2.6M | $4.1k – $7.1k |
| Faktory Enterprise (Budget Config, RocksDB) | $270k – $390k | $630k – $1.05M | $900k – $1.44M | $2.5k – $3.9k |

---
## Positioning
**Packaging / pricing structure**
- Recommendation: align with existing Activity pricing.
- Principle: keep the mental model consistent for developers (Activities are billed consistently whether invoked from a Workflow or standalone).

**Ease of understanding paid concept**
- Simple story: no wrapper Workflow means fewer billable actions.

---
## Cost
- **TL;DR:** Standalone Activity (SAA) is **cheaper to run than** a Single-Activity Workflow (SAW) on a per-execution basis (~50-60% cheaper).
- Bill **1 Action for SAA vs 2 Actions for SAW results in the same or slightly better margins (+1-7PP)**

### Summary of Details
## What we're comparing
- **SAW (Single-Activity Workflow)**: workflow start + schedule 1 activity → **2 Actions** in the simplest happy path.
- **SAA (Standalone Activity)**: start a standalone activity → assumed **1 Action** in the simplest happy path.
## Core result
- SAA is roughly **~50%–60% cheaper** than SAW per execution.
## Why SAA is cheaper
SAA avoids the "workflow worker detour" and reduces persistence + RPC overhead in the happy path:
- Fewer persistence operations, especially in **Astra/Cassandra** and **WAL**.
- Fewer **visibility writes** (or roughly similar, making visibility a limiter on relative savings).
- Lower **payload egress** (roughly half in the simple model).
## Margin implication
If we price **SAA at 1 Action** and **SAW at 2 Actions**, then SAA maintains or improves margins
- SAA margins are **at least as good** as SAW, and likely **slightly better**.
- Example from the analysis: if SAW gross margin were **70%**, SAA would be about **~71%–77%**.

#### PRD Cost Estimates
**Key cost driver**
- DB writes (with > 60% reduction in base and worst cases vs. Workflow + single Activity).

| Cost model | WF + single activity | Standalone Activity | Cost reduction | Cost reduction % |
|---|---|---|---|---|
| Base DB writes | 15 | 4 | 11 | 73.33% |
| Base visibility | 3 | 3 | 0 | 0.00% |
| Worst case | 3 | 1 | 2 | 66.67% |
| **Total base case** | **18** | **7** | **11** | **61.11%** |
| **Total worst case** | **21** | **8** | **13** | **61.90%** |

---
## Pricing recommendation (developer-facing detail)
**Recommendation:** **Same as existing Activity pricing, but now Activities can be invoked standalone.**

**Actions that count**
- **Activity started or retried** (each start/retry counts). De-duplicated starts sharing an Activity ID do **not** count as an Action.
- **Activity Heartbeat recorded** counts as an Action only if it reaches the Temporal Server. SDKs throttle heartbeats (default throttle is 80% of the Heartbeat Timeout). Heartbeats do not apply to Local Activities.

**Storage model**
- Existing storage pricing model applies:
	- Standalone Activity Execution state is Open (Active Storage) or Closed (Retained Storage).
	- Only mutable state contributes to storage usage (no Workflow event history).

**Pricing + account impact**
Same **Activity pricing** — fewer actions
No wrapper Workflow: **2 actions → 1 action** (excludes retries + heartbeats)

**Significant upside**
- Unlocks new cost-sensitive, high-volume use cases
- Enables legacy job queue takeouts

**Limited downside risk**
- Only ~3% of current action volume comes from single-Activity Workflows
- Impacted only if customers choose to migrate

---
## Metering and Reporting
#### Metering
- Pre-release (Feb 28):
	- `non-billable` action
	- metering subtype: `grpc:StartActivityExecution.StandaloneActivity`
- Public preview (May 5 2026):
	- `billable` action
	- metering subtype: `grpc:StartActivityExecution.StandaloneActivity`

---
## Appendix: Road to Replay (pre-release goals)
- Current status: 18 interested customers
- Target number: 30 interested customers
- Product market fit: 50% of interested customers
- Early adopters: 3 high profile customers
- Reference @ Replay: 1 high profile customer

**Product market fit definition**
- Could replace legacy job queues with Temporal
- Price point is in the right ballpark
- Pre-release enabled and cloud usage ≥ 1

SALES: IDENTIFY ADOPTION BLOCKERS SO PRODUCT CAN PRIORITIZE
