---
title: "[1-pager] Standalone Activity: Start from Schedule"
notion_url: "https://www.notion.so/3308fc567738805eb566e278f2df5634"
last_edited: "2026-04-03T17:51:27.496Z"
page_type: spec
---

**Status:** 1-pager Approved | **Driver:** Phil Prasek
**Parent feature:** [Standalone Activity PRD](https://www.notion.so/temporalio/1ee8fc567738806d8b6fe8e2eeae0fc4)

> **Compound blocker note:** Cursor and Anysphere need Schedule + TS SDK (compound hard blocker). Coinbase needs Schedule + Start Delay for full migration. Stripe needs Schedule + Export + Start Delay + Ruby/Java SDK. See also: SAA Start Delay 1-pager.

## **Problem Statement**
### **The Job-to-be-Done**
Replace cron jobs and scheduled batch operations with durable standalone activities triggered on a recurring schedule. Users want `Schedule -> Activity` without wrapping in a throwaway workflow. The wrapper workflow adds complexity, boilerplate, and defeats the simplicity value prop that makes SAA attractive in the first place.
### **Initial Evidence**
- **Cursor** (Owner: Conrad Vogel, SA: Josh Smith) - TS 91%, 2.4B ops. "Only use case we have, will hold off" until Schedule support ships. Hard blocker combined with TS SDK. "Contingent on schedule support." Hard blocker combined with TS SDK.
- **Apollo Global** (Owner: Andrew Eidenshink, SA: Steve Womack) - Python 72%. "Batch jobs, schedule stored proc invocations." Partial blocker. "Asked for literally all of them including production."
- **Coinbase** (Owner: Collin Cook, SA: Taylor Khan) - Go 100%. ~9 namespaces use WithScheduled patterns. Some use cases need scheduled triggers. Partial blocker.
- **Stripe** (Owner: Conrad Vogel, SA: Jimmy Jea) - 76.0B ops total (Ruby 51%, Java 41%). Primary SDKs are Ruby/Java (neither has SAA support yet). Scheduled job patterns. Partial blocker (also blocked on Ruby/Java SDK + Export + Start Delay + Rate limits).
- **Indeed** (SA: Devin Spencer) - Java (iWF framework). "Standalone activities could also fit, though the first use cases that came to me would need the scheduled start extension." Partial blocker - schedule support gates their initial SAA adoption.
- **The Motley Fool** (Prospect) - Evaluating Temporal specifically because of "Issues with Current Infrastructure for Cron Jobs." Schedule-capable SAA directly addresses their evaluation driver.
### **Who and How Many**
- **6 accounts** explicitly blocked or partially blocked - widest blocker by account count across all SAA feature gaps
- **2 hard blockers** (Cursor, Anysphere) - won't adopt SAA without this
- **4 partial blockers** (Apollo Global, Coinbase, Stripe, Indeed) - can use SAA for some use cases but need Schedule for full adoption
- **1 prospect** (The Motley Fool) evaluating Temporal specifically for cron infrastructure replacement
- **Upcoming signal:** EY and Adyen have Replay meetings scheduled with the SAA/Schedule EM - may add to the blocker count
- Segments: AI Native (Cursor, Anysphere), Digital Native (Coinbase, Stripe), G2K / FinServ (Apollo Global, Indeed)
- Combined ops volume of blocked accounts: ~78.4B ops/month (Stripe 76B, Cursor 2.4B)
## **Impact Classification**
- **Bucket:** Core Loop / User Happiness
- **Rationale:** Scheduled execution is a fundamental durable execution pattern. Without it, SAA can't serve the "cron job replacement" use case - one of the most common entry points to Temporal. This directly blocks the SAA value prop of being the simplest path to durable execution.
- **Conviction level:** High - 6 accounts named it unprompted (plus 1 prospect and 2 pending Replay meetings), 2 won't adopt without it, and recurring task execution is a universal pattern across all segments. Long-standing community demand validated by [GitHub #130](https://github.com/temporalio/temporal/issues/130) (open since Feb 2020, "Add cron activity").
## **Strategic Alignment**
- **Target Segment:** Cross-segment (AI Native, Digital Native, G2K / FinServ)
- **FY27 Objectives:**
	- **Primary:** Simplify the End-to-End Temporal Experience - eliminates the need for wrapper workflows, reducing boilerplate and steps to get a scheduled durable task running
	- **Secondary:** Sharpen & Scale Temporal Use Cases - "simple tasks" (email, SMS, notifications) and "data orchestration" are top priority use cases, many of which run on schedules
- **Use Case:** Simple tasks (cron replacement), data orchestration (batch processing), service management (scheduled maintenance)
## **High-Level Solution**
### **Concept**
Extend the existing Temporal Schedule primitive to accept a standalone activity as a target action, alongside the existing workflow target. When a Schedule fires, the server creates a standalone activity execution directly - no intermediate workflow. The Schedule configuration (interval, cron expression, overlap policy, catchup window, pause/unpause) applies unchanged.
**Required Teams**

| Team | Scope | Notes |
|---|---|---|
| ACT | Schedule action dispatch for standalone activities | Core implementation - new action type in Schedule |
| Server (History) | SA execution creation from Schedule trigger | May reuse existing SA start path |
| SDKs (Go, Python, TS) | Expose ActivityAction in Schedule APIs | Go + Python for GA; TS unblocks Cursor/Anysphere |
| Cloud & Cloud UI | Namespace-level enablement if gated | Dynamic config in pre-release |

### **Rough LOE**
**Small-Medium** - The Schedule infrastructure already exists and handles action dispatch. The core work is adding a new action type (activity alongside workflow) in the server's Schedule executor. SDK changes are thin - add a new action variant to existing Schedule APIs. No new UI needed (Cloud UI Schedule views already support multiple action types conceptually). Estimate: 2-4 weeks server + 1-2 weeks per SDK.
## **Key Risks & Open Questions**

| Risk/Question | Why It Matters | How to Resolve |
|---|---|---|
| Overlap policy for long-running activities | Schedule overlap policies (skip, buffer, cancel, terminate) assume workflow semantics. Do they all apply cleanly to standalone activities? | Design review with server team. At minimum: skip and terminate. |
| Retry behavior interaction | SA has its own retry policy. Schedule has catch-up and backfill semantics. How do these compose? | Define: Schedule triggers = new SA execution. SA retry policy governs retries within a single trigger. |
| Pause/resume and backfill | Users expect Schedule pause/resume and backfill to work with SA targets. Any gaps? | Verify existing Schedule features work with new action type. |
| TS SDK dependency | 2 of 5 blocked accounts (Cursor, Anysphere) also need TS SDK. Schedule alone won't unblock them. | Track as compound blocker. TS SDK is a separate workstream. |
| Metering and billing | How are schedule-triggered SA executions billed? Same as manually started SA? | Align with metering team. Should be identical to non-scheduled SA. |

## **Recommendation**
**Pursue** - Ship as part of SAA GA (H2 2026). This is the widest blocker by account count, spans all target segments, and the technical risk is low given existing Schedule infrastructure. Prioritize Go + Python first (unblocks Apollo Global, Coinbase), then TS (unblocks Cursor, Anysphere pending TS SDK). Stripe is multi-blocked and will come online later regardless.
