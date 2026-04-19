---
title: "GTM Strategy & Launch Goals - Standalone Activities"
notion_url: "https://www.notion.so/2f58fc5677388088a8d3dbfee5e71050"
last_edited: "2026-02-11T00:09:24.202Z"
page_type: strategy
---

# Core Strategy: Celery & Redis-Based Job Queue Displacement
The GTM strategy centers on displacing legacy job queue systems—primarily Celery and other Redis-based solutions—with Standalone Activities. This approach targets organizations struggling with durability issues, operational complexity, and infrastructure costs inherent in traditional job queue architectures.

👉 See also: Job Queue Competitors (ranked)
### Primary Competitive Targets
1. **Celery & Redis-Based Job Queues (Highest Priority)**
Celery represents the most attractive takeout opportunity due to:
- **Higher infrastructure costs** - Redis in-memory architecture requires significant memory allocation
- **Durability vulnerabilities** - In-memory storage creates inherent data loss risks during crashes
- **Operational burden** - Teams must operate Celery as a service, developer burden
- **Silent failure modes** - Jobs can disappear without trace during infrastructure events

**2. Faktory & similar Job Queues (Secondary Priority)**
While Faktory addresses some Redis limitations through RocksDB persistence in its Enterprise tier, it presents scaling and operational challenges:
- **Lower cost baseline** - Makes pure cost arbitrage more difficult
- **Operational complexity at scale** - Coinbase has documented significant ops and scaling issues
- **Limited enterprise features** - Missing fairness scheduling, lifecycle management, and advanced observability
- Coinbase serves as a critical proof point: their documented frustrations with Faktory's scaling limitations align perfectly with Standalone Activities' value proposition.

**3. AWS Durable Lambda Functions (Opportunistic)** - Potential wins against AWS Durable Lambda exist, but require more market signal and competitive intelligence before aggressive positioning - it's easier for them to win if customers are already using Lambda and are all-in on AWS unless they also value the clean upgrade path to full Workflow orchestration.
### Market Segmentation
**Ideal Customer Profile**
- Backend engineering teams running 10,000+ jobs/day
- Organizations using Celery, Faktory, Sidekiq, or custom SQS-based systems
- Teams experiencing job queue loss, deployment complexity, or fairness issues
- Companies evaluating orchestration but blocked by complexity/cost

**Current Interested Companies** (18 total as of Jan 27 2026)
Rippling, Stripe, Yubi (Credavenue), Verkada, Justworks, Block, Snap, Datacom, Cursor, Roblox, Square, Relativity Technologies, Xero, Coinbase, Yum Brands, SoFi, Apollo Global Management, CellPoint Digital
## Adoption Barriers & Requested Feature Enhancements
Research indicates some customers require additional Standalone Activity features before they can adopt:
- **Schedules integration** - Recurring job execution without external cron
- **Start delay** - Native delayed job execution (common in job queue workflows)
- **Export capabilities** - Job history and audit log extraction
- **Lower Cost** - Disable Visibility (e.g. Coinbase) but Temporal relies on it for replication today.

These requirements are being tracked through customer conversations and will inform Public Preview and GA feature prioritization.

---
## Goals & Success Metrics
### Pre-Release Goals
**Summary**
- **18 → 30 interested customers** - grow pipe
- **50% of interested customers** - product market fit
	- could replace legacy job queues with Temporal
	- price point is in the right ballpark
	- pre-release enabled & Cloud usage ≥ 1
- **3 high profile customers → early adopters**
- **1 high profile customer → reference @ Replay**
- **Identify & prioritize adoption blockers**

**Validate customers could replace legacy job queues (Celery, …) with Standalone Activities**
- Hypothesis: Standalone Activities will enable us to takeout legacy job queues like Celery, Sidekiq, Faktory, and others.
- Target: 50% of customer could replace their legacy job queue with Standalone Activities
	- Target: Refine takeout play messaging & GTM based on our learnings.

**Validate the price point is in the right ballpark: 1 action to start + storage**
- Target: 50% of interested companies (9 of 18) confirm willingness to pay for current GA feature set (or with select enhancements, for example: start delay, start from schedule, …)
- Method: Direct customer conversations, field feedback sessions, pre-release engagement
- Timeline: Complete validation before Public Preview announcement (End-Mar 2026)

**Cloud Usage from 50% of interested companies**
- ≥ 1 Standalone Activities for 50% of interested companies
- ≥ 1000 Standalone Activities for 3 interested companies
- Track usage per account/namespace through internal Cloud metrics
- Using a suffixed metering type (`grpc:<grpcMethod>.StandaloneActivity`)
	- in particular `grpc:StartActivityExecution.StandaloneActivity`

**Nurture Early Adopters → Lighthouse Customer in Prod for Replay Launch**
- White glove treatment for select high profile customers
- Early candidate list: Block, Rippling, Coinbase, …
- Convert 1 high profile customer into a lighthouse customer for Replay launch

**Identify & Prioritize Adoption Blockers**
- Catalog feature gaps preventing purchase decisions
- Quantify how many prospects are blocked by each missing capability
- Create prioritized roadmap for Public Preview and GA features
- Determine if pricing adjustments are needed for specific segments

**Expand Prospect Pipeline**
- Grow interested company list from 18 to 30+ qualified prospects
- Focus on legacy Job queues (esp. Celery/Redis) with documented pain points
- Engage existing customer base under NDA ahead of Replay Public Preview Launch
### Public Preview Goals (Replay 2026)
**Activation & Validation**
- 5+ companies running production trials
- 1+ reference customers willing to discuss results publicly (ideally on stage at Replay)
- Validate pricing model
- Collect detailed competitive win/loss analysis (Coinbase, etc.)

**Product-Market Fit Indicators**
- Customers can migrate easily
- 60%+ of usage continue past 30 days
- Net Promoter Score > 8 among active users
### GA Goals (+6 months)
**Revenue Targets (+6 months after GA)**
- 20+ paying customers
- $5M ARR (~$420K MRR; ~550M actions/day) from Standalone Activities

**Market Position (+6 months after GA)**
- Recognized as primary Celery alternative in target communities
- Featured in 5+ migration case studies
- Developer advocacy presence in Python, Go, TypeScript job queue communities

---
## Next Steps & Immediate Actions
### Pre-Release Enablement (Now → End-March 2026)
**Customer Development**
- Get feedback from all interested companies
	- Pricing feedback - willingness to pay?
	- Adoption blockers & missing features?
	- Existing legacy job systems that can be replaced?
	- New use cases this unlocks?
- Early access customers validate pre-release in dev/test environments + feedback

**Market Expansion**
- Identify 12+ additional qualified prospects from existing customer base via Field
- Develop targeted content for top job queue alternatives
- Competitive displacement playbook

**More Customer Signal on Requested Enhancements To Make a Biz Case**
- **Schedule** a Standalone Activity (Cursor)
- **Start delay** to start an activity with a time delay / offset (Block, Coinbase)
- **Export** for auditing & compliance (Block)
- **Disable Visibility** for lower cost (Coinbase)
- Add to expanded GA feature set via follow-on 1-pagers, based on customer signal.
### Success Criteria for Public Preview Launch
Before announcing Public Preview, achieve:
- Pricing model validated against customer usage projections
- 9+ companies confirmed willing to pay at target pricing
- Approved 1-pagers to close missing feature gaps ahead of Replay launch
- Sales/SA enablement complete with battle cards and demos
### Success Criteria for GA Launch
- Key feature gaps closed as part of an expanded GA release
- Migration guides for top takeout plays
