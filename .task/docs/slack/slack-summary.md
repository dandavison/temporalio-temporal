# Slack discussions: Standalone Activities

## Metadata

- **Threads captured:** 17 substantive threads
- **Date range:** January 2026 – April 2026 (timestamps span approx. 2026-01-15 to 2026-04-19)
- **Channels covered:**
  - `#topic-standalone-activities` (C09EB1D10UD) — primary public channel; all SAA discussion forwarded here
  - `#crew-standalone-activities` (C08GZQN1C80) — internal engineering/PM working channel
  - `#topic-nexus` (C03SSH1GG0P) — Nexus integration discussions
  - `#competitive` / GTM channels (C01QJKD702X, C025MMBNTEV, C018F52DUKU) — sales/competitive intel
  - `#saas-standalone-activity-pre-release-enablement` (C0AGE0K2A0Z) — customer onboarding requests
  - Various DMs and alerts channels
- **Notes on coverage gaps:** Searches for "activity without workflow" and "job queue temporal" returned mostly noise. The pre-release enablement channel (C0AGE0K2A0Z) has no permalinks on its messages (bot-generated, structured form submissions). Many threads are forwarded from customer-facing channels that may not be directly accessible.

---

## Threads (ordered chronologically, newest first)

---

### 2026-04-19 — #topic-standalone-activities — Demo access for Replay
https://temporaltechnologies.slack.com/archives/C09EB1D10UD/p1776488078667919

Participants: U0A6UPZ4WKT (unknown), Dan Davison (U05GA2YGJKU)

Verbatim excerpt:
> **U0A6UPZ4WKT:** hey folks, @U09EU4L2DSL and I need to put together a demo of standalone activities for Replay, what's the least-friction way we can get access?
>
> **Dan Davison (U05GA2YGJKU):** Hi @U0A6UPZ4WKT. To work on the actual code and functionality for your demo, you should be able to follow the public Python, Go, and .NET SDK docs. They'll instruct you to install a version of the CLI (i.e. dev server) that supports SAA.
>
> If you need it enabled in a cloud namespace for your demo, then just let us know the namespace name.
>
> **Dan Davison:** Java/TS/Ruby/PHP aren't available yet.
>
> **U0A6UPZ4WKT:** thanks @U086A100WJU... pretty sure we're gonna use Python
>
> **Dan Davison:** Let us know of any friction -- however minor! -- in following the public docs.

---

### 2026-04-16 — #crew-standalone-activities — Product value framing for Replay talk
https://temporaltechnologies.slack.com/archives/C08GZQN1C80/p1775831976977299

Participants: Dan Davison (U05GA2YGJKU), Roey Berman (U01G8DXRA8Y), Phil Prasek (U06D9QF1DT3)

Verbatim excerpt:
> **Dan Davison:** The blog post and booth slides are great. So there are two sorts of consistency that SAA brings when added to the platform:
>
> 1. "Same code works standalone or inside a Workflow". "Same code, same worker".
> 2. A long list of platform features apply when submitting a job as SAA vs when submitting a workflow (durability, addressability, visibility, dedup and ID reuse, priority & fairness, replication/MRN, configurable retention & tiered storage, metrics & tracing) and a few that are activity-specific or primarily so (automatic retries & retry policies, timeouts, liveness checks & checkpointing (both via heartbeating))
>
> For the Replay talk I'm thinking that we should emphasize both (1) and (2). So the message would be roughly
>
> - SAA = (same activity code as always) + (submitted as a job just like WFs are)
> - This is simple (consistency) and sophisticated (Temporal platform features).
> - End result is a simple yet sophisticated job queue with seamless upgrade to workflows.
>
> Then there's a list of relevant features in progress which are almost all platform features consistent with WF (pause/unpause/reset/update-options, start delay, start-from-schedule, start-from-nexus-handler, export, eager/optimistic, serverless, worker heartbeating and direct delivery of cancellation/pause/unpause without needing heartbeats). Plus Standalone Nexus Operations will also share many of the same platform features. TBD what subset of these are appropriate to mention at Replay (views/orders on this welcome)
>
> **Dan Davison (follow-up):** I think we should emphasize that the experience of submitting an SAA job, tracking it, and the available features, are very similar between WF and SAA. That platform consistency is a key part of the SAA design, and we don't want people to think "SAA is yet another Temporal thing I have to learn".
>
> **Roey Berman:** Agree.
>
> **Roey Berman:** @U05GA2YGJKU your talk should focus less on the CHASM side of things and more on SAA IMHO.
>
> **Phil Prasek:** Hi @U05GA2YGJKU last time I chatted with @U07H51DJY0Y he was still onboard to have Standalone Activities and Serverless announced in the Wednesday keynote ahead of the Standalone Activity talk -- so IIUC we'll be giving our Standalone Activities talk at 1:45pm (with Edward from Coinbase) after it's been announced in the 1st Wednesday keynote at 9am.

---

### 2026-04-15 — #topic-standalone-activities — Replay Talk abstract / Coinbase co-presenter
https://temporaltechnologies.slack.com/archives/C09EB1D10UD/p1776359088405859

Participants: Phil Prasek (U06D9QF1DT3), Edward Zhu (Coinbase, U08AS8DE1PT)

Verbatim excerpt:
> **Phil Prasek:** FV: [Replay Talk: Durable Job Processing with Standalone Activities](https://docs.google.com/document/d/1B1bvLO8VGF6rDrnx-vwbz9fllXwmNarLPCXc_udb7xE/edit)
>
> - Coinbase is joining us as a co-presenter to co-launch Standalone Activities on stage at Replay!
> - Async feedback/suggestions welcome
> - cc @U08R85TH125 @U07E13E9T5M @U05GA2YGJKU @U0A5GAUERH6 @U01G8DXRA8Y

Speaker bio from Edward Zhu (Coinbase):
> Edward Zhu is a Software Engineer on the Platform team at Coinbase, where he focuses on infrastructure orchestration and reliability. He has led Temporal self-hosted to cloud migration efforts and is currently driving the adoption of Standalone Activities to replace Coinbase's legacy background jobs infrastructure. Edward works at the intersection of platform engineering and developer experience, helping Coinbase's engineering teams move to modern, durable execution patterns at scale.

---

### 2026-04-17 — #topic-nexus — Nexus + SAA: simplifying unreliable-call patterns
https://temporaltechnologies.slack.com/archives/C03SSH1GG0P/p1776463518568129?thread_ts=1776429097.140139&cid=C03SSH1GG0P

Participants: Roey Berman (U01G8DXRA8Y), circle.com customer (via U03P7SPJYN4 / Antonio)

Context: Circle.com asking about Nexus handlers calling unreliable RPCs (blockchain node RPCs). Roey responded:
> **Roey Berman:** Generally speaking, they should not be making unreliable calls from a sync nexus handler. The recommendation today is to wrap the call in a single workflow activity where the handler can control the retries. In a couple of months or so we are introducing standalone activity bindings for nexus, that should simplify their lives quite a bit. They can try to execute the unreliable call synchronously in the handler if they are careful not to exceed the context deadline. If the call doesn't complete successfully, they can fall back to an activity. This will be made easier when we release the inline activity operation.

---

### 2026-03-26 — #crew-standalone-activities — Coinbase POC success + cost exploration
https://temporaltechnologies.slack.com/archives/C09EB1D10UD/p1773947179765689?thread_ts=1773947179.765689&cid=C09EB1D10UD

Participants: Kevin Woo (U07QFQGAB8E), Harani Mukkala (U0A5GAUERH6), Dan Davison (U05GA2YGJKU), Collin Cook (U05DU21EZ55)

Verbatim excerpt:
> **Kevin Woo:** @U06D9QF1DT3 Coinbase would like to get on a call with us to follow up on SAA.
>
> Their POC went well and they're looking to identify & onboard some lower tier teams that are willing to accept the risks and down time to see if there's any issues.
>
> They're also interested in following up on exactly what can be cut for costs.
>
> **Kevin Woo:** SAA features like visibility, addressability, etc
>
> **Dan Davison:** By addressability I'm assuming they mean something more like fire-and-forget. That in my mind relates to the "Batch" ideas that are floating around and maybe the related "async dedupe" threads.
>
> **Kevin Woo:** Yeah, they understood that it's pushed out (and prop reiterated), but would be good to get an idea of what can be nixed because they have really large volumes.
>
> **Kevin Woo:** I think they were one of the larger and first use cases that we identified and did a cost cutting exploration for.
>
> **Collin Cook:** To provide some color re: cost cutting. We are in the midst of renegotiating their next commit for all of their Temporal workloads. They would like to move their high volume background job service (~5B Actions per month) to Temporal and SAA, but the current implementation of BJS is far less costly and also does not have visibility, addressability, etc. as Kevin pointed out. As part of the negotiations today this topic came up and the "ideas" that Phil had explored with them were brought up. It was communicated to CB that any ideas are exploratory and would be at least a year+ away, but my guess is they would like to start exploring them further as part of their review of SAA (which they also told me has gone very well in initial testing) and planning for what they do with BJS. For now, the plan is to lower their PMA by bringing more volume on to cloud, so that should provide some runway for the foreseeable future. We also mentioned we should look at overall design and discuss if there are things like batching could be relevant here as well.

---

### 2026-03-19 — #competitive — River (Postgres job queue) vs SAA competitive thread
https://temporaltechnologies.slack.com/archives/C01QJKD702X/p1773360121915789?thread_ts=1773360121.915789

Participants: Vickie (U08SJPJBADD), Ted (U02M2AGEJTS), Andrew (U08CHN51Q58), Roey Berman (U01G8DXRA8Y), Phil Prasek (U06D9QF1DT3)

Verbatim excerpt:
> **U08SJPJBADD:** Has [River](https://riverqueue.com/) come up for anyone in calls? @U028HCJFWRG mentioned someone offboarding from Temporal to River and LaunchDarkly is asking about Temporal v. River. Do we have anymore competitive intel? Is promoting standalone activities the best play here?
>
> **U08CHN51Q58:** Similarities [with SAA]: Scalable job queues, Scheduling, Retries, Configurable backoffs, Saves state of completed progress. Differences: Single language with go (limits teams that can use it), Created this year (def not production tested), Postgres (likely to have scaling challenges), 2 different products (queues and workflows), WFs only available in the pro version, DAG based WFs (complex to maintain/update). Def trying to be a "easier" Temporal based off what I'm seeing.
>
> **U08SJPJBADD (battlecard summary):** TLDR:
> 1. Does River scale? — Tops out at ~10k jobs/sec on a single Postgres instance. Known issues with table bloat and SKIP LOCKED contention at higher throughput. Not enterprise-grade.
> 2. Win with Standalone Activities? — SA is the perfect counter-positioning. It matches River's simplicity while adding durability, visibility, and an upgrade path to workflows.
> 3. River in Gong calls? — No mentions found across Gong, Slack, or internal competitive intel. This appears to be the first time it's surfacing in a deal.
> 4. Why would a customer compare them? — They're already on Go + Postgres, want minimal new infra, and are starting with simple background jobs before deciding how much to invest.
> 5. River wins & how to defeat them — Their biggest advantages are zero new infrastructure, transactional job emission, and simplicity. Counter with Temporal Cloud (managed), SA (same simplicity + durability), and TCO analysis (free ≠ cheap when you factor in eng ops time).

---

### 2026-03-11 — #topic-standalone-activities / #general-gtm — Pre-release launch announcement
https://temporaltechnologies.slack.com/archives/C09EB1D10UD/p1773268134120489

Participants: Phil Prasek (U06D9QF1DT3)

Verbatim excerpt:
> **Phil Prasek:** 📣 Standalone Activity Pre-release is live, please share with your customers! 🚀
>
> - Learn about Standalone Activities: evaluate & concept docs, product spec, overview slides -> provide feedback.
> - Will you use it as-is, or do you need changes?
> - Ask your account team to enable Standalone Activities pre-release and try it out on a new dev/test namespace!
> - See pre-release limitations

---

### 2026-03-11 — #topic-standalone-activities — Gorgias webhooks 10k/sec use case
https://temporaltechnologies.slack.com/archives/C09EB1D10UD/p1773052187985099

Participants: Donald Forbes (U06UFK1465A / Staff SA)

Verbatim excerpt:
> **Donald Forbes:** Hey All, Gorgias are considering Temporal for a webhooks use case which feels like it may be a good candidate for the standalone activities. They have potentially a fairly high load with peaks up to *10,000 per second.* Is starting that number of standalone activities a potential issue or will the service be able to manage that volume fairly easily? (Note - My understanding is that this is the acceptance of the activity, maybe with dedupe in place but the actual execution/completion can be done in the background.)

---

### 2026-03-11 — #channel-gtm (C025MMBNTEV) — SAA pre-release GTM launch message
https://temporaltechnologies.slack.com/archives/C025MMBNTEV/p1772152074232329?thread_ts=1772152074.232329

Participants: Gapilan "Cubby" Sivasithamparam (U08R85TH125), Phil Prasek (U06D9QF1DT3)

Verbatim excerpt:
> **Cubby Sivasithamparam:** 🎉 Hi everyone! We're excited to share that Standalone Activities is set to launch in pre-release on March 11th!
>
> Here's what you need to know: [Standalone Activities](https://www.notion.so/temporalio/Standalone-Activities-for-durable-job-processing-2638fc56773880f19710f33cc12a8e36?source=copy_link) allows you to run Activities standalone or combine them into Workflow steps, with the same programming model for both! It's a better way for customers to run their job queue systems, and much more cost effective for customers compared to single-Action workflows.
>
> Your calls to action:
> - Pitch customers on Standalone Activities as a job queue replacement.
> - Assess fit - interest, takeout opportunity, willingness to pay, potential deal size, and adoption blockers.
> - Enable pre-release for your customers - ping us on #topic-standalone-activities.
> - Nurture early adopters who can be a lighthouse for our launch at Replay 2026.
> - Schedule a product deep dive with Phil Prasek.

---

### 2026-02-28 — #topic-standalone-activities — Stripe webhook use case / flow control need
https://temporaltechnologies.slack.com/archives/C09EB1D10UD/p1770946937005029

Participants: Phil Prasek (U06D9QF1DT3)

Verbatim excerpt:
> **Phil Prasek:** [Stripe webhook use case for Standalone Activities](https://temporaltechnologies.slack.com/archives/C03HRBUJM3M/p1770945455311299)
>
> - full debrief coming soon …
> - will likely need improved flow control: rate limiting / concurrency limiting for task dispatch
>
> [Forwarded from original] my key takeaways:
> 1. Stripe is sending webhooks to their customers
>    - could be to a mom/pop show with only 3 servers or a large enterprise
>    - hence they need rate limiting or concurrency con[trol]

Follow-up:
> **Phil Prasek:** also note competitive [Inngest flow control features](https://www.inngest.com/docs/guides/flow-control) — added flow control to the list of post-GA (under consideration) features for Standalone Activities based on this discussion

---

### 2026-02-28 — #topic-standalone-activities — Rippling ETA Express comparison + GA timeline pressure
https://temporaltechnologies.slack.com/archives/C054VD4BZFD/p1770323601726269?thread_ts=1770233963.443579&cid=C054VD4BZFD

Participants: Tom Kadlick (U08GZJMCUFM / AE), Phil Prasek (U06D9QF1DT3), U05R2JHQ43C

Verbatim excerpt:
> **Tom Kadlick (Rippling Office Hours notes):** Evan let us know about new use case "ETA Workflows"
> - "Simple Fan in, Fan Out"
> - Made the decision not to use Temporal for that "went back n forth on this, but was decision they made"
> - Evan's perspective is that not using Temporal has come out of the friction that they've seen with Temporal - Deadline-Based Prioritization, Debounce, Concurrency Control
> - "Getting burnt out finding edge cases" - ie worker shutdown with open pollers
> - ETA Workflows is experimental, lower volume - they want to be able to fix their own problems
>
> **Tom Kadlick (later):** @U06D9QF1DT3 - Rippling will be comparing SAA to their home grown "ETA Express" this week we discovered that their time frame for rolling this out is June at latest - but they only want to use GA solutions
>
> - In our favor, they are particularly interested Priority/Fairness being part of SAA - this would be difficult for them to build I imagine
> - What's your latest guess on Pre-Release so we can start a POC for these guys and keep them engaged while we work towards GA?
>
> **U05R2JHQ43C:** Phil, any idea on GA? If they're making a decision in July, then can you see a world where we GA in July?
>
> **Tom Kadlick:** My thought would be if we have a stable SAA pre-release we can get in their hands, with Priority/Fairness, we could get them to drag that decision out a little bit (ie they like what we're showing them and willing to wait another ~month)

---

### 2026-02-20 — #topic-standalone-activities — Anysphere contingent on Schedule support
https://temporaltechnologies.slack.com/archives/C09EB1D10UD/p1771969858065879

Participants: Polina Jastrzebska (U0AD88R224Q / SA)

Verbatim excerpt:
> **Polina Jastrzebska:** Hi Phil @U06D9QF1DT3 and team, thanks so much for all the work on this feature. Following up here - as Tushar mentioned above, [Anysphere remains very interested](https://temporaltechnologies.slack.com/archives/C09AENYB7L3/p1771962287390819), contingent on schedule support being enabled. Reiterating to emphasize the importance and need.

---

### 2026-02-14 — #topic-standalone-activities — DM on orchestration distinction / job queue framing
https://temporaltechnologies.slack.com/archives/D08BP2H5953/p1775763181330319

Participants: Dan Davison (U05GA2YGJKU) (DM context)

Verbatim excerpt:
> **Dan Davison:** Hi Dan, no, I don't think we should have any concern like that with today's one-pagers. The fundamental distinction is that if you need to orchestrate multiple activities / nexus operations / child $things, you have to use a workflow for our resilience / automatic resume semantics. But if you're not orchestrating and you don't need resume, then you can use a standalone activity, and it will be cheaper for you.
>
> None of today's one-pagers erode that distinction: StartDelay, StartFromSchedule, StartFromNexus, and Export are feature parity work that make Temporal consistent (and thus "simple", IMO), but don't change the orchestration distinction. StartDelay is a sensible thing for any job queue, and the others are more Temporal-specific sophistication.

---

### 2026-02-12 — #topic-standalone-activities — Apollo Global interest: needs Schedule + batch jobs
https://temporaltechnologies.slack.com/archives/C09EB1D10UD/p1770058747641239

Participants: Phil Prasek (U06D9QF1DT3)

Verbatim excerpt:
> **Phil Prasek (forwarding Apollo feedback):** [Apollo Global is interested in Standalone Activities, but needs Schedule an Activity support]
>
> The concept looks to be interesting with us having a good number of use cases that will need this or one off triggers to execute such activities. However, to be able to use this properly for Apollo we *need to be able to schedule activities as well*. Cause a good chunk of our processes for the year is going to be batch jobs.
>
> (earlier Apollo notes): The vast majority of existing Jobs that would migrate in a lift+shift would be single-Activity Workflows - will be looking into Standalone Activities for this. Lots of Cron jobs are on Tidal and as k8s jobs.

---

### 2026-01-30 — #topic-standalone-activities — SoFi building similar thing themselves
https://temporaltechnologies.slack.com/archives/C09EB1D10UD/p1768596470110999

Participants: U07QFQGAB8E (Kevin Woo), U06D9QF1DT3 (Phil Prasek)

Verbatim excerpt:
> **Kevin Woo:** I was on a call with SoFi and they have a workflow platform team that is building/offering something similar to standalone activities. Can we add them to a list of interested parties and share the [external spec](https://docs.google.com/document/d/1Wt4YLh5V1LlBUTOr5hzrw35Q06NUilED5zpguGaPu2M/edit?tab=t.0#heading=h.2hjpskgg7l4f) with them?

---

### 2026-01-30 — #topic-standalone-activities — Framing doc: SAA vs Workflows
https://temporaltechnologies.slack.com/archives/C09EB1D10UD/p1769644535440829

Participants: Phil Prasek (U06D9QF1DT3)

Verbatim excerpt:
> **Phil Prasek:** [[Draft] Distinguishing Standalone Activities & Temporal Workflows](https://www.notion.so/temporalio/Draft-Framing-doc-for-Standalone-Activities-vs-Temporal-Workflows-2f68fc567738806aa799d00901a77725)
>
> - draws a box around what a Standalone Activity is and isn't ... and why
> - based on our previous convos plus:
>   - PRD Features: In-Scope / Post-GA (Under Consideration) / Out-of-Scope (Never)
>   - User research findings
>   - Product spec

---

### 2026-02-28 — #topic-standalone-activities — Stripe is excited; wants Export + Schedule (cron) support
https://temporaltechnologies.slack.com/archives/C09EB1D10UD/p1772218631078259

Participants: Phil Prasek (U06D9QF1DT3)

Verbatim excerpt:
> **Phil Prasek:** Stripe is excited about Standalone Activities!
>
> - wants Standalone Activity support for Export & Start from Schedule (cron jobs)
> - added them to the list of customers requesting in the features under consideration for post-GA in the PRD

---

### 2026-02-20 — #topic-standalone-activities — "Job queue replacement" / "activity queue" terminology DM
https://temporaltechnologies.slack.com/archives/D08G86RJUF7/p1771023616260819

Participants: unknown DM participant

Verbatim excerpt:
> Given that "Standalone Activity" is going to be a slightly alien term for people replacing job queue technology, I wonder if there's a way we can also introduce "activity queue" or "activity task queue" into the user-facing terminology that we use related to the product. We have increasing sophistication in our Matching task queues so maybe there's a way of using "queue" language that is both familiar to traditional job queue people and also allows us to show off fairness/priority etc.

---

## Additional signal (pre-release customer list from C0AGE0K2A0Z)

The pre-release enablement channel recorded requests from at least the following named customers/accounts (bot-generated, no per-message permalinks available):

Bank Leumi, Coinbase (multiple requests), HeyGen, Quanta, Rippling, OpenAI (multiple), MLP - Millennium, Roblox, Bitovi, Upstart, Apollo, Relativity.com, Duolingo, Riot Games, SailPoint, Yubi (Credavenue)

Coinbase is the furthest along: POC completed, currently renegotiating commit to include ~5B Actions/month from their background job service (BJS). Edward Zhu from Coinbase will co-present the Replay talk.

---

## Supplementary: GTM framing language (from C0ABE0BK1T7, Jan 2026)

> Standalone Activities are Temporal's durable job queue primitive that gives teams execution guarantees traditional job queues can't deliver—without the operational overhead.

## Supplementary: COGS analysis result (from crew-standalone-activities, Phil Prasek, ~April 2026)

> **Phil Prasek:** I've now run some SAA vs SAW load experiments to estimate resource usage ratios needed in the calculations set out in [Standalone Activity COGS and margins]. The TLDR is that with the [results show >2x lower cost vs. single-activity workflows]. Hey @U0781PY1K47 here are the [Standalone Activities COGS analysis results] which are inline with our initial estimates (> 2x lower cost vs. single-activity workflows). With this data can you finalize the Monetization brief for Standalone Activities and get final approval from all stakeholders in the next 2 weeks?
