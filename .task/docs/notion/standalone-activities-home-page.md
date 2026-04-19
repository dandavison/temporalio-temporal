---
title: "Standalone Activities Home Page"
notion_url: "https://www.notion.so/2698fc567738806da12ac95e951c2754"
last_edited: "2026-04-16T19:04:02.221Z"
page_type: other
---

<page url="https://www.notion.so/2698fc567738806da12ac95e951c2754">
<ancestor-path>
<parent-page url="https://www.notion.so/49ce563cd6244158961b9193a15e0090" title="Workstream Home Pages"/>
<ancestor-2-page url="https://www.notion.so/486567a2aa37432d8e41f31b461d8a12" title="Product"/>
</ancestor-path>
<properties>
{"title":"Standalone Activities Home Page"}
</properties>
<content>
👉 [GTM Hub Standalone Activities](/1d78fc56773880e684d2d890658507cc?p=2638fc56773880f19710f33cc12a8e36&pm=s&pvs=25) 👈
<page url="https://www.notion.so/32f8fc5677388096afcbf8754dee90d3">Launch Enablement Status - Standalone Activities</page>
<mention-page url="https://www.notion.so/2cc8fc56773880bfaa1adc2b6ec8c1bc"/>

## Initial customer feedback on the Product Spec
👉 [External Product Spec - for sharing with customers under NDA (ask them to request access)](https://docs.google.com/document/d/1Wt4YLh5V1LlBUTOr5hzrw35Q06NUilED5zpguGaPu2M/edit)
- Rippling: "this sounds almost exactly like what we're looking for"
- Roblox: "another advantage of Temporal over SQS is workflow IDs for deduplication"
- Roblox: "switching between SQS and Temporal is a pretty easy flag flip"
- Cursor: "this looks good! just gave it a read. we'd use it." - Josh Ma
- Block: "this is AWESOME - thank you guys and we can't wait to try it" - Nick E
- <mention-page url="https://www.notion.so/20e8fc56773880eeba6ffe12ae0d1884"/>

## Customer challenges
- lots of pain points with existing job queue systems that Temporal has solved
- but single-activity workflows are just not economically viable in Temporal today, esp. for high-volume lower biz value use cases (lots of them).
- compared to Kafka and SQS -- Temporal is too expensive for these use cases.
- need a cost-effective solution for durable/scalable single activities.

Example: Coinbase `Background Jobs Service` (BJS) has several problems:
1. **Licensing Concerns:** BJS is a Faktory fork, which uses a restrictive license.
2. **Risk of Job Loss:** BJS has potential for `job` loss, as demonstrated in a recent incident.
3. **Maintenance Overhead:** BJS is an additional T0 service that requires ongoing maintenance.
4. **Scalability Challenges**: A single BJS coordinator process is responsible for moving scheduled jobs and failed jobs back to queues for processing.
5. **Noisy neighbors:** if one namespace has many failed jobs, then the retry sorted set in Redis could be backed up.
6. **Payload size limit:** multiple teams have expressed interest in supporting larger job sizes.
7. **Lack of features**
   1. Web UI with job level details
   2. Dead Letter Queue
   3. Support for draining outstanding job executions during worker shutdown
   4. Heartbeats for long-running jobs
   5. Support for terminating open job executions
   6. Asynchronous completion
   7. Search attributes for job querying

## Key features
- **Execute any Temporal Activity as a top-level primitive** without the overhead of a Workflow.
- **Native async task processing model**: schedule -> dispatch -> process -> result
- **No head-of-line blocking** - a slow task doesn't block the dispatch of other tasks
- **Arbitrary length tasks** with heartbeats to checkpoint progress and handle worker failures
- **At-least-once execution** by default with native retry policy & timeouts
- **At-most-once execution** if retry max attempts is 1
- **Addressable** - get a activity ID / run id and get the result, (un)pause, reset, cancel, terminate
- **Dedupe** - conflict policy: (USE_EXISTING, …), reuse policy: (REJECT_DUPLICATES, …)
- **Priority and fairness** - multi-tenant fairness, weighted priority tiers (e.g. high/medium/low), and safeguards against starvation of lower-weighted tasks, plus no head-of line blocking
- **Visibility** - list executions and see current status, retry count, last error, …
- **Manual completion** by ID (or token): ignore activity return and wait for external completion
- **Dual use** - execute Activities In-Workflow or Standalone with no Worker code changes

## Resources
👉 [GTM Hub Standalone Activities](/1d78fc56773880e684d2d890658507cc?p=2638fc56773880f19710f33cc12a8e36&pm=s&pvs=25) 👈
<page url="https://www.notion.so/28c8fc567738804a839ad598718f1689">Use Cases - Standalone Activities</page>
<page url="https://www.notion.so/2e18fc567738808a9b2bc9cfec250b71">User Research Findings & Feature Naming Recommendation - Standalone Activity</page>
<page url="https://www.notion.so/2df8fc567738805e8524e646c4cb1e4f">SUPERSEDED: Standalone Activities Monetization Brief</page>
<page url="https://www.notion.so/2e48fc56773880668ad9de5eaf7a3d59">GTM message house - Standalone Activities</page>
<page url="https://www.notion.so/2e88fc56773880ffb383c3f6b5d9cbb9">Competitive Brief - Standalone Activities</page>
<page url="https://www.notion.so/2e38fc56773880ffaf8be9ae6c21edfb">Competitive Cost Analysis - Standalone Activities</page>
<page url="https://www.notion.so/2e38fc5677388002ac87e454b13b3493">Celery competitive analysis (job/task queue)</page>
<page url="https://www.notion.so/2e48fc56773881f193a6e0b66f74c6bf">Faktory competitive analysis (job/task queue)</page>
<page url="https://www.notion.so/3078fc56773880b0b2b0d8b30cdfae61">AWS SQS competitive vs Standalone Activities</page>
<page url="https://www.notion.so/2f08fc56773880099287c0e06f57ee2e">AWS Durable Functions competitive analysis (job/task queue)</page>
<mention-page url="https://www.notion.so/27d8fc567738802e80f9e3b8eba4eb0e"/>
<page url="https://www.notion.so/2e68fc5677388083859dcb0551046f94">\[Internal\] Coinbase Faktory fork - all-in costs and issues that can't be resolved</page>
<page url="https://www.notion.so/2e98fc56773880958a32db7aa13a1925">\[Draft\] Product Documentation Guidelines - Standalone Activities</page>
<mention-page url="https://www.notion.so/2ee8fc567738800cba5ee97fe36b4f82"/>
<page url="https://www.notion.so/2f68fc567738806aa799d00901a77725">Distinguishing Standalone Activities & Temporal Workflows</page>
<page url="https://www.notion.so/2fe8fc56773880f9a73bf5454e438dd6">\[Draft\] Evaluating Standalone Activities (Job Queue)</page>

## Internal Reviews
<mention-page url="https://www.notion.so/1ee8fc567738806d8b6fe8e2eeae0fc4"/> - approved 6/25/25 ✅
[Internal Product Spec](https://docs.google.com/document/d/1MjtspkF2lTh0ThcVBJaPSWb6dr5786nhQH93ifSWSpA/edit) - for internal discussion / comment threads ✅
[Standalone Activities Initiative Review](/24c8fc56773880918c22e47fd3a10997?pvs=25#24c8fc56773880ca9ec8cdbc5e27772b) 8/11/25
<page url="https://www.notion.so/3208fc56773880779006f4f765be0c84">Standalone Activities Hands on training 3/11</page>

## Misc
 <mention-page url="https://www.notion.so/1f58fc567738800c8401dc2b5c76885d"/> (Engineering home page)
<mention-page url="https://www.notion.so/21e8fc567738801aa9bbc2c850175a50"/>
<page url="https://www.notion.so/2f88fc567738807aa53afe9cf9f96dc6">COGS Analysis for Standalone Activities Pre-Release</page>
<page url="https://www.notion.so/2f88fc567738817ab94cd59058dd7800">\[INTERNAL\] Product Spec_ Standalone Activities for Lower Cost single-Activity Workflows</page>
<page url="https://www.notion.so/3228fc56773880eaa044ff215b6d0fb0">Standalone Activities Billing and Metering Requirements</page>
<page url="https://www.notion.so/3368fc56773880e394a4d9b5e8f77d7b">**Customer Signal: Schedule Pricing & Cost Concerns**</page>
<page url="https://www.notion.so/33d8fc56773880efa508c3f0df6a68d1">\[DRAFT/HOLD\] Blog Post: **Standalone Activities: Durable Job Processing, Now in Public Preview**</page>
<page url="https://www.notion.so/3438fc567738803f8a6ed0c698019051">Standalone Activities Hands On Product</page>
<page url="https://www.notion.so/3448fc56773880019d85f940efbb33e9">Replay Talk: Durable Job Processing with Standalone Activities</page>
</content>
</page>
