---
title: "PRD: Standalone Activities for durable job processing"
notion_url: "https://www.notion.so/1ee8fc567738806d8b6fe8e2eeae0fc4"
page_type: PRD
---

<page url="https://www.notion.so/1ee8fc567738806d8b6fe8e2eeae0fc4">
<ancestor-path>
<parent-data-source url="collection://4b6ffde6-1ff3-4e60-b8ff-f304c2659a42" name="Product specifications"/>
<ancestor-2-database url="https://www.notion.so/187f71c66f1047c38a5868d1105a9d71" title=""/>
<ancestor-3-page url="https://www.notion.so/acf3278a19bd412f9ea86a8f96fb0f68" title="Product Requirements"/>
<ancestor-4-page url="https://www.notion.so/93879d976fc047f8bf291d27366c9235" title="Product: Archives"/>
<ancestor-5-page url="https://www.notion.so/1040b06f345946fcb1031dc57f634e5e" title="Archive"/>
<ancestor-6-page url="https://www.notion.so/486567a2aa37432d8e41f31b461d8a12" title="Product"/>
</ancestor-path>
<properties>
{"Last edited time":"2026-03-20T01:09:00.546Z","Owner":["<mention-user url=\"user://e5c614d1-1905-4a08-9a6f-39ccd9c18ca3\"></mention-user>"],"Specification":"PRD: Standalone Activities for durable job processing","Stage":"Product design","url":"https://www.notion.so/1ee8fc567738806d8b6fe8e2eeae0fc4"}
</properties>
<content>
# **Exec Summary**
Standalone Activity is a new feature designed to address the high cost of single-Activity workflows within Temporal. Key strategic customers are currently finding full Temporal Workflows too expensive for high-volume, non-business-critical use cases that only require reliable execution of a single function call with retries. This has led to the emergence of non-Temporal task processing solutions. 
The core problem is that single-Activity workflows are too expensive, causing customers to explore alternatives or build their own solutions, which risks Temporal's market expansion and stickiness. Standalone Activities aim to provide a more cost-effective solution, enabling Temporal to capture a larger share of the async task processing market. 
**Key Highlights:**
- **Opportunity:** The global Total Addressable Market (TAM) for async task processing is estimated at \~\$7.5 billion/year in 2025 with a CAGR of 16%. By capturing 20% of this market, <span discussion-urls="discussion://27f8fc56-7738-8061-b946-001c71474bc1">Standalone Activities could generate \$1.5 billion in business</span>. The combined market for async tasks and workflows is projected to reach \~\$28 billion by 2030.
- **Customer Feedback:** Customers like Rippling, Roblox, Cursor, Yubi, and Verkada have expressed a strong need for a more economical solution for single-Activity tasks. They are willing to trade off <span discussion-urls="discussion://2808fc56-7738-80c3-a63a-001c990ea0c5">some</span> advanced workflow features for lower costs and simplified adoption.
- **Solution:** Standalone Activities will allow any Temporal Activity to be executed as a top-level primitive without the overhead of a full Workflow. This includes features like native async task processing, no head-of-line blocking, addressability, deduplication, priority and fairness, and visibility.
- **Pricing**: Same as [existing Activity pricing](https://docs.temporal.io/cloud/actions#activities), but now you can invoke an Activity standalone for 1 action instead of 2 (Workflow + Activity). [Storage billing](https://docs.temporal.io/cloud/pricing#storage) needs to factor in Standalone Activity storage (like Workflows).
- **Cost & Financial Impact:** Standalone Activities are expected to reduce the cost of durable single-Activity use cases by approximately 2x (and potentially 4x with no visibility in the future). This is achieved by eliminating the wrapper Workflow, reducing database writes from \~15 to 4. Gross profit margin increases by \~10% by retaining some of these cost savings. While there is a potential for short-term cannibalization of existing single-Activity workflows, the long-term gain is significant, capturing key use cases on the road to Temporal becoming "THE solution for ALL async workloads." Financial projections indicate a net ARR increase of \$5M with Standalone Activities in FY27 and \$<span discussion-urls="discussion://27f8fc56-7738-80ca-906c-001ce8d76bee">14M in FY28</span>.
- **Competitive Landscape:** <span discussion-urls="discussion://2808fc56-7738-8079-84ed-001c887789b4">Standalone Activities will compete with existing message brokers (RabbitMQ, SQS, Google Pub/Sub), task queue frameworks (Celery, Sidekiq, RQ, BullMQ), and durable execution technologies (Inngest, Restate).</span> <span discussion-urls="discussion://27f8fc56-7738-80f5-a5fb-001cf7fd0176,discussion://2808fc56-7738-80c4-878a-001c4f509453">Temporal's key differentiators will be </span><span discussion-urls="discussion://27f8fc56-7738-80f5-a5fb-001cf7fd0176">native priority and fairness, scalability, deduplication, addressability, observability, adjacency to Temporal Workflow, and re-use of Activities for standalone or in-workflow execution</span> — and above all a simple programming model to write resilient code with fully integrated observability.
- **Positioning:** Initially, Standalone Activities will be positioned as an advanced feature for specific high-volume use cases, rather than being included in the Day 0 developer journey, to avoid overwhelming new users and potentially diluting the Temporal brand. We should launch Standalone Activities as a 
**Product Spec & Customer Feedback:**
- <mention-page url="https://www.notion.so/2698fc567738806da12ac95e951c2754"/>
	- 👉** **[**Product Spec: Standalone Activities for Lower-Cost single-Activity Workflows**](https://docs.google.com/document/d/1MjtspkF2lTh0ThcVBJaPSWb6dr5786nhQH93ifSWSpA/edit)
	- 👉 [**UX Designs: Standalone Activities**](https://www.figma.com/design/8dSoDU2sWeQf7CgZzTqzeW/Cloud-UI?node-id=25886-5045&t=g5b1xbmGBvCA6d86-1)
	- 👉 [**Engineering Blueprint: Standalone Activities**](/21e8fc567738801aa9bbc2c850175a50?pvs=25) [ ](/2698fc567738806da12ac95e951c2754?pvs=25)
	- 👉 <mention-page url="https://www.notion.so/28c8fc567738804a839ad598718f1689"/> 
	- 👉 [GTM Hub - Standalone Activities](/2638fc56773880f19710f33cc12a8e36?pvs=25)
- Customer feedback
	- Rippling: “this sounds almost exactly like what we’re looking for \[…\] since our existing system that is a steaming pile of garbage and is very cheap” - [Tyson Mote](https://www.notion.so/26c8fc567738801ea67ddd233b860cc3?pvs=25#26c8fc567738809c882feba732e04b6f)
	- Roblox: “another advantage of Temporal over SQS is workflow IDs for deduplication” - [Shravan](https://www.notion.so/Roblox-SA-7-9-25-product-spec-2698fc56773880b997a1d5762b0893ec?pvs=21)
	- Roblox: “switching between SQS and Temporal is a pretty easy flag flip” - [Shravan](https://www.notion.so/Roblox-SA-7-9-25-product-spec-2698fc56773880b997a1d5762b0893ec?pvs=21)
	- Cursor: “this looks good! just gave it a read. we'd use it [as long as we can schedule activities](slackMessage://temporaltechnologies.slack.com/C09EB1D10UD/1769463616.076859/1769462129.345829).” - [Josh Ma](https://www.notion.so/2558fc5677388037810bccb118aa54f0?pvs=25#26b8fc56773880caa8e6c0ef575319cb)
	- Block: "this is AWESOME - thank you guys and we can't wait to try it" - [Nick E](/2768fc56773880cfa0cad5dff6074a29?pvs=25)
	- Yubi: “there are many a workflow with only one activity.” - [Sundara P](/2788fc56773880d28913ea23f8fc3c9a?pvs=25#27e8fc567738804ca2ffc8aad6a9b0d2)
# Problem statement
## single-Activity Workflows are too expensive for many use cases
Key strategic customers want to go “all in” on Temporal, but **single-Activity Workflows** are too expensive for high-volume & non-business-critical uses cases that just need to **reliably execute a single function call in a durable way with retries**.
- Need a more cost-effective option for single-Activity Workflows. They are willing to tradeoff certain features (full workflow, replay, event history, visibility, …) to get lower cost.
- If we publish the request to Kafka, that will also work. So do we really need a Temporal workflow here as volume would be high?
## Non-Temporal async solutions are emerging for single-Activity
- To fill the void, companies are prototyping (Rippling) or have incumbent lower-cost task processing systems in place (Square, Stripe) to <span discussion-urls="discussion://27d8fc56-7738-8050-9d32-001cd2198cee">provide a more economic alternative to single-Activity Workflows built on top of Kafka</span>, SQS, ...
- Developers will then have to choose between Temporal and some other less expensive async solution.
## <span discussion-urls="discussion://27f8fc56-7738-8096-9bac-001c691bf8fd">SDK wrapped</span> - Developers are intermediated from Temporal
Some companies (Square/Stripe) have introduced a SimpleTask abstraction that wraps the Temporal SDK. This intermediates the end-Developer from Temporal which could be used as a wedge to plug-in more cost-effective technologies (Kafka, …) below that abstraction. This may ultimately hurts expansion/stickiness as Developers are not using Temporal directly.
- Some Platform teams have introduced a SimpleTask abstraction (Stripe, Square)
	- Wrap a Temporal Workflow and Activity. 
	- [Square estimates that 40% of their Temporal volume is SimpleTasks](https://us-11514.app.gong.io/call?id=6035094571614556488&highlights=%5B%7B%22type%22%3A%22SHARE%22%2C%22from%22%3A651%2C%22to%22%3A667%7D%5D)
- Square: once Simple Tasks were available they saw rapid adoption for multiple use cases.
	- Stepping stone path towards full Temporal Workflows for many teams.
	- Plan to open source their implementation if we don’t add it to the Temporal SDK.
- Datapay: Doesn't want to expose the fullness of Temporal to their domain teams (too much).
	- Wants the domain teams to just write Activities to avoid having to learn about Workflows to get started.
## Kafka/SQS doesn’t provide built-in priority and fairness
- Needed for many single-Activity Workflow use cases (Webhook delivery at Stripe, …)
## AI use cases not solved well with current mix of Temporal primitives
- right now nexus can only do WF or \< 10 second thing with Nexus; bad state of world
- can’t call LLM from Nexus handler; 60 seconds is required
- full Workflows are too expensive for AI use cases
- immediate problem to solve above others - <span discussion-urls="discussion://27f8fc56-7738-80f4-b413-001cf1ebc4cf">Nexus + Standalone Activities</span> solves itf
# Opportunity
👉 **Global TAM (async task processing): \~\$7.5B/year (2025)**
- if we can grab <span discussion-urls="discussion://2808fc56-7738-80cf-a3b7-001c9cd192ce">20</span>% of TAM that’s a \$1.5B business
- A blended CAGR of 14–18%
- **5-Year TAM Projection (2026 → 2030)**
	- Base case CAGR
		- **Async tasks:** **16%**
		- **Workflows:** **20%**
<table header-row="true">
<tr>
<td>**Year**</td>
<td>**Async Tasks (\$B)**</td>
<td>**Workflows (\$B)**</td>
<td>**Combined (\$B)**</td>
</tr>
<tr>
<td>**2026**</td>
<td>**8.70**</td>
<td>**6.00**</td>
<td>**14.70**</td>
</tr>
<tr>
<td>2027</td>
<td>10.09</td>
<td>7.20</td>
<td>17.29</td>
</tr>
<tr>
<td>2028</td>
<td>11.71</td>
<td>8.64</td>
<td>20.35</td>
</tr>
<tr>
<td>2029</td>
<td>13.58</td>
<td>10.37</td>
<td>23.95</td>
</tr>
<tr>
<td>**2030**</td>
<td>**15.75**</td>
<td>**12.44**</td>
<td>**28.19**</td>
</tr>
</table>
See details [below](/1ee8fc567738806d8b6fe8e2eeae0fc4?pvs=25#21d8fc56773880bb9d6fde3972c84f2b).
# Customer <span discussion-urls="discussion://21d8fc56-7738-8060-9d95-001c79ffe041">Feedback</span> {toggle="true"}
	See also: [GTM Hub - Standalone Activities](/2638fc56773880f19710f33cc12a8e36?pvs=25)
	### Quotes
	- Rippling: “this sounds almost exactly like what we’re looking for” - [Tyson Mote](https://www.notion.so/26c8fc567738801ea67ddd233b860cc3?pvs=25#26c8fc567738809c882feba732e04b6f)
	- Roblox: “another advantage of Temporal over SQS is workflow IDs for deduplication” - [Shravan](https://www.notion.so/Roblox-SA-7-9-25-product-spec-2698fc56773880b997a1d5762b0893ec?pvs=21)
	- Roblox: “switching between SQS and Temporal is a pretty easy flag flip” - [Shravan](https://www.notion.so/Roblox-SA-7-9-25-product-spec-2698fc56773880b997a1d5762b0893ec?pvs=21)
	- Cursor: “this looks good! just gave it a read. we'd use it.” - [Josh Ma](https://www.notion.so/2558fc5677388037810bccb118aa54f0?pvs=25#26b8fc56773880caa8e6c0ef575319cb)
	- Block: "this is AWESOME - thank you guys and we can't wait to try it" - [Nick E](slackMessage://temporaltechnologies.slack.com/C03BY3HR2RH/1758573338.844739/1757703696.791129)
	### [Yubi May 2025](slackMessage://temporaltechnologies.slack.com/C08NKV7G6Q2/1748008779.220399/1746793204.626509) - are single activity workflows a good fit for Temporal or is Kafka better? (475K ARR)
	- we have following use cases of single activity:
		1. Calling some other internal service API which might me calling some 3rd. party API. We would be calling internal service api in async way and expect response through webhook call but it is latency sensitive and we expect the response within 2 sec.
		2. Calling some internal service API for Audit Logs - not latency sensitive.
		3. Calling some internal service API for Request Logs - not latency sensitive but traffic volume would be high.There could be other similar scenarios. We need some guidance here on what could be the common guidelines/recommendations here?
	- For point 2 and 3, we need durability. It should be retry on failure. If we publish the request to Kafka, that will also work. So do we really need Temporal workflow here as volume would be high?
	- Follow up [Slack thread](slackMessage://temporaltechnologies.slack.com/C090FA8SUD9/1758731470.878369/1758731470.878369) 🧵
	### [Verkada - May 2024](slackMessage://temporaltechnologies.slack.com/C0614AREJPR/1715632819.845259/1715632819.845259) - need a cost-effective solution for durable/scalable single activities
	- we have single-activity workflows that are just not economically viable in Temporal today.
	- <span discussion-urls="discussion://27d8fc56-7738-80f6-9da9-001c4c3f2bae">We've compared them to SQS and Temporal is orders of magnitude more expensive</span>.
	- You guys have done the hard part - complex workflows - it would be great if you also had a cost-effective solution for durable/scalable single activities.
	### [Rippling ](https://us-11514.app.gong.io/call?id=7442475522754263130)- can’t go all-in on Temporal for single-Activity Workflows / Tasks
	- [April 2025](https://us-11514.app.gong.io/call?id=7442475522754263130): For high-volume, low-latency workloads, Rippling needs to reliably execute a single function/activity with features like concurrency control, debouncing, batching, and simplified visibility, rather than full workflow capabilities.
	- Rippling is aiming to migrate 50% of their ETA (task processing) workloads to Temporal, but the current Temporal contract only covers 38% due to budget constraints. \~12-15B actions/month on ETA today.
	- Rippling is hesitant to pass on Temporal's higher costs to their internal teams, as it could impact adoption. “Quintupling their costs is too much”
	- Rippling is exploring building a custom, cheaper task processing system for workloads not suitable for Temporal”
		- <span discussion-urls="discussion://27d8fc56-7738-80db-8627-001c5cd5bd35">Tyson: "...I prototyped, a very minimal async task processing system that is optimized for high volumes of very basic tasks. It has de-bounce, automatic batching, stream — so you can amortize some of the cost. It's extremely cheap, extremely low latency, low single digit milliseconds, and it'll do 20,000 jobs per second on like a single box."</span> [Link to snippet](https://us-11514.app.gong.io/call?id=7442475522754263130&highlights=%5B%7B%22type%22%3A%22SHARE%22%2C%22from%22%3A886%2C%22to%22%3A926%7D%5D)
	- Temporal migration is being slowed by challenges with Temporal's pricing and feature set.
		- Need 1/2 the features for 1/4 the cost
		- Willing to trade off some features (like complex workflow visibility) to reduce costs for these high-volume workloads.
		- Analogy: Express Workflows in Amazon Step Functions.
		- Desire: fair scheduling based on SLAs.
			- e.g. I want you to run this task within 30 minutes
	- Nitin - Director Eng @ Rippling - [June 2025](/20e8fc5677388095a133fe2691f27fa0?pvs=25):
		- Would like to have a path to migrate everything to Temporal in the next 3-4 months — but we're on track to exceed our budget by 2x to 3x in the next 3-4 months.
		- Not having 1/2 the Workflow features at 1/4 of cost for high volume workloads is preventing us from going all-in on Temporal to replace the backend of our ETA system that is in the middle of everything that Rippling does, every line of code
		- If everything moved over as is today, it’s going to be more than \$2.4M/yr ARR (currently \$300K -\> 1.1M/yr commit)
		- We're on track to exceed our budget in the next 3-4 months -- this is going to become a very big problem, very soon.
	- Post call:  it's [estimated that 60-70% of their workflows are single step](slackMessage://temporaltechnologies.slack.com/C054VD4BZFD/1749595024.806969/1749576006.578819) -- and if we can't provide an alternative simple task/queue system, they will be forced to build an alternate system and [Taylor (account SA) thinks that 60-70% of their current ARR is at risk](slackMessage://temporaltechnologies.slack.com/C054VD4BZFD/1749595310.661109/1749576006.578819), plus the lost additional use cases that standalone activities would enable.
	### [Justworks](slackMessage://temporaltechnologies.slack.com/C09EB1D10UD/1761321332.236779/1761321332.236779)
	- Since we already need to migrate off Sidekiq + Redis (with our Rails → Go migration) we could simplify our tools  with Temporal `Workflows` & `Jobs` (Standalone Activities) instead of adding a new job processing framework - a two birds one stone situation.
	### [Block/Square 11/14/25](slackMessage://temporaltechnologies.slack.com/C09EB1D10UD/1763133174.685919/1763056890.767479) - Josh ← Nick: New 6-figure use case for Block:Square card payment processing blocked on cost
	> We just had a low 6-figure potential use case pop up yesterday, our first with Block:Square card payment processing, that's currently three actions: Workflow, Timer, Activity. We mentioned Workflow Start Delay (so now 2 Actions).
	### [Block - 6/11/25 - Nick](https://us-11514.app.gong.io/call?id=6035094571614556488) - SimpleTask was crucial for driving adoption @ Square
	- The Square SimpleTask (standalone activity) feature was crucial for driving adoption at Square, "...if you're not having problems on the bottom of the growth curve, it's \[what\] we needed to generate growth. So, for us, we'd already signed a contract that we weren't spending. So generating the growth and finding a way to increase utilization was the thing for us. So if that's not core to where you guys are on your adoption curve, I could see that being lower value, but for us, we were, you know, make… good on the investment was sort of was the angle that we went from when we built that." [Link to snippet](https://us-11514.app.gong.io/call?id=6035094571614556488&highlights=%5B%7B%22type%22%3A%22SHARE%22%2C%22from%22%3A790%2C%22to%22%3A824%7D%5D)
	- Square has intentionally limited the configurability of the Square SimpleTask (standalone activity) feature to avoid architectural mistakes and steer users towards full Temporal Workflows.
	- Nick "...it took me weeks to grok Temporal \[and\] I understand what it is to build distributed systems with these kind of characteristics. And I couldn't make sense of the durable execution paradigm even though I was like doing it, you know, with less good tools all the time, so to drop somebody on top of that, so to drop somebody in there with just the promise that it works, it's hard like, yes, our goal is to get somebody running a Workflow as quickly as possible. So they don't fall off. So they don't like churn out of the funnel before they make it to the bottom, which is again just more sales-y stuff like, but… we try and minimize the chances for somebody to, you know, to close the tab. Okay. Yeah, \[standalone activities is\] the second place we invested." [Link to snippet](https://us-11514.app.gong.io/call?id=6035094571614556488&highlights=%5B%7B%22type%22%3A%22SHARE%22%2C%22from%22%3A1036%2C%22to%22%3A1091%7D%5D)
	- Nick: "if \[our Developers\] say they want to compose \[multiple steps\] you're talking about a Workflow. Now take that code you already did all the setup \[with SimpleTask\], just run a Workflow instead and it's been, really important. We have SimpleTask as a Standalone Activity." [Link to snippet](https://us-11514.app.gong.io/call?id=6035094571614556488&highlights=%5B%7B%22type%22%3A%22SHARE%22%2C%22from%22%3A1182%2C%22to%22%3A1199%7D%5D)
	### [Stripe - Drew notes](/1a68fc567738800f8298c3b6441b06dd?pvs=25#1a68fc5677388065ad6ff9c60b19b025) - cheaper tasks needed for high volume use cases - created simple `Task` wrapper around Workflow + Activity
	- Stripe wanted a cheaper Task, and this has continued to be a Temporal adoption blocker for some high-scale use cases.
	- Stripe’s platform team, Workflow Engine, built `Task`, a wrapper around a Workflow + Activity, for use by all its product and infra teams.
	- Simple abstraction
		- entice people to move from the legacy system.  
		- Teams with a mixture of Tasks and Workflows could operate on the same service platform.
		- Migrate teams to service-oriented architecture.
		- provide deduplication
	### [Snap - Oct 2023](https://us-11514.app.gong.io/call?id=323074277046707918) - looking for simple Scheduled tasks that make an API call
	- Arash: "That just one feedback I have as well is like we have a lot of teams that have like these cron (schedule) use cases — we have a lot of teams that wanna use it, but they have to understand the \[Temporal\] primitive of workflows and activities.
		- Netflix has a Simple Task wrapper. You don't actually register a workflow or register an activity. You just say here's a function I want to run and literally everything else is handled in the background. They wrap in the library.
		- A lot of people in Snap want to define Schedules and do an HTTP get or call a gRPC endpoint — that would like get massive adoption at Snap. We're going to build it out ourselves eventually. That's like an easy thing that would easily get a lot of adoption. And like we've asked internally and polled internally and that's what a lot of people want, that’s an easy one I think.”
	### [Datapay - May 2025](slackMessage://temporaltechnologies.slack.com/C08GZQN1C80/1747357386.170499/1747357386.170499) - domain teams should only write activities to get started with Temporal
	- **Doesn't want to expose the fullness of Temporal to their domain teams (too much)**
	- **Wants the domain teams to just write Activities to avoid having to learn about Workflows to get started.**
	### [Cursor - Aug 2025](slackMessage://temporaltechnologies.slack.com/C052L7LC50U/1755713211.339689/1755708197.131309) - interested in standalone activities
	**Durable Queue:** Reliable single-Activity activities (e.g., Linear webhooks, failure handling).
	Followup: [We’ll use it as long as we can schedule activities](slackMessage://temporaltechnologies.slack.com/C09EB1D10UD/1769463616.076859/1769462129.345829)
	### [Roblox - Aug 2025](slackMessage://temporaltechnologies.slack.com/C04S448HNAJ/1755706707.772359/1755706707.772359) - interested in standalone activities
	Are we actively pursuing the single activity / simple task feature with a customer in mind? We were doing this with Stripe in mind for a time. Wondering if there's anyone else, and if we're at the point of being able to estimate costs and how they stack up against queueing frameworks.
	[Thread in team-growth](slackMessage://temporaltechnologies.slack.com/C04S448HNAJ/1755706707.772359/1755706707.772359) \| [Today at 9:18 AM](slackMessage://temporaltechnologies.slack.com/C04S448HNAJ/1755706707.772359/1755706707.772359) \| [View message](slackMessage://temporaltechnologies.slack.com/C04S448HNAJ/1755706707.772359/1755706707.772359)
	## [Coinbase](slackMessage://temporaltechnologies.slack.com/C072RF80FJS/1764719242.085389/1764697553.912879)
	- <mention-page url="https://www.notion.so/2e38fc5677388034baf5d1a45cd4bce2"/> 
	- good with the product spec; want start delay for some use cases
	![](https://ca.slack-edge.com/TT31S6VK5-U06D9QF1DT3-b81ec7aa2eac-24)
	See also: [additional customer notes](/1a68fc567738800f8298c3b6441b06dd?pvs=25)
	## [Apollo Global](slackMessage://temporaltechnologies.slack.com/C08HAT13SF6/1769611754.092689/1769611754.092689)
	Standalone Sctivities: The concept looks to be interesting with us having a good number of use cases that will need this or one off triggers to execute such activities. However, to be able to use this properly for apollo `we need to be able to schedule activities as well`. Cause a good chunk of our processes for the year is going to be batch jobs.
# Goals & Success Metrics
<span discussion-urls="discussion://1f18fc56-7738-8032-ab56-001c925ce4dc">**Goals**</span>**:**
- <span discussion-urls="discussion://21d8fc56-7738-80d6-a3b6-001c30943078,discussion://21d8fc56-7738-80fb-bec1-001ca96d8d42">Provide a more cost-effective single-Activity async solution for high-volume single-Activity Workflow use cases</span>
- Enable cross-team use by making Nexus calls as cheap as an Activity execution
<span discussion-urls="discussion://21c8fc56-7738-80dd-8cb6-001ce4eff946">**Success Metrics**</span>**:**
- reduce cost
	- \~2x reduced cost for durable single-Activity use cases
	- \~4x cost reduction with no visibility (future)
- improve gross margin
	- 10% gross margin improvement vs. single-Activity Workflows
- net ARR with Standalone Activities - see [financial model](https://docs.google.com/spreadsheets/d/1N18VVUk8lErTlcNqzEC0f1uj5Wwqk-Au1zAgQ-xxGv4/edit?gid=160780569#gid=160780569) for details
	- FY27 - \$5M ARR increase (including cannibalization)
	- FY28 - \$14M ARR increase (including cannibalization)
- new business
	- 90% of single-Activity use cases to acquire a broader base of users
- attach
	- 90% attach of our paid customer base
	- 50% attach for priority/fairness/ordering - key differentiator vs. Kafka/SQS for single-Activity use cases
# Target personas & roles
## Platform team
### JTBD
- provide an internal developer platform that provides orchestration capabilities to app dev teams that have single-Activity use cases that are often high-volume and need lower cost
### <span discussion-urls="discussion://1f18fc56-7738-8056-b0ab-001c2e1ac25c">Pains</span>
- want to adopt Temporal’s all async use cases, but Workflows + Activities are too expensive for several non-business-critical and high-volume uses cases like webhook delivery, emails, etc.
- can’t recommend going “all in” on Temporal as the standard way to manage async things within their company without a cost-effective solution for single-Activity high-volume use cases
- orchestration platform isn’t 100% backed by Temporal - resulting in a fragmented multi-tool approach with additional Platform team burden for education, ops, and support
### <span discussion-urls="discussion://21d8fc56-7738-8032-94ae-001cdce2077d">Gains</span>
- \$ cost savings the platform team has to do in education, support, integrations, etc. when there are multiple tools that are consolidated into a single tool (Temporal)
## App dev team
### JTBD
- build apps / services that need to process single-Activity tasks with durability, reliability with reasonable <span discussion-urls="discussion://2808fc56-7738-8005-b725-001ce14eacac">cost</span>
- <span discussion-urls="discussion://27f8fc56-7738-80ff-abb2-001cb30eb97d">write AI agents using Nexus + Standalone Activities</span>
### Pains
- Need reliable execution for single-Activity tasks and incumbent solutions are lacking
- Full Temporal Workflows are too expensive for AI agent use cases, high-volume workloads or those with lower business-value
- No unified programming model for all async backend things - costs more to learn/maintain
### Gains
- build apps/service faster with one Tool (Temporal) a unified programming model for all async backend things
	- unlocked since Standalone Activities are economic for all/most async task processing use cases, so teams are not forced to choose/learn multiple different tools.
# **Scope & Key Features**
- 👉** **[**Product Spec: Standalone Activities for Lower-Cost single-Activity Workflows**](https://docs.google.com/document/d/1MjtspkF2lTh0ThcVBJaPSWb6dr5786nhQH93ifSWSpA/edit)
- 👉 [**UX Designs: Standalone Activities**](https://www.figma.com/design/8dSoDU2sWeQf7CgZzTqzeW/Cloud-UI?node-id=25886-5045&t=g5b1xbmGBvCA6d86-1)
- 👉 [**Engineering Blueprint: Standalone Activities**](/21e8fc567738801aa9bbc2c850175a50?pvs=25)
### <span discussion-urls="discussion://2808fc56-7738-809e-8a68-001ca7ac8568">**In **</span><span discussion-urls="discussion://21d8fc56-7738-8057-b6cc-001cc3c06eb1,discussion://2808fc56-7738-809e-8a68-001ca7ac8568">**Scope**</span><span discussion-urls="discussion://2808fc56-7738-809e-8a68-001ca7ac8568">** (initial GA)**</span>
- 👉** **[**Product Spec: Standalone Activities for Lower-Cost single-Activity Workflows**](https://docs.google.com/document/d/1MjtspkF2lTh0ThcVBJaPSWb6dr5786nhQH93ifSWSpA/edit)
- <span discussion-urls="discussion://21d8fc56-7738-80a6-9d10-001ce5bd2152">Standalone Activities that may be invoked via the Temporal Client SDK, along these lines:</span>
	- Streamlined Standalone Activity state machine
		- [Lower cost for us](https://link.excalidraw.com/l/5BhtDRxD0Iq/5PyMtVNSvCh) (for 1st Activity attempt, but retries & heartbeats will cost the same and could quickly overwhelm)
		- <span discussion-urls="discussion://26c8fc56-7738-8044-9773-001c0d834586">No Workflow event history</span>
		- <span discussion-urls="discussion://21c8fc56-7738-804b-9432-001cb38859c6,discussion://2808fc56-7738-80f8-a9f1-001c40531904">No </span><span discussion-urls="discussion://2808fc56-7738-80f8-a9f1-001c40531904">interactivity (no signals, updates - it’s an activity)</span>
		- Addressable: can get a handle (activity ID, run id) and get the result, (un)pause, reset, cancel, terminate
			- [manual completion by ID](https://docs.temporal.io/develop/typescript/asynchronous-activity-completion) (or token): ignore activity return and wait for external manual completion to be called
			- dedupe - similar to how WF dedupe work: e.g. conflict policy: (USE_EXISTING, …), reuse policy: (REJECT_DUPLICATES, …)
		- Task queue as basis for retries - at-least-once execution (or at most once if retry max attempts is 1)
		- Available (opt-in) priority and fairness - key differentiator vs. Kafka/SQS-based systems
		- Visibility - using <mention-page url="https://www.notion.so/2008fc56773880e2bf05d19292f93bdc"/> **(same as for Schedules)**
		- <span discussion-urls="discussion://27e8fc56-7738-8025-ac59-001c157eca8c">Open Metrics - parity with Workflow high cardinality visibility</span>
		- Internal Metrics to track usage similar to Workflows, but for Standalone Activities - per account/namespace
		- All Temporal SDKs supported for GA: Python, Java, then Go, Typescript, and other SDKs
- <span discussion-urls="discussion://2848fc56-7738-8065-9e4c-001cf31513cd">An Activity Definition will remain what an Activity Definition is today</span>:
	- Update 2/10/26: <mention-page url="https://www.notion.so/2f68fc567738806aa799d00901a77725"/> 
	- We won’t extend Activity Definitions to support additional features like an Actor model or interactivity (signal, update, …)
		- these will be considered in other Temporal primitives (perhaps one we already have like a Workflow)
	- No modifications to the Temporal SDK Activity Definition API
		- e.g. [https://pkg.go.dev/go.temporal.io/sdk/activity#pkg-functions](https://pkg.go.dev/go.temporal.io/sdk/activity#pkg-functions)
	- See also: [Out of Scope](/1ee8fc567738806d8b6fe8e2eeae0fc4?pvs=25#1ee8fc56773880f88915e50c31a84f82) section
	---
### <span discussion-urls="discussion://27f8fc56-7738-80a1-b487-001cb926af51">Post GA (Under Consideration)</span> - pending additional customer feedback
- **Start Delay (**[**Block**](/2768fc56773880cfa0cad5dff6074a29?pvs=25#27f8fc56773880beb55eec4d8f510dda)**, **[**Coinbase**](slackMessage://temporaltechnologies.slack.com/C072RF80FJS/1764719242.085389/1764697553.912879)**) ****`Adoption Blocker for some use cases`**
	- 👉 [Block needs Start Delay](/2768fc56773880cfa0cad5dff6074a29?pvs=25#27f8fc56773880beb55eec4d8f510dda) to move users off their existing Simple Task → Standalone Activities
		- [used when a transaction must be verified complete or cancelled after some time](/2768fc56773880cfa0cad5dff6074a29?pvs=25#27f8fc56773880a197adfd7b8211a0f9) (3 days, …)
		- if the cost is the same for them: workflow + activity (2) vs. activity + start delay (2) , the motivation goes down to migrate to save money
		- in both cases this prevents cannibalization
	- [Coinbase would like start delay for some use cases](slackMessage://temporaltechnologies.slack.com/C072RF80FJS/1764719242.085389/1764697553.912879)
	- 👉 Start Delay would unlock significant new use cases, and sounds like minimal additional cost \~1 additional write, so we could:
		- TBD: charge 1 action for Start Delay - to minimize cannibalization further for WF conversions, but could impact adoption for new use cases
		- don’t charge 1 action - we already have margin buffer vs. single-Activity Workflows so could probably absorb this without margin impact
	- Note: Workflows support [Start Delay](https://temporal.io/change-log/new-feature-start-delay-workflow) today which essentially gives away a Timer for free.
- **Start Standalone Activity from Schedule. ****`Adoption Blocker`**
	- 👉** **[**Coinbase**](slackMessage://temporaltechnologies.slack.com/C09EB1D10UD/1768574257.715479/1768574203.504429)**, **<span discussion-urls="discussion://3118fc56-7738-80f4-8350-001c9255df2f">[**Cursor/Anysphere**](slackMessage://temporaltechnologies.slack.com/C09EB1D10UD/1769463616.076859/1769462129.345829)</span>**, **[**Apollo Global**](slackMessage://temporaltechnologies.slack.com/C08HAT13SF6/1769611754.092689/1769611754.092689)**, Stripe (Tech Day 2/27/26)**
	- [Cursor: Not putting the request for Standalone Activities until we have scheduling support](slackMessage://temporaltechnologies.slack.com/C09EB1D10UD/1771972196.886379/1769462129.345829)
- **Start Standalone Activity from Nexus.**
	- Customer requests: [Abridge](slackMessage://temporaltechnologies.slack.com/C082M9DNY2U/1772130965.785009/1772130965.785009)
	- Back user-defined Nexus operations with standalone `ActivityRunOperation`.
	- impl detail: Callbacks: Standalone Activity Completion Callbacks similar to Workflow Completion Callbacks - will be used by Nexus, schedules, …
- **Start Standalone Activity from Workflow** - to decouple the lifecycle
- **Export ****`Adoption Blocker`**
	- 👉** **[**Block needs Export for Standalone Activities for auditing purposes**](slackMessage://temporaltechnologies.slack.com/C08GZQN1C80/1763142057.401419/1761685364.093549)**, similar to Workflow export - **[**see internal discussion **](slackMessage://temporaltechnologies.slack.com/C08GZQN1C80/1763590560.911049/1761685364.093549)[🧵](slackMessage://temporaltechnologies.slack.com/C08GZQN1C80/1763590560.911049/1761685364.093549)
	- [Stripe needs Export for compliance purposes.](slackMessage://temporaltechnologies.slack.com/C03HRBUJM3M/1771014330.663289/1770917776.030289) [Follow up request](slackMessage://temporaltechnologies.slack.com/C03HRBUJM3M/1771363890.700139/1771363871.386119).
	- Charge 1 action like Workflow export today, and consider cost/billing optimizations later
	- Note: All top level CHASM primitives, like Workflows, Standalone Activities, … will need export.
- Batch operation on Standalone Activities (terminate, cancel, and the operator commands like pause, update options, …).
- **Activity flow control: rate/concurrency limits**
	- [Stripe previously requested an Activity rate/concurrency limit](https://www.notion.so/temporalio/Activity-Concurrency-Limit-1f58fc56773880ffa35cc2808da3b3b9) as a high priority ask (e.g. to protect a DB) and also for webhook use cases
	- [Rippling who also need rate/concurrency control to protect backend APIs.](/2e38fc56773880cd8a30f2074b50dd14?pvs=25)
- ID Space Uniqueness Checks Across Workflow and Activity ID Spaces
	- [Block needs this to migrate](/2768fc56773880cfa0cad5dff6074a29?pvs=25#27f8fc56773880beb55eec4d8f510dda) from Workflow-backed Simple Tasks → Standalone Activities
	- Behavior: try to start an Activity and have it fail if a Workflow is running with the same ID, likely in combo with and additional USE_EXISTING_ONLY_IF_RUNNING
- Additional [cost optimizations](https://link.excalidraw.com/l/5BhtDRxD0Iq/5PyMtVNSvCh)
	- 👉 Opt-out: visibility for lower price point
		- list things, current state, search attributes - 3 writes of 8 total (Coinbase)
		- see [related pricing/cogs discussion thread](slackMessage://temporaltechnologies.slack.com/C08GZQN1C80/1765398999.016769/1765398999.016769)
	- Optimistic start: 1/2 price
	- Eager start: saves 1 write
	- Opt-out: addressability (maybe, but this cuts too much value add)
	- Zero storage retention support (less than 1 day)
		- Needs customer validation, but for ultra-low cost storage can become the dominant cost pretty quickly and we don’t want to get priced out
		- Now that we’re moving to Retained storage after SAA is Closed, this becomes less of an issue
			- see <mention-page url="https://www.notion.so/2ce8fc56773881958fb4d43b37c2c7b7"/>
		[Storage cost model](https://docs.google.com/spreadsheets/d/1N18VVUk8lErTlcNqzEC0f1uj5Wwqk-Au1zAgQ-xxGv4/edit?gid=626695404#gid=626695404) - see [discussion thread](slackMessage://temporaltechnologies.slack.com/C09EB1D10UD/1767985679.779779/1767824939.561979) 🧵
		![](https://prod-files-secure.s3.us-west-2.amazonaws.com/d9be1e2b-72e6-41ae-9b0d-bb3dc6fda325/cc4f6651-a7f2-4991-b14a-14e7816d751f/image.png?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Content-Sha256=UNSIGNED-PAYLOAD&X-Amz-Credential=ASIAZI2LB466Q7NXIPVK%2F20260419%2Fus-west-2%2Fs3%2Faws4_request&X-Amz-Date=20260419T122131Z&X-Amz-Expires=3600&X-Amz-Security-Token=IQoJb3JpZ2luX2VjEDkaCXVzLXdlc3QtMiJHMEUCIQDDgOxmHt67y5Kdfkvx%2F1pMi9ByRn0xdgLHvzUdEAHLzAIgfHTYOensHnxxMUTvjod7PVhlgjEhxK6IEuUs5A1U%2Fkcq%2FwMIAhAAGgw2Mzc0MjMxODM4MDUiDN6JabxQ6sr9MQOKSyrcA5gdJkdlDrR7H%2B4M3bs%2BF%2BXuBYvh%2BwRIj3W%2BVwIXlOBkev%2Bi35Pvgah4mDDxnbMwWzJ0rvZwAfiJz%2Be%2BUFe6x7JtHG8xhDcrVZsIZXdwhF7FuH1gaPuKkF6SW0COvI%2BECqDB4qt4ShdWDtT131Yf9FvbtGpuNqFvB4y75LaItOUCz08XKjrVKkvvVaUM%2BF68EMKeKwR0Qj6E433lTcLOM855JJZbZkdwWaW52U%2BHflv7%2BlRq48i0KCJsXXOepCtWJBCF9kkbnVIPC7l6NRWmppWjLLXDBUgf5B7btaEwsa8Z5g%2Be%2BS%2Be%2Bpgejq6NZdccQbeAG7UkSnKL0iTAM710SjhSYG%2Fxf1Ovq4G9LYXQKQco8I4v0eIi%2BEK%2F8UlyCq7O4M%2BCY0YQaGot2W2J41U40mG41AR%2BKKDudF0m%2FOqt4W%2BF1jGbrSRbISaFJz%2FiMx864MFUOFwHCldoAeCMV%2BJ7FE8AXiUyS63QA8blFUpFpII2h7hAZDj%2FplbFbUBIpLiMUPd9q8PFyre82Xf%2FrAL70ty17zui4bTc3spg6RLDwqPHaLNiPrD3TNX4HlEBaKxs2LLaKyid0ufSlFtUtE5Jccf9SBYsz%2BTmj1pg0hcq2O61rntjwEiKUPk0Y7FXMKqvks8GOqUBDL%2FcJ3X2el1Emv97YxdMQ2qLNT563fwq95%2BsT3i%2FzGwUtSRWQQCNrLi%2BWtriaLXm7B8%2FnWyEeXjeRgbxJsEKwfemK5Wxj81XODwH%2Fg4utJWjeL2ALrGn3u%2FoAW%2BhsVh5C5tbgHeB%2F6SjOgLGu0bb7yU01iMijyB7DtqXlmRwc2txffXGWgzBo6YrPmMiifHjBGC4ax%2B157x077Or3Wgg2nk29V25&X-Amz-Signature=ff5c8c4d6b786ce6e95c40305967adda1f94ee16fa9d4b82ac5147b53cb51e5a&X-Amz-SignedHeaders=host&x-amz-checksum-mode=ENABLED&x-id=GetObject)
		[https://docs.temporal.io/cloud/pricing#payg-storage-pricing](https://docs.temporal.io/cloud/pricing#payg-storage-pricing)
		<table header-row="true">
<tr>
<td>**Storage**</td>
<td>**Price per GBh (USD)**</td>
</tr>
<tr>
<td>Retained</td>
<td>\$0.00105</td>
</tr>
<tr>
<td>Active</td>
<td>\$0.042</td>
</tr>
		</table>
- Over time we should consider implementing all these lower cost options, as long as **the Activity Definition programming model does not change<br>**1. Pure queue. Non addressable at least once with limited duration of tasks and retries. Similar to SQS. Cheapest.<br>2. Ordered stream. Ala Kafka partitions.<br>3. At least once and not addressable but with unlimited duration and retries.<br>
### <span discussion-urls="discussion://21d8fc56-7738-8091-8d96-001c3e3f923d">**Out of Scope**</span>** (for Standalone Activities for all time)**
- The following things are out of scope for Standalone Activities and will be considered separate from Activities (i.e. [we are not changing the shape of an Activity Definition](/1ee8fc567738806d8b6fe8e2eeae0fc4?pvs=25#2848fc5677388038a4f6d399ea9e6c75)):
	- **Actor model** similar to Akka actor model (or perhaps Restate Virtual Objects but each function is actually a workflow, but there is no entity workflow) 
		- (future) key-value store - e.g. for lightweight actors; retry load and continue
		- use k/v to communicate across calls to addressable entity
		- suitable for interaction: signal, update, …
		- if fails and restarts update the address and send (for actor use case)
<empty-block/>
### Key Features & Differentiators
- **Execute any Temporal Activity as a top-level primitive** without the overhead of a Workflow.
- **Native async task processing model**: schedule -\> dispatch -\> process -\> result
- **No head-of-line blocking** - a slow task doesn’t block the dispatch of other tasks
- **Arbitrary length tasks** with heartbeats to checkpoint progress and handle worker failures
- **At-least-once execution** by default with native retry policy & timeouts
- **At-most-once execution** if retry max attempts is 1
- **Addressable** - get a activity ID / run id and get the result, (un)pause, reset, cancel, terminate
- **Dedupe** - conflict policy: (USE_EXISTING, …), reuse policy: (REJECT_DUPLICATES, …)
- **Priority and fairness** - multi-tenant fairness, weighted priority tiers (e.g. high/medium/low), and safeguards against starvation of lower-weighted tasks, plus no head-of line blocking
- **Visibility** - list executions and see current status, retry count, last error, …
- **Manual completion by ID** (or token): ignore activity return and wait for external completion
- **Dual use** - execute Activities In-Workflow or Standalone with no Worker code changes - clean upgrade path to full Workflow orchestration
# <span discussion-urls="discussion://21d8fc56-7738-805e-9b0e-001c821c79c0">Competitive</span> Analysis
 👉 GTM Hub: <mention-page url="https://www.notion.so/2638fc56773880f19710f33cc12a8e36"/> 👈
### Takeout play: vs. Celery, Sidekiq, Faktory, SQS+Redis+custom
We should position Standalone Activities as a competitive takeout vs. Celery/Sidekik/SQS+Redis+custom
> Since we already need to migrate off Sidekiq + Redis (for our Rails -\> Go migration) we could simplify our tools with Temporal `Workflows` & `Jobs` (Standalone Activities) instead of adding a new job processing framework - a two birds one stone situation —JumpCloud
![](https://prod-files-secure.s3.us-west-2.amazonaws.com/d9be1e2b-72e6-41ae-9b0d-bb3dc6fda325/77ca949a-2876-4b91-80a2-cdf712182a63/image.png?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Content-Sha256=UNSIGNED-PAYLOAD&X-Amz-Credential=ASIAZI2LB466Q7NXIPVK%2F20260419%2Fus-west-2%2Fs3%2Faws4_request&X-Amz-Date=20260419T122131Z&X-Amz-Expires=3600&X-Amz-Security-Token=IQoJb3JpZ2luX2VjEDkaCXVzLXdlc3QtMiJHMEUCIQDDgOxmHt67y5Kdfkvx%2F1pMi9ByRn0xdgLHvzUdEAHLzAIgfHTYOensHnxxMUTvjod7PVhlgjEhxK6IEuUs5A1U%2Fkcq%2FwMIAhAAGgw2Mzc0MjMxODM4MDUiDN6JabxQ6sr9MQOKSyrcA5gdJkdlDrR7H%2B4M3bs%2BF%2BXuBYvh%2BwRIj3W%2BVwIXlOBkev%2Bi35Pvgah4mDDxnbMwWzJ0rvZwAfiJz%2Be%2BUFe6x7JtHG8xhDcrVZsIZXdwhF7FuH1gaPuKkF6SW0COvI%2BECqDB4qt4ShdWDtT131Yf9FvbtGpuNqFvB4y75LaItOUCz08XKjrVKkvvVaUM%2BF68EMKeKwR0Qj6E433lTcLOM855JJZbZkdwWaW52U%2BHflv7%2BlRq48i0KCJsXXOepCtWJBCF9kkbnVIPC7l6NRWmppWjLLXDBUgf5B7btaEwsa8Z5g%2Be%2BS%2Be%2Bpgejq6NZdccQbeAG7UkSnKL0iTAM710SjhSYG%2Fxf1Ovq4G9LYXQKQco8I4v0eIi%2BEK%2F8UlyCq7O4M%2BCY0YQaGot2W2J41U40mG41AR%2BKKDudF0m%2FOqt4W%2BF1jGbrSRbISaFJz%2FiMx864MFUOFwHCldoAeCMV%2BJ7FE8AXiUyS63QA8blFUpFpII2h7hAZDj%2FplbFbUBIpLiMUPd9q8PFyre82Xf%2FrAL70ty17zui4bTc3spg6RLDwqPHaLNiPrD3TNX4HlEBaKxs2LLaKyid0ufSlFtUtE5Jccf9SBYsz%2BTmj1pg0hcq2O61rntjwEiKUPk0Y7FXMKqvks8GOqUBDL%2FcJ3X2el1Emv97YxdMQ2qLNT563fwq95%2BsT3i%2FzGwUtSRWQQCNrLi%2BWtriaLXm7B8%2FnWyEeXjeRgbxJsEKwfemK5Wxj81XODwH%2Fg4utJWjeL2ALrGn3u%2FoAW%2BhsVh5C5tbgHeB%2F6SjOgLGu0bb7yU01iMijyB7DtqXlmRwc2txffXGWgzBo6YrPmMiifHjBGC4ax%2B157x077Or3Wgg2nk29V25&X-Amz-Signature=19e3bdc7c74470cec9ea05f4cf993cea73a9550b247fd09728a7a4dc69851a05&X-Amz-SignedHeaders=host&x-amz-checksum-mode=ENABLED&x-id=GetObject)
### **Competitive Cost Analysis**
👉<mention-page url="https://www.notion.so/2e38fc56773880ffaf8be9ae6c21edfb"/> - 300M jobs/day
- <mention-page url="https://www.notion.so/2aa8fc56773880a69fc0ee0361e18009">Appendix A - Estimated Competitive Pricing</mention-page>  - 25M jobs/month - (including build your own)
<table header-row="true">
<tr>
<td>**System / Framework**</td>
<td>**Event Store**</td>
<td>**Billable unit**</td>
<td>**Units (10M tasks)**</td>
<td>**Unit price**</td>
<td>**Cost (approx)**</td>
<td>**Ops/Infra Overhead**</td>
<td>**Total Cost**</td>
</tr>
<tr>
<td>**Temporal Cloud**</td>
<td>Internal (Temporal DB)</td>
<td>Action</td>
<td>10,000,000</td>
<td>\$50 / 1M</td>
<td>**\$500**</td>
<td>None (SaaS)</td>
<td>**\$500**</td>
</tr>
<tr>
<td>**Inngest Cloud**</td>
<td>Internal (SaaS log)</td>
<td><span discussion-urls="discussion://2858fc56-7738-8010-92be-001cbb565bde">Execution</span></td>
<td>10,000,000</td>
<td>\$50 / 1M</td>
<td>**\$500**</td>
<td><span discussion-urls="discussion://2808fc56-7738-8074-bd99-001c18f54a4e">None (SaaS)</span></td>
<td>**\$500**</td>
</tr>
<tr>
<td>**Restate (self-hosted)**</td>
<td>Kafka + RocksDB</td>
<td>Checkpoint write</td>
<td>10,000,000</td>
<td>Infra<span discussion-urls="discussion://2858fc56-7738-80e2-9563-001c1639a1a9"> </span>only</td>
<td>**\~\$500–\$2,000 / mo**</td>
<td>Ops: \~ \$500–\$1,000/mo</td>
<td>**\~\$1,000–\$3,000 / mo**</td>
</tr>
<tr>
<td>**Celery / Sidekiq / RQ / BullMQ**</td>
<td>Redis (queue & state)</td>
<td>Enqueue + Ack</td>
<td>N/A</td>
<td>Infra only</td>
<td>**\~\$500–\$2,000 / mo**</td>
<td>Ops: \~\$500–\$1,000/mo</td>
<td>**\~\$1,000–\$3,000 / mo**</td>
</tr>
<tr>
<td>**Celery**</td>
<td>RabbitMQ + Redis</td>
<td>Enqueue + Ack</td>
<td>N/A</td>
<td>Infra only</td>
<td>**\~\$800–\$2,500 / mo**</td>
<td>Ops: \~\$500–\$1,000/mo</td>
<td>**\~\$1,300–\$3,500 / mo**</td>
</tr>
<tr>
<td>**SQS + Celery / Laravel Queue**</td>
<td>AWS SQS + optional<br>Redis (result backend)</td>
<td>API requests</td>
<td>30M (no batching) 3M (batch=10)</td>
<td>\$0.40–\$0.50 / 1M</td>
<td>**\$1.2–\$15**</td>
<td>Worker infra + monitoring: \~\$500–\$1,500/mo</td>
<td>**\~\$500–\$1,515 / mo**</td>
</tr>
</table>
<empty-block/>
### **Competitive **<span discussion-urls="discussion://3338fc56-7738-80e9-a0ad-001c24e0c637">**Comparison**</span>
👉 <mention-page url="https://www.notion.so/2e98fc56773880bda6deeeec2068fb6b"/> 👈
Key differentiators include native priority and fairness, scalability, deduplication, addressability, observability, adjacency to Temporal Workflow, and re-use of Activities for standalone or in-workflow execution — and above all a simple programming model to write resilient code with fully integrated observability.
<table header-row="true">
<tr>
<td>**Feature**</td>
<td>**Temporal (Standalone Activity)**</td>
<td>**Inngest (Durable Function)**</td>
<td>**Restate (Durable Function)**</td>
<td>**Celery (Redis)**</td>
<td>**Sidekiq (Redis)**</td>
<td>**RQ (Redis)**</td>
<td>**BullMQ (Redis)**</td>
<td>**Celery (RabbitMQ)**</td>
<td>**SQS + Celery**</td>
<td>**SQS + Laravel**</td>
</tr>
<tr>
<td>Execute single function reliably</td>
<td>✅ Yes (Activity primitive)</td>
<td>✅ Yes (durable function invocation)</td>
<td>✅ Yes (durable function invocation)</td>
<td>✅ Yes</td>
<td>✅ Yes</td>
<td>✅ Yes</td>
<td>✅ Yes</td>
<td>✅ Yes</td>
<td>✅ Yes</td>
<td>✅ Yes</td>
</tr>
<tr>
<td>HOLB behavior (cross-function dispatch)</td>
<td>✅ Avoided: **per-queue partitions** + **priority subqueues**</td>
<td>✅ Independent: no HOLB across functions (scaling opaque)</td>
<td>✅ Independent: no HOLB across durable functions (partitioned model)</td>
<td>⚠️ **Per-queue FIFO (dispatch-only)**; manual multiple queues for parallelism</td>
<td>⚠️ Same as Celery (Redis)</td>
<td>⚠️ Same as Celery (Redis)</td>
<td>⚠️ Same as Celery (Redis)</td>
<td>⚠️ Same as Celery (Redis)</td>
<td>⚠️ **Per-queue FIFO (visibility timeout redelivery)**</td>
<td>⚠️ Same as SQS + Celery</td>
</tr>
<tr>
<td>Priority support</td>
<td>✅ Native multi-tier priorities (high, medium, low, custom)</td>
<td>✅ Supported: backlog reordering via ±600s priority factor</td>
<td>❌ None documented</td>
<td>⚠️ Manual: multiple queues required</td>
<td>⚠️ Same as Celery</td>
<td>❌ None</td>
<td>⚠️ Limited (basic job priorities)</td>
<td>⚠️ Manual via multiple queues</td>
<td>❌ FIFO only</td>
<td>❌ FIFO only</td>
</tr>
<tr>
<td>Fairness model</td>
<td>✅ **Weighted anti-starvation fairness** ensures low-priority tasks still progress</td>
<td>⚠️ Local backlog reshuffling only; no global fairness</td>
<td>❌ None; durable functions run FIFO within partitions</td>
<td>❌ None; fairness must be engineered manually</td>
<td>❌ None</td>
<td>❌ None</td>
<td>❌ None</td>
<td>❌ None</td>
<td>❌ None</td>
<td>❌ None</td>
</tr>
<tr>
<td>Scalability</td>
<td>✅ **Automatic**: partitions + subqueues managed by Matching Service</td>
<td>✅  Automatic; functions auto-scale (sharded internally, not exposed)</td>
<td>✅ Automatic partitioning/sharding across cluster; Kafka-like</td>
<td>❌ **Manual**: must create multiple queues; each = one Redis key/shard</td>
<td>❌ Manual multi-queue</td>
<td>❌ Manual multi-queue</td>
<td>❌ Manual multi-queue</td>
<td>⚠️ Per-queue throughput limit; manual scaling</td>
<td>⚠️ Multiple SQS queues needed; no fairness</td>
<td>⚠️ Same as SQS + Celery</td>
</tr>
<tr>
<td>Deduplication</td>
<td>✅ Conflict & reuse policies</td>
<td>⚠️ Idempotency keys</td>
<td>⚠️ Replay dedupe</td>
<td>❌ None</td>
<td>❌ None</td>
<td>❌ None</td>
<td>❌ None</td>
<td>❌ None</td>
<td>⚠️ FIFO dedupe only</td>
<td>⚠️ FIFO dedupe only</td>
</tr>
<tr>
<td>Addressability (pause, cancel, reset)</td>
<td>✅ Full (pause, reset, cancel, manual complete)</td>
<td>✅ Cancel/pause supported per function</td>
<td>✅ Cancel supported per function (reset less clear)</td>
<td>❌ Kill job only</td>
<td>⚠️ Cancel in Pro</td>
<td>❌ Kill job only</td>
<td>⚠️ Cancel via API</td>
<td>❌ Kill job only</td>
<td>❌ None native</td>
<td>⚠️ Basic cancel</td>
</tr>
<tr>
<td>Visibility (status, retries, errors)</td>
<td>✅ Full: list executions, retries, last error, status</td>
<td>⚠️ Console: function logs & metadata</td>
<td>⚠️ Function logs & state snapshots</td>
<td>⚠️ Limited (Flower UI)</td>
<td>⚠️ Sidekiq Web UI (Pro richer)</td>
<td>❌ Minimal</td>
<td>⚠️ Bull Board</td>
<td>⚠️ Limited (Flower UI)</td>
<td>⚠️ Queue-level only</td>
<td>⚠️ Queue-level only</td>
</tr>
<tr>
<td>Error handling / retry policy</td>
<td>✅ Native policies (timeouts, retries, exp backoff)</td>
<td>✅ Automatic retries per function</td>
<td>✅ Automatic retries per function</td>
<td>✅ Framework-level retries & DLQ</td>
<td>✅ Framework-level retries & DLQ</td>
<td>⚠️ Basic retries only</td>
<td>⚠️ Basic retries only</td>
<td>✅ Framework-level retries & DLQ</td>
<td>✅ Native retries & DLQ</td>
<td>✅ Native retries & DLQ</td>
</tr>
<tr>
<td>Delivery semantics</td>
<td>✅ **At-least-once** (default); **at-most-once** optional; **exactly-once **with deduplication. No ack on dispatch.</td>
<td>✅ At-least-once</td>
<td>✅ At-least-once</td>
<td>⚠️ At-least-once, but **ack at dispatch** unless configured</td>
<td>⚠️ Same as Celery</td>
<td>⚠️ Same as Celery</td>
<td>⚠️ Same as Celery</td>
<td>⚠️ Same as Celery</td>
<td>✅ At-least-once (visibility timeout)</td>
<td>✅ At-least-once (visibility timeout)</td>
</tr>
<tr>
<td>Arbitrary-length tasks (max runtime)</td>
<td>✅ **Unlimited** (days–months) with **heartbeats**</td>
<td>✅ Long running if if check-pointed mid-function with step.run</td>
<td>✅ Long running if check-pointed mid-function with await</td>
<td>❌ Typically **minutes** (broker/worker timeouts)</td>
<td>❌ Minutes</td>
<td>❌ Minutes</td>
<td>❌ Minutes</td>
<td>❌ Minutes</td>
<td>⚠️ 30m–12h via SQS visibility timeout</td>
<td>⚠️ Same as SQS + Celery</td>
</tr>
<tr>
<td>Progress checkpointing (single execution)</td>
<td>✅ Fine-grained: **heartbeats** save state mid-function</td>
<td>⚠️ At **step.run boundaries** only</td>
<td>⚠️ At **await boundaries** only</td>
<td>❌ Manual DB checkpoints</td>
<td>❌ Manual DB checkpoints</td>
<td>❌ Manual DB checkpoints</td>
<td>❌ Manual DB checkpoints</td>
<td>❌ Manual DB checkpoints</td>
<td>❌ Manual DB checkpoints</td>
<td>❌ Manual DB checkpoints</td>
</tr>
<tr>
<td>Throughput ceiling (per queue)</td>
<td>✅ Horizontal scale; no hot key</td>
<td>⚠️ Bound by infra (\~1k/sec typical per function)</td>
<td>✅ Partitioned scale-out (Kafka-like)</td>
<td>⚠️ \~1–5k/sec per queue; hot key bottleneck</td>
<td>⚠️ Same as Celery</td>
<td>⚠️ Same as Celery</td>
<td>⚠️ Same as Celery</td>
<td>⚠️ Similar bottlenecks</td>
<td>⚠️ API rate limited; scale via queues</td>
<td>⚠️ Same as SQS + Celery</td>
</tr>
</table>
<empty-block/>
**Inngest and Restate have Durable Functions**
![source: [Berlin Buzzwords Talk: Fixing the Hard Bits of Event Processing with Restate & Kafka](https://youtu.be/k21UkI27WOQ?feature=shared&t=190)](https://prod-files-secure.s3.us-west-2.amazonaws.com/d9be1e2b-72e6-41ae-9b0d-bb3dc6fda325/b748dee4-84e7-499f-9ad9-6776c879d89f/image.png?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Content-Sha256=UNSIGNED-PAYLOAD&X-Amz-Credential=ASIAZI2LB466Q7NXIPVK%2F20260419%2Fus-west-2%2Fs3%2Faws4_request&X-Amz-Date=20260419T122131Z&X-Amz-Expires=3600&X-Amz-Security-Token=IQoJb3JpZ2luX2VjEDkaCXVzLXdlc3QtMiJHMEUCIQDDgOxmHt67y5Kdfkvx%2F1pMi9ByRn0xdgLHvzUdEAHLzAIgfHTYOensHnxxMUTvjod7PVhlgjEhxK6IEuUs5A1U%2Fkcq%2FwMIAhAAGgw2Mzc0MjMxODM4MDUiDN6JabxQ6sr9MQOKSyrcA5gdJkdlDrR7H%2B4M3bs%2BF%2BXuBYvh%2BwRIj3W%2BVwIXlOBkev%2Bi35Pvgah4mDDxnbMwWzJ0rvZwAfiJz%2Be%2BUFe6x7JtHG8xhDcrVZsIZXdwhF7FuH1gaPuKkF6SW0COvI%2BECqDB4qt4ShdWDtT131Yf9FvbtGpuNqFvB4y75LaItOUCz08XKjrVKkvvVaUM%2BF68EMKeKwR0Qj6E433lTcLOM855JJZbZkdwWaW52U%2BHflv7%2BlRq48i0KCJsXXOepCtWJBCF9kkbnVIPC7l6NRWmppWjLLXDBUgf5B7btaEwsa8Z5g%2Be%2BS%2Be%2Bpgejq6NZdccQbeAG7UkSnKL0iTAM710SjhSYG%2Fxf1Ovq4G9LYXQKQco8I4v0eIi%2BEK%2F8UlyCq7O4M%2BCY0YQaGot2W2J41U40mG41AR%2BKKDudF0m%2FOqt4W%2BF1jGbrSRbISaFJz%2FiMx864MFUOFwHCldoAeCMV%2BJ7FE8AXiUyS63QA8blFUpFpII2h7hAZDj%2FplbFbUBIpLiMUPd9q8PFyre82Xf%2FrAL70ty17zui4bTc3spg6RLDwqPHaLNiPrD3TNX4HlEBaKxs2LLaKyid0ufSlFtUtE5Jccf9SBYsz%2BTmj1pg0hcq2O61rntjwEiKUPk0Y7FXMKqvks8GOqUBDL%2FcJ3X2el1Emv97YxdMQ2qLNT563fwq95%2BsT3i%2FzGwUtSRWQQCNrLi%2BWtriaLXm7B8%2FnWyEeXjeRgbxJsEKwfemK5Wxj81XODwH%2Fg4utJWjeL2ALrGn3u%2FoAW%2BhsVh5C5tbgHeB%2F6SjOgLGu0bb7yU01iMijyB7DtqXlmRwc2txffXGWgzBo6YrPmMiifHjBGC4ax%2B157x077Or3Wgg2nk29V25&X-Amz-Signature=58b3d2dd80abc4c1abe5efc7a7458ca97eeeedfbbff70cd92c2cba4588d42e71&X-Amz-SignedHeaders=host&x-amz-checksum-mode=ENABLED&x-id=GetObject)
![](https://prod-files-secure.s3.us-west-2.amazonaws.com/d9be1e2b-72e6-41ae-9b0d-bb3dc6fda325/b2907bd8-998d-4047-8787-05e2bf73a63c/image.png?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Content-Sha256=UNSIGNED-PAYLOAD&X-Amz-Credential=ASIAZI2LB466Q7NXIPVK%2F20260419%2Fus-west-2%2Fs3%2Faws4_request&X-Amz-Date=20260419T122131Z&X-Amz-Expires=3600&X-Amz-Security-Token=IQoJb3JpZ2luX2VjEDkaCXVzLXdlc3QtMiJHMEUCIQDDgOxmHt67y5Kdfkvx%2F1pMi9ByRn0xdgLHvzUdEAHLzAIgfHTYOensHnxxMUTvjod7PVhlgjEhxK6IEuUs5A1U%2Fkcq%2FwMIAhAAGgw2Mzc0MjMxODM4MDUiDN6JabxQ6sr9MQOKSyrcA5gdJkdlDrR7H%2B4M3bs%2BF%2BXuBYvh%2BwRIj3W%2BVwIXlOBkev%2Bi35Pvgah4mDDxnbMwWzJ0rvZwAfiJz%2Be%2BUFe6x7JtHG8xhDcrVZsIZXdwhF7FuH1gaPuKkF6SW0COvI%2BECqDB4qt4ShdWDtT131Yf9FvbtGpuNqFvB4y75LaItOUCz08XKjrVKkvvVaUM%2BF68EMKeKwR0Qj6E433lTcLOM855JJZbZkdwWaW52U%2BHflv7%2BlRq48i0KCJsXXOepCtWJBCF9kkbnVIPC7l6NRWmppWjLLXDBUgf5B7btaEwsa8Z5g%2Be%2BS%2Be%2Bpgejq6NZdccQbeAG7UkSnKL0iTAM710SjhSYG%2Fxf1Ovq4G9LYXQKQco8I4v0eIi%2BEK%2F8UlyCq7O4M%2BCY0YQaGot2W2J41U40mG41AR%2BKKDudF0m%2FOqt4W%2BF1jGbrSRbISaFJz%2FiMx864MFUOFwHCldoAeCMV%2BJ7FE8AXiUyS63QA8blFUpFpII2h7hAZDj%2FplbFbUBIpLiMUPd9q8PFyre82Xf%2FrAL70ty17zui4bTc3spg6RLDwqPHaLNiPrD3TNX4HlEBaKxs2LLaKyid0ufSlFtUtE5Jccf9SBYsz%2BTmj1pg0hcq2O61rntjwEiKUPk0Y7FXMKqvks8GOqUBDL%2FcJ3X2el1Emv97YxdMQ2qLNT563fwq95%2BsT3i%2FzGwUtSRWQQCNrLi%2BWtriaLXm7B8%2FnWyEeXjeRgbxJsEKwfemK5Wxj81XODwH%2Fg4utJWjeL2ALrGn3u%2FoAW%2BhsVh5C5tbgHeB%2F6SjOgLGu0bb7yU01iMijyB7DtqXlmRwc2txffXGWgzBo6YrPmMiifHjBGC4ax%2B157x077Or3Wgg2nk29V25&X-Amz-Signature=b5bbc5fe633f046504cd1ec896a62e6f90e0713e41ef2e5454c924d981f19aab&X-Amz-SignedHeaders=host&x-amz-checksum-mode=ENABLED&x-id=GetObject)
![[https://docs.restate.dev/concepts/services](https://docs.restate.dev/concepts/services)](https://prod-files-secure.s3.us-west-2.amazonaws.com/d9be1e2b-72e6-41ae-9b0d-bb3dc6fda325/0810e6d4-d8c7-49df-88b8-9919c2bd9404/image.png?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Content-Sha256=UNSIGNED-PAYLOAD&X-Amz-Credential=ASIAZI2LB466Q7NXIPVK%2F20260419%2Fus-west-2%2Fs3%2Faws4_request&X-Amz-Date=20260419T122131Z&X-Amz-Expires=3600&X-Amz-Security-Token=IQoJb3JpZ2luX2VjEDkaCXVzLXdlc3QtMiJHMEUCIQDDgOxmHt67y5Kdfkvx%2F1pMi9ByRn0xdgLHvzUdEAHLzAIgfHTYOensHnxxMUTvjod7PVhlgjEhxK6IEuUs5A1U%2Fkcq%2FwMIAhAAGgw2Mzc0MjMxODM4MDUiDN6JabxQ6sr9MQOKSyrcA5gdJkdlDrR7H%2B4M3bs%2BF%2BXuBYvh%2BwRIj3W%2BVwIXlOBkev%2Bi35Pvgah4mDDxnbMwWzJ0rvZwAfiJz%2Be%2BUFe6x7JtHG8xhDcrVZsIZXdwhF7FuH1gaPuKkF6SW0COvI%2BECqDB4qt4ShdWDtT131Yf9FvbtGpuNqFvB4y75LaItOUCz08XKjrVKkvvVaUM%2BF68EMKeKwR0Qj6E433lTcLOM855JJZbZkdwWaW52U%2BHflv7%2BlRq48i0KCJsXXOepCtWJBCF9kkbnVIPC7l6NRWmppWjLLXDBUgf5B7btaEwsa8Z5g%2Be%2BS%2Be%2Bpgejq6NZdccQbeAG7UkSnKL0iTAM710SjhSYG%2Fxf1Ovq4G9LYXQKQco8I4v0eIi%2BEK%2F8UlyCq7O4M%2BCY0YQaGot2W2J41U40mG41AR%2BKKDudF0m%2FOqt4W%2BF1jGbrSRbISaFJz%2FiMx864MFUOFwHCldoAeCMV%2BJ7FE8AXiUyS63QA8blFUpFpII2h7hAZDj%2FplbFbUBIpLiMUPd9q8PFyre82Xf%2FrAL70ty17zui4bTc3spg6RLDwqPHaLNiPrD3TNX4HlEBaKxs2LLaKyid0ufSlFtUtE5Jccf9SBYsz%2BTmj1pg0hcq2O61rntjwEiKUPk0Y7FXMKqvks8GOqUBDL%2FcJ3X2el1Emv97YxdMQ2qLNT563fwq95%2BsT3i%2FzGwUtSRWQQCNrLi%2BWtriaLXm7B8%2FnWyEeXjeRgbxJsEKwfemK5Wxj81XODwH%2Fg4utJWjeL2ALrGn3u%2FoAW%2BhsVh5C5tbgHeB%2F6SjOgLGu0bb7yU01iMijyB7DtqXlmRwc2txffXGWgzBo6YrPmMiifHjBGC4ax%2B157x077Or3Wgg2nk29V25&X-Amz-Signature=5f1253a7ac928798384ff6a9b466c36470e17471661f9c347d744d37e30eccc3&X-Amz-SignedHeaders=host&x-amz-checksum-mode=ENABLED&x-id=GetObject)
# Use Cases
 <mention-page url="https://www.notion.so/28c8fc567738804a839ad598718f1689"/>
- [high-level market segment breakdown with typical use cases](https://www.notion.so/temporalio/PRD-Standalone-Activities-for-lower-cost-single-Activity-Workflows-1ee8fc567738806d8b6fe8e2eeae0fc4?source=copy_link#21d8fc567738808a88b6d79f11bdc32e):
	- notifications, email, webhooks, payment retries, ledger tasks, anti-fraud jobs, adapters, high reliability job retries, cicd, ...
- Internal cloud data shows existing [namespaces with \~100% single-Activity Workflows](https://docs.google.com/spreadsheets/d/1N18VVUk8lErTlcNqzEC0f1uj5Wwqk-Au1zAgQ-xxGv4/edit?gid=149454017#gid=149454017) (often named after the use case)
	- Existing [namespace term frequency for namespaces with \~100% single-Activity Workflows](https://www.notion.so/temporalio/Standalone-Activity-Use-Cases-28c8fc567738804a839ad598718f1689?source=copy_link#28c8fc56773880f59321cb4b97f98637)
	- Ordered by most common: platform, ai, checker, payment, api, billing, notification, service, data, issuing, review, bom, account, card, insights, cash, event, core, merchant, disputes, migration, task, money, risk, offboard, interventions, comms, fraud, zip, token, user, partner, infra, support, reporting, messaging, preview, transfers, connections, institutions, config, files, ingest, group, compliance, tax, digest, onboarding, investigation, credential update, analytics, approvals, delivery, economy, audience, oauth, gateway, flow, report, agent, webhooks, balance, finance, sync, security, ml, payouts, subscriptions, pay, pricing, network, invoicing, model, goals, authentication, stream, defense, integration, wallets, filling, executor, copilot, preference, external, studio, catalog, shopify, runner, contacts, authz, order, minting, inflow, deposits, lake, writer, chat, banking, tracking, dataengg, validation, redaction, taxcode, search, assistant, PRs, scenarios, batch proc, debits, lending, fulfillment, funding, coverage, connect, ...
	- See the [use case doc](https://www.notion.so/temporalio/Standalone-Activity-Use-Cases-28c8fc567738804a839ad598718f1689?source=copy_link#28c8fc56773880f59321cb4b97f98637) for details
![](https://prod-files-secure.s3.us-west-2.amazonaws.com/d9be1e2b-72e6-41ae-9b0d-bb3dc6fda325/c286bc72-e4cd-4976-99c4-3721d1e43b4d/image.png?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Content-Sha256=UNSIGNED-PAYLOAD&X-Amz-Credential=ASIAZI2LB466Q7NXIPVK%2F20260419%2Fus-west-2%2Fs3%2Faws4_request&X-Amz-Date=20260419T122131Z&X-Amz-Expires=3600&X-Amz-Security-Token=IQoJb3JpZ2luX2VjEDkaCXVzLXdlc3QtMiJHMEUCIQDDgOxmHt67y5Kdfkvx%2F1pMi9ByRn0xdgLHvzUdEAHLzAIgfHTYOensHnxxMUTvjod7PVhlgjEhxK6IEuUs5A1U%2Fkcq%2FwMIAhAAGgw2Mzc0MjMxODM4MDUiDN6JabxQ6sr9MQOKSyrcA5gdJkdlDrR7H%2B4M3bs%2BF%2BXuBYvh%2BwRIj3W%2BVwIXlOBkev%2Bi35Pvgah4mDDxnbMwWzJ0rvZwAfiJz%2Be%2BUFe6x7JtHG8xhDcrVZsIZXdwhF7FuH1gaPuKkF6SW0COvI%2BECqDB4qt4ShdWDtT131Yf9FvbtGpuNqFvB4y75LaItOUCz08XKjrVKkvvVaUM%2BF68EMKeKwR0Qj6E433lTcLOM855JJZbZkdwWaW52U%2BHflv7%2BlRq48i0KCJsXXOepCtWJBCF9kkbnVIPC7l6NRWmppWjLLXDBUgf5B7btaEwsa8Z5g%2Be%2BS%2Be%2Bpgejq6NZdccQbeAG7UkSnKL0iTAM710SjhSYG%2Fxf1Ovq4G9LYXQKQco8I4v0eIi%2BEK%2F8UlyCq7O4M%2BCY0YQaGot2W2J41U40mG41AR%2BKKDudF0m%2FOqt4W%2BF1jGbrSRbISaFJz%2FiMx864MFUOFwHCldoAeCMV%2BJ7FE8AXiUyS63QA8blFUpFpII2h7hAZDj%2FplbFbUBIpLiMUPd9q8PFyre82Xf%2FrAL70ty17zui4bTc3spg6RLDwqPHaLNiPrD3TNX4HlEBaKxs2LLaKyid0ufSlFtUtE5Jccf9SBYsz%2BTmj1pg0hcq2O61rntjwEiKUPk0Y7FXMKqvks8GOqUBDL%2FcJ3X2el1Emv97YxdMQ2qLNT563fwq95%2BsT3i%2FzGwUtSRWQQCNrLi%2BWtriaLXm7B8%2FnWyEeXjeRgbxJsEKwfemK5Wxj81XODwH%2Fg4utJWjeL2ALrGn3u%2FoAW%2BhsVh5C5tbgHeB%2F6SjOgLGu0bb7yU01iMijyB7DtqXlmRwc2txffXGWgzBo6YrPmMiifHjBGC4ax%2B157x077Or3Wgg2nk29V25&X-Amz-Signature=6fb0be081a43aaeaaae3bcb7c19fd9774262cd2ba4569f7466b2d81bc56508a1&X-Amz-SignedHeaders=host&x-amz-checksum-mode=ENABLED&x-id=GetObject)
# Market Analysis
via ChatGPT
### TLDR
👉 **Global TAM (async task processing): \~\$7.5B/year (2025)**
- if we can grab 20% of TAM that’s a \$1.5B business
- A blended CAGR of 14–18%
- **5-Year TAM Projection (2026 → 2030)**
	- Base case CAGR
		- **Async tasks:** **16%**
		- **Workflows:** **20%**
<table header-row="true">
<tr>
<td>**Year**</td>
<td>**Async Tasks (\$B)**</td>
<td>**Workflows (\$B)**</td>
<td>**Combined (\$B)**</td>
</tr>
<tr>
<td>**2026**</td>
<td>**8.70**</td>
<td>**6.00**</td>
<td>**14.70**</td>
</tr>
<tr>
<td>2027</td>
<td>10.09</td>
<td>7.20</td>
<td>17.29</td>
</tr>
<tr>
<td>2028</td>
<td>11.71</td>
<td>8.64</td>
<td>20.35</td>
</tr>
<tr>
<td>2029</td>
<td>13.58</td>
<td>10.37</td>
<td>23.95</td>
</tr>
<tr>
<td>**2030**</td>
<td>**15.75**</td>
<td>**12.44**</td>
<td>**28.19**</td>
</tr>
</table>
Note this is a finer-grained breakdown vs. the estimated \$15B TAM for [Temporal as a whole](https://docs.google.com/presentation/d/1nzpRvyq2x9MMqmotQmwP9cRmOKX9ut0VZ17zuMOn3U4/edit?slide=id.g36efbc3ed10_0_115#slide=id.g36efbc3ed10_0_115), presented [in the monthly town hall](https://docs.google.com/presentation/d/1csSJXL3nqx8uFkHpZeP4J7UaVCz_pktfHhLDQz9iIbw/edit?slide=id.g363821e9e6d_8_87#slide=id.g363821e9e6d_8_87).
### 🎯 Key Parameters and Definitions
- **Scope:** North America (U.S.), EMEA, and APJ regions.
- **Use cases:** **Not streaming analytics**, but **durable asynchronous backend tasks**.
- **Buyer persona:** Mid-size to large enterprises, SaaS companies, fintech, e-commerce, cloud-native startups.
- **Pricing basis:** Typically measured by **usage-based infrastructure costs**, SaaS platform fees (e.g., Temporal Cloud, AWS SQS), or developer productivity tools.
---
### 🧮 TAM Methodology
We’ll use **bottom-up** and **top-down triangulation** to estimate.
---
## <span discussion-urls="discussion://2808fc56-7738-808f-8a0d-001c571851e5">🌎 Total Market Size (2025 Estimate)</span>
<table>
<tr>
<td>Region</td>
<td>Est. Enterprise/Cloud-Native Apps</td>
<td>Est. Spend on Async Task Infra/Tooling</td>
<td>TAM (USD)</td>
</tr>
<tr>
<td>US</td>
<td>\~150,000</td>
<td>\$5K–\$100K per year per org (avg: \$25K)</td>
<td>**\$3.75B**</td>
</tr>
<tr>
<td>EMEA</td>
<td>\~120,000</td>
<td>\$4K–\$80K per year (avg: \$20K)</td>
<td>**\$2.4B**</td>
</tr>
<tr>
<td>APJ</td>
<td>\~90,000</td>
<td>\$3K–\$50K per year (avg: \$15K)</td>
<td>**\$1.35B**</td>
</tr>
</table>
> **🌐 Global TAM (async task processing): \~\$7.5B/year (2025)**
---
### 💡 Breakdown of Market Segments
<table>
<tr>
<td>Segment</td>
<td>Description</td>
<td>% of TAM</td>
</tr>
<tr>
<td>SaaS & Cloud Platforms</td>
<td>Async jobs for provisioning, notifications, retries</td>
<td>30%</td>
</tr>
<tr>
<td>E-commerce & Retail</td>
<td>Email, logistics webhooks, payment retries, order pipelines</td>
<td>25%</td>
</tr>
<tr>
<td>Fintech & Payments</td>
<td>High reliability retries, ledger tasks, anti-fraud jobs</td>
<td>15%</td>
</tr>
<tr>
<td>B2B APIs & Integrations</td>
<td>Webhook handling, sync adapters, job retries</td>
<td>20%</td>
</tr>
<tr>
<td>Developer Tools/Platforms</td>
<td>Internal infra for CI/CD, job queues, workflows</td>
<td>10%</td>
</tr>
</table>
---
### ⚙️ Representative Technologies (non-streaming use)
- **Message brokers:** RabbitMQ, Amazon SQS, Google Pub/Sub
- **Task queues:** Celery, Sidekiq, Resque, RQ
- **Workflow engines:** Temporal, Cadence, Prefect (non-ETL usage), Durable Functions
- **Event systems (used for reliability):** Kafka (for durable task replay)
---
### 📈 Growth Drivers
- Explosion of **microservices** and **event-driven architectures**
- Need for **retryable, idempotent, durable** task handling
- Move from cron/scripts to **platforms** (e.g., Temporal, AWS Step Functions)
- Regulatory/PCI pressure for **reliable async payments**
---
### 🧯 What’s Not Included
- Real-time **stream processing** (Kafka Streams, Flink, Spark Streaming)
- **ETL pipelines**, observability pipelines
- Messaging for **chat, video, or pub/sub events**
<empty-block/>
### 📊 Closest Relevant Categories & Their Projected Growth (CAGR)
**Gartner**, **Forrester**, and similar analyst firms don’t publish a CAGR specifically labeled for "**asynchronous task processing**" as a distinct market, we can triangulate the **closest adjacent segments** they *do* track, which include:
<table>
<tr>
<td>Category (Analyst Term)</td>
<td>Description</td>
<td>2024–2028 CAGR</td>
<td>Source</td>
</tr>
<tr>
<td>**Application Infrastructure Middleware**</td>
<td>Includes message queues, event brokers, and orchestration platforms</td>
<td>**8–11%**</td>
<td>Gartner (2023)</td>
</tr>
<tr>
<td>**Cloud Infrastructure Services**</td>
<td>Includes serverless queues, task services (e.g., AWS Lambda + SQS/SNS usage)</td>
<td>**15–18%**</td>
<td>Forrester, IDC</td>
</tr>
<tr>
<td>**Workflow Automation Platforms**</td>
<td>Includes tools like Temporal, Step Functions, Camunda (non-human workflows)</td>
<td>**18–22%**</td>
<td>MarketsandMarkets, IDC</td>
</tr>
<tr>
<td>**Event-Driven Architecture Adoption**</td>
<td>Enterprise adoption of EDA tools & platforms (many async task workloads here)</td>
<td>**\~19%**</td>
<td>IDC, Gartner</td>
</tr>
<tr>
<td>**iPaaS (integration platforms)**</td>
<td>Overlap with async task orchestration in SaaS environments</td>
<td>**\~14%**</td>
<td>Gartner Magic Quadrant</td>
</tr>
</table>
---
### 🎯 Most Applicable CAGR for Async Task Processing
A **blended CAGR** of **14–18%** is reasonable based on:
- Growing shift from manual scripting and in-house queues to **managed durable task infrastructure**
- Emergence of **Temporal, Step Functions, Durable Task Frameworks** as mainstream platforms
- Increased pressure for **reliable transactional processing** in regulated industries (fintech, health, logistics)
- Move from basic brokers (RabbitMQ, SQS) to **platform-centric approaches** (e.g. workflow-as-a-service)
---
### 🧠 Analyst Commentary (Paraphrased)
- **Gartner (Middleware MQ report):** Enterprises are "de-emphasizing raw message queues" in favor of **platform-based orchestration and workflow patterns** — growth driven by microservices, reliability needs, and dev productivity.
- **Forrester (EDA and serverless trends):** The async job market is growing **faster than traditional middleware**, particularly as **SaaS vendors externalize internal workflow engines**.
- **IDC (Cloud Ops Report):** Reliable async processing is one of the fastest-growing segments in **cloud-native app operations**.
## **2026 TAM (point estimate)**
**Scope:** reliable async *task* processing (single-Activity jobs like email/webhook/payment retries) vs. **multi-step orchestration workflows** (Temporal-style stateful workflows, Camunda/BPMN, Step Functions, Inngest/Restate, Conductor, etc.).
**Method:** bottom-up inventory of app types × adoption × typical annual spend; triangulated with adjacent analyst CAGRs. Numbers are **platform + infra spend** (managed services + self-managed software + associated platform fees), not labor.
<table header-row="true">
<tr>
<td>**Market**</td>
<td>**2026 TAM**</td>
</tr>
<tr>
<td>**Async Task Processing (single-Activity)**</td>
<td>**\$8.7B**</td>
</tr>
<tr>
<td>**Multi-step Orchestration Workflows**</td>
<td>**\$6.0B**</td>
</tr>
</table>
2025 baselines used internally were \~\$7.5B (async tasks) and \~\$5.0B (workflows); we roll forward to 2026 using mid-CAGR assumptions (async \~16%, workflows \~20%).
## **TAM: 5-Year Projection (2026 → 2030)**
### **Base case CAGR**
- **Async tasks:** **16%**
- **Workflows:** **20%**
<table header-row="true">
<tr>
<td>**Year**</td>
<td>**Async Tasks (\$B)**</td>
<td>**Workflows (\$B)**</td>
<td>**Combined (\$B)**</td>
</tr>
<tr>
<td>**2026**</td>
<td>**8.70**</td>
<td>**6.00**</td>
<td>**14.70**</td>
</tr>
<tr>
<td>2027</td>
<td>10.09</td>
<td>7.20</td>
<td>17.29</td>
</tr>
<tr>
<td>2028</td>
<td>11.71</td>
<td>8.64</td>
<td>20.35</td>
</tr>
<tr>
<td>2029</td>
<td>13.58</td>
<td>10.37</td>
<td>23.95</td>
</tr>
<tr>
<td>**2030**</td>
<td>**15.75**</td>
<td>**12.44**</td>
<td>**28.19**</td>
</tr>
</table>
## **Takeaways**
- **Async single-Activity** remains the **larger market in 2026** (breadth of use: notifications, webhooks, retries across nearly every SaaS/e-com app).
- **Workflow orchestration** grows **faster** (higher platform premium for correctness, visibility, and compliance), closing the gap by **2030** in most scenarios.
- The **combined market** reaches **\~\$28B by 2030** in the base case; upside crosses **\$32B–\$34B** if both segments run at the high end of the bands.
# Financial model
👉 [Financial Model](https://docs.google.com/spreadsheets/d/1N18VVUk8lErTlcNqzEC0f1uj5Wwqk-Au1zAgQ-xxGv4/edit?gid=160780569#gid=160780569)
## Pricing
👉 See <mention-page url="https://www.notion.so/3118fc56773880ca91edf884f8d621b9"/> 
The same as [existing Activity pricing](https://docs.temporal.io/cloud/actions#activities), but now you can invoke those Activities standalone using the Temporal SDK Client.
- **Activity started or retried**. Occurs each time an Activity is started or retried. De-duplicated Activity starts that share an Activity ID do *not* count as an Action.
- **Activity Heartbeat recorded**. A Heartbeat call from Activity code counts as an Action only if it reaches the [Temporal Server](https://docs.temporal.io/temporal-service/temporal-server). Temporal SDKs throttle [Activity Heartbeats](https://docs.temporal.io/encyclopedia/detecting-activity-failures#activity-heartbeat). The default throttle is 80% of the [Heartbeat Timeout](https://docs.temporal.io/encyclopedia/detecting-activity-failures#heartbeat-timeout). Heartbeats don't apply to Local Activities.
<span discussion-urls="discussion://2bd8fc56-7738-805a-80d4-001cfa29555e">[Storage billing](https://docs.temporal.io/cloud/pricing#storage)</span><span discussion-urls="discussion://2bd8fc56-7738-805a-80d4-001cfa29555e"> needs to factor in Standalone Activity storage (of the mutable state like Workflows)</span>
> The existing [storage pricing model](https://docs.temporal.io/cloud/pricing#payg-storage-pricing) (Active vs. Retained) applies to Standalone Activities where a Standalone Activity Execution may be in one of two states: Open (Active Storage) or Closed (Retained Storage). This is similar to how [Workflow Storage works today](https://docs.temporal.io/cloud/pricing#storage), but for a Standalone Activity only the mutable state contributes to storage usage as there is no Workflow event history.
### Customer perspective on pricing - smells very reasonable and our existing cheap system is a steaming pile of garbage
[Rippling](/26c8fc567738801ea67ddd233b860cc3?pvs=25#26c8fc567738809c882feba732e04b6f):
> 1 action per task (Standalone Activity) and then whatever our negotiated pricing is like 15 dollars per 1,000,000 action. That smells like very reasonable to us.<br><br>**We have this existing system that is a steaming pile of garbage and it's very cheap**. That's the trade off. It's very cheap, but it's just like this unscalable piece of garbage. So this would be more expensive than that, but we would get all the things that are like the most important to us.<br><br>This is very directionally aligned. We'll certainly need to like experiment with it. We'd love to be alpha testers, you know, that sort of thing. But this I'm very optimistic about like this approach.
## ARR Estimates for FY27 (CY26)
- Net ARR: \$5.2M
- Total SA ARR: \$8.7M
	- New biz: \$7M
	- Cost optimization: -\$1.7M (cannibalization of existing single step workflows)
- Gross Profit - see <mention-page url="https://www.notion.so/27d8fc56773880b9a8d0c92bd2dc2ef9">Gross Profit & Breakeven</mention-page> 
### Financial Model Assumptions
Update Oct 16 2025: [Found that \~3% max actions are at risk of cannibalization with point in time data](slackMessage://temporaltechnologies.slack.com/C064K8TKGR2/1760639981.654449/1759252999.024409)[.](slackMessage://temporaltechnologies.slack.com/C064K8TKGR2/1760142959.669759/1759252999.024409)
We don’t currently have per company estimates of the % at risk, but the overall 8% at risk is much lower than the 25% we previously  estimated in the PRD.
- 16% ~~50%~~ of overall actions are from single-Activity workloads
	- see <mention-page url="https://www.notion.so/27d8fc5677388019a00ec99f0a69c194">Cannibalization</mention-page> estimates below
- 50% customer cost reduction
	- 2 actions -\> 1 action
- 20% cannibalization ramp/year
	- requires caller code changes to switch from single-Activity Workflows to Standalone Activities
- 100% growth opportunity (vs. current ARR) for new lower-cost single-Activity workflow use cases
	- equal to or more than TAM for multi-step workloads (orchestration)
	- see <mention-page url="https://www.notion.so/27d8fc5677388062b89cd2f9cb40df75">TAM: 5-Year Projection (2026 → 2030)</mention-page> 
	- higher volume use cases and overall market size
- 20% new workload ramp per year
	- will take time to onboard new use cases
- 4.5 months of actions "under the curve" in FY27
	- assumes we ship GA mid summer CY26
	- take time for cannibalization and new use cases to go into production
![](https://prod-files-secure.s3.us-west-2.amazonaws.com/d9be1e2b-72e6-41ae-9b0d-bb3dc6fda325/3cc98038-f511-4166-8cad-313a403a1bb2/image.png?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Content-Sha256=UNSIGNED-PAYLOAD&X-Amz-Credential=ASIAZI2LB466Q7NXIPVK%2F20260419%2Fus-west-2%2Fs3%2Faws4_request&X-Amz-Date=20260419T122131Z&X-Amz-Expires=3600&X-Amz-Security-Token=IQoJb3JpZ2luX2VjEDkaCXVzLXdlc3QtMiJHMEUCIQDDgOxmHt67y5Kdfkvx%2F1pMi9ByRn0xdgLHvzUdEAHLzAIgfHTYOensHnxxMUTvjod7PVhlgjEhxK6IEuUs5A1U%2Fkcq%2FwMIAhAAGgw2Mzc0MjMxODM4MDUiDN6JabxQ6sr9MQOKSyrcA5gdJkdlDrR7H%2B4M3bs%2BF%2BXuBYvh%2BwRIj3W%2BVwIXlOBkev%2Bi35Pvgah4mDDxnbMwWzJ0rvZwAfiJz%2Be%2BUFe6x7JtHG8xhDcrVZsIZXdwhF7FuH1gaPuKkF6SW0COvI%2BECqDB4qt4ShdWDtT131Yf9FvbtGpuNqFvB4y75LaItOUCz08XKjrVKkvvVaUM%2BF68EMKeKwR0Qj6E433lTcLOM855JJZbZkdwWaW52U%2BHflv7%2BlRq48i0KCJsXXOepCtWJBCF9kkbnVIPC7l6NRWmppWjLLXDBUgf5B7btaEwsa8Z5g%2Be%2BS%2Be%2Bpgejq6NZdccQbeAG7UkSnKL0iTAM710SjhSYG%2Fxf1Ovq4G9LYXQKQco8I4v0eIi%2BEK%2F8UlyCq7O4M%2BCY0YQaGot2W2J41U40mG41AR%2BKKDudF0m%2FOqt4W%2BF1jGbrSRbISaFJz%2FiMx864MFUOFwHCldoAeCMV%2BJ7FE8AXiUyS63QA8blFUpFpII2h7hAZDj%2FplbFbUBIpLiMUPd9q8PFyre82Xf%2FrAL70ty17zui4bTc3spg6RLDwqPHaLNiPrD3TNX4HlEBaKxs2LLaKyid0ufSlFtUtE5Jccf9SBYsz%2BTmj1pg0hcq2O61rntjwEiKUPk0Y7FXMKqvks8GOqUBDL%2FcJ3X2el1Emv97YxdMQ2qLNT563fwq95%2BsT3i%2FzGwUtSRWQQCNrLi%2BWtriaLXm7B8%2FnWyEeXjeRgbxJsEKwfemK5Wxj81XODwH%2Fg4utJWjeL2ALrGn3u%2FoAW%2BhsVh5C5tbgHeB%2F6SjOgLGu0bb7yU01iMijyB7DtqXlmRwc2txffXGWgzBo6YrPmMiifHjBGC4ax%2B157x077Or3Wgg2nk29V25&X-Amz-Signature=dc3adcff8d71b41869f3f2874734559a48c544a1b069ab2bbf90a546ce16e76f&X-Amz-SignedHeaders=host&x-amz-checksum-mode=ENABLED&x-id=GetObject)
## Cost Savings & Margin Improvements
<empty-block/>
### Update: COGS Analysis 1/30/26
- <mention-page url="https://www.notion.so/2f88fc567738807aa53afe9cf9f96dc6"/> 
- 👉 [cogs modeling thread - right after pre-release can be load tested on Cloud](slackMessage://temporaltechnologies.slack.com/C08GZQN1C80/1761162262.837619/1761162262.837619)
### Cost Model
👉 [Cost & Margin Model](https://docs.google.com/spreadsheets/d/1N18VVUk8lErTlcNqzEC0f1uj5Wwqk-Au1zAgQ-xxGv4/edit?gid=626695404#gid=626695404)
- [original discussion thread](slackMessage://temporaltechnologies.slack.com/C08GZQN1C80/1758649267.360379/1758649267.360379)
- [detailed estimates and diagrams](https://app.excalidraw.com/s/5BhtDRxD0Iq/5PyMtVNSvCh)
![](https://prod-files-secure.s3.us-west-2.amazonaws.com/d9be1e2b-72e6-41ae-9b0d-bb3dc6fda325/adff7dfa-77c5-4d94-80c4-447b75eed50f/image.png?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Content-Sha256=UNSIGNED-PAYLOAD&X-Amz-Credential=ASIAZI2LB466Q7NXIPVK%2F20260419%2Fus-west-2%2Fs3%2Faws4_request&X-Amz-Date=20260419T122131Z&X-Amz-Expires=3600&X-Amz-Security-Token=IQoJb3JpZ2luX2VjEDkaCXVzLXdlc3QtMiJHMEUCIQDDgOxmHt67y5Kdfkvx%2F1pMi9ByRn0xdgLHvzUdEAHLzAIgfHTYOensHnxxMUTvjod7PVhlgjEhxK6IEuUs5A1U%2Fkcq%2FwMIAhAAGgw2Mzc0MjMxODM4MDUiDN6JabxQ6sr9MQOKSyrcA5gdJkdlDrR7H%2B4M3bs%2BF%2BXuBYvh%2BwRIj3W%2BVwIXlOBkev%2Bi35Pvgah4mDDxnbMwWzJ0rvZwAfiJz%2Be%2BUFe6x7JtHG8xhDcrVZsIZXdwhF7FuH1gaPuKkF6SW0COvI%2BECqDB4qt4ShdWDtT131Yf9FvbtGpuNqFvB4y75LaItOUCz08XKjrVKkvvVaUM%2BF68EMKeKwR0Qj6E433lTcLOM855JJZbZkdwWaW52U%2BHflv7%2BlRq48i0KCJsXXOepCtWJBCF9kkbnVIPC7l6NRWmppWjLLXDBUgf5B7btaEwsa8Z5g%2Be%2BS%2Be%2Bpgejq6NZdccQbeAG7UkSnKL0iTAM710SjhSYG%2Fxf1Ovq4G9LYXQKQco8I4v0eIi%2BEK%2F8UlyCq7O4M%2BCY0YQaGot2W2J41U40mG41AR%2BKKDudF0m%2FOqt4W%2BF1jGbrSRbISaFJz%2FiMx864MFUOFwHCldoAeCMV%2BJ7FE8AXiUyS63QA8blFUpFpII2h7hAZDj%2FplbFbUBIpLiMUPd9q8PFyre82Xf%2FrAL70ty17zui4bTc3spg6RLDwqPHaLNiPrD3TNX4HlEBaKxs2LLaKyid0ufSlFtUtE5Jccf9SBYsz%2BTmj1pg0hcq2O61rntjwEiKUPk0Y7FXMKqvks8GOqUBDL%2FcJ3X2el1Emv97YxdMQ2qLNT563fwq95%2BsT3i%2FzGwUtSRWQQCNrLi%2BWtriaLXm7B8%2FnWyEeXjeRgbxJsEKwfemK5Wxj81XODwH%2Fg4utJWjeL2ALrGn3u%2FoAW%2BhsVh5C5tbgHeB%2F6SjOgLGu0bb7yU01iMijyB7DtqXlmRwc2txffXGWgzBo6YrPmMiifHjBGC4ax%2B157x077Or3Wgg2nk29V25&X-Amz-Signature=4057738b15f847f478f442cfa312566f62717c3b6535682f85815f1a78877db2&X-Amz-SignedHeaders=host&x-amz-checksum-mode=ENABLED&x-id=GetObject)
### Today: Workflow + Single Activity Costs (DB Writes)
- Counts 2x writes for WAL + Cassandra/Walker for everything but `RecordActivityTaskStarted`
- Separately counts matching writes since sync matching usually happens (sufficient Worker pollers/slots) for:
	- (1) Workflow is started - add workflow task to workflow task queue
	- (4) Activity task added to activity task queue
	- (7) Workflow task added to workflow task queue
- Counts Delete Workflow at the end of the retention period?
- Also include 3x writes to Visibility
Given this for a Workflow + Single Activity we expect:
- 15 DB writes nominally - assuming sync match + delete Workflow at end of retention period
- 3 DB writes for visibility
- Worst case +3 additional writes to matching is sync match isn't possible (Workers down/insufficient pollers/slots)
![](https://prod-files-secure.s3.us-west-2.amazonaws.com/d9be1e2b-72e6-41ae-9b0d-bb3dc6fda325/becb92da-68b3-4f17-b5a7-7785bf2b1bb4/image.png?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Content-Sha256=UNSIGNED-PAYLOAD&X-Amz-Credential=ASIAZI2LB466Q7NXIPVK%2F20260419%2Fus-west-2%2Fs3%2Faws4_request&X-Amz-Date=20260419T122131Z&X-Amz-Expires=3600&X-Amz-Security-Token=IQoJb3JpZ2luX2VjEDkaCXVzLXdlc3QtMiJHMEUCIQDDgOxmHt67y5Kdfkvx%2F1pMi9ByRn0xdgLHvzUdEAHLzAIgfHTYOensHnxxMUTvjod7PVhlgjEhxK6IEuUs5A1U%2Fkcq%2FwMIAhAAGgw2Mzc0MjMxODM4MDUiDN6JabxQ6sr9MQOKSyrcA5gdJkdlDrR7H%2B4M3bs%2BF%2BXuBYvh%2BwRIj3W%2BVwIXlOBkev%2Bi35Pvgah4mDDxnbMwWzJ0rvZwAfiJz%2Be%2BUFe6x7JtHG8xhDcrVZsIZXdwhF7FuH1gaPuKkF6SW0COvI%2BECqDB4qt4ShdWDtT131Yf9FvbtGpuNqFvB4y75LaItOUCz08XKjrVKkvvVaUM%2BF68EMKeKwR0Qj6E433lTcLOM855JJZbZkdwWaW52U%2BHflv7%2BlRq48i0KCJsXXOepCtWJBCF9kkbnVIPC7l6NRWmppWjLLXDBUgf5B7btaEwsa8Z5g%2Be%2BS%2Be%2Bpgejq6NZdccQbeAG7UkSnKL0iTAM710SjhSYG%2Fxf1Ovq4G9LYXQKQco8I4v0eIi%2BEK%2F8UlyCq7O4M%2BCY0YQaGot2W2J41U40mG41AR%2BKKDudF0m%2FOqt4W%2BF1jGbrSRbISaFJz%2FiMx864MFUOFwHCldoAeCMV%2BJ7FE8AXiUyS63QA8blFUpFpII2h7hAZDj%2FplbFbUBIpLiMUPd9q8PFyre82Xf%2FrAL70ty17zui4bTc3spg6RLDwqPHaLNiPrD3TNX4HlEBaKxs2LLaKyid0ufSlFtUtE5Jccf9SBYsz%2BTmj1pg0hcq2O61rntjwEiKUPk0Y7FXMKqvks8GOqUBDL%2FcJ3X2el1Emv97YxdMQ2qLNT563fwq95%2BsT3i%2FzGwUtSRWQQCNrLi%2BWtriaLXm7B8%2FnWyEeXjeRgbxJsEKwfemK5Wxj81XODwH%2Fg4utJWjeL2ALrGn3u%2FoAW%2BhsVh5C5tbgHeB%2F6SjOgLGu0bb7yU01iMijyB7DtqXlmRwc2txffXGWgzBo6YrPmMiifHjBGC4ax%2B157x077Or3Wgg2nk29V25&X-Amz-Signature=a0c470964d5e99a1c0305e0236186d37fd8c93594de69bb99bc6249bdc394d47&X-Amz-SignedHeaders=host&x-amz-checksum-mode=ENABLED&x-id=GetObject)
### Standalone Activity Cost (DB Writes)
Eliminates the wrapper Workflow reducing the total DB writes to the following:
- 4 persistence writes
- 3 visibility writes
- Worst case 1 write to matching
![](https://prod-files-secure.s3.us-west-2.amazonaws.com/d9be1e2b-72e6-41ae-9b0d-bb3dc6fda325/45a0584f-bd97-4c13-a5a0-9ef9376a0a0e/image.png?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Content-Sha256=UNSIGNED-PAYLOAD&X-Amz-Credential=ASIAZI2LB466Q7NXIPVK%2F20260419%2Fus-west-2%2Fs3%2Faws4_request&X-Amz-Date=20260419T122131Z&X-Amz-Expires=3600&X-Amz-Security-Token=IQoJb3JpZ2luX2VjEDkaCXVzLXdlc3QtMiJHMEUCIQDDgOxmHt67y5Kdfkvx%2F1pMi9ByRn0xdgLHvzUdEAHLzAIgfHTYOensHnxxMUTvjod7PVhlgjEhxK6IEuUs5A1U%2Fkcq%2FwMIAhAAGgw2Mzc0MjMxODM4MDUiDN6JabxQ6sr9MQOKSyrcA5gdJkdlDrR7H%2B4M3bs%2BF%2BXuBYvh%2BwRIj3W%2BVwIXlOBkev%2Bi35Pvgah4mDDxnbMwWzJ0rvZwAfiJz%2Be%2BUFe6x7JtHG8xhDcrVZsIZXdwhF7FuH1gaPuKkF6SW0COvI%2BECqDB4qt4ShdWDtT131Yf9FvbtGpuNqFvB4y75LaItOUCz08XKjrVKkvvVaUM%2BF68EMKeKwR0Qj6E433lTcLOM855JJZbZkdwWaW52U%2BHflv7%2BlRq48i0KCJsXXOepCtWJBCF9kkbnVIPC7l6NRWmppWjLLXDBUgf5B7btaEwsa8Z5g%2Be%2BS%2Be%2Bpgejq6NZdccQbeAG7UkSnKL0iTAM710SjhSYG%2Fxf1Ovq4G9LYXQKQco8I4v0eIi%2BEK%2F8UlyCq7O4M%2BCY0YQaGot2W2J41U40mG41AR%2BKKDudF0m%2FOqt4W%2BF1jGbrSRbISaFJz%2FiMx864MFUOFwHCldoAeCMV%2BJ7FE8AXiUyS63QA8blFUpFpII2h7hAZDj%2FplbFbUBIpLiMUPd9q8PFyre82Xf%2FrAL70ty17zui4bTc3spg6RLDwqPHaLNiPrD3TNX4HlEBaKxs2LLaKyid0ufSlFtUtE5Jccf9SBYsz%2BTmj1pg0hcq2O61rntjwEiKUPk0Y7FXMKqvks8GOqUBDL%2FcJ3X2el1Emv97YxdMQ2qLNT563fwq95%2BsT3i%2FzGwUtSRWQQCNrLi%2BWtriaLXm7B8%2FnWyEeXjeRgbxJsEKwfemK5Wxj81XODwH%2Fg4utJWjeL2ALrGn3u%2FoAW%2BhsVh5C5tbgHeB%2F6SjOgLGu0bb7yU01iMijyB7DtqXlmRwc2txffXGWgzBo6YrPmMiifHjBGC4ax%2B157x077Or3Wgg2nk29V25&X-Amz-Signature=7cac71ef734b46f668bafc0cc3ae8cf029cbf65761df20b71ecc17cc539e2452&X-Amz-SignedHeaders=host&x-amz-checksum-mode=ENABLED&x-id=GetObject)
## **Cannibalization**
👉 Update Oct 22 2025: [We now have per company / namespace breakdown of the actions at risk](https://docs.google.com/spreadsheets/d/1N18VVUk8lErTlcNqzEC0f1uj5Wwqk-Au1zAgQ-xxGv4/edit?gid=606644222#gid=606644222) (if a customer ports everything to use Standalone Activities). See [field comms](slackMessage://temporaltechnologies.slack.com/C03BY3HR2RH/1761153855.961779/1761153855.961779). 👈
Update Oct 16 2025: [Found that ](slackMessage://temporaltechnologies.slack.com/C064K8TKGR2/1760639981.654449/1759252999.024409)[`~3% max actions`](slackMessage://temporaltechnologies.slack.com/C064K8TKGR2/1760639981.654449/1759252999.024409)[ are at risk of cannibalization with point in time data](slackMessage://temporaltechnologies.slack.com/C064K8TKGR2/1760639981.654449/1759252999.024409)[.](slackMessage://temporaltechnologies.slack.com/C064K8TKGR2/1760142959.669759/1759252999.024409) See also: [field comms](slackMessage://temporaltechnologies.slack.com/C03BY3HR2RH/1761153855.961779/1761153855.961779)
With Standalone Activities, customer with existing single-Activity Workflow can update their callers to directly invoke the Activity and reduce actions billed by 50% (2 actions → 1 action).
Note: Standalone Activities are higher gross margin: approx. 61% vs. 50% for Workflows - see [cost & margin model](https://docs.google.com/spreadsheets/d/1N18VVUk8lErTlcNqzEC0f1uj5Wwqk-Au1zAgQ-xxGv4/edit?gid=626695404#gid=626695404) for details.
~~Update Oct 11 2025: ~~[~~Found that \~8% max actions are at risk of cannibalization with point in time data.~~](slackMessage://temporaltechnologies.slack.com/C064K8TKGR2/1760142959.669759/1759252999.024409)
~~We don’t currently have per company estimates of the % at risk, but the overall 8% at risk is much lower than the 25% we previously  estimated in the PRD.~~
<empty-block/>
### Cannibalization Estimates
- Square: [Nick estimates that 40% of their Temporal volume is SimpleTasks / StandaloneActivities](https://us-11514.app.gong.io/call?id=6035094571614556488&highlights=%5B%7B%22type%22%3A%22SHARE%22%2C%22from%22%3A651%2C%22to%22%3A667%7D%5D)
- Rippling:  Nitin wants to move *all* of ETA over to Temporal and throw away ETA.  But, he can't justify the cost right now per the above. [Dave: roughly speaking](slackMessage://temporaltechnologies.slack.com/C054VD4BZFD/1749595024.806969/1749576006.578819), I think it's like 60-70% of that 18B, but I don't really know.
- For the model ☝️ we assume 50% of total actions are for single-Activity workflows and calculate ARR cannibalization from that.
See 🧵 on getting [actual single-activity workflow %](slackMessage://temporaltechnologies.slack.com/C064K8TKGR2/1759252999.024409/1759252999.024409) to plug into the financial model.
For now we can find [namespaces that are largely single activity workflows](https://docs.google.com/spreadsheets/d/1N18VVUk8lErTlcNqzEC0f1uj5Wwqk-Au1zAgQ-xxGv4/edit?gid=149454017#gid=149454017), like the following:
- 1.62% total min at risk action volume based on the [namespace granularity query](slackMessage://temporaltechnologies.slack.com/C064K8TKGR2/1759273896.935909/1759252999.024409) we have
![](https://prod-files-secure.s3.us-west-2.amazonaws.com/d9be1e2b-72e6-41ae-9b0d-bb3dc6fda325/00e03cd7-b713-4c92-846e-e0b270c48ee3/image.png?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Content-Sha256=UNSIGNED-PAYLOAD&X-Amz-Credential=ASIAZI2LB466Q7NXIPVK%2F20260419%2Fus-west-2%2Fs3%2Faws4_request&X-Amz-Date=20260419T122131Z&X-Amz-Expires=3600&X-Amz-Security-Token=IQoJb3JpZ2luX2VjEDkaCXVzLXdlc3QtMiJHMEUCIQDDgOxmHt67y5Kdfkvx%2F1pMi9ByRn0xdgLHvzUdEAHLzAIgfHTYOensHnxxMUTvjod7PVhlgjEhxK6IEuUs5A1U%2Fkcq%2FwMIAhAAGgw2Mzc0MjMxODM4MDUiDN6JabxQ6sr9MQOKSyrcA5gdJkdlDrR7H%2B4M3bs%2BF%2BXuBYvh%2BwRIj3W%2BVwIXlOBkev%2Bi35Pvgah4mDDxnbMwWzJ0rvZwAfiJz%2Be%2BUFe6x7JtHG8xhDcrVZsIZXdwhF7FuH1gaPuKkF6SW0COvI%2BECqDB4qt4ShdWDtT131Yf9FvbtGpuNqFvB4y75LaItOUCz08XKjrVKkvvVaUM%2BF68EMKeKwR0Qj6E433lTcLOM855JJZbZkdwWaW52U%2BHflv7%2BlRq48i0KCJsXXOepCtWJBCF9kkbnVIPC7l6NRWmppWjLLXDBUgf5B7btaEwsa8Z5g%2Be%2BS%2Be%2Bpgejq6NZdccQbeAG7UkSnKL0iTAM710SjhSYG%2Fxf1Ovq4G9LYXQKQco8I4v0eIi%2BEK%2F8UlyCq7O4M%2BCY0YQaGot2W2J41U40mG41AR%2BKKDudF0m%2FOqt4W%2BF1jGbrSRbISaFJz%2FiMx864MFUOFwHCldoAeCMV%2BJ7FE8AXiUyS63QA8blFUpFpII2h7hAZDj%2FplbFbUBIpLiMUPd9q8PFyre82Xf%2FrAL70ty17zui4bTc3spg6RLDwqPHaLNiPrD3TNX4HlEBaKxs2LLaKyid0ufSlFtUtE5Jccf9SBYsz%2BTmj1pg0hcq2O61rntjwEiKUPk0Y7FXMKqvks8GOqUBDL%2FcJ3X2el1Emv97YxdMQ2qLNT563fwq95%2BsT3i%2FzGwUtSRWQQCNrLi%2BWtriaLXm7B8%2FnWyEeXjeRgbxJsEKwfemK5Wxj81XODwH%2Fg4utJWjeL2ALrGn3u%2FoAW%2BhsVh5C5tbgHeB%2F6SjOgLGu0bb7yU01iMijyB7DtqXlmRwc2txffXGWgzBo6YrPmMiifHjBGC4ax%2B157x077Or3Wgg2nk29V25&X-Amz-Signature=547a22d682d4575e2c6bf72a3a987271fc679a154cd21d7423a6d2598ae27820&X-Amz-SignedHeaders=host&x-amz-checksum-mode=ENABLED&x-id=GetObject)
To get a full picture of at risk actions, we really need actions by Workflow type actions for both Workflows and Activities. Until then we’re conservatively using 50% at risk in the model.
Also, at least for Block, the bigger question is when is the right time to enable their migration from SimpleTasks - esp. since they need Start Delay and ID Space Uniqueness checks across Workflows and Activities. Sounds like the absence of these features could delay migration/[cannibalization of existing single-Activity workloads](slackMessage://temporaltechnologies.slack.com/C039EGZSVKK/1759274549.405229/1759262770.551459) -- and give more time for new workloads to show up.
### Weighing the Cannibalization <span discussion-urls="discussion://2148fc56-7738-80be-aa00-001ce96c0823">Tradeoffs</span>
- **Short term**, if we do provide a more economic solution (standalone activities) - at say a 2x ARR reduction (for easy math; not 4x since we’ll have priority & fairness; <span discussion-urls="discussion://2118fc56-7738-80ca-a895-001c678e12c1">it’s lower COGS for us too</span>), then we we’d lose 50% of ARR for the single step workflows IF a customer converts to use Standalone Activities. Converting takes time/effort, esp. if Temporal SDK isn’t wrapped with a internal dev platform, but at Square they could convert their SimpleTasks to use Standalone Activities instead and they’d get a 50% ARR reduction for 40% of their overall volume, so a 20% top-line haircut for us (\$600K) but gain whatever new use cases are unlocked.
- **Long term**, if we don’t provide a more economic solution (simple task/queue system), some customers are saying they’ll be forced to build an alternate system and [Taylor (account SA) thinks that 60-70% of their current ARR is at risk](slackMessage://temporaltechnologies.slack.com/C054VD4BZFD/1749595310.661109/1749576006.578819), plus the lost additional use cases that standalone activities would unblock.
	- Taylor Khan - My thought is we will lose these workloads to cost optimization in the first place. A short term revenue gain for sure—but long term is my concern.
	- Dave Cole - I'm with Taylor - if we can get all their async business that would be huge
- Ultimately it comes down to
	- **(a) do nothing** - hold on to existing short term ARR (cash cow? of single step workflows) for as long as we can and losing significant portions of these async workloads over time to alternate home grown or competitive solutions (that are more economic) — <span discussion-urls="discussion://2148fc56-7738-80ee-a853-001c990e0bbd">once these non-Temporal solutions become entrenched it’s hard for us to recapture</span>.  Plus new use cases within a company now have to pick between Temporal and an alternate async solution — those who don’t pick Temporal now have a much harder upgrade path to Temporal / full Workflows later, decreasing adoption of full Workflows when they’re needed. In talking with Block there’s a strong desire to move to fewer tools and if Temporal can’t solve all async use cases, then detractors start positioning it as a niche solution (only suitable for this class of async problems due to cost/complexity) — and not THE answer.
	- **(b) do Standalone Activities** - give up some short term ARR now — but only IF customers switch their code — to capture the total async backend market and **propel Temporal as THE solution for ALL async workloads**, which unlocks significant new ARR for async workloads that can’t justify the cost of a full Workflow + activity for one-step workflows that need durability.
## Gross Profit & Breakeven
[Gross Profit Model](https://docs.google.com/spreadsheets/d/1N18VVUk8lErTlcNqzEC0f1uj5Wwqk-Au1zAgQ-xxGv4/edit?gid=1352283980#gid=1352283980) 
Varies based on <mention-page url="https://www.notion.so/27d8fc56773880c09e7df10382df6e07">Financial Model Assumptions</mention-page> that influence cannibalization and new business numbers:
- **58% gross profit increase**
	- 20% cannibalization
	- 120% new business revenue growth
- **51% gross profit increase**
	- 50% cannibalization
	- 100% new business revenue growth
- 35% gross profit increase
	- 70% cannibalization (est. worst case)
	- 66% new business revenue growth
- 0% gross profit increase (breakeven)
	- 70% cannibalization (est. worst case)
	- 22% new business revenue growth
![](https://prod-files-secure.s3.us-west-2.amazonaws.com/d9be1e2b-72e6-41ae-9b0d-bb3dc6fda325/968470a4-68e3-4e4f-b1bd-a45ea2c72045/image.png?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Content-Sha256=UNSIGNED-PAYLOAD&X-Amz-Credential=ASIAZI2LB466Q7NXIPVK%2F20260419%2Fus-west-2%2Fs3%2Faws4_request&X-Amz-Date=20260419T122131Z&X-Amz-Expires=3600&X-Amz-Security-Token=IQoJb3JpZ2luX2VjEDkaCXVzLXdlc3QtMiJHMEUCIQDDgOxmHt67y5Kdfkvx%2F1pMi9ByRn0xdgLHvzUdEAHLzAIgfHTYOensHnxxMUTvjod7PVhlgjEhxK6IEuUs5A1U%2Fkcq%2FwMIAhAAGgw2Mzc0MjMxODM4MDUiDN6JabxQ6sr9MQOKSyrcA5gdJkdlDrR7H%2B4M3bs%2BF%2BXuBYvh%2BwRIj3W%2BVwIXlOBkev%2Bi35Pvgah4mDDxnbMwWzJ0rvZwAfiJz%2Be%2BUFe6x7JtHG8xhDcrVZsIZXdwhF7FuH1gaPuKkF6SW0COvI%2BECqDB4qt4ShdWDtT131Yf9FvbtGpuNqFvB4y75LaItOUCz08XKjrVKkvvVaUM%2BF68EMKeKwR0Qj6E433lTcLOM855JJZbZkdwWaW52U%2BHflv7%2BlRq48i0KCJsXXOepCtWJBCF9kkbnVIPC7l6NRWmppWjLLXDBUgf5B7btaEwsa8Z5g%2Be%2BS%2Be%2Bpgejq6NZdccQbeAG7UkSnKL0iTAM710SjhSYG%2Fxf1Ovq4G9LYXQKQco8I4v0eIi%2BEK%2F8UlyCq7O4M%2BCY0YQaGot2W2J41U40mG41AR%2BKKDudF0m%2FOqt4W%2BF1jGbrSRbISaFJz%2FiMx864MFUOFwHCldoAeCMV%2BJ7FE8AXiUyS63QA8blFUpFpII2h7hAZDj%2FplbFbUBIpLiMUPd9q8PFyre82Xf%2FrAL70ty17zui4bTc3spg6RLDwqPHaLNiPrD3TNX4HlEBaKxs2LLaKyid0ufSlFtUtE5Jccf9SBYsz%2BTmj1pg0hcq2O61rntjwEiKUPk0Y7FXMKqvks8GOqUBDL%2FcJ3X2el1Emv97YxdMQ2qLNT563fwq95%2BsT3i%2FzGwUtSRWQQCNrLi%2BWtriaLXm7B8%2FnWyEeXjeRgbxJsEKwfemK5Wxj81XODwH%2Fg4utJWjeL2ALrGn3u%2FoAW%2BhsVh5C5tbgHeB%2F6SjOgLGu0bb7yU01iMijyB7DtqXlmRwc2txffXGWgzBo6YrPmMiifHjBGC4ax%2B157x077Or3Wgg2nk29V25&X-Amz-Signature=7086539818a930e2f8b2ab58583b405b6dcdd41fa444d8c759d0cf6053b5efaa&X-Amz-SignedHeaders=host&x-amz-checksum-mode=ENABLED&x-id=GetObject)
![](https://prod-files-secure.s3.us-west-2.amazonaws.com/d9be1e2b-72e6-41ae-9b0d-bb3dc6fda325/672f0975-a461-4f00-85d7-a0c0273dd1c9/image.png?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Content-Sha256=UNSIGNED-PAYLOAD&X-Amz-Credential=ASIAZI2LB466Q7NXIPVK%2F20260419%2Fus-west-2%2Fs3%2Faws4_request&X-Amz-Date=20260419T122131Z&X-Amz-Expires=3600&X-Amz-Security-Token=IQoJb3JpZ2luX2VjEDkaCXVzLXdlc3QtMiJHMEUCIQDDgOxmHt67y5Kdfkvx%2F1pMi9ByRn0xdgLHvzUdEAHLzAIgfHTYOensHnxxMUTvjod7PVhlgjEhxK6IEuUs5A1U%2Fkcq%2FwMIAhAAGgw2Mzc0MjMxODM4MDUiDN6JabxQ6sr9MQOKSyrcA5gdJkdlDrR7H%2B4M3bs%2BF%2BXuBYvh%2BwRIj3W%2BVwIXlOBkev%2Bi35Pvgah4mDDxnbMwWzJ0rvZwAfiJz%2Be%2BUFe6x7JtHG8xhDcrVZsIZXdwhF7FuH1gaPuKkF6SW0COvI%2BECqDB4qt4ShdWDtT131Yf9FvbtGpuNqFvB4y75LaItOUCz08XKjrVKkvvVaUM%2BF68EMKeKwR0Qj6E433lTcLOM855JJZbZkdwWaW52U%2BHflv7%2BlRq48i0KCJsXXOepCtWJBCF9kkbnVIPC7l6NRWmppWjLLXDBUgf5B7btaEwsa8Z5g%2Be%2BS%2Be%2Bpgejq6NZdccQbeAG7UkSnKL0iTAM710SjhSYG%2Fxf1Ovq4G9LYXQKQco8I4v0eIi%2BEK%2F8UlyCq7O4M%2BCY0YQaGot2W2J41U40mG41AR%2BKKDudF0m%2FOqt4W%2BF1jGbrSRbISaFJz%2FiMx864MFUOFwHCldoAeCMV%2BJ7FE8AXiUyS63QA8blFUpFpII2h7hAZDj%2FplbFbUBIpLiMUPd9q8PFyre82Xf%2FrAL70ty17zui4bTc3spg6RLDwqPHaLNiPrD3TNX4HlEBaKxs2LLaKyid0ufSlFtUtE5Jccf9SBYsz%2BTmj1pg0hcq2O61rntjwEiKUPk0Y7FXMKqvks8GOqUBDL%2FcJ3X2el1Emv97YxdMQ2qLNT563fwq95%2BsT3i%2FzGwUtSRWQQCNrLi%2BWtriaLXm7B8%2FnWyEeXjeRgbxJsEKwfemK5Wxj81XODwH%2Fg4utJWjeL2ALrGn3u%2FoAW%2BhsVh5C5tbgHeB%2F6SjOgLGu0bb7yU01iMijyB7DtqXlmRwc2txffXGWgzBo6YrPmMiifHjBGC4ax%2B157x077Or3Wgg2nk29V25&X-Amz-Signature=241fb7d81bfb7506aa078672452889f85fce72153b9067c5ef699732c7a26680&X-Amz-SignedHeaders=host&x-amz-checksum-mode=ENABLED&x-id=GetObject)
# <span discussion-urls="discussion://21d8fc56-7738-80a2-9894-001c3a9422f3">Positioning vs. Full Workflows</span>
👉 See latest <mention-page url="https://www.notion.so/2f68fc567738806aa799d00901a77725"/> 
## Day 0 developer journey
- <span discussion-urls="discussion://21c8fc56-7738-80e9-a9a8-001cf9e722ef">**Option A**</span>: <span discussion-urls="discussion://21d8fc56-7738-8072-84d6-001cf901d89a">Don’t include Standalone Activities in our Day 0 developer journey</span> and position it as an advanced feature for specific high-volume use cases.
	- Rationale:
		- new users must comprehend and decide if they should use a full Workflow vs. a Standalone Activity, which may cause more drop off in the new user journey.
		- potentially dilutes the Temporal brand & moves Temporal down market from orchestration to task processing 
- Option B: Provide a decision tree in the new user journey:
	- If single-Activity: use Standalone Activity
	- If multi-step: use Workflow
- **Decision: start with option A**
	- Rationale: less disruption & we can always add it to Day 0 later.
<empty-block/>
# Appendix A - Estimated Competitive Pricing
See also: [GTM Hub - Standalone Activities](/2638fc56773880f19710f33cc12a8e36?pvs=25)
## All-in monthly cost comparison (infra + SRE)
Bringing it together for 25M jobs/month
<table header-row="true">
<tr>
<td><br>Solution<br></td>
<td><br>Infra Cost / mo<br></td>
<td><br>Common SRE Cost (applies to all)<br></td>
<td><br>Platform-Operation SRE Cost (self-hosted only)<br></td>
<td><br>Total SRE<br></td>
<td><br>All-In Cost / mo<br></td>
</tr>
<tr>
<td><br>Inngest Cloud<br></td>
<td><br>\~\$650<br></td>
<td><br>\$1.25k–\$3.75k<br></td>
<td><br>\$0<br></td>
<td><br>\$1.25k–\$3.75k<br></td>
<td><br>\$1.9k–\$4.4k<br></td>
</tr>
<tr>
<td><br>Restate Cloud<br></td>
<td><br>\~\$1k–\$2k<br></td>
<td><br>\$1.25k–\$3.75k<br></td>
<td><br>\$0<br></td>
<td><br>\$1.25k–\$3.75k<br></td>
<td><br>\$2.25k–\$5.75k<br></td>
</tr>
<tr>
<td><br>Celery + SQS + DynamoDB<br></td>
<td><br>\~\$95<br></td>
<td><br>\$1.25k–\$3.75k<br></td>
<td><br>\$31k–\$50k<br></td>
<td><br>\$32.25k–\$53.75k<br></td>
<td><br>\$32.3k–\$53.8k<br></td>
</tr>
<tr>
<td><br>Sidekiq + Redis<br></td>
<td><br>\~\$356<br></td>
<td><br>\$1.25k–\$3.75k<br></td>
<td><br>\$31k–\$50k<br></td>
<td><br>\$32.25k–\$53.75k<br></td>
<td><br>\$32.6k–\$54.1k<br></td>
</tr>
<tr>
<td><br>RQ + Redis + Postgres<br></td>
<td><br>\~\$409<br></td>
<td><br>\$1.25k–\$3.75k<br></td>
<td><br>\$31k–\$50k<br></td>
<td><br>\$32.25k–\$53.75k<br></td>
<td><br>\$32.7k–\$54.15k<br></td>
</tr>
<tr>
<td><br>BullMQ + Redis + Postgres<br></td>
<td><br>\~\$409<br></td>
<td><br>\$1.25k–\$3.75k<br></td>
<td><br>\$31k–\$50k<br></td>
<td><br>\$32.25k–\$53.75k<br></td>
<td><br>\$32.7k–\$54.15k<br></td>
</tr>
<tr>
<td><br>Build Your Own Durable Engine<br></td>
<td><br>\~\$1k–\$5k<br></td>
<td><br>\$1.25k–\$3.75k<br></td>
<td><br>\$125k–\$175k (5–7 FTE)<br></td>
<td><br>\$126k–\$179k<br></td>
<td><br>\$127k–\$184k<br></td>
</tr>
</table>
Assumptions up front (same for all rows unless noted):
- Region: **us-east-1**, single region, **multi-AZ** where applicable.
- Scale: **25M single-step jobs / month** (\~9.6 jobs/sec sustained).
- Each job:
	- **Celery+SQS+DynamoDB**:
		- 1 enqueue, 1 batched receive/delete ⇒ \~**1.5 SQS requests / job**
		- 1 write + 1 read to result store ⇒ **2 DynamoDB ops / job**
	- **Redis-based frameworks** (Sidekiq, RQ, BullMQ):
		- \~**4 Redis commands / job** (enqueue, reserve, update state, finalize).
		- RQ/BullMQ rows: also **3 SQL statements / job** (insert, update, read history).
- Pricing (rounded, us-east-1):
	- **SQS**: \~\$0.40 / 1M requests, 1M free/month. [Pump+1](https://www.pump.co/blog/aws-sqs-pricing?utm_source=chatgpt.com)
	- **DynamoDB on-demand**: \~**\$1.25 / 1M writes**, **\$0.25 / 1M reads**, storage \~\$0.25/GB-month. [AWS Fundamentals+1](https://awsfundamentals.com/blog/amazon-dynamodb-pricing-explained?utm_source=chatgpt.com)
	- **ElastiCache Redis** `cache.m6g.large`: \~\$0.149/hr ⇒ **\$0.149 × 730 ≈ \$108.8 / node-month**; I’ll assume **3 nodes** (multi-AZ) ⇒ **≈ \$326 / month**. [Vantage+1](https://instances.vantage.sh/aws/elasticache/cache.m6g.large?utm_source=chatgpt.com)
	- **RDS Postgres** `db.t3.small`: \~\$0.036/hr ⇒ **\$26.3 / node-month**; assume **2 nodes (Multi-AZ)** ⇒ **≈ \$52.6 / month**. [Vantage](https://instances.vantage.sh/aws/rds/db.t3.small?utm_source=chatgpt.com)
	- **EC2 t3.small** admin/control nodes: \~\$15.2 / month / instance; assume **2 ⇒ ≈ \$30.4 / month**. [Cost Calculator+1](https://costcalc.cloudoptimo.com/aws-pricing-calculator/ec2/t3.small?utm_source=chatgpt.com)
SRE costs:
- Fully-loaded senior infra engineer ≈ **\$25k / month** (large tech comp levels).
- Assume:
	- **Celery+SQS+DDB**: **2.0 FTE ⇒ \$50k / month**.
	- **Sidekiq / RQ / BullMQ**: **1.5 FTE ⇒ \$37.5k / month**.
- Split across 5 categories:
	1. Design & IaC evolution – 15%
	2. Ops, upgrades, patching – 20%
	3. On-call & incident response – 25%
	4. Observability & capacity planning – 20%
	5. Developer support & consulting – 20%
---
## 1) Infra usage + cost per framework
### 1.1 Queue + DB + control-plane compute
<table header-row="true">
<tr>
<td>Framework</td>
<td>AWS stack (for job system itself)</td>
<td>Queue ops / job</td>
<td>Queue ops / month</td>
<td>Queue \$ / month</td>
<td>DB ops / job</td>
<td>DB ops / month</td>
<td>DB \$ / month</td>
<td>Control-plane compute</td>
<td>Compute \$ / month</td>
<td>**Infra total / month**</td>
</tr>
<tr>
<td>**Celery**</td>
<td>**SQS + DynamoDB** + 2×t3.small</td>
<td>\~1.5 SQS requests (Send, batched Receive+Delete)</td>
<td>25M × 1.5 = **37.5M**</td>
<td>37.5M – 1M free ≈ 36.5M billable ⇒ 36.5 × \$0.40 ≈ **\$15**</td>
<td>1 write + 1 read per job ⇒ 2 ops</td>
<td>25M writes + 25M reads = **50M**</td>
<td>Writes: 25M × \$1.25/1M = **\$31.25**; Reads: 25M × \$0.25/1M = **\$6.25**; storage (say 50GB × \$0.25) ≈ **\$12.5** ⇒ **≈\$50** total</td>
<td>2 × t3.small for beat, Flower/monitoring, admin</td>
<td>\~2 × \$15.2 ≈ **\$30**</td>
<td>**≈ \$15 + \$50 + \$30 = \$95**</td>
</tr>
<tr>
<td>**Sidekiq**</td>
<td>**ElastiCache Redis cluster (3×cache.m6g.large)** + 2×t3.small</td>
<td>\~4 Redis commands (LPUSH / BRPOP / HSET / status update)</td>
<td>25M × 4 ≈ **100M Redis commands**</td>
<td>No per-op charge; cost is Redis cluster: 3×cache.m6g.large @ \~\$108.8 ⇒ **≈ \$326**</td>
<td>Job state also in Redis (same commands)</td>
<td>Included in Redis ops above</td>
<td>Included in Redis cluster cost</td>
<td>2 × t3.small for Sidekiq Web UI, metrics exporter, admin</td>
<td>≈ **\$30**</td>
<td>**≈ \$326 + \$30 = \$356**</td>
</tr>
<tr>
<td>**RQ (Redis Queue)**</td>
<td>**ElastiCache Redis cluster (3×cache.m6g.large) + RDS Postgres (2×db.t3.small)** + 2×t3.small</td>
<td>Same as Sidekiq: \~4 Redis commands</td>
<td>**100M Redis commands**</td>
<td>Redis cluster: **≈ \$326**</td>
<td>Assume 3 SQL statements/job (insert, update, 1 read)</td>
<td>25M × 3 = **75M SQL statements**</td>
<td>RDS: 2×db.t3.small @ \$26.3 ⇒ **≈ \$52.6** (I/O implicit in instance)</td>
<td>2 × t3.small for admin, metrics</td>
<td>≈ **\$30**</td>
<td>**≈ \$326 + \$52.6 + \$30 ≈ \$409**</td>
</tr>
<tr>
<td>**BullMQ**</td>
<td>**ElastiCache Redis cluster (3×cache.m6g.large) + RDS Postgres (2×db.t3.small)** + 2×t3.small</td>
<td>Similar: \~4 Redis commands/job</td>
<td>**100M Redis commands**</td>
<td>Redis cluster: **≈ \$326**</td>
<td>Same as RQ (BullMQ often persists additional metadata / UI state in SQL-ish DB)</td>
<td>**75M SQL statements**</td>
<td>RDS 2×db.t3.small ⇒ **≈ \$52.6**</td>
<td>2 × t3.small for admin, dashboards</td>
<td>≈ **\$30**</td>
<td>**≈ \$409**</td>
</tr>
</table>
So purely on AWS bill:
- **Celery+SQS+DDB** infra: **\~\$95/month**.
- **Sidekiq+Redis** infra: **\~\$356/month**.
- **RQ+Redis+RDS** infra: **\~\$409/month**.
- **BullMQ+Redis+RDS** infra: **\~\$409/month**.
i.e., infra is *tiny* compared to people costs.
---
## 2) SRE / platform OpEx breakdown (5 categories)
### 2.1 Celery + SQS + DynamoDB (2.0 FTE ≈ \$50k/month)
<table header-row="true">
<tr>
<td>Category</td>
<td>% of SRE cost</td>
<td>\$ / month</td>
</tr>
<tr>
<td>1. System design & IaC evolution (Terraform/CDK, patterns, capacity planning for SQS & Dynamo)</td>
<td>15%</td>
<td>**\$7.5k**</td>
</tr>
<tr>
<td>2. Routine ops, upgrades, and patching (workers’ side configs, broker/backends, SDK bumps, security updates)</td>
<td>20%</td>
<td>**\$10k**</td>
</tr>
<tr>
<td>3. On-call & incident response (stuck jobs, poison queues, throttling/limit issues, retry storms)</td>
<td>25%</td>
<td>**\$12.5k**</td>
</tr>
<tr>
<td>4. Observability & capacity (dashboards for latency, DLQs, retry counts; tuning backoffs)</td>
<td>20%</td>
<td>**\$10k**</td>
</tr>
<tr>
<td>5. Developer support & consulting (help teams debug jobs, tune retries, design idempotency, provide runbooks)</td>
<td>20%</td>
<td>**\$10k**</td>
</tr>
<tr>
<td>**Total SRE / month**</td>
<td>100%</td>
<td>**\$50k**</td>
</tr>
</table>
### 2.2 Sidekiq / RQ / BullMQ (1.5 FTE ≈ \$37.5k/month each)
*(Same percentage split, lower FTE)*
<table header-row="true">
<tr>
<td>Category</td>
<td>% of SRE cost</td>
<td>\$ / month (per framework)</td>
</tr>
<tr>
<td>1. System design & IaC</td>
<td>15%</td>
<td>**\$5.6k**</td>
</tr>
<tr>
<td>2. Routine ops & upgrades</td>
<td>20%</td>
<td>**\$7.5k**</td>
</tr>
<tr>
<td>3. On-call & incidents</td>
<td>25%</td>
<td>**\$9.4k**</td>
</tr>
<tr>
<td>4. Observability & capacity</td>
<td>20%</td>
<td>**\$7.5k**</td>
</tr>
<tr>
<td>5. Developer support</td>
<td>20%</td>
<td>**\$7.5k**</td>
</tr>
<tr>
<td>**Total SRE / month**</td>
<td>100%</td>
<td>**\$37.5k**</td>
</tr>
</table>
> These SRE numbers assume “big-tech-ish” compensation and that this is a shared internal platform for many teams, not just a single small product squad.
---
## 4) How to read this
- **Infra cost is noise** at this scale (25M jobs/month). Even with “nice” Redis clusters and multi-AZ RDS, you’re talking **hundreds of dollars** vs **tens of thousands in SRE time**.
- The **differences between frameworks** on infra are \<\$400/month; the real cost differences come from:
	- How much SRE time you burn fighting operational complexity (e.g., Celery+SQS+DDB is a bit more distributed / multi-service than Sidekiq+Redis).
	- How easy it is for product teams to debug jobs themselves vs escalating to platform.
---
<empty-block/>
# Appendix B
## What about Block’s feedback on cheaper multi-step workflows (less durable/available/ephemeral)?
For example: Block has different problems that needed solving. They solved first for (a) **single-Activity flows **and now are asking for (b) cheaper **multi-step flows** for specific multi-step use cases like user sessions or access control checks that can be ephemeral or much less durable/available since they can’t justify the cost of a normal Temporal Workflow for these multi-step flows:
- **policy evaluation which happens on the order of seconds** but has a bunch of flinko, you know, steps to. It has a bunch of control flow to sort through before it gives you a boolean. But **that literally only matters for a second. Maybe maybe a minute is the longest that could possibly take** if you're deciding if someone's allowed to make a payment.
- **storing the user sessions like within cash app**, where we know, if there's something you need to do before you can do the thing you want, we basically put a bunch of stuff on a stack in front of the thing you were trying to do and you have to bump your way down that stack to get to the point you were bad before concretely, it's like if I want to move money, but I don't have a picture of my license in there in a mental model like the screen that says move money, has a bunch of more screens put on top that you have to get through or I add my license, and then you get to there. But again, like if I close my app, that doesn't matter and I'll probably never go. **I wouldn't expect to get that screen open again if I went back to it even six hours later**. So that's another case where **the time is so short, that, the business value is not there for us** to put, you know, our, however many 1,000,000 sessions per day, temporal we drain the budget for nothing.
single-Activity flows are different and the main ask for companies on the 1-pager isn’t to reduce the durability of Activities (queued, retries, …), but to omit other features like full workflow history, visibility and other features to make single-Activity flows less expensive.
Given this the feedback from Block on cheaper/less durable/ephemeral multi-step flows has been moved into the appendix 👇, so cheaper/less durable multi-step flows can be solved with a separate 1-pager.
## **Tasks Offsite Day 1 - cleaned up notes**
see [full day 1 notes](/1a78fc56773880769f07ce818d1dc58b?pvs=25#1ee8fc56773880149437e873ce23549b)
- explored both problems: cost for high-volume use cases and DX
- dimensions considered
	- reduce cost for specific use cases — and to capture market share vs. incumbents faster
	- use cases where durablilty isn’t needed - actor use cases - how do these relate
	- how to onboard temporal in the most simple way - clean simple API that customers can use without any education
	- vehicle to adoption of other temporal features
	- if just a queue and then priority and fairness; better than Kafka, we’d totally win
		- with cost a focus - it’s going to take us in a Task-like direction - single Task
		- queues aren’t addressable like Workflows
		- we’d miss half the features if we cut addressability
	- Inngest has batching; just flipping a knob; maybe they can provide lower cost
	- AI use cases
		- right now nexus can only do WF or \< 10 second thing; bad state of world
		- can’t call LLM from Nexus handler; 60 seconds is required
		- full Workflows are too expensive for AI use cases
		- immediate problem to solve above others - Nexus + standalone activities solves it
	- pub/sub is a different use case from task processing
	- unified UX across different primitives - Workflows, …
	- workflow bundles a few things that we could deconstruct
		- durable execution / event history
		- identity - deduplication
		- addressability
		- task queue - basis for retries
		- visibility - list things, current state, search attributes
	- other considerations
		- (future) key-value store - e.g. for lightweight actors; retry load and continue
		- 2 types of activities: 1 the survives and another that is idempotently retried
			- if addressable, and can route commands, can interact with it
	- the layers - going back to the beginning
		- pure queue: no accessibility, no result; call callback; without long poll; cheapest; fire and forget; not deduped
		- then can store result somewhere ; no dedup; last result wins; multiple run in parallel
		- then if start then dedupe; then some state; retried state flowed between them
		- use k/v to communicate
		- then addressible thing to send info proactively; if fails and restarts update the address and send (for actor use case)
- cost - to compete with Kafka/SQS/Express Step Functions head on for single-Activity Workflows
	- Temporal is expensive for high-volume use cases; if we make it cheaper more will use
	- no reason to use SQS ever, take all async backend, can we get there?
	- we want to ensure normal workflows remain differentiated so we don’t cannibalize existing multi-step Workflow action volume - e.g. and have all dev/stage traffic immediately be moved to a lower-cost option
	- for single step workflows we’d to at least maintain the same net profit for single-Activity Workflows that exist today, while unlocking additional use cases at lower cost - via cost savings we share with the user
	- we want to avoid tripling our API surface
		- avoid too many abstractions
		- keep the existing primitives we have today
	- a better queue with Temporal since it’s part of a unified platform framework and has additional capabilities that users would need to build themselves on top of Kafka/SQS
		- addressability
		- subscribe and get result
		- search attributes
		- visibility - but no history
		- purpose-built runtime state machine to execute single-Activity workflow more efficiently that are run directly in the history service without having to go back and forth to a worker and record more state transitions
	- **cost analysis**
		- Full WF - 100% of the cost
			- 8 state transitions - accounts for 90% of cost
			- 3 visibility - including the delete - accounts for 10% of the cost - elastic search
				-  list of workflows before you double click into a specific workflow
			- 6 round trips
		- Standalone activity with purpose-built state machine \~35% of the cost of full WF
			- 4 state transitions
			- 3 visibility
			- 2 round trips
		- Standalone activity without visibility \~25% of the cost of a full WF
		- Standalone activity with visibility and optimistic start / speculative execution \~20%
		- whole point of CHASM
			- partial read/writes
			- separate payloads (to help avoid 2MB mutable state limit)
		- never do full event history
		- state transitions are cut in half with standalone activities
	- 3x-5x cost reduction is expected if we create a simpler purpose-built state machine for executing single-Activity workflows (standalone activities) by omitting the extraneous Workflow runtime overhead (state transition / DB / storage)
		- single-Activity workflows require an Activity to interact with the outside world
		- workflows themselves must be deterministic are not able to execute arbitrary code
		- thus users must provide an activity function and Temporal will provide an optimized state machine to execute it with visibility (list standalone activities - name TBD)
		- these will activity functions will be
			- implemented exactly how they are today
			- registered with workers exactly as they are today, which means all existing activities are 
			- optionally called as standalone activities which invokes a new CHASM state machine that is purpose-built to execute standalone activities in a more cost-effective manner - with a 3x-5x cost reduction as the expected result.
		- standalone activities will be invokable only from within a namespace
		- exposing standalone activities across namespaces will be done via Nexus, similar to Workflows are exposed today via Nexus
		- cost savings for standalone activities comes from
			- eliminating state transitions associated with wrapper workflows
			- eliminating workflow history entirely - and relying on mutable state to store the input/result
			- visibility (list view with search attributes) remains the same for both, but could also be omitted in the future for extreme cost sensitive scenarios
		- additional cost saving may be possible with
			- optimistic start / speculative execution & batching start flushes
			- providing more control over when state is flushed
			- running in memory only
			- the duration of execution - being less than a few seconds
		- with the 3x-5x cost savings, plus visibility, we can provide a competitive offering vs. Kafka/SQS for most/all transactional workloads
	- lower cost multi-step workloads are not solved with standalone activities
		- for example the Block Plasma use cases, which needs to orchestrate multiple steps
			- requires actor model - or full WF which is too expensive
		- cost reduction brainstorm
			- non-durable ultra-low cost mode - route requests to where it’s running now
			- in-memory actor model - primitive state machine
			- worker-owned history?
			- basic single AZ durable
				- Redis for storage? cross-AZ w/ async replication + snapshot to disk?
				- Redis has size limits - can we constrain it?
				- Redis is 1/3 the cost; machines have big memory these days
			- workflows with worker-local history
			- tunable checkpointing - flush state however often the user wants vs. every step
			- kv store - enable actors to maintain state across multiple calls
			- Walker will reduce costs further - Paul doesn’t want to say how much
			- Workflow tasks are deterministic so can do optimistic WF task execution
				- retries are OK
			- Can we make Temporal cheap enough to run locally in a cell phone with Rust?
				- strength is replication stack
				- durable execution by itself is not enough
				- phone connects and does sync (replication)
		- for Block: 5-10 minutes actor model
			- won’t flush them - they’re in our WAL;
			- wont be in persistence; no history writes
			- tasks through normal channels; only write tasks to DB;
			- tasks very cheap for short lived things
			- you control the flushing; WF which is less than this period; costs less
			- Activity is one action; variable pricing based on duration; if \< 1 second or 5 seconds charge less. if over 10 minutes; If \> 5 minutes 1000x more expensive
		- cannabilizing needs DD
			- if non durable then differentiated
			- like step functions do; BS but ppl buying
			- have WF checkpoint; single task ; single local activities
				- but make remote calls via matching engine;; 
				- can have full WF in memory like Express Step Functions;
				- have signals while running - we need to fix that.
				- do you want non-durable with same workflow
		<empty-block/>
		- EC2 and AWS lowered prices in first few years all the time; 
			- so far we’re just raising prices
			- giving something to customers; being proactive maybe a good thing
			- Drew: 30-40% of simple task was of overall # Workflows not action volume
			- Note: WF start delay shaved 1 action off 3 actions for Block Simple Tasks
		- needs additional brainstorm session
- developer experience complexity
	- new users struggle with the Temporal learning curve around what it means to write deterministic code - which has restrictions and limitations that present a significant hurdle to adoption - for example this [new user’s LinkedIn post on 5/7/25](https://www.linkedin.com/posts/tair-chamiel_what-i-learned-from-introducing-temporal-based-activity-7325929911343366144-sem8?utm_source=share&utm_medium=member_desktop&rcm=ACoAAAAdKGEBVEqPsOROMEDjVfeyJYFQI3-iLvY)
	- standalone activities enable users to just provide normal activity code, that are exactly the same Temporal primitive that activities are today, and import/use any library and execute normal non-deterministic code and still get the benefits of Temporal without taking the immediate learning curve of deterministic workflows.
	- when standalone activities need to be extended to multiple steps
		- the same activity can be used in the user’s first Temporal Workflow, thus providing a progressive learning experience, where standalone activities provide a pre-canned “Workflow” (state machine) for executing their activities.
		- alternatives, activities can call other standalone activities
	- standalone activities also provide enhanced scalability and DX
		- running more Activities that do heavy heart-beating than if those activities were run directly from a workflow as activities are today.
		- this is due to the workflow state machine being responsible for all activity state management
		- standalone activities will have their own state machine and thus be more scalable than normal activities when called from a workflow
		- the lifetime of standalone activities will also be decoupled from the caller’s workflow run, such that for continue-as-new scenarios you won’t have to wait for all activities to complete before continuing as new, since the state machines are decoupled from each other
	<empty-block/>
	## Discovery questions
	- use case(s)
	- alternatives considered
	- what is needed
		- pure queue?: no accessibility, no result; call callback; without long poll; cheapest; fire and forget; not deduped
		- store/fetch result?
		- deduplication - exactly once
		- priority/fairness?
		- visibility (list executions, no event history)
		- ...
	- what cost reduction is needed 3x, 5x?? to be economic
	<empty-block/>
	# Appendix
	<empty-block/>
	<mention-page url="https://www.notion.so/1ee8fc56773880149437e873ce23549b"/> 
	<empty-block/>
	Workflow feature summary we could prune:
	- durable execution / event history
	- identity - deduplication
	- addressability
	- task queue - basis for retries
	- visibility - list things, current state, search attributes
- other considerations
	- (future) key-value store - e.g. for lightweight actors; retry load and continue
	- 2 types of activities: 1 the survives and another that is idempotently retried
		- if addressable, and can route commands, can interact with it
- the layers - going back to the beginning
	- pure queue: no accessibility, no result; call callback; without long poll; cheapest; fire and forget; not deduped
	- then can store result somewhere ; no dedup; last result wins; multiple run in parallel
	- then if start then dedupe; then some state; retried state flowed between them
	- use k/v to communicate
	- then addressible thing to send info proactively; if fails and restarts update the address and send (for actor use case)
<empty-block/>
<empty-block/>
![](https://prod-files-secure.s3.us-west-2.amazonaws.com/d9be1e2b-72e6-41ae-9b0d-bb3dc6fda325/117f5f8a-c848-4405-9307-33a2a019c517/image.png?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Content-Sha256=UNSIGNED-PAYLOAD&X-Amz-Credential=ASIAZI2LB466Q7NXIPVK%2F20260419%2Fus-west-2%2Fs3%2Faws4_request&X-Amz-Date=20260419T122131Z&X-Amz-Expires=3600&X-Amz-Security-Token=IQoJb3JpZ2luX2VjEDkaCXVzLXdlc3QtMiJHMEUCIQDDgOxmHt67y5Kdfkvx%2F1pMi9ByRn0xdgLHvzUdEAHLzAIgfHTYOensHnxxMUTvjod7PVhlgjEhxK6IEuUs5A1U%2Fkcq%2FwMIAhAAGgw2Mzc0MjMxODM4MDUiDN6JabxQ6sr9MQOKSyrcA5gdJkdlDrR7H%2B4M3bs%2BF%2BXuBYvh%2BwRIj3W%2BVwIXlOBkev%2Bi35Pvgah4mDDxnbMwWzJ0rvZwAfiJz%2Be%2BUFe6x7JtHG8xhDcrVZsIZXdwhF7FuH1gaPuKkF6SW0COvI%2BECqDB4qt4ShdWDtT131Yf9FvbtGpuNqFvB4y75LaItOUCz08XKjrVKkvvVaUM%2BF68EMKeKwR0Qj6E433lTcLOM855JJZbZkdwWaW52U%2BHflv7%2BlRq48i0KCJsXXOepCtWJBCF9kkbnVIPC7l6NRWmppWjLLXDBUgf5B7btaEwsa8Z5g%2Be%2BS%2Be%2Bpgejq6NZdccQbeAG7UkSnKL0iTAM710SjhSYG%2Fxf1Ovq4G9LYXQKQco8I4v0eIi%2BEK%2F8UlyCq7O4M%2BCY0YQaGot2W2J41U40mG41AR%2BKKDudF0m%2FOqt4W%2BF1jGbrSRbISaFJz%2FiMx864MFUOFwHCldoAeCMV%2BJ7FE8AXiUyS63QA8blFUpFpII2h7hAZDj%2FplbFbUBIpLiMUPd9q8PFyre82Xf%2FrAL70ty17zui4bTc3spg6RLDwqPHaLNiPrD3TNX4HlEBaKxs2LLaKyid0ufSlFtUtE5Jccf9SBYsz%2BTmj1pg0hcq2O61rntjwEiKUPk0Y7FXMKqvks8GOqUBDL%2FcJ3X2el1Emv97YxdMQ2qLNT563fwq95%2BsT3i%2FzGwUtSRWQQCNrLi%2BWtriaLXm7B8%2FnWyEeXjeRgbxJsEKwfemK5Wxj81XODwH%2Fg4utJWjeL2ALrGn3u%2FoAW%2BhsVh5C5tbgHeB%2F6SjOgLGu0bb7yU01iMijyB7DtqXlmRwc2txffXGWgzBo6YrPmMiifHjBGC4ax%2B157x077Or3Wgg2nk29V25&X-Amz-Signature=afe24b7dc1ce4cc8533146c4ec37bf5ea556a774167f444c8da90316f5e4f583&X-Amz-SignedHeaders=host&x-amz-checksum-mode=ENABLED&x-id=GetObject)
![](https://prod-files-secure.s3.us-west-2.amazonaws.com/d9be1e2b-72e6-41ae-9b0d-bb3dc6fda325/ecda011c-ccb8-4fa9-8a59-13c01823d18d/image.png?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Content-Sha256=UNSIGNED-PAYLOAD&X-Amz-Credential=ASIAZI2LB466Q7NXIPVK%2F20260419%2Fus-west-2%2Fs3%2Faws4_request&X-Amz-Date=20260419T122131Z&X-Amz-Expires=3600&X-Amz-Security-Token=IQoJb3JpZ2luX2VjEDkaCXVzLXdlc3QtMiJHMEUCIQDDgOxmHt67y5Kdfkvx%2F1pMi9ByRn0xdgLHvzUdEAHLzAIgfHTYOensHnxxMUTvjod7PVhlgjEhxK6IEuUs5A1U%2Fkcq%2FwMIAhAAGgw2Mzc0MjMxODM4MDUiDN6JabxQ6sr9MQOKSyrcA5gdJkdlDrR7H%2B4M3bs%2BF%2BXuBYvh%2BwRIj3W%2BVwIXlOBkev%2Bi35Pvgah4mDDxnbMwWzJ0rvZwAfiJz%2Be%2BUFe6x7JtHG8xhDcrVZsIZXdwhF7FuH1gaPuKkF6SW0COvI%2BECqDB4qt4ShdWDtT131Yf9FvbtGpuNqFvB4y75LaItOUCz08XKjrVKkvvVaUM%2BF68EMKeKwR0Qj6E433lTcLOM855JJZbZkdwWaW52U%2BHflv7%2BlRq48i0KCJsXXOepCtWJBCF9kkbnVIPC7l6NRWmppWjLLXDBUgf5B7btaEwsa8Z5g%2Be%2BS%2Be%2Bpgejq6NZdccQbeAG7UkSnKL0iTAM710SjhSYG%2Fxf1Ovq4G9LYXQKQco8I4v0eIi%2BEK%2F8UlyCq7O4M%2BCY0YQaGot2W2J41U40mG41AR%2BKKDudF0m%2FOqt4W%2BF1jGbrSRbISaFJz%2FiMx864MFUOFwHCldoAeCMV%2BJ7FE8AXiUyS63QA8blFUpFpII2h7hAZDj%2FplbFbUBIpLiMUPd9q8PFyre82Xf%2FrAL70ty17zui4bTc3spg6RLDwqPHaLNiPrD3TNX4HlEBaKxs2LLaKyid0ufSlFtUtE5Jccf9SBYsz%2BTmj1pg0hcq2O61rntjwEiKUPk0Y7FXMKqvks8GOqUBDL%2FcJ3X2el1Emv97YxdMQ2qLNT563fwq95%2BsT3i%2FzGwUtSRWQQCNrLi%2BWtriaLXm7B8%2FnWyEeXjeRgbxJsEKwfemK5Wxj81XODwH%2Fg4utJWjeL2ALrGn3u%2FoAW%2BhsVh5C5tbgHeB%2F6SjOgLGu0bb7yU01iMijyB7DtqXlmRwc2txffXGWgzBo6YrPmMiifHjBGC4ax%2B157x077Or3Wgg2nk29V25&X-Amz-Signature=88addd1214cb5b74786ab376a9ebd81726c3bb1055c452814aa313cb7a91e8c6&X-Amz-SignedHeaders=host&x-amz-checksum-mode=ENABLED&x-id=GetObject)
# Other Customer Feedback
## Developer experience complexity
## <span discussion-urls="discussion://1f18fc56-7738-809b-827d-001cdc9d8ea4">Cost and scale issues</span>
### [Block 2/5/25](https://us-11514.app.gong.io/call?id=1131968903748559556&highlights=%5B%7B%22type%22%3A%22SHARE%22%2C%22from%22%3A1358%2C%22to%22%3A1445%7D%5D) - Simple Task wrapper for Workflow + Activities to simplify Temporal for new devs
- Simple Task is a popular tool at Square that provides a simple interface for scheduling and managing short-running tasks, with around 30-40% of their workloads using it.
- Flow chart to guide teams on when to use Simple Task, On Time, Repeated Task, or Temporal Workflows, with a focus on using Temporal for critical or long-running processes.
- Simple Task costs Square 2-3 Temporal actions per task, and they have done cost optimizations (moving from Timers to start delay) to ensure high-volume use cases don't incur excessive costs.
- Teams prefer the ownership model of Simple Task where they manage their own scheduled tasks.
- Nick: "...we see people bang out like two, three, four five simple tasks all in the same namespace in the space of a month or two after they get the first one. Because all of a sudden, you understand this hammer that you have in your hand and you can bang out a bunch of different things now that it's possible and then we see people repeatedly implement it, a bunch of places..."[ ](https://us-11514.app.gong.io/call?id=1131968903748559556&highlights=%5B%7B%22type%22%3A%22SHARE%22%2C%22from%22%3A1303%2C%22to%22%3A1307%7D%5D)[Link to snippet](https://us-11514.app.gong.io/call?id=1131968903748559556&highlights=%5B%7B%22type%22%3A%22SHARE%22%2C%22from%22%3A1282%2C%22to%22%3A1300%7D%5D)
- Nick: “absolutely” \[people are more likely to upgrade to workflows than like somebody who used, like, on time\] "...the main reason is they've paid the mental costs of having to add Temporal to their stack. Once you have it, it's so easy to add a second one or experiment with the full SDK because you've already gotten in the door that — people will commonly do that." [Link to snippet](https://us-11514.app.gong.io/call?id=1131968903748559556&highlights=%5B%7B%22type%22%3A%22SHARE%22%2C%22from%22%3A1246%2C%22to%22%3A1269%7D%5D)
- Nick: "One thing I see a lot, which I hate and you will be up against if this becomes part of the sdk is, people will chain Simple Tasks together rather than just like elevate it to airflow, and pay \[more\] cost versus just putting it as two like two or three or four steps in the damn workflow. Actually, who cares, it’ll actually make you all more money. [Link to snippet](https://us-11514.app.gong.io/call?id=1131968903748559556&highlights=%5B%7B%22type%22%3A%22SHARE%22%2C%22from%22%3A1359%2C%22to%22%3A1448%7D%5D)
	- we've seen it's all about if they're willing to invest and really learn Temporal:
		- plenty of people are just fine to string five simple tasks together and \[pay more\] even though we told them not to.
		- then the other half are like, this is stupid. I should move to the full sdk and then they drop their stuff into an activity and are off to the races. 
		- it's a two bucket function that's very rare that somebody does one simple task and stops.
<empty-block/>
## Customer Feedback: Lower-cost multi-step Workflows with less runtime guarantees - separate workstream
1. Need Temporal to provide a cost-effective option for multi-step Workflows (compose). Willing to tradeoff some runtime characteristics (availability, durability, …) to get lower cost — with the same programming model contract - ex: <span discussion-urls="discussion://2148fc56-7738-8047-9636-001c0ef7ca72">Plasma and Trust Policy use cases at Block</span>; 
	### [Block - 6/11/25 - Nick](https://us-11514.app.gong.io/call?id=6035094571614556488) - wants lower cost Workflows with less guarantees
	- Workflows provide value by allowing engineers to reason about systems in a unified way, even if the workflows are not highly durable — use cases discussed involve short-lived, low-durability state machines, where temporal workflows are still valuable despite the lack of long-term durability.
	- Square is willing to pay for temporal workflows, even if the business value is not high, to provide a consistent experience across the company.
	- Proposed a design-time option to choose different durability levels for temporal workflows based on the use case, similar to reserved vs. spot instances in AWS.
### [**Block - 5/6/25 - Nick**](https://us-11514.app.gong.io/call?id=6088193344110211615)** -**<span discussion-urls="discussion://1f18fc56-7738-8054-a3e9-001cf2690396">** need lower cost multi-step Workflows **</span>**with less guarantees for high-volume lower-value use cases (**Plasma, Trust Policy)
- TL;DR
	- **We want to make temporal the standard way block wide to orchestrate work**, some work was just happening at too high of a scale for us to do that, and some Block use cases do not need the full durability and cost of Temporal.
	- I need to have good answers for why we should standardize on this thing that doesn't hit a bunch of our heaviest use cases.
	- It would 100 percent have to keep the same contract… but you \[could\] lose a nine by not being multi regional, maybe you lose another nine by not being multi zonal, but the price drops by some factor
- Nick: "...while **we want to make temporal the standard way block wide to orchestrate work**, some work was just happening at too high of a scale for us to do that. It left us with an unpleasant… we've got a bunch of work that we'd love to do that way, we just can't afford to do it that way." [Link to snippet](https://us-11514.app.gong.io/call?id=6088193344110211615&highlights=%5B%7B%22type%22%3A%22SHARE%22%2C%22from%22%3A359%2C%22to%22%3A369%7D%5D)
- For… a certain level of business importance, we've decided that the cost is fair. We don't mind paying when it's important... but we have a bunch of places now, where like people want to use the tools that you said I should use, and I just can't for high throughput and low lifetime use cases."[ ](https://us-11514.app.gong.io/call?id=6088193344110211615&highlights=%5B%7B%22type%22%3A%22SHARE%22%2C%22from%22%3A394%2C%22to%22%3A412%7D%5D)[Link to snippet](https://us-11514.app.gong.io/call?id=6088193344110211615&highlights=%5B%7B%22type%22%3A%22SHARE%22%2C%22from%22%3A349%2C%22to%22%3A359%7D%5D)
	- Nick: "\[for example **our Plasma use case**, today only\] one percent of cash app flows use temporal that checks if somebody has abandoned what they're doing ... they probably last five minutes, maybe an hour \[time to find your passport in a desk drawer\] … if I crashed in the middle, I'd want to come back to where it was.
		- but the reality is, if we were trying to replace that state machine with temporal wholesale they probably can't afford to use temporal because **it'll 10x the entire cost of the system as it exists today ...**
		- we would be happy to spend the money on temporal for moving things between bank accounts \[but\] it's not as important as making sure money makes it to your bank account ...
		- we don't want to spend the money on temporal \[on these low business value use cases\]" [Link to snippet](https://us-11514.app.gong.io/call?id=6088193344110211615&highlights=%5B%7B%22type%22%3A%22SHARE%22%2C%22from%22%3A608%2C%22to%22%3A613%7D%5D)
	- Nick: "...the other \[**trust access policy use case**\] is like many orders of magnitude higher, and many magnitudes of order, shorter in timescale \[and\] think of all the decisions you were about to make as idempotent..."
		- This one is pretty tricky, we have a piece inside of cash (and eventually block) because we're merging all this stuff together.
		- We have a team under Trust called access and their job is to determine the eligibility of a user to perform an action through opa policy and rego rule evaluation, can you withdraw up to a 1,000 bucks a day in bitcoin or whatever, or send a transaction to people per hour. ...it's huge the number of policy evaluations that we do" [Link to snippet](https://us-11514.app.gong.io/call?id=6088193344110211615&highlights=%5B%7B%22type%22%3A%22SHARE%22%2C%22from%22%3A626%2C%22to%22%3A675%7D%5D)
- Nick: "People would love to use temporal to drive, the experience... but millions and millions of those are happening or something wild to the point that it's like one or two orders of magnitude off what we can afford. We want to use the same tools so that we have as the same language to talk about what we're doing, but these things last a second, you know, milliseconds or something like that." [Link to snippet](https://us-11514.app.gong.io/call?id=6088193344110211615&highlights=%5B%7B%22type%22%3A%22SHARE%22%2C%22from%22%3A709%2C%22to%22%3A715%7D%5D)
- Nick: "...the core of the problem is we've introduced a paradigm and people really like the paradigm — they want to use the paradigm to solve their business problems at Block, but the paradigm is out of reach in a couple of these really important places… I'm hoping that there's a solution here long term because **it does feel like it addresses a different technical problem than durable workflow execution, i**t's more about the semantics of reasoning about \[how to orchestrate\] work." [Link to snippet](https://us-11514.app.gong.io/call?id=6088193344110211615&highlights=%5B%7B%22type%22%3A%22SHARE%22%2C%22from%22%3A750%2C%22to%22%3A788%7D%5D)
- Nick: "If this thing had guarantees that we can keep your workflows alive for an hour or five minutes and ran entirely in memory, where durability is not important, but the semantics are in the same way that a local activity gives away some safety so that you can get stuff done more quickly and skip a round trip — a version of this where we rely on you for really quickly doing the work in the same way and our guarantees are much less strong… would be really awesome." [Link to snippet](https://us-11514.app.gong.io/call?id=6088193344110211615&highlights=%5B%7B%22type%22%3A%22SHARE%22%2C%22from%22%3A989%2C%22to%22%3A1031%7D%5D)
- Nick: "...I'm a distributed systems engineer who is trying to solve a cost problem like this \[where\] you trade around the things that put the cost of services on there — there could be an optimization." [Link to snippet](https://us-11514.app.gong.io/call?id=6088193344110211615&highlights=%5B%7B%22type%22%3A%22SHARE%22%2C%22from%22%3A1035%2C%22to%22%3A1062%7D%5D)
- Nick: "...the reality of changing the model is if it isn't just going to be the same temporal, we'd probably just look at something different altogether to solve that set of problems. And then we end up worse off than we started, which was homegrown plus temporal. Then we end up with homegrown plus temporal plus some other cheaper, you know, potentially cheaper thing that, God knows what … the last thing I need is somebody to go build a Redis based version, of temporal for stuff that guarantees to fit in a certain size." [Link to snippet](https://us-11514.app.gong.io/call?id=6088193344110211615&highlights=%5B%7B%22type%22%3A%22SHARE%22%2C%22from%22%3A1154%2C%22to%22%3A1186%7D%5D)
- Nick: "<span discussion-urls="discussion://1f18fc56-7738-8082-9d38-001c5953da24">It would 100 percent have to keep the same contract</span>… but you \[could\] lose a nine by not being multi regional, maybe you lose another nine by not being multi zonal, but the price drops by some factor [Link to snippet](https://us-11514.app.gong.io/call?id=6088193344110211615&highlights=%5B%7B%22type%22%3A%22SHARE%22%2C%22from%22%3A1191%2C%22to%22%3A1208%7D%5D)
	- the only requirement would be the same interface for doing work ... we never blocked people from going past our abstraction to using the guts of temporal more directly.
	- the idea here is if somebody realizes they need more, \[and\] they wanted to graduate off of this thing to like multi-region to get another nine or, \[move to\] regular temporal to get the durability, we’d need to make sure it was portable."
	- it'd be great if history recorded the success or failure at the end. the expensive part becomes memory cost of the infrastructure, trying hard not solutioneer, \[but note that\] AWS EC2 Spot Instances have proven people \[would be\] okay with ideas like this.
	- You're not like doing something weird by suggesting a low durability version of a thing which you could otherwise pay more money for.
	- I need to have good answers for why we should standardize on this thing that doesn't hit a bunch of our heaviest use cases. I've got plenty more thoughts if you want to dig into that in the future but should definitely give you requirements not ideas." [Link to snippet](https://us-11514.app.gong.io/call?id=6088193344110211615&highlights=%5B%7B%22type%22%3A%22SHARE%22%2C%22from%22%3A1321%2C%22to%22%3A1411%7D%5D)
<empty-block/>
### Developer experience <span discussion-urls="discussion://21b8fc56-7738-80cc-a42d-001c3926c1da">complexity</span>
### Cost and scale issues preventing all-in adoption of Temporal
1. Impedes their ability to go “all in” on Temporal as the standard way to orchestrate all things within their company with a consistent programming model.
2. Platform teams found many new Temporal users struggle with the Temporal learning curve (Workflows and Activities, Determinism, …).
3. Some Platform teams have introduced a Simple Task abstraction (Stripe, Square) that wrap a Temporal Workflow and Activity to hide this complexity from new users — while enabling the entire orchestration platform to be backed by Temporal. Square: [Nick estimates that 40% of their Temporal volume is SimpleTasks (aka StandaloneActivities](https://us-11514.app.gong.io/call?id=6035094571614556488&highlights=%5B%7B%22type%22%3A%22SHARE%22%2C%22from%22%3A651%2C%22to%22%3A667%7D%5D))
4. Once Simple Tasks are adopted they often see rapid adoption for multiple use cases.
5. When teams need to string a few Simple Tasks together
	1. some teams just string them together even though it costs more
		1. considered a design anti-pattern - but it’s the easy button for app devs
		2. costs more due to the extra wrapper Workflows — but also generates more \$\$
	2. other teams do take the opportunity to dive in and adopt Temporal more fully
		1. have to go through the Temporal learning curve & create multi-step Workflows with multiple Activities
		2. lower cost - avoids the extra wrapper Workflow(s)
6. Block views Simple Tasks as a stepping stone path towards full Temporal Workflows
	1. even though they hide as much of Temporal as possible for new users
	2. Block allows use of the full Temporal SDK for those that want it
	3. plans to open source their Simple Task abstraction - do we want to partner?
7. Snap - the learning curve for Temporal is high and when new teams are focused on using Temporal to speed delivery of projects that learning curve directly trades off with hitting tight deadlines. — Company Offsite Scottsdale 2025
<empty-block/>
---
**Copy and past this to update the status at the top of the document**
<columns>
	<column>
		<span color="yellow_bg">**Discover**</span> {color="yellow_bg"}
	</column>
	<column>
		<span color="gray">Plan</span> {color="gray_bg"}
	</column>
	<column>
		<span color="gray">Build</span> {color="gray_bg"}
	</column>
	<column>
		<span color="gray">Deliver</span> {color="gray_bg"}
	</column>
</columns>
<columns>
	<column>
		<span color="gray">Discover</span> {color="gray_bg"}
	</column>
	<column>
		**Plan** {color="yellow_bg"}
	</column>
	<column>
		<span color="gray">Build</span> {color="gray_bg"}
	</column>
	<column>
		<span color="gray">Deliver</span> {color="gray_bg"}
	</column>
</columns>
<columns>
	<column>
		<span color="gray">Discover</span> {color="gray_bg"}
	</column>
	<column>
		<span color="gray">Plan</span> {color="gray_bg"}
	</column>
	<column>
		**Build** {color="yellow_bg"}
	</column>
	<column>
		<span color="gray">Deliver</span> {color="gray_bg"}
	</column>
</columns>
<columns>
	<column>
		<span color="gray">Discover</span> {color="gray_bg"}
		<empty-block/>
	</column>
	<column>
		<span color="gray">Plan</span> {color="gray_bg"}
		<empty-block/>
	</column>
	<column>
		<span color="gray">Build</span> {color="gray_bg"}
	</column>
	<column>
		**Deliver** {color="yellow_bg"}
	</column>
</columns>
<empty-block/>
<empty-block/>
</content>
</page>