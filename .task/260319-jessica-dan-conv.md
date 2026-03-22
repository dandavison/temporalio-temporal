Batch, Buffering & Collections — Dan and Jessica Discussion

Context: What Are AI Companies Asking For?
Jessica: I started looking at batch explicitly because OpenAI has a use case. I was working with Tushar — they have a bunch of internal jobs where they want to spin up anywhere from a million to 500 million workflows at once. The jobs come from different places: some from database scans where they want to create a workflow per row, some from Kafka queues. Basically, they want to kick off a batch of Temporal workflows at once, and they have pain points with that today.
Dan: So why is batch related? Is that because these data applications have high throughput requirements?
Jessica: Yes. Talking to OpenAI, it seems like there are actually three different flavors of the problem.
The Three Flavors
Jessica: When customers say "I want to start a million workflows at once and I'm hitting pain," it breaks down into three categories:

Flow control / rate limiting — Temporal is rejecting starts because it can't handle that many at a time. Customers already have a queue; they just want some sort of flow control.
Buffering — Customers don't want to maintain a queue at all. They had to build one just for Temporal. They want us to hold work internally so they don't need that infrastructure.
Batch as a first-class concept — When you have a group of 500 million workflows, it's overwhelming to treat them individually. Customers want to interact with the group at a statistical level — things like aggregate retry behavior rather than per-item tracking. OpenAI specifically said they'd like a "fire and forget" / best-effort version.

Dan: That division into three makes complete sense. The first one is where the customer does the queuing on their side. The second is just about buffering — accepting work really quickly. And the third is where a batch becomes some sort of first-class concept, maybe a new kind of execution you can query and paginate over.
Jessica: Right. And it's more about the work groupings getting so big that maybe they don't care that every row completes successfully — they want statistics and retry behavior at the group level. I think these could all layer together into an overall nice experience, but we don't have to do all of them at once.
How Starting a Workflow Works Today
Dan: At the moment, when you start a workflow execution or activity execution, there's only one mode. You start it, wait until it's been scheduled, get a run ID back — the SDKs call that a "handle." That run ID is what lets you address the execution you just started.
To start that execution, a lot happens: the RPC goes into our Envoy proxy, then to frontend, which does some checks and RPCs to history service. History service looks in the workflow cache to see if the workflow already exists, checks the conflict policy, and if the workflow doesn't exist, it does persistence writes — history events and mutable state with the timer task. Then the response comes back through matching to the caller.
So there's an RPC hop from frontend to history and two persistence writes. To match Kafka or SQS levels of throughput, we'd have to accept the raw gRPC request and basically just append it to a WAL or something and return immediately.
Lazy Start Semantics
Jessica: Before OpenAI's engineer was like "fire and forget, don't even send anything back." Tushar is dubious of that — he asked whether we return a run ID and just change the behavior of run ID, which would be a huge deal since maybe the workflow isn't running yet.
Dan: I could imagine something like this. Normally in Python you do await client.start_workflow(...) and get a handle with a run ID. But with a batch, maybe you do client.create_batch(...) or client.submit_batch(...) and get back a token like a UUID. The semantics would be: "we've accepted your work, but we haven't necessarily scheduled a new execution."
A good example is the conflict policy. Today, the default is that if a workflow ID is currently running, we reject a duplicate start. But in the batch case, perhaps we'd accept it without even making that check, in order to be fast enough.
So in Python, you'd do one await and get a thing, then on a second await maybe get an exception saying it wasn't possible to start because it violated the conflict policy — but you'd get that on the second network call, not the first. This batch thing would have different semantics from all the other SDK APIs. But that makes sense because the other APIs have no chance of matching Kafka throughput. It would be kind of like a lazy start.
Jessica: Then it becomes a question of how you surface failures — failures to start. Maybe it's not new since other things can fail to start, but today you get the error instantaneously.
Dan: Exactly. Normally you'd get a "workflow already started" error immediately, but with the new approach you'd get that on a second request when you check status.
Relationship Between Buffering and Batching
Dan: Would we make the batch concept only usable with lazy start semantics? So a batch would not be usable with the traditional start semantics?
Jessica: I think that is what folks are saying. Tushar mentioned someone might have a batch of just 10 and maybe traditional semantics are fine. I think we can build buffering without batching — in theory, Temporal can absorb every individual start-workflow request even if it's beyond what it can immediately start. Something else mentioned: we might want a flag on the start-workflow request like "able to be buffered," because not everything should be buffered. There are priority questions — should batch items always be processed second to non-batch items? There are open questions there, but in theory you could do buffering with individual start-workflow commands. Batching would probably need some buffering.
Cool Cat (Collection of Operation Locators)
Jessica: Paul is all in on something he's calling "Cool Cat."
Dan: I don't even know what it stands for yet.
Jessica: Collection of Operation Locators.
Dan: Paul's acronyms are something else. But I kind of get it — they're sort of like pointers to operations.
Jessica: So far, folks have different ideas about how various parts would get done. Paul has Cool Cat. Tushar had an idea about a WAL implementation, maybe maintaining an internal queue. Nothing's winning yet. A lot depends on customer needs and prioritization. It'd be great to have you in that conversation — people are just throwing out ideas right now.
Navigating Internal Opinions
Dan: At a slightly more meta level, we're going to have people like Max, Rowie, and Paul with a lot of opinions. We need fairly thick skin — they're going to dismiss things and push their own ideas about how it should be. We won't have free reign to design this from scratch.
Paul has been interested in collections ever since he started designing Chasm with Max, Yi Chao, and Rui three years ago. So this is something people have wanted for a long time.
But that's great — it means it's an exciting thing to work on, and those people are all busy with other things, so we should have opportunity to have some impact. I'm just saying it's going to get quite a lot of attention.
Next Steps: Customer Research
Jessica: Next up is talking to more customers. In the original document, I gathered about 30 customers that have had similar problems or have a queue in front of Temporal. My goal over the next week is to talk to five or more customers and see where they fall: do they want flow control, buffering, or batch? I have a little interview guide I wrote to tease apart what solves their problem.
Dan: What companies are you going to talk to?
Jessica: My targets are Stripe, Netflix, Snap, Rippling, Coupang, Meta, and Roblox. I have a feeling that in the fullness of time we might want to do all three things, but figuring out what to work on first and why is the key question.
I'm giving myself about a week to get on as many calendars as I can. In the meantime, if you want to hop into the Slack thread, people are already thinking about design. Then I can come in with a direction based on customer input, and we'll do actual engineering work from there.
Dan: Yeah, I'll catch up with that. Definitely interesting.