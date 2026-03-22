

- Existing gRPC StartExecution APIs guarantee to enforce business ID reuse and conflict policies.



 # Questions

- Is requiring callers to specify a "batch" what customers want?
- Get expanded details on what Roey [refers to](https://temporaltechnologies.slack.com/archives/C0741B07RFC/p1772161260908839?thread_ts=1772142963.827009&cid=C0741B07RFC) as the "Nexus queue"



# Context

There are few conversations inn progress regarding adding "buffering" or "batching" functionality.

The task here is to gather all relevant context into one markdown document.



- The entire contents of the #crew-buffering-batching Slack channel is relevant.
- [Meeting summary](https://www.notion.so/temporalio/Meeting-March-11-2025-3208fc56773880129d8cef1138cacc78) posted in #crew-buffering-batching
- Jessica's [One-pager](https://www.notion.so/temporalio/Buffering-Batching-Handling-Work-Temporal-Can-t-Immediately-Process-31e8fc567738808c91e4d06bf989c581?d=3228fc567738806081f7001c7d96b4ee#3208fc56773880e486bbcaed65caf94e) (be sure to fetch all comments)
- Rippling's batch use case? Missing details on this. Mentioned by Paul [in slack](https://temporaltechnologies.slack.com/archives/C054VD4BZFD/p1773277061358009)
- OpenAI [Gong Call](https://us-11514.app.gong.io/call?id=6120936069767025036&email_type=call-ready-notification&xtid=4bpkim8czg9c1eq03aj) -- I don't have access yet
- Tao (Solution Architect)'s mention of a customer use case [here](https://temporaltechnologies.slack.com/archives/C08GZQN1C80/p1774071197820989)
- [COOLCAT draft one-pager](https://www.notion.so/temporalio/Operation-Collection-CAT-COOLCAT-Collection-Of-Operation-Locators-2508fc56773880e592fcc253b1d0c282)
- [Netflix use case](https://temporaltechnologies.slack.com/archives/C04NYM5D3U6/p1762295432405379) slack thread leading to COOLCAT suggestion
- [Stripe use case](https://temporaltechnologies.slack.com/archives/C0741B07RFC/p1772142963827009) slack thread in #crew-storage-flow-control (note that #crew-flow-control subsequently created, which would be the correct home for that thread) Also [second thread](https://temporaltechnologies.slack.com/archives/C0741B07RFC/p1773059264386509).
- [async dedup](https://temporaltechnologies.slack.com/archives/C09EB1D10UD/p1765834329023869?thread_ts=1765831561.982189&cid=C09EB1D10UD) slack thread
- [coolcat](https://temporaltechnologies.slack.com/archives/C09U97WTWKY/p1763672408912749) slack thread

- https://github.com/drewhoskins/batch-orchestra


Draft of a comment I thought about making in Jessica's one-pager:

 That’s technically possible (first response would contain a token; another API call would be needed
 to get the run ID and construct the “handle” in SDKs) but we can’t just do it because it would
 break semantics surrounding ID conflict policies, eager start, request ID deduplication, etc. And
 so this document’s solution is to do it, but by introducing a new thing which (AIUI) will return a
 precursor token with which the execution handle can be requested subsequently. If customers all
 switched to that instead of legacy APIs it would solve burst. But since it’s a new thing, we’re
 free to do what we want, and it would be even better to make that start a batch of executions
 rather than just starting one execution.

