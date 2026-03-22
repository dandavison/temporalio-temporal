There are few conversations inn progress regarding adding "buffering" or "batching" functionality.

The task here is to gather all relevant context into one markdown document.



- The entire contents of the #crew-buffering-batching Slack channel is relevant.
- [Meeting summary](https://www.notion.so/temporalio/Meeting-March-11-2025-3208fc56773880129d8cef1138cacc78) posted in #crew-buffering-batching
- Jessica's [One-pager](https://www.notion.so/temporalio/Buffering-Batching-Handling-Work-Temporal-Can-t-Immediately-Process-31e8fc567738808c91e4d06bf989c581?d=3228fc567738806081f7001c7d96b4ee#3208fc56773880e486bbcaed65caf94e) (be sure to fetch all comments)
- Rippling's batch use case? Missing details on this. Mentioned by Paul [in slack](https://temporaltechnologies.slack.com/archives/C054VD4BZFD/p1773277061358009)
- OpenAI [Gong Call](https://us-11514.app.gong.io/call?id=6120936069767025036&email_type=call-ready-notification&xtid=4bpkim8czg9c1eq03aj) -- I don't have access yet
- Tao (Solution Architect)'s mention of a customer use case [here](https://temporaltechnologies.slack.com/archives/C08GZQN1C80/p1774071197820989)







Draft of a comment I thought about making in Jessica's one-pager:

 That’s technically possible (first response would contain a token; another API call would be needed
 to get the run ID and construct the “handle” in SDKs) but we can’t just do it because it would
 break semantics surrounding ID conflict policies, eager start, request ID deduplication, etc. And
 so this document’s solution is to do it, but by introducing a new thing which (AIUI) will return a
 precursor token with which the execution handle can be requested subsequently. If customers all
 switched to that instead of legacy APIs it would solve burst. But since it’s a new thing, we’re
 free to do what we want, and it would be even better to make that start a batch of executions
 rather than just starting one execution.


 ### Questions

- Is requiring callers to specify a "batch" what customers want?
-
