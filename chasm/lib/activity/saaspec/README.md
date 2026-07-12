To run these tests

```
# By test function (-run):
go test -tags test_dep -run 'TestStandaloneActivityTestSuite/TestSpecExplorer' ./tests/
go test -tags test_dep -run 'TestStandaloneActivityTestSuite/TestSpecTimerProbes' ./tests/
go test -tags test_dep -run 'TestStandaloneActivityTestSuite/TestSpecKnownGaps' ./tests/
go test ./chasm/lib/activity/saaspec/conformance/ -v      # static check, no server

# Timer probes by event/scenario (now named subtests, slash-hierarchy):
go test -tags test_dep -run 'TestStandaloneActivityTestSuite/TestSpecTimerProbes/heartbeat' -v ./tests/
# also: /STC  /S2S  /startToClose  /heartbeat/started-retry  etc.

# Explorer focused on an event type (SAASPEC_EVENT, comma-separated, case-insensitive):
SAASPEC_EVENT=Reset,Pause go test -tags test_dep -run 'TestStandaloneActivityTestSuite/TestSpecExplorer' -v ./tests/
```
