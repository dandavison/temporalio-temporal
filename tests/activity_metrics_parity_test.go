package tests

// SAA vs WFA metrics parity tests.

import (
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/chasm/lib/activity/model"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/tests/testcore"
)

// activityMetric is one entry in the activity-metric catalog.
type activityMetric struct {
	name     string
	measured bool // whether the test queries the metric
	compared bool // whether the WFA and SAA emitted sets are asserted equal for it; compared ⊆ measured
}

var activityMetricCatalog = []activityMetric{
	{metrics.ActivitySuccess.Name(), true, true},
	{metrics.ActivityFail.Name(), true, true},
	{metrics.ActivityTaskFail.Name(), true, true},
	{metrics.ActivityCancel.Name(), true, true},
	{metrics.ActivityTerminate.Name(), true, false}, // no per-activity terminate on WFA; SAA-only, asserted on its own
	{metrics.ActivityTimeout.Name(), true, true},
	{metrics.ActivityTaskTimeout.Name(), true, true},
	{metrics.ActivityStartToCloseLatency.Name(), true, true},
	{metrics.ActivityScheduleToCloseLatency.Name(), true, true},
	{metrics.ActivityE2ELatency.Name(), true, false}, // deprecated alias; WFA-only by intent
	{metrics.ActivityPause.Name(), true, true},
	{metrics.ActivityUnpause.Name(), true, true},
	{metrics.ActivityReset.Name(), true, true},
	{metrics.ActivityUpdateOptions.Name(), true, true},
	{metrics.ActivityHeartbeatCount.Name(), true, true},
	{metrics.ActivityPayloadSize.Name(), true, true},
	{metrics.ActivityEagerExecutionCounter.Name(), false, false},   // eager WFT path; disabled in the WFA helper
	{metrics.ActivityInfoCount.Name(), false, false},               // periodic mutable-state stats, not per-lifecycle
	{metrics.ActivityInfoSize.Name(), false, false},                // "
	{metrics.TotalActivityCount.Name(), false, false},              // "
	{metrics.WorkerRegistryActivitySlotsUsed.Name(), false, false}, // matching worker registry; no real worker here
}

// activityMetricsScenario drives one activity behavior. cfg.MaxAttempts caps retries, so a terminal
// outcome is actually terminal. saaOnly marks a behavior with no WFA analog. anchor is a metric both implementations
// emit at the end of the trace; when set, the driver waits for it before snapshotting, absorbing the
// async gap between an observed timeout transition and its metric emission. It is empty for a trace
// whose final effect is a synchronous RPC.
type activityMetricsScenario struct {
	name    string
	trace   []model.Event
	cfg     activityConfig
	saaOnly bool
	anchor  string
}

var activityMetricsScenarios = []activityMetricsScenario{
	{name: "Success", trace: []model.Event{model.Poll, model.Complete}, cfg: activityConfig{MaxAttempts: 1}},
	{name: "TerminalFailure", trace: []model.Event{model.Poll, model.FailNonRetryably}, cfg: activityConfig{MaxAttempts: 1}},
	{name: "Cancel", trace: []model.Event{model.Poll, model.RequestCancel, model.RespondCanceled}, cfg: activityConfig{MaxAttempts: 1}},
	{name: "TerminalTimeout", trace: []model.Event{model.Poll, model.StartToCloseElapses}, cfg: activityConfig{MaxAttempts: 1, StartToClose: activityShortTimeout}, anchor: metrics.ActivityTimeout.Name()},
	{name: "RetryableTaskFailure", trace: []model.Event{model.Poll, model.FailRetryably}, cfg: activityConfig{MaxAttempts: 2}},
	{name: "Heartbeat", trace: []model.Event{model.Poll, model.Heartbeat}, cfg: activityConfig{MaxAttempts: 1}},
	{name: "Pause", trace: []model.Event{model.Poll, model.Pause}, cfg: activityConfig{MaxAttempts: 1}},
	{name: "Unpause", trace: []model.Event{model.Poll, model.Pause, model.Unpause}, cfg: activityConfig{MaxAttempts: 1}},
	{name: "Reset", trace: []model.Event{model.Poll, model.Reset}, cfg: activityConfig{MaxAttempts: 1}},
	{name: "UpdateOptions", trace: []model.Event{model.Poll, model.UpdateOptions}, cfg: activityConfig{MaxAttempts: 1}},
	{name: "Terminate", trace: []model.Event{model.Poll, model.Terminate}, cfg: activityConfig{MaxAttempts: 1}, saaOnly: true},
}

// timeoutFiredBy is the timeout whose *Elapses event a trace fires, zero if none.
func timeoutFiredBy(trace []model.Event) model.EventType {
	for _, e := range trace {
		switch e.Type {
		case model.ScheduleToStartElapsesType, model.ScheduleToCloseElapsesType,
			model.StartToCloseElapsesType, model.HeartbeatElapsesType:
			return e.Type
		default: // an event that fires no timeout
		}
	}
	return 0
}

// expectedTimeoutType returns the timeout_type tag value the timeout counters must carry for this
// scenario, or "" if the trace fires no timeout.
func (sc activityMetricsScenario) expectedTimeoutType() string {
	switch timeoutFiredBy(sc.trace) {
	case model.StartToCloseElapsesType:
		return enumspb.TIMEOUT_TYPE_START_TO_CLOSE.String()
	case model.ScheduleToCloseElapsesType:
		return enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE.String()
	case model.ScheduleToStartElapsesType:
		return enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START.String()
	case model.HeartbeatElapsesType:
		return enumspb.TIMEOUT_TYPE_HEARTBEAT.String()
	default:
		return ""
	}
}

// activityMetricSets holds, per implementation, the metrics emitted for one scenario, keyed by name, with a
// representative recording's tag map as the value. wfa is nil for a SAA-only scenario.
type activityMetricSets struct {
	wfa map[string]map[string]string
	saa map[string]map[string]string
	// The namespace each implementation drove in. They differ, which is what separates the two captures.
	wfaNS, saaNS string
}

func (s *activityParityTestSuite) TestWFASAAMetricsParity() {
	t := s.T()
	observed := make(map[string]activityMetricSets, len(activityMetricsScenarios))

	for _, sc := range activityMetricsScenarios {
		t.Run(sc.name, func(t *testing.T) {
			saa, saaNS := s.saaActivityMetrics(t, sc)
			sets := activityMetricSets{saa: saa, saaNS: saaNS}
			if !sc.saaOnly {
				sets.wfa, sets.wfaNS = s.wfaActivityMetrics(t, sc)
			}
			observed[sc.name] = sets

			assertActivityMetricLabels(t, sc, sets)

			if sc.saaOnly {
				_, ok := sets.saa[metrics.ActivityTerminate.Name()]
				require.True(t, ok, "terminating a standalone activity must emit activity_terminate")
				return
			}
			require.Equal(t, comparedSet(sets.wfa), comparedSet(sets.saa),
				"WFA and SAA must emit the same activity metrics for %q", sc.name)
		})
	}

	t.Log(activityMetricsMatrix(observed))
}

func (s *activityParityTestSuite) saaActivityMetrics(t *testing.T, sc activityMetricsScenario) (map[string]map[string]string, string) {
	env := newActivityParityEnv(s.T())
	return s.captureActivityMetrics(t, env, sc, func() {
		newSAADriver(t, env, sc.cfg).driveTrace(t, sc.trace)
	}), env.Namespace().String()
}

func (s *activityParityTestSuite) wfaActivityMetrics(t *testing.T, sc activityMetricsScenario) (map[string]map[string]string, string) {
	env := newActivityParityEnv(s.T())
	return s.captureActivityMetrics(t, env, sc, func() {
		newWFADriver(t, env, sc.cfg).driveTrace(t, sc.trace)
	}), env.Namespace().String()
}

// captureActivityMetrics captures the activity metrics emitted while drive runs, scoped to env's
// namespace. Each implementation drives in its own namespace, so the capture separates them.
func (s *activityParityTestSuite) captureActivityMetrics(t *testing.T, env *testcore.TestEnv, sc activityMetricsScenario, drive func()) map[string]map[string]string {
	capture := env.StartNamespaceMetricCapture()
	drive()

	if sc.anchor != "" {
		await.RequireTrue(t, func() bool {
			return len(capture.Metric(sc.anchor)) > 0
		}, 15*time.Second, 100*time.Millisecond)
	}

	emitted := make(map[string]map[string]string)
	for _, m := range activityMetricCatalog {
		if !m.measured {
			continue
		}
		if recs := capture.Metric(m.name); len(recs) > 0 {
			emitted[m.name] = recs[0].Tags
		}
	}
	return emitted
}

// comparedSet restricts an emitted set to the metrics whose WFA/SAA parity is asserted.
func comparedSet(emitted map[string]map[string]string) map[string]bool {
	out := make(map[string]bool)
	for _, m := range activityMetricCatalog {
		if m.compared {
			if _, ok := emitted[m.name]; ok {
				out[m.name] = true
			}
		}
	}
	return out
}

// activityMetricsMatrix renders the per-metric emission matrix — whether WFA and SAA ever emitted each
// catalog metric across the scenarios — followed by the per-scenario detail and the tag keys.
func activityMetricsMatrix(observed map[string]activityMetricSets) string {
	wfaAny := make(map[string]bool)
	saaAny := make(map[string]bool)
	for _, sets := range observed {
		for name := range sets.wfa {
			wfaAny[name] = true
		}
		for name := range sets.saa {
			saaAny[name] = true
		}
	}

	var b strings.Builder
	b.WriteString("\n=== Activity metric emission: WFA vs SAA (aggregated across scenarios) ===\n")
	for _, m := range activityMetricCatalog {
		note := ""
		switch {
		case !m.measured:
			note = "  (not measured by this driver)"
		case !m.compared:
			note = "  (not asserted: intended asymmetry)"
		default: // measured and compared
		}
		fmt.Fprintf(&b, "  %-40s WFA:%s SAA:%s%s\n", m.name, mark(wfaAny[m.name]), mark(saaAny[m.name]), note)
	}

	b.WriteString("\n=== Per-scenario emitted activity metrics ===\n")
	for _, sc := range activityMetricsScenarios {
		sets := observed[sc.name]
		if sc.saaOnly {
			fmt.Fprintf(&b, "  %-22s SAA-only: %s\n", sc.name, emittedList(sets.saa))
			continue
		}
		fmt.Fprintf(&b, "  %-22s WFA: %s\n", sc.name, emittedList(sets.wfa))
		fmt.Fprintf(&b, "  %-22s SAA: %s\n", "", emittedList(sets.saa))
	}

	b.WriteString("\n=== Tag keys per compared metric (WFA | SAA) ===\n")
	for _, m := range activityMetricCatalog {
		if !m.compared {
			continue
		}
		wfaKeys, saaKeys := "-", "-"
		for _, sc := range activityMetricsScenarios {
			if tags, ok := observed[sc.name].wfa[m.name]; ok {
				wfaKeys = strings.Join(tagKeys(tags), ",")
			}
			if tags, ok := observed[sc.name].saa[m.name]; ok {
				saaKeys = strings.Join(tagKeys(tags), ",")
			}
		}
		fmt.Fprintf(&b, "  %-40s\n    WFA: %s\n    SAA: %s\n", m.name, wfaKeys, saaKeys)
	}
	return b.String()
}

func mark(b bool) string {
	if b {
		return "✓"
	}
	return "·"
}

func emittedList(emitted map[string]map[string]string) string {
	names := make([]string, 0, len(emitted))
	for name := range emitted {
		names = append(names, name)
	}
	sort.Strings(names)
	if len(names) == 0 {
		return "(none)"
	}
	return strings.Join(names, ", ")
}

// wfaOnlyTagKeys are tag keys that WFA emits and SAA is not required to. activity_targeting_method
// distinguishes the Id and Type branches of the legacy PauseActivityRequest oneof; the CHASM RPCs
// address a single activity and have no such branch, so the tag would carry no information there.
var wfaOnlyTagKeys = map[string]bool{
	"activity_targeting_method": true,
}

// comparedTagKeys is tagKeys without the keys SAA is not required to carry.
func comparedTagKeys(tags map[string]string) []string {
	keys := make([]string, 0, len(tags))
	for _, k := range tagKeys(tags) {
		if !wfaOnlyTagKeys[k] {
			keys = append(keys, k)
		}
	}
	return keys
}

func tagKeys(tags map[string]string) []string {
	keys := make([]string, 0, len(tags))
	for k := range tags {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// assertActivityMetricLabels asserts that every metric carries the test namespace, that the two timeout
// counters carry the timeout_type that fired, and that SAA tags a metric with at least the keys WFA
// tags it with, so that a query written against WFA keeps working against SAA.
//
// Superset rather than equality: the CHASM implementation carries the richer tag set, and requiring
// equality would stop it doing so. A WFA-only key has to be justified instead, in wfaOnlyTagKeys.
func assertActivityMetricLabels(t *testing.T, sc activityMetricsScenario, sets activityMetricSets) {
	checkTags := func(implementation string, emitted map[string]map[string]string, ns string) {
		for name, tags := range emitted {
			require.Equal(t, ns, tags["namespace"],
				"%s %s must be tagged with the namespace it was driven in", implementation, name)
		}
		if timeoutType := sc.expectedTimeoutType(); timeoutType != "" {
			for _, name := range []string{metrics.ActivityTaskTimeout.Name(), metrics.ActivityTimeout.Name()} {
				if tags, ok := emitted[name]; ok {
					require.Equal(t, timeoutType, tags["timeout_type"],
						"%s %s must carry the timeout_type that fired", implementation, name)
				}
			}
		}
	}
	checkTags("SAA", sets.saa, sets.saaNS)
	if sc.saaOnly {
		return
	}
	checkTags("WFA", sets.wfa, sets.wfaNS)

	for _, m := range activityMetricCatalog {
		if !m.compared {
			continue
		}
		wfaTags, wfaOK := sets.wfa[m.name]
		saaTags, saaOK := sets.saa[m.name]
		if wfaOK && saaOK {
			require.Subset(t, tagKeys(saaTags), comparedTagKeys(wfaTags),
				"SAA must tag %s with at least the keys WFA tags it with", m.name)
		}
	}
}
