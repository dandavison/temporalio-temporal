package tests

// SAA↔WFA metrics parity. This test establishes, for the exhaustive catalog of activity metrics
// (dandavison/log#275), which ones the standalone activity (SAA) surface emits and which ones the
// workflow activity (WFA) surface emits. It drives one activity through each activity behavior on
// both surfaces with the shared model-based drivers, captures the metrics each surface emits, prints
// the full WFA-vs-SAA emission matrix, and asserts the two surfaces emit the same activity metrics
// for each behavior.
//
// As with the other TestWFASAA* repros, there is no oracle: the equality assertion encodes the
// intended contract (the same behavior should be observable the same way on both surfaces), and a
// failure is useful signal — it can mean SAA is missing a metric, WFA is missing one, or the metric
// belongs on only one surface by design. Two asymmetries are known-intended and excluded from the
// equality assertion (see activityMetricCatalog): the deprecated activity_end_to_end_latency alias
// (WFA-only; the new surface does not carry it) and activity_terminate (a workflow activity has no
// individual terminate path, so the behavior is SAA-only and asserted on its own).
//
// Not every catalog metric is reachable by driving one activity through raw worker/operator RPCs.
// The shard/mutable-state aggregates (activity_info_count/size, total_activity_count), the eager-
// execution counter (workflow-task path, disabled in the WFA helper), and the matching worker-
// registry gauge (needs a registered worker, not raw polls) are not namespace-attributable to a
// single driven activity; they are marked not-measured and shown in the matrix as such rather than
// queried (the namespace capture rejects non-namespaced metrics).

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
)

// activityMetric is one entry in the exhaustive activity-metric catalog. measured marks whether this
// test queries the metric (namespace-scoped and reachable by driving one activity); compared marks
// whether the WFA and SAA emitted sets are asserted equal for it. compared ⊆ measured. The deprecated
// e2e-latency alias and activity_terminate are measured but not compared (intended asymmetries); the
// shard/mutable-state aggregates, the eager-execution counter, and the worker-registry gauge are not
// measured (not attributable to a single activity through this harness) and only appear in the matrix.
type activityMetric struct {
	name     string
	measured bool
	compared bool
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

// activityMetricsScenario drives one activity behavior. maxAttempts sets the retry ceiling (so a
// terminal outcome is actually terminal). saaOnly marks a behavior with no WFA analog. anchor is a
// metric both surfaces emit at the end of the trace; when set, the driver waits for it before
// snapshotting, to absorb the async gap between an observed timeout transition and its metric
// emission. It is empty for traces whose final effect is a synchronous RPC (already committed on
// return).
type activityMetricsScenario struct {
	name        string
	trace       []model.Event
	maxAttempts int32
	saaOnly     bool
	anchor      string
}

var activityMetricsScenarios = []activityMetricsScenario{
	{name: "Success", trace: []model.Event{saaPoll, saaComplete}, maxAttempts: 1},
	{name: "TerminalFailure", trace: []model.Event{saaPoll, saaFailNonRetryably}, maxAttempts: 1},
	{name: "Cancel", trace: []model.Event{saaPoll, saaRequestCancel, {Kind: model.RespondCanceled}}, maxAttempts: 1},
	{name: "TerminalTimeout", trace: []model.Event{saaPoll, saaStartToCloseElapse}, maxAttempts: 1, anchor: metrics.ActivityTimeout.Name()},
	{name: "RetryableTaskFailure", trace: []model.Event{saaPoll, saaFailRetryably}, maxAttempts: 2},
	{name: "Heartbeat", trace: []model.Event{saaPoll, {Kind: model.Heartbeat}}, maxAttempts: 1},
	{name: "Pause", trace: []model.Event{saaPoll, saaPause}, maxAttempts: 1},
	{name: "Unpause", trace: []model.Event{saaPoll, saaPause, {Kind: model.Unpause}}, maxAttempts: 1},
	{name: "Reset", trace: []model.Event{saaPoll, {Kind: model.Reset}}, maxAttempts: 1},
	{name: "UpdateOptions", trace: []model.Event{saaPoll, {Kind: model.UpdateOptions}}, maxAttempts: 1},
	{name: "Terminate", trace: []model.Event{saaPoll, {Kind: model.Terminate}}, maxAttempts: 1, saaOnly: true},
}

// expectedTimeoutType returns the timeout_type tag value the timeout counters must carry for this
// scenario, or "" if the trace fires no timeout.
func (sc activityMetricsScenario) expectedTimeoutType() string {
	switch saaTimeoutIn(sc.trace) {
	case model.StartToCloseElapses:
		return enumspb.TIMEOUT_TYPE_START_TO_CLOSE.String()
	case model.ScheduleToCloseElapses:
		return enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE.String()
	case model.ScheduleToStartElapses:
		return enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START.String()
	case model.HeartbeatElapses:
		return enumspb.TIMEOUT_TYPE_HEARTBEAT.String()
	default:
		return ""
	}
}

// activityMetricSets holds, per surface, the metrics emitted for one scenario keyed by metric name,
// with the tag map of a representative recording as the value (presence is key existence). wfa is nil
// for a SAA-only scenario.
type activityMetricSets struct {
	wfa map[string]map[string]string
	saa map[string]map[string]string
}

func (s *standaloneActivityTestSuite) TestWFASAAMetricsParity() {
	env := s.newTestEnv()
	t := s.T()
	observed := make(map[string]activityMetricSets, len(activityMetricsScenarios))

	for _, sc := range activityMetricsScenarios {
		t.Run(sc.name, func(t *testing.T) {
			sets := activityMetricSets{saa: s.saaActivityMetrics(t, env, sc)}
			if !sc.saaOnly {
				sets.wfa = s.wfaActivityMetrics(t, env, sc)
			}
			observed[sc.name] = sets

			assertActivityMetricLabels(t, env, sc, sets)

			if sc.saaOnly {
				_, ok := sets.saa[metrics.ActivityTerminate.Name()]
				require.True(t, ok, "terminating a standalone activity must emit activity_terminate")
				return
			}
			require.Equal(t, comparedSet(sets.wfa), comparedSet(sets.saa),
				"WFA and SAA must emit the same activity metrics for %q "+
					"(excluding the deprecated e2e-latency alias and non-lifecycle aggregates)", sc.name)
		})
	}

	t.Log(activityMetricsMatrix(observed))
}

func (s *standaloneActivityTestSuite) saaActivityMetrics(t *testing.T, env *standaloneActivityEnv, sc activityMetricsScenario) map[string]map[string]string {
	return s.captureActivityMetrics(t, env, sc, func() {
		h := newSAAHarness(t, env, model.Config{MaxAttempts: sc.maxAttempts})
		h.shortTimeout = saaTimeoutIn(sc.trace)
		h.driveTrace(t, sc.trace)
	})
}

func (s *standaloneActivityTestSuite) wfaActivityMetrics(t *testing.T, env *standaloneActivityEnv, sc activityMetricsScenario) map[string]map[string]string {
	return s.captureActivityMetrics(t, env, sc, func() {
		h := newWFAHarness(t, env, sc.maxAttempts)
		h.shortTimeout = saaTimeoutIn(sc.trace)
		h.driveTrace(t, sc.trace)
	})
}

// captureActivityMetrics captures the activity metrics emitted while drive runs, scoped to the test
// namespace, returning each emitted metric's representative tag map. The two surfaces share the
// namespace but are captured in separate windows read back to back with their drive, so the window
// alone separates them.
func (s *standaloneActivityTestSuite) captureActivityMetrics(t *testing.T, env *standaloneActivityEnv, sc activityMetricsScenario, drive func()) map[string]map[string]string {
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

// comparedSet restricts an emitted set to the names of metrics whose WFA/SAA parity is asserted.
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

// activityMetricsMatrix renders the exhaustive per-metric emission matrix: for each catalog metric,
// whether WFA and/or SAA ever emitted it across the scenarios, followed by the per-scenario detail.
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
			note = "  (not measured by this harness)"
		case !m.compared:
			note = "  (not asserted: intended asymmetry)"
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

func tagKeys(tags map[string]string) []string {
	keys := make([]string, 0, len(tags))
	for k := range tags {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// assertActivityMetricLabels asserts the label conventions for each surface's activity metrics: every
// metric carries the test namespace; the two timeout counters carry the timeout_type that fired; and a
// metric both surfaces emit carries the same tag keys, so the same dashboards and aggregations work
// regardless of surface. The last is where the operator commands diverge (WFA tags them with
// activity_targeting_method and no task-queue scope, SAA with the full per-task-queue lifecycle set).
func assertActivityMetricLabels(t *testing.T, env *standaloneActivityEnv, sc activityMetricsScenario, sets activityMetricSets) {
	checkTags := func(surface string, emitted map[string]map[string]string) {
		for name, tags := range emitted {
			require.Equal(t, env.Namespace().String(), tags["namespace"],
				"%s %s must be tagged with the test namespace", surface, name)
		}
		if timeoutType := sc.expectedTimeoutType(); timeoutType != "" {
			for _, name := range []string{metrics.ActivityTaskTimeout.Name(), metrics.ActivityTimeout.Name()} {
				if tags, ok := emitted[name]; ok {
					require.Equal(t, timeoutType, tags["timeout_type"],
						"%s %s must carry the timeout_type that fired", surface, name)
				}
			}
		}
	}
	checkTags("SAA", sets.saa)
	if sc.saaOnly {
		return
	}
	checkTags("WFA", sets.wfa)

	for _, m := range activityMetricCatalog {
		if !m.compared {
			continue
		}
		wfaTags, wfaOK := sets.wfa[m.name]
		saaTags, saaOK := sets.saa[m.name]
		if wfaOK && saaOK {
			require.Equal(t, tagKeys(wfaTags), tagKeys(saaTags),
				"WFA and SAA must tag %s with the same label keys", m.name)
		}
	}
}
