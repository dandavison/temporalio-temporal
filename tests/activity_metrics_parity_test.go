package tests

// SAA↔WFA metrics parity. For the exhaustive catalog of activity metrics (dandavison/log#275), this
// establishes which ones each implementation emits: it drives one activity through each behavior in
// both implementations, captures the metrics emitted, prints the WFA-vs-SAA emission matrix, and
// asserts that both implementations emit the same metrics with the same tag keys for each behavior.
//
// There is no oracle. The equality assertion encodes the intended contract, so a failure can mean SAA is
// missing a metric, WFA is missing one, or the metric belongs to one implementation by design.
//
// Two asymmetries are intended and excluded from the equality assertion: the deprecated
// activity_end_to_end_latency alias, and activity_terminate (a workflow activity has no individual
// terminate path). See activityMetricCatalog.
//
// The asymmetries that are not intended are in knownDivergences, which records what the two
// implementations do today so that this test can run green while they are fixed one at a time.
//
// Not every catalog metric is attributable to a single driven activity through this driver. The
// shard/mutable-state aggregates, the eager-execution counter, and the matching worker-registry gauge
// are marked not-measured and only shown in the matrix; the namespace capture rejects non-namespaced
// metrics.

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

// activityMetric is one entry in the activity-metric catalog. measured marks whether this test queries
// the metric; compared marks whether the WFA and SAA emitted sets are asserted equal for it.
// compared ⊆ measured.
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

// knownDivergence is a WFA↔SAA difference that exists today and that we intend to fix. It suppresses
// the parity assertion it names and asserts the difference instead, so a divergence that gets fixed
// fails here rather than lingering as a stale exemption. Fixing one means deleting its entry.
//
// This is not activityMetricCatalog's compared=false, which marks an asymmetry nobody should fix.
type knownDivergence struct {
	metric string
	// scenario the divergence appears in; empty for every scenario.
	scenario string
	// emittedBy is the implementation that emits the metric when the other does not, "WFA" or "SAA".
	// Empty means both emit it and only the tag keys differ.
	emittedBy string
	reason    string
}

// operatorCommandTags is shared by the four operator commands, which take the same divergent path.
const operatorCommandTags = "WFA tags the operator commands with activity_targeting_method; SAA tags them " +
	"with the activity/task-queue set the lifecycle metrics use"

var knownDivergences = []knownDivergence{
	{metric: metrics.ActivityPayloadSize.Name(), emittedBy: "WFA",
		reason: "SAA records no payload size"},
	{metric: metrics.ActivityHeartbeatCount.Name(), scenario: "Heartbeat", emittedBy: "WFA",
		reason: "SAA does not count heartbeats"},
	{metric: metrics.ActivityStartToCloseLatency.Name(), scenario: "TerminalTimeout", emittedBy: "SAA",
		reason: "WFA records no start-to-close latency for an attempt that timed out"},
	{metric: metrics.ActivityPause.Name(), reason: operatorCommandTags},
	{metric: metrics.ActivityUnpause.Name(), reason: operatorCommandTags},
	{metric: metrics.ActivityReset.Name(), reason: operatorCommandTags},
	{metric: metrics.ActivityUpdateOptions.Name(), reason: operatorCommandTags},
}

// divergentEmission is the recorded divergence in which metric is emitted by one implementation only
// in this scenario, nil if there is none.
func divergentEmission(metric, scenario string) *knownDivergence {
	for _, d := range knownDivergences {
		if d.metric == metric && d.emittedBy != "" && (d.scenario == "" || d.scenario == scenario) {
			return &d
		}
	}
	return nil
}

// divergentTags is the recorded divergence in which both implementations emit metric but tag it
// differently, nil if there is none.
func divergentTags(metric string) *knownDivergence {
	for _, d := range knownDivergences {
		if d.metric == metric && d.emittedBy == "" {
			return &d
		}
	}
	return nil
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
			assertKnownDivergences(t, sc, sets)
			require.Equal(t, comparedSet(sc, sets.wfa), comparedSet(sc, sets.saa),
				"WFA and SAA must emit the same activity metrics for %q "+
					"(excluding the deprecated e2e-latency alias and non-lifecycle aggregates)", sc.name)
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

// comparedSet restricts an emitted set to the metrics whose WFA/SAA parity is asserted for this
// scenario.
func comparedSet(sc activityMetricsScenario, emitted map[string]map[string]string) map[string]bool {
	out := make(map[string]bool)
	for _, m := range activityMetricCatalog {
		if m.compared && divergentEmission(m.name, sc.name) == nil {
			if _, ok := emitted[m.name]; ok {
				out[m.name] = true
			}
		}
	}
	return out
}

// assertKnownDivergences requires every recorded divergence to still be there. One that has been
// fixed fails here, which is the signal to delete its entry and let the parity assertion cover it.
func assertKnownDivergences(t *testing.T, sc activityMetricsScenario, sets activityMetricSets) {
	for _, d := range knownDivergences {
		if d.scenario != "" && d.scenario != sc.name {
			continue
		}
		wfaTags, inWFA := sets.wfa[d.metric]
		saaTags, inSAA := sets.saa[d.metric]
		switch {
		case d.emittedBy == "WFA":
			require.True(t, inWFA && !inSAA,
				"%s is recorded as WFA-only (%s), but WFA emitted=%v SAA emitted=%v. If SAA now emits it, delete the knownDivergences entry",
				d.metric, d.reason, inWFA, inSAA)
		case d.emittedBy == "SAA":
			require.True(t, inSAA && !inWFA,
				"%s is recorded as SAA-only (%s), but WFA emitted=%v SAA emitted=%v. If WFA now emits it, delete the knownDivergences entry",
				d.metric, d.reason, inWFA, inSAA)
		case inWFA && inSAA:
			require.NotEqual(t, tagKeys(wfaTags), tagKeys(saaTags),
				"%s is recorded as tagged differently by the two implementations (%s), but they now agree. Delete the knownDivergences entry",
				d.metric, d.reason)
		default: // a tag divergence in a scenario where only one implementation emits the metric
		}
	}
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
		case divergentEmission(m.name, "") != nil:
			note = "  (known divergence: emitted by one implementation only)"
		case divergentTags(m.name) != nil:
			note = "  (known divergence: tagged differently)"
		default: // measured, compared, and no divergence recorded
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

// assertActivityMetricLabels asserts that every metric carries the test namespace, that the two timeout
// counters carry the timeout_type that fired, and that a metric both implementations emit carries the
// same tag keys, so that one dashboard works for either implementation.
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
		if !m.compared || divergentTags(m.name) != nil {
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
