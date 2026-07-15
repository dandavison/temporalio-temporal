// Command saaprose renders the standalone-activity operation behavior as Markdown for the
// documentation site (splices into docs/encyclopedia/activities/activity-operations.mdx). It is a
// thin renderer over the shared, spec-derived view-model in ../../lifecycle: every behavior statement
// in the output is computed by executing saaspec.Model. It authors only connective phrasing and never
// touches the implementation. Regenerate via `make saa-activity-operations-doc`.
package main

import (
	"fmt"
	"os"
	"sort"
	"strings"

	"go.temporal.io/server/chasm/lib/activity/saaspec"
	"go.temporal.io/server/chasm/lib/activity/saaspec/projections/lifecycle"
)

// matrixCfg gives the behavior matrix a start_delay (so the start-delay phase appears) and unlimited
// retries (so attempt 1 retries). terminalsCfg caps attempts and enables the timeouts so every
// terminal state is reachable.
var (
	matrixCfg    = saaspec.Config{HasStartDelay: true}
	terminalsCfg = saaspec.Config{
		HasScheduleToClose: true, HasScheduleToStart: true, HasHeartbeat: true, HasStartDelay: true,
		MaxAttempts: 1,
	}
)

func main() {
	var b strings.Builder

	b.WriteString("## Operation behavior by phase\n\n")
	b.WriteString("The effect of an operation depends on the phase the Activity Execution is in:\n\n")
	writeMatrix(&b)
	b.WriteString("\n")
	fmt.Fprintf(&b, "An Activity Execution ends in one of these terminal states: %s.\n",
		strings.Join(terminalStates(), ", "))

	if _, err := fmt.Print(b.String()); err != nil {
		fmt.Fprintln(os.Stderr, "saaprose:", err)
		os.Exit(1)
	}
}

// writeMatrix emits the operation-by-phase table. Columns are the distinct phases of the canonical
// trace (deduped by milestone+timer, so the two attempts' identical phases collapse); each cell is
// the spec-derived classification of that operation in that phase.
func writeMatrix(b *strings.Builder) {
	cols := distinctPhases(lifecycle.CanonicalTrace(matrixCfg))

	headers := []string{"Operation"}
	for _, p := range cols {
		headers = append(headers, phaseHeader(p))
	}
	writeRow(b, headers)
	writeRow(b, repeat("---", len(headers)))

	for _, op := range lifecycle.Ops {
		row := []string{op.Name}
		for _, p := range cols {
			row = append(row, op.Classify(matrixCfg, p.State).Label)
		}
		writeRow(b, row)
	}
}

func distinctPhases(phases []lifecycle.Phase) []lifecycle.Phase {
	seen := map[string]bool{}
	var out []lifecycle.Phase
	for _, p := range phases {
		key := p.Milestone + "|" + p.Timer
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, p)
	}
	return out
}

func phaseHeader(p lifecycle.Phase) string {
	if p.Terminal || p.Timer == "" {
		return p.Milestone
	}
	return fmt.Sprintf("%s (%s)", p.Milestone, p.Timer)
}

// terminalStates lists every reachable terminal state, sorted for stable output.
func terminalStates() []string {
	seen := map[string]bool{}
	var out []string
	for _, s := range lifecycle.Reachable(terminalsCfg) {
		if s.Status.Terminal() && !seen[s.Status.String()] {
			seen[s.Status.String()] = true
			out = append(out, s.Status.String())
		}
	}
	sort.Strings(out)
	return out
}

func writeRow(b *strings.Builder, cells []string) {
	b.WriteString("| " + strings.Join(cells, " | ") + " |\n")
}

func repeat(s string, n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = s
	}
	return out
}
