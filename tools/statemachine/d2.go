package main

import (
	"fmt"
	"strings"
)

// D2Backend renders state machine graphs as D2 diagrams.
type D2Backend struct{}

// Render converts a Graph to D2 format.
func (b *D2Backend) Render(g *Graph) string {
	var sb strings.Builder

	// Write title if graph has a name
	if g.Name != "" {
		sb.WriteString(fmt.Sprintf("title: |md\n  # %s State Machine\n|\n\n", g.Name))
	}

	// Collect unique states to define their shapes
	stateSet := make(map[string]bool)
	var hasInitial bool
	for _, t := range g.Transitions {
		for _, src := range t.Sources {
			if isInitialState(src) {
				hasInitial = true
			} else {
				stateSet[src] = true
			}
		}
		if isInitialState(t.Destination) {
			hasInitial = true
		} else {
			stateSet[t.Destination] = true
		}
	}

	// Define initial state if present
	if hasInitial {
		sb.WriteString("initial: {\n  shape: circle\n  style.fill: \"#000\"\n  width: 20\n  height: 20\n}\n\n")
	}

	// Define terminal states (states with no outgoing transitions)
	terminalStates := findTerminalStates(g)

	// Define state nodes
	for state := range stateSet {
		stateID := sanitizeD2ID(state)
		stateLabel := formatStateLabel(state)
		if terminalStates[state] {
			sb.WriteString(fmt.Sprintf("%s: %s {\n  style.double-border: true\n}\n", stateID, stateLabel))
		} else {
			sb.WriteString(fmt.Sprintf("%s: %s\n", stateID, stateLabel))
		}
	}
	sb.WriteString("\n")

	// Write transitions
	for _, t := range g.Transitions {
		for _, src := range t.Sources {
			srcID := d2StateID(src)
			dstID := d2StateID(t.Destination)
			label := formatTransitionLabel(t.Name)
			sb.WriteString(fmt.Sprintf("%s -> %s: %s\n", srcID, dstID, label))
		}
	}

	return sb.String()
}

// isInitialState checks if a state is an initial/unspecified state.
func isInitialState(s string) bool {
	return s == "" || strings.Contains(strings.ToUpper(s), "UNSPECIFIED")
}

// d2StateID returns the D2 node ID for a state.
func d2StateID(s string) string {
	if isInitialState(s) {
		return "initial"
	}
	return sanitizeD2ID(s)
}

// sanitizeD2ID converts a state name to a valid D2 identifier.
func sanitizeD2ID(s string) string {
	s = stripEnumPrefix(s)
	// D2 identifiers can contain most characters, but we'll use snake_case for consistency
	var result strings.Builder
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' {
			result.WriteRune(r)
		} else {
			result.WriteRune('_')
		}
	}
	return strings.ToLower(result.String())
}

// formatStateLabel formats a state name for display.
func formatStateLabel(s string) string {
	s = stripEnumPrefix(s)
	// Convert CAPS_CASE to Title Case
	words := strings.Split(strings.ToLower(s), "_")
	for i, word := range words {
		if len(word) > 0 {
			words[i] = strings.ToUpper(word[:1]) + word[1:]
		}
	}
	return strings.Join(words, " ")
}

// formatTransitionLabel formats a transition name for display.
func formatTransitionLabel(s string) string {
	// Remove "Transition" prefix if present
	return strings.TrimPrefix(s, "Transition")
}

// findTerminalStates returns states that have no outgoing transitions.
func findTerminalStates(g *Graph) map[string]bool {
	// Collect all source states
	sources := make(map[string]bool)
	destinations := make(map[string]bool)
	for _, t := range g.Transitions {
		for _, src := range t.Sources {
			sources[src] = true
		}
		destinations[t.Destination] = true
	}

	// Terminal states are destinations that are never sources
	terminal := make(map[string]bool)
	for dst := range destinations {
		if !sources[dst] && !isInitialState(dst) {
			terminal[dst] = true
		}
	}
	return terminal
}
