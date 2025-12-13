package main

import (
	"fmt"
	"strings"
)

// MermaidBackend renders state machine graphs as Mermaid stateDiagram-v2.
type MermaidBackend struct{}

// Render converts a Graph to Mermaid stateDiagram-v2 format, wrapped in a markdown code block.
func (b *MermaidBackend) Render(g *Graph) string {
	var sb strings.Builder

	sb.WriteString("```mermaid\n")
	sb.WriteString("stateDiagram-v2\n")

	for _, t := range g.Transitions {
		for _, src := range t.Sources {
			srcID := sanitizeMermaidID(src)
			dstID := sanitizeMermaidID(t.Destination)
			label := sanitizeMermaidLabel(t.Name)
			sb.WriteString(fmt.Sprintf("    %s --> %s: %s\n", srcID, dstID, label))
		}
	}

	sb.WriteString("```\n")
	return sb.String()
}

// sanitizeMermaidID converts a state name to a valid Mermaid state ID.
func sanitizeMermaidID(s string) string {
	// Handle the special [*] start state
	if s == "" || strings.Contains(strings.ToUpper(s), "UNSPECIFIED") {
		return "[*]"
	}
	// Remove common prefixes and clean up the name
	s = stripEnumPrefix(s)
	// Replace any non-alphanumeric characters with underscores
	var result strings.Builder
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' {
			result.WriteRune(r)
		} else {
			result.WriteRune('_')
		}
	}
	return result.String()
}

// sanitizeMermaidLabel cleans up a transition name for use as an edge label.
func sanitizeMermaidLabel(s string) string {
	// Remove "Transition" prefix if present
	s = strings.TrimPrefix(s, "Transition")
	return s
}

// stripEnumPrefix removes common enum prefixes from state names.
func stripEnumPrefix(s string) string {
	// Handle protobuf enum naming conventions
	prefixes := []string{
		"ACTIVITY_EXECUTION_STATUS_",
		"NEXUS_OPERATION_STATUS_",
		"CALLBACK_STATUS_",
	}
	for _, prefix := range prefixes {
		if strings.HasPrefix(s, prefix) {
			return strings.TrimPrefix(s, prefix)
		}
	}
	// Generic: remove everything before the last underscore segment if it looks like an enum
	if idx := strings.LastIndex(s, "_"); idx != -1 {
		// Check if this looks like a CAPS_CASE enum value
		candidate := s[idx+1:]
		if strings.ToUpper(candidate) == candidate && len(candidate) > 2 {
			return candidate
		}
	}
	return s
}
