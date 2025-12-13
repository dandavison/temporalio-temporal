package main

// Graph represents a state machine as a directed graph.
type Graph struct {
	// Name of the state machine (typically the type name).
	Name string
	// States is the set of all states in the state machine.
	States []string
	// Transitions is the list of all transitions between states.
	Transitions []Transition
}

// Transition represents a state machine transition.
type Transition struct {
	// Name of the transition (typically the variable name).
	Name string
	// Sources are the valid source states for this transition.
	Sources []string
	// Destination is the target state after the transition.
	Destination string
}

