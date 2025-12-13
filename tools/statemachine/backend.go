package main

// Backend is an interface for rendering state machine graphs to different formats.
type Backend interface {
	// Render converts a Graph to a string representation in the backend's format.
	Render(g *Graph) string
}

