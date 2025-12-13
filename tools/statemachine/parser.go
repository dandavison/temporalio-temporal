package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
)

// ParsePackage parses a Go package and extracts state machine transitions.
// The pkgPath can be an absolute or relative path to a directory containing Go files.
func ParsePackage(pkgPath string) (*Graph, error) {
	fset := token.NewFileSet()

	pkgs, err := parser.ParseDir(fset, pkgPath, nil, parser.ParseComments)
	if err != nil {
		return nil, fmt.Errorf("failed to parse directory %s: %w", pkgPath, err)
	}

	if len(pkgs) == 0 {
		return nil, fmt.Errorf("no packages found in %s", pkgPath)
	}

	graph := &Graph{
		Name: filepath.Base(pkgPath),
	}
	stateSet := make(map[string]bool)

	for _, pkg := range pkgs {
		for _, file := range pkg.Files {
			transitions := extractTransitions(file)
			for _, t := range transitions {
				graph.Transitions = append(graph.Transitions, t)
				for _, src := range t.Sources {
					stateSet[src] = true
				}
				stateSet[t.Destination] = true
			}
		}
	}

	for state := range stateSet {
		graph.States = append(graph.States, state)
	}

	return graph, nil
}

// extractTransitions finds all chasm.NewTransition calls in a file and extracts transition info.
func extractTransitions(file *ast.File) []Transition {
	var transitions []Transition

	ast.Inspect(file, func(n ast.Node) bool {
		// Look for variable declarations
		genDecl, ok := n.(*ast.GenDecl)
		if !ok || genDecl.Tok != token.VAR {
			return true
		}

		for _, spec := range genDecl.Specs {
			valueSpec, ok := spec.(*ast.ValueSpec)
			if !ok {
				continue
			}

			for i, value := range valueSpec.Values {
				if t, ok := parseNewTransitionCall(value); ok {
					if i < len(valueSpec.Names) {
						t.Name = valueSpec.Names[i].Name
					}
					transitions = append(transitions, t)
				}
			}
		}
		return true
	})

	return transitions
}

// parseNewTransitionCall checks if an expression is a chasm.NewTransition call
// and extracts the transition information.
func parseNewTransitionCall(expr ast.Expr) (Transition, bool) {
	call, ok := expr.(*ast.CallExpr)
	if !ok {
		return Transition{}, false
	}

	// Check if this is a call to NewTransition (possibly with package qualifier)
	if !isNewTransitionCall(call.Fun) {
		return Transition{}, false
	}

	// NewTransition takes at least 3 arguments: sources, destination, apply func
	if len(call.Args) < 2 {
		return Transition{}, false
	}

	sources := extractStateList(call.Args[0])
	destination := extractStateName(call.Args[1])

	return Transition{
		Sources:     sources,
		Destination: destination,
	}, true
}

// isNewTransitionCall checks if the function expression is a call to NewTransition.
func isNewTransitionCall(fun ast.Expr) bool {
	switch f := fun.(type) {
	case *ast.Ident:
		return f.Name == "NewTransition"
	case *ast.SelectorExpr:
		return f.Sel.Name == "NewTransition"
	case *ast.IndexListExpr:
		// Generic instantiation: NewTransition[S, SM, E](...)
		return isNewTransitionCall(f.X)
	case *ast.IndexExpr:
		// Single type parameter generic instantiation
		return isNewTransitionCall(f.X)
	}
	return false
}

// extractStateList extracts state names from a slice literal expression.
func extractStateList(expr ast.Expr) []string {
	comp, ok := expr.(*ast.CompositeLit)
	if !ok {
		return nil
	}

	var states []string
	for _, elt := range comp.Elts {
		if name := extractStateName(elt); name != "" {
			states = append(states, name)
		}
	}
	return states
}

// extractStateName extracts a state name from an expression.
// Handles both simple identifiers and qualified names (pkg.Name).
func extractStateName(expr ast.Expr) string {
	switch e := expr.(type) {
	case *ast.Ident:
		return e.Name
	case *ast.SelectorExpr:
		return e.Sel.Name
	}
	return ""
}
