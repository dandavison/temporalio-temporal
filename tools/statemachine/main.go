package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var (
	outputFormat string
	outputFile   string
)

func main() {
	rootCmd := &cobra.Command{
		Use:   "statemachine [package-path]",
		Short: "Generate state machine visualizations from Go types",
		Long: `A tool to generate state machine diagrams from Go types that implement
the chasm.StateMachine interface. Extracts transitions defined using
chasm.NewTransition and produces Mermaid or D2 output.`,
		Args: cobra.ExactArgs(1),
		RunE: run,
	}

	rootCmd.Flags().StringVar(&outputFormat, "format", "mermaid", "output format: mermaid or d2")
	rootCmd.Flags().StringVarP(&outputFile, "output", "o", "", "output file (default: stdout)")

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func run(cmd *cobra.Command, args []string) error {
	pkgPath := args[0]

	graph, err := ParsePackage(pkgPath)
	if err != nil {
		return fmt.Errorf("failed to parse package: %w", err)
	}

	var backend Backend
	switch outputFormat {
	case "mermaid":
		backend = &MermaidBackend{}
	case "d2":
		backend = &D2Backend{}
	default:
		return fmt.Errorf("unknown format: %s", outputFormat)
	}

	output := backend.Render(graph)

	if outputFile != "" {
		if err := os.WriteFile(outputFile, []byte(output), 0644); err != nil {
			return fmt.Errorf("failed to write output file: %w", err)
		}
	} else {
		fmt.Print(output)
	}

	return nil
}
