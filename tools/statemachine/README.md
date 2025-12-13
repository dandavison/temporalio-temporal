# statemachine

Generates state machine diagrams from `chasm.NewTransition` definitions.

## Build

```bash
go build -o statemachine .
```

## Usage

```bash
# Mermaid (default)
./statemachine ../../chasm/lib/activity

# D2
./statemachine --format d2 ../../chasm/lib/activity

# Write to file
./statemachine -o activity.mmd ../../chasm/lib/activity
```

