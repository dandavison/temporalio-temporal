package stream

import (
	"go.temporal.io/server/chasm"
	"go.uber.org/fx"
)

var Module = fx.Module(
	"chasm.lib.stream",
	fx.Invoke(func(registry *chasm.Registry) error {
		return registry.Register(&library{})
	}),
)
