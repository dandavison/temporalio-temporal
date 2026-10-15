package notifier

import (
	"go.temporal.io/server/chasm"
	"go.uber.org/fx"
)

var Module = fx.Module(
	"chasm.lib.notifier",
	fx.Invoke(func(registry *chasm.Registry) error {
		return registry.Register(&library{})
	}),
)
