package collection

import (
	"go.temporal.io/server/chasm"
	"go.uber.org/fx"
)

var Module = fx.Module(
	"chasm.lib.collection",
	fx.Provide(NewLibrary),
	fx.Invoke(func(l *Library, registry *chasm.Registry) error {
		return registry.Register(l)
	}),
)
