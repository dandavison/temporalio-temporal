package activityserver

import (
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	"go.temporal.io/server/common/resource"
	"go.uber.org/fx"
	"google.golang.org/grpc"
)

var HistoryModule = fx.Module(
	"activity-history",
	fx.Provide(
		ConfigProvider,
		linkValidatorProvider,
		newHandler,
		newHistoryLibrary,
	),
	fx.Invoke(func(l *historyLibrary, registry *chasm.Registry) error {
		return registry.Register(l)
	}),
)

var FrontendModule = fx.Module(
	"activity-frontend",
	fx.Provide(ConfigProvider),
	fx.Provide(linkValidatorProvider),
	fx.Provide(activitypb.NewActivityServiceLayeredClient),
	fx.Provide(NewFrontendHandler),
	fx.Provide(resource.SearchAttributeValidatorProvider),
	fx.Invoke(func(config *Config, registry *chasm.Registry) error {
		// Frontend needs to register the component in order to serialize ComponentRefs, but doesn't
		// need task handlers.
		return registry.Register(activity.NewComponentOnlyLibrary(&config.Config))
	}),
)

// historyLibrary is the activity library with the activity service's gRPC handler.
type historyLibrary struct {
	chasm.Library
	handler *handler
}

type historyLibraryParams struct {
	fx.In

	Config           *Config
	Handler          *handler
	MatchingClient   resource.MatchingClient
	DispatchTaskHook activity.DispatchTaskHook `optional:"true"`
}

func newHistoryLibrary(params historyLibraryParams) *historyLibrary {
	return &historyLibrary{
		Library: activity.NewLibrary(
			matchingServiceAdapter{client: params.MatchingClient},
			params.DispatchTaskHook,
			&params.Config.Config,
		),
		handler: params.Handler,
	}
}

func (l *historyLibrary) RegisterServices(server *grpc.Server) {
	server.RegisterService(&activitypb.ActivityService_ServiceDesc, l.handler)
}
