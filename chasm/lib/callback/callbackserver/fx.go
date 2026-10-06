package callbackserver

import (
	"fmt"
	"net/http"

	"go.temporal.io/server/chasm/lib/callback"

	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/rpc/httpfaults"
	"go.temporal.io/server/common/telemetry"
	"go.temporal.io/server/common/testing/httpfaultstest"
	"go.temporal.io/server/common/testing/testhooks"
	queuescommon "go.temporal.io/server/service/history/queues/common"
	"go.uber.org/fx"
)

type libraryParams struct {
	fx.In

	InvocationTaskExecutor *invocationTaskExecutor
	// Only the history service runs the outbound queue, so only it provides this. Elsewhere it is
	// absent and callbacks are simply never reported as blocked.
	DestinationBlocked callback.DestinationBlockedFn `optional:"true"`
}

func newLibrary(params libraryParams) *callback.Library {
	return callback.NewLibrary(params.InvocationTaskExecutor, params.DestinationBlocked)
}

func register(
	registry *chasm.Registry,
	library *callback.Library,
) error {
	return registry.Register(library)
}

// httpCallerProviderProvider provides an HTTPCallerProvider for CHASM callbacks.
func httpCallerProviderProvider(
	clusterMetadata cluster.Metadata,
	namespaceRegistry namespace.Registry,
	rpcFactory common.RPCFactory,
	httpClientCache *cluster.FrontendHTTPClientCache,
	logger log.Logger,
	httpClientTransportInstrumenter telemetry.HTTPClientTransportInstrumenter,
	testHooks testhooks.TestHooks,
	config *Config,
) (HTTPCallerProvider, error) {
	localClient, err := rpcFactory.CreateLocalFrontendHTTPClient()
	if err != nil {
		return nil, fmt.Errorf("cannot create local frontend HTTP client: %w", err)
	}
	externalTransport, err := common.NewHTTPTransport(nil)
	if err != nil {
		return nil, err
	}
	externalClient := &http.Client{
		Transport: httpClientTransportInstrumenter.Instrument(externalTransport),
	}
	callbackTokenGenerator := commonnexus.NewCallbackTokenGenerator()
	httpFaultGenerator := httpfaultstest.NewGenerator(testHooks)

	m := collection.NewOnceMap(func(key queuescommon.NamespaceIDAndDestination) HTTPCaller {
		scope := httpfaults.Scope{NamespaceID: namespace.ID(key.NamespaceID)}
		caller := func(r *http.Request) (*http.Response, error) {
			return routeRequest(r,
				clusterMetadata,
				namespaceRegistry,
				httpClientCache,
				callbackTokenGenerator,
				externalClient,
				localClient,
				logger,
				config.InspectSourceHeader(),
			)
		}
		return httpfaults.Wrap(httpFaultGenerator, scope, caller)
	})
	return m.Get, nil
}

var Module = fx.Module(
	"chasm.lib.callback",
	fx.Provide(configProvider),
	fx.Provide(httpCallerProviderProvider),
	fx.Provide(newInvocationTaskExecutor),
	fx.Provide(newLibrary),
	fx.Invoke(register),
)
