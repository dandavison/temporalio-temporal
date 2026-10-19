package stream

import (
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/orderedstore"
)

const (
	libraryName   = "stream"
	componentName = "stream"
)

var (
	Archetype   = chasm.FullyQualifiedName(libraryName, componentName)
	ArchetypeID = chasm.GenerateTypeID(Archetype)
)

type library struct {
	chasm.UnimplementedLibrary
}

// NewNilLibrary creates a Library with all nil handlers. Useful for
// registration-only contexts like tdbg where no task execution is needed.
func NewNilLibrary() chasm.Library {
	return &library{}
}

func (l *library) Name() string {
	return libraryName
}

func (l *library) Components() []*chasm.RegistrableComponent {
	return []*chasm.RegistrableComponent{
		chasm.NewRegistrableComponent[*Stream](componentName),
		chasm.NewRegistrableComponent[*Subscription]("subscription", chasm.WithDetached()),
		// The ordered store stub has no library of its own.
		chasm.NewRegistrableComponent[*orderedstore.Log]("orderedstore_log"),
	}
}

func (l *library) Tasks() []*chasm.RegistrableTask {
	return []*chasm.RegistrableTask{
		chasm.NewRegistrablePureTask("subscriptionExpiry", subscriptionExpiryTaskHandler{}),
	}
}
