package stream

import (
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/orderedstore"
)

const (
	libraryName             = "stream"
	componentName           = "stream"
	standaloneComponentName = "standalone"
)

// Archetype is a standalone stream's.
var (
	Archetype   = chasm.FullyQualifiedName(libraryName, standaloneComponentName)
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
		chasm.NewRegistrableComponent[*Standalone](standaloneComponentName, chasm.WithBusinessIDAlias("StreamId")),
		// The ordered store stub has no library of its own.
		chasm.NewRegistrableComponent[*orderedstore.Log]("orderedstore_log"),
	}
}

func (l *library) Tasks() []*chasm.RegistrableTask {
	return []*chasm.RegistrableTask{
		chasm.NewRegistrablePureTask("idleTimeout", idleTimeoutTaskHandler{}),
	}
}
