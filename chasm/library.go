//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination library_mock.go

package chasm

import (
	"google.golang.org/grpc"
)

type (
	Library interface {
		Name() string
		Components() []*RegistrableComponent
		Tasks() []*RegistrableTask
		RegisterServices(server *grpc.Server)

		mustEmbedUnimplementedLibrary()
	}

	UnimplementedLibrary struct{}

	namer interface {
		Name() string
	}
)

func (UnimplementedLibrary) Components() []*RegistrableComponent {
	return nil
}

func (UnimplementedLibrary) Tasks() []*RegistrableTask {
	return nil
}

// RegisterServices Registers the gRPC calls to the handlers of the library.
func (UnimplementedLibrary) RegisterServices(_ *grpc.Server) {
}

// FullyQualifiedName creates a fully qualified name (FQN) by combining a library name
// and a component or task name. The FQN is used to uniquely identify components and
// tasks within the CHASM framework.
// The format of the returned FQN is: "libName.name"
func FullyQualifiedName(libName, name string) string {
	return libName + "." + name
}

// ComponentOrTaskName extracts the component or task name from a fully qualified name.
// For an FQN "libName.name", it returns "name". If there's no ".", returns the input as-is.
func ComponentOrTaskName(fqn string) string {
	for i := range fqn {
		if fqn[i] == '.' {
			return fqn[i+1:]
		}
	}
	return fqn
}

func (UnimplementedLibrary) mustEmbedUnimplementedLibrary() {}
