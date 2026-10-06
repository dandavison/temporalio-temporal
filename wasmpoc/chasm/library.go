package chasm

type (
	Library interface {
		Name() string
		Components() []*RegistrableComponent
		Tasks() []*RegistrableTask

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

func (UnimplementedLibrary) mustEmbedUnimplementedLibrary() {}

// FullyQualifiedName creates a fully qualified name (FQN) by combining a library name
// and a component or task name. The FQN is used to uniquely identify components and
// tasks within the CHASM framework.
// The format of the returned FQN is: "libName.name"
func FullyQualifiedName(libName, name string) string {
	return libName + "." + name
}
