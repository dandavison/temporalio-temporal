package collection

import (
	"go.temporal.io/server/chasm"
)

const (
	libraryName   = "collection"
	componentName = "collection"
)

var (
	Archetype   = chasm.FullyQualifiedName(libraryName, componentName)
	ArchetypeID = chasm.GenerateTypeID(Archetype)
)

type Library struct {
	chasm.UnimplementedLibrary
}

func NewLibrary() *Library {
	return &Library{}
}

func (l *Library) Name() string {
	return libraryName
}

func (l *Library) Components() []*chasm.RegistrableComponent {
	return []*chasm.RegistrableComponent{
		chasm.NewRegistrableComponent[*Collection](componentName),
	}
}
