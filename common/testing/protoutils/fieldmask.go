package protoutils

import (
	"google.golang.org/protobuf/reflect/protoreflect"
)

// FieldPath represents a single field in a proto message, with both snake_case
// (for FieldMask) and camelCase (for display and skip-list lookup) representations.
type FieldPath struct {
	ProtoPath string // snake_case, for FieldMask
	JSONPath  string // camelCase, for display and skip-list lookup
}

// EnumerateFieldPaths walks a proto message descriptor and returns all field paths
// up to the given depth. Depth 1 = top-level fields only; depth 2 = also sub-fields
// of message-typed fields; etc. Well-known scalar wrappers (Duration, Timestamp, etc.)
// are treated as leaves and not recursed into.
func EnumerateFieldPaths(
	md protoreflect.MessageDescriptor, protoPrefix, jsonPrefix string, depth int,
) []FieldPath {
	if depth <= 0 {
		return nil
	}
	var paths []FieldPath
	fields := md.Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		pp := string(fd.Name())
		jp := fd.JSONName()
		if protoPrefix != "" {
			pp = protoPrefix + "." + pp
			jp = jsonPrefix + "." + jp
		}
		paths = append(paths, FieldPath{ProtoPath: pp, JSONPath: jp})
		if fd.Kind() == protoreflect.MessageKind && !isWellKnownScalar(fd.Message().FullName()) {
			paths = append(paths, EnumerateFieldPaths(fd.Message(), pp, jp, depth-1)...)
		}
	}
	return paths
}

func isWellKnownScalar(name protoreflect.FullName) bool {
	switch name {
	case "google.protobuf.Duration",
		"google.protobuf.Timestamp",
		"google.protobuf.StringValue",
		"google.protobuf.BoolValue",
		"google.protobuf.Int32Value",
		"google.protobuf.Int64Value",
		"google.protobuf.UInt32Value",
		"google.protobuf.UInt64Value",
		"google.protobuf.FloatValue",
		"google.protobuf.DoubleValue",
		"google.protobuf.BytesValue":
		return true
	}
	return false
}

// PopulateNonZero recursively sets every field on a proto message to a
// distinguishable non-zero value. For oneofs, only the first variant is set.
func PopulateNonZero(msg protoreflect.Message) {
	fields := msg.Descriptor().Fields()
	oneofsSeen := make(map[protoreflect.Name]bool)
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		if od := fd.ContainingOneof(); od != nil {
			if oneofsSeen[od.Name()] {
				continue
			}
			oneofsSeen[od.Name()] = true
		}
		if fd.IsList() {
			list := msg.Mutable(fd).List()
			if fd.Kind() == protoreflect.MessageKind {
				elem := list.NewElement()
				PopulateNonZero(elem.Message())
				list.Append(elem)
			} else {
				list.Append(nonZeroScalarValue(fd))
			}
			continue
		}
		if fd.IsMap() {
			continue
		}
		if fd.Kind() == protoreflect.MessageKind || fd.Kind() == protoreflect.GroupKind {
			PopulateNonZero(msg.Mutable(fd).Message())
		} else {
			msg.Set(fd, nonZeroScalarValue(fd))
		}
	}
}

func nonZeroScalarValue(fd protoreflect.FieldDescriptor) protoreflect.Value {
	switch fd.Kind() {
	case protoreflect.BoolKind:
		return protoreflect.ValueOfBool(true)
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		return protoreflect.ValueOfInt32(42)
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		return protoreflect.ValueOfInt64(42)
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		return protoreflect.ValueOfUint32(42)
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return protoreflect.ValueOfUint64(42)
	case protoreflect.FloatKind:
		return protoreflect.ValueOfFloat32(1.5)
	case protoreflect.DoubleKind:
		return protoreflect.ValueOfFloat64(1.5)
	case protoreflect.StringKind:
		return protoreflect.ValueOfString("test")
	case protoreflect.BytesKind:
		return protoreflect.ValueOfBytes([]byte("test"))
	case protoreflect.EnumKind:
		return protoreflect.ValueOfEnum(1)
	default:
		return protoreflect.Value{}
	}
}
