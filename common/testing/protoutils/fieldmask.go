package protoutils

import (
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// FieldPath represents a single field in a proto message, with both snake_case
// (for FieldMask) and camelCase (for display and skip-list lookup) representations.
type FieldPath struct {
	ProtoPath string // snake_case, for FieldMask
	JSONPath  string // camelCase, for display and skip-list lookup
}

// EnumerateFieldPaths walks a proto message descriptor and returns all field
// paths, recursing into sub-messages. Well-known scalar wrappers (Duration,
// Timestamp, etc.) are treated as leaves and not recursed into.
func EnumerateFieldPaths(md protoreflect.MessageDescriptor) []FieldPath {
	return enumerateFieldPaths(md, "", "")
}

func enumerateFieldPaths(md protoreflect.MessageDescriptor, protoPrefix, jsonPrefix string) []FieldPath {
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
			paths = append(paths, enumerateFieldPaths(fd.Message(), pp, jp)...)
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
	PopulateNonZeroVariant(msg, 1)
}

// PopulateNonZeroVariant recursively sets every field on a proto message to a
// distinguishable non-zero value derived from variant. For oneofs, only the
// first variant is set.
func PopulateNonZeroVariant(msg protoreflect.Message, variant int) {
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
				PopulateNonZeroVariant(elem.Message(), variant)
				list.Append(elem)
			} else {
				list.Append(nonZeroScalarValue(fd, variant))
			}
			continue
		}
		if fd.IsMap() {
			continue
		}
		if fd.Kind() == protoreflect.MessageKind || fd.Kind() == protoreflect.GroupKind {
			PopulateNonZeroVariant(msg.Mutable(fd).Message(), variant)
		} else {
			msg.Set(fd, nonZeroScalarValue(fd, variant))
		}
	}
}

func CopyPath(dst, src protoreflect.Message, protoPath string) {
	copyPath(dst, src, strings.Split(protoPath, "."))
}

func ClearPath(msg protoreflect.Message, protoPath string) {
	clearPath(msg, strings.Split(protoPath, "."))
}

func copyPath(dst, src protoreflect.Message, parts []string) {
	fd := mustFindField(dst.Descriptor(), parts[0])
	if len(parts) == 1 {
		if !src.Has(fd) {
			dst.Clear(fd)
			return
		}
		switch {
		case fd.IsList():
			dstList := dst.Mutable(fd).List()
			srcList := src.Get(fd).List()
			for dstList.Len() > 0 {
				dstList.Truncate(dstList.Len() - 1)
			}
			for i := 0; i < srcList.Len(); i++ {
				dstList.Append(cloneListElement(srcList.Get(i)))
			}
		case fd.Kind() == protoreflect.MessageKind || fd.Kind() == protoreflect.GroupKind:
			dst.Set(fd, protoreflect.ValueOfMessage(proto.Clone(src.Get(fd).Message().Interface()).ProtoReflect()))
		default:
			dst.Set(fd, src.Get(fd))
		}
		return
	}
	copyPath(dst.Mutable(fd).Message(), src.Get(fd).Message(), parts[1:])
}

func clearPath(msg protoreflect.Message, parts []string) {
	fd := mustFindField(msg.Descriptor(), parts[0])
	if len(parts) == 1 {
		msg.Clear(fd)
		return
	}
	clearPath(msg.Mutable(fd).Message(), parts[1:])
}

func cloneListElement(v protoreflect.Value) protoreflect.Value {
	if m := v.Message(); m.IsValid() {
		return protoreflect.ValueOfMessage(proto.Clone(m.Interface()).ProtoReflect())
	}
	return v
}

func mustFindField(md protoreflect.MessageDescriptor, name string) protoreflect.FieldDescriptor {
	fd := md.Fields().ByName(protoreflect.Name(name))
	if fd == nil {
		panic("unknown proto field path component: " + name)
	}
	return fd
}

func nonZeroScalarValue(fd protoreflect.FieldDescriptor, variant int) protoreflect.Value {
	switch fd.Kind() {
	case protoreflect.BoolKind:
		return protoreflect.ValueOfBool(true)
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		return protoreflect.ValueOfInt32(int32(41 + variant))
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		return protoreflect.ValueOfInt64(int64(41 + variant))
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		return protoreflect.ValueOfUint32(uint32(41 + variant))
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return protoreflect.ValueOfUint64(uint64(41 + variant))
	case protoreflect.FloatKind:
		return protoreflect.ValueOfFloat32(float32(variant) + 0.5)
	case protoreflect.DoubleKind:
		return protoreflect.ValueOfFloat64(float64(variant) + 0.5)
	case protoreflect.StringKind:
		return protoreflect.ValueOfString("test-" + string(rune('0'+variant)))
	case protoreflect.BytesKind:
		return protoreflect.ValueOfBytes([]byte("test-" + string(rune('0'+variant))))
	case protoreflect.EnumKind:
		return protoreflect.ValueOfEnum(protoreflect.EnumNumber(variant))
	default:
		return protoreflect.Value{}
	}
}
