package util

import (
	"strings"

	"golang.org/x/text/cases"
	"golang.org/x/text/language"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

// CloneProtoMap returns a new map containing a clone of each individual proto.
func CloneProtoMap[K comparable, T proto.Message](src map[K]T) map[K]T {
	if src == nil {
		return nil
	}

	result := make(map[K]T, len(src))
	for k, v := range src {
		result[k] = proto.CloneOf(v)
	}
	return result
}

// CloneProtoSlice returns a new slice containing a clone of each individual proto.
func CloneProtoSlice[T proto.Message](src []T) []T {
	if src == nil {
		return nil
	}

	result := make([]T, len(src))
	for i, v := range src {
		result[i] = proto.CloneOf(v)
	}
	return result
}

func ConvertPathToCamel(input string) []string {
	var pathParts []string
	for path := range strings.SplitSeq(input, ".") {
		// Split by "_" and convert each word to Title Case (CamelCase)
		var b strings.Builder
		j := 0
		for word := range strings.SplitSeq(path, "_") {
			if j > 0 {
				b.WriteString(cases.Title(language.Und).String(strings.ToLower(word)))
			} else {
				// lowercase the first letter
				if len(word) > 0 {
					b.WriteString(strings.ToLower(word[:1]))
					if len(word) > 1 {
						b.WriteString(word[1:])
					}
				}
			}
			j++
		}
		// Join the words into a CamelCase substring
		pathParts = append(pathParts, b.String())
	}
	// Join all CamelCase substrings back with "."
	return pathParts
}

func ParseFieldMask(mask *fieldmaskpb.FieldMask) map[string]struct{} {
	fieldMaskPaths := make(map[string]struct{})

	for _, path := range mask.Paths {
		pathParts := ConvertPathToCamel(path)
		jsonPath := strings.Join(pathParts, ".")
		fieldMaskPaths[jsonPath] = struct{}{}
	}

	return fieldMaskPaths
}

func FieldMaskHasSubPath(fieldMaskPaths map[string]struct{}, path string) bool {
	prefix := strings.TrimSuffix(path, ".") + "."
	for field := range fieldMaskPaths {
		if strings.HasPrefix(field, prefix) {
			return true
		}
	}
	return false
}

func FieldMaskHasPathOrSubPath(fieldMaskPaths map[string]struct{}, path string) bool {
	if _, ok := fieldMaskPaths[path]; ok {
		return true
	}
	return FieldMaskHasSubPath(fieldMaskPaths, path)
}
