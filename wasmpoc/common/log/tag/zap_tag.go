package tag

import (
	"fmt"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type (
	// ZapTag is a key and a typed value. The zap logger converts it to a zap.Field; this package does
	// not depend on zap. (The server's version wraps a zap.Field, which links zap into anything that
	// logs, about 3.5 MB of wasm.)
	ZapTag struct {
		key   string
		value any
	}
)

func (t ZapTag) Key() string {
	return t.key
}

func (t ZapTag) Value() any {
	return t.value
}

func NewStringTag(key string, value string) ZapTag {
	return ZapTag{key: key, value: value}
}

func NewStringsTag(key string, value []string) ZapTag {
	return ZapTag{key: key, value: value}
}

// NewStringerTag returns a tag that will lazily generate the string representation
// of the provided fmt.Stringer value. Note that it does **not** cache the result, so
// you should use `NewStringTag` instead if the tag is applied to the logger itself using
// `log.With`.
//
// These are still useful if the String() implementation is complicated, especially if
// you have lots of Debug-level logs that are ignored in production.
func NewStringerTag(key string, value fmt.Stringer) ZapTag {
	return ZapTag{key: key, value: value}
}

// NewStringersTag returns a tag that will lazily generate the string representation
// of the provided fmt.Stringer values. Note that it does **not** cache the results, so
// you should use `NewStringsTag` instead if the tag is applied to the logger itself using
// `log.With`.
//
// These are still useful if the String() implementation is complicated, especially if
// you have lots of Debug-level logs that are ignored in production.
func NewStringersTag(key string, value []fmt.Stringer) ZapTag {
	return ZapTag{key: key, value: value}
}

func NewInt64(key string, value int64) ZapTag {
	return ZapTag{key: key, value: value}
}

func NewInt(key string, value int) ZapTag {
	return ZapTag{key: key, value: value}
}

func NewInt32(key string, value int32) ZapTag {
	return ZapTag{key: key, value: value}
}

func NewUInt32(key string, value uint32) ZapTag {
	return ZapTag{key: key, value: value}
}

func NewUInt64(key string, value uint64) ZapTag {
	return ZapTag{key: key, value: value}
}

func NewFloat64(key string, value float64) ZapTag {
	return ZapTag{key: key, value: value}
}

func NewBoolTag(key string, value bool) ZapTag {
	return ZapTag{key: key, value: value}
}

func NewErrorTag(key string, value error) ZapTag {
	return ZapTag{key: key, value: value}
}

func NewDurationTag(key string, value time.Duration) ZapTag {
	return ZapTag{key: key, value: value}
}

func NewDurationPtrTag(key string, value *durationpb.Duration) ZapTag {
	return ZapTag{key: key, value: value.AsDuration()}
}

func NewTimeTag(key string, value time.Time) ZapTag {
	return ZapTag{key: key, value: value}
}

func NewTimePtrTag(key string, value *timestamppb.Timestamp) ZapTag {
	return ZapTag{key: key, value: value.AsTime()}
}

func NewAnyTag(key string, value any) ZapTag {
	return ZapTag{key: key, value: value}
}

func NewBinaryTag(key string, value []byte) ZapTag {
	return ZapTag{key: key, value: value}
}

// Shorter helpers (aliases for the New* functions above)

func String(key string, value string) ZapTag {
	return NewStringTag(key, value)
}

func Strings(key string, value []string) ZapTag {
	return NewStringsTag(key, value)
}

func Stringer(key string, value fmt.Stringer) ZapTag {
	return NewStringerTag(key, value)
}

func Stringers(key string, value []fmt.Stringer) ZapTag {
	return NewStringersTag(key, value)
}

func Int64(key string, value int64) ZapTag {
	return NewInt64(key, value)
}

func Int(key string, value int) ZapTag {
	return NewInt(key, value)
}

func Int32(key string, value int32) ZapTag {
	return NewInt32(key, value)
}

func UInt32(key string, value uint32) ZapTag {
	return NewUInt32(key, value)
}

func UInt64(key string, value uint64) ZapTag {
	return NewUInt64(key, value)
}

func Float64(key string, value float64) ZapTag {
	return NewFloat64(key, value)
}

func Duration(key string, value time.Duration) ZapTag {
	return NewDurationTag(key, value)
}

func DurationPtr(key string, value *durationpb.Duration) ZapTag {
	return NewDurationPtrTag(key, value)
}

func Time(key string, value time.Time) ZapTag {
	return NewTimeTag(key, value)
}

func TimePtr(key string, value *timestamppb.Timestamp) ZapTag {
	return NewTimePtrTag(key, value)
}

func Any(key string, value any) ZapTag {
	return NewAnyTag(key, value)
}

func Binary(key string, value []byte) ZapTag {
	return NewBinaryTag(key, value)
}

func Bool(key string, b bool) ZapTag {
	return NewBoolTag(key, b)
}
