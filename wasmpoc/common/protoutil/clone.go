// Package protoutil holds proto helpers that need no server dependencies. In the server these
// live in the common root package, which imports most of the server.
package protoutil

import "google.golang.org/protobuf/proto"

func CloneProto[T proto.Message](v T) T {
	return proto.Clone(v).(T)
}
