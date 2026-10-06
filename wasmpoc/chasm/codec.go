package chasm

import (
	"fmt"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"google.golang.org/protobuf/proto"
)

// encodeChasmBlob encodes CHASM data and task payloads as deterministic proto3.
// The server's persistence codec also supports JSON; that support lives in the persistence layer,
// not here.
func encodeChasmBlob(m proto.Message) (*commonpb.DataBlob, error) {
	data, err := proto.MarshalOptions{Deterministic: true}.Marshal(m)
	if err != nil {
		return nil, err
	}
	return &commonpb.DataBlob{EncodingType: enumspb.ENCODING_TYPE_PROTO3, Data: data}, nil
}

func decodeChasmBlob(blob *commonpb.DataBlob, result proto.Message) error {
	if blob.GetEncodingType() != enumspb.ENCODING_TYPE_PROTO3 {
		return fmt.Errorf("unsupported CHASM blob encoding %v", blob.GetEncodingType())
	}
	return proto.Unmarshal(blob.GetData(), result)
}
