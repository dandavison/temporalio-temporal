// The MIT License
//
// Copyright (c) 2023 Temporal Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// source: temporal/server/api/common/v1/dlq.proto

package commonspb

import (
	reflect "reflect"
	sync "sync"

	v1 "go.temporal.io/api/common/v1"
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type HistoryTask struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// shard_id is included to avoid having to deserialize the task blob.
	ShardId int32        `protobuf:"varint,1,opt,name=shard_id,json=shardId,proto3" json:"shard_id,omitempty"`
	Blob    *v1.DataBlob `protobuf:"bytes,2,opt,name=blob,proto3" json:"blob,omitempty"`
}

func (x *HistoryTask) Reset() {
	*x = HistoryTask{}
	if protoimpl.UnsafeEnabled {
		mi := &file_temporal_server_api_common_v1_dlq_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *HistoryTask) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*HistoryTask) ProtoMessage() {}

func (x *HistoryTask) ProtoReflect() protoreflect.Message {
	mi := &file_temporal_server_api_common_v1_dlq_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use HistoryTask.ProtoReflect.Descriptor instead.
func (*HistoryTask) Descriptor() ([]byte, []int) {
	return file_temporal_server_api_common_v1_dlq_proto_rawDescGZIP(), []int{0}
}

func (x *HistoryTask) GetShardId() int32 {
	if x != nil {
		return x.ShardId
	}
	return 0
}

func (x *HistoryTask) GetBlob() *v1.DataBlob {
	if x != nil {
		return x.Blob
	}
	return nil
}

type HistoryDLQTaskMetadata struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// message_id is the zero-indexed sequence number of the message in the queue that contains this history task.
	MessageId int64 `protobuf:"varint,1,opt,name=message_id,json=messageId,proto3" json:"message_id,omitempty"`
}

func (x *HistoryDLQTaskMetadata) Reset() {
	*x = HistoryDLQTaskMetadata{}
	if protoimpl.UnsafeEnabled {
		mi := &file_temporal_server_api_common_v1_dlq_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *HistoryDLQTaskMetadata) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*HistoryDLQTaskMetadata) ProtoMessage() {}

func (x *HistoryDLQTaskMetadata) ProtoReflect() protoreflect.Message {
	mi := &file_temporal_server_api_common_v1_dlq_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use HistoryDLQTaskMetadata.ProtoReflect.Descriptor instead.
func (*HistoryDLQTaskMetadata) Descriptor() ([]byte, []int) {
	return file_temporal_server_api_common_v1_dlq_proto_rawDescGZIP(), []int{1}
}

func (x *HistoryDLQTaskMetadata) GetMessageId() int64 {
	if x != nil {
		return x.MessageId
	}
	return 0
}

// HistoryDLQTask is a history task that has been moved to the DLQ, so it also has a message ID (index within that
// queue).
type HistoryDLQTask struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Metadata *HistoryDLQTaskMetadata `protobuf:"bytes,1,opt,name=metadata,proto3" json:"metadata,omitempty"`
	// This is named payload to prevent stuttering (e.g. task.Task).
	Payload *HistoryTask `protobuf:"bytes,2,opt,name=payload,proto3" json:"payload,omitempty"`
}

func (x *HistoryDLQTask) Reset() {
	*x = HistoryDLQTask{}
	if protoimpl.UnsafeEnabled {
		mi := &file_temporal_server_api_common_v1_dlq_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *HistoryDLQTask) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*HistoryDLQTask) ProtoMessage() {}

func (x *HistoryDLQTask) ProtoReflect() protoreflect.Message {
	mi := &file_temporal_server_api_common_v1_dlq_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use HistoryDLQTask.ProtoReflect.Descriptor instead.
func (*HistoryDLQTask) Descriptor() ([]byte, []int) {
	return file_temporal_server_api_common_v1_dlq_proto_rawDescGZIP(), []int{2}
}

func (x *HistoryDLQTask) GetMetadata() *HistoryDLQTaskMetadata {
	if x != nil {
		return x.Metadata
	}
	return nil
}

func (x *HistoryDLQTask) GetPayload() *HistoryTask {
	if x != nil {
		return x.Payload
	}
	return nil
}

// HistoryDLQKey is a compound key that identifies a history DLQ.
type HistoryDLQKey struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// task_category is the category of the task. The default values are defined in the TaskCategory enum. However, there
	// may also be other categories registered at runtime with the history/tasks package. As a result, the category here
	// is an integer instead of an enum to support both the default values and custom values.
	TaskCategory int32 `protobuf:"varint,1,opt,name=task_category,json=taskCategory,proto3" json:"task_category,omitempty"`
	// source_cluster and target_cluster must both be non-empty. For non-cross DC tasks, i.e. non-replication tasks,
	// they should be the same. The reason for this is that we may support wildcard clusters in the future, and we want
	// to differentiate between queues which go from one cluster to all other clusters, and queues which don't leave the
	// current cluster.
	SourceCluster string `protobuf:"bytes,2,opt,name=source_cluster,json=sourceCluster,proto3" json:"source_cluster,omitempty"`
	TargetCluster string `protobuf:"bytes,3,opt,name=target_cluster,json=targetCluster,proto3" json:"target_cluster,omitempty"`
}

func (x *HistoryDLQKey) Reset() {
	*x = HistoryDLQKey{}
	if protoimpl.UnsafeEnabled {
		mi := &file_temporal_server_api_common_v1_dlq_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *HistoryDLQKey) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*HistoryDLQKey) ProtoMessage() {}

func (x *HistoryDLQKey) ProtoReflect() protoreflect.Message {
	mi := &file_temporal_server_api_common_v1_dlq_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use HistoryDLQKey.ProtoReflect.Descriptor instead.
func (*HistoryDLQKey) Descriptor() ([]byte, []int) {
	return file_temporal_server_api_common_v1_dlq_proto_rawDescGZIP(), []int{3}
}

func (x *HistoryDLQKey) GetTaskCategory() int32 {
	if x != nil {
		return x.TaskCategory
	}
	return 0
}

func (x *HistoryDLQKey) GetSourceCluster() string {
	if x != nil {
		return x.SourceCluster
	}
	return ""
}

func (x *HistoryDLQKey) GetTargetCluster() string {
	if x != nil {
		return x.TargetCluster
	}
	return ""
}

var File_temporal_server_api_common_v1_dlq_proto protoreflect.FileDescriptor

var file_temporal_server_api_common_v1_dlq_proto_rawDesc = []byte{
	0x0a, 0x27, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2f, 0x73, 0x65, 0x72, 0x76, 0x65,
	0x72, 0x2f, 0x61, 0x70, 0x69, 0x2f, 0x63, 0x6f, 0x6d, 0x6d, 0x6f, 0x6e, 0x2f, 0x76, 0x31, 0x2f,
	0x64, 0x6c, 0x71, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x1d, 0x74, 0x65, 0x6d, 0x70, 0x6f,
	0x72, 0x61, 0x6c, 0x2e, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2e, 0x61, 0x70, 0x69, 0x2e, 0x63,
	0x6f, 0x6d, 0x6d, 0x6f, 0x6e, 0x2e, 0x76, 0x31, 0x1a, 0x24, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72,
	0x61, 0x6c, 0x2f, 0x61, 0x70, 0x69, 0x2f, 0x63, 0x6f, 0x6d, 0x6d, 0x6f, 0x6e, 0x2f, 0x76, 0x31,
	0x2f, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0x5e,
	0x0a, 0x0b, 0x48, 0x69, 0x73, 0x74, 0x6f, 0x72, 0x79, 0x54, 0x61, 0x73, 0x6b, 0x12, 0x19, 0x0a,
	0x08, 0x73, 0x68, 0x61, 0x72, 0x64, 0x5f, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x05, 0x52,
	0x07, 0x73, 0x68, 0x61, 0x72, 0x64, 0x49, 0x64, 0x12, 0x34, 0x0a, 0x04, 0x62, 0x6c, 0x6f, 0x62,
	0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x20, 0x2e, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61,
	0x6c, 0x2e, 0x61, 0x70, 0x69, 0x2e, 0x63, 0x6f, 0x6d, 0x6d, 0x6f, 0x6e, 0x2e, 0x76, 0x31, 0x2e,
	0x44, 0x61, 0x74, 0x61, 0x42, 0x6c, 0x6f, 0x62, 0x52, 0x04, 0x62, 0x6c, 0x6f, 0x62, 0x22, 0x37,
	0x0a, 0x16, 0x48, 0x69, 0x73, 0x74, 0x6f, 0x72, 0x79, 0x44, 0x4c, 0x51, 0x54, 0x61, 0x73, 0x6b,
	0x4d, 0x65, 0x74, 0x61, 0x64, 0x61, 0x74, 0x61, 0x12, 0x1d, 0x0a, 0x0a, 0x6d, 0x65, 0x73, 0x73,
	0x61, 0x67, 0x65, 0x5f, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x03, 0x52, 0x09, 0x6d, 0x65,
	0x73, 0x73, 0x61, 0x67, 0x65, 0x49, 0x64, 0x22, 0xa9, 0x01, 0x0a, 0x0e, 0x48, 0x69, 0x73, 0x74,
	0x6f, 0x72, 0x79, 0x44, 0x4c, 0x51, 0x54, 0x61, 0x73, 0x6b, 0x12, 0x51, 0x0a, 0x08, 0x6d, 0x65,
	0x74, 0x61, 0x64, 0x61, 0x74, 0x61, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x35, 0x2e, 0x74,
	0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2e, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2e, 0x61,
	0x70, 0x69, 0x2e, 0x63, 0x6f, 0x6d, 0x6d, 0x6f, 0x6e, 0x2e, 0x76, 0x31, 0x2e, 0x48, 0x69, 0x73,
	0x74, 0x6f, 0x72, 0x79, 0x44, 0x4c, 0x51, 0x54, 0x61, 0x73, 0x6b, 0x4d, 0x65, 0x74, 0x61, 0x64,
	0x61, 0x74, 0x61, 0x52, 0x08, 0x6d, 0x65, 0x74, 0x61, 0x64, 0x61, 0x74, 0x61, 0x12, 0x44, 0x0a,
	0x07, 0x70, 0x61, 0x79, 0x6c, 0x6f, 0x61, 0x64, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x2a,
	0x2e, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2e, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72,
	0x2e, 0x61, 0x70, 0x69, 0x2e, 0x63, 0x6f, 0x6d, 0x6d, 0x6f, 0x6e, 0x2e, 0x76, 0x31, 0x2e, 0x48,
	0x69, 0x73, 0x74, 0x6f, 0x72, 0x79, 0x54, 0x61, 0x73, 0x6b, 0x52, 0x07, 0x70, 0x61, 0x79, 0x6c,
	0x6f, 0x61, 0x64, 0x22, 0x82, 0x01, 0x0a, 0x0d, 0x48, 0x69, 0x73, 0x74, 0x6f, 0x72, 0x79, 0x44,
	0x4c, 0x51, 0x4b, 0x65, 0x79, 0x12, 0x23, 0x0a, 0x0d, 0x74, 0x61, 0x73, 0x6b, 0x5f, 0x63, 0x61,
	0x74, 0x65, 0x67, 0x6f, 0x72, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28, 0x05, 0x52, 0x0c, 0x74, 0x61,
	0x73, 0x6b, 0x43, 0x61, 0x74, 0x65, 0x67, 0x6f, 0x72, 0x79, 0x12, 0x25, 0x0a, 0x0e, 0x73, 0x6f,
	0x75, 0x72, 0x63, 0x65, 0x5f, 0x63, 0x6c, 0x75, 0x73, 0x74, 0x65, 0x72, 0x18, 0x02, 0x20, 0x01,
	0x28, 0x09, 0x52, 0x0d, 0x73, 0x6f, 0x75, 0x72, 0x63, 0x65, 0x43, 0x6c, 0x75, 0x73, 0x74, 0x65,
	0x72, 0x12, 0x25, 0x0a, 0x0e, 0x74, 0x61, 0x72, 0x67, 0x65, 0x74, 0x5f, 0x63, 0x6c, 0x75, 0x73,
	0x74, 0x65, 0x72, 0x18, 0x03, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0d, 0x74, 0x61, 0x72, 0x67, 0x65,
	0x74, 0x43, 0x6c, 0x75, 0x73, 0x74, 0x65, 0x72, 0x42, 0x2f, 0x5a, 0x2d, 0x67, 0x6f, 0x2e, 0x74,
	0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2e, 0x69, 0x6f, 0x2f, 0x73, 0x65, 0x72, 0x76, 0x65,
	0x72, 0x2f, 0x61, 0x70, 0x69, 0x2f, 0x63, 0x6f, 0x6d, 0x6d, 0x6f, 0x6e, 0x2f, 0x76, 0x31, 0x3b,
	0x63, 0x6f, 0x6d, 0x6d, 0x6f, 0x6e, 0x73, 0x70, 0x62, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x33,
}

var (
	file_temporal_server_api_common_v1_dlq_proto_rawDescOnce sync.Once
	file_temporal_server_api_common_v1_dlq_proto_rawDescData = file_temporal_server_api_common_v1_dlq_proto_rawDesc
)

func file_temporal_server_api_common_v1_dlq_proto_rawDescGZIP() []byte {
	file_temporal_server_api_common_v1_dlq_proto_rawDescOnce.Do(func() {
		file_temporal_server_api_common_v1_dlq_proto_rawDescData = protoimpl.X.CompressGZIP(file_temporal_server_api_common_v1_dlq_proto_rawDescData)
	})
	return file_temporal_server_api_common_v1_dlq_proto_rawDescData
}

var file_temporal_server_api_common_v1_dlq_proto_msgTypes = make([]protoimpl.MessageInfo, 4)
var file_temporal_server_api_common_v1_dlq_proto_goTypes = []interface{}{
	(*HistoryTask)(nil),            // 0: temporal.server.api.common.v1.HistoryTask
	(*HistoryDLQTaskMetadata)(nil), // 1: temporal.server.api.common.v1.HistoryDLQTaskMetadata
	(*HistoryDLQTask)(nil),         // 2: temporal.server.api.common.v1.HistoryDLQTask
	(*HistoryDLQKey)(nil),          // 3: temporal.server.api.common.v1.HistoryDLQKey
	(*v1.DataBlob)(nil),            // 4: temporal.api.common.v1.DataBlob
}
var file_temporal_server_api_common_v1_dlq_proto_depIdxs = []int32{
	4, // 0: temporal.server.api.common.v1.HistoryTask.blob:type_name -> temporal.api.common.v1.DataBlob
	1, // 1: temporal.server.api.common.v1.HistoryDLQTask.metadata:type_name -> temporal.server.api.common.v1.HistoryDLQTaskMetadata
	0, // 2: temporal.server.api.common.v1.HistoryDLQTask.payload:type_name -> temporal.server.api.common.v1.HistoryTask
	3, // [3:3] is the sub-list for method output_type
	3, // [3:3] is the sub-list for method input_type
	3, // [3:3] is the sub-list for extension type_name
	3, // [3:3] is the sub-list for extension extendee
	0, // [0:3] is the sub-list for field type_name
}

func init() { file_temporal_server_api_common_v1_dlq_proto_init() }
func file_temporal_server_api_common_v1_dlq_proto_init() {
	if File_temporal_server_api_common_v1_dlq_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_temporal_server_api_common_v1_dlq_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*HistoryTask); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_temporal_server_api_common_v1_dlq_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*HistoryDLQTaskMetadata); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_temporal_server_api_common_v1_dlq_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*HistoryDLQTask); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_temporal_server_api_common_v1_dlq_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*HistoryDLQKey); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_temporal_server_api_common_v1_dlq_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   4,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_temporal_server_api_common_v1_dlq_proto_goTypes,
		DependencyIndexes: file_temporal_server_api_common_v1_dlq_proto_depIdxs,
		MessageInfos:      file_temporal_server_api_common_v1_dlq_proto_msgTypes,
	}.Build()
	File_temporal_server_api_common_v1_dlq_proto = out.File
	file_temporal_server_api_common_v1_dlq_proto_rawDesc = nil
	file_temporal_server_api_common_v1_dlq_proto_goTypes = nil
	file_temporal_server_api_common_v1_dlq_proto_depIdxs = nil
}
