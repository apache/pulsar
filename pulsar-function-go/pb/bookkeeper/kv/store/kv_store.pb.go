//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.31.0
// 	protoc        v3.15.8
// source: kv_store.proto

package store

import (
	rpc "bookkeeper/kv/rpc"
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type ValueType int32

const (
	ValueType_BYTES  ValueType = 0
	ValueType_NUMBER ValueType = 1 // this is to support increment
)

// Enum value maps for ValueType.
var (
	ValueType_name = map[int32]string{
		0: "BYTES",
		1: "NUMBER",
	}
	ValueType_value = map[string]int32{
		"BYTES":  0,
		"NUMBER": 1,
	}
)

func (x ValueType) Enum() *ValueType {
	p := new(ValueType)
	*p = x
	return p
}

func (x ValueType) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (ValueType) Descriptor() protoreflect.EnumDescriptor {
	return file_kv_store_proto_enumTypes[0].Descriptor()
}

func (ValueType) Type() protoreflect.EnumType {
	return &file_kv_store_proto_enumTypes[0]
}

func (x ValueType) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use ValueType.Descriptor instead.
func (ValueType) EnumDescriptor() ([]byte, []int) {
	return file_kv_store_proto_rawDescGZIP(), []int{0}
}

// KeyRecord holds mvcc metadata for a given key
type KeyMeta struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// create_revision is the revision of the last creation on the key
	CreateRevision int64 `protobuf:"varint,1,opt,name=create_revision,json=createRevision,proto3" json:"create_revision,omitempty"`
	// mod_revision is the revision of the last modification on the key
	ModRevision int64 `protobuf:"varint,2,opt,name=mod_revision,json=modRevision,proto3" json:"mod_revision,omitempty"`
	// version is the version of the most recent value
	Version int64 `protobuf:"varint,3,opt,name=version,proto3" json:"version,omitempty"`
	// value type
	ValueType ValueType `protobuf:"varint,4,opt,name=value_type,json=valueType,proto3,enum=bookkeeper.proto.kv.store.ValueType" json:"value_type,omitempty"`
	// time in milliseconds when the record expires (0 for none)
	ExpireTime int64 `protobuf:"varint,5,opt,name=expireTime,proto3" json:"expireTime,omitempty"`
}

func (x *KeyMeta) Reset() {
	*x = KeyMeta{}
	if protoimpl.UnsafeEnabled {
		mi := &file_kv_store_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *KeyMeta) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*KeyMeta) ProtoMessage() {}

func (x *KeyMeta) ProtoReflect() protoreflect.Message {
	mi := &file_kv_store_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use KeyMeta.ProtoReflect.Descriptor instead.
func (*KeyMeta) Descriptor() ([]byte, []int) {
	return file_kv_store_proto_rawDescGZIP(), []int{0}
}

func (x *KeyMeta) GetCreateRevision() int64 {
	if x != nil {
		return x.CreateRevision
	}
	return 0
}

func (x *KeyMeta) GetModRevision() int64 {
	if x != nil {
		return x.ModRevision
	}
	return 0
}

func (x *KeyMeta) GetVersion() int64 {
	if x != nil {
		return x.Version
	}
	return 0
}

func (x *KeyMeta) GetValueType() ValueType {
	if x != nil {
		return x.ValueType
	}
	return ValueType_BYTES
}

func (x *KeyMeta) GetExpireTime() int64 {
	if x != nil {
		return x.ExpireTime
	}
	return 0
}

type NopRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields
}

func (x *NopRequest) Reset() {
	*x = NopRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_kv_store_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *NopRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*NopRequest) ProtoMessage() {}

func (x *NopRequest) ProtoReflect() protoreflect.Message {
	mi := &file_kv_store_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use NopRequest.ProtoReflect.Descriptor instead.
func (*NopRequest) Descriptor() ([]byte, []int) {
	return file_kv_store_proto_rawDescGZIP(), []int{1}
}

type Command struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// Types that are assignable to Req:
	//
	//	*Command_NopReq
	//	*Command_PutReq
	//	*Command_DeleteReq
	//	*Command_TxnReq
	//	*Command_IncrReq
	Req isCommand_Req `protobuf_oneof:"req"`
}

func (x *Command) Reset() {
	*x = Command{}
	if protoimpl.UnsafeEnabled {
		mi := &file_kv_store_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Command) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Command) ProtoMessage() {}

func (x *Command) ProtoReflect() protoreflect.Message {
	mi := &file_kv_store_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Command.ProtoReflect.Descriptor instead.
func (*Command) Descriptor() ([]byte, []int) {
	return file_kv_store_proto_rawDescGZIP(), []int{2}
}

func (m *Command) GetReq() isCommand_Req {
	if m != nil {
		return m.Req
	}
	return nil
}

func (x *Command) GetNopReq() *NopRequest {
	if x, ok := x.GetReq().(*Command_NopReq); ok {
		return x.NopReq
	}
	return nil
}

func (x *Command) GetPutReq() *rpc.PutRequest {
	if x, ok := x.GetReq().(*Command_PutReq); ok {
		return x.PutReq
	}
	return nil
}

func (x *Command) GetDeleteReq() *rpc.DeleteRangeRequest {
	if x, ok := x.GetReq().(*Command_DeleteReq); ok {
		return x.DeleteReq
	}
	return nil
}

func (x *Command) GetTxnReq() *rpc.TxnRequest {
	if x, ok := x.GetReq().(*Command_TxnReq); ok {
		return x.TxnReq
	}
	return nil
}

func (x *Command) GetIncrReq() *rpc.IncrementRequest {
	if x, ok := x.GetReq().(*Command_IncrReq); ok {
		return x.IncrReq
	}
	return nil
}

type isCommand_Req interface {
	isCommand_Req()
}

type Command_NopReq struct {
	NopReq *NopRequest `protobuf:"bytes,1,opt,name=nop_req,json=nopReq,proto3,oneof"`
}

type Command_PutReq struct {
	PutReq *rpc.PutRequest `protobuf:"bytes,2,opt,name=put_req,json=putReq,proto3,oneof"`
}

type Command_DeleteReq struct {
	DeleteReq *rpc.DeleteRangeRequest `protobuf:"bytes,3,opt,name=delete_req,json=deleteReq,proto3,oneof"`
}

type Command_TxnReq struct {
	TxnReq *rpc.TxnRequest `protobuf:"bytes,4,opt,name=txn_req,json=txnReq,proto3,oneof"`
}

type Command_IncrReq struct {
	IncrReq *rpc.IncrementRequest `protobuf:"bytes,5,opt,name=incr_req,json=incrReq,proto3,oneof"`
}

func (*Command_NopReq) isCommand_Req() {}

func (*Command_PutReq) isCommand_Req() {}

func (*Command_DeleteReq) isCommand_Req() {}

func (*Command_TxnReq) isCommand_Req() {}

func (*Command_IncrReq) isCommand_Req() {}

type FileInfo struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Name     string `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	Checksum string `protobuf:"bytes,2,opt,name=checksum,proto3" json:"checksum,omitempty"`
}

func (x *FileInfo) Reset() {
	*x = FileInfo{}
	if protoimpl.UnsafeEnabled {
		mi := &file_kv_store_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *FileInfo) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*FileInfo) ProtoMessage() {}

func (x *FileInfo) ProtoReflect() protoreflect.Message {
	mi := &file_kv_store_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use FileInfo.ProtoReflect.Descriptor instead.
func (*FileInfo) Descriptor() ([]byte, []int) {
	return file_kv_store_proto_rawDescGZIP(), []int{3}
}

func (x *FileInfo) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *FileInfo) GetChecksum() string {
	if x != nil {
		return x.Checksum
	}
	return ""
}

type CheckpointMetadata struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Files     []string    `protobuf:"bytes,1,rep,name=files,proto3" json:"files,omitempty"`
	Txid      []byte      `protobuf:"bytes,2,opt,name=txid,proto3" json:"txid,omitempty"`
	CreatedAt uint64      `protobuf:"varint,3,opt,name=created_at,json=createdAt,proto3" json:"created_at,omitempty"`
	FileInfos []*FileInfo `protobuf:"bytes,4,rep,name=fileInfos,proto3" json:"fileInfos,omitempty"`
}

func (x *CheckpointMetadata) Reset() {
	*x = CheckpointMetadata{}
	if protoimpl.UnsafeEnabled {
		mi := &file_kv_store_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *CheckpointMetadata) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*CheckpointMetadata) ProtoMessage() {}

func (x *CheckpointMetadata) ProtoReflect() protoreflect.Message {
	mi := &file_kv_store_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use CheckpointMetadata.ProtoReflect.Descriptor instead.
func (*CheckpointMetadata) Descriptor() ([]byte, []int) {
	return file_kv_store_proto_rawDescGZIP(), []int{4}
}

func (x *CheckpointMetadata) GetFiles() []string {
	if x != nil {
		return x.Files
	}
	return nil
}

func (x *CheckpointMetadata) GetTxid() []byte {
	if x != nil {
		return x.Txid
	}
	return nil
}

func (x *CheckpointMetadata) GetCreatedAt() uint64 {
	if x != nil {
		return x.CreatedAt
	}
	return 0
}

func (x *CheckpointMetadata) GetFileInfos() []*FileInfo {
	if x != nil {
		return x.FileInfos
	}
	return nil
}

var File_kv_store_proto protoreflect.FileDescriptor

var file_kv_store_proto_rawDesc = []byte{
	0x0a, 0x0e, 0x6b, 0x76, 0x5f, 0x73, 0x74, 0x6f, 0x72, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x12, 0x19, 0x62, 0x6f, 0x6f, 0x6b, 0x6b, 0x65, 0x65, 0x70, 0x65, 0x72, 0x2e, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x2e, 0x6b, 0x76, 0x2e, 0x73, 0x74, 0x6f, 0x72, 0x65, 0x1a, 0x0c, 0x6b, 0x76, 0x5f,
	0x72, 0x70, 0x63, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0xd4, 0x01, 0x0a, 0x07, 0x4b, 0x65,
	0x79, 0x4d, 0x65, 0x74, 0x61, 0x12, 0x27, 0x0a, 0x0f, 0x63, 0x72, 0x65, 0x61, 0x74, 0x65, 0x5f,
	0x72, 0x65, 0x76, 0x69, 0x73, 0x69, 0x6f, 0x6e, 0x18, 0x01, 0x20, 0x01, 0x28, 0x03, 0x52, 0x0e,
	0x63, 0x72, 0x65, 0x61, 0x74, 0x65, 0x52, 0x65, 0x76, 0x69, 0x73, 0x69, 0x6f, 0x6e, 0x12, 0x21,
	0x0a, 0x0c, 0x6d, 0x6f, 0x64, 0x5f, 0x72, 0x65, 0x76, 0x69, 0x73, 0x69, 0x6f, 0x6e, 0x18, 0x02,
	0x20, 0x01, 0x28, 0x03, 0x52, 0x0b, 0x6d, 0x6f, 0x64, 0x52, 0x65, 0x76, 0x69, 0x73, 0x69, 0x6f,
	0x6e, 0x12, 0x18, 0x0a, 0x07, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x18, 0x03, 0x20, 0x01,
	0x28, 0x03, 0x52, 0x07, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x12, 0x43, 0x0a, 0x0a, 0x76,
	0x61, 0x6c, 0x75, 0x65, 0x5f, 0x74, 0x79, 0x70, 0x65, 0x18, 0x04, 0x20, 0x01, 0x28, 0x0e, 0x32,
	0x24, 0x2e, 0x62, 0x6f, 0x6f, 0x6b, 0x6b, 0x65, 0x65, 0x70, 0x65, 0x72, 0x2e, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x2e, 0x6b, 0x76, 0x2e, 0x73, 0x74, 0x6f, 0x72, 0x65, 0x2e, 0x56, 0x61, 0x6c, 0x75,
	0x65, 0x54, 0x79, 0x70, 0x65, 0x52, 0x09, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x54, 0x79, 0x70, 0x65,
	0x12, 0x1e, 0x0a, 0x0a, 0x65, 0x78, 0x70, 0x69, 0x72, 0x65, 0x54, 0x69, 0x6d, 0x65, 0x18, 0x05,
	0x20, 0x01, 0x28, 0x03, 0x52, 0x0a, 0x65, 0x78, 0x70, 0x69, 0x72, 0x65, 0x54, 0x69, 0x6d, 0x65,
	0x22, 0x0c, 0x0a, 0x0a, 0x4e, 0x6f, 0x70, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x22, 0xe8,
	0x02, 0x0a, 0x07, 0x43, 0x6f, 0x6d, 0x6d, 0x61, 0x6e, 0x64, 0x12, 0x40, 0x0a, 0x07, 0x6e, 0x6f,
	0x70, 0x5f, 0x72, 0x65, 0x71, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x25, 0x2e, 0x62, 0x6f,
	0x6f, 0x6b, 0x6b, 0x65, 0x65, 0x70, 0x65, 0x72, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2e, 0x6b,
	0x76, 0x2e, 0x73, 0x74, 0x6f, 0x72, 0x65, 0x2e, 0x4e, 0x6f, 0x70, 0x52, 0x65, 0x71, 0x75, 0x65,
	0x73, 0x74, 0x48, 0x00, 0x52, 0x06, 0x6e, 0x6f, 0x70, 0x52, 0x65, 0x71, 0x12, 0x3e, 0x0a, 0x07,
	0x70, 0x75, 0x74, 0x5f, 0x72, 0x65, 0x71, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x23, 0x2e,
	0x62, 0x6f, 0x6f, 0x6b, 0x6b, 0x65, 0x65, 0x70, 0x65, 0x72, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x2e, 0x6b, 0x76, 0x2e, 0x72, 0x70, 0x63, 0x2e, 0x50, 0x75, 0x74, 0x52, 0x65, 0x71, 0x75, 0x65,
	0x73, 0x74, 0x48, 0x00, 0x52, 0x06, 0x70, 0x75, 0x74, 0x52, 0x65, 0x71, 0x12, 0x4c, 0x0a, 0x0a,
	0x64, 0x65, 0x6c, 0x65, 0x74, 0x65, 0x5f, 0x72, 0x65, 0x71, 0x18, 0x03, 0x20, 0x01, 0x28, 0x0b,
	0x32, 0x2b, 0x2e, 0x62, 0x6f, 0x6f, 0x6b, 0x6b, 0x65, 0x65, 0x70, 0x65, 0x72, 0x2e, 0x70, 0x72,
	0x6f, 0x74, 0x6f, 0x2e, 0x6b, 0x76, 0x2e, 0x72, 0x70, 0x63, 0x2e, 0x44, 0x65, 0x6c, 0x65, 0x74,
	0x65, 0x52, 0x61, 0x6e, 0x67, 0x65, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x48, 0x00, 0x52,
	0x09, 0x64, 0x65, 0x6c, 0x65, 0x74, 0x65, 0x52, 0x65, 0x71, 0x12, 0x3e, 0x0a, 0x07, 0x74, 0x78,
	0x6e, 0x5f, 0x72, 0x65, 0x71, 0x18, 0x04, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x23, 0x2e, 0x62, 0x6f,
	0x6f, 0x6b, 0x6b, 0x65, 0x65, 0x70, 0x65, 0x72, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2e, 0x6b,
	0x76, 0x2e, 0x72, 0x70, 0x63, 0x2e, 0x54, 0x78, 0x6e, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74,
	0x48, 0x00, 0x52, 0x06, 0x74, 0x78, 0x6e, 0x52, 0x65, 0x71, 0x12, 0x46, 0x0a, 0x08, 0x69, 0x6e,
	0x63, 0x72, 0x5f, 0x72, 0x65, 0x71, 0x18, 0x05, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x29, 0x2e, 0x62,
	0x6f, 0x6f, 0x6b, 0x6b, 0x65, 0x65, 0x70, 0x65, 0x72, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2e,
	0x6b, 0x76, 0x2e, 0x72, 0x70, 0x63, 0x2e, 0x49, 0x6e, 0x63, 0x72, 0x65, 0x6d, 0x65, 0x6e, 0x74,
	0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x48, 0x00, 0x52, 0x07, 0x69, 0x6e, 0x63, 0x72, 0x52,
	0x65, 0x71, 0x42, 0x05, 0x0a, 0x03, 0x72, 0x65, 0x71, 0x22, 0x3a, 0x0a, 0x08, 0x46, 0x69, 0x6c,
	0x65, 0x49, 0x6e, 0x66, 0x6f, 0x12, 0x12, 0x0a, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x01, 0x20,
	0x01, 0x28, 0x09, 0x52, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x12, 0x1a, 0x0a, 0x08, 0x63, 0x68, 0x65,
	0x63, 0x6b, 0x73, 0x75, 0x6d, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x08, 0x63, 0x68, 0x65,
	0x63, 0x6b, 0x73, 0x75, 0x6d, 0x22, 0xa0, 0x01, 0x0a, 0x12, 0x43, 0x68, 0x65, 0x63, 0x6b, 0x70,
	0x6f, 0x69, 0x6e, 0x74, 0x4d, 0x65, 0x74, 0x61, 0x64, 0x61, 0x74, 0x61, 0x12, 0x14, 0x0a, 0x05,
	0x66, 0x69, 0x6c, 0x65, 0x73, 0x18, 0x01, 0x20, 0x03, 0x28, 0x09, 0x52, 0x05, 0x66, 0x69, 0x6c,
	0x65, 0x73, 0x12, 0x12, 0x0a, 0x04, 0x74, 0x78, 0x69, 0x64, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0c,
	0x52, 0x04, 0x74, 0x78, 0x69, 0x64, 0x12, 0x1d, 0x0a, 0x0a, 0x63, 0x72, 0x65, 0x61, 0x74, 0x65,
	0x64, 0x5f, 0x61, 0x74, 0x18, 0x03, 0x20, 0x01, 0x28, 0x04, 0x52, 0x09, 0x63, 0x72, 0x65, 0x61,
	0x74, 0x65, 0x64, 0x41, 0x74, 0x12, 0x41, 0x0a, 0x09, 0x66, 0x69, 0x6c, 0x65, 0x49, 0x6e, 0x66,
	0x6f, 0x73, 0x18, 0x04, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x23, 0x2e, 0x62, 0x6f, 0x6f, 0x6b, 0x6b,
	0x65, 0x65, 0x70, 0x65, 0x72, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2e, 0x6b, 0x76, 0x2e, 0x73,
	0x74, 0x6f, 0x72, 0x65, 0x2e, 0x46, 0x69, 0x6c, 0x65, 0x49, 0x6e, 0x66, 0x6f, 0x52, 0x09, 0x66,
	0x69, 0x6c, 0x65, 0x49, 0x6e, 0x66, 0x6f, 0x73, 0x2a, 0x22, 0x0a, 0x09, 0x56, 0x61, 0x6c, 0x75,
	0x65, 0x54, 0x79, 0x70, 0x65, 0x12, 0x09, 0x0a, 0x05, 0x42, 0x59, 0x54, 0x45, 0x53, 0x10, 0x00,
	0x12, 0x0a, 0x0a, 0x06, 0x4e, 0x55, 0x4d, 0x42, 0x45, 0x52, 0x10, 0x01, 0x42, 0x17, 0x50, 0x01,
	0x5a, 0x13, 0x62, 0x6f, 0x6f, 0x6b, 0x6b, 0x65, 0x65, 0x70, 0x65, 0x72, 0x2f, 0x6b, 0x76, 0x2f,
	0x73, 0x74, 0x6f, 0x72, 0x65, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_kv_store_proto_rawDescOnce sync.Once
	file_kv_store_proto_rawDescData = file_kv_store_proto_rawDesc
)

func file_kv_store_proto_rawDescGZIP() []byte {
	file_kv_store_proto_rawDescOnce.Do(func() {
		file_kv_store_proto_rawDescData = protoimpl.X.CompressGZIP(file_kv_store_proto_rawDescData)
	})
	return file_kv_store_proto_rawDescData
}

var file_kv_store_proto_enumTypes = make([]protoimpl.EnumInfo, 1)
var file_kv_store_proto_msgTypes = make([]protoimpl.MessageInfo, 5)
var file_kv_store_proto_goTypes = []interface{}{
	(ValueType)(0),                 // 0: bookkeeper.proto.kv.store.ValueType
	(*KeyMeta)(nil),                // 1: bookkeeper.proto.kv.store.KeyMeta
	(*NopRequest)(nil),             // 2: bookkeeper.proto.kv.store.NopRequest
	(*Command)(nil),                // 3: bookkeeper.proto.kv.store.Command
	(*FileInfo)(nil),               // 4: bookkeeper.proto.kv.store.FileInfo
	(*CheckpointMetadata)(nil),     // 5: bookkeeper.proto.kv.store.CheckpointMetadata
	(*rpc.PutRequest)(nil),         // 6: bookkeeper.proto.kv.rpc.PutRequest
	(*rpc.DeleteRangeRequest)(nil), // 7: bookkeeper.proto.kv.rpc.DeleteRangeRequest
	(*rpc.TxnRequest)(nil),         // 8: bookkeeper.proto.kv.rpc.TxnRequest
	(*rpc.IncrementRequest)(nil),   // 9: bookkeeper.proto.kv.rpc.IncrementRequest
}
var file_kv_store_proto_depIdxs = []int32{
	0, // 0: bookkeeper.proto.kv.store.KeyMeta.value_type:type_name -> bookkeeper.proto.kv.store.ValueType
	2, // 1: bookkeeper.proto.kv.store.Command.nop_req:type_name -> bookkeeper.proto.kv.store.NopRequest
	6, // 2: bookkeeper.proto.kv.store.Command.put_req:type_name -> bookkeeper.proto.kv.rpc.PutRequest
	7, // 3: bookkeeper.proto.kv.store.Command.delete_req:type_name -> bookkeeper.proto.kv.rpc.DeleteRangeRequest
	8, // 4: bookkeeper.proto.kv.store.Command.txn_req:type_name -> bookkeeper.proto.kv.rpc.TxnRequest
	9, // 5: bookkeeper.proto.kv.store.Command.incr_req:type_name -> bookkeeper.proto.kv.rpc.IncrementRequest
	4, // 6: bookkeeper.proto.kv.store.CheckpointMetadata.fileInfos:type_name -> bookkeeper.proto.kv.store.FileInfo
	7, // [7:7] is the sub-list for method output_type
	7, // [7:7] is the sub-list for method input_type
	7, // [7:7] is the sub-list for extension type_name
	7, // [7:7] is the sub-list for extension extendee
	0, // [0:7] is the sub-list for field type_name
}

func init() { file_kv_store_proto_init() }
func file_kv_store_proto_init() {
	if File_kv_store_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_kv_store_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*KeyMeta); i {
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
		file_kv_store_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*NopRequest); i {
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
		file_kv_store_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Command); i {
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
		file_kv_store_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*FileInfo); i {
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
		file_kv_store_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*CheckpointMetadata); i {
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
	file_kv_store_proto_msgTypes[2].OneofWrappers = []interface{}{
		(*Command_NopReq)(nil),
		(*Command_PutReq)(nil),
		(*Command_DeleteReq)(nil),
		(*Command_TxnReq)(nil),
		(*Command_IncrReq)(nil),
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_kv_store_proto_rawDesc,
			NumEnums:      1,
			NumMessages:   5,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_kv_store_proto_goTypes,
		DependencyIndexes: file_kv_store_proto_depIdxs,
		EnumInfos:         file_kv_store_proto_enumTypes,
		MessageInfos:      file_kv_store_proto_msgTypes,
	}.Build()
	File_kv_store_proto = out.File
	file_kv_store_proto_rawDesc = nil
	file_kv_store_proto_goTypes = nil
	file_kv_store_proto_depIdxs = nil
}
