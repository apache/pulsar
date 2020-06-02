//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//

package api

import (
	fmt "fmt"
	math "math"

	proto "github.com/golang/protobuf/proto"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion3 // please upgrade the proto package

type ServiceRequest_ServiceRequestType int32

const (
	ServiceRequest_UPDATE     ServiceRequest_ServiceRequestType = 0
	ServiceRequest_DELETE     ServiceRequest_ServiceRequestType = 1
	ServiceRequest_INITIALIZE ServiceRequest_ServiceRequestType = 2
)

var ServiceRequest_ServiceRequestType_name = map[int32]string{
	0: "UPDATE",
	1: "DELETE",
	2: "INITIALIZE",
}

var ServiceRequest_ServiceRequestType_value = map[string]int32{
	"UPDATE":     0,
	"DELETE":     1,
	"INITIALIZE": 2,
}

func (x ServiceRequest_ServiceRequestType) String() string {
	return proto.EnumName(ServiceRequest_ServiceRequestType_name, int32(x))
}

func (ServiceRequest_ServiceRequestType) EnumDescriptor() ([]byte, []int) {
	return fileDescriptor_ac56d74daff02b5a, []int{0, 0}
}

type ServiceRequest struct {
	ServiceRequestType   ServiceRequest_ServiceRequestType `protobuf:"varint,1,opt,name=serviceRequestType,proto3,enum=proto.ServiceRequest_ServiceRequestType" json:"serviceRequestType,omitempty"`
	RequestId            string                            `protobuf:"bytes,2,opt,name=requestId,proto3" json:"requestId,omitempty"`
	FunctionMetaData     *FunctionMetaData                 `protobuf:"bytes,3,opt,name=functionMetaData,proto3" json:"functionMetaData,omitempty"`
	WorkerId             string                            `protobuf:"bytes,4,opt,name=workerId,proto3" json:"workerId,omitempty"`
	XXX_NoUnkeyedLiteral struct{}                          `json:"-"`
	XXX_unrecognized     []byte                            `json:"-"`
	XXX_sizecache        int32                             `json:"-"`
}

func (m *ServiceRequest) Reset()         { *m = ServiceRequest{} }
func (m *ServiceRequest) String() string { return proto.CompactTextString(m) }
func (*ServiceRequest) ProtoMessage()    {}
func (*ServiceRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_ac56d74daff02b5a, []int{0}
}

func (m *ServiceRequest) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_ServiceRequest.Unmarshal(m, b)
}
func (m *ServiceRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_ServiceRequest.Marshal(b, m, deterministic)
}
func (m *ServiceRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_ServiceRequest.Merge(m, src)
}
func (m *ServiceRequest) XXX_Size() int {
	return xxx_messageInfo_ServiceRequest.Size(m)
}
func (m *ServiceRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_ServiceRequest.DiscardUnknown(m)
}

var xxx_messageInfo_ServiceRequest proto.InternalMessageInfo

func (m *ServiceRequest) GetServiceRequestType() ServiceRequest_ServiceRequestType {
	if m != nil {
		return m.ServiceRequestType
	}
	return ServiceRequest_UPDATE
}

func (m *ServiceRequest) GetRequestId() string {
	if m != nil {
		return m.RequestId
	}
	return ""
}

func (m *ServiceRequest) GetFunctionMetaData() *FunctionMetaData {
	if m != nil {
		return m.FunctionMetaData
	}
	return nil
}

func (m *ServiceRequest) GetWorkerId() string {
	if m != nil {
		return m.WorkerId
	}
	return ""
}

func init() {
	proto.RegisterEnum("proto.ServiceRequest_ServiceRequestType", ServiceRequest_ServiceRequestType_name, ServiceRequest_ServiceRequestType_value)
	proto.RegisterType((*ServiceRequest)(nil), "proto.ServiceRequest")
}

func init() { proto.RegisterFile("Request.proto", fileDescriptor_ac56d74daff02b5a) }

var fileDescriptor_ac56d74daff02b5a = []byte{
	// 247 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xe2, 0xe2, 0x0d, 0x4a, 0x2d, 0x2c,
	0x4d, 0x2d, 0x2e, 0xd1, 0x2b, 0x28, 0xca, 0x2f, 0xc9, 0x17, 0x62, 0x05, 0x53, 0x52, 0x7c, 0x6e,
	0xa5, 0x79, 0xc9, 0x25, 0x99, 0xf9, 0x79, 0x10, 0x61, 0xa5, 0xe5, 0x4c, 0x5c, 0x7c, 0xc1, 0xa9,
	0x45, 0x65, 0x99, 0xc9, 0xa9, 0x50, 0xf5, 0x42, 0x11, 0x5c, 0x42, 0xc5, 0x28, 0x22, 0x21, 0x95,
	0x05, 0xa9, 0x12, 0x8c, 0x0a, 0x8c, 0x1a, 0x7c, 0x46, 0x1a, 0x10, 0x6d, 0x7a, 0xa8, 0x5a, 0xd0,
	0xb8, 0x20, 0xf5, 0x41, 0x58, 0xcc, 0x10, 0x92, 0xe1, 0xe2, 0x2c, 0x82, 0x70, 0x3d, 0x53, 0x24,
	0x98, 0x14, 0x18, 0x35, 0x38, 0x83, 0x10, 0x02, 0x42, 0xce, 0x5c, 0x02, 0x69, 0x50, 0xc7, 0xf9,
	0xa6, 0x96, 0x24, 0xba, 0x24, 0x96, 0x24, 0x4a, 0x30, 0x2b, 0x30, 0x6a, 0x70, 0x1b, 0x89, 0x43,
	0x6d, 0x75, 0x43, 0x93, 0x0e, 0xc2, 0xd0, 0x20, 0x24, 0xc5, 0xc5, 0x51, 0x9e, 0x5f, 0x94, 0x9d,
	0x5a, 0xe4, 0x99, 0x22, 0xc1, 0x02, 0xb6, 0x01, 0xce, 0x57, 0xb2, 0xe1, 0x12, 0xc2, 0x74, 0xa8,
	0x10, 0x17, 0x17, 0x5b, 0x68, 0x80, 0x8b, 0x63, 0x88, 0xab, 0x00, 0x03, 0x88, 0xed, 0xe2, 0xea,
	0xe3, 0x1a, 0xe2, 0x2a, 0xc0, 0x28, 0xc4, 0xc7, 0xc5, 0xe5, 0xe9, 0xe7, 0x19, 0xe2, 0xe9, 0xe8,
	0xe3, 0x19, 0xe5, 0x2a, 0xc0, 0xe4, 0xa4, 0xc3, 0xa5, 0x98, 0x5f, 0x94, 0xae, 0x97, 0x58, 0x90,
	0x98, 0x9c, 0x91, 0xaa, 0x57, 0x50, 0x9a, 0x53, 0x9c, 0x58, 0xa4, 0x07, 0xb3, 0xbf, 0x18, 0xe2,
	0x42, 0x27, 0x76, 0xa8, 0xc9, 0x49, 0x6c, 0x60, 0xbe, 0x31, 0x20, 0x00, 0x00, 0xff, 0xff, 0xfa,
	0x6c, 0xdd, 0x44, 0x86, 0x01, 0x00, 0x00,
}
