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
	context "context"
	fmt "fmt"
	math "math"

	proto "github.com/golang/protobuf/proto"
	empty "github.com/golang/protobuf/ptypes/empty"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
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

type FunctionStatus struct {
	Running          bool   `protobuf:"varint,1,opt,name=running,proto3" json:"running,omitempty"`
	FailureException string `protobuf:"bytes,2,opt,name=failureException,proto3" json:"failureException,omitempty"`
	NumRestarts      int64  `protobuf:"varint,3,opt,name=numRestarts,proto3" json:"numRestarts,omitempty"`
	// int64 numProcessed = 4;
	NumReceived              int64                                  `protobuf:"varint,17,opt,name=numReceived,proto3" json:"numReceived,omitempty"`
	NumSuccessfullyProcessed int64                                  `protobuf:"varint,5,opt,name=numSuccessfullyProcessed,proto3" json:"numSuccessfullyProcessed,omitempty"`
	NumUserExceptions        int64                                  `protobuf:"varint,6,opt,name=numUserExceptions,proto3" json:"numUserExceptions,omitempty"`
	LatestUserExceptions     []*FunctionStatus_ExceptionInformation `protobuf:"bytes,7,rep,name=latestUserExceptions,proto3" json:"latestUserExceptions,omitempty"`
	NumSystemExceptions      int64                                  `protobuf:"varint,8,opt,name=numSystemExceptions,proto3" json:"numSystemExceptions,omitempty"`
	LatestSystemExceptions   []*FunctionStatus_ExceptionInformation `protobuf:"bytes,9,rep,name=latestSystemExceptions,proto3" json:"latestSystemExceptions,omitempty"`
	NumSourceExceptions      int64                                  `protobuf:"varint,18,opt,name=numSourceExceptions,proto3" json:"numSourceExceptions,omitempty"`
	LatestSourceExceptions   []*FunctionStatus_ExceptionInformation `protobuf:"bytes,19,rep,name=latestSourceExceptions,proto3" json:"latestSourceExceptions,omitempty"`
	NumSinkExceptions        int64                                  `protobuf:"varint,20,opt,name=numSinkExceptions,proto3" json:"numSinkExceptions,omitempty"`
	LatestSinkExceptions     []*FunctionStatus_ExceptionInformation `protobuf:"bytes,21,rep,name=latestSinkExceptions,proto3" json:"latestSinkExceptions,omitempty"`
	// map from topic name to number of deserialization exceptions
	//    map<string, int64> deserializationExceptions = 10;
	// number of serialization exceptions on the output
	//    int64 serializationExceptions = 11;
	// average latency
	AverageLatency float64 `protobuf:"fixed64,12,opt,name=averageLatency,proto3" json:"averageLatency,omitempty"`
	// When was the last time the function was invoked.
	// expressed in ms since epoch
	LastInvocationTime int64  `protobuf:"varint,13,opt,name=lastInvocationTime,proto3" json:"lastInvocationTime,omitempty"`
	InstanceId         string `protobuf:"bytes,14,opt,name=instanceId,proto3" json:"instanceId,omitempty"`
	//    MetricsData metrics = 15 [deprecated=true];
	// owner of function-instance
	WorkerId             string   `protobuf:"bytes,16,opt,name=workerId,proto3" json:"workerId,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *FunctionStatus) Reset()         { *m = FunctionStatus{} }
func (m *FunctionStatus) String() string { return proto.CompactTextString(m) }
func (*FunctionStatus) ProtoMessage()    {}
func (*FunctionStatus) Descriptor() ([]byte, []int) {
	return fileDescriptor_93ee26d3627c79da, []int{0}
}

func (m *FunctionStatus) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_FunctionStatus.Unmarshal(m, b)
}
func (m *FunctionStatus) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_FunctionStatus.Marshal(b, m, deterministic)
}
func (m *FunctionStatus) XXX_Merge(src proto.Message) {
	xxx_messageInfo_FunctionStatus.Merge(m, src)
}
func (m *FunctionStatus) XXX_Size() int {
	return xxx_messageInfo_FunctionStatus.Size(m)
}
func (m *FunctionStatus) XXX_DiscardUnknown() {
	xxx_messageInfo_FunctionStatus.DiscardUnknown(m)
}

var xxx_messageInfo_FunctionStatus proto.InternalMessageInfo

func (m *FunctionStatus) GetRunning() bool {
	if m != nil {
		return m.Running
	}
	return false
}

func (m *FunctionStatus) GetFailureException() string {
	if m != nil {
		return m.FailureException
	}
	return ""
}

func (m *FunctionStatus) GetNumRestarts() int64 {
	if m != nil {
		return m.NumRestarts
	}
	return 0
}

func (m *FunctionStatus) GetNumReceived() int64 {
	if m != nil {
		return m.NumReceived
	}
	return 0
}

func (m *FunctionStatus) GetNumSuccessfullyProcessed() int64 {
	if m != nil {
		return m.NumSuccessfullyProcessed
	}
	return 0
}

func (m *FunctionStatus) GetNumUserExceptions() int64 {
	if m != nil {
		return m.NumUserExceptions
	}
	return 0
}

func (m *FunctionStatus) GetLatestUserExceptions() []*FunctionStatus_ExceptionInformation {
	if m != nil {
		return m.LatestUserExceptions
	}
	return nil
}

func (m *FunctionStatus) GetNumSystemExceptions() int64 {
	if m != nil {
		return m.NumSystemExceptions
	}
	return 0
}

func (m *FunctionStatus) GetLatestSystemExceptions() []*FunctionStatus_ExceptionInformation {
	if m != nil {
		return m.LatestSystemExceptions
	}
	return nil
}

func (m *FunctionStatus) GetNumSourceExceptions() int64 {
	if m != nil {
		return m.NumSourceExceptions
	}
	return 0
}

func (m *FunctionStatus) GetLatestSourceExceptions() []*FunctionStatus_ExceptionInformation {
	if m != nil {
		return m.LatestSourceExceptions
	}
	return nil
}

func (m *FunctionStatus) GetNumSinkExceptions() int64 {
	if m != nil {
		return m.NumSinkExceptions
	}
	return 0
}

func (m *FunctionStatus) GetLatestSinkExceptions() []*FunctionStatus_ExceptionInformation {
	if m != nil {
		return m.LatestSinkExceptions
	}
	return nil
}

func (m *FunctionStatus) GetAverageLatency() float64 {
	if m != nil {
		return m.AverageLatency
	}
	return 0
}

func (m *FunctionStatus) GetLastInvocationTime() int64 {
	if m != nil {
		return m.LastInvocationTime
	}
	return 0
}

func (m *FunctionStatus) GetInstanceId() string {
	if m != nil {
		return m.InstanceId
	}
	return ""
}

func (m *FunctionStatus) GetWorkerId() string {
	if m != nil {
		return m.WorkerId
	}
	return ""
}

type FunctionStatus_ExceptionInformation struct {
	ExceptionString      string   `protobuf:"bytes,1,opt,name=exceptionString,proto3" json:"exceptionString,omitempty"`
	MsSinceEpoch         int64    `protobuf:"varint,2,opt,name=msSinceEpoch,proto3" json:"msSinceEpoch,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *FunctionStatus_ExceptionInformation) Reset()         { *m = FunctionStatus_ExceptionInformation{} }
func (m *FunctionStatus_ExceptionInformation) String() string { return proto.CompactTextString(m) }
func (*FunctionStatus_ExceptionInformation) ProtoMessage()    {}
func (*FunctionStatus_ExceptionInformation) Descriptor() ([]byte, []int) {
	return fileDescriptor_93ee26d3627c79da, []int{0, 0}
}

func (m *FunctionStatus_ExceptionInformation) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_FunctionStatus_ExceptionInformation.Unmarshal(m, b)
}
func (m *FunctionStatus_ExceptionInformation) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_FunctionStatus_ExceptionInformation.Marshal(b, m, deterministic)
}
func (m *FunctionStatus_ExceptionInformation) XXX_Merge(src proto.Message) {
	xxx_messageInfo_FunctionStatus_ExceptionInformation.Merge(m, src)
}
func (m *FunctionStatus_ExceptionInformation) XXX_Size() int {
	return xxx_messageInfo_FunctionStatus_ExceptionInformation.Size(m)
}
func (m *FunctionStatus_ExceptionInformation) XXX_DiscardUnknown() {
	xxx_messageInfo_FunctionStatus_ExceptionInformation.DiscardUnknown(m)
}

var xxx_messageInfo_FunctionStatus_ExceptionInformation proto.InternalMessageInfo

func (m *FunctionStatus_ExceptionInformation) GetExceptionString() string {
	if m != nil {
		return m.ExceptionString
	}
	return ""
}

func (m *FunctionStatus_ExceptionInformation) GetMsSinceEpoch() int64 {
	if m != nil {
		return m.MsSinceEpoch
	}
	return 0
}

// Deprecated
type FunctionStatusList struct {
	Error                string            `protobuf:"bytes,2,opt,name=error,proto3" json:"error,omitempty"`
	FunctionStatusList   []*FunctionStatus `protobuf:"bytes,1,rep,name=functionStatusList,proto3" json:"functionStatusList,omitempty"`
	XXX_NoUnkeyedLiteral struct{}          `json:"-"`
	XXX_unrecognized     []byte            `json:"-"`
	XXX_sizecache        int32             `json:"-"`
}

func (m *FunctionStatusList) Reset()         { *m = FunctionStatusList{} }
func (m *FunctionStatusList) String() string { return proto.CompactTextString(m) }
func (*FunctionStatusList) ProtoMessage()    {}
func (*FunctionStatusList) Descriptor() ([]byte, []int) {
	return fileDescriptor_93ee26d3627c79da, []int{1}
}

func (m *FunctionStatusList) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_FunctionStatusList.Unmarshal(m, b)
}
func (m *FunctionStatusList) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_FunctionStatusList.Marshal(b, m, deterministic)
}
func (m *FunctionStatusList) XXX_Merge(src proto.Message) {
	xxx_messageInfo_FunctionStatusList.Merge(m, src)
}
func (m *FunctionStatusList) XXX_Size() int {
	return xxx_messageInfo_FunctionStatusList.Size(m)
}
func (m *FunctionStatusList) XXX_DiscardUnknown() {
	xxx_messageInfo_FunctionStatusList.DiscardUnknown(m)
}

var xxx_messageInfo_FunctionStatusList proto.InternalMessageInfo

func (m *FunctionStatusList) GetError() string {
	if m != nil {
		return m.Error
	}
	return ""
}

func (m *FunctionStatusList) GetFunctionStatusList() []*FunctionStatus {
	if m != nil {
		return m.FunctionStatusList
	}
	return nil
}

type MetricsData struct {
	// Total number of records function received from source
	ReceivedTotal      int64 `protobuf:"varint,2,opt,name=receivedTotal,proto3" json:"receivedTotal,omitempty"`
	ReceivedTotal_1Min int64 `protobuf:"varint,10,opt,name=receivedTotal_1min,json=receivedTotal1min,proto3" json:"receivedTotal_1min,omitempty"`
	// Total number of records successfully processed by user function
	ProcessedSuccessfullyTotal      int64 `protobuf:"varint,4,opt,name=processedSuccessfullyTotal,proto3" json:"processedSuccessfullyTotal,omitempty"`
	ProcessedSuccessfullyTotal_1Min int64 `protobuf:"varint,12,opt,name=processedSuccessfullyTotal_1min,json=processedSuccessfullyTotal1min,proto3" json:"processedSuccessfullyTotal_1min,omitempty"`
	// Total number of system exceptions thrown
	SystemExceptionsTotal      int64 `protobuf:"varint,5,opt,name=systemExceptionsTotal,proto3" json:"systemExceptionsTotal,omitempty"`
	SystemExceptionsTotal_1Min int64 `protobuf:"varint,13,opt,name=systemExceptionsTotal_1min,json=systemExceptionsTotal1min,proto3" json:"systemExceptionsTotal_1min,omitempty"`
	// Total number of user exceptions thrown
	UserExceptionsTotal      int64 `protobuf:"varint,6,opt,name=userExceptionsTotal,proto3" json:"userExceptionsTotal,omitempty"`
	UserExceptionsTotal_1Min int64 `protobuf:"varint,14,opt,name=userExceptionsTotal_1min,json=userExceptionsTotal1min,proto3" json:"userExceptionsTotal_1min,omitempty"`
	// Average process latency for function
	AvgProcessLatency      float64 `protobuf:"fixed64,7,opt,name=avgProcessLatency,proto3" json:"avgProcessLatency,omitempty"`
	AvgProcessLatency_1Min float64 `protobuf:"fixed64,15,opt,name=avgProcessLatency_1min,json=avgProcessLatency1min,proto3" json:"avgProcessLatency_1min,omitempty"`
	// Timestamp of when the function was last invoked
	LastInvocation int64 `protobuf:"varint,8,opt,name=lastInvocation,proto3" json:"lastInvocation,omitempty"`
	// User defined metrics
	UserMetrics          map[string]float64 `protobuf:"bytes,9,rep,name=userMetrics,proto3" json:"userMetrics,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"fixed64,2,opt,name=value,proto3"`
	XXX_NoUnkeyedLiteral struct{}           `json:"-"`
	XXX_unrecognized     []byte             `json:"-"`
	XXX_sizecache        int32              `json:"-"`
}

func (m *MetricsData) Reset()         { *m = MetricsData{} }
func (m *MetricsData) String() string { return proto.CompactTextString(m) }
func (*MetricsData) ProtoMessage()    {}
func (*MetricsData) Descriptor() ([]byte, []int) {
	return fileDescriptor_93ee26d3627c79da, []int{2}
}

func (m *MetricsData) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_MetricsData.Unmarshal(m, b)
}
func (m *MetricsData) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_MetricsData.Marshal(b, m, deterministic)
}
func (m *MetricsData) XXX_Merge(src proto.Message) {
	xxx_messageInfo_MetricsData.Merge(m, src)
}
func (m *MetricsData) XXX_Size() int {
	return xxx_messageInfo_MetricsData.Size(m)
}
func (m *MetricsData) XXX_DiscardUnknown() {
	xxx_messageInfo_MetricsData.DiscardUnknown(m)
}

var xxx_messageInfo_MetricsData proto.InternalMessageInfo

func (m *MetricsData) GetReceivedTotal() int64 {
	if m != nil {
		return m.ReceivedTotal
	}
	return 0
}

func (m *MetricsData) GetReceivedTotal_1Min() int64 {
	if m != nil {
		return m.ReceivedTotal_1Min
	}
	return 0
}

func (m *MetricsData) GetProcessedSuccessfullyTotal() int64 {
	if m != nil {
		return m.ProcessedSuccessfullyTotal
	}
	return 0
}

func (m *MetricsData) GetProcessedSuccessfullyTotal_1Min() int64 {
	if m != nil {
		return m.ProcessedSuccessfullyTotal_1Min
	}
	return 0
}

func (m *MetricsData) GetSystemExceptionsTotal() int64 {
	if m != nil {
		return m.SystemExceptionsTotal
	}
	return 0
}

func (m *MetricsData) GetSystemExceptionsTotal_1Min() int64 {
	if m != nil {
		return m.SystemExceptionsTotal_1Min
	}
	return 0
}

func (m *MetricsData) GetUserExceptionsTotal() int64 {
	if m != nil {
		return m.UserExceptionsTotal
	}
	return 0
}

func (m *MetricsData) GetUserExceptionsTotal_1Min() int64 {
	if m != nil {
		return m.UserExceptionsTotal_1Min
	}
	return 0
}

func (m *MetricsData) GetAvgProcessLatency() float64 {
	if m != nil {
		return m.AvgProcessLatency
	}
	return 0
}

func (m *MetricsData) GetAvgProcessLatency_1Min() float64 {
	if m != nil {
		return m.AvgProcessLatency_1Min
	}
	return 0
}

func (m *MetricsData) GetLastInvocation() int64 {
	if m != nil {
		return m.LastInvocation
	}
	return 0
}

func (m *MetricsData) GetUserMetrics() map[string]float64 {
	if m != nil {
		return m.UserMetrics
	}
	return nil
}

type HealthCheckResult struct {
	Success              bool     `protobuf:"varint,1,opt,name=success,proto3" json:"success,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *HealthCheckResult) Reset()         { *m = HealthCheckResult{} }
func (m *HealthCheckResult) String() string { return proto.CompactTextString(m) }
func (*HealthCheckResult) ProtoMessage()    {}
func (*HealthCheckResult) Descriptor() ([]byte, []int) {
	return fileDescriptor_93ee26d3627c79da, []int{3}
}

func (m *HealthCheckResult) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_HealthCheckResult.Unmarshal(m, b)
}
func (m *HealthCheckResult) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_HealthCheckResult.Marshal(b, m, deterministic)
}
func (m *HealthCheckResult) XXX_Merge(src proto.Message) {
	xxx_messageInfo_HealthCheckResult.Merge(m, src)
}
func (m *HealthCheckResult) XXX_Size() int {
	return xxx_messageInfo_HealthCheckResult.Size(m)
}
func (m *HealthCheckResult) XXX_DiscardUnknown() {
	xxx_messageInfo_HealthCheckResult.DiscardUnknown(m)
}

var xxx_messageInfo_HealthCheckResult proto.InternalMessageInfo

func (m *HealthCheckResult) GetSuccess() bool {
	if m != nil {
		return m.Success
	}
	return false
}

type Metrics struct {
	Metrics              []*Metrics_InstanceMetrics `protobuf:"bytes,1,rep,name=metrics,proto3" json:"metrics,omitempty"`
	XXX_NoUnkeyedLiteral struct{}                   `json:"-"`
	XXX_unrecognized     []byte                     `json:"-"`
	XXX_sizecache        int32                      `json:"-"`
}

func (m *Metrics) Reset()         { *m = Metrics{} }
func (m *Metrics) String() string { return proto.CompactTextString(m) }
func (*Metrics) ProtoMessage()    {}
func (*Metrics) Descriptor() ([]byte, []int) {
	return fileDescriptor_93ee26d3627c79da, []int{4}
}

func (m *Metrics) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_Metrics.Unmarshal(m, b)
}
func (m *Metrics) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_Metrics.Marshal(b, m, deterministic)
}
func (m *Metrics) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Metrics.Merge(m, src)
}
func (m *Metrics) XXX_Size() int {
	return xxx_messageInfo_Metrics.Size(m)
}
func (m *Metrics) XXX_DiscardUnknown() {
	xxx_messageInfo_Metrics.DiscardUnknown(m)
}

var xxx_messageInfo_Metrics proto.InternalMessageInfo

func (m *Metrics) GetMetrics() []*Metrics_InstanceMetrics {
	if m != nil {
		return m.Metrics
	}
	return nil
}

type Metrics_InstanceMetrics struct {
	Name                 string       `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	InstanceId           int32        `protobuf:"varint,2,opt,name=instanceId,proto3" json:"instanceId,omitempty"`
	MetricsData          *MetricsData `protobuf:"bytes,3,opt,name=metricsData,proto3" json:"metricsData,omitempty"`
	XXX_NoUnkeyedLiteral struct{}     `json:"-"`
	XXX_unrecognized     []byte       `json:"-"`
	XXX_sizecache        int32        `json:"-"`
}

func (m *Metrics_InstanceMetrics) Reset()         { *m = Metrics_InstanceMetrics{} }
func (m *Metrics_InstanceMetrics) String() string { return proto.CompactTextString(m) }
func (*Metrics_InstanceMetrics) ProtoMessage()    {}
func (*Metrics_InstanceMetrics) Descriptor() ([]byte, []int) {
	return fileDescriptor_93ee26d3627c79da, []int{4, 0}
}

func (m *Metrics_InstanceMetrics) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_Metrics_InstanceMetrics.Unmarshal(m, b)
}
func (m *Metrics_InstanceMetrics) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_Metrics_InstanceMetrics.Marshal(b, m, deterministic)
}
func (m *Metrics_InstanceMetrics) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Metrics_InstanceMetrics.Merge(m, src)
}
func (m *Metrics_InstanceMetrics) XXX_Size() int {
	return xxx_messageInfo_Metrics_InstanceMetrics.Size(m)
}
func (m *Metrics_InstanceMetrics) XXX_DiscardUnknown() {
	xxx_messageInfo_Metrics_InstanceMetrics.DiscardUnknown(m)
}

var xxx_messageInfo_Metrics_InstanceMetrics proto.InternalMessageInfo

func (m *Metrics_InstanceMetrics) GetName() string {
	if m != nil {
		return m.Name
	}
	return ""
}

func (m *Metrics_InstanceMetrics) GetInstanceId() int32 {
	if m != nil {
		return m.InstanceId
	}
	return 0
}

func (m *Metrics_InstanceMetrics) GetMetricsData() *MetricsData {
	if m != nil {
		return m.MetricsData
	}
	return nil
}

func init() {
	proto.RegisterType((*FunctionStatus)(nil), "proto.FunctionStatus")
	proto.RegisterType((*FunctionStatus_ExceptionInformation)(nil), "proto.FunctionStatus.ExceptionInformation")
	proto.RegisterType((*FunctionStatusList)(nil), "proto.FunctionStatusList")
	proto.RegisterType((*MetricsData)(nil), "proto.MetricsData")
	proto.RegisterMapType((map[string]float64)(nil), "proto.MetricsData.UserMetricsEntry")
	proto.RegisterType((*HealthCheckResult)(nil), "proto.HealthCheckResult")
	proto.RegisterType((*Metrics)(nil), "proto.Metrics")
	proto.RegisterType((*Metrics_InstanceMetrics)(nil), "proto.Metrics.InstanceMetrics")
}

func init() { proto.RegisterFile("InstanceCommunication.proto", fileDescriptor_93ee26d3627c79da) }

var fileDescriptor_93ee26d3627c79da = []byte{
	// 917 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x9c, 0x56, 0xef, 0x6e, 0x1b, 0x45,
	0x10, 0xcf, 0xd5, 0x75, 0x9d, 0x8c, 0x53, 0x27, 0x9e, 0xc4, 0xe1, 0x70, 0xa5, 0x60, 0x0e, 0x54,
	0x59, 0x55, 0x7b, 0x2d, 0xa1, 0x48, 0x25, 0x12, 0x15, 0x4d, 0x6a, 0x82, 0xa5, 0x22, 0xa1, 0x73,
	0xfb, 0x15, 0xb4, 0x39, 0xaf, 0x9d, 0x53, 0xee, 0x76, 0xcd, 0xee, 0x9e, 0xc1, 0xe2, 0x45, 0x78,
	0x1c, 0x1e, 0x84, 0x2f, 0xbc, 0x09, 0xba, 0xdd, 0x3b, 0xfb, 0xfe, 0x19, 0x41, 0x3e, 0xf9, 0x76,
	0x7e, 0xf3, 0x9b, 0xf9, 0x79, 0x77, 0x66, 0x76, 0xe1, 0xd1, 0x98, 0x49, 0x45, 0x98, 0x4f, 0x2f,
	0x79, 0x14, 0xc5, 0x2c, 0xf0, 0x89, 0x0a, 0x38, 0x73, 0x17, 0x82, 0x2b, 0x8e, 0x4d, 0xfd, 0xd3,
	0x7f, 0x34, 0xe7, 0x7c, 0x1e, 0xd2, 0xe7, 0x7a, 0x75, 0x1d, 0xcf, 0x9e, 0xd3, 0x68, 0xa1, 0x56,
	0xc6, 0xc7, 0xf9, 0x63, 0x17, 0x3a, 0xdf, 0xc5, 0xcc, 0x4f, 0x68, 0x13, 0x45, 0x54, 0x2c, 0xd1,
	0x86, 0x96, 0x88, 0x19, 0x0b, 0xd8, 0xdc, 0xb6, 0x06, 0xd6, 0x70, 0xd7, 0xcb, 0x96, 0xf8, 0x04,
	0x0e, 0x67, 0x24, 0x08, 0x63, 0x41, 0x47, 0xbf, 0xf9, 0x74, 0x91, 0x70, 0xec, 0x7b, 0x03, 0x6b,
	0xb8, 0xe7, 0x55, 0xec, 0x38, 0x80, 0x36, 0x8b, 0x23, 0x8f, 0x4a, 0x45, 0x84, 0x92, 0x76, 0x63,
	0x60, 0x0d, 0x1b, 0x5e, 0xde, 0xb4, 0xf6, 0xf0, 0x69, 0xb0, 0xa4, 0x53, 0xbb, 0x9b, 0xf3, 0x30,
	0x26, 0x3c, 0x07, 0x9b, 0xc5, 0xd1, 0x24, 0xf6, 0x7d, 0x2a, 0xe5, 0x2c, 0x0e, 0xc3, 0xd5, 0x8f,
	0x82, 0x27, 0xdf, 0x74, 0x6a, 0x37, 0xb5, 0xfb, 0x56, 0x1c, 0x9f, 0x42, 0x97, 0xc5, 0xd1, 0x07,
	0x49, 0xc5, 0x5a, 0x93, 0xb4, 0x1f, 0x68, 0x52, 0x15, 0xc0, 0x9f, 0xe0, 0x38, 0x24, 0x8a, 0x4a,
	0x55, 0x22, 0xb4, 0x06, 0x8d, 0x61, 0xfb, 0xec, 0x89, 0xd9, 0x2c, 0xb7, 0xb8, 0x51, 0xee, 0xda,
	0x6f, 0xcc, 0x66, 0x5c, 0x44, 0x7a, 0xeb, 0xbd, 0xda, 0x38, 0xf8, 0x02, 0x8e, 0x12, 0xa5, 0x2b,
	0xa9, 0x68, 0x94, 0x0b, 0xbf, 0xab, 0xf5, 0xd4, 0x41, 0x78, 0x0d, 0x27, 0x26, 0x52, 0x85, 0xb4,
	0xf7, 0xbf, 0x35, 0x6d, 0x89, 0x94, 0xa9, 0xe2, 0xb1, 0xf0, 0x69, 0x2e, 0x01, 0x6e, 0x54, 0x95,
	0xa0, 0x9c, 0xaa, 0x32, 0xe9, 0xe8, 0xce, 0xaa, 0xca, 0x39, 0xcc, 0xc9, 0x4d, 0x02, 0x76, 0x9b,
	0x0b, 0x7f, 0xbc, 0x3e, 0xb9, 0x22, 0xb0, 0x39, 0xb9, 0x12, 0xa1, 0x77, 0xd7, 0x93, 0x2b, 0xc5,
	0x7f, 0x0c, 0x1d, 0xb2, 0xa4, 0x82, 0xcc, 0xe9, 0x3b, 0xa2, 0x28, 0xf3, 0x57, 0xf6, 0xfe, 0xc0,
	0x1a, 0x5a, 0x5e, 0xc9, 0x8a, 0x2e, 0x60, 0x48, 0xa4, 0x1a, 0xb3, 0x25, 0x37, 0x4d, 0xf8, 0x3e,
	0x88, 0xa8, 0xfd, 0x50, 0xcb, 0xae, 0x41, 0xf0, 0x14, 0x20, 0x48, 0x7b, 0x77, 0x3c, 0xb5, 0x3b,
	0xba, 0x8b, 0x72, 0x16, 0xec, 0xc3, 0xee, 0xaf, 0x5c, 0xdc, 0x52, 0x31, 0x9e, 0xda, 0x87, 0x1a,
	0x5d, 0xaf, 0xfb, 0x53, 0x38, 0xae, 0xfb, 0x07, 0x38, 0x84, 0x03, 0x9a, 0xd9, 0x27, 0x4a, 0x64,
	0x1d, 0xbc, 0xe7, 0x95, 0xcd, 0xe8, 0xc0, 0x7e, 0x24, 0x27, 0x01, 0xf3, 0xe9, 0x68, 0xc1, 0xfd,
	0x1b, 0xdd, 0xc5, 0x0d, 0xaf, 0x60, 0x73, 0x7e, 0x01, 0x2c, 0x6e, 0xdb, 0xbb, 0x40, 0x2a, 0x3c,
	0x86, 0x26, 0x15, 0x82, 0x8b, 0xb4, 0xf1, 0xcd, 0x02, 0x47, 0x80, 0xb3, 0x8a, 0xaf, 0x6d, 0xe9,
	0x33, 0xe8, 0xd5, 0x9e, 0x81, 0x57, 0x43, 0x70, 0xfe, 0x6e, 0x42, 0xfb, 0x07, 0xaa, 0x44, 0xe0,
	0xcb, 0xb7, 0x44, 0x11, 0xfc, 0x1c, 0x1e, 0x8a, 0x74, 0x18, 0xbc, 0xe7, 0x8a, 0x84, 0xa9, 0xce,
	0xa2, 0x11, 0x9f, 0x01, 0x16, 0x0c, 0x3f, 0x7f, 0x11, 0x05, 0xcc, 0x06, 0x53, 0x31, 0x05, 0x24,
	0x01, 0xf0, 0x35, 0xf4, 0x17, 0xd9, 0x98, 0xc8, 0xcf, 0x0e, 0x93, 0xe1, 0xbe, 0xa6, 0xfd, 0x8b,
	0x07, 0x5e, 0xc1, 0x27, 0xdb, 0x51, 0x93, 0x7b, 0x5f, 0x07, 0x39, 0xdd, 0xee, 0xa6, 0x85, 0xbc,
	0x84, 0x9e, 0x2c, 0xb5, 0xa4, 0xd1, 0x60, 0x66, 0x5b, 0x3d, 0x88, 0xdf, 0x40, 0xbf, 0x16, 0x30,
	0x99, 0x4d, 0xc1, 0x7d, 0x5c, 0xeb, 0xa1, 0x93, 0xbe, 0x80, 0xa3, 0xb8, 0x30, 0x9b, 0x4c, 0x4a,
	0x33, 0x19, 0xeb, 0x20, 0xfc, 0x1a, 0xec, 0x1a, 0xb3, 0x49, 0xd7, 0xd1, 0xb4, 0x8f, 0x6a, 0x70,
	0x9d, 0xec, 0x29, 0x74, 0xc9, 0x72, 0x9e, 0x0e, 0xe5, 0xac, 0x7f, 0x5a, 0xba, 0x7f, 0xaa, 0x00,
	0x7e, 0x05, 0x27, 0x15, 0xa3, 0x49, 0x73, 0xa0, 0x29, 0xbd, 0x0a, 0xaa, 0x93, 0x3c, 0x86, 0x4e,
	0xb1, 0xbf, 0xd2, 0xb1, 0x5a, 0xb2, 0xe2, 0x08, 0xda, 0x89, 0xce, 0xb4, 0xbe, 0xd2, 0x31, 0xfa,
	0x59, 0x5a, 0x9c, 0xb9, 0xaa, 0x73, 0x3f, 0x6c, 0xbc, 0x46, 0x4c, 0x89, 0x95, 0x97, 0xe7, 0xf5,
	0x5f, 0xc3, 0x61, 0xd9, 0x01, 0x0f, 0xa1, 0x71, 0x4b, 0x57, 0x69, 0xb3, 0x25, 0x9f, 0x49, 0x9b,
	0x2c, 0x49, 0x18, 0x53, 0x5d, 0xb1, 0x96, 0x67, 0x16, 0xe7, 0xf7, 0x5e, 0x59, 0xce, 0x33, 0xe8,
	0x7e, 0x4f, 0x49, 0xa8, 0x6e, 0x2e, 0x6f, 0xa8, 0x7f, 0xeb, 0x51, 0x19, 0x87, 0x2a, 0xb9, 0x73,
	0xa5, 0xa9, 0x91, 0xec, 0xce, 0x4d, 0x97, 0xce, 0x9f, 0x16, 0xb4, 0xd2, 0x5c, 0xf8, 0x0a, 0x5a,
	0x51, 0xaa, 0xde, 0xb4, 0xd6, 0x69, 0x51, 0xbd, 0x9b, 0xbd, 0x06, 0xd2, 0xb5, 0x97, 0xb9, 0xf7,
	0x7f, 0x87, 0x83, 0x12, 0x86, 0x08, 0xf7, 0x19, 0x89, 0x68, 0x2a, 0x5a, 0x7f, 0x97, 0x86, 0x52,
	0x22, 0xbd, 0x59, 0x18, 0x4a, 0x2f, 0xa1, 0x1d, 0x6d, 0x36, 0x4a, 0x5f, 0xea, 0xed, 0x33, 0xac,
	0x6e, 0xa1, 0x97, 0x77, 0x3b, 0xfb, 0xeb, 0xde, 0x26, 0xfb, 0x25, 0x67, 0x4a, 0xf0, 0x10, 0xdf,
	0x42, 0xf7, 0x8a, 0xaa, 0xd2, 0xcb, 0xe3, 0xc4, 0x35, 0x4f, 0x15, 0x37, 0x7b, 0xaa, 0xb8, 0xa3,
	0xe4, 0xa9, 0xd2, 0xaf, 0x9f, 0x20, 0xce, 0x0e, 0x5e, 0x00, 0x5e, 0x51, 0xf5, 0x86, 0x4d, 0x3d,
	0x2a, 0xa9, 0xca, 0xfe, 0xd9, 0xb6, 0x30, 0x35, 0x42, 0x9d, 0x1d, 0xfc, 0x16, 0xf6, 0xff, 0x13,
	0x7b, 0x8b, 0xdd, 0xd9, 0xc1, 0x73, 0x80, 0xab, 0xbb, 0x66, 0x7f, 0x03, 0xed, 0x5c, 0x35, 0x6c,
	0x25, 0xdb, 0x29, 0xb9, 0x52, 0x39, 0xce, 0xce, 0xc5, 0x39, 0x7c, 0xca, 0xc5, 0xdc, 0x25, 0x0b,
	0xe2, 0xdf, 0x50, 0x77, 0x11, 0x87, 0x92, 0x08, 0x37, 0x1b, 0xae, 0xd2, 0x10, 0x2f, 0x7a, 0xb5,
	0x0f, 0xc5, 0xeb, 0x07, 0x1a, 0xfd, 0xf2, 0x9f, 0x00, 0x00, 0x00, 0xff, 0xff, 0x52, 0xef, 0x2d,
	0x97, 0x48, 0x0a, 0x00, 0x00,
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConn

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion4

// InstanceControlClient is the client API for InstanceControl service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://godoc.org/google.golang.org/grpc#ClientConn.NewStream.
type InstanceControlClient interface {
	GetFunctionStatus(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*FunctionStatus, error)
	GetAndResetMetrics(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*MetricsData, error)
	ResetMetrics(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*empty.Empty, error)
	GetMetrics(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*MetricsData, error)
	HealthCheck(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*HealthCheckResult, error)
}

type instanceControlClient struct {
	cc *grpc.ClientConn
}

func NewInstanceControlClient(cc *grpc.ClientConn) InstanceControlClient {
	return &instanceControlClient{cc}
}

func (c *instanceControlClient) GetFunctionStatus(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*FunctionStatus, error) {
	out := new(FunctionStatus)
	err := c.cc.Invoke(ctx, "/proto.InstanceControl/GetFunctionStatus", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *instanceControlClient) GetAndResetMetrics(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*MetricsData, error) {
	out := new(MetricsData)
	err := c.cc.Invoke(ctx, "/proto.InstanceControl/GetAndResetMetrics", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *instanceControlClient) ResetMetrics(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*empty.Empty, error) {
	out := new(empty.Empty)
	err := c.cc.Invoke(ctx, "/proto.InstanceControl/ResetMetrics", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *instanceControlClient) GetMetrics(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*MetricsData, error) {
	out := new(MetricsData)
	err := c.cc.Invoke(ctx, "/proto.InstanceControl/GetMetrics", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *instanceControlClient) HealthCheck(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*HealthCheckResult, error) {
	out := new(HealthCheckResult)
	err := c.cc.Invoke(ctx, "/proto.InstanceControl/HealthCheck", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// InstanceControlServer is the server API for InstanceControl service.
type InstanceControlServer interface {
	GetFunctionStatus(context.Context, *empty.Empty) (*FunctionStatus, error)
	GetAndResetMetrics(context.Context, *empty.Empty) (*MetricsData, error)
	ResetMetrics(context.Context, *empty.Empty) (*empty.Empty, error)
	GetMetrics(context.Context, *empty.Empty) (*MetricsData, error)
	HealthCheck(context.Context, *empty.Empty) (*HealthCheckResult, error)
}

// UnimplementedInstanceControlServer can be embedded to have forward compatible implementations.
type UnimplementedInstanceControlServer struct {
}

func (*UnimplementedInstanceControlServer) GetFunctionStatus(ctx context.Context, req *empty.Empty) (*FunctionStatus, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetFunctionStatus not implemented")
}
func (*UnimplementedInstanceControlServer) GetAndResetMetrics(ctx context.Context, req *empty.Empty) (*MetricsData, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetAndResetMetrics not implemented")
}
func (*UnimplementedInstanceControlServer) ResetMetrics(ctx context.Context, req *empty.Empty) (*empty.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ResetMetrics not implemented")
}
func (*UnimplementedInstanceControlServer) GetMetrics(ctx context.Context, req *empty.Empty) (*MetricsData, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetMetrics not implemented")
}
func (*UnimplementedInstanceControlServer) HealthCheck(ctx context.Context, req *empty.Empty) (*HealthCheckResult, error) {
	return nil, status.Errorf(codes.Unimplemented, "method HealthCheck not implemented")
}

func RegisterInstanceControlServer(s *grpc.Server, srv InstanceControlServer) {
	s.RegisterService(&_InstanceControl_serviceDesc, srv)
}

func _InstanceControl_GetFunctionStatus_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(empty.Empty)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(InstanceControlServer).GetFunctionStatus(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/proto.InstanceControl/GetFunctionStatus",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(InstanceControlServer).GetFunctionStatus(ctx, req.(*empty.Empty))
	}
	return interceptor(ctx, in, info, handler)
}

func _InstanceControl_GetAndResetMetrics_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(empty.Empty)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(InstanceControlServer).GetAndResetMetrics(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/proto.InstanceControl/GetAndResetMetrics",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(InstanceControlServer).GetAndResetMetrics(ctx, req.(*empty.Empty))
	}
	return interceptor(ctx, in, info, handler)
}

func _InstanceControl_ResetMetrics_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(empty.Empty)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(InstanceControlServer).ResetMetrics(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/proto.InstanceControl/ResetMetrics",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(InstanceControlServer).ResetMetrics(ctx, req.(*empty.Empty))
	}
	return interceptor(ctx, in, info, handler)
}

func _InstanceControl_GetMetrics_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(empty.Empty)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(InstanceControlServer).GetMetrics(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/proto.InstanceControl/GetMetrics",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(InstanceControlServer).GetMetrics(ctx, req.(*empty.Empty))
	}
	return interceptor(ctx, in, info, handler)
}

func _InstanceControl_HealthCheck_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(empty.Empty)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(InstanceControlServer).HealthCheck(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/proto.InstanceControl/HealthCheck",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(InstanceControlServer).HealthCheck(ctx, req.(*empty.Empty))
	}
	return interceptor(ctx, in, info, handler)
}

var _InstanceControl_serviceDesc = grpc.ServiceDesc{
	ServiceName: "proto.InstanceControl",
	HandlerType: (*InstanceControlServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "GetFunctionStatus",
			Handler:    _InstanceControl_GetFunctionStatus_Handler,
		},
		{
			MethodName: "GetAndResetMetrics",
			Handler:    _InstanceControl_GetAndResetMetrics_Handler,
		},
		{
			MethodName: "ResetMetrics",
			Handler:    _InstanceControl_ResetMetrics_Handler,
		},
		{
			MethodName: "GetMetrics",
			Handler:    _InstanceControl_GetMetrics_Handler,
		},
		{
			MethodName: "HealthCheck",
			Handler:    _InstanceControl_HealthCheck_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "InstanceCommunication.proto",
}
