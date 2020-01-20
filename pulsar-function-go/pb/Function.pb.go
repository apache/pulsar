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

type ProcessingGuarantees int32

const (
	ProcessingGuarantees_ATLEAST_ONCE     ProcessingGuarantees = 0
	ProcessingGuarantees_ATMOST_ONCE      ProcessingGuarantees = 1
	ProcessingGuarantees_EFFECTIVELY_ONCE ProcessingGuarantees = 2
)

var ProcessingGuarantees_name = map[int32]string{
	0: "ATLEAST_ONCE",
	1: "ATMOST_ONCE",
	2: "EFFECTIVELY_ONCE",
}

var ProcessingGuarantees_value = map[string]int32{
	"ATLEAST_ONCE":     0,
	"ATMOST_ONCE":      1,
	"EFFECTIVELY_ONCE": 2,
}

func (x ProcessingGuarantees) String() string {
	return proto.EnumName(ProcessingGuarantees_name, int32(x))
}

func (ProcessingGuarantees) EnumDescriptor() ([]byte, []int) {
	return fileDescriptor_225cf355fcfb169c, []int{0}
}

type SubscriptionType int32

const (
	SubscriptionType_SHARED   SubscriptionType = 0
	SubscriptionType_FAILOVER SubscriptionType = 1
)

var SubscriptionType_name = map[int32]string{
	0: "SHARED",
	1: "FAILOVER",
}

var SubscriptionType_value = map[string]int32{
	"SHARED":   0,
	"FAILOVER": 1,
}

func (x SubscriptionType) String() string {
	return proto.EnumName(SubscriptionType_name, int32(x))
}

func (SubscriptionType) EnumDescriptor() ([]byte, []int) {
	return fileDescriptor_225cf355fcfb169c, []int{1}
}

type SubscriptionPosition int32

const (
	SubscriptionPosition_LATEST   SubscriptionPosition = 0
	SubscriptionPosition_EARLIEST SubscriptionPosition = 1
)

var SubscriptionPosition_name = map[int32]string{
	0: "LATEST",
	1: "EARLIEST",
}

var SubscriptionPosition_value = map[string]int32{
	"LATEST":   0,
	"EARLIEST": 1,
}

func (x SubscriptionPosition) String() string {
	return proto.EnumName(SubscriptionPosition_name, int32(x))
}

func (SubscriptionPosition) EnumDescriptor() ([]byte, []int) {
	return fileDescriptor_225cf355fcfb169c, []int{2}
}

type FunctionState int32

const (
	FunctionState_RUNNING FunctionState = 0
	FunctionState_STOPPED FunctionState = 1
)

var FunctionState_name = map[int32]string{
	0: "RUNNING",
	1: "STOPPED",
}

var FunctionState_value = map[string]int32{
	"RUNNING": 0,
	"STOPPED": 1,
}

func (x FunctionState) String() string {
	return proto.EnumName(FunctionState_name, int32(x))
}

func (FunctionState) EnumDescriptor() ([]byte, []int) {
	return fileDescriptor_225cf355fcfb169c, []int{3}
}

type FunctionDetails_Runtime int32

const (
	FunctionDetails_JAVA   FunctionDetails_Runtime = 0
	FunctionDetails_PYTHON FunctionDetails_Runtime = 1
	FunctionDetails_GO     FunctionDetails_Runtime = 3
)

var FunctionDetails_Runtime_name = map[int32]string{
	0: "JAVA",
	1: "PYTHON",
	3: "GO",
}

var FunctionDetails_Runtime_value = map[string]int32{
	"JAVA":   0,
	"PYTHON": 1,
	"GO":     3,
}

func (x FunctionDetails_Runtime) String() string {
	return proto.EnumName(FunctionDetails_Runtime_name, int32(x))
}

func (FunctionDetails_Runtime) EnumDescriptor() ([]byte, []int) {
	return fileDescriptor_225cf355fcfb169c, []int{2, 0}
}

type FunctionDetails_ComponentType int32

const (
	FunctionDetails_UNKNOWN  FunctionDetails_ComponentType = 0
	FunctionDetails_FUNCTION FunctionDetails_ComponentType = 1
	FunctionDetails_SOURCE   FunctionDetails_ComponentType = 2
	FunctionDetails_SINK     FunctionDetails_ComponentType = 3
)

var FunctionDetails_ComponentType_name = map[int32]string{
	0: "UNKNOWN",
	1: "FUNCTION",
	2: "SOURCE",
	3: "SINK",
}

var FunctionDetails_ComponentType_value = map[string]int32{
	"UNKNOWN":  0,
	"FUNCTION": 1,
	"SOURCE":   2,
	"SINK":     3,
}

func (x FunctionDetails_ComponentType) String() string {
	return proto.EnumName(FunctionDetails_ComponentType_name, int32(x))
}

func (FunctionDetails_ComponentType) EnumDescriptor() ([]byte, []int) {
	return fileDescriptor_225cf355fcfb169c, []int{2, 1}
}

type Resources struct {
	Cpu                  float64  `protobuf:"fixed64,1,opt,name=cpu,proto3" json:"cpu,omitempty"`
	Ram                  int64    `protobuf:"varint,2,opt,name=ram,proto3" json:"ram,omitempty"`
	Disk                 int64    `protobuf:"varint,3,opt,name=disk,proto3" json:"disk,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *Resources) Reset()         { *m = Resources{} }
func (m *Resources) String() string { return proto.CompactTextString(m) }
func (*Resources) ProtoMessage()    {}
func (*Resources) Descriptor() ([]byte, []int) {
	return fileDescriptor_225cf355fcfb169c, []int{0}
}

func (m *Resources) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_Resources.Unmarshal(m, b)
}
func (m *Resources) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_Resources.Marshal(b, m, deterministic)
}
func (m *Resources) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Resources.Merge(m, src)
}
func (m *Resources) XXX_Size() int {
	return xxx_messageInfo_Resources.Size(m)
}
func (m *Resources) XXX_DiscardUnknown() {
	xxx_messageInfo_Resources.DiscardUnknown(m)
}

var xxx_messageInfo_Resources proto.InternalMessageInfo

func (m *Resources) GetCpu() float64 {
	if m != nil {
		return m.Cpu
	}
	return 0
}

func (m *Resources) GetRam() int64 {
	if m != nil {
		return m.Ram
	}
	return 0
}

func (m *Resources) GetDisk() int64 {
	if m != nil {
		return m.Disk
	}
	return 0
}

type RetryDetails struct {
	MaxMessageRetries    int32    `protobuf:"varint,1,opt,name=maxMessageRetries,proto3" json:"maxMessageRetries,omitempty"`
	DeadLetterTopic      string   `protobuf:"bytes,2,opt,name=deadLetterTopic,proto3" json:"deadLetterTopic,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *RetryDetails) Reset()         { *m = RetryDetails{} }
func (m *RetryDetails) String() string { return proto.CompactTextString(m) }
func (*RetryDetails) ProtoMessage()    {}
func (*RetryDetails) Descriptor() ([]byte, []int) {
	return fileDescriptor_225cf355fcfb169c, []int{1}
}

func (m *RetryDetails) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_RetryDetails.Unmarshal(m, b)
}
func (m *RetryDetails) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_RetryDetails.Marshal(b, m, deterministic)
}
func (m *RetryDetails) XXX_Merge(src proto.Message) {
	xxx_messageInfo_RetryDetails.Merge(m, src)
}
func (m *RetryDetails) XXX_Size() int {
	return xxx_messageInfo_RetryDetails.Size(m)
}
func (m *RetryDetails) XXX_DiscardUnknown() {
	xxx_messageInfo_RetryDetails.DiscardUnknown(m)
}

var xxx_messageInfo_RetryDetails proto.InternalMessageInfo

func (m *RetryDetails) GetMaxMessageRetries() int32 {
	if m != nil {
		return m.MaxMessageRetries
	}
	return 0
}

func (m *RetryDetails) GetDeadLetterTopic() string {
	if m != nil {
		return m.DeadLetterTopic
	}
	return ""
}

type FunctionDetails struct {
	Tenant               string                        `protobuf:"bytes,1,opt,name=tenant,proto3" json:"tenant,omitempty"`
	Namespace            string                        `protobuf:"bytes,2,opt,name=namespace,proto3" json:"namespace,omitempty"`
	Name                 string                        `protobuf:"bytes,3,opt,name=name,proto3" json:"name,omitempty"`
	ClassName            string                        `protobuf:"bytes,4,opt,name=className,proto3" json:"className,omitempty"`
	LogTopic             string                        `protobuf:"bytes,5,opt,name=logTopic,proto3" json:"logTopic,omitempty"`
	ProcessingGuarantees ProcessingGuarantees          `protobuf:"varint,6,opt,name=processingGuarantees,proto3,enum=proto.ProcessingGuarantees" json:"processingGuarantees,omitempty"`
	UserConfig           string                        `protobuf:"bytes,7,opt,name=userConfig,proto3" json:"userConfig,omitempty"`
	SecretsMap           string                        `protobuf:"bytes,16,opt,name=secretsMap,proto3" json:"secretsMap,omitempty"`
	Runtime              FunctionDetails_Runtime       `protobuf:"varint,8,opt,name=runtime,proto3,enum=proto.FunctionDetails_Runtime" json:"runtime,omitempty"`
	AutoAck              bool                          `protobuf:"varint,9,opt,name=autoAck,proto3" json:"autoAck,omitempty"`
	Parallelism          int32                         `protobuf:"varint,10,opt,name=parallelism,proto3" json:"parallelism,omitempty"`
	Source               *SourceSpec                   `protobuf:"bytes,11,opt,name=source,proto3" json:"source,omitempty"`
	Sink                 *SinkSpec                     `protobuf:"bytes,12,opt,name=sink,proto3" json:"sink,omitempty"`
	Resources            *Resources                    `protobuf:"bytes,13,opt,name=resources,proto3" json:"resources,omitempty"`
	PackageUrl           string                        `protobuf:"bytes,14,opt,name=packageUrl,proto3" json:"packageUrl,omitempty"`
	RetryDetails         *RetryDetails                 `protobuf:"bytes,15,opt,name=retryDetails,proto3" json:"retryDetails,omitempty"`
	RuntimeFlags         string                        `protobuf:"bytes,17,opt,name=runtimeFlags,proto3" json:"runtimeFlags,omitempty"`
	ComponentType        FunctionDetails_ComponentType `protobuf:"varint,18,opt,name=componentType,proto3,enum=proto.FunctionDetails_ComponentType" json:"componentType,omitempty"`
	CustomRuntimeOptions string                        `protobuf:"bytes,19,opt,name=customRuntimeOptions,proto3" json:"customRuntimeOptions,omitempty"`
	XXX_NoUnkeyedLiteral struct{}                      `json:"-"`
	XXX_unrecognized     []byte                        `json:"-"`
	XXX_sizecache        int32                         `json:"-"`
}

func (m *FunctionDetails) Reset()         { *m = FunctionDetails{} }
func (m *FunctionDetails) String() string { return proto.CompactTextString(m) }
func (*FunctionDetails) ProtoMessage()    {}
func (*FunctionDetails) Descriptor() ([]byte, []int) {
	return fileDescriptor_225cf355fcfb169c, []int{2}
}

func (m *FunctionDetails) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_FunctionDetails.Unmarshal(m, b)
}
func (m *FunctionDetails) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_FunctionDetails.Marshal(b, m, deterministic)
}
func (m *FunctionDetails) XXX_Merge(src proto.Message) {
	xxx_messageInfo_FunctionDetails.Merge(m, src)
}
func (m *FunctionDetails) XXX_Size() int {
	return xxx_messageInfo_FunctionDetails.Size(m)
}
func (m *FunctionDetails) XXX_DiscardUnknown() {
	xxx_messageInfo_FunctionDetails.DiscardUnknown(m)
}

var xxx_messageInfo_FunctionDetails proto.InternalMessageInfo

func (m *FunctionDetails) GetTenant() string {
	if m != nil {
		return m.Tenant
	}
	return ""
}

func (m *FunctionDetails) GetNamespace() string {
	if m != nil {
		return m.Namespace
	}
	return ""
}

func (m *FunctionDetails) GetName() string {
	if m != nil {
		return m.Name
	}
	return ""
}

func (m *FunctionDetails) GetClassName() string {
	if m != nil {
		return m.ClassName
	}
	return ""
}

func (m *FunctionDetails) GetLogTopic() string {
	if m != nil {
		return m.LogTopic
	}
	return ""
}

func (m *FunctionDetails) GetProcessingGuarantees() ProcessingGuarantees {
	if m != nil {
		return m.ProcessingGuarantees
	}
	return ProcessingGuarantees_ATLEAST_ONCE
}

func (m *FunctionDetails) GetUserConfig() string {
	if m != nil {
		return m.UserConfig
	}
	return ""
}

func (m *FunctionDetails) GetSecretsMap() string {
	if m != nil {
		return m.SecretsMap
	}
	return ""
}

func (m *FunctionDetails) GetRuntime() FunctionDetails_Runtime {
	if m != nil {
		return m.Runtime
	}
	return FunctionDetails_JAVA
}

func (m *FunctionDetails) GetAutoAck() bool {
	if m != nil {
		return m.AutoAck
	}
	return false
}

func (m *FunctionDetails) GetParallelism() int32 {
	if m != nil {
		return m.Parallelism
	}
	return 0
}

func (m *FunctionDetails) GetSource() *SourceSpec {
	if m != nil {
		return m.Source
	}
	return nil
}

func (m *FunctionDetails) GetSink() *SinkSpec {
	if m != nil {
		return m.Sink
	}
	return nil
}

func (m *FunctionDetails) GetResources() *Resources {
	if m != nil {
		return m.Resources
	}
	return nil
}

func (m *FunctionDetails) GetPackageUrl() string {
	if m != nil {
		return m.PackageUrl
	}
	return ""
}

func (m *FunctionDetails) GetRetryDetails() *RetryDetails {
	if m != nil {
		return m.RetryDetails
	}
	return nil
}

func (m *FunctionDetails) GetRuntimeFlags() string {
	if m != nil {
		return m.RuntimeFlags
	}
	return ""
}

func (m *FunctionDetails) GetComponentType() FunctionDetails_ComponentType {
	if m != nil {
		return m.ComponentType
	}
	return FunctionDetails_UNKNOWN
}

func (m *FunctionDetails) GetCustomRuntimeOptions() string {
	if m != nil {
		return m.CustomRuntimeOptions
	}
	return ""
}

type ConsumerSpec struct {
	SchemaType           string                          `protobuf:"bytes,1,opt,name=schemaType,proto3" json:"schemaType,omitempty"`
	SerdeClassName       string                          `protobuf:"bytes,2,opt,name=serdeClassName,proto3" json:"serdeClassName,omitempty"`
	IsRegexPattern       bool                            `protobuf:"varint,3,opt,name=isRegexPattern,proto3" json:"isRegexPattern,omitempty"`
	ReceiverQueueSize    *ConsumerSpec_ReceiverQueueSize `protobuf:"bytes,4,opt,name=receiverQueueSize,proto3" json:"receiverQueueSize,omitempty"`
	XXX_NoUnkeyedLiteral struct{}                        `json:"-"`
	XXX_unrecognized     []byte                          `json:"-"`
	XXX_sizecache        int32                           `json:"-"`
}

func (m *ConsumerSpec) Reset()         { *m = ConsumerSpec{} }
func (m *ConsumerSpec) String() string { return proto.CompactTextString(m) }
func (*ConsumerSpec) ProtoMessage()    {}
func (*ConsumerSpec) Descriptor() ([]byte, []int) {
	return fileDescriptor_225cf355fcfb169c, []int{3}
}

func (m *ConsumerSpec) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_ConsumerSpec.Unmarshal(m, b)
}
func (m *ConsumerSpec) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_ConsumerSpec.Marshal(b, m, deterministic)
}
func (m *ConsumerSpec) XXX_Merge(src proto.Message) {
	xxx_messageInfo_ConsumerSpec.Merge(m, src)
}
func (m *ConsumerSpec) XXX_Size() int {
	return xxx_messageInfo_ConsumerSpec.Size(m)
}
func (m *ConsumerSpec) XXX_DiscardUnknown() {
	xxx_messageInfo_ConsumerSpec.DiscardUnknown(m)
}

var xxx_messageInfo_ConsumerSpec proto.InternalMessageInfo

func (m *ConsumerSpec) GetSchemaType() string {
	if m != nil {
		return m.SchemaType
	}
	return ""
}

func (m *ConsumerSpec) GetSerdeClassName() string {
	if m != nil {
		return m.SerdeClassName
	}
	return ""
}

func (m *ConsumerSpec) GetIsRegexPattern() bool {
	if m != nil {
		return m.IsRegexPattern
	}
	return false
}

func (m *ConsumerSpec) GetReceiverQueueSize() *ConsumerSpec_ReceiverQueueSize {
	if m != nil {
		return m.ReceiverQueueSize
	}
	return nil
}

type ConsumerSpec_ReceiverQueueSize struct {
	Value                int32    `protobuf:"varint,1,opt,name=value,proto3" json:"value,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *ConsumerSpec_ReceiverQueueSize) Reset()         { *m = ConsumerSpec_ReceiverQueueSize{} }
func (m *ConsumerSpec_ReceiverQueueSize) String() string { return proto.CompactTextString(m) }
func (*ConsumerSpec_ReceiverQueueSize) ProtoMessage()    {}
func (*ConsumerSpec_ReceiverQueueSize) Descriptor() ([]byte, []int) {
	return fileDescriptor_225cf355fcfb169c, []int{3, 0}
}

func (m *ConsumerSpec_ReceiverQueueSize) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_ConsumerSpec_ReceiverQueueSize.Unmarshal(m, b)
}
func (m *ConsumerSpec_ReceiverQueueSize) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_ConsumerSpec_ReceiverQueueSize.Marshal(b, m, deterministic)
}
func (m *ConsumerSpec_ReceiverQueueSize) XXX_Merge(src proto.Message) {
	xxx_messageInfo_ConsumerSpec_ReceiverQueueSize.Merge(m, src)
}
func (m *ConsumerSpec_ReceiverQueueSize) XXX_Size() int {
	return xxx_messageInfo_ConsumerSpec_ReceiverQueueSize.Size(m)
}
func (m *ConsumerSpec_ReceiverQueueSize) XXX_DiscardUnknown() {
	xxx_messageInfo_ConsumerSpec_ReceiverQueueSize.DiscardUnknown(m)
}

var xxx_messageInfo_ConsumerSpec_ReceiverQueueSize proto.InternalMessageInfo

func (m *ConsumerSpec_ReceiverQueueSize) GetValue() int32 {
	if m != nil {
		return m.Value
	}
	return 0
}

type SourceSpec struct {
	ClassName string `protobuf:"bytes,1,opt,name=className,proto3" json:"className,omitempty"`
	// map in json format
	Configs       string `protobuf:"bytes,2,opt,name=configs,proto3" json:"configs,omitempty"`
	TypeClassName string `protobuf:"bytes,5,opt,name=typeClassName,proto3" json:"typeClassName,omitempty"`
	// configs used only when source feeds into functions
	SubscriptionType SubscriptionType `protobuf:"varint,3,opt,name=subscriptionType,proto3,enum=proto.SubscriptionType" json:"subscriptionType,omitempty"`
	// @deprecated -- use topicsToSchema
	TopicsToSerDeClassName map[string]string `protobuf:"bytes,4,rep,name=topicsToSerDeClassName,proto3" json:"topicsToSerDeClassName,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"` // Deprecated: Do not use.
	//*
	//
	InputSpecs    map[string]*ConsumerSpec `protobuf:"bytes,10,rep,name=inputSpecs,proto3" json:"inputSpecs,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
	TimeoutMs     uint64                   `protobuf:"varint,6,opt,name=timeoutMs,proto3" json:"timeoutMs,omitempty"`
	TopicsPattern string                   `protobuf:"bytes,7,opt,name=topicsPattern,proto3" json:"topicsPattern,omitempty"` // Deprecated: Do not use.
	// If specified, this will refer to an archive that is
	// already present in the server
	Builtin              string               `protobuf:"bytes,8,opt,name=builtin,proto3" json:"builtin,omitempty"`
	SubscriptionName     string               `protobuf:"bytes,9,opt,name=subscriptionName,proto3" json:"subscriptionName,omitempty"`
	CleanupSubscription  bool                 `protobuf:"varint,11,opt,name=cleanupSubscription,proto3" json:"cleanupSubscription,omitempty"`
	SubscriptionPosition SubscriptionPosition `protobuf:"varint,12,opt,name=subscriptionPosition,proto3,enum=proto.SubscriptionPosition" json:"subscriptionPosition,omitempty"`
	XXX_NoUnkeyedLiteral struct{}             `json:"-"`
	XXX_unrecognized     []byte               `json:"-"`
	XXX_sizecache        int32                `json:"-"`
}

func (m *SourceSpec) Reset()         { *m = SourceSpec{} }
func (m *SourceSpec) String() string { return proto.CompactTextString(m) }
func (*SourceSpec) ProtoMessage()    {}
func (*SourceSpec) Descriptor() ([]byte, []int) {
	return fileDescriptor_225cf355fcfb169c, []int{4}
}

func (m *SourceSpec) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_SourceSpec.Unmarshal(m, b)
}
func (m *SourceSpec) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_SourceSpec.Marshal(b, m, deterministic)
}
func (m *SourceSpec) XXX_Merge(src proto.Message) {
	xxx_messageInfo_SourceSpec.Merge(m, src)
}
func (m *SourceSpec) XXX_Size() int {
	return xxx_messageInfo_SourceSpec.Size(m)
}
func (m *SourceSpec) XXX_DiscardUnknown() {
	xxx_messageInfo_SourceSpec.DiscardUnknown(m)
}

var xxx_messageInfo_SourceSpec proto.InternalMessageInfo

func (m *SourceSpec) GetClassName() string {
	if m != nil {
		return m.ClassName
	}
	return ""
}

func (m *SourceSpec) GetConfigs() string {
	if m != nil {
		return m.Configs
	}
	return ""
}

func (m *SourceSpec) GetTypeClassName() string {
	if m != nil {
		return m.TypeClassName
	}
	return ""
}

func (m *SourceSpec) GetSubscriptionType() SubscriptionType {
	if m != nil {
		return m.SubscriptionType
	}
	return SubscriptionType_SHARED
}

// Deprecated: Do not use.
func (m *SourceSpec) GetTopicsToSerDeClassName() map[string]string {
	if m != nil {
		return m.TopicsToSerDeClassName
	}
	return nil
}

func (m *SourceSpec) GetInputSpecs() map[string]*ConsumerSpec {
	if m != nil {
		return m.InputSpecs
	}
	return nil
}

func (m *SourceSpec) GetTimeoutMs() uint64 {
	if m != nil {
		return m.TimeoutMs
	}
	return 0
}

// Deprecated: Do not use.
func (m *SourceSpec) GetTopicsPattern() string {
	if m != nil {
		return m.TopicsPattern
	}
	return ""
}

func (m *SourceSpec) GetBuiltin() string {
	if m != nil {
		return m.Builtin
	}
	return ""
}

func (m *SourceSpec) GetSubscriptionName() string {
	if m != nil {
		return m.SubscriptionName
	}
	return ""
}

func (m *SourceSpec) GetCleanupSubscription() bool {
	if m != nil {
		return m.CleanupSubscription
	}
	return false
}

func (m *SourceSpec) GetSubscriptionPosition() SubscriptionPosition {
	if m != nil {
		return m.SubscriptionPosition
	}
	return SubscriptionPosition_LATEST
}

type SinkSpec struct {
	ClassName string `protobuf:"bytes,1,opt,name=className,proto3" json:"className,omitempty"`
	// map in json format
	Configs       string `protobuf:"bytes,2,opt,name=configs,proto3" json:"configs,omitempty"`
	TypeClassName string `protobuf:"bytes,5,opt,name=typeClassName,proto3" json:"typeClassName,omitempty"`
	// configs used only when functions output to sink
	Topic          string `protobuf:"bytes,3,opt,name=topic,proto3" json:"topic,omitempty"`
	SerDeClassName string `protobuf:"bytes,4,opt,name=serDeClassName,proto3" json:"serDeClassName,omitempty"`
	// If specified, this will refer to an archive that is
	// already present in the server
	Builtin string `protobuf:"bytes,6,opt,name=builtin,proto3" json:"builtin,omitempty"`
	//*
	// Builtin schema type or custom schema class name
	SchemaType           string   `protobuf:"bytes,7,opt,name=schemaType,proto3" json:"schemaType,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *SinkSpec) Reset()         { *m = SinkSpec{} }
func (m *SinkSpec) String() string { return proto.CompactTextString(m) }
func (*SinkSpec) ProtoMessage()    {}
func (*SinkSpec) Descriptor() ([]byte, []int) {
	return fileDescriptor_225cf355fcfb169c, []int{5}
}

func (m *SinkSpec) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_SinkSpec.Unmarshal(m, b)
}
func (m *SinkSpec) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_SinkSpec.Marshal(b, m, deterministic)
}
func (m *SinkSpec) XXX_Merge(src proto.Message) {
	xxx_messageInfo_SinkSpec.Merge(m, src)
}
func (m *SinkSpec) XXX_Size() int {
	return xxx_messageInfo_SinkSpec.Size(m)
}
func (m *SinkSpec) XXX_DiscardUnknown() {
	xxx_messageInfo_SinkSpec.DiscardUnknown(m)
}

var xxx_messageInfo_SinkSpec proto.InternalMessageInfo

func (m *SinkSpec) GetClassName() string {
	if m != nil {
		return m.ClassName
	}
	return ""
}

func (m *SinkSpec) GetConfigs() string {
	if m != nil {
		return m.Configs
	}
	return ""
}

func (m *SinkSpec) GetTypeClassName() string {
	if m != nil {
		return m.TypeClassName
	}
	return ""
}

func (m *SinkSpec) GetTopic() string {
	if m != nil {
		return m.Topic
	}
	return ""
}

func (m *SinkSpec) GetSerDeClassName() string {
	if m != nil {
		return m.SerDeClassName
	}
	return ""
}

func (m *SinkSpec) GetBuiltin() string {
	if m != nil {
		return m.Builtin
	}
	return ""
}

func (m *SinkSpec) GetSchemaType() string {
	if m != nil {
		return m.SchemaType
	}
	return ""
}

type PackageLocationMetaData struct {
	PackagePath          string   `protobuf:"bytes,1,opt,name=packagePath,proto3" json:"packagePath,omitempty"`
	OriginalFileName     string   `protobuf:"bytes,2,opt,name=originalFileName,proto3" json:"originalFileName,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *PackageLocationMetaData) Reset()         { *m = PackageLocationMetaData{} }
func (m *PackageLocationMetaData) String() string { return proto.CompactTextString(m) }
func (*PackageLocationMetaData) ProtoMessage()    {}
func (*PackageLocationMetaData) Descriptor() ([]byte, []int) {
	return fileDescriptor_225cf355fcfb169c, []int{6}
}

func (m *PackageLocationMetaData) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_PackageLocationMetaData.Unmarshal(m, b)
}
func (m *PackageLocationMetaData) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_PackageLocationMetaData.Marshal(b, m, deterministic)
}
func (m *PackageLocationMetaData) XXX_Merge(src proto.Message) {
	xxx_messageInfo_PackageLocationMetaData.Merge(m, src)
}
func (m *PackageLocationMetaData) XXX_Size() int {
	return xxx_messageInfo_PackageLocationMetaData.Size(m)
}
func (m *PackageLocationMetaData) XXX_DiscardUnknown() {
	xxx_messageInfo_PackageLocationMetaData.DiscardUnknown(m)
}

var xxx_messageInfo_PackageLocationMetaData proto.InternalMessageInfo

func (m *PackageLocationMetaData) GetPackagePath() string {
	if m != nil {
		return m.PackagePath
	}
	return ""
}

func (m *PackageLocationMetaData) GetOriginalFileName() string {
	if m != nil {
		return m.OriginalFileName
	}
	return ""
}

type FunctionMetaData struct {
	FunctionDetails      *FunctionDetails            `protobuf:"bytes,1,opt,name=functionDetails,proto3" json:"functionDetails,omitempty"`
	PackageLocation      *PackageLocationMetaData    `protobuf:"bytes,2,opt,name=packageLocation,proto3" json:"packageLocation,omitempty"`
	Version              uint64                      `protobuf:"varint,3,opt,name=version,proto3" json:"version,omitempty"`
	CreateTime           uint64                      `protobuf:"varint,4,opt,name=createTime,proto3" json:"createTime,omitempty"`
	InstanceStates       map[int32]FunctionState     `protobuf:"bytes,5,rep,name=instanceStates,proto3" json:"instanceStates,omitempty" protobuf_key:"varint,1,opt,name=key,proto3" protobuf_val:"varint,2,opt,name=value,proto3,enum=proto.FunctionState"`
	FunctionAuthSpec     *FunctionAuthenticationSpec `protobuf:"bytes,6,opt,name=functionAuthSpec,proto3" json:"functionAuthSpec,omitempty"`
	XXX_NoUnkeyedLiteral struct{}                    `json:"-"`
	XXX_unrecognized     []byte                      `json:"-"`
	XXX_sizecache        int32                       `json:"-"`
}

func (m *FunctionMetaData) Reset()         { *m = FunctionMetaData{} }
func (m *FunctionMetaData) String() string { return proto.CompactTextString(m) }
func (*FunctionMetaData) ProtoMessage()    {}
func (*FunctionMetaData) Descriptor() ([]byte, []int) {
	return fileDescriptor_225cf355fcfb169c, []int{7}
}

func (m *FunctionMetaData) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_FunctionMetaData.Unmarshal(m, b)
}
func (m *FunctionMetaData) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_FunctionMetaData.Marshal(b, m, deterministic)
}
func (m *FunctionMetaData) XXX_Merge(src proto.Message) {
	xxx_messageInfo_FunctionMetaData.Merge(m, src)
}
func (m *FunctionMetaData) XXX_Size() int {
	return xxx_messageInfo_FunctionMetaData.Size(m)
}
func (m *FunctionMetaData) XXX_DiscardUnknown() {
	xxx_messageInfo_FunctionMetaData.DiscardUnknown(m)
}

var xxx_messageInfo_FunctionMetaData proto.InternalMessageInfo

func (m *FunctionMetaData) GetFunctionDetails() *FunctionDetails {
	if m != nil {
		return m.FunctionDetails
	}
	return nil
}

func (m *FunctionMetaData) GetPackageLocation() *PackageLocationMetaData {
	if m != nil {
		return m.PackageLocation
	}
	return nil
}

func (m *FunctionMetaData) GetVersion() uint64 {
	if m != nil {
		return m.Version
	}
	return 0
}

func (m *FunctionMetaData) GetCreateTime() uint64 {
	if m != nil {
		return m.CreateTime
	}
	return 0
}

func (m *FunctionMetaData) GetInstanceStates() map[int32]FunctionState {
	if m != nil {
		return m.InstanceStates
	}
	return nil
}

func (m *FunctionMetaData) GetFunctionAuthSpec() *FunctionAuthenticationSpec {
	if m != nil {
		return m.FunctionAuthSpec
	}
	return nil
}

type FunctionAuthenticationSpec struct {
	//*
	// function authentication related data that the function authentication provider
	// needs to cache/distribute to all workers support function authentication.
	// Depending on the function authentication provider implementation, this can be the actual auth credentials
	// or a pointer to the auth credentials that this function should use
	Data []byte `protobuf:"bytes,1,opt,name=data,proto3" json:"data,omitempty"`
	//*
	// classname of the function auth provicer this data is relevant to
	Provider             string   `protobuf:"bytes,2,opt,name=provider,proto3" json:"provider,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *FunctionAuthenticationSpec) Reset()         { *m = FunctionAuthenticationSpec{} }
func (m *FunctionAuthenticationSpec) String() string { return proto.CompactTextString(m) }
func (*FunctionAuthenticationSpec) ProtoMessage()    {}
func (*FunctionAuthenticationSpec) Descriptor() ([]byte, []int) {
	return fileDescriptor_225cf355fcfb169c, []int{8}
}

func (m *FunctionAuthenticationSpec) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_FunctionAuthenticationSpec.Unmarshal(m, b)
}
func (m *FunctionAuthenticationSpec) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_FunctionAuthenticationSpec.Marshal(b, m, deterministic)
}
func (m *FunctionAuthenticationSpec) XXX_Merge(src proto.Message) {
	xxx_messageInfo_FunctionAuthenticationSpec.Merge(m, src)
}
func (m *FunctionAuthenticationSpec) XXX_Size() int {
	return xxx_messageInfo_FunctionAuthenticationSpec.Size(m)
}
func (m *FunctionAuthenticationSpec) XXX_DiscardUnknown() {
	xxx_messageInfo_FunctionAuthenticationSpec.DiscardUnknown(m)
}

var xxx_messageInfo_FunctionAuthenticationSpec proto.InternalMessageInfo

func (m *FunctionAuthenticationSpec) GetData() []byte {
	if m != nil {
		return m.Data
	}
	return nil
}

func (m *FunctionAuthenticationSpec) GetProvider() string {
	if m != nil {
		return m.Provider
	}
	return ""
}

type Instance struct {
	FunctionMetaData     *FunctionMetaData `protobuf:"bytes,1,opt,name=functionMetaData,proto3" json:"functionMetaData,omitempty"`
	InstanceId           int32             `protobuf:"varint,2,opt,name=instanceId,proto3" json:"instanceId,omitempty"`
	XXX_NoUnkeyedLiteral struct{}          `json:"-"`
	XXX_unrecognized     []byte            `json:"-"`
	XXX_sizecache        int32             `json:"-"`
}

func (m *Instance) Reset()         { *m = Instance{} }
func (m *Instance) String() string { return proto.CompactTextString(m) }
func (*Instance) ProtoMessage()    {}
func (*Instance) Descriptor() ([]byte, []int) {
	return fileDescriptor_225cf355fcfb169c, []int{9}
}

func (m *Instance) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_Instance.Unmarshal(m, b)
}
func (m *Instance) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_Instance.Marshal(b, m, deterministic)
}
func (m *Instance) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Instance.Merge(m, src)
}
func (m *Instance) XXX_Size() int {
	return xxx_messageInfo_Instance.Size(m)
}
func (m *Instance) XXX_DiscardUnknown() {
	xxx_messageInfo_Instance.DiscardUnknown(m)
}

var xxx_messageInfo_Instance proto.InternalMessageInfo

func (m *Instance) GetFunctionMetaData() *FunctionMetaData {
	if m != nil {
		return m.FunctionMetaData
	}
	return nil
}

func (m *Instance) GetInstanceId() int32 {
	if m != nil {
		return m.InstanceId
	}
	return 0
}

type Assignment struct {
	Instance             *Instance `protobuf:"bytes,1,opt,name=instance,proto3" json:"instance,omitempty"`
	WorkerId             string    `protobuf:"bytes,2,opt,name=workerId,proto3" json:"workerId,omitempty"`
	XXX_NoUnkeyedLiteral struct{}  `json:"-"`
	XXX_unrecognized     []byte    `json:"-"`
	XXX_sizecache        int32     `json:"-"`
}

func (m *Assignment) Reset()         { *m = Assignment{} }
func (m *Assignment) String() string { return proto.CompactTextString(m) }
func (*Assignment) ProtoMessage()    {}
func (*Assignment) Descriptor() ([]byte, []int) {
	return fileDescriptor_225cf355fcfb169c, []int{10}
}

func (m *Assignment) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_Assignment.Unmarshal(m, b)
}
func (m *Assignment) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_Assignment.Marshal(b, m, deterministic)
}
func (m *Assignment) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Assignment.Merge(m, src)
}
func (m *Assignment) XXX_Size() int {
	return xxx_messageInfo_Assignment.Size(m)
}
func (m *Assignment) XXX_DiscardUnknown() {
	xxx_messageInfo_Assignment.DiscardUnknown(m)
}

var xxx_messageInfo_Assignment proto.InternalMessageInfo

func (m *Assignment) GetInstance() *Instance {
	if m != nil {
		return m.Instance
	}
	return nil
}

func (m *Assignment) GetWorkerId() string {
	if m != nil {
		return m.WorkerId
	}
	return ""
}

func init() {
	proto.RegisterEnum("proto.ProcessingGuarantees", ProcessingGuarantees_name, ProcessingGuarantees_value)
	proto.RegisterEnum("proto.SubscriptionType", SubscriptionType_name, SubscriptionType_value)
	proto.RegisterEnum("proto.SubscriptionPosition", SubscriptionPosition_name, SubscriptionPosition_value)
	proto.RegisterEnum("proto.FunctionState", FunctionState_name, FunctionState_value)
	proto.RegisterEnum("proto.FunctionDetails_Runtime", FunctionDetails_Runtime_name, FunctionDetails_Runtime_value)
	proto.RegisterEnum("proto.FunctionDetails_ComponentType", FunctionDetails_ComponentType_name, FunctionDetails_ComponentType_value)
	proto.RegisterType((*Resources)(nil), "proto.Resources")
	proto.RegisterType((*RetryDetails)(nil), "proto.RetryDetails")
	proto.RegisterType((*FunctionDetails)(nil), "proto.FunctionDetails")
	proto.RegisterType((*ConsumerSpec)(nil), "proto.ConsumerSpec")
	proto.RegisterType((*ConsumerSpec_ReceiverQueueSize)(nil), "proto.ConsumerSpec.ReceiverQueueSize")
	proto.RegisterType((*SourceSpec)(nil), "proto.SourceSpec")
	proto.RegisterMapType((map[string]*ConsumerSpec)(nil), "proto.SourceSpec.InputSpecsEntry")
	proto.RegisterMapType((map[string]string)(nil), "proto.SourceSpec.TopicsToSerDeClassNameEntry")
	proto.RegisterType((*SinkSpec)(nil), "proto.SinkSpec")
	proto.RegisterType((*PackageLocationMetaData)(nil), "proto.PackageLocationMetaData")
	proto.RegisterType((*FunctionMetaData)(nil), "proto.FunctionMetaData")
	proto.RegisterMapType((map[int32]FunctionState)(nil), "proto.FunctionMetaData.InstanceStatesEntry")
	proto.RegisterType((*FunctionAuthenticationSpec)(nil), "proto.FunctionAuthenticationSpec")
	proto.RegisterType((*Instance)(nil), "proto.Instance")
	proto.RegisterType((*Assignment)(nil), "proto.Assignment")
}

func init() { proto.RegisterFile("Function.proto", fileDescriptor_225cf355fcfb169c) }

var fileDescriptor_225cf355fcfb169c = []byte{
	// 1410 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xb4, 0x56, 0x4f, 0x6f, 0xdb, 0x36,
	0x14, 0x8f, 0xe2, 0xf8, 0xdf, 0xb3, 0x13, 0x2b, 0x8c, 0xd1, 0x0a, 0xe9, 0x50, 0xa4, 0x5e, 0xb7,
	0x39, 0x69, 0x6b, 0x14, 0xd9, 0x61, 0xc5, 0x4e, 0x75, 0x1d, 0xa7, 0x75, 0xeb, 0xd8, 0x1e, 0xad,
	0xb4, 0xe8, 0x69, 0x60, 0x15, 0xc6, 0x21, 0x2c, 0x4b, 0x02, 0x49, 0x65, 0xcd, 0xce, 0xbb, 0xee,
	0xe3, 0xed, 0xbe, 0x4f, 0x32, 0x0c, 0xa4, 0x24, 0x5b, 0x92, 0x9d, 0xdd, 0x76, 0x92, 0xde, 0xef,
	0xfd, 0x23, 0x7f, 0x7c, 0xef, 0x91, 0xb0, 0x77, 0x1e, 0x7a, 0x8e, 0x64, 0xbe, 0xd7, 0x09, 0xb8,
	0x2f, 0x7d, 0x54, 0xd4, 0x9f, 0x56, 0x0f, 0xaa, 0x98, 0x0a, 0x3f, 0xe4, 0x0e, 0x15, 0xc8, 0x84,
	0x82, 0x13, 0x84, 0x96, 0x71, 0x64, 0xb4, 0x0d, 0xac, 0x7e, 0x15, 0xc2, 0xc9, 0xc2, 0xda, 0x3e,
	0x32, 0xda, 0x05, 0xac, 0x7e, 0x11, 0x82, 0x9d, 0x2b, 0x26, 0xe6, 0x56, 0x41, 0x43, 0xfa, 0xbf,
	0x75, 0x0d, 0x75, 0x4c, 0x25, 0xbf, 0x3b, 0xa3, 0x92, 0x30, 0x57, 0xa0, 0xe7, 0xb0, 0xbf, 0x20,
	0x5f, 0x2f, 0xa8, 0x10, 0x64, 0x46, 0x95, 0x86, 0x51, 0xa1, 0xa3, 0x16, 0xf1, 0xba, 0x02, 0xb5,
	0xa1, 0x71, 0x45, 0xc9, 0xd5, 0x90, 0x4a, 0x49, 0xb9, 0xed, 0x07, 0xcc, 0xd1, 0xf9, 0xaa, 0x38,
	0x0f, 0xb7, 0xfe, 0x28, 0x43, 0x23, 0xd9, 0x46, 0x92, 0xeb, 0x01, 0x94, 0x24, 0xf5, 0x88, 0x27,
	0x75, 0x82, 0x2a, 0x8e, 0x25, 0xf4, 0x0d, 0x54, 0x3d, 0xb2, 0xa0, 0x22, 0x20, 0x0e, 0x8d, 0xe3,
	0xad, 0x00, 0xb5, 0x0b, 0x25, 0xe8, 0x5d, 0x54, 0xb1, 0xfe, 0x57, 0x1e, 0x8e, 0x4b, 0x84, 0x18,
	0x29, 0xc5, 0x4e, 0xe4, 0xb1, 0x04, 0xd0, 0x21, 0x54, 0x5c, 0x7f, 0x16, 0x2d, 0xaf, 0xa8, 0x95,
	0x4b, 0x19, 0x8d, 0xa1, 0x19, 0x70, 0xdf, 0xa1, 0x42, 0x30, 0x6f, 0xf6, 0x36, 0x24, 0x9c, 0x78,
	0x92, 0x52, 0x61, 0x95, 0x8e, 0x8c, 0xf6, 0xde, 0xe9, 0xa3, 0x88, 0xf1, 0xce, 0x64, 0x83, 0x09,
	0xde, 0xe8, 0x88, 0x1e, 0x03, 0x84, 0x82, 0xf2, 0x9e, 0xef, 0x5d, 0xb3, 0x99, 0x55, 0xd6, 0xe9,
	0x52, 0x88, 0xd2, 0x0b, 0xea, 0x70, 0x2a, 0xc5, 0x05, 0x09, 0x2c, 0x33, 0xd2, 0xaf, 0x10, 0xf4,
	0x0a, 0xca, 0x3c, 0xf4, 0x24, 0x5b, 0x50, 0xab, 0xa2, 0xd7, 0xf0, 0x38, 0x5e, 0x43, 0x8e, 0xbd,
	0x0e, 0x8e, 0xac, 0x70, 0x62, 0x8e, 0x2c, 0x28, 0x93, 0x50, 0xfa, 0x5d, 0x67, 0x6e, 0x55, 0x8f,
	0x8c, 0x76, 0x05, 0x27, 0x22, 0x3a, 0x82, 0x5a, 0x40, 0x38, 0x71, 0x5d, 0xea, 0x32, 0xb1, 0xb0,
	0x40, 0x1f, 0x67, 0x1a, 0x42, 0xc7, 0x50, 0x8a, 0x2a, 0xc9, 0xaa, 0x1d, 0x19, 0xed, 0xda, 0xe9,
	0x7e, 0x9c, 0x74, 0xaa, 0xc1, 0x69, 0x40, 0x1d, 0x1c, 0x1b, 0xa0, 0x6f, 0x61, 0x47, 0x30, 0x6f,
	0x6e, 0xd5, 0xb5, 0x61, 0x23, 0x31, 0x64, 0xde, 0x5c, 0x9b, 0x69, 0x25, 0xea, 0x40, 0x95, 0x27,
	0xb5, 0x69, 0xed, 0x6a, 0x4b, 0x33, 0xb6, 0x5c, 0xd6, 0x2c, 0x5e, 0x99, 0x28, 0x56, 0x02, 0xe2,
	0xcc, 0xc9, 0x8c, 0x5e, 0x72, 0xd7, 0xda, 0x8b, 0x58, 0x59, 0x21, 0xe8, 0x27, 0xa8, 0xf3, 0x54,
	0x99, 0x5a, 0x0d, 0x1d, 0xf2, 0x60, 0x19, 0x72, 0xa5, 0xc2, 0x19, 0x43, 0xd4, 0x82, 0x7a, 0xcc,
	0xcf, 0xb9, 0x4b, 0x66, 0xc2, 0xda, 0xd7, 0xa1, 0x33, 0x18, 0x7a, 0x0f, 0xbb, 0x8e, 0xbf, 0x08,
	0x7c, 0x8f, 0x7a, 0xd2, 0xbe, 0x0b, 0xa8, 0x85, 0x34, 0xf1, 0x4f, 0xef, 0x21, 0xbe, 0x97, 0xb6,
	0xc5, 0x59, 0x57, 0x74, 0x0a, 0x4d, 0x27, 0x14, 0xd2, 0x5f, 0xc4, 0xc7, 0x33, 0x0e, 0x94, 0xab,
	0xb0, 0x0e, 0x74, 0xde, 0x8d, 0xba, 0xd6, 0x0f, 0x50, 0x8e, 0x11, 0x54, 0x81, 0x9d, 0xf7, 0xdd,
	0x8f, 0x5d, 0x73, 0x0b, 0x01, 0x94, 0x26, 0x9f, 0xed, 0x77, 0xe3, 0x91, 0x69, 0xa0, 0x12, 0x6c,
	0xbf, 0x1d, 0x9b, 0x85, 0xd6, 0x6b, 0xd8, 0xcd, 0x24, 0x47, 0x35, 0x28, 0x5f, 0x8e, 0x3e, 0x8c,
	0xc6, 0x9f, 0x46, 0xe6, 0x16, 0xaa, 0x43, 0xe5, 0xfc, 0x72, 0xd4, 0xb3, 0x07, 0xda, 0x07, 0xa0,
	0x34, 0x1d, 0x5f, 0xe2, 0x5e, 0xdf, 0xdc, 0x56, 0x51, 0xa7, 0x83, 0xd1, 0x07, 0xb3, 0xd0, 0xfa,
	0xc7, 0x80, 0x7a, 0xcf, 0xf7, 0x44, 0xb8, 0xa0, 0x5c, 0x1d, 0x97, 0x2e, 0x47, 0xe7, 0x86, 0x2e,
	0x88, 0xde, 0xb8, 0x11, 0x97, 0xe3, 0x12, 0x41, 0xdf, 0xc3, 0x9e, 0xa0, 0xfc, 0x8a, 0xf6, 0x96,
	0xed, 0x15, 0x35, 0x64, 0x0e, 0x55, 0x76, 0x4c, 0x60, 0x3a, 0xa3, 0x5f, 0x27, 0x44, 0x75, 0xbd,
	0xa7, 0xfb, 0xb3, 0x82, 0x73, 0x28, 0x9a, 0xc2, 0x3e, 0xa7, 0x0e, 0x65, 0xb7, 0x94, 0xff, 0x12,
	0xd2, 0x90, 0x4e, 0xd9, 0xef, 0x51, 0xc7, 0xd6, 0x4e, 0xbf, 0x8b, 0xf9, 0x4e, 0xaf, 0xaf, 0x83,
	0xf3, 0xc6, 0x78, 0xdd, 0xff, 0xf0, 0x18, 0xf6, 0xd7, 0xec, 0x50, 0x13, 0x8a, 0xb7, 0xc4, 0x0d,
	0x69, 0x3c, 0xbd, 0x22, 0xa1, 0xf5, 0x67, 0x09, 0x60, 0x55, 0xd4, 0xd9, 0xc1, 0x61, 0xe4, 0x07,
	0x87, 0x05, 0x65, 0x47, 0x77, 0xad, 0x88, 0x77, 0x9d, 0x88, 0xe8, 0x29, 0xec, 0xca, 0xbb, 0x20,
	0xc5, 0x4a, 0x34, 0x57, 0xb2, 0x20, 0xea, 0x81, 0x29, 0xc2, 0x2f, 0xc2, 0xe1, 0x4c, 0x9f, 0xb4,
	0xa6, 0xb8, 0xa0, 0x6b, 0xeb, 0x61, 0xd2, 0x36, 0x39, 0x35, 0x5e, 0x73, 0x40, 0x0c, 0x1e, 0x48,
	0x35, 0xaa, 0x84, 0xed, 0x4f, 0x29, 0x3f, 0x4b, 0xe5, 0xdc, 0x39, 0x2a, 0xb4, 0x6b, 0xa7, 0x2f,
	0xd6, 0x5a, 0xb5, 0x63, 0x6f, 0xb4, 0xef, 0x7b, 0x92, 0xdf, 0xbd, 0xd9, 0xb6, 0x0c, 0x7c, 0x4f,
	0x40, 0xd4, 0x05, 0x60, 0x5e, 0x10, 0x4a, 0x15, 0x44, 0x58, 0xa0, 0xc3, 0x3f, 0x59, 0x0f, 0x3f,
	0x58, 0xda, 0xe8, 0x90, 0x38, 0xe5, 0xa4, 0x08, 0x55, 0x85, 0xec, 0x87, 0xf2, 0x22, 0x1a, 0xa2,
	0x3b, 0x78, 0x05, 0xa0, 0x36, 0xec, 0x46, 0xa9, 0x93, 0x22, 0xd1, 0xf3, 0x51, 0xaf, 0x29, 0xab,
	0x50, 0xd4, 0x7f, 0x09, 0x99, 0x2b, 0x99, 0xa7, 0xc7, 0x60, 0x15, 0x27, 0x22, 0x3a, 0xc9, 0x92,
	0xaa, 0x99, 0xa8, 0x6a, 0x93, 0x35, 0x1c, 0xbd, 0x84, 0x03, 0xc7, 0xa5, 0xc4, 0x0b, 0x83, 0x34,
	0xd1, 0x7a, 0xc6, 0x55, 0xf0, 0x26, 0x95, 0xba, 0x0f, 0xd2, 0x51, 0x26, 0xbe, 0x60, 0xda, 0xa5,
	0x9e, 0xb9, 0x0f, 0xa6, 0x1b, 0x4c, 0xf0, 0x46, 0xc7, 0xc3, 0x01, 0x3c, 0xfa, 0x8f, 0xe3, 0x50,
	0xb7, 0xf4, 0x9c, 0xde, 0xc5, 0xa5, 0xa7, 0x7e, 0x57, 0x75, 0x1b, 0x95, 0x5c, 0x24, 0xfc, 0xbc,
	0xfd, 0xca, 0x38, 0xc4, 0xd0, 0xc8, 0x51, 0xbf, 0xc1, 0xfd, 0x38, 0xed, 0xbe, 0x1a, 0x91, 0xe9,
	0xa6, 0x4a, 0xc5, 0x6c, 0xfd, 0x6d, 0x40, 0x25, 0x99, 0xdd, 0xff, 0x73, 0x37, 0x34, 0xa1, 0xa8,
	0xcf, 0x38, 0xbe, 0xb9, 0x23, 0x21, 0x1e, 0x30, 0xd9, 0xb2, 0x4e, 0x06, 0x4c, 0xba, 0x36, 0x53,
	0x05, 0x51, 0xca, 0x16, 0x44, 0x76, 0x84, 0x95, 0xf3, 0x23, 0xac, 0x35, 0x83, 0x87, 0x93, 0xe8,
	0x26, 0x19, 0xfa, 0x0e, 0x51, 0x87, 0x72, 0x41, 0x25, 0x39, 0x23, 0x92, 0x44, 0x17, 0xa3, 0x56,
	0x4d, 0x88, 0xbc, 0x89, 0xb7, 0x9c, 0x86, 0x54, 0xb5, 0xf9, 0x9c, 0xcd, 0x98, 0x47, 0xdc, 0x73,
	0xe6, 0xd2, 0xd4, 0x04, 0x5c, 0xc3, 0x5b, 0x7f, 0x15, 0xc0, 0x4c, 0x2e, 0x8b, 0x65, 0x8a, 0xd7,
	0xd0, 0xb8, 0xce, 0x5e, 0x20, 0x3a, 0x4d, 0xed, 0xf4, 0xc1, 0xe6, 0xeb, 0x05, 0xe7, 0xcd, 0xd1,
	0x3b, 0x68, 0x04, 0xd9, 0xf5, 0xc7, 0x67, 0x9b, 0xbc, 0x0c, 0xee, 0xd9, 0x1d, 0xce, 0xbb, 0x29,
	0x0e, 0x6f, 0x29, 0x17, 0x2a, 0x42, 0x41, 0xb7, 0x66, 0x22, 0x2a, 0x0e, 0x1d, 0x4e, 0x89, 0xa4,
	0x36, 0x8b, 0x4f, 0x60, 0x07, 0xa7, 0x10, 0x34, 0x85, 0x3d, 0xe6, 0x09, 0x49, 0x3c, 0x87, 0x4e,
	0x25, 0x91, 0x54, 0x58, 0x45, 0x3d, 0x1d, 0x9e, 0xe5, 0x36, 0x91, 0xe4, 0xee, 0x0c, 0x32, 0xd6,
	0xd1, 0x9c, 0xc8, 0x85, 0x40, 0x17, 0x60, 0x26, 0x7b, 0xed, 0x86, 0xf2, 0x46, 0x95, 0xa0, 0x3e,
	0xdb, 0xd5, 0xd0, 0x39, 0x4f, 0xa9, 0xa9, 0x27, 0x59, 0xb4, 0x0f, 0x5d, 0xc3, 0x6b, 0xae, 0x87,
	0x9f, 0xe0, 0x60, 0x43, 0xd6, 0x74, 0x8b, 0x14, 0xa3, 0x16, 0x39, 0x49, 0xb7, 0xc8, 0xde, 0x69,
	0x33, 0x97, 0x4c, 0x3b, 0xa7, 0x7b, 0x64, 0x08, 0x87, 0xf7, 0x2f, 0x44, 0xbf, 0xaa, 0x89, 0x24,
	0x3a, 0x41, 0x1d, 0xeb, 0x7f, 0xf5, 0xe2, 0x0c, 0xb8, 0x7f, 0xcb, 0xae, 0x28, 0x8f, 0xab, 0x65,
	0x29, 0xb7, 0x7c, 0xa8, 0x24, 0xcb, 0x54, 0x17, 0xc4, 0x75, 0x8e, 0xb9, 0xb8, 0x3a, 0x1e, 0xde,
	0x43, 0x2c, 0x5e, 0x73, 0x50, 0x67, 0x97, 0x10, 0x3b, 0xb8, 0xd2, 0xe9, 0x8a, 0x38, 0x85, 0xb4,
	0x2e, 0x01, 0xba, 0x42, 0xb0, 0x99, 0xb7, 0xa0, 0x9e, 0x44, 0xcf, 0xa0, 0x92, 0xe8, 0xe2, 0x54,
	0xc9, 0x13, 0x2e, 0x59, 0x15, 0x5e, 0x1a, 0xa8, 0x7d, 0xfc, 0xe6, 0xf3, 0x39, 0xe5, 0x71, 0xe0,
	0x2a, 0x5e, 0xca, 0x27, 0x63, 0x68, 0x6e, 0x7a, 0x16, 0x23, 0x13, 0xea, 0x5d, 0x7b, 0xd8, 0xef,
	0x4e, 0xed, 0x5f, 0xc7, 0xa3, 0x5e, 0xdf, 0xdc, 0x42, 0x0d, 0xa8, 0x75, 0xed, 0x8b, 0x71, 0x02,
	0x18, 0xa8, 0x09, 0x66, 0xff, 0xfc, 0xbc, 0xdf, 0xb3, 0x07, 0x1f, 0xfb, 0xc3, 0xcf, 0x11, 0xba,
	0x7d, 0xf2, 0x1c, 0xcc, 0xfc, 0x75, 0xa8, 0x5f, 0x31, 0xef, 0xba, 0xb8, 0x7f, 0x16, 0xbf, 0x6f,
	0xba, 0x83, 0xe1, 0xf8, 0x63, 0x1f, 0x9b, 0xc6, 0xc9, 0x4b, 0x68, 0x6e, 0x9a, 0xc2, 0xca, 0x63,
	0xd8, 0xb5, 0xfb, 0x53, 0x3b, 0xf2, 0xe8, 0x77, 0xf1, 0x70, 0xa0, 0x24, 0xe3, 0xe4, 0x18, 0x76,
	0x33, 0x47, 0xac, 0x5e, 0x4f, 0xf8, 0x72, 0x34, 0x1a, 0x8c, 0xde, 0x9a, 0x5b, 0x4a, 0x98, 0xda,
	0xe3, 0xc9, 0xa4, 0x7f, 0x66, 0x1a, 0x6f, 0x5e, 0xc0, 0x13, 0x9f, 0xcf, 0x3a, 0x24, 0x20, 0xce,
	0x0d, 0xed, 0x04, 0xa1, 0x2b, 0x08, 0xef, 0x24, 0xc4, 0x8b, 0x88, 0xaf, 0x37, 0x95, 0x24, 0xda,
	0x97, 0x92, 0x06, 0x7e, 0xfc, 0x37, 0x00, 0x00, 0xff, 0xff, 0xb1, 0xfd, 0x7e, 0x68, 0xa9, 0x0d,
	0x00, 0x00,
}
