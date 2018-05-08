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

package pulsar

/*
#cgo CFLAGS: -I../../pulsar-client-cpp/include
#cgo LDFLAGS: -lpulsar -L../../pulsar-client-cpp/lib
#include "c_go_pulsar.h"
*/
import "C"

import (
	"reflect"
	"runtime"
	"unsafe"
)

type message struct {
	ptr *C.pulsar_message_t
}

type messageBuilder struct {
	ptr *C.pulsar_message_t
}

type messageId struct {
	ptr *C.pulsar_message_id_t
}

////////////////////////////////////////////////////////////

func newMessage() MessageBuilder {
	builder := &messageBuilder{
		ptr: C.pulsar_message_create(),
	}

	runtime.SetFinalizer(builder, messageBuilderFinalizer)
	return builder
}

func messageBuilderFinalizer(b *messageBuilder) {
	if b.ptr != nil {
		C.pulsar_message_free(b.ptr)
	}
}

func (b *messageBuilder) Build() Message {
	msg := &message{
		ptr: b.ptr,
	}

	// We have transferred the ownership of the pointer to the message struct
	b.ptr = nil

	runtime.SetFinalizer(msg, messageFinalizer)
	return msg
}

func (b *messageBuilder) Key(key string) MessageBuilder {
	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))
	C.pulsar_message_set_partition_key(b.ptr, cKey)
	return b
}

func (b *messageBuilder) Payload(payload []byte) MessageBuilder {
	C.pulsar_message_set_content(b.ptr, unsafe.Pointer(&payload[0]), C.ulong(len(payload)))
	return b
}

func (b *messageBuilder) Property(name string, value string) MessageBuilder {
	cName := C.CString(name)
	cValue := C.CString(value)
	defer C.free(unsafe.Pointer(cName))
	defer C.free(unsafe.Pointer(cValue))

	C.pulsar_message_set_property(b.ptr, cName, cValue)
	return b
}

func (b *messageBuilder) EventTime(timestamp uint64) MessageBuilder {
	C.pulsar_message_set_event_timestamp(b.ptr, C.ulonglong(timestamp))
	return b
}

func (b *messageBuilder) SequenceId(sequenceId int64) MessageBuilder {
	C.pulsar_message_set_event_timestamp(b.ptr, C.ulonglong(sequenceId))
	return b
}

func (b *messageBuilder) ReplicationClusters(clusters []string) MessageBuilder {
	// TODO
	return b
}

func (b *messageBuilder) DisableReplication() MessageBuilder {
	C.pulsar_message_disable_replication(b.ptr, cBool(true))
	return b
}

////////////// Message

func newMessageWrapper(ptr *C.pulsar_message_t) Message {
	msg := &message{ptr: ptr}
	runtime.SetFinalizer(msg, messageFinalizer)
	return msg
}

func messageFinalizer(msg *message) {
	C.pulsar_message_free(msg.ptr)
}

func (m *message) Properties() map[string]string {
	// TODO
	return nil
}

func (m *message) HasProperty(name string) bool {
	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))
	return C.pulsar_message_has_property(m.ptr, cName) == 1
}

func (m *message) Property(name string) string {
	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))
	return C.GoString(C.pulsar_message_get_property(m.ptr, cName))
}

func (m *message) Payload() []byte {
	payload := C.pulsar_message_get_data(m.ptr)
	size := C.pulsar_message_get_length(m.ptr)

	// Get the byte array without copying the data. The array will be valid
	// until we free the message in m.ptr
	slice := &reflect.SliceHeader{Data: uintptr(payload), Len: int(size), Cap: int(size)}
	return *(*[]byte)(unsafe.Pointer(slice))
}

func (m *message) MessageId() MessageId {
	return newMessageId(m.ptr)
}

func (m *message) PublishTime() uint64 {
	return uint64(C.pulsar_message_get_publish_timestamp(m.ptr))
}

func (m *message) EventTime() uint64 {
	return uint64(C.pulsar_message_get_event_timestamp(m.ptr))
}

func (m *message) HasKey() bool {
	return C.pulsar_message_has_partition_key(m.ptr) == 1
}

func (m *message) Key() string {
	return C.GoString(C.pulsar_message_get_partitionKey(m.ptr))
}

//////// MessageId

func newMessageId(msg *C.pulsar_message_t) MessageId {
	msgId := &messageId{ptr: C.pulsar_message_get_message_id(msg)}
	runtime.SetFinalizer(msgId, messageIdFinalizer)
	return msgId
}

func messageIdFinalizer(msgId *messageId) {
	C.pulsar_message_id_free(msgId.ptr)
}

func (m *messageId) Serialize() []byte {
	var len C.int
	buf := C.pulsar_message_id_serialize(m.ptr, &len)
	defer C.free(unsafe.Pointer(buf))
	return C.GoBytes(buf, len)
}

func deserializeMessageId(data []byte) MessageId {
	msgId := &messageId{ptr: C.pulsar_message_id_deserialize(unsafe.Pointer(&data[0]), C.uint(len(data)))}
	runtime.SetFinalizer(msgId, messageIdFinalizer)
	return msgId
}

func (m *messageId) String() string {
	str := C.pulsar_message_id_str(m.ptr)
	defer C.free(unsafe.Pointer(str))
	return C.GoString(str)
}

func messageIdEarliest() *messageId {
	// No need to use finalizer since the pointer doesn't need to be freed
	return &messageId{C.pulsar_message_id_earliest()}
}

func messageIdLatest() *messageId {
	// No need to use finalizer since the pointer doesn't need to be freed
	return &messageId{C.pulsar_message_id_latest()}
}
