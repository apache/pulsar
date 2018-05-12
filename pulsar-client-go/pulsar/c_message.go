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

func buildMessage(builder MessageBuilder) *C.pulsar_message_t {

	msg := C.pulsar_message_create()

	if builder.Key != "" {
		cKey := C.CString(builder.Key)
		defer C.free(unsafe.Pointer(cKey))
		C.pulsar_message_set_partition_key(msg, cKey)
	}

	if builder.Payload != nil {
		C.pulsar_message_set_content(msg, unsafe.Pointer(&builder.Payload[0]), C.ulong(len(builder.Payload)))
	}

	if builder.Properties != nil {
		for key, value := range builder.Properties {
			cKey := C.CString(key)
			cValue := C.CString(value)
			defer C.free(unsafe.Pointer(cKey))
			defer C.free(unsafe.Pointer(cValue))

			C.pulsar_message_set_property(msg, cKey, cValue)
		}
	}

	if builder.EventTime != 0 {
		C.pulsar_message_set_event_timestamp(msg, C.ulonglong(builder.EventTime))
	}

	if builder.ReplicationClusters != nil {
		if len(builder.ReplicationClusters) == 0 {
			// Empty list means to disable replication
			C.pulsar_message_disable_replication(msg, C.int(1))
		} else {
			// TODO: Pass the list of clusters
		}
	}

	return msg
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

func (m *message) Payload() []byte {
	payload := C.pulsar_message_get_data(m.ptr)
	size := C.pulsar_message_get_length(m.ptr)

	// Get the byte array without copying the data. The array will be valid
	// until we free the message in m.ptr
	slice := &reflect.SliceHeader{Data: uintptr(payload), Len: int(size), Cap: int(size)}
	return *(*[]byte)(unsafe.Pointer(slice))
}

func (m *message) Id() MessageId {
	return newMessageId(m.ptr)
}

func (m *message) PublishTime() uint64 {
	return uint64(C.pulsar_message_get_publish_timestamp(m.ptr))
}

func (m *message) EventTime() uint64 {
	return uint64(C.pulsar_message_get_event_timestamp(m.ptr))
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

func earliestMessageId() *messageId {
	// No need to use finalizer since the pointer doesn't need to be freed
	return &messageId{C.pulsar_message_id_earliest()}
}

func latestMessageId() *messageId {
	// No need to use finalizer since the pointer doesn't need to be freed
	return &messageId{C.pulsar_message_id_latest()}
}
