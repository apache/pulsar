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
	"time"
)

type message struct {
	ptr *C.pulsar_message_t
}

type messageID struct {
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

			C.pulsar_message_set_property(msg, cKey, cValue)

			C.free(unsafe.Pointer(cKey))
			C.free(unsafe.Pointer(cValue))
		}
	}

	if builder.EventTime.UnixNano() != 0 {
		C.pulsar_message_set_event_timestamp(msg, timeToUnixTimestampMillis(builder.EventTime))
	}

	if builder.ReplicationClusters != nil {
		if len(builder.ReplicationClusters) == 0 {
			// Empty list means to disable replication
			C.pulsar_message_disable_replication(msg, C.int(1))
		} else {
			size := C.int(len(builder.ReplicationClusters))
			array := C.newStringArray(size)
			defer C.freeStringArray(array, size)

			for i, s := range builder.ReplicationClusters {
				C.setString(array, C.CString(s), C.int(i))
			}

			C.pulsar_message_set_replication_clusters(msg, array)
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
	cProperties := C.pulsar_message_get_properties(m.ptr)
	defer C.pulsar_string_map_free(cProperties)

	properties := make(map[string]string)
	count := int(C.pulsar_string_map_size(cProperties))
	for i := 0; i < count; i++ {
		key := C.GoString(C.pulsar_string_map_get_key(cProperties, C.int(i)))
		value := C.GoString(C.pulsar_string_map_get_value(cProperties, C.int(i)))

		properties[key] = value
	}

	return properties
}

func (m *message) Payload() []byte {
	payload := C.pulsar_message_get_data(m.ptr)
	size := C.pulsar_message_get_length(m.ptr)

	// Get the byte array without copying the data. The array will be valid
	// until we free the message in m.ptr
	slice := &reflect.SliceHeader{Data: uintptr(payload), Len: int(size), Cap: int(size)}
	return *(*[]byte)(unsafe.Pointer(slice))
}

func (m *message) ID() MessageID {
	return newMessageId(m.ptr)
}

func (m *message) PublishTime() time.Time {
	return timeFromUnixTimestampMillis(C.pulsar_message_get_publish_timestamp(m.ptr))
}

func (m *message) EventTime() *time.Time {
	eventTime := C.pulsar_message_get_event_timestamp(m.ptr)
	if uint64(eventTime) == 0 {
		return nil
	} else {
		res := timeFromUnixTimestampMillis(eventTime)
		return &res
	}
}

func (m *message) Key() string {
	return C.GoString(C.pulsar_message_get_partitionKey(m.ptr))
}

//////// MessageID

func newMessageId(msg *C.pulsar_message_t) MessageID {
	msgId := &messageID{ptr: C.pulsar_message_get_message_id(msg)}
	runtime.SetFinalizer(msgId, messageIdFinalizer)
	return msgId
}

func messageIdFinalizer(msgID *messageID) {
	C.pulsar_message_id_free(msgID.ptr)
}

func (m *messageID) Serialize() []byte {
	var size C.int
	buf := C.pulsar_message_id_serialize(m.ptr, &size)
	defer C.free(unsafe.Pointer(buf))
	return C.GoBytes(buf, size)
}

func deserializeMessageId(data []byte) MessageID {
	msgId := &messageID{ptr: C.pulsar_message_id_deserialize(unsafe.Pointer(&data[0]), C.uint(len(data)))}
	runtime.SetFinalizer(msgId, messageIdFinalizer)
	return msgId
}

func (m *messageID) String() string {
	str := C.pulsar_message_id_str(m.ptr)
	defer C.free(unsafe.Pointer(str))
	return C.GoString(str)
}

func earliestMessageID() *messageID {
	// No need to use finalizer since the pointer doesn't need to be freed
	return &messageID{C.pulsar_message_id_earliest()}
}

func latestMessageID() *messageID {
	// No need to use finalizer since the pointer doesn't need to be freed
	return &messageID{C.pulsar_message_id_latest()}
}

func timeFromUnixTimestampMillis(timestamp C.ulonglong) time.Time {
	ts := int64(timestamp)
	seconds := ts / int64(time.Millisecond)
	millis := ts - seconds
	nanos := millis * int64(time.Millisecond)
	return time.Unix(seconds, nanos)
}

func timeToUnixTimestampMillis(t time.Time) C.ulonglong {
	nanos := t.UnixNano()
	millis := nanos / int64(time.Millisecond)
	return C.ulonglong(millis)
}
