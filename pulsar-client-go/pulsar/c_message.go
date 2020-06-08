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
#include "c_go_pulsar.h"
*/
import "C"

import (
	"reflect"
	"runtime"
	"time"
	"unsafe"
)

type message struct {
	ptr    *C.pulsar_message_t
	schema Schema
}

type messageID struct {
	ptr *C.pulsar_message_id_t
}

////////////////////////////////////////////////////////////

func buildMessage(message ProducerMessage) *C.pulsar_message_t {

	cMsg := C.pulsar_message_create()

	if message.Key != "" {
		cKey := C.CString(message.Key)
		defer C.free(unsafe.Pointer(cKey))
		C.pulsar_message_set_partition_key(cMsg, cKey)
	}

	if message.Payload != nil {
		C.pulsar_message_set_content(cMsg, unsafe.Pointer(&message.Payload[0]), C.size_t(len(message.Payload)))
	}

	if message.Properties != nil {
		for key, value := range message.Properties {
			cKey := C.CString(key)
			cValue := C.CString(value)

			C.pulsar_message_set_property(cMsg, cKey, cValue)

			C.free(unsafe.Pointer(cKey))
			C.free(unsafe.Pointer(cValue))
		}
	}

	if message.EventTime.UnixNano() != 0 {
		C.pulsar_message_set_event_timestamp(cMsg, C.uint64_t(timeToUnixTimestampMillis(message.EventTime)))
	}

	if message.DeliverAfter != 0 {
		C.pulsar_message_set_deliver_after(cMsg, C.uint64_t(durationToUnixTimestampMillis(message.DeliverAfter)))
	}

	if message.SequenceID != 0 {
		C.pulsar_message_set_sequence_id(cMsg, C.int64_t(message.SequenceID))
	}

	if message.ReplicationClusters != nil {
		if len(message.ReplicationClusters) == 0 {
			// Empty list means to disable replication
			C.pulsar_message_disable_replication(cMsg, C.int(1))
		} else {
			size := C.int(len(message.ReplicationClusters))
			array := C.newStringArray(size)
			defer C.freeStringArray(array, size)

			for i, s := range message.ReplicationClusters {
				C.setString(array, C.CString(s), C.int(i))
			}

			C.pulsar_message_set_replication_clusters(cMsg, array, C.size_t(size))
		}
	}

	return cMsg
}

////////////// Message

func newMessageWrapper(schema Schema, ptr *C.pulsar_message_t) Message {
	msg := &message{schema: schema, ptr: ptr}
	runtime.SetFinalizer(msg, messageFinalizer)
	return msg
}

func messageFinalizer(msg *message) {
	C.pulsar_message_free(msg.ptr)
}

func (m *message) GetValue(v interface{}) error {
	return m.schema.Decode(m.Payload(), v)
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
	return timeFromUnixTimestampMillis(C.ulonglong(C.pulsar_message_get_publish_timestamp(m.ptr)))
}

func (m *message) EventTime() *time.Time {
	eventTime := C.pulsar_message_get_event_timestamp(m.ptr)
	if uint64(eventTime) == 0 {
		return nil
	} else {
		res := timeFromUnixTimestampMillis(C.ulonglong(eventTime))
		return &res
	}
}

func (m *message) Key() string {
	return C.GoString(C.pulsar_message_get_partitionKey(m.ptr))
}

func (m *message) Topic() string {
	return C.GoString(C.pulsar_message_get_topic_name(m.ptr))
}

//////// MessageID

func newMessageId(msg *C.pulsar_message_t) MessageID {
	msgId := &messageID{ptr: C.pulsar_message_get_message_id(msg)}
	runtime.SetFinalizer(msgId, messageIdFinalizer)
	return msgId
}

func getMessageId(messageId *C.pulsar_message_id_t) MessageID {
	msgId := &messageID{ptr: messageId}
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
	msgId := &messageID{ptr: C.pulsar_message_id_deserialize(unsafe.Pointer(&data[0]), C.uint32_t(len(data)))}
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
	ts := int64(timestamp) * int64(time.Millisecond)
	seconds := ts / int64(time.Second)
	nanos := ts - (seconds * int64(time.Second))
	return time.Unix(seconds, nanos)
}

func timeToUnixTimestampMillis(t time.Time) C.ulonglong {
	nanos := t.UnixNano()
	millis := nanos / int64(time.Millisecond)
	return C.ulonglong(millis)
}

func durationToUnixTimestampMillis(t time.Duration) C.ulonglong {
	return C.ulonglong(t.Milliseconds())
}
