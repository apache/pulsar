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
	"errors"
	"github.com/mattn/go-pointer"
	"runtime"
	"time"
	"unsafe"
)

/// ReaderBuilder

type readerBuilder struct {
	client         *client
	topic          string
	startMessageId *messageId
	ptr            *C.pulsar_reader_configuration_t
	reader         *reader
}

type reader struct {
	ptr            *C.pulsar_reader_t
	defaultChannel chan ReaderMessage
}

func readerBuilderFinalizer(cb *readerBuilder) {
	C.pulsar_reader_configuration_free(cb.ptr)
}

func newReaderBuilder(client *client) ReaderBuilder {
	builder := &readerBuilder{
		client: client,
		ptr:    C.pulsar_reader_configuration_create(),
		reader: &reader{ptr: nil},
	}

	runtime.SetFinalizer(builder, readerBuilderFinalizer)
	return builder
}

func (rb *readerBuilder) Create() (Reader, error) {
	c := make(chan struct {
		Reader
		error
	})

	rb.CreateAsync(func(reader Reader, err error) {
		c <- struct {
			Reader
			error
		}{reader, err}
		close(c)
	})

	res := <-c
	return res.Reader, res.error
}

func readerFinalizer(c *reader) {
	if c.ptr != nil {
		C.pulsar_reader_free(c.ptr)
	}
}

//export pulsarCreateReaderCallbackProxy
func pulsarCreateReaderCallbackProxy(res C.pulsar_result, ptr *C.pulsar_reader_t, ctx unsafe.Pointer) {
	cc := pointer.Restore(ctx).(*readerAndCallback)

	if res != C.pulsar_result_Ok {
		cc.callback(nil, NewError(res, "Failed to create Producer"))
	} else {
		cc.reader.ptr = ptr
		runtime.SetFinalizer(cc.reader, readerFinalizer)
		cc.callback(cc.reader, nil)
	}
}

type readerAndCallback struct {
	reader   *reader
	callback func(Reader, error)
}

func (rb *readerBuilder) CreateAsync(callback func(Reader, error)) {
	if rb.topic == "" {
		callback(nil, errors.New("topic is required"))
		return
	}

	if rb.startMessageId == nil {
		rb.startMessageId = messageIdLatest()
		return
	}

	if C.pulsar_reader_configuration_has_reader_listener(rb.ptr) == 0 {
		// If there is no message listener, set a default channel so that we can have receive to
		// use that
		rb.reader.defaultChannel = make(chan ReaderMessage)
		rb.ReaderListener(rb.reader.defaultChannel)
	}

	topic := C.CString(rb.topic)
	defer C.free(unsafe.Pointer(topic))
	C._pulsar_client_create_reader_async(rb.client.ptr, topic, rb.startMessageId.ptr,
		rb.ptr, pointer.Save(&readerAndCallback{rb.reader, callback}))
}

func (rb *readerBuilder) Topic(topic string) ReaderBuilder {
	rb.topic = topic
	return rb
}

type readerCallback struct {
	reader  Reader
	channel chan ReaderMessage
}

//export pulsarReaderListenerProxy
func pulsarReaderListenerProxy(cReader *C.pulsar_reader_t, message *C.pulsar_message_t, ctx unsafe.Pointer) {
	rc := pointer.Restore(ctx).(*readerCallback)
	rc.channel <- ReaderMessage{rc.reader, newMessageWrapper(message)}
}

func (rb *readerBuilder) ReaderListener(listener chan ReaderMessage) ReaderBuilder {
	C._pulsar_reader_configuration_set_reader_listener(rb.ptr, pointer.Save(&readerCallback{
		reader:  rb.reader,
		channel: listener,
	}))
	return rb
}

func (rb *readerBuilder) ReceiverQueueSize(receiverQueueSize int) ReaderBuilder {
	C.pulsar_reader_configuration_set_receiver_queue_size(rb.ptr, C.int(receiverQueueSize))
	return rb
}

func (rb *readerBuilder) ReaderName(readerName string) ReaderBuilder {
	name := C.CString(readerName)
	defer C.free(unsafe.Pointer(name))

	C.pulsar_reader_configuration_set_reader_name(rb.ptr, name)
	return rb
}

func (rb *readerBuilder) StartFromEarliest() ReaderBuilder {
	rb.startMessageId = messageIdEarliest()
	return rb
}

func (rb *readerBuilder) StartFromLatest() ReaderBuilder {
	rb.startMessageId = messageIdLatest()
	return rb
}

func (rb *readerBuilder) StartMessageId(startMessageId MessageId) ReaderBuilder {
	rb.startMessageId = startMessageId.(*messageId)
	return rb
}

func (rb *readerBuilder) SubscriptionRolePrefix(subscriptionRolePrefix string) ReaderBuilder {
	prefix := C.CString(subscriptionRolePrefix)
	defer C.free(unsafe.Pointer(prefix))
	C.pulsar_reader_configuration_set_subscription_role_prefix(rb.ptr, prefix)
	return rb
}

//// Consumer

func (r *reader) Topic() string {
	return C.GoString(C.pulsar_reader_get_topic(r.ptr))
}

func (r *reader) ReadNext() (Message, error) {
	rm := <-r.defaultChannel
	return rm.Message, nil
}

func (r *reader) ReadNextWithTimeout(timeoutMillis int) (Message, error) {
	select {
	case rm := <-r.defaultChannel:
		return rm.Message, nil
	case <-time.After(time.Duration(timeoutMillis) * time.Millisecond):
		return nil, NewError(C.pulsar_result_Timeout, "Timeout on reader read")
	}
}

func (r *reader) Close() error {
	channel := make(chan error)
	r.CloseAsync(func(err error) { channel <- err; close(channel) })
	return <-channel
}

func (r *reader) CloseAsync(callback Callback) {
	if r.defaultChannel != nil {
		close(r.defaultChannel)
	}

	C._pulsar_reader_close_async(r.ptr, pointer.Save(callback))
}

//export pulsarReaderCloseCallbackProxy
func pulsarReaderCloseCallbackProxy(res C.pulsar_result, ctx unsafe.Pointer) {
	callback := pointer.Restore(ctx).(func(err error))

	if res != C.pulsar_result_Ok {
		callback(NewError(res, "Failed to close Reader"))
	} else {
		callback(nil)
	}
}
