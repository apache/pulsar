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
	"runtime"
	"unsafe"
	"context"
)

type reader struct {
	schema         Schema
	client         *client
	ptr            *C.pulsar_reader_t
	defaultChannel chan ReaderMessage
}

func readerFinalizer(c *reader) {
	if c.ptr != nil {
		C.pulsar_reader_free(c.ptr)
	}
}

//export pulsarCreateReaderCallbackProxy
func pulsarCreateReaderCallbackProxy(res C.pulsar_result, ptr *C.pulsar_reader_t, ctx unsafe.Pointer) {
	cc := restorePointer(ctx).(*readerAndCallback)

	C.pulsar_reader_configuration_free(cc.conf)

	if res != C.pulsar_result_Ok {
		cc.callback(nil, newError(res, "Failed to create Reader"))
	} else {
		cc.reader.ptr = ptr
		cc.reader.schema = cc.schema
		runtime.SetFinalizer(cc.reader, readerFinalizer)
		cc.callback(cc.reader, nil)
	}
}

type readerAndCallback struct {
	schema   Schema
	reader   *reader
	conf     *C.pulsar_reader_configuration_t
	callback func(Reader, error)
}

func createReaderAsync(client *client, schema Schema, options ReaderOptions, callback func(Reader, error)) {
	if options.Topic == "" {
		go callback(nil, newError(C.pulsar_result_InvalidConfiguration, "topic is required"))
		return
	}

	if options.StartMessageID == nil {
		go callback(nil, newError(C.pulsar_result_InvalidConfiguration, "start message id is required"))
		return
	}

	reader := &reader{client: client}

	if options.MessageChannel == nil {
		// If there is no message listener, set a default channel so that we can have receive to
		// use that
		reader.defaultChannel = make(chan ReaderMessage)
		options.MessageChannel = reader.defaultChannel
	}

	conf := C.pulsar_reader_configuration_create()

	C._pulsar_reader_configuration_set_reader_listener(conf, savePointer(&readerCallback{
		reader:  reader,
		channel: options.MessageChannel,
	}))

	if options.ReceiverQueueSize != 0 {
		C.pulsar_reader_configuration_set_receiver_queue_size(conf, C.int(options.ReceiverQueueSize))
	}

	if options.SubscriptionRolePrefix != "" {
		prefix := C.CString(options.SubscriptionRolePrefix)
		defer C.free(unsafe.Pointer(prefix))
		C.pulsar_reader_configuration_set_subscription_role_prefix(conf, prefix)
	}

	C.pulsar_reader_configuration_set_read_compacted(conf, cBool(options.ReadCompacted))

	if options.Name != "" {
		name := C.CString(options.Name)
		defer C.free(unsafe.Pointer(name))

		C.pulsar_reader_configuration_set_reader_name(conf, name)
	}

	topic := C.CString(options.Topic)
	defer C.free(unsafe.Pointer(topic))

	C._pulsar_client_create_reader_async(client.ptr, topic, options.StartMessageID.(*messageID).ptr,
		conf, savePointer(&readerAndCallback{schema: schema, reader: reader, conf: conf, callback: callback}))
}

type readerCallback struct {
	reader  Reader
	channel chan ReaderMessage
}

//export pulsarReaderListenerProxy
func pulsarReaderListenerProxy(cReader *C.pulsar_reader_t, message *C.pulsar_message_t, ctx unsafe.Pointer) {
	rc := restorePointerNoDelete(ctx).(*readerCallback)

	defer func() {
		ex := recover()
		if ex != nil {
			// There was an error when sending channel (eg: already closed)
		}
	}()

	rc.channel <- ReaderMessage{rc.reader, newMessageWrapper(rc.reader.Schema(), message)}
}

func (r *reader) Topic() string {
	return C.GoString(C.pulsar_reader_get_topic(r.ptr))
}

func (r *reader) Schema() Schema {
	return r.schema
}

func (r *reader) Next(ctx context.Context) (Message, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()

	case rm := <-r.defaultChannel:
		return rm.Message, nil
	}
}

func (r *reader) HasNext() (bool, error) {
	value := C.int(0)
	res := C.pulsar_reader_has_message_available(r.ptr, &value)

	if res != C.pulsar_result_Ok {
		return false, newError(res, "Failed to check if next message is available")
	} else if value == C.int(1) {
		return true, nil
	} else {
		return false, nil
	}
}

func (r *reader) Close() error {
	channel := make(chan error, 1)
	r.CloseAsync(func(err error) { channel <- err; close(channel) })
	return <-channel
}

func (r *reader) CloseAsync(callback func(error)) {
	if r.defaultChannel != nil {
		close(r.defaultChannel)
	}

	C._pulsar_reader_close_async(r.ptr, savePointer(callback))
}

//export pulsarReaderCloseCallbackProxy
func pulsarReaderCloseCallbackProxy(res C.pulsar_result, ctx unsafe.Pointer) {
	callback := restorePointer(ctx).(func(err error))

	if res != C.pulsar_result_Ok {
		callback(newError(res, "Failed to close Reader"))
	} else {
		callback(nil)
	}
}
