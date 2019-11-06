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
	"context"
	"errors"
	"runtime"
	"time"
	"unsafe"
)

type createProducerCtx struct {
	client   *client
	schema   Schema
	callback func(producer Producer, err error)
	conf     *C.pulsar_producer_configuration_t
}

//export pulsarCreateProducerCallbackProxy
func pulsarCreateProducerCallbackProxy(res C.pulsar_result, ptr *C.pulsar_producer_t, ctx unsafe.Pointer) {
	producerCtx := restorePointer(ctx).(createProducerCtx)

	C.pulsar_producer_configuration_free(producerCtx.conf)

	if res != C.pulsar_result_Ok {
		producerCtx.callback(nil, newError(res, "Failed to create Producer"))
	} else {
		p := &producer{client: producerCtx.client, schema: producerCtx.schema, ptr: ptr}
		runtime.SetFinalizer(p, producerFinalizer)
		producerCtx.callback(p, nil)
	}
}

func createProducerAsync(client *client, schema Schema, options ProducerOptions, callback func(producer Producer, err error)) {
	if options.Topic == "" {
		go callback(nil, newError(C.pulsar_result_InvalidConfiguration, "topic is required when creating producer"))
		return
	}

	conf := C.pulsar_producer_configuration_create()

	if options.Name != "" {
		cName := C.CString(options.Name)
		defer C.free(unsafe.Pointer(cName))
		C.pulsar_producer_configuration_set_producer_name(conf, cName)
	}

	// If SendTimeout is 0, we'll leave the default configured value on C library
	if options.SendTimeout > 0 {
		timeoutMillis := options.SendTimeout.Nanoseconds() / int64(time.Millisecond)
		C.pulsar_producer_configuration_set_send_timeout(conf, C.int(timeoutMillis))
	} else if options.SendTimeout < 0 {
		// Set infinite publish timeout, which is specified as 0 in C API
		C.pulsar_producer_configuration_set_send_timeout(conf, C.int(0))
	}

	if options.MaxPendingMessages != 0 {
		C.pulsar_producer_configuration_set_max_pending_messages(conf, C.int(options.MaxPendingMessages))
	}

	if options.MaxPendingMessagesAcrossPartitions != 0 {
		C.pulsar_producer_configuration_set_max_pending_messages_across_partitions(conf, C.int(options.MaxPendingMessagesAcrossPartitions))
	}

	if options.BlockIfQueueFull {
		C.pulsar_producer_configuration_set_block_if_queue_full(conf, cBool(options.BlockIfQueueFull))
	}

	switch options.MessageRoutingMode {
	case RoundRobinDistribution:
		C.pulsar_producer_configuration_set_partitions_routing_mode(conf, C.pulsar_RoundRobinDistribution)
	case UseSinglePartition:
		C.pulsar_producer_configuration_set_partitions_routing_mode(conf, C.pulsar_UseSinglePartition)
	case CustomPartition:
		C.pulsar_producer_configuration_set_partitions_routing_mode(conf, C.pulsar_CustomPartition)
	}

	switch options.HashingScheme {
	case JavaStringHash:
		C.pulsar_producer_configuration_set_hashing_scheme(conf, C.pulsar_JavaStringHash)
	case Murmur3_32Hash:
		C.pulsar_producer_configuration_set_hashing_scheme(conf, C.pulsar_Murmur3_32Hash)
	case BoostHash:
		C.pulsar_producer_configuration_set_hashing_scheme(conf, C.pulsar_BoostHash)
	}

	if options.CompressionType != NoCompression {
		C.pulsar_producer_configuration_set_compression_type(conf, C.pulsar_compression_type(options.CompressionType))
	}

	if schema != nil && schema.GetSchemaInfo() != nil {
		if schema.GetSchemaInfo().Type != NONE {
			cName := C.CString(schema.GetSchemaInfo().Name)
			cSchema := C.CString(schema.GetSchemaInfo().Schema)
			properties := C.pulsar_string_map_create()
			defer C.free(unsafe.Pointer(cName))
			defer C.free(unsafe.Pointer(cSchema))
			defer C.pulsar_string_map_free(properties)

			for key, value := range schema.GetSchemaInfo().Properties {
				cKey := C.CString(key)
				cValue := C.CString(value)

				C.pulsar_string_map_put(properties, cKey, cValue)

				C.free(unsafe.Pointer(cKey))
				C.free(unsafe.Pointer(cValue))
			}
			C.pulsar_producer_configuration_set_schema_info(conf, C.pulsar_schema_type(schema.GetSchemaInfo().Type),
				cName, cSchema, properties)
		} else {
			cName := C.CString("BYTES")
			cSchema := C.CString("")
			properties := C.pulsar_string_map_create()
			defer C.free(unsafe.Pointer(cName))
			defer C.free(unsafe.Pointer(cSchema))
			defer C.pulsar_string_map_free(properties)

			for key, value := range schema.GetSchemaInfo().Properties {
				cKey := C.CString(key)
				cValue := C.CString(value)

				C.pulsar_string_map_put(properties, cKey, cValue)

				C.free(unsafe.Pointer(cKey))
				C.free(unsafe.Pointer(cValue))
			}
			C.pulsar_producer_configuration_set_schema_info(conf, C.pulsar_schema_type(BYTES),
				cName, cSchema, properties)
		}
	}

	if options.MessageRouter != nil {
		C._pulsar_producer_configuration_set_message_router(conf, savePointer(&options.MessageRouter))
	}

	C.pulsar_producer_configuration_set_batching_enabled(conf, cBool(options.Batching))

	if options.BatchingMaxPublishDelay != 0 {
		delayMillis := options.BatchingMaxPublishDelay.Nanoseconds() / int64(time.Millisecond)
		C.pulsar_producer_configuration_set_batching_max_publish_delay_ms(conf, C.ulong(delayMillis))
	}

	if options.BatchingMaxMessages != 0 {
		C.pulsar_producer_configuration_set_batching_max_messages(conf, C.uint(options.BatchingMaxMessages))
	}

	if options.Properties != nil {
		for key, value := range options.Properties {
			cKey := C.CString(key)
			cValue := C.CString(value)

			C.pulsar_producer_configuration_set_property(conf, cKey, cValue)

			C.free(unsafe.Pointer(cKey))
			C.free(unsafe.Pointer(cValue))
		}
	}

	topicName := C.CString(options.Topic)
	defer C.free(unsafe.Pointer(topicName))

	C._pulsar_client_create_producer_async(client.ptr, topicName, conf,
		savePointer(createProducerCtx{client, schema, callback, conf}))
}

type topicMetadata struct {
	numPartitions int
}

func (tm *topicMetadata) NumPartitions() int {
	return tm.numPartitions
}

//export pulsarRouterCallbackProxy
func pulsarRouterCallbackProxy(msg *C.pulsar_message_t, metadata *C.pulsar_topic_metadata_t, ctx unsafe.Pointer) C.int {
	router := restorePointerNoDelete(ctx).(*func(msg Message, metadata TopicMetadata) int)
	partitionIdx := (*router)(&message{ptr: msg}, &topicMetadata{int(C.pulsar_topic_metadata_get_num_partitions(metadata))})
	return C.int(partitionIdx)
}

/// Producer

type producer struct {
	client *client
	ptr    *C.pulsar_producer_t
	schema Schema
}

func producerFinalizer(p *producer) {
	C.pulsar_producer_free(p.ptr)
}

func (p *producer) Topic() string {
	return C.GoString(C.pulsar_producer_get_topic(p.ptr))
}

func (p *producer) Name() string {
	return C.GoString(C.pulsar_producer_get_producer_name(p.ptr))
}

func (p *producer) Schema() Schema {
	return p.schema
}

func (p *producer) LastSequenceID() int64 {
	return int64(C.pulsar_producer_get_last_sequence_id(p.ptr))
}

func (p *producer) Send(ctx context.Context, msg ProducerMessage) error {
	c := make(chan error, 1)
	p.SendAsync(ctx, msg, func(msg ProducerMessage, err error) { c <- err; close(c) })

	select {
	case <-ctx.Done():
		return ctx.Err()

	case cm := <-c:
		return cm
	}
}

type msgID struct {
	err error
	id  MessageID
}

func (p *producer) SendAndGetMsgID(ctx context.Context, msg ProducerMessage) (MessageID, error) {
	c := make(chan msgID, 10)

	p.SendAndGetMsgIDAsync(ctx, msg, func(id MessageID, err error) {
		tmpMsgID := msgID{
			err: err,
			id:  id,
		}
		c <- tmpMsgID
		close(c)
	})

	select {
	case <-ctx.Done():
		return nil, ctx.Err()

	case cm := <-c:
		return cm.id, cm.err
	}
}


type sendCallback struct {
	message  ProducerMessage
	callback func(ProducerMessage, error)
}

type sendCallbackWithMsgID struct {
	message  ProducerMessage
	callback func(MessageID, error)
}

//export pulsarProducerSendCallbackProxy
func pulsarProducerSendCallbackProxy(res C.pulsar_result, message *C.pulsar_message_t, ctx unsafe.Pointer) {
	sendCallback := restorePointer(ctx).(sendCallback)

	if res != C.pulsar_result_Ok {
		sendCallback.callback(sendCallback.message, newError(res, "Failed to send message"))
	} else {
		sendCallback.callback(sendCallback.message, nil)
	}
}

//export pulsarProducerSendCallbackProxyWithMsgID
func pulsarProducerSendCallbackProxyWithMsgID(res C.pulsar_result, messageId *C.pulsar_message_id_t, ctx unsafe.Pointer) {
	sendCallback := restorePointer(ctx).(sendCallbackWithMsgID)

	if res != C.pulsar_result_Ok {
		sendCallback.callback(getMessageId(messageId), newError(res, "Failed to send message"))
	} else {
		sendCallback.callback(getMessageId(messageId), nil)
	}
}

func (p *producer) SendAsync(ctx context.Context, msg ProducerMessage, callback func(ProducerMessage, error)) {
	if p.schema != nil {
		if msg.Value == nil {
			callback(msg, errors.New("message value is nil, please check"))
			return
		}
		payLoad, err := p.schema.Encode(msg.Value)
		if err != nil {
			callback(msg, errors.New("serialize message value error, please check"))
			return
		}
		msg.Payload = payLoad
	} else {
		if msg.Value != nil {
			callback(msg, errors.New("message value is set but no schema is provided, please check"))
			return
		}
	}
	cMsg := buildMessage(msg)
	defer C.pulsar_message_free(cMsg)

	C._pulsar_producer_send_async(p.ptr, cMsg, savePointer(sendCallback{message: msg, callback: callback}))
}

func (p *producer) SendAndGetMsgIDAsync(ctx context.Context, msg ProducerMessage, callback func(MessageID, error)) {
	if p.schema != nil {
		if msg.Value == nil {
			callback(nil, errors.New("message value is nil, please check"))
			return
		}
		payLoad, err := p.schema.Encode(msg.Value)
		if err != nil {
			callback(nil, errors.New("serialize message value error, please check"))
			return
		}
		msg.Payload = payLoad
	} else {
		if msg.Value != nil {
			callback(nil, errors.New("message value is set but no schema is provided, please check"))
			return
		}
	}
	cMsg := buildMessage(msg)
	defer C.pulsar_message_free(cMsg)

	C._pulsar_producer_send_async_msg_id(p.ptr, cMsg, savePointer(sendCallbackWithMsgID{message: msg, callback: callback}))
}

func (p *producer) Close() error {
	c := make(chan error, 1)
	p.CloseAsync(func(err error) { c <- err; close(c) })
	return <-c
}

func (p *producer) CloseAsync(callback func(error)) {
	C._pulsar_producer_close_async(p.ptr, savePointer(callback))
}

//export pulsarProducerCloseCallbackProxy
func pulsarProducerCloseCallbackProxy(res C.pulsar_result, ctx unsafe.Pointer) {
	callback := restorePointer(ctx).(func(error))

	if res != C.pulsar_result_Ok {
		callback(newError(res, "Failed to close Producer"))
	} else {
		callback(nil)
	}
}

func (p *producer) Flush() error {
	f := make(chan error, 1)
	p.FlushAsync(func(err error) {
		f <- err
		close(f)
	})
	return <-f
}

func (p *producer) FlushAsync(callback func(error)) {
	C._pulsar_producer_flush_async(p.ptr, savePointer(callback))
}

//export pulsarProducerFlushCallbackProxy
func pulsarProducerFlushCallbackProxy(res C.pulsar_result, ctx unsafe.Pointer) {
	callback := restorePointer(ctx).(func(error))

	if res != C.pulsar_result_Ok {
		callback(newError(res, "Failed to flush Producer"))
	} else {
		callback(nil)
	}
}
