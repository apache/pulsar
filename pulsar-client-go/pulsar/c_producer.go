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
	"runtime"
	"unsafe"

	"errors"
	"github.com/mattn/go-pointer"
)

/// ProducerBuilder

type producerBuilder struct {
	client *client
	topic  string
	ptr    *C.pulsar_producer_configuration_t
}

func producerBuilderFinalizer(cb *producerBuilder) {
	C.pulsar_producer_configuration_free(cb.ptr)
}

func newProducerBuilder(client *client) ProducerBuilder {
	builder := producerBuilder{
		client: client,
		ptr:    C.pulsar_producer_configuration_create(),
	}

	runtime.SetFinalizer(&builder, producerBuilderFinalizer)
	return &builder
}

func (pb *producerBuilder) Create() (Producer, error) {
	// Create is implemented on async create with a channel to wait for
	// completion without blocking the real thread
	c := make(chan struct {
		Producer
		error
	})

	pb.CreateAsync(func(producer Producer, err error) {
		c <- struct {
			Producer
			error
		}{producer, err}
		close(c)
	})

	res := <-c
	return res.Producer, res.error
}

//export pulsarCreateProducerCallbackProxy
func pulsarCreateProducerCallbackProxy(res C.pulsar_result, ptr *C.pulsar_producer_t, ctx unsafe.Pointer) {
	callback := pointer.Restore(ctx).(func(producer Producer, err error))

	if res != C.pulsar_result_Ok {
		callback(nil, NewError(res, "Failed to create Producer"))
	} else {
		p := &producer{ptr: ptr}
		runtime.SetFinalizer(p, producerFinalizer)
		callback(p, nil)
	}
}

func (pb *producerBuilder) CreateAsync(callback func(producer Producer, err error)) {
	if pb.topic == "" {
		callback(nil, errors.New("topic is required"))
		return
	}

	C._pulsar_client_create_producer_async(pb.client.ptr, C.CString(pb.topic), pb.ptr, pointer.Save(callback))
}

func (pb *producerBuilder) Topic(topic string) ProducerBuilder {
	pb.topic = topic
	return pb
}

func (pb *producerBuilder) ProducerName(producerName string) ProducerBuilder {
	cName := C.CString(producerName)
	defer C.free(unsafe.Pointer(cName))
	C.pulsar_producer_configuration_set_producer_name(pb.ptr, cName)
	return pb
}

func (pb *producerBuilder) SendTimeout(sendTimeoutMillis int) ProducerBuilder {
	C.pulsar_producer_configuration_set_send_timeout(pb.ptr, C.int(sendTimeoutMillis))
	return pb
}

func (pb *producerBuilder) MaxPendingMessages(maxPendingMessages int) ProducerBuilder {
	C.pulsar_producer_configuration_set_max_pending_messages(pb.ptr, C.int(maxPendingMessages))
	return pb
}

func (pb *producerBuilder) MaxPendingMessagesAcrossPartitions(maxPendingMessagesAcrossPartitions int) ProducerBuilder {
	C.pulsar_producer_configuration_set_max_pending_messages_across_partitions(pb.ptr, C.int(maxPendingMessagesAcrossPartitions))
	return pb
}

func (pb *producerBuilder) BlockIfQueueFull(blockIfQueueFull bool) ProducerBuilder {
	C.pulsar_producer_configuration_set_block_if_queue_full(pb.ptr, cBool(blockIfQueueFull))
	return pb
}

func (pb *producerBuilder) MessageRoutingMode(messageRoutingMode MessageRoutingMode) ProducerBuilder {
	C.pulsar_producer_configuration_set_partitions_routing_mode(pb.ptr, C.pulsar_partitions_routing_mode(messageRoutingMode))
	return pb
}

func (pb *producerBuilder) HashingScheme(hashingScheme HashingScheme) ProducerBuilder {
	C.pulsar_producer_configuration_set_hashing_scheme(pb.ptr, C.pulsar_hashing_scheme(hashingScheme))
	return pb
}

func (pb *producerBuilder) CompressionType(compressionType CompressionType) ProducerBuilder {
	C.pulsar_producer_configuration_set_compression_type(pb.ptr, C.pulsar_compression_type(compressionType))
	return pb
}

type topicMetadata struct {
	numPartitions int
}

func (tm *topicMetadata) NumPartitions() int {
	return tm.NumPartitions()
}

//export pulsarRouterCallbackProxy
func pulsarRouterCallbackProxy(msg *C.pulsar_message_t, metadata *C.pulsar_topic_metadata_t, ctx unsafe.Pointer) C.int {
	router := pointer.Restore(ctx).(*func(msg Message, metadata TopicMetadata) int)

	partitionIdx := (*router)(&message{msg}, &topicMetadata{C.pulsar_topic_metadata_get_num_partitions(metadata)})
	return partitionIdx
}

func (pb *producerBuilder) MessageRouter(messageRouter func(msg Message, metadata TopicMetadata) int) ProducerBuilder {
	C._pulsar_producer_configuration_set_message_router(pb.ptr, unsafe.Pointer(&messageRouter))
	return pb
}

func (pb *producerBuilder) EnableBatching(enableBatching bool) ProducerBuilder {
	C.pulsar_producer_configuration_set_batching_enabled(pb.ptr, cBool(enableBatching))
	return pb
}

func (pb *producerBuilder) BatchingMaxPublishDelay(batchDelayMillis uint64) ProducerBuilder {
	C.pulsar_producer_configuration_set_batching_max_publish_delay_ms(pb.ptr, C.ulong(batchDelayMillis))
	return pb
}

func (pb *producerBuilder) BatchingMaxMessages(batchMessagesMaxMessagesPerBatch uint) ProducerBuilder {
	C.pulsar_producer_configuration_set_batching_max_messages(pb.ptr, C.uint(batchMessagesMaxMessagesPerBatch))
	return pb
}

func (pb *producerBuilder) InitialSequenceId(initialSequenceId int64) ProducerBuilder {
	C.pulsar_producer_configuration_set_initial_sequence_id(pb.ptr, C.longlong(initialSequenceId))
	return pb
}

/// Producer

type producer struct {
	ptr *C.pulsar_producer_t
}

func producerFinalizer(p *producer) {
	C.pulsar_producer_free(p.ptr)
}

func (p *producer) Topic() string {
	return C.GoString(C.pulsar_producer_get_topic(p.ptr))
}

func (p *producer) ProducerName() string {
	return C.GoString(C.pulsar_producer_get_producer_name(p.ptr))
}

func (p *producer) SendBytes(payload []byte) error {
	return p.Send(NewMessage().Payload(payload).Build())
}

func (p *producer) SendBytesAsync(payload []byte, callback Callback) {
	p.SendAsync(NewMessage().Payload(payload).Build(), callback)
}

func (p *producer) Send(msg Message) error {
	c := make(chan error)
	p.SendAsync(msg, func(err error) { c <- err; close(c) })
	return <-c
}

//export pulsarProducerSendCallbackProxy
func pulsarProducerSendCallbackProxy(res C.pulsar_result, message *C.pulsar_message_t, ctx unsafe.Pointer) {
	callback := pointer.Restore(ctx).(Callback)

	if res != C.pulsar_result_Ok {
		callback(NewError(res, "Failed to send message"))
	} else {
		callback(nil)
	}
}

func (p *producer) SendAsync(msg Message, callback Callback) {
	cMsg := msg.(*message)

	C._pulsar_producer_send_async(p.ptr, cMsg.ptr, pointer.Save(callback))
}

func (p *producer) Close() error {
	c := make(chan error)
	p.CloseAsync(func(err error) { c <- err; close(c) })
	return <-c
}

func (p *producer) CloseAsync(callback Callback) {
	C._pulsar_producer_close_async(p.ptr, pointer.Save(callback))
}

//export pulsarProducerCloseCallbackProxy
func pulsarProducerCloseCallbackProxy(res C.pulsar_result, ctx unsafe.Pointer) {
	callback := pointer.Restore(ctx).(Callback)

	if res != C.pulsar_result_Ok {
		callback(NewError(res, "Failed to close Producer"))
	} else {
		callback(nil)
	}
}
