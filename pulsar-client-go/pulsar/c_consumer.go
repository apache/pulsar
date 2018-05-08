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
	"fmt"
	"github.com/mattn/go-pointer"
	"runtime"
	"time"
	"unsafe"
)

/// ConsumerBuilder

type consumerBuilder struct {
	client   *client
	topic    string
	subName  string
	ptr      *C.pulsar_consumer_configuration_t
	consumer *consumer
}

type consumer struct {
	ptr            *C.pulsar_consumer_t
	defaultChannel chan ConsumerMessage
}

func consumerBuilderFinalizer(cb *consumerBuilder) {
	C.pulsar_consumer_configuration_free(cb.ptr)
}

func newConsumerBuilder(client *client) ConsumerBuilder {
	builder := &consumerBuilder{
		client:   client,
		ptr:      C.pulsar_consumer_configuration_create(),
		consumer: &consumer{ptr: nil},
	}

	runtime.SetFinalizer(builder, consumerBuilderFinalizer)
	return builder
}

func (cb *consumerBuilder) Subscribe() (Consumer, error) {
	c := make(chan struct {
		Consumer
		error
	})

	cb.SubscribeAsync(func(consumer Consumer, err error) {
		c <- struct {
			Consumer
			error
		}{consumer, err}
		close(c)
	})

	res := <-c
	return res.Consumer, res.error
}

func consumerFinalizer(c *consumer) {
	if c.ptr != nil {
		C.pulsar_consumer_free(c.ptr)
	}
}

//export pulsarSubscribeCallbackProxy
func pulsarSubscribeCallbackProxy(res C.pulsar_result, ptr *C.pulsar_consumer_t, ctx unsafe.Pointer) {
	cc := pointer.Restore(ctx).(*consumerAndCallback)

	if res != C.pulsar_result_Ok {
		cc.callback(nil, NewError(res, "Failed to create Producer"))
	} else {
		cc.consumer.ptr = ptr
		runtime.SetFinalizer(cc.consumer, consumerFinalizer)
		cc.callback(cc.consumer, nil)
	}
}

type consumerAndCallback struct {
	consumer *consumer
	callback func(Consumer, error)
}

func (cb *consumerBuilder) SubscribeAsync(callback func(Consumer, error)) {
	if cb.topic == "" {
		callback(nil, errors.New("topic is required"))
		return
	}

	if cb.subName == "" {
		callback(nil, errors.New("subscription name is required"))
		return
	}

	if C.pulsar_consumer_configuration_has_message_listener(cb.ptr) == 0 {
		fmt.Println("Adding default listener")
		// If there is no message listener, set a default channel so that we can have receive to
		// use that
		cb.consumer.defaultChannel = make(chan ConsumerMessage)
		cb.MessageListener(cb.consumer.defaultChannel)
	}

	topic := C.CString(cb.topic)
	subName := C.CString(cb.subName)
	defer C.free(unsafe.Pointer(topic))
	defer C.free(unsafe.Pointer(subName))
	C._pulsar_client_subscribe_async(cb.client.ptr, topic, subName,
		cb.ptr, pointer.Save(&consumerAndCallback{cb.consumer, callback}))
}

func (cb *consumerBuilder) Topic(topic string) ConsumerBuilder {
	cb.topic = topic
	return cb
}

func (cb *consumerBuilder) SubscriptionName(subscriptionName string) ConsumerBuilder {
	cb.subName = subscriptionName
	return cb
}

func (cb *consumerBuilder) AckTimeout(ackTimeoutMillis int64) ConsumerBuilder {
	C.pulsar_consumer_set_unacked_messages_timeout_ms(cb.ptr, C.ulonglong(ackTimeoutMillis))
	return cb
}

func (cb *consumerBuilder) SubscriptionType(subscriptionType SubscriptionType) ConsumerBuilder {
	C.pulsar_consumer_configuration_set_consumer_type(cb.ptr, C.pulsar_consumer_type(subscriptionType))
	return cb
}

type consumerCallback struct {
	consumer Consumer
	channel  chan ConsumerMessage
}

//export pulsarMessageListenerProxy
func pulsarMessageListenerProxy(cConsumer *C.pulsar_consumer_t, message *C.pulsar_message_t, ctx unsafe.Pointer) {
	cc := pointer.Restore(ctx).(*consumerCallback)
	cc.channel <- ConsumerMessage{cc.consumer, newMessageWrapper(message)}
}

func (cb *consumerBuilder) MessageListener(listener chan ConsumerMessage) ConsumerBuilder {
	C._pulsar_consumer_configuration_set_message_listener(cb.ptr, pointer.Save(&consumerCallback{
		consumer: cb.consumer,
		channel:  listener,
	}))
	return cb
}

func (cb *consumerBuilder) ReceiverQueueSize(receiverQueueSize int) ConsumerBuilder {
	C.pulsar_consumer_configuration_set_receiver_queue_size(cb.ptr, C.int(receiverQueueSize))
	return cb
}

func (cb *consumerBuilder) MaxTotalReceiverQueueSizeAcrossPartitions(maxTotalReceiverQueueSizeAcrossPartitions int) ConsumerBuilder {
	C.pulsar_consumer_set_max_total_receiver_queue_size_across_partitions(cb.ptr, C.int(maxTotalReceiverQueueSizeAcrossPartitions))
	return cb
}

func (cb *consumerBuilder) ConsumerName(consumerName string) ConsumerBuilder {
	name := C.CString(consumerName)
	defer C.free(unsafe.Pointer(name))

	C.pulsar_consumer_set_consumer_name(cb.ptr, name)
	return cb
}

//// Consumer

func (c *consumer) Topic() string {
	return C.GoString(C.pulsar_consumer_get_topic(c.ptr))
}

func (c *consumer) Subscription() string {
	return C.GoString(C.pulsar_consumer_get_subscription_name(c.ptr))
}

func (c *consumer) Unsubscribe() error {
	channel := make(chan error)
	c.UnsubscribeAsync(func(err error) { channel <- err; close(channel) })
	return <-channel
}

func (c *consumer) UnsubscribeAsync(callback Callback) {
	C._pulsar_consumer_unsubscribe_async(c.ptr, pointer.Save(callback))
}

//export pulsarConsumerUnsubscribeCallbackProxy
func pulsarConsumerUnsubscribeCallbackProxy(res C.pulsar_result, ctx unsafe.Pointer) {
	callback := pointer.Restore(ctx).(func(err error))

	if res != C.pulsar_result_Ok {
		callback(NewError(res, "Failed to unsubscribe consumer"))
	} else {
		callback(nil)
	}
}

func (c *consumer) Receive() (Message, error) {
	cm := <-c.defaultChannel
	return cm.Message, nil
}

func (c *consumer) ReceiveWithTimeout(timeoutMillis int) (Message, error) {
	select {
	case cm := <-c.defaultChannel:
		return cm.Message, nil
	case <-time.After(time.Duration(timeoutMillis) * time.Millisecond):
		return nil, NewError(C.pulsar_result_Timeout, "Timeout on consumer receive")
	}
}

func (c *consumer) Acknowledge(msg Message) error {
	C.pulsar_consumer_acknowledge_async(c.ptr, msg.(*message).ptr, nil, nil)
	return nil
}

func (c *consumer) AcknowledgeId(msgId MessageId) error {
	C.pulsar_consumer_acknowledge_async_id(c.ptr, msgId.(*messageId).ptr, nil, nil)
	return nil
}

func (c *consumer) AcknowledgeCumulative(msg Message) error {
	C.pulsar_consumer_acknowledge_cumulative_async(c.ptr, msg.(*message).ptr, nil, nil)
	return nil
}

func (c *consumer) AcknowledgeCumulativeId(msgId MessageId) error {
	C.pulsar_consumer_acknowledge_cumulative_async_id(c.ptr, msgId.(*messageId).ptr, nil, nil)
	return nil
}

func (c *consumer) Close() error {
	channel := make(chan error)
	c.CloseAsync(func(err error) { channel <- err; close(channel) })
	return <-channel
}

func (c *consumer) CloseAsync(callback Callback) {
	if c.defaultChannel != nil {
		close(c.defaultChannel)
	}

	C._pulsar_consumer_close_async(c.ptr, pointer.Save(callback))
}

//export pulsarConsumerCloseCallbackProxy
func pulsarConsumerCloseCallbackProxy(res C.pulsar_result, ctx unsafe.Pointer) {
	callback := pointer.Restore(ctx).(func(err error))

	if res != C.pulsar_result_Ok {
		callback(NewError(res, "Failed to close Consumer"))
	} else {
		callback(nil)
	}
}

func (c *consumer) RedeliverUnacknowledgedMessages() {
	C.pulsar_consumer_redeliver_unacknowledged_messages(c.ptr)
}
