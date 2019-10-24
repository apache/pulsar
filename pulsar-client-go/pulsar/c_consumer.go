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
	"runtime"
	"time"
	"unsafe"
)

type consumer struct {
	schema         Schema
	client         *client
	ptr            *C.pulsar_consumer_t
	defaultChannel chan ConsumerMessage
}

func consumerFinalizer(c *consumer) {
	if c.ptr != nil {
		C.pulsar_consumer_free(c.ptr)
	}
}

//export pulsarSubscribeCallbackProxy
func pulsarSubscribeCallbackProxy(res C.pulsar_result, ptr *C.pulsar_consumer_t, ctx unsafe.Pointer) {
	cc := restorePointer(ctx).(*subscribeContext)

	C.pulsar_consumer_configuration_free(cc.conf)

	if res != C.pulsar_result_Ok {
		cc.callback(nil, newError(res, "Failed to subscribe to topic"))
	} else {
		cc.consumer.ptr = ptr
		cc.consumer.schema = cc.schema
		runtime.SetFinalizer(cc.consumer, consumerFinalizer)
		cc.callback(cc.consumer, nil)
	}
}

type subscribeContext struct {
	schema   Schema
	conf     *C.pulsar_consumer_configuration_t
	consumer *consumer
	callback func(Consumer, error)
}

func subscribeAsync(client *client, options ConsumerOptions, schema Schema, callback func(Consumer, error)) {
	if options.Topic == "" && options.Topics == nil && options.TopicsPattern == "" {
		go callback(nil, newError(C.pulsar_result_InvalidConfiguration, "topic is required"))
		return
	}

	if options.SubscriptionName == "" {
		go callback(nil, newError(C.pulsar_result_InvalidConfiguration, "subscription name is required"))
		return
	}

	conf := C.pulsar_consumer_configuration_create()

	consumer := &consumer{client: client}

	if options.MessageChannel == nil {
		// If there is no message listener, set a default channel so that we can have receive to
		// use that
		consumer.defaultChannel = make(chan ConsumerMessage)
		options.MessageChannel = consumer.defaultChannel
	}

	C._pulsar_consumer_configuration_set_message_listener(conf, savePointer(&consumerCallback{
		consumer: consumer,
		channel:  options.MessageChannel,
	}))

	if options.AckTimeout != 0 {
		timeoutMillis := options.AckTimeout.Nanoseconds() / int64(time.Millisecond)
		C.pulsar_consumer_set_unacked_messages_timeout_ms(conf, C.uint64_t(timeoutMillis))
	}

	if options.NackRedeliveryDelay != nil {
		delayMillis := options.NackRedeliveryDelay.Nanoseconds() / int64(time.Millisecond)
		C.pulsar_configure_set_negative_ack_redelivery_delay_ms(conf, C.long(delayMillis))
	}

	if options.Type != Exclusive {
		C.pulsar_consumer_configuration_set_consumer_type(conf, C.pulsar_consumer_type(options.Type))
	}

	if options.SubscriptionInitPos != Latest {
		C.pulsar_consumer_set_subscription_initial_position(conf, C.initial_position(options.SubscriptionInitPos))
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
			C.pulsar_consumer_configuration_set_schema_info(conf, C.pulsar_schema_type(schema.GetSchemaInfo().Type),
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
			C.pulsar_consumer_configuration_set_schema_info(conf, C.pulsar_schema_type(BYTES),
				cName, cSchema, properties)
		}
	}

	// ReceiverQueueSize==0 means to use the default queue size
	// -1 means to disable the consumer prefetching
	if options.ReceiverQueueSize > 0 {
		C.pulsar_consumer_configuration_set_receiver_queue_size(conf, C.int(options.ReceiverQueueSize))
	} else if options.ReceiverQueueSize < 0 {
		// In C++ client lib, 0 means disable prefetching
		C.pulsar_consumer_configuration_set_receiver_queue_size(conf, C.int(0))
	}

	if options.MaxTotalReceiverQueueSizeAcrossPartitions != 0 {
		C.pulsar_consumer_set_max_total_receiver_queue_size_across_partitions(conf,
			C.int(options.MaxTotalReceiverQueueSizeAcrossPartitions))
	}

	if options.Name != "" {
		name := C.CString(options.Name)
		defer C.free(unsafe.Pointer(name))

		C.pulsar_consumer_set_consumer_name(conf, name)
	}

	if options.Properties != nil {
		for key, value := range options.Properties {
			cKey := C.CString(key)
			cValue := C.CString(value)

			C.pulsar_consumer_configuration_set_property(conf, cKey, cValue)

			C.free(unsafe.Pointer(cKey))
			C.free(unsafe.Pointer(cValue))
		}
	}

	C.pulsar_consumer_set_read_compacted(conf, cBool(options.ReadCompacted))

	subName := C.CString(options.SubscriptionName)
	defer C.free(unsafe.Pointer(subName))

	callbackPtr := savePointer(&subscribeContext{schema: schema, conf: conf, consumer: consumer, callback: callback})

	if options.Topic != "" {
		topic := C.CString(options.Topic)
		defer C.free(unsafe.Pointer(topic))
		C._pulsar_client_subscribe_async(client.ptr, topic, subName, conf, callbackPtr)
	} else if options.Topics != nil {
		cArray := C.malloc(C.size_t(len(options.Topics)) * C.size_t(unsafe.Sizeof(uintptr(0))))

		// convert the C array to a Go Array so we can index it
		a := (*[1<<30 - 1]*C.char)(cArray)

		for idx, topic := range options.Topics {
			a[idx] = C.CString(topic)
		}

		C._pulsar_client_subscribe_multi_topics_async(client.ptr, (**C.char)(cArray), C.int(len(options.Topics)),
			subName, conf, callbackPtr)

		for idx := range options.Topics {
			C.free(unsafe.Pointer(a[idx]))
		}

		C.free(cArray)
	} else if options.TopicsPattern != "" {
		topicsPattern := C.CString(options.TopicsPattern)
		defer C.free(unsafe.Pointer(topicsPattern))
		C._pulsar_client_subscribe_pattern_async(client.ptr, topicsPattern, subName, conf, callbackPtr)
	}
}

type consumerCallback struct {
	consumer Consumer
	channel  chan ConsumerMessage
}

//export pulsarMessageListenerProxy
func pulsarMessageListenerProxy(cConsumer *C.pulsar_consumer_t, message *C.pulsar_message_t, ctx unsafe.Pointer) {
	cc := restorePointerNoDelete(ctx).(*consumerCallback)

	defer func() {
		ex := recover()
		if ex != nil {
			// There was an error when sending channel (eg: already closed)
		}
	}()
	cc.channel <- ConsumerMessage{cc.consumer, newMessageWrapper(cc.consumer.Schema(), message)}
}

//// Consumer

func (c *consumer) Schema() Schema {
	return c.schema
}

func (c *consumer) Topic() string {
	return C.GoString(C.pulsar_consumer_get_topic(c.ptr))
}

func (c *consumer) Subscription() string {
	return C.GoString(C.pulsar_consumer_get_subscription_name(c.ptr))
}

func (c *consumer) Unsubscribe() error {
	channel := make(chan error, 1)
	c.UnsubscribeAsync(func(err error) {
		channel <- err
		close(channel)
	})
	return <-channel
}

func (c *consumer) UnsubscribeAsync(callback func(error)) {
	C._pulsar_consumer_unsubscribe_async(c.ptr, savePointer(callback))
}

//export pulsarConsumerUnsubscribeCallbackProxy
func pulsarConsumerUnsubscribeCallbackProxy(res C.pulsar_result, ctx unsafe.Pointer) {
	callback := restorePointer(ctx).(func(err error))

	if res != C.pulsar_result_Ok {
		go callback(newError(res, "Failed to unsubscribe consumer"))
	} else {
		go callback(nil)
	}
}

func (c *consumer) Receive(ctx context.Context) (Message, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()

	case cm := <-c.defaultChannel:
		return cm.Message, nil
	}
}

func (c *consumer) Ack(msg Message) error {
	C.pulsar_consumer_acknowledge_async(c.ptr, msg.(*message).ptr, nil, nil)
	return nil
}

func (c *consumer) AckID(msgId MessageID) error {
	C.pulsar_consumer_acknowledge_async_id(c.ptr, msgId.(*messageID).ptr, nil, nil)
	return nil
}

func (c *consumer) AckCumulative(msg Message) error {
	C.pulsar_consumer_acknowledge_cumulative_async(c.ptr, msg.(*message).ptr, nil, nil)
	return nil
}

func (c *consumer) AckCumulativeID(msgId MessageID) error {
	C.pulsar_consumer_acknowledge_cumulative_async_id(c.ptr, msgId.(*messageID).ptr, nil, nil)
	return nil
}

func (c *consumer) Nack(msg Message) error {
	C.pulsar_consumer_negative_acknowledge(c.ptr, msg.(*message).ptr)
	return nil
}

func (c *consumer) NackID(msgId MessageID) error {
	C.pulsar_consumer_negative_acknowledge_id(c.ptr, msgId.(*messageID).ptr)
	return nil
}

func (c *consumer) Close() error {
	channel := make(chan error, 1)
	c.CloseAsync(func(err error) { channel <- err; close(channel) })
	return <-channel
}

func (c *consumer) CloseAsync(callback func(error)) {
	if c.defaultChannel != nil {
		close(c.defaultChannel)
	}

	C._pulsar_consumer_close_async(c.ptr, savePointer(callback))
}

//export pulsarConsumerCloseCallbackProxy
func pulsarConsumerCloseCallbackProxy(res C.pulsar_result, ctx unsafe.Pointer) {
	callback := restorePointer(ctx).(func(err error))

	if res != C.pulsar_result_Ok {
		go callback(newError(res, "Failed to close Consumer"))
	} else {
		go callback(nil)
	}
}

func (c *consumer) RedeliverUnackedMessages() {
	C.pulsar_consumer_redeliver_unacknowledged_messages(c.ptr)
}

func (c *consumer) Seek(msgID MessageID) error {
	channel := make(chan error, 1)
	c.SeekAsync(msgID, func(err error) {
		channel <- err
		close(channel)
	})
	return <-channel
}

func (c *consumer) SeekAsync(msgID MessageID, callback func(error)) {
	C._pulsar_consumer_seek_async(c.ptr, msgID.(*messageID).ptr, savePointer(callback))
}

//export pulsarConsumerSeekCallbackProxy
func pulsarConsumerSeekCallbackProxy(res C.pulsar_result, ctx unsafe.Pointer) {
	callback := restorePointer(ctx).(func(err error))

	if res != C.pulsar_result_Ok {
		go callback(newError(res, "Failed to seek Consumer"))
	} else {
		go callback(nil)
	}
}
