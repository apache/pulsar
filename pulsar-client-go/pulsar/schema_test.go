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

import (
	"context"
	"testing"

	log "github.com/apache/pulsar/pulsar-client-go/logutil"
	"github.com/apache/pulsar/pulsar-client-go/pulsar/pb"
	"github.com/stretchr/testify/assert"
)

type testSchema struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

func TestJsonSchema(t *testing.T) {
	// create client
	lookupUrl := "pulsar://localhost:6650"
	client, err := NewClient(ClientOptions{
		URL: lookupUrl,
	})
	assert.Nil(t, err)
	defer client.Close()

	// create producer
	js := new(JsonSchema)
	schema, err := js.Serialize(testSchema{
		ID:   100,
		Name: "pulsar",
	})
	str := string(schema)
	schemaJsonInfo := NewSchemaInfo("jsonTopic", str, JSON)
	js.SchemaInfo = *schemaJsonInfo

	if !js.Validate() {
		panic("validate schema type error.")
	}
	producer, err := client.CreateProducer(ProducerOptions{
		Topic:      "jsonTopic",
		SchemaInfo: *schemaJsonInfo,
	})
	assert.Nil(t, err)
	ctx := context.Background()
	if err := producer.Send(ctx, ProducerMessage{
		Payload: schema,
	}); err != nil {
		log.Fatal(err)
	}

	//create consumer
	var s testSchema

	schemaInfo := NewSchemaInfo("jsonTopic", "", JSON)
	js.SchemaInfo = *schemaInfo
	if !js.Validate() {
		panic("validate json schema type error")
	}
	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:            "jsonTopic",
		SubscriptionName: "sub-2",
		Schema:           *schemaInfo,
	})
	assert.Nil(t, err)

	msg, err := consumer.Receive(ctx)
	assert.Nil(t, err)
	err = js.UnSerialize(msg.Payload(), &s)
	assert.Nil(t, err)
	assert.Equal(t, s.ID, 100)
	assert.Equal(t, s.Name, "pulsar")

	defer consumer.Close()
}

func TestProtoSchema(t *testing.T) {
	// create client
	lookupUrl := "pulsar://localhost:6650"
	client, err := NewClient(ClientOptions{
		URL: lookupUrl,
	})
	assert.Nil(t, err)
	defer client.Close()

	// create producer
	ps := new(ProtoSchema)
	obj := &pb.Test{Num: 100, Msf: "pulsar"}
	data, err := ps.Serialize(obj)
	assert.Nil(t, err)

	str := string(data)
	schemaJsonInfo := NewSchemaInfo("proto", str, PROTOBUF)
	ps.SchemaInfo = *schemaJsonInfo

	if !ps.Validate() {
		panic("validate schema type error.")
	}
	producer, err := client.CreateProducer(ProducerOptions{
		Topic:      "proto",
		SchemaInfo: *schemaJsonInfo,
	})
	ctx := context.Background()
	if err := producer.Send(ctx, ProducerMessage{
		Payload: data,
	}); err != nil {
		log.Fatal(err)
	}

	//create consumer
	unobj := &pb.Test{}

	schemaInfo := NewSchemaInfo("proto", "", PROTOBUF)
	ps.SchemaInfo = *schemaInfo
	if !ps.Validate() {
		panic("validate json schema type error")
	}
	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:            "proto",
		SubscriptionName: "sub-1",
		Schema:           *schemaInfo,
	})
	assert.Nil(t, err)

	msg, err := consumer.Receive(ctx)
	assert.Nil(t, err)
	err = ps.UnSerialize(msg.Payload(), unobj)
	assert.Nil(t, err)
	assert.Equal(t, unobj.Num, int32(100))
	assert.Equal(t, unobj.Msf, "pulsar")
	defer consumer.Close()
}

func TestStringSchema(t *testing.T) {
	// create client
	lookupUrl := "pulsar://localhost:6650"
	client, err := NewClient(ClientOptions{
		URL: lookupUrl,
	})
	assert.Nil(t, err)
	defer client.Close()

	str := "hello pulsar"

	ssProducer := NewStringSchema()
	schema := ssProducer.Serialize(str)
	producer, err := client.CreateProducer(ProducerOptions{
		Topic:      "strTopic",
		SchemaInfo: ssProducer.SchemaInfo,
	})
	assert.Nil(t, err)
	ctx := context.Background()
	if err := producer.Send(ctx, ProducerMessage{
		Payload: schema,
	}); err != nil {
		log.Fatal(err)
	}
	producer.Close()

	ssConsumer := NewStringSchema()
	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:            "strTopic",
		SubscriptionName: "sub-2",
		Schema:           ssConsumer.SchemaInfo,
	})
	assert.Nil(t, err)

	msg, err := consumer.Receive(ctx)
	res := ssConsumer.UnSerialize(msg.Payload())
	assert.Nil(t, err)
	assert.Equal(t, res, str)

	defer consumer.Close()
}

func TestBytesSchema(t *testing.T) {
	// create client
	lookupUrl := "pulsar://localhost:6650"
	client, err := NewClient(ClientOptions{
		URL: lookupUrl,
	})
	assert.Nil(t, err)
	defer client.Close()

	bytes := []byte{121, 110, 121, 110}

	bsProducer := NewBytesSchema()
	schema := bsProducer.Serialize(bytes)
	producer, err := client.CreateProducer(ProducerOptions{
		Topic:      "bytesTopic",
		SchemaInfo: bsProducer.SchemaInfo,
	})
	assert.Nil(t, err)
	ctx := context.Background()
	if err := producer.Send(ctx, ProducerMessage{
		Payload: schema,
	}); err != nil {
		log.Fatal(err)
	}
	producer.Close()

	bsConsumer := NewBytesSchema()
	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:            "bytesTopic",
		SubscriptionName: "sub-2",
		Schema:           bsConsumer.SchemaInfo,
	})
	assert.Nil(t, err)

	msg, err := consumer.Receive(ctx)
	res := bsConsumer.UnSerialize(msg.Payload())
	assert.Nil(t, err)
	assert.Equal(t, res, bytes)

	defer consumer.Close()
}

func TestInt8Schema(t *testing.T) {
	// create client
	lookupUrl := "pulsar://localhost:6650"
	client, err := NewClient(ClientOptions{
		URL: lookupUrl,
	})
	assert.Nil(t, err)
	defer client.Close()

	in := int8(1)

	i8sProducer := NewInt8Schema()
	schema, err := i8sProducer.Serialize(in)
	assert.Nil(t, err)
	producer, err := client.CreateProducer(ProducerOptions{
		Topic:      "int8Topic",
		SchemaInfo: i8sProducer.SchemaInfo,
	})
	assert.Nil(t, err)
	ctx := context.Background()
	if err := producer.Send(ctx, ProducerMessage{
		Payload: schema,
	}); err != nil {
		log.Fatal(err)
	}
	producer.Close()

	i8sConsumer := NewInt8Schema()
	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:            "int8Topic",
		SubscriptionName: "sub-2",
		Schema:           i8sConsumer.SchemaInfo,
	})
	assert.Nil(t, err)

	buf := []byte{0x01}
	msg, err := consumer.Receive(ctx)
	assert.Nil(t, err)
	err = i8sConsumer.UnSerialize(msg.Payload(), int8(0))
	assert.Nil(t, err)
	assert.Equal(t, msg.Payload(), buf)

	defer consumer.Close()
}

func TestInt16Schema(t *testing.T) {
	// create client
	lookupUrl := "pulsar://localhost:6650"
	client, err := NewClient(ClientOptions{
		URL: lookupUrl,
	})
	assert.Nil(t, err)
	defer client.Close()

	in := int16(1)

	i16sProducer := NewInt16Schema()
	schema, err := i16sProducer.Serialize(in)
	assert.Nil(t, err)
	producer, err := client.CreateProducer(ProducerOptions{
		Topic:      "int16Topic",
		SchemaInfo: i16sProducer.SchemaInfo,
	})
	assert.Nil(t, err)
	ctx := context.Background()
	if err := producer.Send(ctx, ProducerMessage{
		Payload: schema,
	}); err != nil {
		log.Fatal(err)
	}
	producer.Close()

	i16sConsumer := NewInt16Schema()
	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:            "int16Topic",
		SubscriptionName: "sub-2",
		Schema:           i16sConsumer.SchemaInfo,
	})
	assert.Nil(t, err)

	buf := []byte{0x00, 0x01}
	msg, err := consumer.Receive(ctx)
	assert.Nil(t, err)
	err = i16sConsumer.UnSerialize(msg.Payload(), int16(0))
	assert.Nil(t, err)
	assert.Equal(t, msg.Payload(), buf)

	defer consumer.Close()
}

func TestInt32Schema(t *testing.T) {
	// create client
	lookupUrl := "pulsar://localhost:6650"
	client, err := NewClient(ClientOptions{
		URL: lookupUrl,
	})
	assert.Nil(t, err)
	defer client.Close()

	in := int32(1)

	i32sProducer := NewInt32Schema()
	schema, err := i32sProducer.Serialize(in)
	assert.Nil(t, err)
	producer, err := client.CreateProducer(ProducerOptions{
		Topic:      "int32Topic-1",
		SchemaInfo: i32sProducer.SchemaInfo,
	})
	assert.Nil(t, err)
	ctx := context.Background()
	if err := producer.Send(ctx, ProducerMessage{
		Payload: schema,
	}); err != nil {
		log.Fatal(err)
	}
	producer.Close()

	i32sConsumer := NewInt32Schema()
	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:            "int32Topic-1",
		SubscriptionName: "sub-2",
		Schema:           i32sConsumer.SchemaInfo,
	})
	assert.Nil(t, err)

	buf := []byte{0x00, 0x00, 0x00, 0x01}
	msg, err := consumer.Receive(ctx)
	assert.Nil(t, err)
	err = i32sConsumer.UnSerialize(msg.Payload(), int32(0))
	assert.Nil(t, err)
	assert.Equal(t, msg.Payload(), buf)

	defer consumer.Close()
}

func TestInt64Schema(t *testing.T) {
	// create client
	lookupUrl := "pulsar://localhost:6650"
	client, err := NewClient(ClientOptions{
		URL: lookupUrl,
	})
	assert.Nil(t, err)
	defer client.Close()

	in := int64(1)

	i64sProducer := NewInt64Schema()
	schema, err := i64sProducer.Serialize(in)
	assert.Nil(t, err)
	producer, err := client.CreateProducer(ProducerOptions{
		Topic:      "int64Topic",
		SchemaInfo: i64sProducer.SchemaInfo,
	})
	assert.Nil(t, err)
	ctx := context.Background()
	if err := producer.Send(ctx, ProducerMessage{
		Payload: schema,
	}); err != nil {
		log.Fatal(err)
	}
	producer.Close()

	i64sConsumer := NewInt64Schema()
	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:            "int64Topic",
		SubscriptionName: "sub-2",
		Schema:           i64sConsumer.SchemaInfo,
	})
	assert.Nil(t, err)

	buf := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01}
	msg, err := consumer.Receive(ctx)
	assert.Nil(t, err)
	err = i64sConsumer.UnSerialize(msg.Payload(), int64(0))
	assert.Nil(t, err)
	assert.Equal(t, msg.Payload(), buf)

	defer consumer.Close()
}

func TestFloat32Schema(t *testing.T) {
	// create client
	lookupUrl := "pulsar://localhost:6650"
	client, err := NewClient(ClientOptions{
		URL: lookupUrl,
	})
	assert.Nil(t, err)
	defer client.Close()

	in := int64(1)

	f32sProducer := NewFloat32Schema()
	schema, err := f32sProducer.Serialize(in)
	assert.Nil(t, err)
	producer, err := client.CreateProducer(ProducerOptions{
		Topic:      "float32Topic",
		SchemaInfo: f32sProducer.SchemaInfo,
	})
	assert.Nil(t, err)
	ctx := context.Background()
	if err := producer.Send(ctx, ProducerMessage{
		Payload: schema,
	}); err != nil {
		log.Fatal(err)
	}
	producer.Close()

	f32sConsumer := NewFloat32Schema()
	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:            "float32Topic",
		SubscriptionName: "sub-2",
		Schema:           f32sConsumer.SchemaInfo,
	})
	assert.Nil(t, err)

	msg, err := consumer.Receive(ctx)
	assert.Nil(t, err)
	res, err := f32sConsumer.UnSerialize(msg.Payload())
	assert.Nil(t, err)
	assert.Equal(t, res, float32(1))

	defer consumer.Close()
}

func TestFloat64Schema(t *testing.T) {
	// create client
	lookupUrl := "pulsar://localhost:6650"
	client, err := NewClient(ClientOptions{
		URL: lookupUrl,
	})
	assert.Nil(t, err)
	defer client.Close()

	in := int64(1)

	f64sProducer := NewFloat64Schema()
	schema, err := f64sProducer.Serialize(in)
	assert.Nil(t, err)
	producer, err := client.CreateProducer(ProducerOptions{
		Topic:      "float64Topic",
		SchemaInfo: f64sProducer.SchemaInfo,
	})
	assert.Nil(t, err)
	ctx := context.Background()
	if err := producer.Send(ctx, ProducerMessage{
		Payload: schema,
	}); err != nil {
		log.Fatal(err)
	}
	producer.Close()

	f64sConsumer := NewFloat64Schema()
	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:            "float64Topic",
		SubscriptionName: "sub-2",
		Schema:           f64sConsumer.SchemaInfo,
	})
	assert.Nil(t, err)

	msg, err := consumer.Receive(ctx)
	assert.Nil(t, err)
	res, err := f64sConsumer.UnSerialize(msg.Payload())
	assert.Nil(t, err)
	assert.Equal(t, res, float64(1))

	defer consumer.Close()
}
