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

type testJson struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

type testAvro struct {
	ID   int
	Name string
}

var (
	exampleSchemaDef = "{\"type\":\"record\",\"name\":\"Example\",\"namespace\":\"test\"," +
		"\"fields\":[{\"name\":\"ID\",\"type\":\"int\"},{\"name\":\"Name\",\"type\":\"string\"}]}"
	protoSchemaDef = "{\"type\":\"record\",\"name\":\"Example\",\"namespace\":\"test\"," +
		"\"fields\":[{\"name\":\"num\",\"type\":\"int\"},{\"name\":\"msf\",\"type\":\"string\"}]}"
)

func createClient() Client {
	// create client
	lookupUrl := "pulsar://localhost:6650"
	client, err := NewClient(ClientOptions{
		URL: lookupUrl,
	})
	if err != nil {
		log.Fatal(err)
	}
	return client
}

func TestJsonSchema(t *testing.T) {
	client := createClient()
	defer client.Close()

	jsonSchema := NewJsonSchema(exampleSchemaDef, nil)
	producer, err := client.CreateProducerWithSchema(ProducerOptions{
		Topic: "jsonTopic",
	}, jsonSchema)
	err = producer.Send(context.Background(), ProducerMessage{
		Value: &testJson{
			ID:   100,
			Name: "pulsar",
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()

	properties := make(map[string]string)
	properties["pulsar"]="hello"
	jsonSchemaWithProperties := NewJsonSchema(exampleSchemaDef, properties)
	producer1, err := client.CreateProducerWithSchema(ProducerOptions{
		Topic: "jsonTopic",
	}, jsonSchemaWithProperties)
	err = producer1.Send(context.Background(), ProducerMessage{
		Value: &testJson{
			ID:   100,
			Name: "pulsar",
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer producer1.Close()

	//create consumer
	var s testJson

	consumerJS := NewJsonSchema(exampleSchemaDef, nil)
	consumer, err := client.SubscribeWithSchema(ConsumerOptions{
		Topic:            "jsonTopic",
		SubscriptionName: "sub-2",
	}, consumerJS)
	assert.Nil(t, err)
	msg, err := consumer.Receive(context.Background())
	assert.Nil(t, err)
	err = msg.GetValue(&s)
	assert.Nil(t, err)
	assert.Equal(t, s.ID, 100)
	assert.Equal(t, s.Name, "pulsar")

	defer consumer.Close()
}

func TestProtoSchema(t *testing.T) {
	client := createClient()
	defer client.Close()

	// create producer
	psProducer := NewProtoSchema(protoSchemaDef, nil)
	producer, err := client.CreateProducerWithSchema(ProducerOptions{
		Topic: "proto",
	}, psProducer)
	if err := producer.Send(context.Background(), ProducerMessage{
		Value: &pb.Test{
			Num: 100,
			Msf: "pulsar",
		},
	}); err != nil {
		log.Fatal(err)
	}

	//create consumer
	unobj := pb.Test{}
	psConsumer := NewProtoSchema(protoSchemaDef, nil)
	consumer, err := client.SubscribeWithSchema(ConsumerOptions{
		Topic:            "proto",
		SubscriptionName: "sub-1",
	}, psConsumer)
	assert.Nil(t, err)

	msg, err := consumer.Receive(context.Background())
	assert.Nil(t, err)
	err = msg.GetValue(&unobj)
	assert.Nil(t, err)
	assert.Equal(t, unobj.Num, int32(100))
	assert.Equal(t, unobj.Msf, "pulsar")
	defer consumer.Close()
}

func TestAvroSchema(t *testing.T) {
	client := createClient()
	defer client.Close()

	// create producer
	asProducer := NewAvroSchema(exampleSchemaDef, nil)
	producer, err := client.CreateProducerWithSchema(ProducerOptions{
		Topic: "avro-topic",
	}, asProducer)
	assert.Nil(t, err)
	if err := producer.Send(context.Background(), ProducerMessage{
		Value: testAvro{
			ID:   100,
			Name: "pulsar",
		},
	}); err != nil {
		log.Fatal(err)
	}

	//create consumer
	unobj := testAvro{}

	asConsumer := NewAvroSchema(exampleSchemaDef, nil)
	consumer, err := client.SubscribeWithSchema(ConsumerOptions{
		Topic:            "avro-topic",
		SubscriptionName: "sub-1",
	}, asConsumer)
	assert.Nil(t, err)

	msg, err := consumer.Receive(context.Background())
	assert.Nil(t, err)
	err = msg.GetValue(&unobj)
	assert.Nil(t, err)
	assert.Equal(t, unobj.ID, 100)
	assert.Equal(t, unobj.Name, "pulsar")
	defer consumer.Close()
}

func TestStringSchema(t *testing.T) {
	client := createClient()
	defer client.Close()

	ssProducer := NewStringSchema(nil)
	producer, err := client.CreateProducerWithSchema(ProducerOptions{
		Topic: "strTopic",
	}, ssProducer)
	assert.Nil(t, err)
	if err := producer.Send(context.Background(), ProducerMessage{
		Value: "hello pulsar",
	}); err != nil {
		log.Fatal(err)
	}
	defer producer.Close()

	var res *string
	consumer, err := client.SubscribeWithSchema(ConsumerOptions{
		Topic:            "strTopic",
		SubscriptionName: "sub-2",
	}, NewStringSchema(nil))
	assert.Nil(t, err)

	msg, err := consumer.Receive(context.Background())
	assert.Nil(t, err)
	err = msg.GetValue(&res)
	assert.Equal(t, *res, "hello pulsar")

	defer consumer.Close()
}

func TestBytesSchema(t *testing.T) {
	client := createClient()
	defer client.Close()

	bytes := []byte{121, 110, 121, 110}
	producer, err := client.CreateProducerWithSchema(ProducerOptions{
		Topic: "bytesTopic",
	}, NewBytesSchema(nil))
	assert.Nil(t, err)
	ctx := context.Background()
	if err := producer.Send(ctx, ProducerMessage{
		Value: bytes,
	}); err != nil {
		log.Fatal(err)
	}
	defer producer.Close()

	var res []byte
	consumer, err := client.SubscribeWithSchema(ConsumerOptions{
		Topic:            "bytesTopic",
		SubscriptionName: "sub-2",
	}, NewBytesSchema(nil))
	assert.Nil(t, err)

	msg, err := consumer.Receive(context.Background())
	assert.Nil(t, err)
	err = msg.GetValue(&res)
	assert.Equal(t, res, bytes)

	defer consumer.Close()
}

func TestInt8Schema(t *testing.T) {
	client := createClient()
	defer client.Close()

	producer, err := client.CreateProducerWithSchema(ProducerOptions{
		Topic: "int8Topic1",
	}, NewInt8Schema(nil))
	assert.Nil(t, err)
	ctx := context.Background()
	if err := producer.Send(ctx, ProducerMessage{
		Value: int8(1),
	}); err != nil {
		log.Fatal(err)
	}
	defer producer.Close()

	consumer, err := client.SubscribeWithSchema(ConsumerOptions{
		Topic:            "int8Topic1",
		SubscriptionName: "sub-2",
	}, NewInt8Schema(nil))
	assert.Nil(t, err)

	var res int8
	msg, err := consumer.Receive(ctx)
	assert.Nil(t, err)
	err = msg.GetValue(&res)
	assert.Nil(t, err)
	assert.Equal(t, res, int8(1))

	defer consumer.Close()
}

func TestInt16Schema(t *testing.T) {
	client := createClient()
	defer client.Close()

	producer, err := client.CreateProducerWithSchema(ProducerOptions{
		Topic: "int16Topic",
	}, NewInt16Schema(nil))
	assert.Nil(t, err)
	ctx := context.Background()
	if err := producer.Send(ctx, ProducerMessage{
		Value: int16(1),
	}); err != nil {
		log.Fatal(err)
	}
	defer producer.Close()

	consumer, err := client.SubscribeWithSchema(ConsumerOptions{
		Topic:            "int16Topic",
		SubscriptionName: "sub-2",
	}, NewInt16Schema(nil))
	assert.Nil(t, err)

	var res int16
	msg, err := consumer.Receive(ctx)
	assert.Nil(t, err)
	err = msg.GetValue(&res)
	assert.Nil(t, err)
	assert.Equal(t, res, int16(1))
	defer consumer.Close()
}

func TestInt32Schema(t *testing.T) {
	client := createClient()
	defer client.Close()

	producer, err := client.CreateProducerWithSchema(ProducerOptions{
		Topic: "int32Topic1",
	}, NewInt32Schema(nil))
	assert.Nil(t, err)
	ctx := context.Background()
	if err := producer.Send(ctx, ProducerMessage{
		Value: int32(1),
	}); err != nil {
		log.Fatal(err)
	}
	defer producer.Close()

	consumer, err := client.SubscribeWithSchema(ConsumerOptions{
		Topic:            "int32Topic1",
		SubscriptionName: "sub-2",
	}, NewInt32Schema(nil))
	assert.Nil(t, err)

	var res int32
	msg, err := consumer.Receive(ctx)
	assert.Nil(t, err)
	err = msg.GetValue(&res)
	assert.Nil(t, err)
	assert.Equal(t, res, int32(1))
	defer consumer.Close()
}

func TestInt64Schema(t *testing.T) {
	client := createClient()
	defer client.Close()

	producer, err := client.CreateProducerWithSchema(ProducerOptions{
		Topic: "int64Topic",
	}, NewInt64Schema(nil))
	assert.Nil(t, err)
	ctx := context.Background()
	if err := producer.Send(ctx, ProducerMessage{
		Value: int64(1),
	}); err != nil {
		log.Fatal(err)
	}
	defer producer.Close()

	consumer, err := client.SubscribeWithSchema(ConsumerOptions{
		Topic:            "int64Topic",
		SubscriptionName: "sub-2",
	}, NewInt64Schema(nil))
	assert.Nil(t, err)

	var res int64
	msg, err := consumer.Receive(ctx)
	assert.Nil(t, err)
	err = msg.GetValue(&res)
	assert.Nil(t, err)
	assert.Equal(t, res, int64(1))
	defer consumer.Close()
}

func TestFloatSchema(t *testing.T) {
	client := createClient()
	defer client.Close()

	producer, err := client.CreateProducerWithSchema(ProducerOptions{
		Topic: "floatTopic",
	}, NewFloatSchema(nil))
	assert.Nil(t, err)
	if err := producer.Send(context.Background(), ProducerMessage{
		Value: float32(1),
	}); err != nil {
		log.Fatal(err)
	}
	defer producer.Close()

	consumer, err := client.SubscribeWithSchema(ConsumerOptions{
		Topic:            "floatTopic",
		SubscriptionName: "sub-2",
	}, NewFloatSchema(nil))
	assert.Nil(t, err)

	var res float32
	msg, err := consumer.Receive(context.Background())
	assert.Nil(t, err)
	err = msg.GetValue(&res)
	assert.Nil(t, err)
	assert.Equal(t, res, float32(1))
	defer consumer.Close()
}

func TestDoubleSchema(t *testing.T) {
	client := createClient()
	defer client.Close()

	producer, err := client.CreateProducerWithSchema(ProducerOptions{
		Topic: "doubleTopic",
	}, NewDoubleSchema(nil))
	assert.Nil(t, err)
	ctx := context.Background()
	if err := producer.Send(ctx, ProducerMessage{
		Value: float64(1),
	}); err != nil {
		log.Fatal(err)
	}
	defer producer.Close()

	consumer, err := client.SubscribeWithSchema(ConsumerOptions{
		Topic:            "doubleTopic",
		SubscriptionName: "sub-2",
	}, NewDoubleSchema(nil))
	assert.Nil(t, err)

	var res float64
	msg, err := consumer.Receive(ctx)
	assert.Nil(t, err)
	err = msg.GetValue(&res)
	assert.Nil(t, err)
	assert.Equal(t, res, float64(1))
	defer consumer.Close()
}
