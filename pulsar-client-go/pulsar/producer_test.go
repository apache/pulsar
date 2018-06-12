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
	"testing"
	"fmt"
	"context"
	"time"
)

func TestInvalidURL(t *testing.T) {
	client, err := NewClient(ClientOptions{})

	if client != nil || err == nil {
		t.Fatal("Should have failed to create client")
	}
}

func TestProducerConnectError(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: "pulsar://invalid-hostname:6650",
	})

	assertNil(t, err)

	defer client.Close()

	producer, err := client.CreateProducer(ProducerOptions{
		Topic: "my-topic",
	})

	// Expect error in creating producer
	assertNil(t, producer)
	assertNotNil(t, err)

	assertEqual(t, err.(*Error).Result(), ConnectError);
}

func TestProducer(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL:                      "pulsar://localhost:6650",
		StatsIntervalInSeconds:   10,
		IOThreads:                1,
		OperationTimeoutSeconds:  30,
		ConcurrentLookupRequests: 1000,
		MessageListenerThreads:   5,
		EnableTLS:                false,
	})

	assertNil(t, err)
	defer client.Close()

	producer, err := client.CreateProducer(ProducerOptions{
		Topic:                   "my-topic",
		Name:                    "my-producer-name",
		SendTimeout:             10 * time.Second,
		Batching:                true,
		BatchingMaxMessages:     100,
		BatchingMaxPublishDelay: 10 * time.Millisecond,
		MaxPendingMessages:      100,
		BlockIfQueueFull:        true,
		CompressionType:         LZ4,
	})

	assertNil(t, err)
	defer producer.Close()

	assertEqual(t, producer.Topic(), "persistent://public/default/my-topic")
	assertEqual(t, producer.Name(), "my-producer-name")

	ctx := context.Background()

	for i := 0; i < 10; i++ {
		if err := producer.Send(ctx, ProducerMessage{
			Payload: []byte(fmt.Sprintf("hello-%d", i)),
		}); err != nil {
			t.Fatal(err)
		}
	}
}

func TestProducerNoTopic(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: "pulsar://localhost:6650",
	})

	if err != nil {
		t.Fatal(err)
		return
	}

	defer client.Close()

	producer, err := client.CreateProducer(ProducerOptions{
	})

	// Expect error in creating producer
	assertNil(t, producer)
	assertNotNil(t, err)

	assertEqual(t, err.(*Error).Result(), InvalidConfiguration)
}

func TestMessageRouter(t *testing.T) {
	// Create topic with 5 partitions
	httpPut("http://localhost:8080/admin/v2/persistent/public/default/my-partitioned-topic/partitions",
		5)

	client, err := NewClient(ClientOptions{
		URL: "pulsar://localhost:6650",
	})

	assertNil(t, err)
	defer client.Close()

	// Only subscribe on the specific partition
	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:            "my-partitioned-topic-partition-2",
		SubscriptionName: "my-sub",
	})

	assertNil(t, err)
	defer consumer.Close()

	producer, err := client.CreateProducer(ProducerOptions{
		Topic: "my-partitioned-topic",
		MessageRouter: func(msg Message, tm TopicMetadata) int {
			fmt.Println("Routing message ", msg, " -- Partitions: ", tm.NumPartitions())
			return 2
		},
	})

	assertNil(t, err)
	defer producer.Close()

	ctx := context.Background()

	err = producer.Send(ctx, ProducerMessage{
		Payload: []byte("hello"),
	})
	assertNil(t, err)

	fmt.Println("PUBLISHED")

	// Verify message was published on partition 2
	msg, err := consumer.Receive(ctx)
	assertNil(t, err)
	assertNotNil(t, msg)
	assertEqual(t, string(msg.Payload()), "hello")
}
