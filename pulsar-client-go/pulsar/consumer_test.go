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
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestConsumerConnectError(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: "pulsar://invalid-hostname:6650",
	})

	assertNil(t, err)

	defer client.Close()

	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:            "my-topic",
		SubscriptionName: "my-subscription",
	})

	// Expect error in creating consumer
	assertNil(t, consumer)
	assertNotNil(t, err)

	assertEqual(t, err.(*Error).Result(), ConnectError);
}

func TestConsumer(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: "pulsar://localhost:6650",
	})

	assertNil(t, err)
	defer client.Close()

	producer, err := client.CreateProducer(ProducerOptions{
		Topic: "my-topic",
	})

	assertNil(t, err)
	defer producer.Close()

	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:                                     "my-topic",
		SubscriptionName:                          "my-sub",
		AckTimeout:                                1 * time.Minute,
		Name:                                      "my-consumer-name",
		ReceiverQueueSize:                         100,
		MaxTotalReceiverQueueSizeAcrossPartitions: 10000,
		Type:                                      Shared,
	})

	assertNil(t, err)
	defer consumer.Close()

	assertEqual(t, consumer.Topic(), "persistent://public/default/my-topic")
	assertEqual(t, consumer.Subscription(), "my-sub")

	ctx := context.Background()

	for i := 0; i < 10; i++ {
		if err := producer.Send(ctx, ProducerMessage{
			Payload: []byte(fmt.Sprintf("hello-%d", i)),
		}); err != nil {
			t.Fatal(err)
		}

		msg, err := consumer.Receive(ctx)
		assertNil(t, err)
		assertNotNil(t, msg)

		assertEqual(t, string(msg.Payload()), fmt.Sprintf("hello-%d", i))

		consumer.Ack(msg)
	}

	consumer.Unsubscribe()
}

func TestConsumerCompaction(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: "pulsar://localhost:6650",
	})

	assertNil(t, err)
	defer client.Close()

	topic := fmt.Sprintf("my-compaction-topic-%d", time.Now().Unix())

	producer, err := client.CreateProducer(ProducerOptions{
		Topic: topic,
	})

	assertNil(t, err)
	defer producer.Close()

	// Pre-create both subscriptions to retain published messages
	consumer1, err := client.Subscribe(ConsumerOptions{
		Topic:            topic,
		SubscriptionName: "my-sub-1",
	})

	assertNil(t, err)
	consumer1.Close()

	consumer2, err := client.Subscribe(ConsumerOptions{
		Topic:            topic,
		SubscriptionName: "my-sub-2",
		ReadCompacted:    true,
	})

	assertNil(t, err)
	consumer2.Close()

	ctx := context.Background()

	for i := 0; i < 10; i++ {
		if err := producer.Send(ctx, ProducerMessage{
			Payload: []byte(fmt.Sprintf("hello-%d", i)),
			Key:     "Same-Key",
		}); err != nil {
			t.Fatal(err)
		}
	}

	// Compact topic and wait for operation to complete
	url := fmt.Sprintf("http://localhost:8080/admin/v2/persistent/public/default/%s/compaction", topic)
	makeHttpPutCall(t, url)
	for {
		res := makeHttpGetCall(t, url)
		if strings.Contains(res, "RUNNING") {
			fmt.Println("Compaction still running")
			time.Sleep(100 * time.Millisecond)
			continue
		} else {
			assertEqual(t, strings.Contains(res, "SUCCESS"), true)
			fmt.Println("Compaction is done")
			break
		}
	}

	// Restart the consumers

	consumer1, err = client.Subscribe(ConsumerOptions{
		Topic:            topic,
		SubscriptionName: "my-sub-1",
	})

	assertNil(t, err)
	defer consumer1.Close()

	consumer2, err = client.Subscribe(ConsumerOptions{
		Topic:            topic,
		SubscriptionName: "my-sub-2",
		ReadCompacted:    true,
	})

	assertNil(t, err)
	defer consumer2.Close()

	// Consumer-1 will receive all messages
	for i := 0; i < 10; i++ {
		msg, err := consumer1.Receive(context.Background())
		assertNil(t, err)
		assertNotNil(t, msg)

		assertEqual(t, string(msg.Payload()), fmt.Sprintf("hello-%d", i))
	}

	// Consumer-2 will only receive the last message
	msg, err := consumer2.Receive(context.Background())
	assertNil(t, err)
	assertNotNil(t, msg)
	assertEqual(t, string(msg.Payload()), fmt.Sprintf("hello-9"))

	// No more messages on consumer-2
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	msg, err = consumer2.Receive(ctx)
	assertNil(t, msg)
	assertNotNil(t, err)
}

func TestConsumerWithInvalidConf(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: "pulsar://localhost:6650",
	})

	if err != nil {
		t.Fatal(err)
		return
	}

	defer client.Close()

	consumer, err := client.Subscribe(ConsumerOptions{
		Topic: "my-topic",
	})

	// Expect error in creating cosnumer
	assertNil(t, consumer)
	assertNotNil(t, err)

	assertEqual(t, err.(*Error).Result(), InvalidConfiguration)

	consumer, err = client.Subscribe(ConsumerOptions{
		SubscriptionName: "my-subscription",
	})

	// Expect error in creating consumer
	assertNil(t, consumer)
	assertNotNil(t, err)

	assertEqual(t, err.(*Error).Result(), InvalidConfiguration)
}

func makeHttpPutCall(t *testing.T, url string) string {
	return makeHttpCall(t, http.MethodPut, url)
}

func makeHttpGetCall(t *testing.T, url string) string {
	return makeHttpCall(t, http.MethodGet, url)
}

func makeHttpCall(t *testing.T, method string, url string) string {
	client := http.Client{}

	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		t.Fatal(err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	res, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}

	return string(body)
}

func TestConsumerMultiTopics(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: "pulsar://localhost:6650",
	})

	assertNil(t, err)
	defer client.Close()

	producer1, err := client.CreateProducer(ProducerOptions{
		Topic: "multi-topic-1",
	})

	assertNil(t, err)

	producer2, err := client.CreateProducer(ProducerOptions{
		Topic: "multi-topic-2",
	})

	assertNil(t, err)
	defer producer1.Close()
	defer producer2.Close()

	consumer, err := client.Subscribe(ConsumerOptions{
		Topics:           []string{"multi-topic-1", "multi-topic-2"},
		SubscriptionName: "my-sub",
	})

	assertNil(t, err)
	defer consumer.Close()

	assertEqual(t, consumer.Subscription(), "my-sub")

	ctx := context.Background()

	for i := 0; i < 10; i++ {
		if err := producer1.Send(ctx, ProducerMessage{
			Payload: []byte(fmt.Sprintf("hello-%d", i)),
		}); err != nil {
			t.Fatal(err)
		}

		if err := producer2.Send(ctx, ProducerMessage{
			Payload: []byte(fmt.Sprintf("hello-%d", i)),
		}); err != nil {
			t.Fatal(err)
		}
	}

	for i := 0; i < 20; i++ {
		msg, err := consumer.Receive(ctx)
		assertNil(t, err)
		assertNotNil(t, msg)

		consumer.Ack(msg)
	}

	consumer.Unsubscribe()
}


func TestConsumerRegex(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: "pulsar://localhost:6650",
	})

	assertNil(t, err)
	defer client.Close()

	producer1, err := client.CreateProducer(ProducerOptions{
		Topic: "topic-1",
	})

	assertNil(t, err)

	producer2, err := client.CreateProducer(ProducerOptions{
		Topic: "topic-2",
	})

	assertNil(t, err)
	defer producer1.Close()
	defer producer2.Close()

	consumer, err := client.Subscribe(ConsumerOptions{
		TopicsPattern: "topic-\\d+",
		SubscriptionName: "my-sub",
	})

	assertNil(t, err)
	defer consumer.Close()

	assertEqual(t, consumer.Subscription(), "my-sub")

	ctx := context.Background()

	for i := 0; i < 10; i++ {
		if err := producer1.Send(ctx, ProducerMessage{
			Payload: []byte(fmt.Sprintf("hello-%d", i)),
		}); err != nil {
			t.Fatal(err)
		}

		if err := producer2.Send(ctx, ProducerMessage{
			Payload: []byte(fmt.Sprintf("hello-%d", i)),
		}); err != nil {
			t.Fatal(err)
		}
	}

	for i := 0; i < 20; i++ {
		msg, err := consumer.Receive(ctx)
		assertNil(t, err)
		assertNotNil(t, msg)

		consumer.Ack(msg)
	}

	consumer.Unsubscribe()
}