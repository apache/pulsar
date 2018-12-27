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
	"testing"
)

func TestGetTopicPartitions(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL:                      "pulsar://localhost:6650",
	})

	assertNil(t, err)
	defer client.Close()

	// Create topic with 5 partitions
	httpPut("http://localhost:8080/admin/v2/persistent/public/default/TestGetTopicPartitions/partitions",
		5)

	partitionedTopic := "persistent://public/default/TestGetTopicPartitions"

	partitions, err := client.TopicPartitions(partitionedTopic)
	assertNil(t, err)
	assertEqual(t, len(partitions), 5)
	for i := 0; i < 5; i++ {
		assertEqual(t, partitions[i],
			fmt.Sprintf("%s-partition-%d", partitionedTopic, i))
	}

	// Non-Partitioned topic
	topic := "persistent://public/default/TestGetTopicPartitions-nopartitions"

	partitions, err = client.TopicPartitions(topic)
	assertNil(t, err)
	assertEqual(t, len(partitions), 1)
	assertEqual(t, partitions[0], topic)
}

const TestTokenFilePath = "/tmp/pulsar-test-data/certs/token.txt"

func readToken(t *testing.T) string {
	data, err := ioutil.ReadFile(TestTokenFilePath)
	assertNil(t, err)

	return string(data)
}

func TestTokenAuth(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL:            "pulsar://localhost:6650",
		Authentication: NewAuthenticationToken(readToken(t)),
	})

	assertNil(t, err)
	defer client.Close()

	topic := "persistent://private/auth/TestTokenAuth"

	producer, err := client.CreateProducer(ProducerOptions{
		Topic: topic,
	})

	assertNil(t, err)
	defer producer.Close()

	ctx := context.Background()

	for i := 0; i < 10; i++ {
		if err := producer.Send(ctx, ProducerMessage{
			Payload: []byte(fmt.Sprintf("hello-%d", i)),
		}); err != nil {
			t.Fatal(err)
		}
	}
}

func TestTokenAuthSupplier(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL:            "pulsar://localhost:6650",
		Authentication: NewAuthenticationTokenSupplier(func () string {
			return readToken(t)
		}),
	})

	assertNil(t, err)
	defer client.Close()

	topic := "persistent://private/auth/TestTokenAuth"

	producer, err := client.CreateProducer(ProducerOptions{
		Topic: topic,
	})

	assertNil(t, err)
	defer producer.Close()

	ctx := context.Background()

	for i := 0; i < 10; i++ {
		if err := producer.Send(ctx, ProducerMessage{
			Payload: []byte(fmt.Sprintf("hello-%d", i)),
		}); err != nil {
			t.Fatal(err)
		}
	}
}
