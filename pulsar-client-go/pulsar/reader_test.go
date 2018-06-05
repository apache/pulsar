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
)

func TestReaderConnectError(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: "pulsar://invalid-hostname:6650",
	})

	assertNil(t, err)

	defer client.Close()

	reader, err := client.CreateReader(ReaderOptions{
		Topic:          "my-topic",
		StartMessageID: EarliestMessage,
	})

	// Expect error in creating reader
	assertNil(t, reader)
	assertNotNil(t, err)

	assertEqual(t, err.(*Error).Result(), ConnectError);
}

func TestReader(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: "pulsar://localhost:6650",
	})

	assertNil(t, err)
	defer client.Close()

	producer, err := client.CreateProducer(ProducerOptions{
		Topic: "my-reader-topic",
	})

	assertNil(t, err)
	defer producer.Close()

	reader, err := client.CreateReader(ReaderOptions{
		Topic:          "my-reader-topic",
		StartMessageID: LatestMessage,
	})

	assertNil(t, err)
	defer reader.Close()

	assertEqual(t, reader.Topic(), "persistent://public/default/my-reader-topic")

	ctx := context.Background()

	for i := 0; i < 10; i++ {
		if err := producer.Send(ctx, ProducerMessage{
			Payload: []byte(fmt.Sprintf("hello-%d", i)),
		}); err != nil {
			t.Fatal(err)
		}

		msg, err := reader.Next(ctx)
		assertNil(t, err)
		assertNotNil(t, msg)

		assertEqual(t, string(msg.Payload()), fmt.Sprintf("hello-%d", i))
	}
}

func TestReaderWithInvalidConf(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: "pulsar://localhost:6650",
	})

	if err != nil {
		t.Fatal(err)
		return
	}

	defer client.Close()

	reader, err := client.CreateReader(ReaderOptions{
		Topic: "my-topic",
	})

	// Expect error in creating cosnumer
	assertNil(t, reader)
	assertNotNil(t, err)

	assertEqual(t, err.(*Error).Result(), InvalidConfiguration)

	reader, err = client.CreateReader(ReaderOptions{
		StartMessageID: LatestMessage,
	})

	// Expect error in creating cosnumer
	assertNil(t, reader)
	assertNotNil(t, err)

	assertEqual(t, err.(*Error).Result(), InvalidConfiguration)
}
