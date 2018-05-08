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

package main

import (
	"../../pulsar"
	"fmt"
	"log"
)

func main() {
	client := pulsar.NewClient().
		ServiceUrl("pulsar://localhost:6650").Build()

	consumer, err := client.NewConsumer().
		Topic("my-topic").
		SubscriptionName("my-subscription").
		SubscriptionType(pulsar.Shared).
		Subscribe()
	if err != nil {
		log.Fatal(err)
	}

	for {
		msg, err := consumer.Receive()
		if err != nil {
			log.Fatal(err)
		}

		fmt.Printf("Received message  msgId: %s -- content: '%s'\n",
			msg.MessageId(), string(msg.Payload()))

		consumer.Acknowledge(msg)
	}

	consumer.Close()
}
