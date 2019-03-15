/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package util

import (
	"fmt"

	"github.com/apache/pulsar/pulsar-client-go/pulsar"
)

func SetProducer() {
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: "pulsar://localhost:6650",
	})
	if err != nil {
		fmt.Printf("error:%v\n", err)
	}
	defer client.Close()

	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: "topic-3",
	})
	defer producer.Close()

	//ctx := context.Background()
	//for i := 0; i < 10; i++ {
	//	// Create a different message to send asynchronously
	//	asyncMsg := pulsar.ProducerMessage{
	//		Payload: []byte(fmt.Sprintf("async-message-%d", i)),
	//	}
	//	// Attempt to send the message asynchronously and handle the response
	//	producer.SendAsync(ctx, asyncMsg, func(msg pulsar.ProducerMessage, err error) {
	//		if err != nil {
	//			log.Fatal(err)
	//		}
	//	})
	//	producer.Flush()
	//}
}

