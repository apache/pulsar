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
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:       "pulsar://localhost:6650",
		IoThreads: 5,
	})

	if err != nil {
		log.Fatal(err)
	}

	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic:        "my-topic",
		ProducerName: "xyz",
	})
	if err != nil {
		log.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		err := producer.SendBytes([]byte(fmt.Sprintf("hello-%d", i)))
		if err != nil {
			log.Fatal(err)
		}
	}

	producer.Close()
}
