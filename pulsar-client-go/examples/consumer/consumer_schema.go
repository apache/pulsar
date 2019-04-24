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
	"context"
	"log"
	"os"

	"github.com/apache/pulsar/pulsar-client-go/pulsar"
)

type testJson struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

var exampleSchemaDef = "{\"type\":\"record\",\"name\":\"Example\",\"namespace\":\"test\"," +
	"\"fields\":[{\"name\":\"ID\",\"type\":\"int\"},{\"name\":\"Name\",\"type\":\"string\"}]}"

var (
	serviceURL = os.Args[1]
	topicName  = os.Args[2]
)

func createClient() pulsar.Client {
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: serviceURL,
	})
	if err != nil {
		log.Fatal(err)
	}
	return client
}

func main() {
	client := createClient()
	defer client.Close()

	var s testJson

	consumerJS := pulsar.NewJsonSchema(exampleSchemaDef, nil)
	consumer, err := client.SubscribeWithSchema(pulsar.ConsumerOptions{
		Topic:               topicName,
		SubscriptionName:    "sub-2",
		SubscriptionInitPos: pulsar.Earliest,
	}, consumerJS)
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()
	msg, err := consumer.Receive(context.Background())
	if err != nil {
		log.Fatal(err)
	}

	err = msg.GetValue(&s)
	if err != nil && s.ID != 100 && s.Name != "pulsar" {
		log.Fatalf("schema decode error:%v", err)
	}
}
