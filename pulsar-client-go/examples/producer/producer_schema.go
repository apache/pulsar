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

	jsonSchema := pulsar.NewJsonSchema(exampleSchemaDef, nil)
	producer, err := client.CreateProducerWithSchema(pulsar.ProducerOptions{
		Topic: topicName,
	}, jsonSchema)
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()
	err = producer.Send(context.Background(), pulsar.ProducerMessage{
		Value: &testJson{
			ID:   100,
			Name: "pulsar",
		},
	})
	if err != nil {
		log.Fatal(err)
	}
}
