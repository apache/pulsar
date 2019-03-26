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
package pulsar

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSchemaInfo(t *testing.T) {
	lookupUrl := "pulsar://localhost:6650"
	exampleSchema := "{\"type\":\"record\",\"name\":\"Example\",\"namespace\":\"test\"," +
		"\"fields\":[{\"name\":\"a\",\"type\":\"int\"},{\"name\":\"b\",\"type\":\"int\"}]}"
	client, err := NewClient(ClientOptions{
		URL: lookupUrl,
	})
	assert.Nil(t, err)
	defer client.Close()

	schemaAvroInfo := NewSchemaInfo("avro", exampleSchema, AVRO)
	producer, err := client.CreateProducer(ProducerOptions{
		Topic:      "topic-avro",
		SchemaInfo: *schemaAvroInfo,
	})

	assert.Nil(t, err)
	defer producer.Close()

	schemaJsonInfo := NewSchemaInfo("json", "{}", JSON)
	producer1, err := client.CreateProducer(ProducerOptions{
		Topic:      "topic-json",
		SchemaInfo: *schemaJsonInfo,
	})

	assert.Nil(t, err)
 	producer1.Close()

	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:            "topic-avro",
		SubscriptionName: "sub-2",
		Type:             Shared,
	})
	assert.Nil(t, err)
	defer consumer.Close()

	schemaInfo12 := NewSchemaInfo("avro", exampleSchema, AVRO)
	consumer, err = client.Subscribe(ConsumerOptions{
		Type:             Shared,
		Topic:            "topic-avro",
		SubscriptionName: "sub-2",
		Schema:           *schemaInfo12,
	})
	assert.Nil(t, err)
	defer consumer.Close()
}
