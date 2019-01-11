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
#include <gtest/gtest.h>
#include <pulsar/Client.h>

using namespace pulsar;

static std::string lookupUrl = "pulsar://localhost:6650";

static const std::string exampleSchema =
    "{\"type\":\"record\",\"name\":\"Example\",\"namespace\":\"test\","
    "\"fields\":[{\"name\":\"a\",\"type\":\"int\"},{\"name\":\"b\",\"type\":\"int\"}]}";

TEST(SchemaTest, testSchema) {
    ClientConfiguration config;
    Client client(lookupUrl);
    Result res;

    Producer producer;
    ProducerConfiguration producerConf;
    producerConf.setSchema(SchemaInfo(AVRO, "Avro", exampleSchema));
    res = client.createProducer("topic-avro", producerConf, producer);
    producer.close();

    ASSERT_EQ(ResultOk, res);

    // Creating producer with no schema on same topic should fail
    producerConf.setSchema(SchemaInfo(JSON, "Json", "{}"));
    res = client.createProducer("topic-avro", producerConf, producer);
    ASSERT_EQ(ResultIncompatibleSchema, res);

    // Creating producer with no schema on same topic should succeed
    // because standalone broker is configured by default to not
    // require the schema to be set
    res = client.createProducer("topic-avro", producer);
    ASSERT_EQ(ResultOk, res);

    ConsumerConfiguration consumerConf;
    Consumer consumer;
    // Subscribing with no schema will still succeed
    res = client.subscribe("topic-avro", "sub-1", consumerConf, consumer);
    ASSERT_EQ(ResultOk, res);

    // Subscribing with same Avro schema will succeed
    consumerConf.setSchema(SchemaInfo(AVRO, "Avro", exampleSchema));
    res = client.subscribe("topic-avro", "sub-2", consumerConf, consumer);
    ASSERT_EQ(ResultOk, res);

    // Subscribing with different schema type will fail
    consumerConf.setSchema(SchemaInfo(JSON, "Json", "{}"));
    res = client.subscribe("topic-avro", "sub-2", consumerConf, consumer);
    ASSERT_EQ(ResultIncompatibleSchema, res);
}
