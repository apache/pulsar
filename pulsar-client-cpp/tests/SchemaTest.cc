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
    ASSERT_EQ(res, ResultOk);

    // Check schema version
    ASSERT_FALSE(producer.getSchemaVersion().empty());
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

    client.close();
}

TEST(SchemaTest, testHasSchemaVersion) {
    Client client(lookupUrl);
    std::string topic = "SchemaTest-HasSchemaVersion";
    SchemaInfo stringSchema(SchemaType::STRING, "String", "");

    Consumer consumer;
    ASSERT_EQ(ResultOk, client.subscribe(topic + "1", "sub", ConsumerConfiguration().setSchema(stringSchema),
                                         consumer));
    Producer batchedProducer;
    ASSERT_EQ(ResultOk, client.createProducer(topic + "1", ProducerConfiguration().setSchema(stringSchema),
                                              batchedProducer));
    Producer nonBatchedProducer;
    ASSERT_EQ(ResultOk, client.createProducer(topic + "1", ProducerConfiguration().setSchema(stringSchema),
                                              nonBatchedProducer));

    ASSERT_EQ(ResultOk, batchedProducer.send(MessageBuilder().setContent("msg-0").build()));
    ASSERT_EQ(ResultOk, nonBatchedProducer.send(MessageBuilder().setContent("msg-1").build()));

    Message msgs[2];
    ASSERT_EQ(ResultOk, consumer.receive(msgs[0], 3000));
    ASSERT_EQ(ResultOk, consumer.receive(msgs[1], 3000));

    std::string schemaVersion(8, '\0');
    ASSERT_EQ(msgs[0].getDataAsString(), "msg-0");
    ASSERT_TRUE(msgs[0].hasSchemaVersion());
    ASSERT_EQ(msgs[0].getSchemaVersion(), schemaVersion);

    ASSERT_EQ(msgs[1].getDataAsString(), "msg-1");
    ASSERT_TRUE(msgs[1].hasSchemaVersion());
    ASSERT_EQ(msgs[1].getSchemaVersion(), schemaVersion);

    client.close();
}
