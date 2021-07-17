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
#include <pulsar/ProtobufNativeSchema.h>
#include "Test.pb.h"  // generated from "pulsar-client/src/test/proto/Test.proto"

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

TEST(SchemaTest, testProtobufNativeSchema) {
    const std::string expectedSchemaJson =
        "{\"fileDescriptorSet\":"
        "\"CtMDCgpUZXN0LnByb3RvEgVwcm90bxoSRXh0ZXJuYWxUZXN0LnByb3RvImUKClN1Yk1lc3NhZ2USCwoDZm9vGAEgASgJEgsKA2"
        "JhchgCIAEoARo9Cg1OZXN0ZWRNZXNzYWdlEgsKA3VybBgBIAEoCRINCgV0aXRsZRgCIAEoCRIQCghzbmlwcGV0cxgDIAMoCSLlAQ"
        "oLVGVzdE1lc3NhZ2USEwoLc3RyaW5nRmllbGQYASABKAkSEwoLZG91YmxlRmllbGQYAiABKAESEAoIaW50RmllbGQYBiABKAUSIQ"
        "oIdGVzdEVudW0YBCABKA4yDy5wcm90by5UZXN0RW51bRImCgtuZXN0ZWRGaWVsZBgFIAEoCzIRLnByb3RvLlN1Yk1lc3NhZ2USFQ"
        "oNcmVwZWF0ZWRGaWVsZBgKIAMoCRI4Cg9leHRlcm5hbE1lc3NhZ2UYCyABKAsyHy5wcm90by5leHRlcm5hbC5FeHRlcm5hbE1lc3"
        "NhZ2UqJAoIVGVzdEVudW0SCgoGU0hBUkVEEAASDAoIRkFJTE9WRVIQAUItCiVvcmcuYXBhY2hlLnB1bHNhci5jbGllbnQuc2NoZW"
        "1hLnByb3RvQgRUZXN0YgZwcm90bzMKoAEKEkV4dGVybmFsVGVzdC5wcm90bxIOcHJvdG8uZXh0ZXJuYWwiOwoPRXh0ZXJuYWxNZX"
        "NzYWdlEhMKC3N0cmluZ0ZpZWxkGAEgASgJEhMKC2RvdWJsZUZpZWxkGAIgASgBQjUKJW9yZy5hcGFjaGUucHVsc2FyLmNsaWVudC"
        "5zY2hlbWEucHJvdG9CDEV4dGVybmFsVGVzdGIGcHJvdG8z\",\"rootMessageTypeName\":\"proto.TestMessage\","
        "\"rootFileDescriptorName\":\"Test.proto\"}";
    const auto schemaInfo = createProtobufNativeSchema(::proto::TestMessage::GetDescriptor());

    ASSERT_EQ(schemaInfo.getSchemaType(), pulsar::PROTOBUF_NATIVE);
    ASSERT_TRUE(schemaInfo.getName().empty());
    ASSERT_EQ(schemaInfo.getSchema(), expectedSchemaJson);
    ASSERT_TRUE(schemaInfo.getProperties().empty());
}
