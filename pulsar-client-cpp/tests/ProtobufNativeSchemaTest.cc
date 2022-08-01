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
#include <stdexcept>
#include "PaddingDemo.pb.h"
#include "Test.pb.h"  // generated from "pulsar-client/src/test/proto/Test.proto"

using namespace pulsar;

static std::string lookupUrl = "pulsar://localhost:6650";

TEST(ProtobufNativeSchemaTest, testSchemaJson) {
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

TEST(ProtobufNativeSchemaTest, testAutoCreateSchema) {
    const std::string topicPrefix = "ProtobufNativeSchemaTest-testAutoCreateSchema-";
    Client client(lookupUrl);

    const auto schemaInfo = createProtobufNativeSchema(::proto::TestMessage::GetDescriptor());
    Producer producer;
    ASSERT_EQ(ResultOk, client.createProducer(topicPrefix + "producer",
                                              ProducerConfiguration().setSchema(schemaInfo), producer));
    Consumer consumer;
    ASSERT_EQ(ResultOk, client.subscribe(topicPrefix + "consumer", "my-sub",
                                         ConsumerConfiguration().setSchema(schemaInfo), consumer));
    client.close();
}

TEST(ProtobufNativeSchemaTest, testSchemaIncompatibility) {
    const std::string topic = "ProtobufNativeSchemaTest-testSchemaIncompatibility";
    Client client(lookupUrl);

    Producer producer;
    auto createProducerResult = [&](const google::protobuf::Descriptor* descriptor) {
        return client.createProducer(
            topic, ProducerConfiguration().setSchema(createProtobufNativeSchema(descriptor)), producer);
    };

    // Create the protobuf native schema automatically
    ASSERT_EQ(ResultOk, createProducerResult(::proto::TestMessage::GetDescriptor()));
    producer.close();

    // Try to create producer with another protobuf generated class
    ASSERT_EQ(ResultIncompatibleSchema,
              createProducerResult(::proto::external::ExternalMessage::GetDescriptor()));

    // Try to create producer with the original schema again
    ASSERT_EQ(ResultOk, createProducerResult(::proto::TestMessage::GetDescriptor()));

    // createProtobufNativeSchema() cannot accept a null descriptor
    try {
        createProducerResult(nullptr);
    } catch (const std::invalid_argument& e) {
        ASSERT_STREQ(e.what(), "descriptor is null");
    }

    client.close();
}

TEST(ProtobufNativeSchemaTest, testEndToEnd) {
    const std::string topic = "ProtobufSchemaTest-testEndToEnd";
    Client client(lookupUrl);

    const auto schemaInfo = createProtobufNativeSchema(::proto::TestMessage::GetDescriptor());
    Consumer consumer;
    ASSERT_EQ(ResultOk,
              client.subscribe(topic, "my-sub", ConsumerConfiguration().setSchema(schemaInfo), consumer));
    Producer producer;
    ASSERT_EQ(ResultOk,
              client.createProducer(topic, ProducerConfiguration().setSchema(schemaInfo), producer));

    // Send a message that is serialized from a ProtoBuf class
    ::proto::TestMessage testMessage;
    testMessage.set_testenum(::proto::TestEnum::FAILOVER);
    std::string content(testMessage.ByteSizeLong(), '\0');
    testMessage.SerializeToArray(const_cast<char*>(content.data()), content.size());
    ASSERT_EQ(ResultOk, producer.send(MessageBuilder().setContent(content).build()));

    // Receive a message and parse it to the ProtoBuf class
    ::proto::TestMessage receivedTestMessage;
    ASSERT_EQ(receivedTestMessage.testenum(), ::proto::TestEnum::SHARED);

    Message msg;
    ASSERT_EQ(ResultOk, consumer.receive(msg, 3000));
    receivedTestMessage.ParseFromArray(msg.getData(), msg.getLength());
    ASSERT_EQ(receivedTestMessage.testenum(), ::proto::TestEnum::FAILOVER);

    ASSERT_TRUE(msg.hasSchemaVersion());
    ASSERT_EQ(msg.getSchemaVersion(), std::string(8L, '\0'));

    client.close();
}

TEST(ProtobufNativeSchemaTest, testBase64WithPadding) {
    const auto schemaInfo = createProtobufNativeSchema(::padding::demo::Person::GetDescriptor());
    const auto schemaJson = schemaInfo.getSchema();
    size_t pos = schemaJson.find(R"(","rootMessageTypeName":)");
    ASSERT_NE(pos, std::string::npos);
    ASSERT_TRUE(pos > 0);
    ASSERT_EQ(schemaJson[pos - 1], '=');  // the tail of fileDescriptorSet is a padding character

    Client client(lookupUrl);

    const std::string topic = "ProtobufSchemaTest-testBase64WithPadding";
    Producer producer;
    ASSERT_EQ(ResultOk,
              client.createProducer(topic, ProducerConfiguration().setSchema(schemaInfo), producer));

    client.close();
}
