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

#include <pulsar/Authentication.h>

#include <gtest/gtest.h>
#include <pulsar/Client.h>
#include <boost/asio.hpp>
#include <boost/algorithm/string.hpp>
#include <lib/LogUtils.h>

#include <string>
#include <fstream>
#include <streambuf>

#include "lib/Future.h"
#include "lib/Utils.h"
DECLARE_LOG_OBJECT()

using namespace pulsar;

static const std::string serviceUrl = "pulsar://localhost:6650";
static const std::string serviceUrlHttp = "http://localhost:8080";

static const std::string tokenPath = "/tmp/pulsar-test-data/tokens/token.txt";

static std::string getToken() {
    std::ifstream file(tokenPath);
    std::string str((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());
    return str;
}

TEST(AuthPluginToken, testToken) {
    ClientConfiguration config = ClientConfiguration();
    std::string token = getToken();
    AuthenticationPtr auth = pulsar::AuthToken::createWithToken(token);

    ASSERT_TRUE(auth != NULL);
    ASSERT_EQ(auth->getAuthMethodName(), "token");

    pulsar::AuthenticationDataPtr data;
    ASSERT_EQ(auth->getAuthData(data), pulsar::ResultOk);
    ASSERT_EQ(data->hasDataFromCommand(), true);
    ASSERT_EQ(data->getCommandData(), token);
    ASSERT_EQ(data->hasDataForTls(), false);
    ASSERT_EQ(data->hasDataForHttp(), true);
    ASSERT_EQ(auth.use_count(), 1);

    config.setAuth(auth);
    Client client(serviceUrl, config);

    std::string topicName = "persistent://private/auth/test-token";
    std::string subName = "subscription-name";
    int numOfMessages = 10;

    Producer producer;
    Result result = client.createProducer(topicName, producer);
    ASSERT_EQ(ResultOk, result);

    Consumer consumer;
    result = client.subscribe(topicName, subName, consumer);
    ASSERT_EQ(ResultOk, result);

    // handling dangling subscriptions
    consumer.unsubscribe();
    client.subscribe(topicName, subName, consumer);

    std::string temp = producer.getTopic();
    ASSERT_EQ(temp, topicName);
    temp = consumer.getTopic();
    ASSERT_EQ(temp, topicName);
    ASSERT_EQ(consumer.getSubscriptionName(), subName);

    // Send Asynchronously
    std::string prefix = "test-token-message-";
    for (int i = 0; i < numOfMessages; i++) {
        std::string messageContent = prefix + std::to_string(i);
        Message msg =
            MessageBuilder().setContent(messageContent).setProperty("msgIndex", std::to_string(i)).build();
        producer.sendAsync(msg, NULL);
        LOG_INFO("sending message " << messageContent);
    }

    producer.flush();

    Message receivedMsg;
    for (int i = 0; i < numOfMessages; i++) {
        Result res = consumer.receive(receivedMsg);
        ASSERT_EQ(ResultOk, res);

        std::string expectedMessageContent = prefix + std::to_string(i);
        LOG_INFO("Received Message with [ content - "
                 << receivedMsg.getDataAsString() << "] [ messageID = " << receivedMsg.getMessageId() << "]");
        ASSERT_EQ(receivedMsg.getProperty("msgIndex"), std::to_string(i));
        ASSERT_EQ(expectedMessageContent, receivedMsg.getDataAsString());
        ASSERT_EQ(ResultOk, consumer.acknowledge(receivedMsg));
    }
}

TEST(AuthPluginToken, testTokenWithHttpUrl) {
    ClientConfiguration config = ClientConfiguration();
    std::string token = getToken();
    config.setAuth(pulsar::AuthToken::createWithToken(token));
    Client client(serviceUrlHttp, config);

    std::string topicName = "persistent://private/auth/test-token-http";
    std::string subName = "subscription-name";
    int numOfMessages = 10;

    Producer producer;
    Result result = client.createProducer(topicName, producer);
    ASSERT_EQ(ResultOk, result);

    Consumer consumer;
    result = client.subscribe(topicName, subName, consumer);
    ASSERT_EQ(ResultOk, result);

    // handling dangling subscriptions
    consumer.unsubscribe();
    client.subscribe(topicName, subName, consumer);

    std::string temp = producer.getTopic();
    ASSERT_EQ(temp, topicName);
    temp = consumer.getTopic();
    ASSERT_EQ(temp, topicName);
    ASSERT_EQ(consumer.getSubscriptionName(), subName);

    // Send Asynchronously
    std::string prefix = "test-token-message-";
    for (int i = 0; i < numOfMessages; i++) {
        std::string messageContent = prefix + std::to_string(i);
        Message msg =
            MessageBuilder().setContent(messageContent).setProperty("msgIndex", std::to_string(i)).build();
        producer.sendAsync(msg, NULL);
        LOG_INFO("sending message " << messageContent);
    }

    producer.flush();

    Message receivedMsg;
    for (int i = 0; i < numOfMessages; i++) {
        Result res = consumer.receive(receivedMsg);
        ASSERT_EQ(ResultOk, res);

        std::string expectedMessageContent = prefix + std::to_string(i);
        LOG_INFO("Received Message with [ content - "
                 << receivedMsg.getDataAsString() << "] [ messageID = " << receivedMsg.getMessageId() << "]");
        ASSERT_EQ(receivedMsg.getProperty("msgIndex"), std::to_string(i));
        ASSERT_EQ(expectedMessageContent, receivedMsg.getDataAsString());
        ASSERT_EQ(ResultOk, consumer.acknowledge(receivedMsg));
    }
}

TEST(AuthPluginToken, testNoAuth) {
    ClientConfiguration config;
    Client client(serviceUrl, config);

    std::string topicName = "persistent://private/auth/test-token";
    std::string subName = "subscription-name";

    Producer producer;
    Result result = client.createProducer(topicName, producer);
    ASSERT_EQ(ResultAuthorizationError, result);

    Consumer consumer;
    result = client.subscribe(topicName, subName, consumer);
    ASSERT_EQ(ResultAuthorizationError, result);
}

TEST(AuthPluginToken, testNoAuthWithHttp) {
    ClientConfiguration config;
    Client client(serviceUrlHttp, config);

    std::string topicName = "persistent://private/auth/test-token";
    std::string subName = "subscription-name";

    Producer producer;
    Result result = client.createProducer(topicName, producer);
    ASSERT_EQ(ResultConnectError, result);

    Consumer consumer;
    result = client.subscribe(topicName, subName, consumer);
    ASSERT_EQ(ResultConnectError, result);
}
