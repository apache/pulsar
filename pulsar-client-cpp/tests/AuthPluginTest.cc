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
#include "pulsar/Authentication.h"
#include <gtest/gtest.h>
#include <pulsar/Client.h>
#include <boost/lexical_cast.hpp>
#include <lib/LogUtils.h>

#include "lib/Future.h"
#include "lib/Utils.h"
DECLARE_LOG_OBJECT()

using namespace pulsar;

int globalTestTlsMessagesCounter = 0;
static std::string lookupUrlTls = "pulsar+ssl://localhost:9886";

static void sendCallBackTls(Result r, const Message& msg) {
    ASSERT_EQ(r, ResultOk);
    std::string prefix = "test-tls-message-";
    std::string messageContent = prefix + boost::lexical_cast<std::string>(globalTestTlsMessagesCounter++);
    ASSERT_EQ(messageContent, msg.getDataAsString());
    LOG_DEBUG("Received publish acknowledgement for " << msg.getDataAsString());
}

TEST(AuthPluginTest, testCreate) {
    pulsar::AuthenticationDataPtr data;

    pulsar::AuthenticationPtr auth = pulsar::AuthFactory::create("../lib/auth/libauthtls.so");
    ASSERT_TRUE(auth != NULL);
    ASSERT_EQ(auth->getAuthMethodName(), "tls");
    ASSERT_EQ(auth->getAuthData(data), pulsar::ResultOk);
    ASSERT_EQ(data->getCommandData(), "none");
    ASSERT_EQ(data->hasDataForTls(), true);
    ASSERT_EQ(auth.use_count(), 1);
}

TEST(AuthPluginTest, testTls) {
    ClientConfiguration config = ClientConfiguration();
    config.setUseTls(true);
    std::string certfile = "../../pulsar-broker/src/test/resources/authentication/tls/cacert.pem";
    std::string params =  "tlsCertFile:../../pulsar-broker/src/test/resources/authentication/tls/client-cert.pem,tlsKeyFile:../../pulsar-broker/src/test/resources/authentication/tls/client-key.pem";
    config.setTlsTrustCertsFilePath(certfile);
    config.setTlsAllowInsecureConnection(false);
    AuthenticationPtr auth = pulsar::AuthFactory::create("../lib/auth/libauthtls.so", params);
    config.setAuth(auth);
    Client client(lookupUrlTls,config);

    std::string topicName = "persistent://property/cluster/namespace/test-tls";
    std::string subName = "subscription-name";
    int numOfMessages = 10;

    Producer producer;
    Promise<Result, Producer> producerPromise;
    client.createProducerAsync(topicName, WaitForCallbackValue<Producer>(producerPromise));
    Future<Result, Producer> producerFuture = producerPromise.getFuture();
    Result result = producerFuture.get(producer);
    ASSERT_EQ(ResultOk, result);

    Consumer consumer;
    Promise<Result, Consumer> consumerPromise;
    client.subscribeAsync(topicName, subName, WaitForCallbackValue<Consumer>(consumerPromise));
    Future<Result, Consumer> consumerFuture = consumerPromise.getFuture();
    result = consumerFuture.get(consumer);
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
    std::string prefix = "test-tls-message-";
    for (int i = 0; i<numOfMessages; i++) {
        std::string messageContent = prefix + boost::lexical_cast<std::string>(i);
        Message msg = MessageBuilder().setContent(messageContent).setProperty("msgIndex", boost::lexical_cast<std::string>(i)).build();
        producer.sendAsync(msg, &sendCallBackTls);
        LOG_INFO("sending message " << messageContent);
    }

    Message receivedMsg;
    int i = 0;
    while (consumer.receive(receivedMsg, 5000) == ResultOk) {
        std::string expectedMessageContent = prefix + boost::lexical_cast<std::string>(i);
        LOG_INFO("Received Message with [ content - " << receivedMsg.getDataAsString() << "] [ messageID = " << receivedMsg.getMessageId() << "]");
        ASSERT_EQ(receivedMsg.getProperty("msgIndex"), boost::lexical_cast<std::string>(i++));
        ASSERT_EQ(expectedMessageContent, receivedMsg.getDataAsString());
        ASSERT_EQ(ResultOk, consumer.acknowledge(receivedMsg));
    }
    // Number of messages produced
    ASSERT_EQ(globalTestTlsMessagesCounter, numOfMessages);

    // Number of messages consumed
    ASSERT_EQ(i, numOfMessages);
}

TEST(AuthPluginTest, testDisable) {
    pulsar::AuthenticationDataPtr data;

    pulsar::AuthenticationPtr auth = pulsar::AuthFactory::Disabled();
    ASSERT_TRUE(auth != NULL);
    ASSERT_EQ(auth->getAuthMethodName(), "none");
    ASSERT_EQ(auth->getAuthData(data), pulsar::ResultOk);
    ASSERT_EQ(data->getCommandData(), "none");
	ASSERT_EQ(auth.use_count(), 1);
}
