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
#include <boost/asio.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/thread.hpp>
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

TEST(AuthPluginTest, testTls) {
    ClientConfiguration config = ClientConfiguration();
    config.setUseTls(true);
    config.setTlsTrustCertsFilePath("../../pulsar-broker/src/test/resources/authentication/tls/cacert.pem");
    config.setTlsAllowInsecureConnection(false);
    AuthenticationPtr auth =
        pulsar::AuthTls::create("../../pulsar-broker/src/test/resources/authentication/tls/client-cert.pem",
                                "../../pulsar-broker/src/test/resources/authentication/tls/client-key.pem");

    ASSERT_TRUE(auth != NULL);
    ASSERT_EQ(auth->getAuthMethodName(), "tls");

    pulsar::AuthenticationDataPtr data;
    ASSERT_EQ(auth->getAuthData(data), pulsar::ResultOk);
    ASSERT_EQ(data->getCommandData(), "none");
    ASSERT_EQ(data->hasDataForTls(), true);
    ASSERT_EQ(auth.use_count(), 1);

    config.setAuth(auth);
    Client client(lookupUrlTls, config);

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
    for (int i = 0; i < numOfMessages; i++) {
        std::string messageContent = prefix + boost::lexical_cast<std::string>(i);
        Message msg = MessageBuilder()
                          .setContent(messageContent)
                          .setProperty("msgIndex", boost::lexical_cast<std::string>(i))
                          .build();
        producer.sendAsync(msg, &sendCallBackTls);
        LOG_INFO("sending message " << messageContent);
    }

    Message receivedMsg;
    int i = 0;
    while (consumer.receive(receivedMsg, 5000) == ResultOk) {
        std::string expectedMessageContent = prefix + boost::lexical_cast<std::string>(i);
        LOG_INFO("Received Message with [ content - "
                 << receivedMsg.getDataAsString() << "] [ messageID = " << receivedMsg.getMessageId() << "]");
        ASSERT_EQ(receivedMsg.getProperty("msgIndex"), boost::lexical_cast<std::string>(i++));
        ASSERT_EQ(expectedMessageContent, receivedMsg.getDataAsString());
        ASSERT_EQ(ResultOk, consumer.acknowledge(receivedMsg));
    }
    // Number of messages produced
    ASSERT_EQ(globalTestTlsMessagesCounter, numOfMessages);

    // Number of messages consumed
    ASSERT_EQ(i, numOfMessages);
}

TEST(AuthPluginTest, testTlsDetectPulsarSsl) {
    ClientConfiguration config = ClientConfiguration();
    config.setTlsTrustCertsFilePath("../../pulsar-broker/src/test/resources/authentication/tls/cacert.pem");
    config.setTlsAllowInsecureConnection(false);
    AuthenticationPtr auth =
        pulsar::AuthTls::create("../../pulsar-broker/src/test/resources/authentication/tls/client-cert.pem",
                                "../../pulsar-broker/src/test/resources/authentication/tls/client-key.pem");
    config.setAuth(auth);

    Client client("pulsar+ssl://localhost:9886", config);

    std::string topicName = "persistent://property/cluster/namespace/test-tls-detect";

    Producer producer;
    Promise<Result, Producer> producerPromise;
    client.createProducerAsync(topicName, WaitForCallbackValue<Producer>(producerPromise));
    Future<Result, Producer> producerFuture = producerPromise.getFuture();
    Result result = producerFuture.get(producer);
    ASSERT_EQ(ResultOk, result);
}

TEST(AuthPluginTest, testTlsDetectHttps) {
    ClientConfiguration config = ClientConfiguration();
    config.setUseTls(true);  // shouldn't be needed soon
    config.setTlsTrustCertsFilePath("../../pulsar-broker/src/test/resources/authentication/tls/cacert.pem");
    config.setTlsAllowInsecureConnection(false);
    AuthenticationPtr auth =
        pulsar::AuthTls::create("../../pulsar-broker/src/test/resources/authentication/tls/client-cert.pem",
                                "../../pulsar-broker/src/test/resources/authentication/tls/client-key.pem");
    config.setAuth(auth);

    Client client("https://localhost:9766", config);

    std::string topicName = "persistent://property/cluster/namespace/test-tls-detect-https";

    Producer producer;
    Promise<Result, Producer> producerPromise;
    client.createProducerAsync(topicName, WaitForCallbackValue<Producer>(producerPromise));
    Future<Result, Producer> producerFuture = producerPromise.getFuture();
    Result result = producerFuture.get(producer);
    ASSERT_EQ(ResultOk, result);
}

namespace testAthenz {
std::string principalToken;
void mockZTS() {
    boost::asio::io_service io;
    boost::asio::ip::tcp::iostream stream;
    boost::asio::ip::tcp::acceptor acceptor(io,
                                            boost::asio::ip::tcp::endpoint(boost::asio::ip::tcp::v4(), 9999));
    acceptor.accept(*stream.rdbuf());
    std::string headerLine;
    while (getline(stream, headerLine)) {
        std::vector<std::string> kv;
        boost::algorithm::split(kv, headerLine, boost::is_any_of(" "));
        if (kv[0] == "Athenz-Principal-Auth:") {
            principalToken = kv[1];
        }

        if (headerLine == "\r" || headerLine == "\n" || headerLine == "\r\n") {
            std::string mockToken = "{\"token\":\"mockToken\",\"expiryTime\":4133980800}";
            stream << "HTTP/1.1 200 OK" << std::endl;
            stream << "Host: localhost" << std::endl;
            stream << "Content-Type: application/json" << std::endl;
            stream << "Content-Length: " << mockToken.size() << std::endl;
            stream << std::endl;
            stream << mockToken << std::endl;
            break;
        }
    }
}
}  // namespace testAthenz

TEST(AuthPluginTest, testAthenz) {
    boost::thread zts(&testAthenz::mockZTS);
    pulsar::AuthenticationDataPtr data;
    std::string params = R"({
        "tenantDomain": "pulsar.test.tenant",
        "tenantService": "service",
        "providerDomain": "pulsar.test.provider",
        "privateKey": "file:../../pulsar-broker/src/test/resources/authentication/tls/client-key.pem",
        "ztsUrl": "http://localhost:9999"
    })";
    pulsar::AuthenticationPtr auth = pulsar::AuthAthenz::create(params);
    ASSERT_EQ(auth->getAuthMethodName(), "athenz");
    ASSERT_EQ(auth->getAuthData(data), pulsar::ResultOk);
    ASSERT_EQ(data->hasDataForHttp(), true);
    ASSERT_EQ(data->hasDataFromCommand(), true);
    ASSERT_EQ(data->getHttpHeaders(), "Athenz-Role-Auth: mockToken");
    ASSERT_EQ(data->getCommandData(), "mockToken");
    zts.join();
    std::vector<std::string> kvs;
    boost::algorithm::split(kvs, testAthenz::principalToken, boost::is_any_of(";"));
    for (std::vector<std::string>::iterator itr = kvs.begin(); itr != kvs.end(); itr++) {
        std::vector<std::string> kv;
        boost::algorithm::split(kv, *itr, boost::is_any_of("="));
        if (kv[0] == "d") {
            ASSERT_EQ(kv[1], "pulsar.test.tenant");
        } else if (kv[0] == "n") {
            ASSERT_EQ(kv[1], "service");
        }
    }
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

TEST(AuthPluginTest, testAuthFactoryTls) {
    pulsar::AuthenticationDataPtr data;
    std::string tlsCertFile = "../../pulsar-broker/src/test/resources/authentication/tls/client-cert.pem";
    std::string tlsKeyFile = "../../pulsar-broker/src/test/resources/authentication/tls/client-key.pem";
    AuthenticationPtr auth =
        pulsar::AuthFactory::create("tls", "tlsCertFile:" + tlsCertFile + ",tlsKeyFile:" + tlsKeyFile);
    ASSERT_EQ(auth->getAuthMethodName(), "tls");
    ASSERT_EQ(auth->getAuthData(data), pulsar::ResultOk);
    ASSERT_EQ(data->hasDataForTls(), true);
    ASSERT_EQ(data->getTlsCertificates(), tlsCertFile);
    ASSERT_EQ(data->getTlsPrivateKey(), tlsKeyFile);

    ClientConfiguration config = ClientConfiguration();
    config.setAuth(auth);
    config.setTlsTrustCertsFilePath("../../pulsar-broker/src/test/resources/authentication/tls/cacert.pem");
    config.setTlsAllowInsecureConnection(false);
    Client client("pulsar+ssl://localhost:9886", config);

    std::string topicName = "persistent://property/cluster/namespace/test-tls-factory";
    Producer producer;
    Promise<Result, Producer> producerPromise;
    client.createProducerAsync(topicName, WaitForCallbackValue<Producer>(producerPromise));
    Future<Result, Producer> producerFuture = producerPromise.getFuture();
    Result result = producerFuture.get(producer);
    ASSERT_EQ(ResultOk, result);
}

TEST(AuthPluginTest, testAuthFactoryAthenz) {
    boost::thread zts(&testAthenz::mockZTS);
    pulsar::AuthenticationDataPtr data;
    std::string params = R"({
        "tenantDomain": "pulsar.test.tenant",
        "tenantService": "service",
        "providerDomain": "pulsar.test.provider",
        "privateKey": "file:../../pulsar-broker/src/test/resources/authentication/tls/client-key.pem",
        "ztsUrl": "http://localhost:9999"
    })";
    pulsar::AuthenticationPtr auth = pulsar::AuthFactory::create("athenz", params);
    ASSERT_EQ(auth->getAuthMethodName(), "athenz");
    ASSERT_EQ(auth->getAuthData(data), pulsar::ResultOk);
    ASSERT_EQ(data->hasDataForHttp(), true);
    ASSERT_EQ(data->hasDataFromCommand(), true);
    ASSERT_EQ(data->getHttpHeaders(), "Athenz-Role-Auth: mockToken");
    ASSERT_EQ(data->getCommandData(), "mockToken");
    zts.join();
    std::vector<std::string> kvs;
    boost::algorithm::split(kvs, testAthenz::principalToken, boost::is_any_of(";"));
    for (std::vector<std::string>::iterator itr = kvs.begin(); itr != kvs.end(); itr++) {
        std::vector<std::string> kv;
        boost::algorithm::split(kv, *itr, boost::is_any_of("="));
        if (kv[0] == "d") {
            ASSERT_EQ(kv[1], "pulsar.test.tenant");
        } else if (kv[0] == "n") {
            ASSERT_EQ(kv[1], "service");
        }
    }
}
