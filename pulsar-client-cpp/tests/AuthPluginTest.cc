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
#include <boost/asio.hpp>
#include <boost/algorithm/string.hpp>
#include <thread>
#include <lib/LogUtils.h>
#include <lib/auth/AuthOauth2.h>

#include "lib/Future.h"
#include "lib/Utils.h"
DECLARE_LOG_OBJECT()

using namespace pulsar;

int globalTestTlsMessagesCounter = 0;
static const std::string serviceUrlTls = "pulsar+ssl://localhost:6651";
static const std::string serviceUrlHttps = "https://localhost:8443";

static const std::string caPath = "../../pulsar-broker/src/test/resources/authentication/tls/cacert.pem";
static const std::string clientPublicKeyPath =
    "../../pulsar-broker/src/test/resources/authentication/tls/client-cert.pem";
static const std::string clientPrivateKeyPath =
    "../../pulsar-broker/src/test/resources/authentication/tls/client-key.pem";

static void sendCallBackTls(Result r, const MessageId& msgId) {
    ASSERT_EQ(r, ResultOk);
    globalTestTlsMessagesCounter++;
}

TEST(AuthPluginTest, testTls) {
    ClientConfiguration config = ClientConfiguration();
    config.setTlsTrustCertsFilePath(caPath);
    config.setTlsAllowInsecureConnection(false);
    AuthenticationPtr auth = pulsar::AuthTls::create(clientPublicKeyPath, clientPrivateKeyPath);

    ASSERT_TRUE(auth != NULL);
    ASSERT_EQ(auth->getAuthMethodName(), "tls");

    pulsar::AuthenticationDataPtr data;
    ASSERT_EQ(auth->getAuthData(data), pulsar::ResultOk);
    ASSERT_EQ(data->getCommandData(), "none");
    ASSERT_EQ(data->hasDataForTls(), true);
    ASSERT_EQ(auth.use_count(), 1);

    config.setAuth(auth);
    Client client(serviceUrlTls, config);

    std::string topicName = "persistent://private/auth/test-tls";
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
        std::string messageContent = prefix + std::to_string(i);
        Message msg =
            MessageBuilder().setContent(messageContent).setProperty("msgIndex", std::to_string(i)).build();
        producer.sendAsync(msg, &sendCallBackTls);
        LOG_INFO("sending message " << messageContent);
    }

    Message receivedMsg;
    int i = 0;
    while (consumer.receive(receivedMsg, 5000) == ResultOk) {
        std::string expectedMessageContent = prefix + std::to_string(i);
        LOG_INFO("Received Message with [ content - "
                 << receivedMsg.getDataAsString() << "] [ messageID = " << receivedMsg.getMessageId() << "]");
        ASSERT_EQ(receivedMsg.getProperty("msgIndex"), std::to_string(i++));
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
    config.setTlsTrustCertsFilePath(caPath);
    config.setTlsAllowInsecureConnection(false);
    config.setAuth(pulsar::AuthTls::create(clientPublicKeyPath, clientPrivateKeyPath));

    Client client(serviceUrlTls, config);

    std::string topicName = "persistent://private/auth/test-tls-detect";

    Producer producer;
    Promise<Result, Producer> producerPromise;
    client.createProducerAsync(topicName, WaitForCallbackValue<Producer>(producerPromise));
    Future<Result, Producer> producerFuture = producerPromise.getFuture();
    Result result = producerFuture.get(producer);
    ASSERT_EQ(ResultOk, result);
}

TEST(AuthPluginTest, testTlsDetectPulsarSslWithHostNameValidation) {
    ClientConfiguration config = ClientConfiguration();
    config.setTlsTrustCertsFilePath(caPath);
    config.setTlsAllowInsecureConnection(false);
    config.setValidateHostName(true);
    config.setAuth(pulsar::AuthTls::create(clientPublicKeyPath, clientPrivateKeyPath));

    Client client(serviceUrlTls, config);
    std::string topicName = "persistent://private/auth/testTlsDetectPulsarSslWithHostNameValidation";

    Producer producer;
    Result res = client.createProducer(topicName, producer);
    ASSERT_EQ(ResultConnectError, res);
}

TEST(AuthPluginTest, testTlsDetectHttps) {
    ClientConfiguration config = ClientConfiguration();
    config.setUseTls(true);  // shouldn't be needed soon
    config.setTlsTrustCertsFilePath(caPath);
    config.setTlsAllowInsecureConnection(false);
    config.setAuth(pulsar::AuthTls::create(clientPublicKeyPath, clientPrivateKeyPath));

    Client client(serviceUrlHttps, config);

    std::string topicName = "persistent://private/auth/test-tls-detect-https";

    Producer producer;
    Promise<Result, Producer> producerPromise;
    client.createProducerAsync(topicName, WaitForCallbackValue<Producer>(producerPromise));
    Future<Result, Producer> producerFuture = producerPromise.getFuture();
    Result result = producerFuture.get(producer);
    ASSERT_EQ(ResultOk, result);
}

TEST(AuthPluginTest, testTlsDetectHttpsWithHostNameValidation) {
    ClientConfiguration config = ClientConfiguration();
    config.setUseTls(true);  // shouldn't be needed soon
    config.setTlsTrustCertsFilePath(caPath);
    config.setTlsAllowInsecureConnection(false);
    config.setAuth(pulsar::AuthTls::create(clientPublicKeyPath, clientPrivateKeyPath));
    config.setValidateHostName(true);

    Client client(serviceUrlHttps, config);

    std::string topicName = "persistent://private/auth/test-tls-detect-https-with-hostname-validation";

    Producer producer;
    Result res = client.createProducer(topicName, producer);
    ASSERT_NE(ResultOk, res);
}

namespace testAthenz {
std::string principalToken;
void mockZTS(int port) {
    LOG_INFO("-- MockZTS started");
    boost::asio::io_service io;
    boost::asio::ip::tcp::iostream stream;
    boost::asio::ip::tcp::acceptor acceptor(io,
                                            boost::asio::ip::tcp::endpoint(boost::asio::ip::tcp::v4(), port));

    LOG_INFO("-- MockZTS waiting for connnection");
    acceptor.accept(*stream.rdbuf());
    LOG_INFO("-- MockZTS got connection");

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

    LOG_INFO("-- MockZTS exiting");
}
}  // namespace testAthenz

TEST(AuthPluginTest, testAthenz) {
    std::thread zts(std::bind(&testAthenz::mockZTS, 9999));
    pulsar::AuthenticationDataPtr data;
    std::string params = R"({
        "tenantDomain": "pulsar.test.tenant",
        "tenantService": "service",
        "providerDomain": "pulsar.test.provider",
        "privateKey": "file:)" +
                         clientPrivateKeyPath + R"(",
        "ztsUrl": "http://localhost:9999"
    })";

    LOG_INFO("PARAMS: " << params);
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
    AuthenticationPtr auth = pulsar::AuthFactory::create(
        "tls", "tlsCertFile:" + clientPublicKeyPath + ",tlsKeyFile:" + clientPrivateKeyPath);
    ASSERT_EQ(auth->getAuthMethodName(), "tls");
    ASSERT_EQ(auth->getAuthData(data), pulsar::ResultOk);
    ASSERT_EQ(data->hasDataForTls(), true);
    ASSERT_EQ(data->getTlsCertificates(), clientPublicKeyPath);
    ASSERT_EQ(data->getTlsPrivateKey(), clientPrivateKeyPath);

    ClientConfiguration config = ClientConfiguration();
    config.setAuth(auth);
    config.setTlsTrustCertsFilePath(caPath);
    config.setTlsAllowInsecureConnection(false);
    Client client(serviceUrlTls, config);

    std::string topicName = "persistent://private/auth/test-tls-factory";
    Producer producer;
    Promise<Result, Producer> producerPromise;
    client.createProducerAsync(topicName, WaitForCallbackValue<Producer>(producerPromise));
    Future<Result, Producer> producerFuture = producerPromise.getFuture();
    Result result = producerFuture.get(producer);
    ASSERT_EQ(ResultOk, result);
}

TEST(AuthPluginTest, testAuthFactoryAthenz) {
    std::thread zts(std::bind(&testAthenz::mockZTS, 9998));
    pulsar::AuthenticationDataPtr data;
    std::string params = R"({
        "tenantDomain": "pulsar.test2.tenant",
        "tenantService": "service",
        "providerDomain": "pulsar.test.provider",
        "privateKey": "file:)" +
                         clientPrivateKeyPath + R"(",
        "ztsUrl": "http://localhost:9998"
    })";
    LOG_INFO("PARAMS: " << params);
    pulsar::AuthenticationPtr auth = pulsar::AuthFactory::create("athenz", params);
    ASSERT_EQ(auth->getAuthMethodName(), "athenz");
    ASSERT_EQ(auth->getAuthData(data), pulsar::ResultOk);
    ASSERT_EQ(data->hasDataForHttp(), true);
    ASSERT_EQ(data->hasDataFromCommand(), true);
    ASSERT_EQ(data->getHttpHeaders(), "Athenz-Role-Auth: mockToken");
    ASSERT_EQ(data->getCommandData(), "mockToken");

    LOG_INFO("Calling zts.join()");
    zts.join();
    LOG_INFO("Done zts.join()");

    std::vector<std::string> kvs;
    boost::algorithm::split(kvs, testAthenz::principalToken, boost::is_any_of(";"));
    for (std::vector<std::string>::iterator itr = kvs.begin(); itr != kvs.end(); itr++) {
        std::vector<std::string> kv;
        boost::algorithm::split(kv, *itr, boost::is_any_of("="));
        if (kv[0] == "d") {
            ASSERT_EQ(kv[1], "pulsar.test2.tenant");
        } else if (kv[0] == "n") {
            ASSERT_EQ(kv[1], "service");
        }
    }
}

TEST(AuthPluginTest, testOauth2) {
    // test success get token from oauth2 server.
    pulsar::AuthenticationDataPtr data;
    std::string params = R"({
        "type": "client_credentials",
        "issuer_url": "https://dev-kt-aa9ne.us.auth0.com",
        "client_id": "Xd23RHsUnvUlP7wchjNYOaIfazgeHd9x",
        "client_secret": "rT7ps7WY8uhdVuBTKWZkttwLdQotmdEliaM5rLfmgNibvqziZ-g07ZH52N_poGAb",
        "audience": "https://dev-kt-aa9ne.us.auth0.com/api/v2/"})";

    int expectedTokenLength = 3379;
    LOG_INFO("PARAMS: " << params);
    pulsar::AuthenticationPtr auth = pulsar::AuthOauth2::create(params);

    ASSERT_EQ(auth->getAuthMethodName(), "token");
    ASSERT_EQ(auth->getAuthData(data), pulsar::ResultOk);
    ASSERT_EQ(data->hasDataForHttp(), true);
    ASSERT_EQ(data->hasDataFromCommand(), true);
    ASSERT_EQ(data->getCommandData().length(), expectedTokenLength);
}

TEST(AuthPluginTest, testOauth2WrongSecret) {
    pulsar::AuthenticationDataPtr data;

    std::string params = R"({
    "type": "client_credentials",
    "issuer_url": "https://dev-kt-aa9ne.us.auth0.com",
    "client_id": "Xd23RHsUnvUlP7wchjNYOaIfazgeHd9x",
    "client_secret": "rT7ps7WY8uhdVuBTKWZkttwLdQotmdEliaM5rLfmgNibvqziZ",
    "audience": "https://dev-kt-aa9ne.us.auth0.com/api/v2/"})";

    LOG_INFO("PARAMS: " << params);
    pulsar::AuthenticationPtr auth = pulsar::AuthOauth2::create(params);
    ASSERT_EQ(auth->getAuthMethodName(), "token");
    ASSERT_EQ(auth->getAuthData(data), ResultAuthenticationError);
}

TEST(AuthPluginTest, testOauth2CredentialFile) {
    // test success get token from oauth2 server.
    pulsar::AuthenticationDataPtr data;
    std::string params = R"({
        "type": "client_credentials",
        "issuer_url": "https://dev-kt-aa9ne.us.auth0.com",
        "private_key": "../../pulsar-broker/src/test/resources/authentication/token/cpp_credentials_file.json",
        "audience": "https://dev-kt-aa9ne.us.auth0.com/api/v2/"})";

    int expectedTokenLength = 3379;
    LOG_INFO("PARAMS: " << params);
    pulsar::AuthenticationPtr auth = pulsar::AuthOauth2::create(params);
    ASSERT_EQ(auth->getAuthMethodName(), "token");
    ASSERT_EQ(auth->getAuthData(data), pulsar::ResultOk);
    ASSERT_EQ(data->hasDataForHttp(), true);
    ASSERT_EQ(data->hasDataFromCommand(), true);
    ASSERT_EQ(data->getCommandData().length(), expectedTokenLength);
}

TEST(AuthPluginTest, testOauth2RequestBody) {
    ParamMap params;
    params["issuer_url"] = "https://dev-kt-aa9ne.us.auth0.com";
    params["client_id"] = "Xd23RHsUnvUlP7wchjNYOaIfazgeHd9x";
    params["client_secret"] = "rT7ps7WY8uhdVuBTKWZkttwLdQotmdEliaM5rLfmgNibvqziZ-g07ZH52N_poGAb";
    params["audience"] = "https://dev-kt-aa9ne.us.auth0.com/api/v2/";

    auto createExpectedResult = [&] {
        auto paramsCopy = params;
        paramsCopy.emplace("grant_type", "client_credentials");
        paramsCopy.erase("issuer_url");
        return paramsCopy;
    };

    const auto expectedResult1 = createExpectedResult();
    ClientCredentialFlow flow1(params);
    ASSERT_EQ(flow1.generateParamMap(), expectedResult1);

    params["scope"] = "test-scope";
    const auto expectedResult2 = createExpectedResult();
    ClientCredentialFlow flow2(params);
    ASSERT_EQ(flow2.generateParamMap(), expectedResult2);
}

TEST(AuthPluginTest, testOauth2Failure) {
    ParamMap params;
    auto addKeyValue = [&](const std::string& key, const std::string& value) {
        params[key] = value;
        LOG_INFO("Configure \"" << key << "\" to \"" << value << "\"");
    };

    auto createClient = [&]() -> Client {
        ClientConfiguration conf;
        conf.setAuth(AuthOauth2::create(params));
        return {"pulsar://localhost:6650", conf};
    };

    const std::string topic = "AuthPluginTest-testOauth2Failure";
    Producer producer;

    // No issuer_url
    auto client1 = createClient();
    ASSERT_EQ(client1.createProducer(topic, producer), ResultAuthenticationError);
    client1.close();

    // Invalid issuer_url
    addKeyValue("issuer_url", "hello");
    auto client2 = createClient();
    ASSERT_EQ(client2.createProducer(topic, producer), ResultAuthenticationError);
    client2.close();

    addKeyValue("issuer_url", "https://google.com");
    auto client3 = createClient();
    ASSERT_EQ(client3.createProducer(topic, producer), ResultAuthenticationError);
    client3.close();

    // No client id and secret
    addKeyValue("issuer_url", "https://dev-kt-aa9ne.us.auth0.com");
    auto client4 = createClient();
    ASSERT_EQ(client4.createProducer(topic, producer), ResultAuthenticationError);
    client4.close();

    // Invalid client_id and client_secret
    addKeyValue("client_id", "my_id");
    addKeyValue("client_secret", "my-secret");
    auto client5 = createClient();
    ASSERT_EQ(client5.createProducer(topic, producer), ResultAuthenticationError);
    client5.close();
}
