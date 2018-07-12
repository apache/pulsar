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
#include <boost/lexical_cast.hpp>
#include <lib/LogUtils.h>
#include <pulsar/MessageBuilder.h>
#include <lib/Commands.h>
#include <lib/Latch.h>
#include <sstream>
#include "boost/date_time/posix_time/posix_time.hpp"
#include "CustomRoutingPolicy.h"
#include <boost/thread.hpp>
#include <boost/thread/mutex.hpp>
#include <lib/TopicName.h>
#include "PulsarFriend.h"
#include "HttpHelper.h"
#include <set>
#include <vector>
#include "lib/Future.h"
#include "lib/Utils.h"
DECLARE_LOG_OBJECT()

using namespace pulsar;

boost::mutex mutex_;
static int globalTestBatchMessagesCounter = 0;
static int globalCount = 0;
static long globalResendMessageCount = 0;
static std::string lookupUrl = "pulsar://localhost:8885";
static std::string adminUrl = "http://localhost:8765/";

static void messageListenerFunction(Consumer consumer, const Message& msg) {
    globalCount++;
    consumer.acknowledge(msg);
}

static void messageListenerFunctionWithoutAck(Consumer consumer, const Message& msg, Latch& latch,
                                              const std::string& content) {
    globalCount++;
    ASSERT_EQ(content, msg.getDataAsString());
    latch.countdown();
}

static void sendCallBack(Result r, const Message& msg, std::string prefix) {
    ASSERT_EQ(r, ResultOk);
    std::string messageContent = prefix + boost::lexical_cast<std::string>(globalTestBatchMessagesCounter++);
    ASSERT_EQ(messageContent, msg.getDataAsString());
    LOG_DEBUG("Received publish acknowledgement for " << msg.getDataAsString());
}

static void sendCallBack(Result r, const Message& msg, std::string prefix, double percentage,
                         uint64_t delayInMicros) {
    if ((rand() % 100) <= percentage) {
        usleep(delayInMicros);
    }
    sendCallBack(r, msg, prefix);
}

class EncKeyReader : public CryptoKeyReader {
   private:
    void readFile(std::string fileName, std::string& fileContents) const {
        std::ifstream ifs(fileName);
        std::stringstream fileStream;
        fileStream << ifs.rdbuf();

        fileContents = fileStream.str();
    }

   public:
    EncKeyReader() {}

    Result getPublicKey(const std::string& keyName, std::map<std::string, std::string>& metadata,
                        EncryptionKeyInfo& encKeyInfo) const {
        std::string CERT_FILE_PATH =
            "../../pulsar-broker/src/test/resources/certificate/public-key." + keyName;
        std::string keyContents;
        readFile(CERT_FILE_PATH, keyContents);

        encKeyInfo.setKey(keyContents);
        return ResultOk;
    }

    Result getPrivateKey(const std::string& keyName, std::map<std::string, std::string>& metadata,
                         EncryptionKeyInfo& encKeyInfo) const {
        std::string CERT_FILE_PATH =
            "../../pulsar-broker/src/test/resources/certificate/private-key." + keyName;
        std::string keyContents;
        readFile(CERT_FILE_PATH, keyContents);

        encKeyInfo.setKey(keyContents);
        return ResultOk;
    }
};

TEST(BasicEndToEndTest, testBatchMessages) {
    ClientConfiguration config;
    Client client(lookupUrl);
    std::string topicName = "persistent://property/cluster/namespace/test-batch-messages";
    std::string subName = "subscription-name";
    Producer producer;

    // Enable batching on producer side
    int batchSize = 2;
    int numOfMessages = 1000;

    ProducerConfiguration conf;
    conf.setCompressionType(CompressionLZ4);
    conf.setBatchingMaxMessages(batchSize);
    conf.setBatchingEnabled(true);
    conf.setBlockIfQueueFull(true);

    Promise<Result, Producer> producerPromise;
    client.createProducerAsync(topicName, conf, WaitForCallbackValue<Producer>(producerPromise));
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
    std::string prefix = "msg-batch-";
    for (int i = 0; i < numOfMessages; i++) {
        std::string messageContent = prefix + boost::lexical_cast<std::string>(i);
        Message msg = MessageBuilder()
                          .setContent(messageContent)
                          .setProperty("msgIndex", boost::lexical_cast<std::string>(i))
                          .build();
        producer.sendAsync(msg, boost::bind(&sendCallBack, _1, _2, prefix));
        LOG_DEBUG("sending message " << messageContent);
    }

    Message receivedMsg;
    int i = 0;
    while (consumer.receive(receivedMsg, 5000) == ResultOk) {
        std::string expectedMessageContent = prefix + boost::lexical_cast<std::string>(i);
        LOG_DEBUG("Received Message with [ content - " << receivedMsg.getDataAsString() << "] [ messageID = "
                                                       << receivedMsg.getMessageId() << "]");
        ASSERT_EQ(receivedMsg.getProperty("msgIndex"), boost::lexical_cast<std::string>(i++));
        ASSERT_EQ(expectedMessageContent, receivedMsg.getDataAsString());
        ASSERT_EQ(ResultOk, consumer.acknowledge(receivedMsg));
    }
    // Number of messages produced
    ASSERT_EQ(globalTestBatchMessagesCounter, numOfMessages);
    globalTestBatchMessagesCounter = 0;
    // Number of messages consumed
    ASSERT_EQ(i, numOfMessages);
}

void resendMessage(Result r, const Message msg, Producer producer) {
    Lock lock(mutex_);
    if (r != ResultOk) {
        LOG_DEBUG("globalResendMessageCount" << globalResendMessageCount);
        if (++globalResendMessageCount >= 3) {
            return;
        }
    }
    lock.unlock();
    producer.sendAsync(MessageBuilder().build(), boost::bind(resendMessage, _1, _2, producer));
}

TEST(BasicEndToEndTest, testProduceConsume) {
    ClientConfiguration config;
    Client client(lookupUrl);
    std::string topicName = "persistent://prop/unit/ns1/my-topic";
    std::string subName = "my-sub-name";
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
    std::string temp = producer.getTopic();
    ASSERT_EQ(temp, topicName);
    temp = consumer.getTopic();
    ASSERT_EQ(temp, topicName);
    ASSERT_EQ(consumer.getSubscriptionName(), subName);

    // Send synchronously
    std::string content = "msg-1-content";
    Message msg = MessageBuilder().setContent(content).build();
    result = producer.send(msg);
    ASSERT_EQ(ResultOk, result);

    Message receivedMsg;
    consumer.receive(receivedMsg);
    ASSERT_EQ(content, receivedMsg.getDataAsString());
    ASSERT_EQ(ResultOk, consumer.unsubscribe());
    ASSERT_EQ(ResultAlreadyClosed, consumer.close());
    ASSERT_EQ(ResultOk, producer.close());
    ASSERT_EQ(ResultOk, client.close());
}

TEST(BasicEndToEndTest, testLookupThrottling) {
    std::string topicName = "persistent://prop/unit/ns1/testLookupThrottling";
    ClientConfiguration config;
    config.setConcurrentLookupRequest(0);
    Client client(lookupUrl, config);

    Producer producer;
    Result result = client.createProducer(topicName, producer);
    ASSERT_EQ(ResultTooManyLookupRequestException, result);

    Consumer consumer1;
    result = client.subscribe(topicName, "my-sub-name", consumer1);
    ASSERT_EQ(ResultTooManyLookupRequestException, result);
}

TEST(BasicEndToEndTest, testNonExistingTopic) {
    Client client(lookupUrl);
    Producer producer;
    Result result = client.createProducer("persistent://prop//unit/ns1/testNonExistingTopic", producer);
    ASSERT_EQ(ResultInvalidTopicName, result);

    Consumer consumer;
    result = client.subscribe("persistent://prop//unit/ns1/testNonExistingTopic", "my-sub-name", consumer);
    ASSERT_EQ(ResultInvalidTopicName, result);
}

TEST(BasicEndToEndTest, testNonPersistentTopic) {
    std::string topicName = "non-persistent://prop/unit/ns1/testNonPersistentTopic";
    Client client(lookupUrl);
    Producer producer;
    Result result = client.createProducer(topicName, producer);
    ASSERT_EQ(ResultOk, result);

    Consumer consumer;
    result = client.subscribe(topicName, "my-sub-name", consumer);
    ASSERT_EQ(ResultOk, result);
}

TEST(BasicEndToEndTest, testV2TopicProtobuf) {
    std::string topicName = "testV2TopicProtobuf";
    Client client(lookupUrl);
    Producer producer;
    Result result = client.createProducer(topicName, producer);
    ASSERT_EQ(ResultOk, result);

    Consumer consumer;
    result = client.subscribe(topicName, "my-sub-name", consumer);
    ASSERT_EQ(ResultOk, result);

    producer.close();
    consumer.close();
}

TEST(BasicEndToEndTest, testV2TopicHttp) {
    std::string topicName = "testV2TopicHttp";
    Client client(adminUrl);
    Producer producer;
    Result result = client.createProducer(topicName, producer);
    ASSERT_EQ(ResultOk, result);

    Consumer consumer;
    result = client.subscribe(topicName, "my-sub-name", consumer);
    ASSERT_EQ(ResultOk, result);

    producer.close();
    consumer.close();
}

TEST(BasicEndToEndTest, testSingleClientMultipleSubscriptions) {
    std::string topicName = "persistent://prop/unit/ns1/testSingleClientMultipleSubscriptions";

    Client client(lookupUrl);

    Producer producer;
    Result result = client.createProducer(topicName, producer);
    ASSERT_EQ(ResultOk, result);

    Consumer consumer1;
    result = client.subscribe(topicName, "my-sub-name", consumer1);
    ASSERT_EQ(ResultOk, result);

    Consumer consumer2;
    result = client.subscribe(topicName, "my-sub-name", consumer2);
    ASSERT_EQ(ResultConsumerBusy, result);
    // at this point connection gets destroyed because this consumer creation fails
}

TEST(BasicEndToEndTest, testMultipleClientsMultipleSubscriptions) {
    std::string topicName = "persistent://prop/unit/ns1/testMultipleClientsMultipleSubscriptions";
    Client client1(lookupUrl);
    Client client2(lookupUrl);

    Producer producer1;
    Result result = client1.createProducer(topicName, producer1);
    ASSERT_EQ(ResultOk, result);

    Consumer consumer1;
    result = client1.subscribe(topicName, "my-sub-name", consumer1);
    ASSERT_EQ(ResultOk, result);

    Consumer consumer2;
    result = client2.subscribe(topicName, "my-sub-name", consumer2);
    ASSERT_EQ(ResultConsumerBusy, result);

    ASSERT_EQ(ResultOk, producer1.close());
    ASSERT_EQ(ResultOk, consumer1.close());
    ASSERT_EQ(ResultAlreadyClosed, consumer1.close());
    ASSERT_EQ(ResultConsumerNotInitialized, consumer2.close());
    ASSERT_EQ(ResultOk, client1.close());

    // 2 seconds
    usleep(2 * 1000 * 1000);

    ASSERT_EQ(ResultOk, client2.close());
}

TEST(BasicEndToEndTest, testProduceAndConsumeAfterClientClose) {
    std::string topicName = "persistent://prop/unit/ns1/testProduceAndConsumeAfterClientClose";
    Client client(lookupUrl);

    Producer producer;
    Result result = client.createProducer(topicName, producer);
    ASSERT_EQ(ResultOk, result);

    Consumer consumer;
    result = client.subscribe(topicName, "my-sub-name", consumer);

    // Clean dangling subscription
    consumer.unsubscribe();
    result = client.subscribe(topicName, "my-sub-name", consumer);

    ASSERT_EQ(ResultOk, result);

    // Send 10 messages synchronously
    std::string msgContent = "msg-content";
    LOG_INFO("Publishing 10 messages synchronously");
    int numMsg = 0;
    for (; numMsg < 10; numMsg++) {
        Message msg = MessageBuilder()
                          .setContent(msgContent)
                          .setProperty("msgIndex", boost::lexical_cast<std::string>(numMsg))
                          .build();
        ASSERT_EQ(ResultOk, producer.send(msg));
    }

    LOG_INFO("Trying to receive 10 messages");
    Message msgReceived;
    for (int i = 0; i < 10; i++) {
        consumer.receive(msgReceived, 1000);
        LOG_DEBUG("Received message :" << msgReceived.getMessageId());
        ASSERT_EQ(msgContent, msgReceived.getDataAsString());
        ASSERT_EQ(boost::lexical_cast<std::string>(i), msgReceived.getProperty("msgIndex"));
        ASSERT_EQ(ResultOk, consumer.acknowledgeCumulative(msgReceived));
    }

    LOG_INFO("Closing client");
    ASSERT_EQ(ResultOk, client.close());

    LOG_INFO("Trying to publish a message after closing the client");
    Message msg = MessageBuilder()
                      .setContent(msgContent)
                      .setProperty("msgIndex", boost::lexical_cast<std::string>(numMsg))
                      .build();

    ASSERT_EQ(ResultAlreadyClosed, producer.send(msg));

    LOG_INFO("Trying to consume a message after closing the client");
    ASSERT_EQ(ResultAlreadyClosed, consumer.receive(msgReceived));
}

TEST(BasicEndToEndTest, testIamSoFancyCharactersInTopicName) {
    Client client(lookupUrl);
    Producer producer;
    Result result = client.createProducer("persistent://prop/unit/ns1/topic@%*)(&!%$#@#$><?", producer);
    ASSERT_EQ(ResultOk, result);

    Consumer consumer;
    result = client.subscribe("persistent://prop/unit/ns1/topic@%*)(&!%$#@#$><?", "my-sub-name", consumer);
    ASSERT_EQ(ResultOk, result);
}

TEST(BasicEndToEndTest, testSubscribeCloseUnsubscribeSherpaScenario) {
    ClientConfiguration config;
    Client client(lookupUrl, config);
    std::string topicName = "persistent://prop/unit/ns1/::,::bf11";
    std::string subName = "weird-ass-characters-@%*)(&!%$#@#$><?)";
    Producer producer;
    Result result = client.createProducer(topicName, producer);
    ASSERT_EQ(ResultOk, result);

    Consumer consumer;
    result = client.subscribe(topicName, subName, consumer);
    ASSERT_EQ(ResultOk, result);

    result = consumer.close();
    ASSERT_EQ(ResultOk, result);

    Consumer consumer1;
    result = client.subscribe(topicName, subName, consumer1);
    result = consumer1.unsubscribe();
    ASSERT_EQ(ResultOk, result);
}

TEST(BasicEndToEndTest, testInvalidUrlPassed) {
    Client client("localhost:4080");
    std::string topicName = "persistent://prop/unit/ns1/testInvalidUrlPassed";
    std::string subName = "test-sub";
    Producer producer;
    Result result = client.createProducer(topicName, producer);
    ASSERT_EQ(ResultConnectError, result);

    Client client1("test://localhost");
    result = client1.createProducer(topicName, producer);
    ASSERT_EQ(ResultConnectError, result);

    Client client2("test://:4080");
    result = client2.createProducer(topicName, producer);
    ASSERT_EQ(ResultConnectError, result);

    Client client3("");
    result = client3.createProducer(topicName, producer);
    ASSERT_EQ(ResultConnectError, result);

    Client client4("Dream of the day when this will be a valid URL");
    result = client4.createProducer(topicName, producer);
    ASSERT_EQ(ResultConnectError, result);
}

TEST(BasicEndToEndTest, testPartitionedProducerConsumer) {
    Client client(lookupUrl);
    std::string topicName = "persistent://prop/unit/ns/testPartitionedProducerConsumer";

    // call admin api to make it partitioned
    std::string url = adminUrl + "admin/persistent/prop/unit/ns/testPartitionedProducerConsumer/partitions";
    int res = makePutRequest(url, "3");

    LOG_INFO("res = " << res);
    ASSERT_FALSE(res != 204 && res != 409);

    Producer producer;
    Result result = client.createProducer(topicName, producer);
    ASSERT_EQ(ResultOk, result);

    Consumer consumer;
    result = client.subscribe(topicName, "subscription-A", consumer);
    ASSERT_EQ(ResultOk, result);

    for (int i = 0; i < 10; i++) {
        boost::posix_time::ptime t(boost::posix_time::microsec_clock::universal_time());
        long nanoSeconds = t.time_of_day().total_nanoseconds();
        std::stringstream ss;
        ss << nanoSeconds;
        Message msg = MessageBuilder().setContent(ss.str()).setPartitionKey(ss.str()).build();
        ASSERT_EQ(ResultOk, producer.send(msg));
        LOG_DEBUG("Message Timestamp is " << msg.getPublishTimestamp());
        LOG_DEBUG("Message is " << msg);
    }

    ASSERT_EQ(consumer.getSubscriptionName(), "subscription-A");
    for (int i = 0; i < 10; i++) {
        Message m;
        consumer.receive(m, 10000);
        consumer.acknowledge(m);
    }
    client.shutdown();
}

TEST(BasicEndToEndTest, testPartitionedProducerConsumerSubscriptionName) {
    Client client(lookupUrl);
    std::string topicName = "persistent://prop/unit/ns/testPartitionedProducerConsumerSubscriptionName";

    // call admin api to make it partitioned
    std::string url =
        adminUrl + "admin/persistent/prop/unit/ns/testPartitionedProducerConsumerSubscriptionName/partitions";
    int res = makePutRequest(url, "3");

    LOG_INFO("res = " << res);
    ASSERT_FALSE(res != 204 && res != 409);

    Consumer partitionedConsumer;
    Result result = client.subscribe(topicName, "subscription-A", partitionedConsumer);
    ASSERT_EQ(ResultOk, result);

    // The consumer should be already be registered "subscription-A" for all the partitions
    Consumer individualPartitionConsumer;
    result = client.subscribe(topicName + "-partition-0", "subscription-A", individualPartitionConsumer);
    ASSERT_EQ(ResultConsumerBusy, result);

    client.shutdown();
}

TEST(BasicEndToEndTest, testMessageTooBig) {
    ClientConfiguration config;
    Client client(lookupUrl);
    std::string topicName = "persistent://prop/unit/ns1/testMessageTooBig";
    Producer producer;
    Result result = client.createProducer(topicName, producer);
    ASSERT_EQ(ResultOk, result);

    int size = Commands::MaxMessageSize + 1;
    char* content = new char[size];
    Message msg = MessageBuilder().setAllocatedContent(content, size).build();
    result = producer.send(msg);
    ASSERT_EQ(ResultMessageTooBig, result);

    // Anything up to MaxMessageSize should be allowed
    size = Commands::MaxMessageSize;
    msg = MessageBuilder().setAllocatedContent(content, size).build();
    result = producer.send(msg);
    ASSERT_EQ(ResultOk, result);

    delete[] content;
}

TEST(BasicEndToEndTest, testCompressionLZ4) {
    ClientConfiguration config;
    Client client(lookupUrl);
    std::string topicName = "persistent://prop/unit/namespace1/testCompressionLZ4";
    std::string subName = "my-sub-name";
    Producer producer;
    ProducerConfiguration conf;
    conf.setCompressionType(CompressionLZ4);
    Result result = client.createProducer(topicName, conf, producer);
    ASSERT_EQ(ResultOk, result);

    Consumer consumer;
    client.subscribe(topicName, subName, consumer);

    // Send synchronously
    std::string content1 = "msg-1-content";
    Message msg = MessageBuilder().setContent(content1).build();
    result = producer.send(msg);
    ASSERT_EQ(ResultOk, result);

    std::string content2 = "msg-2-content";
    msg = MessageBuilder().setContent(content2).build();
    result = producer.send(msg);
    ASSERT_EQ(ResultOk, result);

    Message receivedMsg;
    consumer.receive(receivedMsg);
    ASSERT_EQ(content1, receivedMsg.getDataAsString());

    consumer.receive(receivedMsg);
    ASSERT_EQ(content2, receivedMsg.getDataAsString());

    ASSERT_EQ(ResultOk, consumer.unsubscribe());
    ASSERT_EQ(ResultAlreadyClosed, consumer.close());
    ASSERT_EQ(ResultOk, producer.close());
    ASSERT_EQ(ResultOk, client.close());
}

TEST(BasicEndToEndTest, testCompressionZLib) {
    ClientConfiguration config;
    Client client(lookupUrl);
    std::string topicName = "persistent://prop/unit/ns1/testCompressionZLib";
    std::string subName = "my-sub-name";
    Producer producer;
    ProducerConfiguration conf;
    conf.setCompressionType(CompressionZLib);
    Result result = client.createProducer(topicName, conf, producer);
    ASSERT_EQ(ResultOk, result);

    Consumer consumer;
    client.subscribe(topicName, subName, consumer);

    // Send synchronously
    std::string content1 = "msg-1-content";
    Message msg = MessageBuilder().setContent(content1).build();
    result = producer.send(msg);
    ASSERT_EQ(ResultOk, result);

    std::string content2 = "msg-2-content";
    msg = MessageBuilder().setContent(content2).build();
    result = producer.send(msg);
    ASSERT_EQ(ResultOk, result);

    Message receivedMsg;
    consumer.receive(receivedMsg);
    ASSERT_EQ(content1, receivedMsg.getDataAsString());

    consumer.receive(receivedMsg);
    ASSERT_EQ(content2, receivedMsg.getDataAsString());

    ASSERT_EQ(ResultOk, consumer.unsubscribe());
    ASSERT_EQ(ResultAlreadyClosed, consumer.close());
    ASSERT_EQ(ResultOk, producer.close());
    ASSERT_EQ(ResultOk, client.close());
}

TEST(BasicEndToEndTest, testConfigurationFile) {
    ClientConfiguration config1;
    config1.setOperationTimeoutSeconds(100);
    config1.setIOThreads(10);
    config1.setMessageListenerThreads(1);
    config1.setLogConfFilePath("/tmp/");

    ClientConfiguration config2 = config1;
    AuthenticationDataPtr authData;
    ASSERT_EQ(ResultOk, config1.getAuth().getAuthData(authData));
    ASSERT_EQ(100, config2.getOperationTimeoutSeconds());
    ASSERT_EQ(10, config2.getIOThreads());
    ASSERT_EQ(1, config2.getMessageListenerThreads());
    ASSERT_EQ(config2.getLogConfFilePath().compare("/tmp/"), 0);
}

TEST(BasicEndToEndTest, testSinglePartitionRoutingPolicy) {
    Client client(lookupUrl);
    std::string topicName = "persistent://prop/unit/ns/partition-testSinglePartitionRoutingPolicy";

    // call admin api to make it partitioned
    std::string url =
        adminUrl + "admin/persistent/prop/unit/ns/partition-testSinglePartitionRoutingPolicy/partitions";
    int res = makePutRequest(url, "5");

    LOG_INFO("res = " << res);
    ASSERT_FALSE(res != 204 && res != 409);

    Producer producer;
    ProducerConfiguration producerConfiguration;
    producerConfiguration.setPartitionsRoutingMode(ProducerConfiguration::UseSinglePartition);
    Result result = client.createProducer(topicName, producerConfiguration, producer);

    Consumer consumer;
    result = client.subscribe(topicName, "subscription-A", consumer);
    ASSERT_EQ(ResultOk, result);

    ASSERT_EQ(ResultOk, result);
    for (int i = 0; i < 10; i++) {
        boost::posix_time::ptime t(boost::posix_time::microsec_clock::universal_time());
        long nanoSeconds = t.time_of_day().total_nanoseconds();
        std::stringstream ss;
        ss << nanoSeconds;
        Message msg = MessageBuilder().setContent(ss.str()).build();
        ASSERT_EQ(ResultOk, producer.send(msg));
    }

    for (int i = 0; i < 10; i++) {
        Message m;
        consumer.receive(m);
        consumer.acknowledgeCumulative(m);
    }

    consumer.close();
    producer.close();
    client.close();
}

TEST(BasicEndToEndTest, testNamespaceName) {
    boost::shared_ptr<NamespaceName> nameSpaceName = NamespaceName::get("property", "bf1", "nameSpace");
    ASSERT_STREQ(nameSpaceName->getCluster().c_str(), "bf1");
    ASSERT_STREQ(nameSpaceName->getLocalName().c_str(), "nameSpace");
    ASSERT_STREQ(nameSpaceName->getProperty().c_str(), "property");
}

TEST(BasicEndToEndTest, testConsumerClose) {
    ClientConfiguration config;
    Client client(lookupUrl);
    std::string topicName = "persistent://prop/unit/ns1/testConsumerClose";
    std::string subName = "my-sub-name";
    Consumer consumer;
    ASSERT_EQ(ResultOk, client.subscribe(topicName, subName, consumer));
    ASSERT_EQ(consumer.close(), ResultOk);
    ASSERT_EQ(consumer.close(), ResultAlreadyClosed);
}

TEST(BasicEndToEndTest, testDuplicateConsumerCreationOnPartitionedTopic) {
    Client client(lookupUrl);
    std::string topicName =
        "persistent://prop/unit/ns/partition-testDuplicateConsumerCreationOnPartitionedTopic";

    // call admin api to make it partitioned
    std::string url =
        adminUrl + "admin/persistent/prop/unit/ns/testDuplicateConsumerCreationOnPartitionedTopic/partitions";
    int res = makePutRequest(url, "5");

    LOG_INFO("res = " << res);
    ASSERT_FALSE(res != 204 && res != 409);

    usleep(2 * 1000 * 1000);

    Producer producer;
    ProducerConfiguration producerConfiguration;
    producerConfiguration.setPartitionsRoutingMode(ProducerConfiguration::CustomPartition);
    producerConfiguration.setMessageRouter(boost::make_shared<CustomRoutingPolicy>());

    Result result = client.createProducer(topicName, producer);
    ASSERT_EQ(ResultOk, result);
    for (int i = 0; i < 10; i++) {
        boost::posix_time::ptime t(boost::posix_time::microsec_clock::universal_time());
        long nanoSeconds = t.time_of_day().total_nanoseconds();
        std::stringstream ss;
        ss << nanoSeconds;
        Message msg = MessageBuilder().setContent(ss.str()).build();
        ASSERT_EQ(ResultOk, producer.send(msg));
    }

    LOG_INFO("Creating Subscriber");
    std::string consumerId = "CONSUMER";
    ConsumerConfiguration tempConsConfig;
    tempConsConfig.setConsumerType(ConsumerExclusive);
    ConsumerConfiguration consConfig = tempConsConfig;
    ASSERT_EQ(consConfig.getConsumerType(), ConsumerExclusive);
    Consumer consumer;
    Result subscribeResult = client.subscribe(topicName, consumerId, consConfig, consumer);
    ASSERT_EQ(ResultOk, subscribeResult);

    LOG_INFO("Creating Another Subscriber");
    Consumer consumer2;
    ASSERT_EQ(consumer2.getSubscriptionName(), "");
    subscribeResult = client.subscribe(topicName, consumerId, consConfig, consumer2);
    ASSERT_EQ(ResultConsumerBusy, subscribeResult);
    consumer.close();
    producer.close();
}

TEST(BasicEndToEndTest, testRoundRobinRoutingPolicy) {
    Client client(lookupUrl);
    std::string topicName = "persistent://prop/unit/ns/partition-testRoundRobinRoutingPolicy";
    // call admin api to make it partitioned
    std::string url =
        adminUrl + "admin/persistent/prop/unit/ns/partition-testRoundRobinRoutingPolicy/partitions";
    int res = makePutRequest(url, "5");

    LOG_INFO("res = " << res);
    ASSERT_FALSE(res != 204 && res != 409);

    Producer producer;
    ProducerConfiguration tempProducerConfiguration;
    tempProducerConfiguration.setPartitionsRoutingMode(ProducerConfiguration::RoundRobinDistribution);
    ProducerConfiguration producerConfiguration = tempProducerConfiguration;
    Result result = client.createProducer(topicName, producerConfiguration, producer);
    ASSERT_EQ(ResultOk, result);
    ASSERT_EQ(producer.getTopic(), topicName);

    // Topic is partitioned into 5 partitions so each partition will receive two messages
    LOG_INFO("Creating Subscriber");
    std::string consumerId = "CONSUMER";
    ConsumerConfiguration consConfig;
    consConfig.setConsumerType(ConsumerExclusive);
    consConfig.setReceiverQueueSize(2);
    ASSERT_FALSE(consConfig.hasMessageListener());
    Consumer consumer[5];
    Result subscribeResult;
    for (int i = 0; i < 5; i++) {
        std::stringstream partitionedTopicName;
        partitionedTopicName << topicName << "-partition-" << i;

        std::stringstream partitionedConsumerId;
        partitionedConsumerId << consumerId << i;
        subscribeResult = client.subscribe(partitionedTopicName.str(), partitionedConsumerId.str(),
                                           consConfig, consumer[i]);

        ASSERT_EQ(ResultOk, subscribeResult);
        ASSERT_EQ(consumer[i].getTopic(), partitionedTopicName.str());
    }

    for (int i = 0; i < 10; i++) {
        boost::posix_time::ptime t(boost::posix_time::microsec_clock::universal_time());
        long nanoSeconds = t.time_of_day().total_nanoseconds();
        std::stringstream ss;
        ss << nanoSeconds;
        Message msg = MessageBuilder().setContent(ss.str()).build();
        ASSERT_EQ(ResultOk, producer.send(msg));
    }

    Message m;
    for (int i = 0; i < 2; i++) {
        for (int partitionIndex = 0; partitionIndex < 5; partitionIndex++) {
            ASSERT_EQ(ResultOk, consumer[partitionIndex].receive(m));
            ASSERT_EQ(ResultOk, consumer[partitionIndex].acknowledge(m));
        }
    }

    for (int partitionIndex = 0; partitionIndex < 5; partitionIndex++) {
        consumer[partitionIndex].close();
    }
    producer.close();
    client.shutdown();
}

TEST(BasicEndToEndTest, testMessageListener) {
    Client client(lookupUrl);
    std::string topicName = "persistent://prop/unit/ns/partition-testMessageListener";
    // call admin api to make it partitioned
    std::string url = adminUrl + "admin/persistent/prop/unit/ns/partition-testMessageListener/partitions";
    int res = makePutRequest(url, "5");

    LOG_INFO("res = " << res);
    ASSERT_FALSE(res != 204 && res != 409);

    Producer producer;
    ProducerConfiguration producerConfiguration;
    producerConfiguration.setPartitionsRoutingMode(ProducerConfiguration::UseSinglePartition);
    Result result = client.createProducer(topicName, producerConfiguration, producer);

    // Initializing global Count
    globalCount = 0;

    ConsumerConfiguration consumerConfig;
    consumerConfig.setMessageListener(boost::bind(messageListenerFunction, _1, _2));
    Consumer consumer;
    result = client.subscribe(topicName, "subscription-A", consumerConfig, consumer);

    ASSERT_EQ(ResultOk, result);
    for (int i = 0; i < 10; i++) {
        boost::posix_time::ptime t(boost::posix_time::microsec_clock::universal_time());
        long nanoSeconds = t.time_of_day().total_nanoseconds();
        std::stringstream ss;
        ss << nanoSeconds;
        Message msg = MessageBuilder().setContent(ss.str()).setPartitionKey(ss.str()).build();
        ASSERT_EQ(ResultOk, producer.send(msg));
    }

    // Sleeping for 5 seconds
    usleep(5 * 1000 * 1000);
    ASSERT_EQ(globalCount, 10);
    consumer.close();
    producer.close();
    client.close();
}

TEST(BasicEndToEndTest, testMessageListenerPause) {
    Client client(lookupUrl);
    std::string topicName = "persistent://property/cluster/namespace/partition-testMessageListenerPause";

    // call admin api to make it partitioned
    std::string url =
        adminUrl +
        "admin/persistent/property/cluster/namespace/partition-testMessageListener-pauses/partitions";
    int res = makePutRequest(url, "5");

    LOG_INFO("res = " << res);
    ASSERT_FALSE(res != 204 && res != 409);

    Producer producer;
    ProducerConfiguration producerConfiguration;
    producerConfiguration.setPartitionsRoutingMode(ProducerConfiguration::UseSinglePartition);
    Result result = client.createProducer(topicName, producerConfiguration, producer);

    // Initializing global Count
    globalCount = 0;

    ConsumerConfiguration consumerConfig;
    consumerConfig.setMessageListener(boost::bind(messageListenerFunction, _1, _2));
    Consumer consumer;
    // Removing dangling subscription from previous test failures
    result = client.subscribe(topicName, "subscription-name", consumerConfig, consumer);
    consumer.unsubscribe();

    result = client.subscribe(topicName, "subscription-name", consumerConfig, consumer);
    ASSERT_EQ(ResultOk, result);
    int temp = 1000;
    for (int i = 0; i < 10000; i++) {
        if (i && i % 1000 == 0) {
            usleep(2 * 1000 * 1000);
            ASSERT_EQ(globalCount, temp);
            consumer.resumeMessageListener();
            usleep(2 * 1000 * 1000);
            ASSERT_EQ(globalCount, i);
            temp = globalCount;
            consumer.pauseMessageListener();
        }
        Message msg = MessageBuilder().build();
        ASSERT_EQ(ResultOk, producer.send(msg));
    }

    ASSERT_EQ(globalCount, temp);
    consumer.resumeMessageListener();
    // Sleeping for 2 seconds
    usleep(2 * 1000 * 1000);

    ASSERT_EQ(globalCount, 10000);
    consumer.close();
    producer.close();
    client.close();
}

TEST(BasicEndToEndTest, testResendViaSendCallback) {
    ClientConfiguration clientConfiguration;
    clientConfiguration.setIOThreads(1);
    Client client(lookupUrl, clientConfiguration);
    std::string topicName = "persistent://my-property/my-cluster/my-namespace/testResendViaListener";

    Producer producer;

    Promise<Result, Producer> producerPromise;
    ProducerConfiguration producerConfiguration;

    // Setting timeout of 1 ms
    producerConfiguration.setSendTimeout(1);
    client.createProducerAsync(topicName, producerConfiguration,
                               WaitForCallbackValue<Producer>(producerPromise));
    Future<Result, Producer> producerFuture = producerPromise.getFuture();
    Result result = producerFuture.get(producer);
    ASSERT_EQ(ResultOk, result);

    // Send asynchronously for 3 seconds
    // Expect timeouts since we have set timeout to 1 ms
    // On receiving timeout send the message using the Pulsar client IO thread via cb function.
    for (int i = 0; i < 10000; i++) {
        producer.sendAsync(MessageBuilder().build(), boost::bind(resendMessage, _1, _2, producer));
    }
    // 3 seconds
    usleep(3 * 1000 * 1000);
    producer.close();
    Lock lock(mutex_);
    ASSERT_GE(globalResendMessageCount, 3);
}

TEST(BasicEndToEndTest, testStatsLatencies) {
    ClientConfiguration config;
    config.setIOThreads(1);
    config.setMessageListenerThreads(1);
    config.setStatsIntervalInSeconds(5);
    Client client(lookupUrl, config);
    std::string topicName = "persistent://property/cluster/namespace/testStatsLatencies";
    std::string subName = "subscription-name";
    Producer producer;

    // Start Producer and Consumer
    int numOfMessages = 1000;

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

    ProducerStatsImplPtr producerStatsImplPtr = PulsarFriend::getProducerStatsPtr(producer);

    // Send Asynchronously
    std::string prefix = "msg-stats-";
    for (int i = 0; i < numOfMessages; i++) {
        std::string messageContent = prefix + boost::lexical_cast<std::string>(i);
        Message msg = MessageBuilder()
                          .setContent(messageContent)
                          .setProperty("msgIndex", boost::lexical_cast<std::string>(i))
                          .build();
        producer.sendAsync(msg, boost::bind(&sendCallBack, _1, _2, prefix, 15, 2 * 1e3));
        LOG_DEBUG("sending message " << messageContent);
    }

    // Wait for all messages to be acked by broker
    while (PulsarFriend::sum(producerStatsImplPtr->getTotalSendMap()) < numOfMessages) {
        usleep(1000);  // 1 ms
    }

    // Get latencies
    LatencyAccumulator totalLatencyAccumulator = producerStatsImplPtr->getTotalLatencyAccumulator();
    boost::accumulators::detail::extractor_result<
        LatencyAccumulator, boost::accumulators::tag::extended_p_square>::type totalLatencies =
        boost::accumulators::extended_p_square(totalLatencyAccumulator);

    LatencyAccumulator latencyAccumulator = producerStatsImplPtr->getLatencyAccumulator();
    boost::accumulators::detail::extractor_result<
        LatencyAccumulator, boost::accumulators::tag::extended_p_square>::type latencies =
        boost::accumulators::extended_p_square(latencyAccumulator);

    // Since 15% of the messages have a delay of
    ASSERT_EQ((uint64_t)latencies[1], (uint64_t)totalLatencies[1]);
    ASSERT_EQ((uint64_t)latencies[2], (uint64_t)totalLatencies[2]);
    ASSERT_EQ((uint64_t)latencies[3], (uint64_t)totalLatencies[3]);

    ASSERT_GE((uint64_t)latencies[1], 20 * 100);
    ASSERT_GE((uint64_t)latencies[2], 20 * 100);
    ASSERT_GE((uint64_t)latencies[3], 20 * 100);

    ASSERT_GE((uint64_t)totalLatencies[1], 20 * 100);
    ASSERT_GE((uint64_t)totalLatencies[2], 20 * 100);
    ASSERT_GE((uint64_t)totalLatencies[3], 20 * 100);

    while (producerStatsImplPtr->getNumMsgsSent() != 0) {
        usleep(1e6);  // wait till stats flush
    }

    usleep(1 * 1e6);  // 1 second

    latencyAccumulator = producerStatsImplPtr->getLatencyAccumulator();
    latencies = boost::accumulators::extended_p_square(latencyAccumulator);

    totalLatencyAccumulator = producerStatsImplPtr->getTotalLatencyAccumulator();
    totalLatencies = boost::accumulators::extended_p_square(totalLatencyAccumulator);

    ASSERT_NE((uint64_t)latencies[1], (uint64_t)totalLatencies[1]);
    ASSERT_NE((uint64_t)latencies[2], (uint64_t)totalLatencies[2]);
    ASSERT_NE((uint64_t)latencies[3], (uint64_t)totalLatencies[3]);

    ASSERT_EQ((uint64_t)latencies[1], 0);
    ASSERT_EQ((uint64_t)latencies[2], 0);
    ASSERT_EQ((uint64_t)latencies[3], 0);

    ASSERT_GE((uint64_t)totalLatencies[1], 20 * 1000);
    ASSERT_GE((uint64_t)totalLatencies[2], 20 * 1000);
    ASSERT_GE((uint64_t)totalLatencies[3], 20 * 1000);

    Message receivedMsg;
    int i = 0;
    ConsumerStatsImplPtr consumerStatsImplPtr = PulsarFriend::getConsumerStatsPtr(consumer);

    while (consumer.receive(receivedMsg, 5000) == ResultOk) {
        std::string expectedMessageContent = prefix + boost::lexical_cast<std::string>(i);
        LOG_DEBUG("Received Message with [ content - " << receivedMsg.getDataAsString() << "] [ messageID = "
                                                       << receivedMsg.getMessageId() << "]");
        ASSERT_EQ(receivedMsg.getProperty("msgIndex"), boost::lexical_cast<std::string>(i++));
        ASSERT_EQ(PulsarFriend::sum(consumerStatsImplPtr->getTotalReceivedMsgMap()), i);
        ASSERT_EQ(expectedMessageContent, receivedMsg.getDataAsString());
        ASSERT_EQ(PulsarFriend::sum(consumerStatsImplPtr->getTotalAckedMsgMap()), i - 1);
        ASSERT_EQ(ResultOk, consumer.acknowledge(receivedMsg));
        ASSERT_EQ(PulsarFriend::sum(consumerStatsImplPtr->getTotalAckedMsgMap()), i);
    }
    // Number of messages consumed
    ASSERT_EQ(i, numOfMessages);
}

TEST(BasicEndToEndTest, testProduceMessageSize) {
    ClientConfiguration config;
    Client client(lookupUrl);
    std::string topicName = "persistent://prop/unit/ns1/testProduceMessageSize";
    std::string subName = "my-sub-name";
    Producer producer1;
    Producer producer2;

    Promise<Result, Producer> producerPromise;
    client.createProducerAsync(topicName, WaitForCallbackValue<Producer>(producerPromise));
    Future<Result, Producer> producerFuture = producerPromise.getFuture();
    Result result = producerFuture.get(producer1);
    ASSERT_EQ(ResultOk, result);

    Promise<Result, Producer> producerPromise2;
    ProducerConfiguration conf;
    conf.setCompressionType(CompressionLZ4);
    client.createProducerAsync(topicName, conf, WaitForCallbackValue<Producer>(producerPromise2));
    producerFuture = producerPromise2.getFuture();
    result = producerFuture.get(producer2);
    ASSERT_EQ(ResultOk, result);

    int size = Commands::MaxMessageSize + 1;
    char* content = new char[size];
    Message msg = MessageBuilder().setAllocatedContent(content, size).build();
    result = producer1.send(msg);
    ASSERT_EQ(ResultMessageTooBig, result);

    Consumer consumer;
    Promise<Result, Consumer> consumerPromise;
    client.subscribeAsync(topicName, subName, WaitForCallbackValue<Consumer>(consumerPromise));
    Future<Result, Consumer> consumerFuture = consumerPromise.getFuture();
    result = consumerFuture.get(consumer);
    ASSERT_EQ(ResultOk, result);

    msg = MessageBuilder().setAllocatedContent(content, size).build();
    result = producer2.send(msg);
    ASSERT_EQ(ResultOk, result);

    Message receivedMsg;
    consumer.receive(receivedMsg);
    ASSERT_EQ(size, receivedMsg.getDataAsString().length());

    producer1.closeAsync(0);
    producer2.closeAsync(0);
    consumer.close();
    client.close();

    delete[] content;
}

TEST(BasicEndToEndTest, testHandlerReconnectionLogic) {
    Client client(adminUrl);
    std::string topicName = "persistent://prop/unit/ns1/testHandlerReconnectionLogic";

    Producer producer;
    Consumer consumer;

    ASSERT_EQ(client.subscribe(topicName, "my-sub", consumer), ResultOk);
    ASSERT_EQ(client.createProducer(topicName, producer), ResultOk);

    std::vector<ClientConnectionPtr> oldConnections;

    int numOfMessages = 10;
    std::string propertyName = "msgIndex";
    for (int i = 0; i < numOfMessages; i++) {
        std::string messageContent = "msg-" + boost::lexical_cast<std::string>(i);
        Message msg = MessageBuilder()
                          .setContent(messageContent)
                          .setProperty(propertyName, boost::lexical_cast<std::string>(i))
                          .build();
        if (i % 3 == 1) {
            ProducerImpl& pImpl = PulsarFriend::getProducerImpl(producer);
            ClientConnectionPtr clientConnectionPtr;
            do {
                ClientConnectionWeakPtr clientConnectionWeakPtr = PulsarFriend::getClientConnection(pImpl);
                clientConnectionPtr = clientConnectionWeakPtr.lock();
                usleep(1 * 1e6);
            } while (!clientConnectionPtr);
            oldConnections.push_back(clientConnectionPtr);
            clientConnectionPtr->close();
        }
        ASSERT_EQ(producer.send(msg), ResultOk);
    }

    std::set<std::string> receivedMsgContent;
    std::set<std::string> receivedMsgIndex;

    Message msg;
    while (consumer.receive(msg, 30000) == ResultOk) {
        receivedMsgContent.insert(msg.getDataAsString());
        receivedMsgIndex.insert(msg.getProperty(propertyName));
    }

    ConsumerImpl& cImpl = PulsarFriend::getConsumerImpl(consumer);
    ClientConnectionWeakPtr clientConnectionWeakPtr = PulsarFriend::getClientConnection(cImpl);
    ClientConnectionPtr clientConnectionPtr = clientConnectionWeakPtr.lock();
    oldConnections.push_back(clientConnectionPtr);
    clientConnectionPtr->close();

    while (consumer.receive(msg, 30000) == ResultOk) {
        consumer.acknowledge(msg);
        receivedMsgContent.insert(msg.getDataAsString());
        receivedMsgIndex.insert(msg.getProperty(propertyName));
    }

    ASSERT_EQ(receivedMsgContent.size(), 10);
    ASSERT_EQ(receivedMsgIndex.size(), 10);

    for (int i = 0; i < numOfMessages; i++) {
        ASSERT_TRUE(receivedMsgContent.find("msg-" + boost::lexical_cast<std::string>(i)) !=
                    receivedMsgContent.end());
        ASSERT_TRUE(receivedMsgIndex.find(boost::lexical_cast<std::string>(i)) != receivedMsgIndex.end());
    }
}

TEST(BasicEndToEndTest, testRSAEncryption) {
    ClientConfiguration config;
    Client client(lookupUrl);
    std::string topicName = "persistent://prop/unit/ns1/my-rsaenctopic";
    std::string subName = "my-sub-name";
    Producer producer;

    boost::shared_ptr<EncKeyReader> keyReader = boost::make_shared<EncKeyReader>();
    ProducerConfiguration conf;
    conf.setCompressionType(CompressionLZ4);
    conf.addEncryptionKey("client-rsa.pem");
    conf.setCryptoKeyReader(keyReader);

    Promise<Result, Producer> producerPromise;
    client.createProducerAsync(topicName, conf, WaitForCallbackValue<Producer>(producerPromise));
    Future<Result, Producer> producerFuture = producerPromise.getFuture();
    Result result = producerFuture.get(producer);
    ASSERT_EQ(ResultOk, result);

    ConsumerConfiguration consConfig;
    consConfig.setCryptoKeyReader(keyReader);
    // consConfig.setCryptoFailureAction(ConsumerCryptoFailureAction::CONSUME);

    Consumer consumer;
    Promise<Result, Consumer> consumerPromise;
    client.subscribeAsync(topicName, subName, consConfig, WaitForCallbackValue<Consumer>(consumerPromise));
    Future<Result, Consumer> consumerFuture = consumerPromise.getFuture();
    result = consumerFuture.get(consumer);
    ASSERT_EQ(ResultOk, result);

    // Send 1000 messages synchronously
    std::string msgContent = "msg-content";
    LOG_INFO("Publishing 1000 messages synchronously");
    int msgNum = 0;
    for (; msgNum < 1000; msgNum++) {
        std::stringstream stream;
        stream << msgContent << msgNum;
        Message msg = MessageBuilder().setContent(stream.str()).build();
        ASSERT_EQ(ResultOk, producer.send(msg));
    }

    LOG_INFO("Trying to receive 1000 messages");
    Message msgReceived;
    for (msgNum = 0; msgNum < 1000; msgNum++) {
        consumer.receive(msgReceived, 1000);
        LOG_DEBUG("Received message :" << msgReceived.getMessageId());
        std::stringstream expected;
        expected << msgContent << msgNum;
        ASSERT_EQ(expected.str(), msgReceived.getDataAsString());
        ASSERT_EQ(ResultOk, consumer.acknowledge(msgReceived));
    }

    ASSERT_EQ(ResultOk, consumer.unsubscribe());
    ASSERT_EQ(ResultAlreadyClosed, consumer.close());
    ASSERT_EQ(ResultOk, producer.close());
    ASSERT_EQ(ResultOk, client.close());
}

TEST(BasicEndToEndTest, testEncryptionFailure) {
    ClientConfiguration config;
    Client client(lookupUrl);
    std::string topicName = "persistent://prop/unit/ns1/my-rsaencfailtopic";
    std::string subName = "my-sub-name";
    Producer producer;

    boost::shared_ptr<EncKeyReader> keyReader = boost::make_shared<EncKeyReader>();

    ConsumerConfiguration consConfig;

    Consumer consumer;
    Promise<Result, Consumer> consumerPromise;
    client.subscribeAsync(topicName, subName, consConfig, WaitForCallbackValue<Consumer>(consumerPromise));
    Future<Result, Consumer> consumerFuture = consumerPromise.getFuture();
    Result result = consumerFuture.get(consumer);
    ASSERT_EQ(ResultOk, result);

    std::string msgContent = "msg-content";
    int msgNum = 0;
    int totalMsgs = 10;
    std::stringstream stream;
    stream << msgContent << msgNum;
    Message msg = MessageBuilder().setContent(msgContent).build();

    // 1. Non existing key

    {
        ProducerConfiguration prodConf;
        prodConf.setCryptoKeyReader(keyReader);
        prodConf.addEncryptionKey("client-non-existing-rsa.pem");

        Promise<Result, Producer> producerPromise;
        client.createProducerAsync(topicName, prodConf, WaitForCallbackValue<Producer>(producerPromise));
        Future<Result, Producer> producerFuture = producerPromise.getFuture();
        result = producerFuture.get(producer);
        ASSERT_EQ(ResultOk, result);

        ASSERT_EQ(ResultCryptoError, producer.send(msg));
    }

    // 2. Add valid key
    {
        ProducerConfiguration prodConf;
        prodConf.setCryptoKeyReader(keyReader);
        prodConf.addEncryptionKey("client-rsa.pem");

        Promise<Result, Producer> producerPromise;
        client.createProducerAsync(topicName, prodConf, WaitForCallbackValue<Producer>(producerPromise));
        Future<Result, Producer> producerFuture = producerPromise.getFuture();
        result = producerFuture.get(producer);
        ASSERT_EQ(ResultOk, result);

        msgNum++;
        for (; msgNum < totalMsgs; msgNum++) {
            std::stringstream stream;
            stream << msgContent << msgNum;
            Message msg = MessageBuilder().setContent(stream.str()).build();
            ASSERT_EQ(ResultOk, producer.send(msg));
        }
    }

    // 3. Key reader is not set by consumer
    Message msgReceived;
    ASSERT_EQ(ResultTimeout, consumer.receive(msgReceived, 5000));
    ASSERT_EQ(ResultOk, consumer.close());

    // 4. Set consumer config to consume even if decryption fails
    consConfig.setCryptoFailureAction(ConsumerCryptoFailureAction::CONSUME);

    Promise<Result, Consumer> consumerPromise2;
    client.subscribeAsync(topicName, subName, consConfig, WaitForCallbackValue<Consumer>(consumerPromise2));
    consumerFuture = consumerPromise2.getFuture();
    result = consumerFuture.get(consumer);
    ASSERT_EQ(ResultOk, consumer.receive(msgReceived, 1000));

    // Received message 0. Skip message comparision since its encrypted
    ASSERT_EQ(ResultOk, result);
    ASSERT_EQ(ResultOk, consumer.close());

    // 5. Set valid keyreader and consume messages
    msgNum = 1;
    consConfig.setCryptoKeyReader(keyReader);
    consConfig.setCryptoFailureAction(ConsumerCryptoFailureAction::FAIL);
    Promise<Result, Consumer> consumerPromise3;
    client.subscribeAsync(topicName, subName, consConfig, WaitForCallbackValue<Consumer>(consumerPromise3));
    consumerFuture = consumerPromise3.getFuture();
    result = consumerFuture.get(consumer);

    for (; msgNum < totalMsgs - 1; msgNum++) {
        ASSERT_EQ(ResultOk, consumer.receive(msgReceived, 1000));
        LOG_DEBUG("Received message :" << msgReceived.getMessageId());
        std::stringstream expected;
        expected << msgContent << msgNum;
        ASSERT_EQ(expected.str(), msgReceived.getDataAsString());
        ASSERT_EQ(ResultOk, consumer.acknowledge(msgReceived));
    }
    ASSERT_EQ(ResultOk, consumer.close());

    // 6. Discard message if decryption fails
    ConsumerConfiguration consConfig2;
    consConfig2.setCryptoFailureAction(ConsumerCryptoFailureAction::DISCARD);

    Promise<Result, Consumer> consumerPromise4;
    client.subscribeAsync(topicName, subName, consConfig2, WaitForCallbackValue<Consumer>(consumerPromise4));
    consumerFuture = consumerPromise4.getFuture();
    result = consumerFuture.get(consumer);

    // Since messag is discarded, no message will be received.
    ASSERT_EQ(ResultTimeout, consumer.receive(msgReceived, 5000));
}

TEST(BasicEndToEndTest, testEventTime) {
    ClientConfiguration config;
    Client client(lookupUrl, config);
    std::string topicName = "persistent://prop/unit/ns1/topic";
    Producer producer;
    ProducerConfiguration producerConf;
    producerConf.setBatchingEnabled(true);
    Result result = client.createProducer(topicName, producerConf, producer);
    ASSERT_EQ(ResultOk, result);

    Consumer consumer;
    result = client.subscribe(topicName, "sub", consumer);
    ASSERT_EQ(ResultOk, result);

    producer.send(MessageBuilder().setContent("test").setEventTimestamp(5).build());

    Message msg;
    result = consumer.receive(msg);
    ASSERT_EQ(ResultOk, result);

    ASSERT_EQ(msg.getEventTimestamp(), 5);

    consumer.close();
    producer.close();
}

TEST(BasicEndToEndTest, testSeek) {
    ClientConfiguration config;
    Client client(lookupUrl);
    std::string topicName = "persistent://prop/unit/ns1/testSeek";
    std::string subName = "sub-testSeek";
    Producer producer;

    Promise<Result, Producer> producerPromise;
    client.createProducerAsync(topicName, WaitForCallbackValue<Producer>(producerPromise));
    Future<Result, Producer> producerFuture = producerPromise.getFuture();
    Result result = producerFuture.get(producer);
    ASSERT_EQ(ResultOk, result);

    Consumer consumer;
    ConsumerConfiguration consConfig;
    consConfig.setReceiverQueueSize(1);
    Promise<Result, Consumer> consumerPromise;
    client.subscribeAsync(topicName, subName, consConfig, WaitForCallbackValue<Consumer>(consumerPromise));
    Future<Result, Consumer> consumerFuture = consumerPromise.getFuture();
    result = consumerFuture.get(consumer);
    ASSERT_EQ(ResultOk, result);
    std::string temp = producer.getTopic();
    ASSERT_EQ(temp, topicName);
    temp = consumer.getTopic();
    ASSERT_EQ(temp, topicName);
    ASSERT_EQ(consumer.getSubscriptionName(), subName);

    // Send 1000 messages synchronously
    std::string msgContent = "msg-content";
    LOG_INFO("Publishing 100 messages synchronously");
    int msgNum = 0;
    for (; msgNum < 100; msgNum++) {
        std::stringstream stream;
        stream << msgContent << msgNum;
        Message msg = MessageBuilder().setContent(stream.str()).build();
        ASSERT_EQ(ResultOk, producer.send(msg));
    }

    LOG_INFO("Trying to receive 100 messages");
    Message msgReceived;
    for (msgNum = 0; msgNum < 100; msgNum++) {
        consumer.receive(msgReceived, 100);
        LOG_DEBUG("Received message :" << msgReceived.getMessageId());
        std::stringstream expected;
        expected << msgContent << msgNum;
        ASSERT_EQ(expected.str(), msgReceived.getDataAsString());
        ASSERT_EQ(ResultOk, consumer.acknowledge(msgReceived));
    }

    // seek to earliest, expected receive first message.
    result = consumer.seek(MessageId::earliest());
    // Sleeping for 500ms to wait for consumer re-connect
    usleep(500 * 1000);

    ASSERT_EQ(ResultOk, result);
    consumer.receive(msgReceived, 100);
    LOG_ERROR("Received message :" << msgReceived.getMessageId());
    std::stringstream expected;
    msgNum = 0;
    expected << msgContent << msgNum;
    ASSERT_EQ(expected.str(), msgReceived.getDataAsString());
    ASSERT_EQ(ResultOk, consumer.acknowledge(msgReceived));
    ASSERT_EQ(ResultOk, consumer.unsubscribe());
    ASSERT_EQ(ResultAlreadyClosed, consumer.close());
    ASSERT_EQ(ResultOk, producer.close());
    ASSERT_EQ(ResultOk, client.close());
}

TEST(BasicEndToEndTest, testUnAckedMessageTimeout) {
    Client client(lookupUrl);
    std::string topicName = "persistent://prop/unit/ns1/testUnAckedMessageTimeout";
    std::string subName = "my-sub-name";
    std::string content = "msg-content";

    Producer producer;
    Result result = client.createProducer(topicName, producer);
    ASSERT_EQ(ResultOk, result);

    Consumer consumer;
    ConsumerConfiguration consConfig;
    consConfig.setUnAckedMessagesTimeoutMs(10 * 1000);
    result = client.subscribe(topicName, subName, consConfig, consumer);
    ASSERT_EQ(ResultOk, result);

    Message msg = MessageBuilder().setContent(content).build();
    result = producer.send(msg);
    ASSERT_EQ(ResultOk, result);

    Message receivedMsg1;
    MessageId msgId1;
    consumer.receive(receivedMsg1);
    msgId1 = receivedMsg1.getMessageId();
    ASSERT_EQ(content, receivedMsg1.getDataAsString());

    Message receivedMsg2;
    MessageId msgId2;
    consumer.receive(receivedMsg2, 30 * 1000);
    msgId2 = receivedMsg2.getMessageId();
    ASSERT_EQ(content, receivedMsg2.getDataAsString());

    ASSERT_EQ(msgId1, msgId2);

    consumer.unsubscribe();
    consumer.close();
    producer.close();
    client.close();
}

TEST(BasicEndToEndTest, testUnAckedMessageTimeoutListener) {
    Client client(lookupUrl);
    std::string topicName = "persistent://prop/unit/ns1/testUnAckedMessageTimeoutListener";
    std::string subName = "my-sub-name";
    std::string content = "msg-content";

    Producer producer;
    Result result = client.createProducer(topicName, producer);
    ASSERT_EQ(ResultOk, result);

    Consumer consumer;
    ConsumerConfiguration consConfig;
    consConfig.setUnAckedMessagesTimeoutMs(10 * 1000);
    Latch latch(2);
    consConfig.setMessageListener(boost::bind(messageListenerFunctionWithoutAck, _1, _2, latch, content));
    result = client.subscribe(topicName, subName, consConfig, consumer);
    ASSERT_EQ(ResultOk, result);

    globalCount = 0;

    Message msg = MessageBuilder().setContent(content).build();
    result = producer.send(msg);
    ASSERT_EQ(ResultOk, result);

    ASSERT_TRUE(latch.wait(milliseconds(30 * 1000)));
    ASSERT_GE(globalCount, 2);

    consumer.unsubscribe();
    consumer.close();
    producer.close();
    client.close();
}
