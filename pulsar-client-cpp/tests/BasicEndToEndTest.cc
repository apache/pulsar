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
#include <set>
#include <mutex>
#include <chrono>
#include <thread>
#include <vector>
#include <cstring>
#include <sstream>
#include <algorithm>
#include <functional>

#include <gtest/gtest.h>
#include <pulsar/Client.h>
#include <pulsar/Consumer.h>
#include <pulsar/MessageBuilder.h>
#include <pulsar/CryptoKeyReader.h>

#include <lib/Latch.h>
#include <lib/Utils.h>
#include <lib/Future.h>
#include <lib/Commands.h>
#include <lib/LogUtils.h>
#include <lib/TimeUtils.h>
#include <lib/TopicName.h>
#include <lib/ClientImpl.h>
#include <lib/ConsumerImpl.h>
#include <lib/PulsarApi.pb.h>
#include <lib/MultiTopicsConsumerImpl.h>
#include <lib/AckGroupingTrackerEnabled.h>
#include <lib/AckGroupingTrackerDisabled.h>
#include <lib/PatternMultiTopicsConsumerImpl.h>

#include "HttpHelper.h"
#include "PulsarFriend.h"
#include "CustomRoutingPolicy.h"

DECLARE_LOG_OBJECT()

using namespace pulsar;

std::mutex mutex_;
static int globalCount = 0;
static long globalResendMessageCount = 0;
std::string lookupUrl = "pulsar://localhost:6650";
static std::string adminUrl = "http://localhost:8080/";
static int uniqueCounter = 0;

std::string unique_str() {
    long nanos = std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::steady_clock::now().time_since_epoch())
                     .count();

    return std::to_string(uniqueCounter++) + "_" + std::to_string(nanos);
}

static void messageListenerFunction(Consumer consumer, const Message &msg) {
    globalCount++;
    consumer.acknowledge(msg);
}

static void messageListenerFunctionWithoutAck(Consumer consumer, const Message &msg, Latch &latch,
                                              const std::string &content) {
    globalCount++;
    ASSERT_EQ(content, msg.getDataAsString());
    latch.countdown();
}

static void sendCallBack(Result r, const MessageId &msgId, std::string prefix, int *count) {
    static std::mutex sendMutex_;
    sendMutex_.lock();
    ASSERT_EQ(r, ResultOk);
    *count += 1;
    sendMutex_.unlock();
}

static void receiveCallBack(Result r, const Message &msg, std::string &messageContent, bool checkContent,
                            bool *isFailed, int *count) {
    static std::mutex receiveMutex_;
    receiveMutex_.lock();

    if (r == ResultOk) {
        LOG_DEBUG("received msg " << msg.getDataAsString() << " expected: " << messageContent
                                  << " count =" << *count);
        if (checkContent) {
            ASSERT_EQ(messageContent, msg.getDataAsString());
        }
        *count += 1;
    } else {
        *isFailed = true;
    }
    receiveMutex_.unlock();
}

static void sendCallBackWithDelay(Result r, const MessageId &msgId, std::string prefix, double percentage,
                                  uint64_t delayInMicros, int *count) {
    if ((rand() % 100) <= percentage) {
        std::this_thread::sleep_for(std::chrono::microseconds(delayInMicros));
    }
    sendCallBack(r, msgId, prefix, count);
}

TEST(BasicEndToEndTest, testBatchMessages) {
    ClientConfiguration config;
    Client client(lookupUrl);
    std::string topicName = "persistent://public/default/test-batch-messages";
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
    conf.setProperty("producer-name", "test-producer-name");
    conf.setProperty("producer-id", "test-producer-id");

    Promise<Result, Producer> producerPromise;
    client.createProducerAsync(topicName, conf, WaitForCallbackValue<Producer>(producerPromise));
    Future<Result, Producer> producerFuture = producerPromise.getFuture();
    Result result = producerFuture.get(producer);
    ASSERT_EQ(ResultOk, result);

    Consumer consumer;
    ConsumerConfiguration consumerConfig;
    consumerConfig.setProperty("consumer-name", "test-consumer-name");
    consumerConfig.setProperty("consumer-id", "test-consumer-id");
    Promise<Result, Consumer> consumerPromise;
    client.subscribeAsync(topicName, subName, consumerConfig,
                          WaitForCallbackValue<Consumer>(consumerPromise));
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
    int msgCount = 0;
    for (int i = 0; i < numOfMessages; i++) {
        std::string messageContent = prefix + std::to_string(i);
        Message msg =
            MessageBuilder().setContent(messageContent).setProperty("msgIndex", std::to_string(i)).build();
        producer.sendAsync(
            msg, std::bind(&sendCallBack, std::placeholders::_1, std::placeholders::_2, prefix, &msgCount));
        LOG_DEBUG("sending message " << messageContent);
    }

    Message receivedMsg;
    int i = 0;
    while (consumer.receive(receivedMsg, 5000) == ResultOk) {
        std::string expectedMessageContent = prefix + std::to_string(i);
        LOG_INFO("Received Message with [ content - "
                 << receivedMsg.getDataAsString() << "] [ messageID = " << receivedMsg.getMessageId() << "]");
        LOG_INFO("msg-index " << receivedMsg.getProperty("msgIndex") << ", expected " << std::to_string(i));
        ASSERT_EQ(receivedMsg.getProperty("msgIndex"), std::to_string(i++));
        ASSERT_EQ(expectedMessageContent, receivedMsg.getDataAsString());
        ASSERT_EQ(ResultOk, consumer.acknowledge(receivedMsg));
    }
    // Number of messages produced
    ASSERT_EQ(msgCount, numOfMessages);
    // Number of messages consumed
    ASSERT_EQ(i, numOfMessages);
}

void resendMessage(Result r, const MessageId msgId, Producer producer) {
    Lock lock(mutex_);
    if (r != ResultOk) {
        LOG_DEBUG("globalResendMessageCount" << globalResendMessageCount);
        if (++globalResendMessageCount >= 3) {
            return;
        }
    }
    lock.unlock();
    producer.sendAsync(MessageBuilder().build(),
                       std::bind(resendMessage, std::placeholders::_1, std::placeholders::_2, producer));
}

TEST(BasicEndToEndTest, testProduceConsume) {
    ClientConfiguration config;
    Client client(lookupUrl);
    std::string topicName = "persistent://public/default/test-produce-consume";
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
    ASSERT_EQ(MessageId(-1, -1, -1, -1), msg.getMessageId());
    result = producer.send(msg);
    ASSERT_EQ(ResultOk, result);
    ASSERT_NE(MessageId(-1, -1, -1, -1), msg.getMessageId());

    Message receivedMsg;
    consumer.receive(receivedMsg);
    ASSERT_EQ(content, receivedMsg.getDataAsString());
    ASSERT_EQ(ResultOk, consumer.unsubscribe());
    ASSERT_EQ(ResultAlreadyClosed, consumer.close());
    ASSERT_EQ(ResultOk, producer.close());
    ASSERT_EQ(ResultOk, client.close());
}

TEST(BasicEndToEndTest, testRedeliveryCount) {
    ClientConfiguration config;
    Client client(lookupUrl, config);
    std::string topicName = "persistent://public/default/test-redelivery-count";
    std::string subName = "my-sub-name";

    Producer producer;
    Promise<Result, Producer> producerPromise;
    client.createProducerAsync(topicName, WaitForCallbackValue<Producer>(producerPromise));
    Future<Result, Producer> producerFuture = producerPromise.getFuture();
    Result result = producerFuture.get(producer);
    ASSERT_EQ(ResultOk, result);

    Consumer consumer;
    Promise<Result, Consumer> consumerPromise;
    ConsumerConfiguration consumerConf;
    consumerConf.setNegativeAckRedeliveryDelayMs(500);
    consumerConf.setConsumerType(ConsumerShared);
    client.subscribeAsync(topicName, subName, consumerConf, WaitForCallbackValue<Consumer>(consumerPromise));
    Future<Result, Consumer> consumerFuture = consumerPromise.getFuture();
    result = consumerFuture.get(consumer);
    ASSERT_EQ(ResultOk, result);
    std::string temp = producer.getTopic();
    ASSERT_EQ(temp, topicName);
    temp = consumer.getTopic();
    ASSERT_EQ(temp, topicName);
    ASSERT_EQ(consumer.getSubscriptionName(), subName);

    std::string content = "msg-content";
    Message msg = MessageBuilder().setContent(content).build();
    producer.send(msg);

    int redeliveryCount = 0;
    Message msgReceived;
    for (int i = 0; i < 4; i++) {
        consumer.receive(msgReceived);
        LOG_INFO("Received message " << msgReceived.getDataAsString());
        consumer.negativeAcknowledge(msgReceived);
        redeliveryCount = msgReceived.getRedeliveryCount();
    }

    ASSERT_EQ(3, redeliveryCount);
    consumer.acknowledge(msgReceived);
    consumer.close();
    producer.close();
}

TEST(BasicEndToEndTest, testLookupThrottling) {
    std::string topicName = "testLookupThrottling";
    ClientConfiguration config;
    config.setConcurrentLookupRequest(0);
    config.setLogger(new ConsoleLoggerFactory(Logger::LEVEL_DEBUG));
    Client client(lookupUrl, config);

    Producer producer;
    Result result = client.createProducer(topicName, producer);
    ASSERT_EQ(ResultTooManyLookupRequestException, result);

    Consumer consumer1;
    result = client.subscribe(topicName, "my-sub-name", consumer1);
    ASSERT_EQ(ResultTooManyLookupRequestException, result);

    client.close();
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
    std::string topicName = "non-persistent://public/default/testNonPersistentTopic";
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
    std::string topicName = "testSingleClientMultipleSubscriptions";

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
    std::string topicName = "testMultipleClientsMultipleSubscriptions";
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
    std::this_thread::sleep_for(std::chrono::microseconds(2 * 1000 * 1000));

    ASSERT_EQ(ResultOk, client2.close());
}

TEST(BasicEndToEndTest, testProduceAndConsumeAfterClientClose) {
    std::string topicName = "testProduceAndConsumeAfterClientClose";
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
        Message msg =
            MessageBuilder().setContent(msgContent).setProperty("msgIndex", std::to_string(numMsg)).build();
        ASSERT_EQ(ResultOk, producer.send(msg));
    }

    LOG_INFO("Trying to receive 10 messages");
    Message msgReceived;
    for (int i = 0; i < 10; i++) {
        consumer.receive(msgReceived, 1000);
        LOG_DEBUG("Received message :" << msgReceived.getMessageId());
        ASSERT_EQ(msgContent, msgReceived.getDataAsString());
        ASSERT_EQ(std::to_string(i), msgReceived.getProperty("msgIndex"));
        ASSERT_EQ(ResultOk, consumer.acknowledgeCumulative(msgReceived));
    }

    LOG_INFO("Closing client");
    ASSERT_EQ(ResultOk, client.close());

    LOG_INFO("Trying to publish a message after closing the client");
    Message msg =
        MessageBuilder().setContent(msgContent).setProperty("msgIndex", std::to_string(numMsg)).build();

    ASSERT_EQ(ResultAlreadyClosed, producer.send(msg));

    LOG_INFO("Trying to consume a message after closing the client");
    ASSERT_EQ(ResultAlreadyClosed, consumer.receive(msgReceived));
}

TEST(BasicEndToEndTest, testIamSoFancyCharactersInTopicName) {
    Client client(lookupUrl);
    Producer producer;
    Result result = client.createProducer("persistent://public/default/topic@%*)(&!%$#@#$><?", producer);
    ASSERT_EQ(ResultOk, result);

    Consumer consumer;
    result = client.subscribe("persistent://public/default/topic@%*)(&!%$#@#$><?", "my-sub-name", consumer);
    ASSERT_EQ(ResultOk, result);
}

TEST(BasicEndToEndTest, testSubscribeCloseUnsubscribeSherpaScenario) {
    ClientConfiguration config;
    Client client(lookupUrl, config);
    std::string topicName = "persistent://public/default/::,::bf11";
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
    std::string topicName = "testInvalidUrlPassed";
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

void testPartitionedProducerConsumer(bool lazyStartPartitionedProducers, std::string topicName) {
    Client client(lookupUrl);

    // call admin api to make it partitioned
    std::string url = adminUrl + "admin/v2/persistent/public/default/" + topicName + "/partitions";
    makeDeleteRequest(url);
    int res = makePutRequest(url, "3");

    LOG_INFO("res = " << res);
    ASSERT_FALSE(res != 204 && res != 409);

    ProducerConfiguration conf;
    conf.setLazyStartPartitionedProducers(lazyStartPartitionedProducers);
    Producer producer;
    Result result = client.createProducer(topicName, conf, producer);
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

TEST(BasicEndToEndTest, testPartitionedProducerConsumer) {
    testPartitionedProducerConsumer(false, "testPartitionedProducerConsumer");
}

TEST(BasicEndToEndTest, testPartitionedLazyProducerConsumer) {
    testPartitionedProducerConsumer(true, "testPartitionedProducerConsumerLazy");
}

TEST(BasicEndToEndTest, testPartitionedProducerConsumerSubscriptionName) {
    Client client(lookupUrl);
    std::string topicName = "testPartitionedProducerConsumerSubscriptionName" + unique_str();

    // call admin api to make it partitioned
    std::string url = adminUrl + "admin/v2/persistent/public/default/" + topicName + "/partitions";
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
    std::string topicName = "testMessageTooBig";
    Producer producer;
    ProducerConfiguration conf;
    conf.setBatchingEnabled(false);
    Result result = client.createProducer(topicName, conf, producer);
    ASSERT_EQ(ResultOk, result);

    int size = ClientConnection::getMaxMessageSize() + 1000 * 100;
    char *content = new char[size];
    memset(content, 0, size);
    Message msg = MessageBuilder().setAllocatedContent(content, size).build();
    result = producer.send(msg);
    ASSERT_EQ(ResultMessageTooBig, result);

    // Anything up to MaxMessageSize should be allowed
    size = ClientConnection::getMaxMessageSize();
    msg = MessageBuilder().setAllocatedContent(content, size).build();
    result = producer.send(msg);
    ASSERT_EQ(ResultOk, result);

    delete[] content;
}

TEST(BasicEndToEndTest, testCompressionLZ4) {
    ClientConfiguration config;
    Client client(lookupUrl);
    std::string topicName = "testCompressionLZ4";
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
    std::string topicName = "testCompressionZLib";
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
    std::string topicName = "partition-testSinglePartitionRoutingPolicy" + unique_str();

    // call admin api to make it partitioned
    std::string url = adminUrl + "admin/v2/persistent/public/default/" + topicName + "/partitions";
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
    std::shared_ptr<NamespaceName> nameSpaceName = NamespaceName::get("property", "bf1", "nameSpace");
    ASSERT_STREQ(nameSpaceName->getCluster().c_str(), "bf1");
    ASSERT_STREQ(nameSpaceName->getLocalName().c_str(), "nameSpace");
    ASSERT_STREQ(nameSpaceName->getProperty().c_str(), "property");
}

TEST(BasicEndToEndTest, testConsumerClose) {
    ClientConfiguration config;
    Client client(lookupUrl);
    std::string topicName = "testConsumerClose";
    std::string subName = "my-sub-name";
    Consumer consumer;
    ASSERT_EQ(ResultOk, client.subscribe(topicName, subName, consumer));
    ASSERT_EQ(consumer.close(), ResultOk);
    ASSERT_EQ(consumer.close(), ResultAlreadyClosed);
}

TEST(BasicEndToEndTest, testDuplicateConsumerCreationOnPartitionedTopic) {
    Client client(lookupUrl);
    std::string topicName = "partition-testDuplicateConsumerCreationOnPartitionedTopic";

    // call admin api to make it partitioned
    std::string url =
        adminUrl +
        "admin/v2/persistent/public/default/testDuplicateConsumerCreationOnPartitionedTopic/partitions";
    int res = makePutRequest(url, "5");

    LOG_INFO("res = " << res);
    ASSERT_FALSE(res != 204 && res != 409);

    std::this_thread::sleep_for(std::chrono::microseconds(2 * 1000 * 1000));

    Producer producer;
    ProducerConfiguration producerConfiguration;
    producerConfiguration.setPartitionsRoutingMode(ProducerConfiguration::CustomPartition);
    producerConfiguration.setMessageRouter(std::make_shared<CustomRoutingPolicy>());

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
    std::string topicName = "persistent://public/default/partition-testRoundRobinRoutingPolicy";
    // call admin api to make it partitioned
    std::string url =
        adminUrl + "admin/v2/persistent/public/default/partition-testRoundRobinRoutingPolicy/partitions";
    int res = makePutRequest(url, "5");

    LOG_INFO("res = " << res);
    ASSERT_FALSE(res != 204 && res != 409);

    Producer producer;
    ProducerConfiguration tempProducerConfiguration;
    tempProducerConfiguration.setPartitionsRoutingMode(ProducerConfiguration::RoundRobinDistribution);
    tempProducerConfiguration.setMessageRouter(std::make_shared<SimpleRoundRobinRoutingPolicy>());
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
    std::string topicName = "partition-testMessageListener";
    // call admin api to make it partitioned
    std::string url =
        adminUrl + "admin/v2/persistent/public/default/partition-testMessageListener/partitions";
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
    consumerConfig.setMessageListener(
        std::bind(messageListenerFunction, std::placeholders::_1, std::placeholders::_2));
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
    std::this_thread::sleep_for(std::chrono::microseconds(5 * 1000 * 1000));
    ASSERT_EQ(globalCount, 10);
    consumer.close();
    producer.close();
    client.close();
}

TEST(BasicEndToEndTest, testMessageListenerPause) {
    Client client(lookupUrl);
    std::string topicName = "partition-testMessageListenerPause";

    // call admin api to make it partitioned
    std::string url =
        adminUrl + "admin/v2/persistent/public/default/partition-testMessageListener-pauses/partitions";
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
    consumerConfig.setMessageListener(
        std::bind(messageListenerFunction, std::placeholders::_1, std::placeholders::_2));
    Consumer consumer;
    // Removing dangling subscription from previous test failures
    result = client.subscribe(topicName, "subscription-name", consumerConfig, consumer);
    consumer.unsubscribe();

    result = client.subscribe(topicName, "subscription-name", consumerConfig, consumer);
    ASSERT_EQ(ResultOk, result);
    int temp = 1000;
    for (int i = 0; i < 10000; i++) {
        if (i && i % 1000 == 0) {
            std::this_thread::sleep_for(std::chrono::microseconds(2 * 1000 * 1000));
            ASSERT_EQ(globalCount, temp);
            consumer.resumeMessageListener();
            std::this_thread::sleep_for(std::chrono::microseconds(2 * 1000 * 1000));
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
    std::this_thread::sleep_for(std::chrono::microseconds(2 * 1000 * 1000));

    ASSERT_EQ(globalCount, 10000);
    consumer.close();
    producer.close();
    client.close();
}

TEST(BasicEndToEndTest, testResendViaSendCallback) {
    ClientConfiguration clientConfiguration;
    clientConfiguration.setIOThreads(1);
    Client client(lookupUrl, clientConfiguration);
    std::string topicName = "testResendViaListener";

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
        producer.sendAsync(MessageBuilder().build(),
                           std::bind(resendMessage, std::placeholders::_1, std::placeholders::_2, producer));
    }
    // 3 seconds
    std::this_thread::sleep_for(std::chrono::microseconds(3 * 1000 * 1000));
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
    std::string topicName = "persistent://public/default/testStatsLatencies";
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
    int count = 0;
    for (int i = 0; i < numOfMessages; i++) {
        std::string messageContent = prefix + std::to_string(i);
        Message msg =
            MessageBuilder().setContent(messageContent).setProperty("msgIndex", std::to_string(i)).build();
        producer.sendAsync(msg, std::bind(&sendCallBackWithDelay, std::placeholders::_1,
                                          std::placeholders::_2, prefix, 15, 2 * 1e3, &count));
        LOG_DEBUG("sending message " << messageContent);
    }

    // Wait for all messages to be acked by broker
    while (PulsarFriend::sum(producerStatsImplPtr->getTotalSendMap()) < numOfMessages) {
        std::this_thread::sleep_for(std::chrono::microseconds(1000));  // 1 ms
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
        std::this_thread::sleep_for(std::chrono::seconds(1));  // wait till stats flush
    }

    std::this_thread::sleep_for(std::chrono::seconds(1));  // 1 second

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
        std::string expectedMessageContent = prefix + std::to_string(i);
        LOG_DEBUG("Received Message with [ content - " << receivedMsg.getDataAsString() << "] [ messageID = "
                                                       << receivedMsg.getMessageId() << "]");
        ASSERT_EQ(receivedMsg.getProperty("msgIndex"), std::to_string(i++));
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
    std::string topicName = "testProduceMessageSize";
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
    conf.setBatchingEnabled(false);
    client.createProducerAsync(topicName, conf, WaitForCallbackValue<Producer>(producerPromise2));
    producerFuture = producerPromise2.getFuture();
    result = producerFuture.get(producer2);
    ASSERT_EQ(ResultOk, result);

    int size = ClientConnection::getMaxMessageSize() + 1000 * 100;
    char *content = new char[size];
    memset(content, 0, size);
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

TEST(BasicEndToEndTest, testBigMessageSizeBatching) {
    ClientConfiguration config;
    Client client(lookupUrl);
    std::string topicName = "testBigMessageSizeBatching";
    std::string subName = "my-sub-name";

    ProducerConfiguration conf1;
    conf1.setCompressionType(CompressionNone);
    conf1.setBatchingEnabled(true);

    Producer producer1;
    Result result = client.createProducer(topicName, conf1, producer1);
    ASSERT_EQ(ResultOk, result);

    ProducerConfiguration conf2;
    conf2.setCompressionType(CompressionLZ4);
    conf2.setBatchingEnabled(true);

    Producer producer2;
    result = client.createProducer(topicName, conf2, producer2);
    ASSERT_EQ(ResultOk, result);

    int size = ClientConnection::getMaxMessageSize() + 1000 * 100;
    char *content = new char[size];
    memset(content, 0, size);
    Message msg = MessageBuilder().setAllocatedContent(content, size).build();
    result = producer1.send(msg);
    ASSERT_EQ(ResultMessageTooBig, result);

    msg = MessageBuilder().setAllocatedContent(content, size).build();
    result = producer2.send(msg);
    ASSERT_EQ(ResultOk, result);

    producer1.close();
    producer2.close();
    client.close();

    delete[] content;
}

TEST(BasicEndToEndTest, testHandlerReconnectionLogic) {
    Client client(adminUrl);
    std::string topicName = "testHandlerReconnectionLogic";

    Producer producer;
    Consumer consumer;

    ASSERT_EQ(client.subscribe(topicName, "my-sub", consumer), ResultOk);
    ASSERT_EQ(client.createProducer(topicName, producer), ResultOk);

    std::vector<ClientConnectionPtr> oldConnections;

    int numOfMessages = 10;
    std::string propertyName = "msgIndex";
    for (int i = 0; i < numOfMessages; i++) {
        std::string messageContent = "msg-" + std::to_string(i);
        Message msg =
            MessageBuilder().setContent(messageContent).setProperty(propertyName, std::to_string(i)).build();
        if (i % 3 == 1) {
            ProducerImpl &pImpl = PulsarFriend::getProducerImpl(producer);
            ClientConnectionPtr clientConnectionPtr;
            do {
                ClientConnectionWeakPtr clientConnectionWeakPtr = PulsarFriend::getClientConnection(pImpl);
                clientConnectionPtr = clientConnectionWeakPtr.lock();
                std::this_thread::sleep_for(std::chrono::seconds(1));
            } while (!clientConnectionPtr);
            oldConnections.push_back(clientConnectionPtr);
            clientConnectionPtr->close();
        }
        LOG_INFO("checking message " << i);
        ASSERT_EQ(producer.send(msg), ResultOk);
    }

    std::set<std::string> receivedMsgContent;
    std::set<std::string> receivedMsgIndex;

    Message msg;
    while (consumer.receive(msg, 30000) == ResultOk) {
        receivedMsgContent.insert(msg.getDataAsString());
        receivedMsgIndex.insert(msg.getProperty(propertyName));
    }

    ConsumerImpl &cImpl = PulsarFriend::getConsumerImpl(consumer);
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
        ASSERT_TRUE(receivedMsgContent.find("msg-" + std::to_string(i)) != receivedMsgContent.end());
        ASSERT_TRUE(receivedMsgIndex.find(std::to_string(i)) != receivedMsgIndex.end());
    }
}

void testHandlerReconnectionPartitionProducers(bool lazyStartPartitionedProducers, bool batchingEnabled) {
    Client client(adminUrl);
    std::string uniqueChunk = unique_str();
    std::string topicName = "testHandlerReconnectionLogicLazyProducers" + uniqueChunk;

    std::string url = adminUrl + "admin/v2/persistent/public/default/" + topicName + "/partitions";
    int res = makePutRequest(url, "1");
    LOG_INFO("res = " << res);
    ASSERT_FALSE(res != 204 && res != 409);

    ProducerConfiguration producerConf;
    producerConf.setSendTimeout(10000);
    producerConf.setLazyStartPartitionedProducers(lazyStartPartitionedProducers);
    producerConf.setBatchingEnabled(batchingEnabled);
    Producer producer;

    ASSERT_EQ(client.createProducer(topicName, producerConf, producer), ResultOk);

    std::vector<ClientConnectionPtr> oldConnections;

    int numOfMessages = 10;
    std::string propertyName = "msgIndex";
    for (int i = 0; i < numOfMessages; i++) {
        std::string messageContent = "msg-" + std::to_string(i);
        Message msg =
            MessageBuilder().setContent(messageContent).setProperty(propertyName, std::to_string(i)).build();
        if (i % 3 == 1) {
            ProducerImpl &pImpl = PulsarFriend::getInternalProducerImpl(producer, 0);
            ClientConnectionPtr clientConnectionPtr;
            do {
                ClientConnectionWeakPtr clientConnectionWeakPtr = PulsarFriend::getClientConnection(pImpl);
                clientConnectionPtr = clientConnectionWeakPtr.lock();
                std::this_thread::sleep_for(std::chrono::seconds(1));
            } while (!clientConnectionPtr);
            oldConnections.push_back(clientConnectionPtr);
            clientConnectionPtr->close();
        }
        ASSERT_EQ(producer.send(msg), ResultOk);
    }
}

TEST(BasicEndToEndTest, testHandlerReconnectionPartitionedProducersWithoutBatching) {
    testHandlerReconnectionPartitionProducers(false, false);
}

TEST(BasicEndToEndTest, testHandlerReconnectionPartitionedProducersWithBatching) {
    testHandlerReconnectionPartitionProducers(false, true);
}

TEST(BasicEndToEndTest, testHandlerReconnectionLazyPartitionedProducersWithoutBatching) {
    testHandlerReconnectionPartitionProducers(true, false);
}

TEST(BasicEndToEndTest, testHandlerReconnectionLazyPartitionedProducersWithBatching) {
    testHandlerReconnectionPartitionProducers(true, true);
}

TEST(BasicEndToEndTest, testRSAEncryption) {
    ClientConfiguration config;
    Client client(lookupUrl);
    std::string topicNames[] = {"my-rsaenctopic", "persistent://public/default-4/my-rsaenctopic"};
    std::string subName = "my-sub-name";
    Producer producer;

    std::string PUBLIC_CERT_FILE_PATH =
        "../../pulsar-broker/src/test/resources/certificate/public-key.client-rsa.pem";

    std::string PRIVATE_CERT_FILE_PATH =
        "../../pulsar-broker/src/test/resources/certificate/private-key.client-rsa.pem";

    std::shared_ptr<pulsar::DefaultCryptoKeyReader> keyReader =
        std::make_shared<pulsar::DefaultCryptoKeyReader>(PUBLIC_CERT_FILE_PATH, PRIVATE_CERT_FILE_PATH);
    ProducerConfiguration conf;
    conf.setCompressionType(CompressionLZ4);
    conf.addEncryptionKey("client-rsa.pem");
    conf.setCryptoKeyReader(keyReader);

    for (const auto &topicName : topicNames) {
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
        client.subscribeAsync(topicName, subName, consConfig,
                              WaitForCallbackValue<Consumer>(consumerPromise));
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
    }
    ASSERT_EQ(ResultOk, client.close());
}

TEST(BasicEndToEndTest, testEncryptionFailure) {
    ClientConfiguration config;
    Client client(lookupUrl);
    std::string topicName = "my-rsaencfailtopic";
    std::string subName = "my-sub-name";
    Producer producer;

    std::string PUBLIC_CERT_FILE_PATH =
        "../../pulsar-broker/src/test/resources/certificate/public-key.client-rsa-test.pem";

    std::string PRIVATE_CERT_FILE_PATH =
        "../../pulsar-broker/src/test/resources/certificate/private-key.client-rsa-test.pem";

    std::shared_ptr<pulsar::DefaultCryptoKeyReader> keyReader =
        std::make_shared<pulsar::DefaultCryptoKeyReader>(PUBLIC_CERT_FILE_PATH, PRIVATE_CERT_FILE_PATH);

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
        prodConf.setBatchingEnabled(false);
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
        PUBLIC_CERT_FILE_PATH =
            "../../pulsar-broker/src/test/resources/certificate/public-key.client-rsa.pem";

        PRIVATE_CERT_FILE_PATH =
            "../../pulsar-broker/src/test/resources/certificate/private-key.client-rsa.pem";
        keyReader =
            std::make_shared<pulsar::DefaultCryptoKeyReader>(PUBLIC_CERT_FILE_PATH, PRIVATE_CERT_FILE_PATH);
        ProducerConfiguration prodConf;
        prodConf.setCryptoKeyReader(keyReader);
        prodConf.setBatchingEnabled(false);
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
    std::string topicName = "test-event-time";
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
    std::string topicName = "persistent://public/default/testSeek";
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
        consumer.receive(msgReceived, 3000);
        LOG_DEBUG("Received message :" << msgReceived.getMessageId());
        std::stringstream expected;
        expected << msgContent << msgNum;
        ASSERT_EQ(expected.str(), msgReceived.getDataAsString());
        ASSERT_EQ(ResultOk, consumer.acknowledge(msgReceived));
    }

    // seek to earliest, expected receive first message.
    result = consumer.seek(MessageId::earliest());
    // Sleeping for 500ms to wait for consumer re-connect
    std::this_thread::sleep_for(std::chrono::microseconds(500 * 1000));

    ASSERT_EQ(ResultOk, result);
    consumer.receive(msgReceived, 3000);
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

TEST(BasicEndToEndTest, testSeekOnPartitionedTopic) {
    ClientConfiguration config;
    Client client(lookupUrl);
    std::string topicName = "persistent://public/default/testSeekOnPartitionedTopic";

    std::string url =
        adminUrl + "admin/v2/persistent/public/default/testSeekOnPartitionedTopic" + "/partitions";
    int res = makePutRequest(url, "3");
    LOG_INFO("res = " << res);
    ASSERT_FALSE(res != 204 && res != 409);

    std::string subName = "sub-testSeekOnPartitionedTopic";
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

    uint64_t timestampMillis = TimeUtils::currentTimeMillis();

    // Send 100 messages synchronously
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
        consumer.receive(msgReceived, 3000);
        LOG_DEBUG("Received message :" << msgReceived.getMessageId());
        std::stringstream expected;
        expected << msgContent << msgNum;
        ASSERT_EQ(expected.str(), msgReceived.getDataAsString());
        ASSERT_EQ(ResultOk, consumer.acknowledge(msgReceived));
    }

    // seek to the time before sending messages, expected receive first message.
    result = consumer.seek(timestampMillis);
    // Sleeping for 500ms to wait for consumer re-connect
    std::this_thread::sleep_for(std::chrono::microseconds(500 * 1000));

    ASSERT_EQ(ResultOk, result);
    consumer.receive(msgReceived, 3000);
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
    std::string topicName = "testUnAckedMessageTimeout";
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

static long messagesReceived = 0;

static void unackMessageListenerFunction(Consumer consumer, const Message &msg) { messagesReceived++; }

TEST(BasicEndToEndTest, testPartitionTopicUnAckedMessageTimeout) {
    Client client(lookupUrl);
    long unAckedMessagesTimeoutMs = 10000;

    std::string uniqueChunk = unique_str();
    std::string topicName =
        "persistent://public/default/testPartitionTopicUnAckedMessageTimeout" + uniqueChunk;

    // call admin api to make it partitioned
    std::string url = adminUrl +
                      "admin/v2/persistent/public/default/testPartitionTopicUnAckedMessageTimeout" +
                      uniqueChunk + "/partitions";
    int res = makePutRequest(url, "3");

    LOG_INFO("res = " << res);
    ASSERT_FALSE(res != 204 && res != 409);

    std::string subName = "my-sub-name";

    Producer producer;
    Result result = client.createProducer(topicName, producer);
    ASSERT_EQ(ResultOk, result);

    Consumer consumer;
    ConsumerConfiguration consConfig;
    consConfig.setMessageListener(
        std::bind(unackMessageListenerFunction, std::placeholders::_1, std::placeholders::_2));
    consConfig.setUnAckedMessagesTimeoutMs(unAckedMessagesTimeoutMs);
    result = client.subscribe(topicName, subName, consConfig, consumer);
    ASSERT_EQ(ResultOk, result);
    ASSERT_EQ(consumer.getSubscriptionName(), subName);

    for (int i = 0; i < 10; i++) {
        Message msg = MessageBuilder().setContent("test-" + std::to_string(i)).build();
        producer.sendAsync(msg, nullptr);
    }

    producer.flush();
    long timeWaited = 0;
    while (true) {
        // maximum wait time
        ASSERT_LE(timeWaited, unAckedMessagesTimeoutMs * 3);
        if (messagesReceived >= 10 * 2) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        timeWaited += 500;
    }

    client.close();
}

TEST(BasicEndToEndTest, testUnAckedMessageTimeoutListener) {
    Client client(lookupUrl);
    std::string topicName = "testUnAckedMessageTimeoutListener";
    std::string subName = "my-sub-name";
    std::string content = "msg-content";

    Producer producer;
    Result result = client.createProducer(topicName, producer);
    ASSERT_EQ(ResultOk, result);

    Consumer consumer;
    ConsumerConfiguration consConfig;
    consConfig.setUnAckedMessagesTimeoutMs(10 * 1000);
    Latch latch(2);
    consConfig.setMessageListener(std::bind(messageListenerFunctionWithoutAck, std::placeholders::_1,
                                            std::placeholders::_2, latch, content));
    result = client.subscribe(topicName, subName, consConfig, consumer);
    ASSERT_EQ(ResultOk, result);

    globalCount = 0;

    Message msg = MessageBuilder().setContent(content).build();
    result = producer.send(msg);
    ASSERT_EQ(ResultOk, result);

    ASSERT_TRUE(latch.wait(std::chrono::seconds(30)));
    ASSERT_GE(globalCount, 2);

    consumer.unsubscribe();
    consumer.close();
    producer.close();
    client.close();
}

TEST(BasicEndToEndTest, testMultiTopicsConsumerTopicNameInvalid) {
    Client client(lookupUrl);
    std::vector<std::string> topicNames;
    topicNames.reserve(3);
    std::string subName = "testMultiTopicsTopicNameInvalid";
    // cluster empty
    std::string topicName1 = "persistent://tenant/testMultiTopicsTopicNameInvalid";

    // empty topics
    ASSERT_EQ(0, topicNames.size());
    ConsumerConfiguration consConfig;
    consConfig.setConsumerType(ConsumerShared);
    Consumer consumer;
    Promise<Result, Consumer> consumerPromise;
    client.subscribeAsync(topicNames, subName, consConfig, WaitForCallbackValue<Consumer>(consumerPromise));
    Future<Result, Consumer> consumerFuture = consumerPromise.getFuture();
    Result result = consumerFuture.get(consumer);
    ASSERT_EQ(ResultOk, result);
    LOG_INFO("subscribe on empty topics");
    consumer.close();

    // Invalid topic names
    Consumer consumer1;
    std::string subName1 = "testMultiTopicsTopicNameInvalid";
    topicNames.push_back(topicName1);
    Promise<Result, Consumer> consumerPromise1;
    client.subscribeAsync(topicNames, subName1, consConfig, WaitForCallbackValue<Consumer>(consumerPromise1));
    Future<Result, Consumer> consumerFuture1 = consumerPromise1.getFuture();
    result = consumerFuture1.get(consumer1);
    ASSERT_EQ(ResultInvalidTopicName, result);
    LOG_INFO("subscribe on TopicName1 failed");
    consumer1.close();

    client.shutdown();
}

TEST(BasicEndToEndTest, testMultiTopicsConsumerConnectError) {
    Client client("pulsar://invalid-hostname:6650");
    std::vector<std::string> topicNames;
    topicNames.push_back("topic-1");
    topicNames.push_back("topic-2");

    Consumer consumer;
    Result res = client.subscribe(topicNames, "sub", consumer);
    ASSERT_EQ(ResultConnectError, res);

    client.shutdown();
}

TEST(BasicEndToEndTest, testMultiTopicsConsumerDifferentNamespace) {
    Client client(lookupUrl);
    std::vector<std::string> topicNames;
    topicNames.reserve(3);
    std::string subName = "testMultiTopicsDifferentNamespace";
    std::string topicName1 = "persistent://public/default/testMultiTopicsConsumerDifferentNamespace1";
    std::string topicName2 = "persistent://public/default-2/testMultiTopicsConsumerDifferentNamespace2";
    std::string topicName3 = "persistent://public/default-3/testMultiTopicsConsumerDifferentNamespace3";

    topicNames.push_back(topicName1);
    topicNames.push_back(topicName2);
    topicNames.push_back(topicName3);

    // key: message value integer, value: a pair of (topic, message id)
    using MessageInfo = std::pair<std::string, MessageId>;
    std::map<int, MessageInfo> messageIndexToInfo;
    int index = 0;
    // Produce some messages for each topic
    for (const auto &topic : topicNames) {
        Producer producer;
        ProducerConfiguration producerConfig;
        ASSERT_EQ(ResultOk, client.createProducer(topic, producerConfig, producer));

        const auto message = MessageBuilder().setContent(std::to_string(index)).build();
        MessageId messageId;
        ASSERT_EQ(ResultOk, producer.send(message, messageId));
        messageIndexToInfo[index] = std::make_pair(topic, messageId);
        LOG_INFO("Send " << index << " to " << topic << ", " << messageId);

        ASSERT_EQ(ResultOk, producer.close());
        index++;
    }

    ConsumerConfiguration consConfig;
    consConfig.setSubscriptionInitialPosition(InitialPositionEarliest);
    Consumer consumer;
    ASSERT_EQ(ResultOk, client.subscribe(topicNames, subName, consConfig, consumer));

    for (int i = 0; i < index; i++) {
        Message message;
        ASSERT_EQ(ResultOk, consumer.receive(message, 3000));
        ASSERT_EQ(ResultOk, consumer.acknowledge(message));
        const int index = std::stoi(message.getDataAsString());
        LOG_INFO("Receive " << index << " from " << message.getTopicName() << "," << message.getMessageId());
        ASSERT_EQ(messageIndexToInfo.count(index), 1);
        ASSERT_EQ(messageIndexToInfo[index], std::make_pair(message.getTopicName(), message.getMessageId()));
    }

    consumer.close();

    client.shutdown();
}

// Test subscribe 3 topics using MultiTopicsConsumer
TEST(BasicEndToEndTest, testMultiTopicsConsumerPubSub) {
    Client client(lookupUrl);
    std::vector<std::string> topicNames;
    topicNames.reserve(3);
    std::string subName = "testMultiTopicsConsumer";
    std::string topicName1 = "testMultiTopicsConsumer1";
    std::string topicName2 = "testMultiTopicsConsumer2";
    std::string topicName3 = "testMultiTopicsConsumer3";
    std::string topicName4 = "testMultiTopicsConsumer4";

    topicNames.push_back(topicName1);
    topicNames.push_back(topicName2);
    topicNames.push_back(topicName3);
    topicNames.push_back(topicName4);

    // call admin api to make topics partitioned
    std::string url1 = adminUrl + "admin/v2/persistent/public/default/testMultiTopicsConsumer1/partitions";
    std::string url2 = adminUrl + "admin/v2/persistent/public/default/testMultiTopicsConsumer2/partitions";
    std::string url3 = adminUrl + "admin/v2/persistent/public/default/testMultiTopicsConsumer3/partitions";

    int res = makePutRequest(url1, "2");
    ASSERT_FALSE(res != 204 && res != 409);
    res = makePutRequest(url2, "3");
    ASSERT_FALSE(res != 204 && res != 409);
    res = makePutRequest(url3, "4");
    ASSERT_FALSE(res != 204 && res != 409);

    Producer producer1;
    Result result = client.createProducer(topicName1, producer1);
    ASSERT_EQ(ResultOk, result);
    Producer producer2;
    result = client.createProducer(topicName2, producer2);
    ASSERT_EQ(ResultOk, result);
    Producer producer3;
    result = client.createProducer(topicName3, producer3);
    ASSERT_EQ(ResultOk, result);

    Producer producer4;
    result = client.createProducer(topicName4, producer4);
    ASSERT_EQ(ResultOk, result);

    LOG_INFO("created 4 producers");

    int messageNumber = 100;
    ConsumerConfiguration consConfig;
    consConfig.setConsumerType(ConsumerShared);
    consConfig.setReceiverQueueSize(10);  // size for each sub-consumer
    Consumer consumer;
    Promise<Result, Consumer> consumerPromise;
    client.subscribeAsync(topicNames, subName, consConfig, WaitForCallbackValue<Consumer>(consumerPromise));
    Future<Result, Consumer> consumerFuture = consumerPromise.getFuture();
    result = consumerFuture.get(consumer);
    ASSERT_EQ(ResultOk, result);
    ASSERT_EQ(consumer.getSubscriptionName(), subName);
    LOG_INFO("created topics consumer on 4 topics");

    std::string msgContent = "msg-content";
    LOG_INFO("Publishing 100 messages by producer 1 synchronously");
    for (int msgNum = 0; msgNum < messageNumber; msgNum++) {
        std::stringstream stream;
        stream << msgContent << msgNum;
        Message msg = MessageBuilder().setContent(stream.str()).build();
        ASSERT_EQ(ResultOk, producer1.send(msg));
    }

    msgContent = "msg-content2";
    LOG_INFO("Publishing 100 messages by producer 2 synchronously");
    for (int msgNum = 0; msgNum < messageNumber; msgNum++) {
        std::stringstream stream;
        stream << msgContent << msgNum;
        Message msg = MessageBuilder().setContent(stream.str()).build();
        ASSERT_EQ(ResultOk, producer2.send(msg));
    }

    msgContent = "msg-content3";
    LOG_INFO("Publishing 100 messages by producer 3 synchronously");
    for (int msgNum = 0; msgNum < messageNumber; msgNum++) {
        std::stringstream stream;
        stream << msgContent << msgNum;
        Message msg = MessageBuilder().setContent(stream.str()).build();
        ASSERT_EQ(ResultOk, producer3.send(msg));
    }

    msgContent = "msg-content4";
    LOG_INFO("Publishing 100 messages by producer 4 synchronously");
    for (int msgNum = 0; msgNum < messageNumber; msgNum++) {
        std::stringstream stream;
        stream << msgContent << msgNum;
        Message msg = MessageBuilder().setContent(stream.str()).build();
        ASSERT_EQ(ResultOk, producer4.send(msg));
    }

    LOG_INFO("Consuming and acking 400 messages by multiTopicsConsumer");
    for (int i = 0; i < 4 * messageNumber; i++) {
        Message m;
        ASSERT_EQ(ResultOk, consumer.receive(m, 10000));
        ASSERT_EQ(ResultOk, consumer.acknowledge(m));
    }

    LOG_INFO("Consumed and acked 400 messages by multiTopicsConsumer");

    ASSERT_EQ(ResultOk, consumer.unsubscribe());

    client.shutdown();
}

TEST(BasicEndToEndTest, testPatternTopicsConsumerInvalid) {
    Client client(lookupUrl);

    // invalid namespace
    std::string pattern = "invalidDomain://prop/unit/ns/patternMultiTopicsConsumerInvalid.*";
    std::string subName = "testPatternMultiTopicsConsumerInvalid";

    Consumer consumer;
    Promise<Result, Consumer> consumerPromise;
    client.subscribeWithRegexAsync(pattern, subName, WaitForCallbackValue<Consumer>(consumerPromise));
    Future<Result, Consumer> consumerFuture = consumerPromise.getFuture();
    Result result = consumerFuture.get(consumer);
    ASSERT_EQ(ResultInvalidTopicName, result);

    client.shutdown();
}

// create 4 topics, in which 3 topics match the pattern,
// verify PatternMultiTopicsConsumer subscribed matched topics,
// and only receive messages from matched topics.
TEST(BasicEndToEndTest, testPatternMultiTopicsConsumerPubSub) {
    Client client(lookupUrl);
    std::string pattern = "persistent://public/default/patternMultiTopicsConsumer.*";

    std::string subName = "testPatternMultiTopicsConsumer";
    std::string topicName1 = "persistent://public/default/patternMultiTopicsConsumerPubSub1";
    std::string topicName2 = "persistent://public/default/patternMultiTopicsConsumerPubSub2";
    std::string topicName3 = "persistent://public/default/patternMultiTopicsConsumerPubSub3";
    // This will not match pattern
    std::string topicName4 = "persistent://public/default/patternMultiTopicsNotMatchPubSub4";

    // call admin api to make topics partitioned
    std::string url1 =
        adminUrl + "admin/v2/persistent/public/default/patternMultiTopicsConsumerPubSub1/partitions";
    std::string url2 =
        adminUrl + "admin/v2/persistent/public/default/patternMultiTopicsConsumerPubSub2/partitions";
    std::string url3 =
        adminUrl + "admin/v2/persistent/public/default/patternMultiTopicsConsumerPubSub3/partitions";
    std::string url4 =
        adminUrl + "admin/v2/persistent/public/default/patternMultiTopicsNotMatchPubSub4/partitions";

    makeDeleteRequest(url1);
    int res = makePutRequest(url1, "2");
    ASSERT_FALSE(res != 204 && res != 409);
    makeDeleteRequest(url2);
    res = makePutRequest(url2, "3");
    ASSERT_FALSE(res != 204 && res != 409);
    makeDeleteRequest(url3);
    res = makePutRequest(url3, "4");
    ASSERT_FALSE(res != 204 && res != 409);
    makeDeleteRequest(url4);
    res = makePutRequest(url4, "4");
    ASSERT_FALSE(res != 204 && res != 409);

    Producer producer1;
    Result result = client.createProducer(topicName1, producer1);
    ASSERT_EQ(ResultOk, result);
    Producer producer2;
    result = client.createProducer(topicName2, producer2);
    ASSERT_EQ(ResultOk, result);
    Producer producer3;
    result = client.createProducer(topicName3, producer3);
    ASSERT_EQ(ResultOk, result);
    Producer producer4;
    result = client.createProducer(topicName4, producer4);
    ASSERT_EQ(ResultOk, result);

    LOG_INFO("created 3 producers that match, with partitions: 2, 3, 4, and 1 producer not match");

    int messageNumber = 100;
    ConsumerConfiguration consConfig;
    consConfig.setConsumerType(ConsumerShared);
    consConfig.setReceiverQueueSize(10);  // size for each sub-consumer
    Consumer consumer;
    Promise<Result, Consumer> consumerPromise;
    client.subscribeWithRegexAsync(pattern, subName, consConfig,
                                   WaitForCallbackValue<Consumer>(consumerPromise));
    Future<Result, Consumer> consumerFuture = consumerPromise.getFuture();
    result = consumerFuture.get(consumer);
    ASSERT_EQ(ResultOk, result);
    ASSERT_EQ(consumer.getSubscriptionName(), subName);
    LOG_INFO("created topics consumer on a pattern that match 3 topics");

    std::string msgContent = "msg-content";
    LOG_INFO("Publishing 100 messages by producer 1 synchronously");
    for (int msgNum = 0; msgNum < messageNumber; msgNum++) {
        std::stringstream stream;
        stream << msgContent << msgNum;
        Message msg = MessageBuilder().setContent(stream.str()).build();
        ASSERT_EQ(ResultOk, producer1.send(msg));
    }

    msgContent = "msg-content2";
    LOG_INFO("Publishing 100 messages by producer 2 synchronously");
    for (int msgNum = 0; msgNum < messageNumber; msgNum++) {
        std::stringstream stream;
        stream << msgContent << msgNum;
        Message msg = MessageBuilder().setContent(stream.str()).build();
        ASSERT_EQ(ResultOk, producer2.send(msg));
    }

    msgContent = "msg-content3";
    LOG_INFO("Publishing 100 messages by producer 3 synchronously");
    for (int msgNum = 0; msgNum < messageNumber; msgNum++) {
        std::stringstream stream;
        stream << msgContent << msgNum;
        Message msg = MessageBuilder().setContent(stream.str()).build();
        ASSERT_EQ(ResultOk, producer3.send(msg));
    }

    msgContent = "msg-content4";
    LOG_INFO("Publishing 100 messages by producer 4 synchronously");
    for (int msgNum = 0; msgNum < messageNumber; msgNum++) {
        std::stringstream stream;
        stream << msgContent << msgNum;
        Message msg = MessageBuilder().setContent(stream.str()).build();
        ASSERT_EQ(ResultOk, producer4.send(msg));
    }

    LOG_INFO("Consuming and acking 300 messages by multiTopicsConsumer");
    for (int i = 0; i < 3 * messageNumber; i++) {
        Message m;
        ASSERT_EQ(ResultOk, consumer.receive(m, 1000));
        ASSERT_EQ(ResultOk, consumer.acknowledge(m));
    }
    LOG_INFO("Consumed and acked 300 messages by multiTopicsConsumer");

    // verify no more to receive, because producer4 not match pattern
    Message m;
    ASSERT_EQ(ResultTimeout, consumer.receive(m, 1000));

    ASSERT_EQ(ResultOk, consumer.unsubscribe());

    client.shutdown();
}

// User adminUrl to create client, to protect http related services
TEST(BasicEndToEndTest, testpatternMultiTopicsHttpConsumerPubSub) {
    Client client(adminUrl);
    std::string pattern = "persistent://public/default/patternMultiTopicsHttpConsumer.*";

    std::string subName = "testpatternMultiTopicsHttpConsumer";
    std::string topicName1 = "persistent://public/default/patternMultiTopicsHttpConsumerPubSub1";
    std::string topicName2 = "persistent://public/default/patternMultiTopicsHttpConsumerPubSub2";
    std::string topicName3 = "persistent://public/default/patternMultiTopicsHttpConsumerPubSub3";

    // call admin api to make topics partitioned
    std::string url1 =
        adminUrl + "admin/v2/persistent/public/default/patternMultiTopicsHttpConsumerPubSub1/partitions";
    std::string url2 =
        adminUrl + "admin/v2/persistent/public/default/patternMultiTopicsHttpConsumerPubSub2/partitions";
    std::string url3 =
        adminUrl + "admin/v2/persistent/public/default/patternMultiTopicsHttpConsumerPubSub3/partitions";

    makeDeleteRequest(url1);
    int res = makePutRequest(url1, "2");
    ASSERT_FALSE(res != 204 && res != 409);
    makeDeleteRequest(url2);
    res = makePutRequest(url2, "3");
    ASSERT_FALSE(res != 204 && res != 409);
    makeDeleteRequest(url3);
    res = makePutRequest(url3, "4");
    ASSERT_FALSE(res != 204 && res != 409);

    Producer producer1;
    Result result = client.createProducer(topicName1, producer1);
    ASSERT_EQ(ResultOk, result);
    Producer producer2;
    result = client.createProducer(topicName2, producer2);
    ASSERT_EQ(ResultOk, result);
    Producer producer3;
    result = client.createProducer(topicName3, producer3);
    ASSERT_EQ(ResultOk, result);

    LOG_INFO("created 3 producers that match, with partitions: 2, 3, 4");

    int messageNumber = 100;
    ConsumerConfiguration consConfig;
    consConfig.setConsumerType(ConsumerShared);
    consConfig.setReceiverQueueSize(10);  // size for each sub-consumer
    Consumer consumer;
    Promise<Result, Consumer> consumerPromise;
    client.subscribeWithRegexAsync(pattern, subName, consConfig,
                                   WaitForCallbackValue<Consumer>(consumerPromise));
    Future<Result, Consumer> consumerFuture = consumerPromise.getFuture();
    result = consumerFuture.get(consumer);
    ASSERT_EQ(ResultOk, result);
    ASSERT_EQ(consumer.getSubscriptionName(), subName);
    LOG_INFO("created topics consumer on a pattern that match 3 topics");

    std::string msgContent = "msg-content";
    LOG_INFO("Publishing 100 messages by producer 1 synchronously");
    for (int msgNum = 0; msgNum < messageNumber; msgNum++) {
        std::stringstream stream;
        stream << msgContent << msgNum;
        Message msg = MessageBuilder().setContent(stream.str()).build();
        ASSERT_EQ(ResultOk, producer1.send(msg));
    }

    msgContent = "msg-content2";
    LOG_INFO("Publishing 100 messages by producer 2 synchronously");
    for (int msgNum = 0; msgNum < messageNumber; msgNum++) {
        std::stringstream stream;
        stream << msgContent << msgNum;
        Message msg = MessageBuilder().setContent(stream.str()).build();
        ASSERT_EQ(ResultOk, producer2.send(msg));
    }

    msgContent = "msg-content3";
    LOG_INFO("Publishing 100 messages by producer 3 synchronously");
    for (int msgNum = 0; msgNum < messageNumber; msgNum++) {
        std::stringstream stream;
        stream << msgContent << msgNum;
        Message msg = MessageBuilder().setContent(stream.str()).build();
        ASSERT_EQ(ResultOk, producer3.send(msg));
    }

    LOG_INFO("Consuming and acking 300 messages by multiTopicsConsumer");
    for (int i = 0; i < 3 * messageNumber; i++) {
        Message m;
        ASSERT_EQ(ResultOk, consumer.receive(m, 1000));
        ASSERT_EQ(ResultOk, consumer.acknowledge(m));
    }
    LOG_INFO("Consumed and acked 300 messages by multiTopicsConsumer");

    // verify no more to receive
    Message m;
    ASSERT_EQ(ResultTimeout, consumer.receive(m, 1000));

    ASSERT_EQ(ResultOk, consumer.unsubscribe());

    client.shutdown();
}

TEST(BasicEndToEndTest, testPatternEmptyUnsubscribe) {
    Client client(lookupUrl);
    std::string pattern = "persistent://public/default/patternEmptyUnsubscribe.*";

    std::string subName = "testPatternMultiTopicsConsumer";

    ConsumerConfiguration consConfig;
    Consumer consumer;
    Result result = client.subscribeWithRegex(pattern, subName, consConfig, consumer);
    ASSERT_EQ(ResultOk, result);
    ASSERT_EQ(consumer.getSubscriptionName(), subName);
    LOG_INFO("created topics consumer on a pattern that match 0 topics");

    result = consumer.unsubscribe();
    LOG_INFO("unsubscribed topics consumer : " << result);
    ASSERT_EQ(ResultOk, result) << "expected " << ResultOk << " but found " << result;

    // TODO: flaky test
    // client.shutdown();
}

// create a pattern consumer, which contains no match topics at beginning.
// create 4 topics, in which 3 topics match the pattern.
// verify PatternMultiTopicsConsumer subscribed matched topics, after a while,
// and only receive messages from matched topics.
TEST(BasicEndToEndTest, testPatternMultiTopicsConsumerAutoDiscovery) {
    Client client(lookupUrl);
    std::string pattern = "persistent://public/default/patternTopicsAutoConsumer.*";
    Result result;
    std::string subName = "testPatternTopicsAutoConsumer";

    // 1.  create a pattern consumer, which contains no match topics at beginning.
    ConsumerConfiguration consConfig;
    consConfig.setConsumerType(ConsumerShared);
    consConfig.setReceiverQueueSize(10);          // size for each sub-consumer
    consConfig.setPatternAutoDiscoveryPeriod(1);  // set waiting time for auto discovery
    Consumer consumer;
    Promise<Result, Consumer> consumerPromise;
    client.subscribeWithRegexAsync(pattern, subName, consConfig,
                                   WaitForCallbackValue<Consumer>(consumerPromise));
    Future<Result, Consumer> consumerFuture = consumerPromise.getFuture();
    result = consumerFuture.get(consumer);
    ASSERT_EQ(ResultOk, result);
    ASSERT_EQ(consumer.getSubscriptionName(), subName);
    LOG_INFO("created pattern consumer with not match topics at beginning");

    auto createProducer = [&client](Producer &producer, const std::string &topic, int numPartitions) {
        if (numPartitions > 0) {
            const std::string url = adminUrl + "admin/v2/persistent/public/default/" + topic + "/partitions";
            makeDeleteRequest(url);
            int res = makePutRequest(url, std::to_string(numPartitions));
            ASSERT_TRUE(res == 204 || res == 409);
        }

        const std::string fullTopicName = "persistent://public/default/" + topic;
        Result result = client.createProducer(fullTopicName, producer);
        ASSERT_EQ(ResultOk, result);
    };

    // 2. create 4 topics, in which 3 match the pattern.
    std::vector<Producer> producers(4);
    createProducer(producers[0], "patternTopicsAutoConsumerPubSub1", 2);
    createProducer(producers[1], "patternTopicsAutoConsumerPubSub2", 3);
    createProducer(producers[2], "patternTopicsAutoConsumerPubSub3", 4);
    // This will not match pattern
    createProducer(producers[3], "notMatchPatternTopicsAutoConsumerPubSub4", 4);

    constexpr int messageNumber = 100;

    std::thread consumeThread([&consumer] {
        LOG_INFO("Consuming and acking 300 messages by pattern topics consumer");
        for (int i = 0; i < 3 * messageNumber; i++) {
            Message m;
            // Ensure new topics can be discovered when the consumer is blocked by receive(Message&, int)
            ASSERT_EQ(ResultOk, consumer.receive(m, 30000));
            ASSERT_EQ(ResultOk, consumer.acknowledge(m));
        }
        // 5. pattern consumer already subscribed 3 topics
        LOG_INFO("Consumed and acked 300 messages by pattern topics consumer");

        // verify no more to receive, because producers[3] not match pattern
        Message m;
        ASSERT_EQ(ResultTimeout, consumer.receive(m, 1000));
    });

    // 3. wait enough time to trigger auto discovery
    std::this_thread::sleep_for(std::chrono::seconds(2));

    // 4. produce data.
    for (size_t i = 0; i < producers.size(); i++) {
        const std::string msgContent = "msg-content" + std::to_string(i);
        LOG_INFO("Publishing " << messageNumber << " messages by producer " << i << " synchronously");
        for (int j = 0; j < messageNumber; j++) {
            Message msg = MessageBuilder().setContent(msgContent).build();
            ASSERT_EQ(ResultOk, producers[i].send(msg));
        }
    }

    consumeThread.join();

    consumeThread = std::thread([&consumer] {
        LOG_INFO("Consuming and acking 100 messages by pattern topics consumer");
        for (int i = 0; i < messageNumber; i++) {
            Message m;
            // Ensure new topics can be discovered when the consumer is blocked by receive(Message&)
            ASSERT_EQ(ResultOk, consumer.receive(m));
            ASSERT_EQ(ResultOk, consumer.acknowledge(m));
        }
        // 9. pattern consumer subscribed a new topic
        LOG_INFO("Consumed and acked 100 messages by pattern topics consumer");

        // verify no more to receive
        Message m;
        ASSERT_EQ(ResultTimeout, consumer.receive(m, 1000));
    });
    // 6. Create a producer to a new topic
    createProducer(producers[0], "patternTopicsAutoConsumerPubSub5", 4);

    // 7. wait enough time to trigger auto discovery
    std::this_thread::sleep_for(std::chrono::seconds(2));

    // 8. produce data
    for (int i = 0; i < messageNumber; i++) {
        Message msg = MessageBuilder().setContent("msg-content-5").build();
        ASSERT_EQ(ResultOk, producers[0].send(msg));
    }

    consumeThread.join();
    ASSERT_EQ(ResultOk, consumer.unsubscribe());
    client.shutdown();
}

TEST(BasicEndToEndTest, testSyncFlushBatchMessages) {
    ClientConfiguration config;
    Client client(lookupUrl);
    std::string topicName = "test-flush-batch-messages-" + std::to_string(time(NULL));
    std::string subName = "subscription-name";
    Producer producer;

    int numOfMessages = 10;

    ProducerConfiguration conf;

    conf.setBatchingEnabled(true);
    // set batch message number numOfMessages, and max delay 60s
    conf.setBatchingMaxMessages(numOfMessages);
    conf.setBatchingMaxPublishDelayMs(60000);

    conf.setBlockIfQueueFull(true);
    conf.setProperty("producer-name", "test-producer-name");
    conf.setProperty("producer-id", "test-producer-id");

    Promise<Result, Producer> producerPromise;
    client.createProducerAsync(topicName, conf, WaitForCallbackValue<Producer>(producerPromise));
    Future<Result, Producer> producerFuture = producerPromise.getFuture();
    Result result = producerFuture.get(producer);
    ASSERT_EQ(ResultOk, result);

    Consumer consumer;
    ConsumerConfiguration consumerConfig;
    consumerConfig.setProperty("consumer-name", "test-consumer-name");
    consumerConfig.setProperty("consumer-id", "test-consumer-id");
    Promise<Result, Consumer> consumerPromise;
    client.subscribeAsync(topicName, subName, consumerConfig,
                          WaitForCallbackValue<Consumer>(consumerPromise));
    Future<Result, Consumer> consumerFuture = consumerPromise.getFuture();
    result = consumerFuture.get(consumer);
    ASSERT_EQ(ResultOk, result);

    // Send Asynchronously of half the messages
    std::string prefix = "msg-batch-async";
    int msgCount = 0;
    for (int i = 0; i < numOfMessages / 2; i++) {
        std::string messageContent = prefix + std::to_string(i);
        Message msg =
            MessageBuilder().setContent(messageContent).setProperty("msgIndex", std::to_string(i)).build();
        producer.sendAsync(
            msg, std::bind(&sendCallBack, std::placeholders::_1, std::placeholders::_2, prefix, &msgCount));
        LOG_DEBUG("async sending message " << messageContent);
    }
    LOG_INFO("sending first half messages in async, should timeout to receive");

    // message not reached max batch number, should not receive any data.
    Message receivedMsg;
    ASSERT_EQ(ResultTimeout, consumer.receive(receivedMsg, 1000));

    // Send Asynchronously of the other half the messages
    for (int i = numOfMessages / 2; i < numOfMessages; i++) {
        std::string messageContent = prefix + std::to_string(i);
        Message msg =
            MessageBuilder().setContent(messageContent).setProperty("msgIndex", std::to_string(i)).build();
        producer.sendAsync(
            msg, std::bind(&sendCallBack, std::placeholders::_1, std::placeholders::_2, prefix, &msgCount));
        LOG_DEBUG("async sending message " << messageContent);
    }
    LOG_INFO("sending the other half messages in async, should able to receive");
    // message not reached max batch number, should received the messages
    ASSERT_EQ(ResultOk, consumer.receive(receivedMsg, 2000));

    LOG_INFO("Receive all messages");
    // receive all the messages.
    int i = 1;
    while (consumer.receive(receivedMsg, 1000) == ResultOk) {
        std::string expectedMessageContent = prefix + std::to_string(i);
        LOG_INFO("Received Message with [ content - "
                 << receivedMsg.getDataAsString() << "] [ messageID = " << receivedMsg.getMessageId() << "]"
                 << "property = " << receivedMsg.getProperty("msgIndex"));
        ASSERT_EQ(receivedMsg.getProperty("msgIndex"), std::to_string(i++));
        ASSERT_EQ(expectedMessageContent, receivedMsg.getDataAsString());
        ASSERT_EQ(ResultOk, consumer.acknowledge(receivedMsg));
    }

    LOG_INFO("Last sync send round");
    // Send sync of half the messages, this will triggerFlush, and could get the messages.
    prefix = "msg-batch-sync";
    for (int i = 0; i < numOfMessages / 2; i++) {
        std::string messageContent = prefix + std::to_string(i);
        Message msg =
            MessageBuilder().setContent(messageContent).setProperty("msgIndex", std::to_string(i)).build();
        producer.send(msg);
        LOG_INFO("sync sending message " << messageContent);
    }
    // message not reached max batch number, should received the messages, and not timeout
    ASSERT_EQ(ResultOk, consumer.receive(receivedMsg, 1000));

    producer.close();
    client.shutdown();
}

// for partitioned reason, it may hard to verify message id.
static void simpleCallback(Result code, const MessageId &msgId) {
    LOG_INFO("Received code: " << code << " -- MsgID: " << msgId);
}

void testSyncFlushBatchMessagesPartitionedTopic(bool lazyStartPartitionedProducers) {
    Client client(lookupUrl);
    std::string uniqueChunk = unique_str();
    std::string topicName = "persistent://public/default/partition-testSyncFlushBatchMessages" + uniqueChunk;
    // call admin api to make it partitioned
    std::string url = adminUrl + "admin/v2/persistent/public/default/partition-testSyncFlushBatchMessages" +
                      uniqueChunk + "/partitions";
    int res = makePutRequest(url, "5");
    const int numberOfPartitions = 5;

    LOG_INFO("res = " << res);
    ASSERT_FALSE(res != 204 && res != 409);

    Producer producer;
    int numOfMessages = 20;
    // lazy partitioned producers make a single call to the message router during createProducer
    int initPart = lazyStartPartitionedProducers ? 1 : 0;
    ProducerConfiguration tempProducerConfiguration;
    tempProducerConfiguration.setMessageRouter(std::make_shared<SimpleRoundRobinRoutingPolicy>());
    ProducerConfiguration producerConfiguration = tempProducerConfiguration;
    producerConfiguration.setBatchingEnabled(true);
    // set batch message number numOfMessages, and max delay 60s
    producerConfiguration.setBatchingMaxMessages(numOfMessages / numberOfPartitions);
    producerConfiguration.setBatchingMaxPublishDelayMs(60000);
    producerConfiguration.setLazyStartPartitionedProducers(lazyStartPartitionedProducers);

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
    std::vector<Consumer> consumer(numberOfPartitions);
    Result subscribeResult;
    for (int i = 0; i < numberOfPartitions; i++) {
        std::stringstream partitionedTopicName;
        partitionedTopicName << topicName << "-partition-" << i;

        std::stringstream partitionedConsumerId;
        partitionedConsumerId << consumerId << i;
        client.subscribe(partitionedTopicName.str(), partitionedConsumerId.str(), consConfig, consumer[i]);
        consumer[i].unsubscribe();
        subscribeResult = client.subscribe(partitionedTopicName.str(), partitionedConsumerId.str(),
                                           consConfig, consumer[i]);

        ASSERT_EQ(ResultOk, subscribeResult);
        ASSERT_EQ(consumer[i].getTopic(), partitionedTopicName.str());
    }

    // Send asynchronously of first part the messages
    std::string prefix = "msg-batch-async";
    for (int i = 0; i < numOfMessages / numberOfPartitions / 2; i++) {
        std::string messageContent = prefix + std::to_string(i);
        Message msg =
            MessageBuilder().setContent(messageContent).setProperty("msgIndex", std::to_string(i)).build();
        producer.sendAsync(msg, simpleCallback);
        LOG_DEBUG("async sending message " << messageContent);
    }
    LOG_INFO("sending first part messages in async, should timeout to receive");

    Message m;
    ASSERT_EQ(ResultTimeout, consumer[initPart].receive(m, 5000));

    for (int i = numOfMessages / numberOfPartitions / 2; i < numOfMessages; i++) {
        std::string messageContent = prefix + std::to_string(i);
        Message msg =
            MessageBuilder().setContent(messageContent).setProperty("msgIndex", std::to_string(i)).build();
        producer.sendAsync(msg, simpleCallback);
        LOG_DEBUG("async sending message " << messageContent);
    }
    LOG_INFO("sending second part messages in async, should be able to receive");

    for (int i = 0; i < numOfMessages / numberOfPartitions; i++) {
        for (int partitionIndex = 0; partitionIndex < numberOfPartitions; partitionIndex++) {
            ASSERT_EQ(ResultOk, consumer[partitionIndex].receive(m));
            ASSERT_EQ(ResultOk, consumer[partitionIndex].acknowledge(m));
        }
    }

    // Sync send of first part of the messages, this will triggerFlush, and could get the messages.
    prefix = "msg-batch-sync";
    for (int i = 0; i < numOfMessages / numberOfPartitions / 2; i++) {
        std::string messageContent = prefix + std::to_string(i);
        Message msg =
            MessageBuilder().setContent(messageContent).setProperty("msgIndex", std::to_string(i)).build();
        ASSERT_EQ(ResultOk, producer.send(msg));
        LOG_DEBUG("sync sending message " << messageContent);
    }

    LOG_INFO("sending first part messages in sync, should not timeout to receive");
    ASSERT_EQ(ResultOk, consumer[initPart].receive(m, 10000));

    producer.close();
    client.shutdown();
}

TEST(BasicEndToEndTest, testSyncFlushBatchMessagesPartitionedTopic) {
    testSyncFlushBatchMessagesPartitionedTopic(false);
}

TEST(BasicEndToEndTest, testSyncFlushBatchMessagesPartitionedTopicLazyProducers) {
    testSyncFlushBatchMessagesPartitionedTopic(true);
}

TEST(BasicEndToEndTest, testGetTopicPartitions) {
    Client client(lookupUrl);
    std::string topicName = "persistent://public/default/testGetPartitions";

    // call admin api to make it partitioned
    std::string url = adminUrl + "admin/v2/persistent/public/default/testGetPartitions/partitions";
    int res = makePutRequest(url, "3");

    LOG_INFO("res = " << res);
    ASSERT_FALSE(res != 204 && res != 409);
    std::vector<std::string> partitionsList;
    Result result = client.getPartitionsForTopic(topicName, partitionsList);
    ASSERT_EQ(ResultOk, result);
    ASSERT_EQ(3, partitionsList.size());
    ASSERT_EQ(topicName + "-partition-0", partitionsList[0]);
    ASSERT_EQ(topicName + "-partition-1", partitionsList[1]);
    ASSERT_EQ(topicName + "-partition-2", partitionsList[2]);

    std::vector<std::string> partitionsList2;
    result = client.getPartitionsForTopic("persistent://public/default/testGetPartitions-non-partitioned",
                                          partitionsList2);
    ASSERT_EQ(ResultOk, result);
    ASSERT_EQ(1, partitionsList2.size());
    ASSERT_EQ(partitionsList2[0], "persistent://public/default/testGetPartitions-non-partitioned");

    client.shutdown();
}

TEST(BasicEndToEndTest, testFlushInProducer) {
    ClientConfiguration config;
    Client client(lookupUrl);
    std::string topicName = "test-flush-in-producer";
    std::string subName = "subscription-name";
    Producer producer;
    int numOfMessages = 10;

    ProducerConfiguration conf;
    conf.setBatchingEnabled(true);
    // set batch message number numOfMessages, and max delay 60s
    conf.setBatchingMaxMessages(numOfMessages);
    conf.setBatchingMaxPublishDelayMs(60000);

    conf.setBlockIfQueueFull(true);
    conf.setProperty("producer-name", "test-producer-name");
    conf.setProperty("producer-id", "test-producer-id");

    Promise<Result, Producer> producerPromise;
    client.createProducerAsync(topicName, conf, WaitForCallbackValue<Producer>(producerPromise));
    Future<Result, Producer> producerFuture = producerPromise.getFuture();
    Result result = producerFuture.get(producer);
    ASSERT_EQ(ResultOk, result);

    Consumer consumer;
    ConsumerConfiguration consumerConfig;
    consumerConfig.setProperty("consumer-name", "test-consumer-name");
    consumerConfig.setProperty("consumer-id", "test-consumer-id");
    Promise<Result, Consumer> consumerPromise;
    client.subscribe(topicName, subName, consumerConfig, consumer);
    consumer.unsubscribe();
    client.subscribeAsync(topicName, subName, consumerConfig,
                          WaitForCallbackValue<Consumer>(consumerPromise));
    Future<Result, Consumer> consumerFuture = consumerPromise.getFuture();
    result = consumerFuture.get(consumer);
    ASSERT_EQ(ResultOk, result);

    // Send Asynchronously of half the messages
    std::string prefix = "msg-batch-async";
    int msgCount = 0;
    for (int i = 0; i < numOfMessages / 2; i++) {
        std::string messageContent = prefix + std::to_string(i);
        Message msg =
            MessageBuilder().setContent(messageContent).setProperty("msgIndex", std::to_string(i)).build();
        producer.sendAsync(
            msg, std::bind(&sendCallBack, std::placeholders::_1, std::placeholders::_2, prefix, &msgCount));
        LOG_DEBUG("async sending message " << messageContent);
    }
    LOG_INFO("sending half of messages in async, should timeout to receive");

    // message not reached max batch number, should not receive any data.
    Message receivedMsg;
    ASSERT_EQ(ResultTimeout, consumer.receive(receivedMsg, 2000));

    // After flush, it should get the message
    producer.flush();
    ASSERT_EQ(ResultOk, consumer.receive(receivedMsg, 2000));

    // receive all the messages.
    while (consumer.receive(receivedMsg, 2000) == ResultOk) {
        ASSERT_EQ(ResultOk, consumer.acknowledge(receivedMsg));
    }

    // Send Asynchronously of another round of the messages
    for (int i = numOfMessages / 2; i < numOfMessages; i++) {
        std::string messageContent = prefix + std::to_string(i);
        Message msg =
            MessageBuilder().setContent(messageContent).setProperty("msgIndex", std::to_string(i)).build();
        producer.sendAsync(
            msg, std::bind(&sendCallBack, std::placeholders::_1, std::placeholders::_2, prefix, &msgCount));
        LOG_DEBUG("async sending message " << messageContent);
    }
    LOG_INFO(
        "sending the other half messages in async, should still timeout, since first half already flushed");
    ASSERT_EQ(ResultTimeout, consumer.receive(receivedMsg, 2000));

    // After flush async, it should get the message
    Promise<bool, Result> promise;
    producer.flushAsync(WaitForCallback(promise));
    Promise<bool, Result> promise1;
    producer.flushAsync(WaitForCallback(promise1));
    promise.getFuture().get(result);
    ASSERT_EQ(ResultOk, result);
    ASSERT_EQ(ResultOk, consumer.receive(receivedMsg, 2000));

    producer.close();
    client.shutdown();
}

void testFlushInPartitionedProducer(bool lazyStartPartitionedProducers) {
    Client client(lookupUrl);
    std::string uniqueChunk = unique_str();
    std::string topicName =
        "persistent://public/default/partition-testFlushInPartitionedProducer" + uniqueChunk;
    // call admin api to make it partitioned
    std::string url = adminUrl +
                      "admin/v2/persistent/public/default/partition-testFlushInPartitionedProducer" +
                      uniqueChunk + "/partitions";
    int res = makePutRequest(url, "5");
    const int numberOfPartitions = 5;

    LOG_INFO("res = " << res);
    ASSERT_FALSE(res != 204 && res != 409);

    Producer producer;
    int numOfMessages = 10;
    ProducerConfiguration tempProducerConfiguration;
    tempProducerConfiguration.setPartitionsRoutingMode(ProducerConfiguration::RoundRobinDistribution);
    ProducerConfiguration producerConfiguration = tempProducerConfiguration;
    producerConfiguration.setBatchingEnabled(true);
    // set batch message number numOfMessages, and max delay 60s
    producerConfiguration.setBatchingMaxMessages(numOfMessages / numberOfPartitions);
    producerConfiguration.setBatchingMaxPublishDelayMs(60000);
    producerConfiguration.setMessageRouter(std::make_shared<SimpleRoundRobinRoutingPolicy>());
    producerConfiguration.setLazyStartPartitionedProducers(lazyStartPartitionedProducers);

    Result result = client.createProducer(topicName, producerConfiguration, producer);
    ASSERT_EQ(ResultOk, result);
    ASSERT_EQ(producer.getTopic(), topicName);

    LOG_INFO("Creating Subscriber");
    std::string consumerId = "CONSUMER";
    ConsumerConfiguration consConfig;
    consConfig.setConsumerType(ConsumerExclusive);
    consConfig.setReceiverQueueSize(2);
    ASSERT_FALSE(consConfig.hasMessageListener());
    std::vector<Consumer> consumer(numberOfPartitions);
    Result subscribeResult;
    for (int i = 0; i < numberOfPartitions; i++) {
        std::stringstream partitionedTopicName;
        partitionedTopicName << topicName << "-partition-" << i;

        std::stringstream partitionedConsumerId;
        partitionedConsumerId << consumerId << i;
        subscribeResult = client.subscribe(partitionedTopicName.str(), partitionedConsumerId.str(),
                                           consConfig, consumer[i]);
        consumer[i].unsubscribe();
        subscribeResult = client.subscribe(partitionedTopicName.str(), partitionedConsumerId.str(),
                                           consConfig, consumer[i]);
        ASSERT_EQ(ResultOk, subscribeResult);
        ASSERT_EQ(consumer[i].getTopic(), partitionedTopicName.str());
    }

    // Send asynchronously of first part the messages
    std::string prefix = "msg-batch-async";
    for (int i = 0; i < numOfMessages / 2; i++) {
        std::string messageContent = prefix + std::to_string(i);
        Message msg =
            MessageBuilder().setContent(messageContent).setProperty("msgIndex", std::to_string(i)).build();
        producer.sendAsync(msg, simpleCallback);
        LOG_DEBUG("async sending message " << messageContent);
    }

    LOG_INFO("sending first part messages in async, should timeout to receive");
    Message m;
    ASSERT_EQ(ResultTimeout, consumer[0].receive(m, 2000));

    // After flush, should be able to consume.
    producer.flush();
    LOG_INFO("After flush, should be able to receive");
    ASSERT_EQ(ResultOk, consumer[0].receive(m, 2000));

    LOG_INFO("Receive all messages.");
    // receive all the messages.
    for (int partitionIndex = 0; partitionIndex < numberOfPartitions; partitionIndex++) {
        while (consumer[partitionIndex].receive(m, 2000) == ResultOk) {
            // ASSERT_EQ(ResultOk, consumer[partitionIndex].acknowledge(m));
            ASSERT_EQ(ResultOk, consumer[partitionIndex].acknowledge(m));
        }
    }

    // send message again.
    for (int i = numOfMessages / 2; i < numOfMessages; i++) {
        std::string messageContent = prefix + std::to_string(i);
        Message msg =
            MessageBuilder().setContent(messageContent).setProperty("msgIndex", std::to_string(i)).build();
        producer.sendAsync(msg, simpleCallback);
        LOG_DEBUG("async sending message " << messageContent);
    }

    // After flush async, it should get the message
    Promise<bool, Result> promise;
    producer.flushAsync(WaitForCallback(promise));
    Promise<bool, Result> promise1;
    producer.flushAsync(WaitForCallback(promise1));
    promise.getFuture().get(result);
    ASSERT_EQ(ResultOk, result);
    ASSERT_EQ(ResultOk, consumer[0].receive(m, 2000));

    producer.close();
    client.shutdown();
}

TEST(BasicEndToEndTest, testFlushInPartitionedProducer) { testFlushInPartitionedProducer(false); }

TEST(BasicEndToEndTest, testFlushInLazyPartitionedProducer) { testFlushInPartitionedProducer(true); }

TEST(BasicEndToEndTest, testReceiveAsync) {
    ClientConfiguration config;
    Client client(lookupUrl);
    std::string topicName = "persistent://public/default/receiveAsync";
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

    std::string content = "msg-1-content";
    int count = 0;
    int totalMsgs = 5;
    bool isFailed = false;
    for (int i = 0; i < totalMsgs; i++) {
        consumer.receiveAsync(std::bind(&receiveCallBack, std::placeholders::_1, std::placeholders::_2,
                                        content, true, &isFailed, &count));
    }
    // Send synchronously
    for (int i = 0; i < totalMsgs; i++) {
        Message msg = MessageBuilder().setContent(content).build();
        result = producer.send(msg);
        ASSERT_EQ(ResultOk, result);
    }

    // check strategically
    for (int i = 0; i < 3; i++) {
        if (count == totalMsgs) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::microseconds(1 * 1000 * 1000));
    }
    ASSERT_FALSE(isFailed);
    ASSERT_EQ(count, totalMsgs);
    client.shutdown();
}

TEST(BasicEndToEndTest, testPartitionedReceiveAsync) {
    Client client(lookupUrl);
    std::string topicName = "persistent://public/default/receiveAsync-partition";

    // call admin api to make it partitioned
    std::string url = adminUrl + "admin/v2/persistent/public/default/receiveAsync-partition/partitions";
    int res = makePutRequest(url, "3");

    LOG_INFO("res = " << res);
    ASSERT_FALSE(res != 204 && res != 409);

    Producer producer;
    Result result = client.createProducer(topicName, producer);
    ASSERT_EQ(ResultOk, result);

    Consumer consumer;
    result = client.subscribe(topicName, "subscription-A", consumer);
    ASSERT_EQ(ResultOk, result);

    int totalMsgs = 10;
    std::string content;
    int count = 0;
    bool isFailed = false;
    for (int i = 0; i < totalMsgs; i++) {
        consumer.receiveAsync(std::bind(&receiveCallBack, std::placeholders::_1, std::placeholders::_2,
                                        content, false, &isFailed, &count));
    }

    for (int i = 0; i < totalMsgs; i++) {
        boost::posix_time::ptime t(boost::posix_time::microsec_clock::universal_time());
        long nanoSeconds = t.time_of_day().total_nanoseconds();
        std::stringstream ss;
        ss << nanoSeconds;
        Message msg = MessageBuilder().setContent(ss.str()).setPartitionKey(ss.str()).build();
        ASSERT_EQ(ResultOk, producer.send(msg));
        LOG_DEBUG("Message Timestamp is " << msg.getPublishTimestamp());
        LOG_DEBUG("Message is " << msg);
    }

    // check strategically
    for (int i = 0; i < 3; i++) {
        if (count == totalMsgs) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::microseconds(1 * 1000 * 1000));
    }
    ASSERT_FALSE(isFailed);
    ASSERT_EQ(count, totalMsgs);
    client.shutdown();
}

TEST(BasicEndToEndTest, testBatchMessagesReceiveAsync) {
    ClientConfiguration config;
    Client client(lookupUrl);
    std::string topicName = "persistent://public/default/receiveAsync-batch";
    std::string subName = "subscription-name";
    Producer producer;

    // Enable batching on producer side
    int batchSize = 2;
    int numOfMessages = 100;

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

    std::string content;
    int count = 0;
    bool isFailed = false;
    for (int i = 0; i < numOfMessages; i++) {
        consumer.receiveAsync(std::bind(&receiveCallBack, std::placeholders::_1, std::placeholders::_2,
                                        content, false, &isFailed, &count));
    }

    // Send Asynchronously
    std::string prefix = "msg-batch-";
    int msgCount = 0;
    for (int i = 0; i < numOfMessages; i++) {
        std::string messageContent = prefix + std::to_string(i);
        Message msg =
            MessageBuilder().setContent(messageContent).setProperty("msgIndex", std::to_string(i)).build();
        producer.sendAsync(
            msg, std::bind(&sendCallBack, std::placeholders::_1, std::placeholders::_2, prefix, &msgCount));
        LOG_DEBUG("sending message " << messageContent);
    }

    // check strategically
    for (int i = 0; i < 3; i++) {
        if (count == numOfMessages) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::microseconds(1 * 1000 * 1000));
    }
    ASSERT_FALSE(isFailed);
    ASSERT_EQ(count, numOfMessages);
}

TEST(BasicEndToEndTest, testReceiveAsyncFailedConsumer) {
    ClientConfiguration config;
    Client client(lookupUrl);
    std::string topicName = "persistent://public/default/receiveAsync-failed";
    std::string subName = "my-sub-name";

    Consumer consumer;
    Promise<Result, Consumer> consumerPromise;
    client.subscribeAsync(topicName, subName, WaitForCallbackValue<Consumer>(consumerPromise));
    Future<Result, Consumer> consumerFuture = consumerPromise.getFuture();
    Result result = consumerFuture.get(consumer);
    ASSERT_EQ(ResultOk, result);

    bool isFailedOnConsumerClosing = false;
    std::string content;
    int closingCunt = 0;
    // callback should immediately fail
    consumer.receiveAsync(std::bind(&receiveCallBack, std::placeholders::_1, std::placeholders::_2, content,
                                    false, &isFailedOnConsumerClosing, &closingCunt));

    // close consumer
    consumer.close();
    bool isFailedOnConsumerClosed = false;
    int count = 0;
    // callback should immediately fail
    consumer.receiveAsync(std::bind(&receiveCallBack, std::placeholders::_1, std::placeholders::_2, content,
                                    false, &isFailedOnConsumerClosed, &count));

    // check strategically
    for (int i = 0; i < 3; i++) {
        if (isFailedOnConsumerClosing && isFailedOnConsumerClosed) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::microseconds(1 * 1000 * 1000));
    }

    ASSERT_TRUE(isFailedOnConsumerClosing);
    ASSERT_TRUE(isFailedOnConsumerClosed);
    ASSERT_EQ(count, 0);

    client.shutdown();
}

TEST(BasicEndToEndTest, testPartitionedReceiveAsyncFailedConsumer) {
    Client client(lookupUrl);
    std::string topicName = "persistent://public/default/receiveAsync-fail-partition";

    // call admin api to make it partitioned
    std::string url = adminUrl + "admin/v2/persistent/public/default/receiveAsync-fail-partition/partitions";
    int res = makePutRequest(url, "3");

    LOG_INFO("res = " << res);
    ASSERT_FALSE(res != 204 && res != 409);

    Consumer consumer;
    Result result = client.subscribe(topicName, "subscription-A", consumer);
    ASSERT_EQ(ResultOk, result);

    bool isFailedOnConsumerClosing = false;
    std::string content;
    int closingCunt = 0;
    // callback should immediately fail
    consumer.receiveAsync(std::bind(&receiveCallBack, std::placeholders::_1, std::placeholders::_2, content,
                                    false, &isFailedOnConsumerClosing, &closingCunt));
    // close consumer
    consumer.close();

    int count = 0;
    bool isFailedOnConsumerClosed = false;
    consumer.receiveAsync(std::bind(&receiveCallBack, std::placeholders::_1, std::placeholders::_2, content,
                                    false, &isFailedOnConsumerClosed, &count));

    // check strategically
    for (int i = 0; i < 3; i++) {
        if (isFailedOnConsumerClosing && isFailedOnConsumerClosed) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::microseconds(1 * 1000 * 1000));
    }

    ASSERT_TRUE(isFailedOnConsumerClosing);
    ASSERT_TRUE(isFailedOnConsumerClosed);
    ASSERT_EQ(count, 0);
    client.shutdown();
}

static void expectTimeoutOnRecv(Consumer &consumer) {
    Message msg;
    Result res = consumer.receive(msg, 100);
    if (res != ResultTimeout) {
        LOG_ERROR("Received a msg when not expecting to id(" << msg.getMessageId() << ") "
                                                             << msg.getDataAsString());
    }
    ASSERT_EQ(ResultTimeout, res);
}

void testNegativeAcks(const std::string &topic, bool batchingEnabled) {
    Client client(lookupUrl);
    Consumer consumer;
    ConsumerConfiguration conf;
    conf.setNegativeAckRedeliveryDelayMs(100);
    Result result = client.subscribe(topic, "test", conf, consumer);
    ASSERT_EQ(ResultOk, result);

    Producer producer;
    ProducerConfiguration producerConf;
    producerConf.setBatchingEnabled(batchingEnabled);
    result = client.createProducer(topic, producerConf, producer);
    ASSERT_EQ(ResultOk, result);

    for (int i = 0; i < 10; i++) {
        Message msg = MessageBuilder().setContent("test-" + std::to_string(i)).build();
        producer.sendAsync(msg, nullptr);
    }

    producer.flush();

    std::vector<MessageId> toNeg;
    for (int i = 0; i < 10; i++) {
        Message msg;
        consumer.receive(msg);

        LOG_INFO("Received message " << msg.getDataAsString());
        ASSERT_EQ(msg.getDataAsString(), "test-" + std::to_string(i));
        toNeg.push_back(msg.getMessageId());
    }
    // No more messages expected
    expectTimeoutOnRecv(consumer);

    PulsarFriend::setNegativeAckEnabled(consumer, false);
    // negatively acknowledge all at once
    for (auto &&msgId : toNeg) {
        consumer.negativeAcknowledge(msgId);
    }
    PulsarFriend::setNegativeAckEnabled(consumer, true);

    for (int i = 0; i < 10; i++) {
        Message msg;
        consumer.receive(msg);
        LOG_INFO("-- Redelivery -- Received message " << msg.getDataAsString());

        ASSERT_EQ(msg.getDataAsString(), "test-" + std::to_string(i));

        consumer.acknowledge(msg);
    }

    // No more messages expected
    expectTimeoutOnRecv(consumer);

    client.shutdown();
}

TEST(BasicEndToEndTest, testNegativeAcks) {
    testNegativeAcks("testNegativeAcks-" + std::to_string(time(nullptr)), false);
}

TEST(BasicEndToEndTest, testNegativeAcksWithBatching) {
    testNegativeAcks("testNegativeAcksWithBatching-" + std::to_string(time(nullptr)), true);
}

TEST(BasicEndToEndTest, testNegativeAcksWithPartitions) {
    std::string topicName = "testNegativeAcksWithPartitions-" + std::to_string(time(nullptr));

    // call admin api to make it partitioned
    std::string url = adminUrl + "admin/v2/persistent/public/default/" + topicName + "/partitions";
    int res = makePutRequest(url, "3");

    LOG_INFO("res = " << res);
    ASSERT_FALSE(res != 204 && res != 409);

    testNegativeAcks(topicName, true);
}

static long regexTestMessagesReceived = 0;

static void regexMessageListenerFunction(Consumer consumer, const Message &msg) {
    regexTestMessagesReceived++;
}

TEST(BasicEndToEndTest, testRegexTopicsWithMessageListener) {
    ClientConfiguration config;
    Client client(lookupUrl);
    long unAckedMessagesTimeoutMs = 10000;
    std::string subsName = "testRegexTopicsWithMessageListener-sub";
    std::string pattern = "persistent://public/default/testRegexTopicsWithMessageListenerTopic-.*";
    ConsumerConfiguration consumerConf;
    consumerConf.setConsumerType(ConsumerShared);
    consumerConf.setMessageListener(
        std::bind(regexMessageListenerFunction, std::placeholders::_1, std::placeholders::_2));
    consumerConf.setUnAckedMessagesTimeoutMs(unAckedMessagesTimeoutMs);

    Producer producer;
    ProducerConfiguration producerConf;
    Result result = client.createProducer(
        "persistent://public/default/testRegexTopicsWithMessageListenerTopic-1", producerConf, producer);
    ASSERT_EQ(ResultOk, result);

    Consumer consumer;
    result = client.subscribeWithRegex(pattern, subsName, consumerConf, consumer);
    ASSERT_EQ(ResultOk, result);
    ASSERT_EQ(consumer.getSubscriptionName(), subsName);

    for (int i = 0; i < 10; i++) {
        Message msg = MessageBuilder().setContent("test-" + std::to_string(i)).build();
        producer.sendAsync(msg, nullptr);
    }

    producer.flush();
    long timeWaited = 0;
    while (true) {
        // maximum wait time
        ASSERT_LE(timeWaited, unAckedMessagesTimeoutMs * 3);
        if (regexTestMessagesReceived >= 10 * 2) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        timeWaited += 500;
    }
}

TEST(BasicEndToEndTest, testRegexTopicsWithInitialPosition) {
    ClientConfiguration config;
    Client client(lookupUrl);

    std::string topicName =
        "persistent://public/default/test-regex-initial-position-" + std::to_string(time(NULL));

    Producer producer;
    Result result = client.createProducer(topicName, producer);
    ASSERT_EQ(ResultOk, result);

    for (int i = 0; i < 10; i++) {
        producer.send(MessageBuilder().setContent("test-" + std::to_string(i)).build());
    }

    std::string subsName = "testRegexTopicsWithMessageListener-sub";
    std::string pattern = topicName + ".*";

    // Subscription gets created after messages are produced but it will start from the beginning of the topic
    ConsumerConfiguration consumerConf;
    consumerConf.setSubscriptionInitialPosition(InitialPositionEarliest);

    Consumer consumer;
    result = client.subscribeWithRegex(pattern, subsName, consumerConf, consumer);
    ASSERT_EQ(ResultOk, result);
    ASSERT_EQ(consumer.getSubscriptionName(), subsName);

    for (int i = 0; i < 10; i++) {
        Message msg;
        ASSERT_EQ(ResultOk, consumer.receive(msg));
    }

    client.close();
}

TEST(BasicEndToEndTest, testPartitionedTopicWithOnePartition) {
    ClientConfiguration config;
    Client client(lookupUrl);
    std::string topicName = "testPartitionedTopicWithOnePartition" + unique_str();
    std::string subsName = topicName + "-sub-";

    // call admin api to make 1 partition
    std::string url = adminUrl + "admin/v2/persistent/public/default/" + topicName + "/partitions";
    int putRes = makePutRequest(url, "1");
    LOG_INFO("res = " << putRes);
    ASSERT_FALSE(putRes != 204 && putRes != 409);

    Consumer consumer1;
    ConsumerConfiguration conf;
    Result result = client.subscribe(topicName, subsName + "1", consumer1);
    ASSERT_EQ(ResultOk, result);

    Consumer consumer2;
    result = client.subscribe(topicName + "-partition-0", subsName + "2", consumer2);
    ASSERT_EQ(ResultOk, result);

    LOG_INFO("created 2 consumer");

    Producer producer1;
    ProducerConfiguration producerConf;
    producerConf.setBatchingEnabled(false);
    result = client.createProducer(topicName, producerConf, producer1);
    ASSERT_EQ(ResultOk, result);

    Producer producer2;
    result = client.createProducer(topicName + "-partition-0", producerConf, producer2);
    ASSERT_EQ(ResultOk, result);

    LOG_INFO("created 2 producer");

    // create messages
    int numMessages = 10;
    for (int i = 0; i < numMessages; i++) {
        Message msg = MessageBuilder().setContent("test-producer1-" + topicName + std::to_string(i)).build();
        producer1.send(msg);
        msg = MessageBuilder().setContent("test-producer2-" + topicName + std::to_string(i)).build();
        producer2.send(msg);
    }

    // produced 10 messages by each producer.
    // expected receive 20 messages by each consumer.
    for (int i = 0; i < numMessages * 2; i++) {
        LOG_INFO("begin to receive message " << i);

        Message msg;
        Result res = consumer1.receive(msg, 3000);
        ASSERT_EQ(ResultOk, res);
        consumer1.acknowledge(msg);

        res = consumer2.receive(msg, 3000);
        ASSERT_EQ(ResultOk, res);
        consumer2.acknowledge(msg);
    }

    // No more messages expected
    Message msg;
    Result res = consumer1.receive(msg, 100);
    ASSERT_EQ(ResultTimeout, res);

    res = consumer2.receive(msg, 100);
    ASSERT_EQ(ResultTimeout, res);
    client.shutdown();
}

TEST(BasicEndToEndTest, testDelayedMessages) {
    std::string topicName = "testDelayedMessages-" + std::to_string(TimeUtils::currentTimeMillis());
    Client client(lookupUrl);

    Producer producer;
    Result result = client.createProducer(topicName, producer);
    ASSERT_EQ(ResultOk, result);

    Consumer consumer;
    ConsumerConfiguration consumerConf;
    consumerConf.setConsumerType(ConsumerShared);
    result = client.subscribe(topicName, "my-sub-name", consumerConf, consumer);
    ASSERT_EQ(ResultOk, result);

    Message msg1 =
        MessageBuilder().setContent("msg-1").setDeliverAfter(std::chrono::milliseconds(5000)).build();
    ASSERT_EQ(ResultOk, producer.send(msg1));

    // 2nd message without delay
    Message msg2 = MessageBuilder().setContent("msg-2").build();
    ASSERT_EQ(ResultOk, producer.send(msg2));

    Message msgReceived;
    result = consumer.receive(msgReceived);
    ASSERT_EQ(ResultOk, result);
    ASSERT_EQ("msg-2", msgReceived.getDataAsString());

    auto result1 = client.close();
    std::cout << "closed with " << result1 << std::endl;
    ASSERT_EQ(ResultOk, result1);
}

TEST(BasicEndToEndTest, testCumulativeAcknowledgeNotAllowed) {
    ClientConfiguration config;
    Client client(lookupUrl);
    std::string topicName = "testCumulativeAcknowledgeNotAllowed";
    std::string subsName = topicName + "-sub-";

    Consumer consumer1;
    ConsumerConfiguration consumerConfiguration1;
    consumerConfiguration1.setConsumerType(ConsumerShared);

    Result result = client.subscribe(topicName, subsName + "1", consumerConfiguration1, consumer1);
    ASSERT_EQ(ResultOk, result);

    Consumer consumer2;
    ConsumerConfiguration consumerConfiguration2;
    consumerConfiguration2.setConsumerType(ConsumerKeyShared);

    result = client.subscribe(topicName, subsName + "2", consumerConfiguration2, consumer2);
    ASSERT_EQ(ResultOk, result);

    Producer producer;
    result = client.createProducer(topicName, producer);
    ASSERT_EQ(ResultOk, result);

    // publish messages
    int numMessages = 10;
    for (int i = 0; i < numMessages; i++) {
        Message msg = MessageBuilder().setContent("test-producer-" + topicName + std::to_string(i)).build();
        producer.send(msg);
    }

    // test cannot use acknowledgeCumulative on Shared subscription
    for (int i = 0; i < numMessages; i++) {
        Message msg;
        Result res = consumer1.receive(msg, 3000);
        ASSERT_EQ(ResultOk, res);
        if (i == 9) {
            res = consumer1.acknowledgeCumulative(msg);
            ASSERT_EQ(ResultCumulativeAcknowledgementNotAllowedError, res);
        }
    }

    // test cannot use acknowledgeCumulative on Key_Shared subscription
    for (int i = 0; i < numMessages; i++) {
        Message msg;
        Result res = consumer2.receive(msg, 3000);
        ASSERT_EQ(ResultOk, res);
        if (i == 9) {
            res = consumer2.acknowledgeCumulative(msg);
            ASSERT_EQ(ResultCumulativeAcknowledgementNotAllowedError, res);
        }
    }
    client.shutdown();
}

TEST(BasicEndToEndTest, testSendCallback) {
    const std::string topicName = "persistent://public/default/BasicEndToEndTest-testSendCallback";

    Client client(lookupUrl);

    Producer producer;
    ASSERT_EQ(ResultOk, client.createProducer(topicName, producer));

    Consumer consumer;
    ASSERT_EQ(ResultOk, client.subscribe(topicName, "SubscriptionName", consumer));

    Latch latch(100);
    std::set<MessageId> sentIdSet;
    for (int i = 0; i < 100; i++) {
        const auto msg = MessageBuilder().setContent("a").build();
        producer.sendAsync(msg, [&sentIdSet, &latch](Result result, const MessageId &id) {
            ASSERT_EQ(ResultOk, result);
            sentIdSet.emplace(id);
            latch.countdown();
        });
    }

    std::set<MessageId> receivedIdSet;
    for (int i = 0; i < 100; i++) {
        Message msg;
        ASSERT_EQ(ResultOk, consumer.receive(msg));
        receivedIdSet.emplace(msg.getMessageId());
        consumer.acknowledge(msg);
    }

    latch.wait();
    ASSERT_EQ(sentIdSet, receivedIdSet);

    consumer.close();
    producer.close();

    const std::string partitionedTopicName = topicName + "-" + std::to_string(time(nullptr));
    const std::string url = adminUrl + "admin/v2/persistent/" +
                            partitionedTopicName.substr(partitionedTopicName.find("://") + 3) + "/partitions";
    const int numPartitions = 3;

    int res = makePutRequest(url, std::to_string(numPartitions));
    ASSERT_TRUE(res == 204 || res == 409) << "res: " << res;

    std::this_thread::sleep_for(std::chrono::seconds(2));

    ProducerConfiguration producerConfig;
    producerConfig.setBatchingEnabled(false);
    producerConfig.setPartitionsRoutingMode(ProducerConfiguration::RoundRobinDistribution);
    ASSERT_EQ(ResultOk, client.createProducer(partitionedTopicName, producerConfig, producer));
    ASSERT_EQ(ResultOk, client.subscribe(partitionedTopicName, "SubscriptionName", consumer));

    sentIdSet.clear();
    receivedIdSet.clear();

    const int numMessages = numPartitions * 2;
    latch = Latch(numMessages);
    for (int i = 0; i < numMessages; i++) {
        const auto msg = MessageBuilder().setContent("a").build();
        producer.sendAsync(msg, [&sentIdSet, &latch](Result result, const MessageId &id) {
            ASSERT_EQ(ResultOk, result);
            sentIdSet.emplace(id);
            latch.countdown();
        });
    }

    for (int i = 0; i < numMessages; i++) {
        Message msg;
        ASSERT_EQ(ResultOk, consumer.receive(msg));
        receivedIdSet.emplace(msg.getMessageId());
        consumer.acknowledge(msg);
    }

    latch.wait();
    ASSERT_EQ(sentIdSet, receivedIdSet);

    std::set<int> partitionIndexSet;
    for (const auto &id : sentIdSet) {
        partitionIndexSet.emplace(id.partition());
    }
    std::set<int> expectedPartitionIndexSet;
    for (int i = 0; i < numPartitions; i++) {
        expectedPartitionIndexSet.emplace(i);
    }
    ASSERT_EQ(sentIdSet, receivedIdSet);

    consumer.close();
    producer.close();
    client.close();
}

class AckGroupingTrackerMock : public AckGroupingTracker {
   public:
    explicit AckGroupingTrackerMock(bool mockAck) : mockAck_(mockAck) {}

    bool callDoImmediateAck(ClientConnectionWeakPtr connWeakPtr, uint64_t consumerId, const MessageId &msgId,
                            proto::CommandAck_AckType ackType) {
        if (!this->mockAck_) {
            // Not mocking ACK, expose this method.
            return this->doImmediateAck(connWeakPtr, consumerId, msgId, ackType);
        } else {
            // Mocking ACK.
            return true;
        }
    }

    bool callDoImmediateAck(ClientConnectionWeakPtr connWeakPtr, uint64_t consumerId,
                            const std::set<MessageId> &msgIds) {
        if (!this->mockAck_) {
            // Not mocking ACK, expose this method.
            return this->doImmediateAck(connWeakPtr, consumerId, msgIds);
        } else {
            // Mocking ACK.
            return true;
        }
    }

   private:
    bool mockAck_;
};  // class AckGroupingTrackerMock

TEST(BasicEndToEndTest, testAckGroupingTrackerDefaultBehavior) {
    ConsumerConfiguration configConsumer;
    ASSERT_EQ(configConsumer.getAckGroupingTimeMs(), 100);
    ASSERT_EQ(configConsumer.getAckGroupingMaxSize(), 1000);

    AckGroupingTracker tracker;
    Message msg;
    ASSERT_FALSE(tracker.isDuplicate(msg.getMessageId()));
}

TEST(BasicEndToEndTest, testAckGroupingTrackerSingleAckBehavior) {
    constexpr auto numMsg = 10;
    const std::string topicName = "testAckGroupingTrackerSingleAckBehavior" + std::to_string(time(nullptr));
    const std::string subName = "sub-ack-grp-single-ack-behavior";

    // Setup client, producer and consumer.
    Client client(lookupUrl);

    Producer producer;
    ProducerConfiguration configProducer;
    configProducer.setBatchingEnabled(false);
    ASSERT_EQ(ResultOk, client.createProducer(topicName, configProducer, producer));

    Consumer consumer;
    ASSERT_EQ(ResultOk, client.subscribe(topicName, subName, consumer));

    auto &consumerImpl = PulsarFriend::getConsumerImpl(consumer);
    auto connWeakPtr = PulsarFriend::getClientConnection(consumerImpl);

    // Sending and receiving messages.
    for (auto count = 0; count < numMsg; ++count) {
        Message msg = MessageBuilder().setContent(std::string("MSG-") + std::to_string(count)).build();
        ASSERT_EQ(ResultOk, producer.send(msg));
    }

    std::vector<MessageId> recvMsgId;
    for (auto count = 0; count < numMsg; ++count) {
        Message msg;
        ASSERT_EQ(ResultOk, consumer.receive(msg, 1000));
        recvMsgId.emplace_back(msg.getMessageId());
    }

    // Send ACK.
    AckGroupingTrackerMock tracker(false);
    tracker.start();
    for (auto msgIdx = 0; msgIdx < numMsg; ++msgIdx) {
        auto connPtr = connWeakPtr.lock();
        ASSERT_NE(connPtr, nullptr);
        ASSERT_TRUE(tracker.callDoImmediateAck(connWeakPtr, consumerImpl.getConsumerId(), recvMsgId[msgIdx],
                                               proto::CommandAck::Individual));
    }
    Message msg;
    ASSERT_EQ(ResultTimeout, consumer.receive(msg, 1000));
    consumer.close();

    std::this_thread::sleep_for(std::chrono::seconds(1));
    ASSERT_EQ(ResultOk, client.subscribe(topicName, subName, consumer));
    ASSERT_EQ(ResultTimeout, consumer.receive(msg, 1000));
    consumer.close();
}

TEST(BasicEndToEndTest, testAckGroupingTrackerMultiAckBehavior) {
    constexpr auto numMsg = 10;
    const std::string topicName = "testAckGroupingTrackerMultiAckBehavior" + std::to_string(time(nullptr));
    const std::string subName = "sub-ack-grp-multi-ack-behavior";

    // Setup client, producer and consumer.
    Client client(lookupUrl);

    Producer producer;
    ProducerConfiguration configProducer;
    configProducer.setBatchingEnabled(false);
    ASSERT_EQ(ResultOk, client.createProducer(topicName, configProducer, producer));

    Consumer consumer;
    ASSERT_EQ(ResultOk, client.subscribe(topicName, subName, consumer));

    auto &consumerImpl = PulsarFriend::getConsumerImpl(consumer);
    auto connWeakPtr = PulsarFriend::getClientConnection(consumerImpl);

    // Sending and receiving messages.
    for (auto count = 0; count < numMsg; ++count) {
        Message msg = MessageBuilder().setContent(std::string("MSG-") + std::to_string(count)).build();
        ASSERT_EQ(ResultOk, producer.send(msg));
    }

    std::vector<MessageId> recvMsgId;
    for (auto count = 0; count < numMsg; ++count) {
        Message msg;
        ASSERT_EQ(ResultOk, consumer.receive(msg, 1000));
        recvMsgId.emplace_back(msg.getMessageId());
    }

    // Send ACK.
    AckGroupingTrackerMock tracker(false);
    tracker.start();
    std::set<MessageId> restMsgId(recvMsgId.begin(), recvMsgId.end());
    ASSERT_EQ(restMsgId.size(), numMsg);
    ASSERT_TRUE(tracker.callDoImmediateAck(connWeakPtr, consumerImpl.getConsumerId(), restMsgId));
    consumer.close();

    std::this_thread::sleep_for(std::chrono::seconds(1));
    ASSERT_EQ(ResultOk, client.subscribe(topicName, subName, consumer));
    Message msg;
    auto ret = consumer.receive(msg, 1000);
    ASSERT_EQ(ResultTimeout, ret) << "Received redundant message: " << msg.getDataAsString();
    consumer.close();
}

TEST(BasicEndToEndTest, testAckGroupingTrackerDisabledIndividualAck) {
    constexpr auto numMsg = 10;
    const std::string topicName =
        "testAckGroupingTrackerDisabledIndividualAck" + std::to_string(time(nullptr));
    const std::string subName = "sub-ack-grp-disabled-ind-ack";

    // Setup client, producer and consumer.
    Client client(lookupUrl);

    Producer producer;
    ProducerConfiguration configProducer;
    configProducer.setBatchingEnabled(false);
    ASSERT_EQ(ResultOk, client.createProducer(topicName, configProducer, producer));

    Consumer consumer;
    ASSERT_EQ(ResultOk, client.subscribe(topicName, subName, consumer));
    auto &consumerImpl = PulsarFriend::getConsumerImpl(consumer);

    // Sending and receiving messages.
    for (auto count = 0; count < numMsg; ++count) {
        Message msg = MessageBuilder().setContent(std::string("MSG-") + std::to_string(count)).build();
        ASSERT_EQ(ResultOk, producer.send(msg));
    }

    std::vector<MessageId> recvMsgId;
    for (auto count = 0; count < numMsg; ++count) {
        Message msg;
        ASSERT_EQ(ResultOk, consumer.receive(msg, 1000));
        recvMsgId.emplace_back(msg.getMessageId());
    }

    // Send ACK.
    AckGroupingTrackerDisabled tracker(consumerImpl, consumerImpl.getConsumerId());
    for (auto &msgId : recvMsgId) {
        tracker.addAcknowledge(msgId);
    }
    consumer.close();

    std::this_thread::sleep_for(std::chrono::seconds(1));
    ASSERT_EQ(ResultOk, client.subscribe(topicName, subName, consumer));
    Message msg;
    auto ret = consumer.receive(msg, 1000);
    ASSERT_EQ(ResultTimeout, ret) << "Received redundant message: " << msg.getDataAsString();
    consumer.close();
}

TEST(BasicEndToEndTest, testAckGroupingTrackerDisabledCumulativeAck) {
    constexpr auto numMsg = 10;
    const std::string topicName =
        "testAckGroupingTrackerDisabledCumulativeAck" + std::to_string(time(nullptr));
    const std::string subName = "sub-ack-grp-disabled-cum-ack";

    // Setup client, producer and consumer.
    Client client(lookupUrl);

    Producer producer;
    ProducerConfiguration configProducer;
    configProducer.setBatchingEnabled(false);
    ASSERT_EQ(ResultOk, client.createProducer(topicName, configProducer, producer));

    Consumer consumer;
    ASSERT_EQ(ResultOk, client.subscribe(topicName, subName, consumer));
    auto &consumerImpl = PulsarFriend::getConsumerImpl(consumer);

    // Sending and receiving messages.
    for (auto count = 0; count < numMsg; ++count) {
        Message msg = MessageBuilder().setContent(std::string("MSG-") + std::to_string(count)).build();
        ASSERT_EQ(ResultOk, producer.send(msg));
    }

    std::vector<MessageId> recvMsgId;
    for (auto count = 0; count < numMsg; ++count) {
        Message msg;
        ASSERT_EQ(ResultOk, consumer.receive(msg, 1000));
        recvMsgId.emplace_back(msg.getMessageId());
    }

    // Send ACK.
    AckGroupingTrackerDisabled tracker(consumerImpl, consumerImpl.getConsumerId());
    auto &latestMsgId = *std::max_element(recvMsgId.begin(), recvMsgId.end());
    tracker.addAcknowledgeCumulative(latestMsgId);
    consumer.close();

    std::this_thread::sleep_for(std::chrono::seconds(1));
    ASSERT_EQ(ResultOk, client.subscribe(topicName, subName, consumer));
    Message msg;
    auto ret = consumer.receive(msg, 1000);
    ASSERT_EQ(ResultTimeout, ret) << "Received redundant message: " << msg.getDataAsString();
    consumer.close();
}

class AckGroupingTrackerEnabledMock : public AckGroupingTrackerEnabled {
   public:
    AckGroupingTrackerEnabledMock(ClientImplPtr clientPtr, const HandlerBasePtr &handlerPtr,
                                  uint64_t consumerId, long ackGroupingTimeMs, long ackGroupingMaxSize)
        : AckGroupingTrackerEnabled(clientPtr, handlerPtr, consumerId, ackGroupingTimeMs,
                                    ackGroupingMaxSize) {}
    const std::set<MessageId> &getPendingIndividualAcks() { return this->pendingIndividualAcks_; }
    const long getAckGroupingTimeMs() { return this->ackGroupingTimeMs_; }
    const long getAckGroupingMaxSize() { return this->ackGroupingMaxSize_; }
    const MessageId getNextCumulativeAckMsgId() { return this->nextCumulativeAckMsgId_; }
    const bool requireCumulativeAck() { return this->requireCumulativeAck_; }
};  // class AckGroupingTrackerEnabledMock

TEST(BasicEndToEndTest, testAckGroupingTrackerEnabledIndividualAck) {
    constexpr auto numMsg = 10;
    constexpr auto ackGroupingTimeMs = 1000;
    constexpr auto ackGroupingMaxSize = 5000;
    const std::string topicName =
        "testAckGroupingTrackerEnabledIndividualAck" + std::to_string(time(nullptr));
    const std::string subName = "sub-ack-grp-enabled-ind-ack";

    // Setup client, producer and consumer.
    Client client(lookupUrl);
    auto clientImplPtr = PulsarFriend::getClientImplPtr(client);

    Producer producer;
    ProducerConfiguration configProducer;
    configProducer.setBatchingEnabled(false);
    ASSERT_EQ(ResultOk, client.createProducer(topicName, configProducer, producer));

    Consumer consumer;
    ASSERT_EQ(ResultOk, client.subscribe(topicName, subName, consumer));
    auto consumerImpl = PulsarFriend::getConsumerImplPtr(consumer);

    // Sending and receiving messages.
    for (auto count = 0; count < numMsg; ++count) {
        Message msg = MessageBuilder().setContent(std::string("MSG-") + std::to_string(count)).build();
        ASSERT_EQ(ResultOk, producer.send(msg));
    }

    std::vector<MessageId> recvMsgId;
    for (auto count = 0; count < numMsg; ++count) {
        Message msg;
        ASSERT_EQ(ResultOk, consumer.receive(msg, 1000));
        recvMsgId.emplace_back(msg.getMessageId());
    }

    auto tracker = std::make_shared<AckGroupingTrackerEnabledMock>(
        clientImplPtr, consumerImpl, consumerImpl->getConsumerId(), ackGroupingTimeMs, ackGroupingMaxSize);
    tracker->start();
    ASSERT_EQ(tracker->getPendingIndividualAcks().size(), 0);
    ASSERT_EQ(tracker->getAckGroupingTimeMs(), ackGroupingTimeMs);
    ASSERT_EQ(tracker->getAckGroupingMaxSize(), ackGroupingMaxSize);
    for (auto &msgId : recvMsgId) {
        ASSERT_FALSE(tracker->isDuplicate(msgId));
        tracker->addAcknowledge(msgId);
        ASSERT_TRUE(tracker->isDuplicate(msgId));
    }
    ASSERT_EQ(tracker->getPendingIndividualAcks().size(), recvMsgId.size());

    std::this_thread::sleep_for(std::chrono::seconds(2));
    ASSERT_EQ(tracker->getPendingIndividualAcks().size(), 0);
    for (auto &msgId : recvMsgId) {
        ASSERT_FALSE(tracker->isDuplicate(msgId));
    }
    consumer.close();

    ASSERT_EQ(ResultOk, client.subscribe(topicName, subName, consumer));
    Message msg;
    auto ret = consumer.receive(msg, 1000);
    ASSERT_EQ(ResultTimeout, ret) << "Received redundant message: " << msg.getDataAsString();
}

TEST(BasicEndToEndTest, testAckGroupingTrackerEnabledCumulativeAck) {
    constexpr auto numMsg = 10;
    constexpr auto ackGroupingTimeMs = 1000;
    constexpr auto ackGroupingMaxSize = 5000;
    const std::string topicName =
        "testAckGroupingTrackerEnabledCumulativeAck" + std::to_string(time(nullptr));
    const std::string subName = "sub-ack-grp-enabled-cum-ack";

    // Setup client, producer and consumer.
    Client client(lookupUrl);
    auto clientImplPtr = PulsarFriend::getClientImplPtr(client);

    Producer producer;
    ProducerConfiguration configProducer;
    configProducer.setBatchingEnabled(false);
    ASSERT_EQ(ResultOk, client.createProducer(topicName, configProducer, producer));

    Consumer consumer;
    ASSERT_EQ(ResultOk, client.subscribe(topicName, subName, consumer));
    auto consumerImpl0 = PulsarFriend::getConsumerImplPtr(consumer);

    // Sending and receiving messages.
    for (auto count = 0; count < numMsg; ++count) {
        Message msg = MessageBuilder().setContent(std::string("MSG-") + std::to_string(count)).build();
        ASSERT_EQ(ResultOk, producer.send(msg));
    }

    std::vector<MessageId> recvMsgId;
    for (auto count = 0; count < numMsg; ++count) {
        Message msg;
        ASSERT_EQ(ResultOk, consumer.receive(msg, 1000));
        recvMsgId.emplace_back(msg.getMessageId());
    }
    std::sort(recvMsgId.begin(), recvMsgId.end());

    auto tracker0 = std::make_shared<AckGroupingTrackerEnabledMock>(
        clientImplPtr, consumerImpl0, consumerImpl0->getConsumerId(), ackGroupingTimeMs, ackGroupingMaxSize);
    tracker0->start();
    ASSERT_EQ(tracker0->getNextCumulativeAckMsgId(), MessageId::earliest());
    ASSERT_FALSE(tracker0->requireCumulativeAck());

    auto targetMsgId = recvMsgId[numMsg / 2];
    for (auto idx = 0; idx <= numMsg / 2; ++idx) {
        ASSERT_FALSE(tracker0->isDuplicate(recvMsgId[idx]));
    }
    tracker0->addAcknowledgeCumulative(targetMsgId);
    for (auto idx = 0; idx <= numMsg / 2; ++idx) {
        ASSERT_TRUE(tracker0->isDuplicate(recvMsgId[idx]));
    }
    ASSERT_EQ(tracker0->getNextCumulativeAckMsgId(), targetMsgId);
    ASSERT_TRUE(tracker0->requireCumulativeAck());

    std::this_thread::sleep_for(std::chrono::seconds(2));
    ASSERT_FALSE(tracker0->requireCumulativeAck());
    for (auto idx = 0; idx <= numMsg / 2; ++idx) {
        ASSERT_TRUE(tracker0->isDuplicate(recvMsgId[idx]));
    }
    consumer.close();

    std::this_thread::sleep_for(std::chrono::seconds(1));
    ASSERT_EQ(ResultOk, client.subscribe(topicName, subName, consumer));
    auto consumerImpl1 = PulsarFriend::getConsumerImplPtr(consumer);
    std::set<MessageId> restMsgId(recvMsgId.begin() + numMsg / 2 + 1, recvMsgId.end());
    for (auto count = numMsg / 2 + 1; count < numMsg; ++count) {
        Message msg;
        ASSERT_EQ(ResultOk, consumer.receive(msg, 1000));
        ASSERT_EQ(restMsgId.count(msg.getMessageId()), 1);
    }
    Message msg;
    auto ret = consumer.receive(msg, 1000);
    ASSERT_EQ(ResultTimeout, ret) << "Received redundant message: " << msg.getDataAsString();
    auto tracker1 = std::make_shared<AckGroupingTrackerEnabledMock>(
        clientImplPtr, consumerImpl1, consumerImpl1->getConsumerId(), ackGroupingTimeMs, ackGroupingMaxSize);
    tracker1->start();
    tracker1->addAcknowledgeCumulative(recvMsgId[numMsg - 1]);
    tracker1->close();
    consumer.close();

    ASSERT_EQ(ResultOk, client.subscribe(topicName, subName, consumer));
    ret = consumer.receive(msg, 1000);
    ASSERT_EQ(ResultTimeout, ret) << "Received redundant message ID: " << msg.getMessageId();
}

class UnAckedMessageTrackerEnabledMock : public UnAckedMessageTrackerEnabled {
   public:
    UnAckedMessageTrackerEnabledMock(long timeoutMs, const ClientImplPtr client, ConsumerImplBase &consumer)
        : UnAckedMessageTrackerEnabled(timeoutMs, timeoutMs, client, consumer) {}
    const long getUnAckedMessagesTimeoutMs() { return this->timeoutMs_; }
    const long getTickDurationInMs() { return this->tickDurationInMs_; }
    bool isEmpty() { return UnAckedMessageTrackerEnabled::isEmpty(); }
    long size() { return UnAckedMessageTrackerEnabled::size(); }
};  // class UnAckedMessageTrackerEnabledMock

TEST(BasicEndToEndTest, testUnAckedMessageTrackerDefaultBehavior) {
    ConsumerConfiguration configConsumer;
    ASSERT_EQ(configConsumer.getUnAckedMessagesTimeoutMs(), 0);
    ASSERT_EQ(configConsumer.getTickDurationInMs(), 1000);

    UnAckedMessageTrackerDisabled tracker;
    Message msg;
    ASSERT_FALSE(tracker.add(msg.getMessageId()));
    ASSERT_FALSE(tracker.remove(msg.getMessageId()));
}

TEST(BasicEndToEndTest, testUnAckedMessageTrackerDisabled) {
    constexpr auto numMsg = 10;
    const std::string topicName =
        "testUnAckedMessageTrackerDisabledIndividualAck" + std::to_string(time(nullptr));
    const std::string subName = "sub-un-acked-msg-disabled-ind-ack";

    // Setup client, producer and consumer.
    Client client(lookupUrl);

    Producer producer;
    ProducerConfiguration configProducer;
    configProducer.setBatchingEnabled(false);
    ASSERT_EQ(ResultOk, client.createProducer(topicName, configProducer, producer));

    Consumer consumer;
    ASSERT_EQ(ResultOk, client.subscribe(topicName, subName, consumer));

    // Sending and receiving messages.
    for (auto count = 0; count < numMsg; ++count) {
        Message msg = MessageBuilder().setContent(std::string("MSG-") + std::to_string(count)).build();
        ASSERT_EQ(ResultOk, producer.send(msg));
    }

    UnAckedMessageTrackerDisabled tracker;
    for (auto count = 0; count < numMsg; ++count) {
        Message msg;

        ASSERT_EQ(ResultOk, consumer.receive(msg, 1000));
        ASSERT_FALSE(tracker.add(msg.getMessageId()));

        consumer.acknowledge(msg.getMessageId());
        ASSERT_FALSE(tracker.remove(msg.getMessageId()));
    }
    consumer.close();

    std::this_thread::sleep_for(std::chrono::seconds(1));
    ASSERT_EQ(ResultOk, client.subscribe(topicName, subName, consumer));
    Message msg;
    auto ret = consumer.receive(msg, 1000);
    ASSERT_EQ(ResultTimeout, ret) << "Received redundant message: " << msg.getDataAsString();
    consumer.close();
    client.close();
}

TEST(BasicEndToEndTest, testUnAckedMessageTrackerEnabledIndividualAck) {
    constexpr auto numMsg = 10;
    constexpr auto unAckedMessagesTimeoutMs = 1000;
    const std::string topicName =
        "testUnAckedMessageTrackerEnabledIndividualAck" + std::to_string(time(nullptr));
    const std::string subName = "sub-un-acked-msg-enabled-ind-ack";

    // Setup client, producer and consumer.
    Client client(lookupUrl);
    auto clientImplPtr = PulsarFriend::getClientImplPtr(client);

    Producer producer;
    ASSERT_EQ(ResultOk, client.createProducer(topicName, producer));

    Consumer consumer;
    ASSERT_EQ(ResultOk, client.subscribe(topicName, subName, consumer));
    auto &consumerImpl0 = PulsarFriend::getConsumerImpl(consumer);

    // Sending and receiving messages.
    for (auto count = 0; count < numMsg; ++count) {
        Message msg = MessageBuilder().setContent(std::string("MSG-") + std::to_string(count)).build();
        ASSERT_EQ(ResultOk, producer.send(msg));
    }

    std::vector<MessageId> recvMsgId;
    for (auto count = 0; count < numMsg; ++count) {
        Message msg;
        ASSERT_EQ(ResultOk, consumer.receive(msg, 1000));
        recvMsgId.emplace_back(msg.getMessageId());
    }

    auto tracker0 = std::make_shared<UnAckedMessageTrackerEnabledMock>(unAckedMessagesTimeoutMs,
                                                                       clientImplPtr, consumerImpl0);
    ASSERT_EQ(tracker0->getUnAckedMessagesTimeoutMs(), unAckedMessagesTimeoutMs);
    ASSERT_EQ(tracker0->getTickDurationInMs(), unAckedMessagesTimeoutMs);

    for (auto idx = 0; idx < numMsg; ++idx) {
        ASSERT_TRUE(tracker0->add(recvMsgId[idx]));
    }
    ASSERT_EQ(numMsg, tracker0->size());
    ASSERT_FALSE(tracker0->isEmpty());

    std::this_thread::sleep_for(std::chrono::seconds(4));
    ASSERT_EQ(0, tracker0->size());
    ASSERT_TRUE(tracker0->isEmpty());
    consumer.close();

    ASSERT_EQ(ResultOk, client.subscribe(topicName, subName, consumer));
    auto &consumerImpl1 = PulsarFriend::getConsumerImpl(consumer);
    std::set<MessageId> restMsgId(recvMsgId.begin(), recvMsgId.end());
    for (auto count = 0; count < numMsg; ++count) {
        Message msg;
        ASSERT_EQ(ResultOk, consumer.receive(msg, 1000));
        ASSERT_EQ(restMsgId.count(msg.getMessageId()), 1);
        ASSERT_EQ(ResultOk, consumer.acknowledge(msg));
    }

    auto tracker1 = std::make_shared<UnAckedMessageTrackerEnabledMock>(unAckedMessagesTimeoutMs,
                                                                       clientImplPtr, consumerImpl1);
    for (auto idx = 0; idx < numMsg; ++idx) {
        ASSERT_TRUE(tracker1->add(recvMsgId[idx]));
        ASSERT_TRUE(tracker1->remove(recvMsgId[idx]));
    }
    ASSERT_EQ(0, tracker1->size());
    ASSERT_TRUE(tracker1->isEmpty());
    consumer.close();

    std::this_thread::sleep_for(std::chrono::seconds(2));
    ASSERT_EQ(ResultOk, client.subscribe(topicName, subName, consumer));
    Message msg;
    auto ret = consumer.receive(msg, 1000);
    ASSERT_EQ(ResultTimeout, ret) << "Received redundant message ID: " << msg.getMessageId();
    consumer.close();
    client.close();
}

TEST(BasicEndToEndTest, testUnAckedMessageTrackerEnabledCumulativeAck) {
    constexpr auto numMsg = 10;
    constexpr auto unAckedMessagesTimeoutMs = 1000;
    const std::string topicName =
        "testUnAckedMessageTrackerEnabledCumulativeAck" + std::to_string(time(nullptr));
    const std::string subName = "sub-un-acked-msg-enabled-cum-ack";

    // Setup client, producer and consumer.
    Client client(lookupUrl);
    auto clientImplPtr = PulsarFriend::getClientImplPtr(client);

    Producer producer;
    ASSERT_EQ(ResultOk, client.createProducer(topicName, producer));

    Consumer consumer;
    ASSERT_EQ(ResultOk, client.subscribe(topicName, subName, consumer));
    auto &consumerImpl0 = PulsarFriend::getConsumerImpl(consumer);

    // Sending and receiving messages.
    for (auto count = 0; count < numMsg; ++count) {
        Message msg = MessageBuilder().setContent(std::string("MSG-") + std::to_string(count)).build();
        ASSERT_EQ(ResultOk, producer.send(msg));
    }

    std::vector<MessageId> recvMsgId;
    for (auto count = 0; count < numMsg; ++count) {
        Message msg;
        ASSERT_EQ(ResultOk, consumer.receive(msg, 1000));
        recvMsgId.emplace_back(msg.getMessageId());
    }
    auto tracker = std::make_shared<UnAckedMessageTrackerEnabledMock>(unAckedMessagesTimeoutMs, clientImplPtr,
                                                                      consumerImpl0);
    for (auto idx = 0; idx < numMsg; ++idx) {
        ASSERT_TRUE(tracker->add(recvMsgId[idx]));
    }
    ASSERT_EQ(numMsg, tracker->size());
    ASSERT_FALSE(tracker->isEmpty());

    std::sort(recvMsgId.begin(), recvMsgId.end());

    auto targetMsgId = recvMsgId[numMsg / 2];
    ASSERT_EQ(ResultOk, consumer.acknowledgeCumulative(targetMsgId));
    tracker->removeMessagesTill(targetMsgId);
    ASSERT_EQ(numMsg - (numMsg / 2 + 1), tracker->size());
    ASSERT_FALSE(tracker->isEmpty());

    std::this_thread::sleep_for(std::chrono::seconds(4));
    ASSERT_EQ(0, tracker->size());
    ASSERT_TRUE(tracker->isEmpty());
    consumer.close();

    std::this_thread::sleep_for(std::chrono::seconds(1));
    ASSERT_EQ(ResultOk, client.subscribe(topicName, subName, consumer));
    for (auto count = numMsg / 2 + 1; count < numMsg; ++count) {
        Message msg;
        ASSERT_EQ(ResultOk, consumer.receive(msg, 1000));
        ASSERT_EQ(ResultOk, consumer.acknowledge(msg.getMessageId()));
    }
    Message msg;
    auto ret = consumer.receive(msg, 1000);
    ASSERT_EQ(ResultTimeout, ret) << "Received redundant message ID: " << msg.getMessageId();
    consumer.close();
    client.close();
}
