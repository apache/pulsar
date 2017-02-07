/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <gtest/gtest.h>
#include <pulsar/Client.h>
#include <boost/lexical_cast.hpp>
#include <lib/LogUtils.h>
#include <pulsar/MessageBuilder.h>
#include "DestinationName.h"
#include <lib/Commands.h>
#include <sstream>
#include "boost/date_time/posix_time/posix_time.hpp"
#include "CustomRoutingPolicy.h"
#include <boost/thread.hpp>

#include "HttpHelper.h"

#include "lib/Future.h"
#include "lib/Utils.h"
DECLARE_LOG_OBJECT()

using namespace pulsar;

static int globalTestBatchMessagesCounter = 0;
static int globalCount = 0;
static int globalResendMessageCount = 0;
static std::string lookupUrl = "pulsar://localhost:8885";
static std::string adminUrl = "http://localhost:8765/";

static void messageListenerFunction(Consumer consumer, const Message& msg) {
    globalCount++;
    consumer.acknowledge(msg);
}

static void sendCallBack(Result r, const Message& msg) {
    ASSERT_EQ(r, ResultOk);
    std::string prefix = "msg-batch-";
    std::string messageContent = prefix + boost::lexical_cast<std::string>(globalTestBatchMessagesCounter++);
    ASSERT_EQ(messageContent, msg.getDataAsString());
    LOG_DEBUG("Received publish acknowledgement for " << msg.getDataAsString());
}

TEST(BasicEndToEndTest, testBatchMessages)
{
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
    for (int i = 0; i<numOfMessages; i++) {
        std::string messageContent = prefix + boost::lexical_cast<std::string>(i);
        Message msg = MessageBuilder().setContent(messageContent).setProperty("msgIndex", boost::lexical_cast<std::string>(i)).build();
        producer.sendAsync(msg, &sendCallBack);
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
    ASSERT_EQ(globalTestBatchMessagesCounter, numOfMessages);

    // Number of messages consumed
    ASSERT_EQ(i, numOfMessages);

}

void resendMessage(Result r, const Message& msg, Producer &producer) {
    if (r != ResultOk) {
        int attemptNumber = boost::lexical_cast<int>(msg.getProperty("attempt#"));
        if (attemptNumber++ < 3) {
            globalResendMessageCount++;
            producer.sendAsync(MessageBuilder().setProperty("attempt#", boost::lexical_cast<std::string>(attemptNumber)).build(),
                                   boost::bind(resendMessage, _1, _2, producer));
        }
    }
}

    TEST(BasicEndToEndTest, testProduceConsume)
{
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

    TEST(BasicEndToEndTest, testNonExistingTopic)
{
    Client client(lookupUrl);
    Producer producer;
    Result result = client.createProducer("persistent://prop/unit/ns1", producer);
    ASSERT_EQ(ResultInvalidTopicName, result);

    Consumer consumer;
    result = client.subscribe("persistent://prop/unit/ns1", "my-sub-name", consumer);
    ASSERT_EQ(ResultInvalidTopicName, result);
}

    TEST(BasicEndToEndTest, testNonPersistentTopic)
{
    Client client(lookupUrl);
    Producer producer;
    Result result = client.createProducer("non-persistent://prop/unit/ns1/destination", producer);
    ASSERT_EQ(ResultInvalidTopicName, result);

    Consumer consumer;
    result = client.subscribe("non-persistent://prop/unit/ns1/destination", "my-sub-name",
                              consumer);
    ASSERT_EQ(ResultInvalidTopicName, result);
}

    TEST(BasicEndToEndTest, testSingleClientMultipleSubscriptions)
{
    Client client(lookupUrl);

    Producer producer;
    Result result = client.createProducer("persistent://prop/unit/ns1/my-topic-1", producer);
    ASSERT_EQ(ResultOk, result);

    Consumer consumer1;
    result = client.subscribe("persistent://prop/unit/ns1/my-topic-1", "my-sub-name", consumer1);
    ASSERT_EQ(ResultOk, result);

    Consumer consumer2;
    result = client.subscribe("persistent://prop/unit/ns1/my-topic-1", "my-sub-name", consumer2);
    ASSERT_EQ(ResultConsumerBusy, result);
    //at this point connection gets destroyed because this consumer creation fails
}

    TEST(BasicEndToEndTest, testMultipleClientsMultipleSubscriptions)
{
    Client client1(lookupUrl);
    Client client2(lookupUrl);

    Producer producer1;
    Result result = client1.createProducer("persistent://prop/unit/ns1/my-topic-2", producer1);
    ASSERT_EQ(ResultOk, result);

    Consumer consumer1;
    result = client1.subscribe("persistent://prop/unit/ns1/my-topic-2", "my-sub-name", consumer1);
    ASSERT_EQ(ResultOk, result);

    Consumer consumer2;
    result = client2.subscribe("persistent://prop/unit/ns1/my-topic-2", "my-sub-name", consumer2);
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

    TEST(BasicEndToEndTest, testProduceAndConsumeAfterClientClose)
{
    Client client(lookupUrl);

    Producer producer;
    Result result = client.createProducer("persistent://prop/unit/ns1/my-topic-3", producer);
    ASSERT_EQ(ResultOk, result);

    Consumer consumer;
    result = client.subscribe("persistent://prop/unit/ns1/my-topic-3", "my-sub-name", consumer);
    ASSERT_EQ(ResultOk, result);

    // Send 10 messages synchronosly
    std::string msgContent = "msg-content";
    LOG_INFO("Publishing 10 messages synchronously");
    int numMsg = 0;
    for (; numMsg < 10; numMsg++) {
        Message msg = MessageBuilder().setContent(msgContent).setProperty(
                                                                          "msgIndex", boost::lexical_cast<std::string>(numMsg)).build();
        ASSERT_EQ(ResultOk, producer.send(msg));
    }

    LOG_INFO("Trying to receive 10 messages");
    Message msgReceived;
    for (int i = 0; i < 10; i++) {
        consumer.receive(msgReceived, 1000);
        LOG_INFO("Received message :" << msgReceived.getMessageId());
        ASSERT_EQ(msgContent, msgReceived.getDataAsString());
        ASSERT_EQ(boost::lexical_cast<std::string>(i), msgReceived.getProperty("msgIndex"));
        ASSERT_EQ(ResultOk, consumer.acknowledgeCumulative(msgReceived));
    }

    LOG_INFO("Closing client");
    ASSERT_EQ(ResultOk, client.close());

    LOG_INFO("Trying to publish a message after closing the client");
    Message msg = MessageBuilder().setContent(msgContent).setProperty(
                                                                      "msgIndex", boost::lexical_cast<std::string>(numMsg)).build();

    ASSERT_EQ(ResultAlreadyClosed, producer.send(msg));

    LOG_INFO("Trying to consume a message after closing the client");
    ASSERT_EQ(ResultAlreadyClosed, consumer.receive(msgReceived));
}

    TEST(BasicEndToEndTest, testIamSoFancyCharactersInTopicName)
{
    Client client(lookupUrl);
    Producer producer;
    Result result = client.createProducer("persistent://prop/unit/ns1/topic@%*)(&!%$#@#$><?", producer);
    ASSERT_EQ(ResultOk, result);

    Consumer consumer;
    result = client.subscribe("persistent://prop/unit/ns1/topic@%*)(&!%$#@#$><?", "my-sub-name", consumer);
    ASSERT_EQ(ResultOk, result);
}

    TEST(BasicEndToEndTest, testSubscribeCloseUnsubscribeSherpaScenario)
{
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
    ASSERT_EQ(ResultOk,result);

    Consumer consumer1;
    result = client.subscribe(topicName, subName, consumer1);
    result = consumer1.unsubscribe();
    ASSERT_EQ(ResultOk, result);
}

    TEST(BasicEndToEndTest, testInvalidUrlPassed)
{
    Client client("localhost:4080");
    std::string topicName = "persistent://prop/unit/ns1/test";
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

TEST(BasicEndToEndTest, testPartitionedProducerConsumer)
{
    Client client(lookupUrl);
    std::string topicName = "persistent://prop/unit/ns/partition-test";

    // call admin api to make it partitioned
    std::string url = lookupUrl + "/admin/persistent/prop/unit/ns/partition-test/partitions";
    int res = makePutRequest(lookupUrl, "3");

    if (res != 204 && res != 409) {
        LOG_DEBUG("Unable to create partitioned topic.");
        return;
    }
    Producer producer;
    Result result = client.createProducer(topicName, producer);
    ASSERT_EQ(ResultOk, result);
    for (int i = 0; i < 10; i++ ) {
        boost::posix_time::ptime t(boost::posix_time::microsec_clock::universal_time());
        long nanoSeconds = t.time_of_day().total_nanoseconds();
        std::stringstream ss;
        ss << nanoSeconds;
        Message msg = MessageBuilder().setContent(ss.str()).setPartitionKey(ss.str()).build();
        ASSERT_EQ(ResultOk, producer.send(msg));
        LOG_INFO("Message Timestamp is " << msg.getPublishTimestamp());
        LOG_INFO("Message is " << msg);
    }
    Consumer consumer;
    result = client.subscribe(topicName, "subscription-A", consumer);
    ASSERT_EQ(ResultOk, result);
    ASSERT_EQ(consumer.getSubscriptionName(), "subscription-A");
    for (int i = 0; i < 10; i++) {
        Message m;
        consumer.receive(m, 10000);
        consumer.acknowledge(m);
    }
    client.shutdown();
}

    TEST(BasicEndToEndTest, testMessageTooBig)
{
    ClientConfiguration config;
    Client client(lookupUrl);
    std::string topicName = "persistent://prop/unit/ns1/my-topic";
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

    TEST(BasicEndToEndTest, testCompressionLZ4)
{
    ClientConfiguration config;
    Client client(lookupUrl);
    std::string topicName = "persistent://prop/unit/namespace1/my-topic-lz4";
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

    TEST(BasicEndToEndTest, testCompressionZLib)
{
    ClientConfiguration config;
    Client client(lookupUrl);
    std::string topicName = "persistent://prop/unit/ns1/my-topic-zlib";
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

    TEST(BasicEndToEndTest, testConfigurationFile)
{
    ClientConfiguration config1;
    config1.setOperationTimeoutSeconds(100);
    config1.setIOThreads(10);
    config1.setMessageListenerThreads(1);
    config1.setLogConfFilePath("/tmp/");

    ClientConfiguration config2 = config1;
    std::string str = "";
    ASSERT_EQ(ResultOk, config1.getAuthentication().getAuthData(str));
    ASSERT_EQ(100, config2.getOperationTimeoutSeconds());
    ASSERT_EQ(10, config2.getIOThreads());
    ASSERT_EQ(1, config2.getMessageListenerThreads());
    ASSERT_EQ(config2.getLogConfFilePath().compare("/tmp/"), 0);

}

TEST(BasicEndToEndTest, testSinglePartitionRoutingPolicy)
{
    Client client(lookupUrl);
    std::string topicName = "persistent://prop/unit/ns/partition-testSinglePartitionRoutingPolicy";

    // call admin api to make it partitioned
    std::string url = lookupUrl + "/admin/persistent/prop/unit/ns/partition-testSinglePartitionRoutingPolicy/partitions";
    int res = makePutRequest(lookupUrl, "5");

    if (res != 204 && res != 409) {
      LOG_DEBUG("Unable to create partitioned topic.");
      return;
    }

    Producer producer;
    ProducerConfiguration producerConfiguration;
    producerConfiguration.setPartitionsRoutingMode(ProducerConfiguration::UseSinglePartition);

    Result result = client.createProducer(topicName, producerConfiguration, producer);
    ASSERT_EQ(ResultOk, result);
    for (int i = 0; i < 10; i++ ) {
        boost::posix_time::ptime t(boost::posix_time::microsec_clock::universal_time());
        long nanoSeconds = t.time_of_day().total_nanoseconds();
        std::stringstream ss;
        ss << nanoSeconds;
        Message msg = MessageBuilder().setContent(ss.str()).build();
        ASSERT_EQ(ResultOk, producer.send(msg));
    }
    Consumer consumer;
    result = client.subscribe(topicName, "subscription-A", consumer);
    ASSERT_EQ(ResultOk, result);
    for (int i = 0; i < 10; i++) {
        Message m;
        consumer.receive(m);
        consumer.acknowledgeCumulative(m);
    }
    consumer.close();
    producer.closeAsync(0);
    client.close();
}

    TEST(BasicEndToEndTest, testNamespaceName)
{
    boost::shared_ptr<NamespaceName> nameSpaceName = NamespaceName::get("property", "bf1", "nameSpace");
    ASSERT_STREQ(nameSpaceName->getCluster().c_str(), "bf1");
    ASSERT_STREQ(nameSpaceName->getLocalName().c_str(), "nameSpace");
    ASSERT_STREQ(nameSpaceName->getProperty().c_str(), "property");
}

    TEST(BasicEndToEndTest, testConsumerClose)
{
    ClientConfiguration config;
    Client client(lookupUrl);
    std::string topicName = "persistent://prop/unit/ns1/testConsumerClose";
    std::string subName = "my-sub-name";
    Consumer consumer;
    ASSERT_EQ(ResultOk, client.subscribe(topicName, subName, consumer));
    ASSERT_EQ(consumer.close(), ResultOk);
    ASSERT_EQ(consumer.close(), ResultAlreadyClosed);
}

    TEST(BasicEndToEndTest, testDuplicateConsumerCreationOnPartitionedTopic)
{
    Client client(lookupUrl);
    std::string topicName = "persistent://prop/unit/ns/partition-testDuplicateConsumerCreationOnPartitionedTopic";

    // call admin api to make it partitioned
    std::string url = lookupUrl + "/admin/persistent/prop/unit/ns/testDuplicateConsumerCreationOnPartitionedTopic/partitions";
    int res = makePutRequest(lookupUrl, "5");

    if (res != 204 && res != 409) {
        LOG_DEBUG("Unable to create partitioned topic.");
        return;
    }

    usleep(2 * 1000 * 1000);

    Producer producer;
    ProducerConfiguration producerConfiguration;
    producerConfiguration.setPartitionsRoutingMode(ProducerConfiguration::CustomPartition);
    producerConfiguration.setMessageRouter(boost::make_shared<CustomRoutingPolicy>());

    Result result = client.createProducer(topicName, producer);
    ASSERT_EQ(ResultOk, result);
    for (int i = 0; i < 10; i++ ) {
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
    Result subscribeResult = client.subscribe(topicName, consumerId,
        consConfig, consumer);
    ASSERT_EQ(ResultOk, subscribeResult);

    LOG_INFO("Creating Another Subscriber");
    Consumer consumer2;
    ASSERT_EQ(consumer2.getSubscriptionName(), "");
    subscribeResult = client.subscribe(topicName, consumerId,
        consConfig, consumer2);
    ASSERT_EQ(ResultConsumerBusy, subscribeResult);
    consumer.close();
    producer.close();
}

TEST(BasicEndToEndTest, testRoundRobinRoutingPolicy)
{
    Client client(lookupUrl);
    std::string topicName = "persistent://prop/unit/ns/partition-testRoundRobinRoutingPolicy";
    // call admin api to make it partitioned
    std::string url = lookupUrl + "/admin/persistent/prop/unit/ns/partition-testRoundRobinRoutingPolicy/partitions";
    int res = makePutRequest(lookupUrl, "5");

    if (res != 204 && res != 409) {
        LOG_DEBUG("Unable to create partitioned topic.");
        return;
    }

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
        subscribeResult = client.subscribe(partitionedTopicName.str(), partitionedConsumerId.str(), consConfig, consumer[i]);

        ASSERT_EQ(ResultOk, subscribeResult);
        ASSERT_EQ(consumer[i].getTopic(), partitionedTopicName.str());
    }

    for (int i = 0; i < 10; i++ ) {
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

TEST(BasicEndToEndTest, testMessageListener)
{
    Client client(lookupUrl);
    std::string topicName = "persistent://prop/unit/ns/partition-testMessageListener";
    // call admin api to make it partitioned
    std::string url = lookupUrl + "/admin/persistent/prop/unit/ns/partition-testMessageListener/partitions";
    int res = makePutRequest(lookupUrl, "5");

    if (res != 204 && res != 409) {
        LOG_DEBUG("Unable to create partitioned topic.");
        return;
    }

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
    for (int i = 0; i < 10; i++ ) {
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
    producer.closeAsync(0);
    client.close();
}

TEST(BasicEndToEndTest, testMessageListenerPause)
{
    Client client(lookupUrl);
    std::string topicName = "persistent://property/cluster/namespace/partition-testMessageListener-pauses";

    // call admin api to make it partitioned
    std::string url = lookupUrl + "/admin/persistent/prop/unit/ns/partition-testMessageListener-pauses/partitions";
    int res = makePutRequest(lookupUrl, "5");

    if (res != 204 && res != 409) {
        LOG_DEBUG("Unable to create partitioned topic.");
        return;
    }
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
    for (int i = 0; i < 10000; i++ ) {
        if(i && i%1000 == 0) {
            usleep(5 * 1000 * 1000);
            ASSERT_EQ(globalCount, temp);
            consumer.resumeMessageListener();
            usleep(5 * 1000 * 1000);
            ASSERT_EQ(globalCount, i);
            temp = globalCount;
            consumer.pauseMessageListener();
        }
        Message msg = MessageBuilder().build();
        ASSERT_EQ(ResultOk, producer.send(msg));
    }

    ASSERT_EQ(globalCount, temp);
    consumer.resumeMessageListener();
    // Sleeping for 5 seconds
    usleep(5 * 1000 * 1000);
    ASSERT_EQ(globalCount, 10000);
    consumer.close();
    producer.closeAsync(0);
    client.close();
}

    TEST(BasicEndToEndTest, testResendViaListener)
{
    Client client(lookupUrl);
    std::string topicName = "persistent://my-property/my-cluster/my-namespace/testResendViaListener";

    Producer producer;

    Promise<Result, Producer> producerPromise;
    ProducerConfiguration producerConfiguration;

    // Setting timeout of 1 ms
    producerConfiguration.setSendTimeout(1);
    client.createProducerAsync(topicName, producerConfiguration, WaitForCallbackValue<Producer>(producerPromise));
    Future<Result, Producer> producerFuture = producerPromise.getFuture();
    Result result = producerFuture.get(producer);
    ASSERT_EQ(ResultOk, result);

    // Send asynchronously
    producer.sendAsync(MessageBuilder().setProperty("attempt#", boost::lexical_cast<std::string>(0)).build(), boost::bind(resendMessage, _1, _2, producer));

    // 3 seconds
    usleep(3 * 1000 * 1000);

    ASSERT_EQ(globalResendMessageCount, 3);
}
