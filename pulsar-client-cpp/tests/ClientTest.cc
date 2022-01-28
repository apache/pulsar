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

#include "HttpHelper.h"

#include <future>
#include <pulsar/Client.h>
#include "../lib/checksum/ChecksumProvider.h"

using namespace pulsar;

static std::string lookupUrl = "pulsar://localhost:6650";

TEST(ClientTest, testChecksumComputation) {
    std::string data = "test";
    std::string doubleData = "testtest";

    // (1) compute checksum of specific chunk of string
    int checksum1 = computeChecksum(0, (char *)data.c_str(), data.length());
    int checksum2 = computeChecksum(0, (char *)doubleData.c_str() + 4, 4);
    ASSERT_EQ(checksum1, checksum2);

    //(2) compute incremental checksum
    // (a) checksum on full data
    int doubleChecksum = computeChecksum(0, (char *)doubleData.c_str(), doubleData.length());
    // (b) incremental checksum on multiple partial data
    checksum1 = computeChecksum(0, (char *)data.c_str(), data.length());
    int incrementalChecksum = computeChecksum(checksum1, (char *)data.c_str(), data.length());
    ASSERT_EQ(incrementalChecksum, doubleChecksum);
}

TEST(ClientTest, testSwHwChecksum) {
    std::string data = "test";
    std::string doubleData = "testtest";

    // (1) compute checksum of specific chunk of string
    // (a) HW
    uint32_t hwChecksum1 = crc32cHw(0, (char *)data.c_str(), data.length());
    uint32_t hwChecksum2 = crc32cHw(0, (char *)doubleData.c_str() + 4, 4);
    // (b) SW
    uint32_t swChecksum1 = crc32cSw(0, (char *)data.c_str(), data.length());
    uint32_t swChecksum2 = crc32cSw(0, (char *)doubleData.c_str() + 4, 4);
    // (c) HW ARM
    uint32_t hwArmChecksum1 = crc32cHwArm(0, (char *)data.c_str(), data.length());
    uint32_t hwArmChecksum2 = crc32cHwArm(0, (char *)doubleData.c_str() + 4, 4);

    ASSERT_EQ(hwChecksum1, hwChecksum2);
    ASSERT_EQ(hwChecksum1, swChecksum1);
    ASSERT_EQ(hwChecksum2, swChecksum2);
    ASSERT_EQ(hwArmChecksum1, swChecksum1);
    ASSERT_EQ(hwArmChecksum2, swChecksum2);

    //(2) compute incremental checksum
    // (a.1) hw: checksum on full data
    uint32_t hwDoubleChecksum = crc32cHw(0, (char *)doubleData.c_str(), doubleData.length());
    // (a.2) hw: incremental checksum on multiple partial data
    hwChecksum1 = crc32cHw(0, (char *)data.c_str(), data.length());
    uint32_t hwIncrementalChecksum = crc32cHw(hwChecksum1, (char *)data.c_str(), data.length());
    // (b.1) sw: checksum on full data
    uint32_t swDoubleChecksum = crc32cSw(0, (char *)doubleData.c_str(), doubleData.length());
    ASSERT_EQ(swDoubleChecksum, hwDoubleChecksum);
    // (b.2) sw: incremental checksum on multiple partial data
    swChecksum1 = crc32cHw(0, (char *)data.c_str(), data.length());
    uint32_t swIncrementalChecksum = crc32cSw(swChecksum1, (char *)data.c_str(), data.length());
    ASSERT_EQ(hwIncrementalChecksum, hwDoubleChecksum);
    ASSERT_EQ(hwIncrementalChecksum, swIncrementalChecksum);
    // (c.1) hw arm: checksum on full data
    uint32_t hwArmDoubleChecksum = crc32cHwArm(0, (char *)doubleData.c_str(), doubleData.length());
    // (c.2) hw arm: incremental checksum on multiple partial data
    hwArmChecksum1 = crc32cHwArm(0, (char *)data.c_str(), data.length());
    uint32_t hwArmIncrementalChecksum = crc32cHw(hwArmChecksum1, (char *)data.c_str(), data.length());
    ASSERT_EQ(swDoubleChecksum, hwArmDoubleChecksum);
    ASSERT_EQ(hwArmIncrementalChecksum, hwArmDoubleChecksum);
    ASSERT_EQ(hwArmIncrementalChecksum, swIncrementalChecksum);
}

TEST(ClientTest, testServerConnectError) {
    const std::string topic = "test-server-connect-error";
    Client client("pulsar://localhost:65535");
    Producer producer;
    ASSERT_EQ(ResultConnectError, client.createProducer(topic, producer));
    Consumer consumer;
    ASSERT_EQ(ResultConnectError, client.subscribe(topic, "sub", consumer));
    Reader reader;
    ReaderConfiguration readerConf;
    ASSERT_EQ(ResultConnectError, client.createReader(topic, MessageId::earliest(), readerConf, reader));
    client.close();
}

TEST(ClientTest, testConnectTimeout) {
    // 192.0.2.0/24 is assigned for documentation, should be a deadend
    const std::string blackHoleBroker = "pulsar://192.0.2.1:1234";
    const std::string topic = "test-connect-timeout";

    Client clientLow(blackHoleBroker, ClientConfiguration().setConnectionTimeout(1000));
    Client clientDefault(blackHoleBroker);

    std::promise<Result> promiseLow;
    clientLow.createProducerAsync(
        topic, [&promiseLow](Result result, Producer producer) { promiseLow.set_value(result); });

    std::promise<Result> promiseDefault;
    clientDefault.createProducerAsync(
        topic, [&promiseDefault](Result result, Producer producer) { promiseDefault.set_value(result); });

    auto futureLow = promiseLow.get_future();
    ASSERT_EQ(futureLow.wait_for(std::chrono::milliseconds(1500)), std::future_status::ready);
    ASSERT_EQ(futureLow.get(), ResultConnectError);

    auto futureDefault = promiseDefault.get_future();
    ASSERT_EQ(futureDefault.wait_for(std::chrono::milliseconds(10)), std::future_status::timeout);

    clientLow.close();
    clientDefault.close();
}

TEST(ClientTest, testGetNumberOfReferences) {
    Client client("pulsar://localhost:6650");

    // Producer test
    uint64_t numberOfProducers = 0;
    const std::string nonPartitionedTopic =
        "testGetNumberOfReferencesNonPartitionedTopic" + std::to_string(time(nullptr));

    const std::string partitionedTopic =
        "testGetNumberOfReferencesPartitionedTopic" + std::to_string(time(nullptr));
    Producer producer;
    client.createProducer(nonPartitionedTopic, producer);
    numberOfProducers = 1;
    ASSERT_EQ(numberOfProducers, client.getNumberOfProducers());

    producer.close();
    numberOfProducers = 0;
    ASSERT_EQ(numberOfProducers, client.getNumberOfProducers());

    // PartitionedProducer
    int res = makePutRequest(
        "http://localhost:8080/admin/v2/persistent/public/default/" + partitionedTopic + "/partitions", "2");
    ASSERT_TRUE(res == 204 || res == 409) << "res: " << res;

    client.createProducer(partitionedTopic, producer);
    numberOfProducers = 2;
    ASSERT_EQ(numberOfProducers, client.getNumberOfProducers());
    producer.close();
    numberOfProducers = 0;
    ASSERT_EQ(numberOfProducers, client.getNumberOfProducers());

    // Consumer test
    uint64_t numberOfConsumers = 0;

    Consumer consumer1;
    client.subscribe(nonPartitionedTopic, "consumer-1", consumer1);
    numberOfConsumers = 1;
    ASSERT_EQ(numberOfConsumers, client.getNumberOfConsumers());

    consumer1.close();
    numberOfConsumers = 0;
    ASSERT_EQ(numberOfConsumers, client.getNumberOfConsumers());

    Consumer consumer2;
    Consumer consumer3;
    client.subscribe(partitionedTopic, "consumer-2", consumer2);
    numberOfConsumers = 2;
    ASSERT_EQ(numberOfConsumers, client.getNumberOfConsumers());
    client.subscribe(nonPartitionedTopic, "consumer-3", consumer3);
    numberOfConsumers = 3;
    ASSERT_EQ(numberOfConsumers, client.getNumberOfConsumers());
    consumer2.close();
    consumer3.close();
    numberOfConsumers = 0;
    ASSERT_EQ(numberOfConsumers, client.getNumberOfConsumers());

    client.close();
}
