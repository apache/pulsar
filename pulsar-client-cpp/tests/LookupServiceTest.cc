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
#include <BinaryProtoLookupService.h>
#include <HTTPLookupService.h>
#include <pulsar/Client.h>

#include <gtest/gtest.h>
#include <Future.h>
#include <Utils.h>
#include "ConnectionPool.h"
#include "HttpHelper.h"
#include <pulsar/Authentication.h>
#include <boost/exception/all.hpp>
#include "LogUtils.h"

#include <algorithm>

using namespace pulsar;

DECLARE_LOG_OBJECT()

TEST(LookupServiceTest, basicLookup) {
    ExecutorServiceProviderPtr service = std::make_shared<ExecutorServiceProvider>(1);
    AuthenticationPtr authData = AuthFactory::Disabled();
    std::string url = "pulsar://localhost:6650";
    ClientConfiguration conf;
    ExecutorServiceProviderPtr ioExecutorProvider_(std::make_shared<ExecutorServiceProvider>(1));
    ConnectionPool pool_(conf, ioExecutorProvider_, authData, true);
    ServiceNameResolver serviceNameResolver(url);
    BinaryProtoLookupService lookupService(serviceNameResolver, pool_, "");

    TopicNamePtr topicName = TopicName::get("topic");

    Future<Result, LookupDataResultPtr> partitionFuture = lookupService.getPartitionMetadataAsync(topicName);
    LookupDataResultPtr lookupData;
    Result result = partitionFuture.get(lookupData);
    ASSERT_TRUE(lookupData != NULL);
    ASSERT_EQ(0, lookupData->getPartitions());

    const auto topicNamePtr = TopicName::get("topic");
    auto future = lookupService.getBroker(*topicNamePtr);
    LookupService::LookupResult lookupResult;
    result = future.get(lookupResult);

    ASSERT_EQ(ResultOk, result);
    ASSERT_EQ(url, lookupResult.logicalAddress);
    ASSERT_EQ(url, lookupResult.physicalAddress);
}

TEST(LookupServiceTest, basicGetNamespaceTopics) {
    std::string url = "pulsar://localhost:6650";
    std::string adminUrl = "http://localhost:8080/";
    Result result;
    // 1. create some topics under same namespace
    Client client(url);

    std::string topicName1 = "persistent://public/default/basicGetNamespaceTopics1";
    std::string topicName2 = "persistent://public/default/basicGetNamespaceTopics2";
    std::string topicName3 = "persistent://public/default/basicGetNamespaceTopics3";
    // This is not in same namespace.
    std::string topicName4 = "persistent://public/default-2/basicGetNamespaceTopics4";

    // call admin api to make topics partitioned
    std::string url1 = adminUrl + "admin/v2/persistent/public/default/basicGetNamespaceTopics1/partitions";
    std::string url2 = adminUrl + "admin/v2/persistent/public/default/basicGetNamespaceTopics2/partitions";
    std::string url3 = adminUrl + "admin/v2/persistent/public/default/basicGetNamespaceTopics3/partitions";

    int res = makePutRequest(url1, "2");
    ASSERT_FALSE(res != 204 && res != 409);
    res = makePutRequest(url2, "3");
    ASSERT_FALSE(res != 204 && res != 409);
    res = makePutRequest(url3, "4");
    ASSERT_FALSE(res != 204 && res != 409);

    Producer producer1;
    result = client.createProducer(topicName1, producer1);
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

    // 2.  call getTopicsOfNamespaceAsync
    ExecutorServiceProviderPtr service = std::make_shared<ExecutorServiceProvider>(1);
    AuthenticationPtr authData = AuthFactory::Disabled();
    ClientConfiguration conf;
    ExecutorServiceProviderPtr ioExecutorProvider_(std::make_shared<ExecutorServiceProvider>(1));
    ConnectionPool pool_(conf, ioExecutorProvider_, authData, true);
    ServiceNameResolver serviceNameResolver(url);
    BinaryProtoLookupService lookupService(serviceNameResolver, pool_, "");

    TopicNamePtr topicName = TopicName::get(topicName1);
    NamespaceNamePtr nsName = topicName->getNamespaceName();

    Future<Result, NamespaceTopicsPtr> getTopicsFuture = lookupService.getTopicsOfNamespaceAsync(nsName);
    NamespaceTopicsPtr topicsData;
    result = getTopicsFuture.get(topicsData);
    ASSERT_EQ(ResultOk, result);
    ASSERT_TRUE(topicsData != NULL);

    // 3. verify result contains first 3 topic
    ASSERT_TRUE(std::find(topicsData->begin(), topicsData->end(), topicName1) != topicsData->end());
    ASSERT_TRUE(std::find(topicsData->begin(), topicsData->end(), topicName2) != topicsData->end());
    ASSERT_TRUE(std::find(topicsData->begin(), topicsData->end(), topicName3) != topicsData->end());
    ASSERT_FALSE(std::find(topicsData->begin(), topicsData->end(), topicName4) != topicsData->end());

    client.shutdown();
}

static void testMultiAddresses(LookupService& lookupService) {
    std::vector<Result> results;
    constexpr int numRequests = 6;

    auto verifySuccessCount = [&results] {
        // Only half of them succeeded
        ASSERT_EQ(std::count(results.cbegin(), results.cend(), ResultOk), numRequests / 2);
        ASSERT_EQ(std::count(results.cbegin(), results.cend(), ResultConnectError), numRequests / 2);
    };

    for (int i = 0; i < numRequests; i++) {
        const auto topicNamePtr = TopicName::get("topic");
        LookupService::LookupResult lookupResult;
        const auto result = lookupService.getBroker(*topicNamePtr).get(lookupResult);
        LOG_INFO("getBroker [" << i << "] " << result << ", " << lookupResult);
        results.emplace_back(result);
    }
    verifySuccessCount();

    results.clear();
    for (int i = 0; i < numRequests; i++) {
        LookupDataResultPtr data;
        const auto result = lookupService.getPartitionMetadataAsync(TopicName::get("topic")).get(data);
        LOG_INFO("getPartitionMetadataAsync [" << i << "] " << result);
        results.emplace_back(result);
    }
    verifySuccessCount();

    results.clear();
    for (int i = 0; i < numRequests; i++) {
        NamespaceTopicsPtr data;
        const auto result =
            lookupService.getTopicsOfNamespaceAsync(TopicName::get("topic")->getNamespaceName()).get(data);
        LOG_INFO("getTopicsOfNamespaceAsync [" << i << "] " << result);
        results.emplace_back(result);
    }
    verifySuccessCount();
}

TEST(LookupServiceTest, testMultiAddresses) {
    ConnectionPool pool({}, std::make_shared<ExecutorServiceProvider>(1), AuthFactory::Disabled(), true);
    ServiceNameResolver serviceNameResolver("pulsar://localhost,localhost:9999");
    BinaryProtoLookupService binaryLookupService(serviceNameResolver, pool, "");
    testMultiAddresses(binaryLookupService);

    // HTTPLookupService calls shared_from_this() internally, we must create a shared pointer to test
    ServiceNameResolver serviceNameResolverForHttp("http://localhost,localhost:9999");
    auto httpLookupServicePtr = std::make_shared<HTTPLookupService>(
        std::ref(serviceNameResolverForHttp), ClientConfiguration{}, AuthFactory::Disabled());
    testMultiAddresses(*httpLookupServicePtr);
}
