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
#include "RetryableLookupService.h"
#include "PulsarFriend.h"

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
        ASSERT_EQ(std::count(results.cbegin(), results.cend(), ResultRetryable), numRequests / 2);
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
TEST(LookupServiceTest, testRetry) {
    auto executorProvider = std::make_shared<ExecutorServiceProvider>(1);
    ConnectionPool pool({}, executorProvider, AuthFactory::Disabled(), true);
    ServiceNameResolver serviceNameResolver("pulsar://localhost:9999,localhost");

    auto lookupService = RetryableLookupService::create(
        std::make_shared<BinaryProtoLookupService>(serviceNameResolver, pool, ""), 30 /* seconds */,
        executorProvider);

    PulsarFriend::setServiceUrlIndex(serviceNameResolver, 0);
    auto topicNamePtr = TopicName::get("lookup-service-test-retry");
    auto future1 = lookupService->getBroker(*topicNamePtr);
    LookupService::LookupResult lookupResult;
    ASSERT_EQ(ResultOk, future1.get(lookupResult));
    LOG_INFO("getBroker returns logicalAddress: " << lookupResult.logicalAddress
                                                  << ", physicalAddress: " << lookupResult.physicalAddress);

    PulsarFriend::setServiceUrlIndex(serviceNameResolver, 0);
    auto future2 = lookupService->getPartitionMetadataAsync(topicNamePtr);
    LookupDataResultPtr lookupDataResultPtr;
    ASSERT_EQ(ResultOk, future2.get(lookupDataResultPtr));
    LOG_INFO("getPartitionMetadataAsync returns " << lookupDataResultPtr->getPartitions() << " partitions");

    PulsarFriend::setServiceUrlIndex(serviceNameResolver, 0);
    auto future3 = lookupService->getTopicsOfNamespaceAsync(topicNamePtr->getNamespaceName());
    NamespaceTopicsPtr namespaceTopicsPtr;
    ASSERT_EQ(ResultOk, future3.get(namespaceTopicsPtr));
    LOG_INFO("getTopicPartitionName Async returns " << namespaceTopicsPtr->size() << " topics");

    std::atomic_int retryCount{0};
    constexpr int totalRetryCount = 3;
    auto future4 = lookupService->executeAsync<int>("key", [&retryCount]() -> Future<Result, int> {
        Promise<Result, int> promise;
        if (++retryCount < totalRetryCount) {
            LOG_INFO("Retry count: " << retryCount);
            promise.setFailed(ResultRetryable);
        } else {
            LOG_INFO("Retry done with " << retryCount << " times");
            promise.setValue(100);
        }
        return promise.getFuture();
    });
    int customResult = 0;
    ASSERT_EQ(ResultOk, future4.get(customResult));
    ASSERT_EQ(customResult, 100);
    ASSERT_EQ(retryCount.load(), totalRetryCount);

    ASSERT_EQ(PulsarFriend::getNumberOfPendingTasks(*lookupService), 0);
}

TEST(LookupServiceTest, testTimeout) {
    auto executorProvider = std::make_shared<ExecutorServiceProvider>(1);
    ConnectionPool pool({}, executorProvider, AuthFactory::Disabled(), true);
    ServiceNameResolver serviceNameResolver("pulsar://localhost:9990,localhost:9902,localhost:9904");

    constexpr int timeoutInSeconds = 2;
    auto lookupService = RetryableLookupService::create(
        std::make_shared<BinaryProtoLookupService>(serviceNameResolver, pool, ""), timeoutInSeconds,
        executorProvider);
    auto topicNamePtr = TopicName::get("lookup-service-test-retry");

    decltype(std::chrono::high_resolution_clock::now()) startTime;
    auto beforeMethod = [&startTime] { startTime = std::chrono::high_resolution_clock::now(); };
    auto afterMethod = [&startTime](const std::string& name) {
        auto timeInterval = std::chrono::duration_cast<std::chrono::milliseconds>(
                                std::chrono::high_resolution_clock::now() - startTime)
                                .count();
        LOG_INFO(name << " took " << timeInterval << " seconds");
        ASSERT_TRUE(timeInterval >= timeoutInSeconds * 1000L);
    };

    beforeMethod();
    auto future1 = lookupService->getBroker(*topicNamePtr);
    LookupService::LookupResult lookupResult;
    ASSERT_EQ(ResultTimeout, future1.get(lookupResult));
    afterMethod("getBroker");

    beforeMethod();
    auto future2 = lookupService->getPartitionMetadataAsync(topicNamePtr);
    LookupDataResultPtr lookupDataResultPtr;
    ASSERT_EQ(ResultTimeout, future2.get(lookupDataResultPtr));
    afterMethod("getPartitionMetadataAsync");

    beforeMethod();
    auto future3 = lookupService->getTopicsOfNamespaceAsync(topicNamePtr->getNamespaceName());
    NamespaceTopicsPtr namespaceTopicsPtr;
    ASSERT_EQ(ResultTimeout, future3.get(namespaceTopicsPtr));
    afterMethod("getTopicsOfNamespaceAsync");

    ASSERT_EQ(PulsarFriend::getNumberOfPendingTasks(*lookupService), 0);
}
