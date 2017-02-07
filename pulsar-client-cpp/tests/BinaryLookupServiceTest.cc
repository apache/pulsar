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

#include <BinaryProtoLookupService.h>
#include <pulsar/Client.h>

#include <gtest/gtest.h>
#include <boost/shared_ptr.hpp>
#include <boost/make_shared.hpp>
#include <Future.h>
#include <Utils.h>
#include "ConnectionPool.h"
#include <pulsar/Auth.h>
#include <boost/exception/all.hpp>

using namespace pulsar;

TEST(BinaryLookupServiceTest, basicLookup) {
    ExecutorServiceProviderPtr service = boost::make_shared<ExecutorServiceProvider>(1);
    AuthenticationPtr authData = Auth::Disabled();
    std::string url = "pulsar://localhost:8885";
    ClientConfiguration conf;
    ExecutorServiceProviderPtr ioExecutorProvider_(boost::make_shared<ExecutorServiceProvider>(1));
    ConnectionPool pool_(conf, ioExecutorProvider_, authData, true);
    BinaryProtoLookupService lookupService(pool_, url);

    std::string topic = "persistent://prop/unit/ns1/destination";
    DestinationNamePtr dn = DestinationName::get(topic);

    Future<Result, LookupDataResultPtr> partitionFuture = lookupService.getPartitionMetadataAsync(dn);
    LookupDataResultPtr lookupData;
    Result result = partitionFuture.get(lookupData);
    ASSERT_TRUE(lookupData != NULL);
    ASSERT_EQ(0, lookupData->getPartitions());

    Future<Result, LookupDataResultPtr> future = lookupService.lookupAsync("persistent://prop/unit/ns1/destination");
    result = future.get(lookupData);

    ASSERT_EQ(ResultOk, result);
    ASSERT_TRUE(lookupData != NULL);
    ASSERT_EQ(url, lookupData->getBrokerUrl());
}
