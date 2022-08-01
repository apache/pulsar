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
#include <pulsar/ClientConfiguration.h>

using namespace pulsar;

TEST(ClientConfigurationTest, testDefaultConfig) {
    ClientConfiguration conf;
    ASSERT_EQ(conf.getMemoryLimit(), 64 * 1024 * 1024);
    ASSERT_EQ(conf.getIOThreads(), 1);
    ASSERT_EQ(conf.getOperationTimeoutSeconds(), 30);
    ASSERT_EQ(conf.getMessageListenerThreads(), 1);
    ASSERT_EQ(conf.getConcurrentLookupRequest(), 50000);
    ASSERT_EQ(conf.getLogConfFilePath(), "");
    ASSERT_EQ(conf.isUseTls(), false);
    ASSERT_EQ(conf.getTlsTrustCertsFilePath(), "");
    ASSERT_EQ(conf.getStatsIntervalInSeconds(), 600);
    ASSERT_EQ(conf.isValidateHostName(), false);
    ASSERT_EQ(conf.getPartitionsUpdateInterval(), 60);
    ASSERT_EQ(conf.getConnectionTimeout(), 10000);
}