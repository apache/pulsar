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
#include <pulsar/BatchReceivePolicy.h>

using namespace pulsar;

TEST(BatchReceivePolicyTest, testBatchReceivePolicy) {
    ASSERT_THROW(BatchReceivePolicy(-1, -1, -1), std::invalid_argument);

    {
        BatchReceivePolicy batchReceivePolicy;
        ASSERT_EQ(batchReceivePolicy.getMaxNumMessages(), -1);
        ASSERT_EQ(batchReceivePolicy.getMaxNumBytes(), 10 * 1024 * 1024);
        ASSERT_EQ(batchReceivePolicy.getTimeoutMs(), 100);
    }

    {
        BatchReceivePolicy batchReceivePolicy(-1, -1, 123);
        ASSERT_EQ(batchReceivePolicy.getMaxNumMessages(), -1);
        ASSERT_EQ(batchReceivePolicy.getMaxNumBytes(), 10 * 1024 * 1024);
        ASSERT_EQ(batchReceivePolicy.getTimeoutMs(), 123);
    }
}
