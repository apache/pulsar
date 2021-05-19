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
package org.apache.pulsar.tests.integration.messaging;

import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.Test;

@Slf4j
public class PersistentTopicMessagingTest extends TopicMessagingBase {

    @Test(dataProvider = "ServiceUrls")
    public void testNonPartitionedTopicMessagingWithExclusive(Supplier<String> serviceUrl) throws Exception {
        nonPartitionedTopicSendAndReceiveWithExclusive(serviceUrl.get(), true);
    }

    @Test(dataProvider = "ServiceUrls")
    public void testPartitionedTopicMessagingWithExclusive(Supplier<String> serviceUrl) throws Exception {
        partitionedTopicSendAndReceiveWithExclusive(serviceUrl.get(), true);
    }

    @Test(dataProvider = "ServiceUrls")
    public void testNonPartitionedTopicMessagingWithFailover(Supplier<String> serviceUrl) throws Exception {
        nonPartitionedTopicSendAndReceiveWithFailover(serviceUrl.get(), true);
    }

    @Test(dataProvider = "ServiceUrls")
    public void testPartitionedTopicMessagingWithFailover(Supplier<String> serviceUrl) throws Exception {
        partitionedTopicSendAndReceiveWithFailover(serviceUrl.get(), true);
    }

    @Test(dataProvider = "ServiceUrls")
    public void testNonPartitionedTopicMessagingWithShared(Supplier<String> serviceUrl) throws Exception {
        nonPartitionedTopicSendAndReceiveWithShared(serviceUrl.get(), true);
    }

    @Test(dataProvider = "ServiceUrls")
    public void testPartitionedTopicMessagingWithShared(Supplier<String> serviceUrl) throws Exception {
        partitionedTopicSendAndReceiveWithShared(serviceUrl.get(), true);
    }

    @Test(dataProvider = "ServiceUrls")
    public void testNonPartitionedTopicMessagingWithKeyShared(Supplier<String> serviceUrl) throws Exception {
        nonPartitionedTopicSendAndReceiveWithKeyShared(serviceUrl.get(), true);
    }

    @Test(dataProvider = "ServiceUrls")
    public void testPartitionedTopicMessagingWithKeyShared(Supplier<String> serviceUrl) throws Exception {
        partitionedTopicSendAndReceiveWithKeyShared(serviceUrl.get(), true);
    }

}
