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

import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.Test;

@Slf4j
public class NonPersistentTopicMessagingTest extends TopicMessagingBase {

    @Test(dataProvider = "ServiceUrls")
    public void testNonPartitionedTopicMessagingWithExclusive(String serviceUrl) throws Exception {
        nonPartitionedTopicSendAndReceiveWithExclusive(serviceUrl, false);
    }

    @Test(dataProvider = "ServiceUrls")
    public void testPartitionedTopicMessagingWithExclusive(String serviceUrl) throws Exception {
        partitionedTopicSendAndReceiveWithExclusive(serviceUrl, false);
    }

    @Test(dataProvider = "ServiceUrls")
    public void testNonPartitionedTopicMessagingWithFailover(String serviceUrl) throws Exception {
        nonPartitionedTopicSendAndReceiveWithFailover(serviceUrl, false);
    }

    @Test(dataProvider = "ServiceUrls")
    public void testPartitionedTopicMessagingWithFailover(String serviceUrl) throws Exception {
        partitionedTopicSendAndReceiveWithFailover(serviceUrl, false);
    }

    @Test(dataProvider = "ServiceUrls")
    public void testNonPartitionedTopicMessagingWithShared(String serviceUrl) throws Exception {
        nonPartitionedTopicSendAndReceiveWithShared(serviceUrl, false);
    }

    @Test(dataProvider = "ServiceUrls")
    public void testPartitionedTopicMessagingWithShared(String serviceUrl) throws Exception {
        partitionedTopicSendAndReceiveWithShared(serviceUrl, false);
    }

    @Test(dataProvider = "ServiceUrls")
    public void testNonPartitionedTopicMessagingWithKeyShared(String serviceUrl) throws Exception {
        nonPartitionedTopicSendAndReceiveWithKeyShared(serviceUrl, false);
    }

    @Test(dataProvider = "ServiceUrls")
    public void testPartitionedTopicMessagingWithKeyShared(String serviceUrl) throws Exception {
        partitionedTopicSendAndReceiveWithKeyShared(serviceUrl, false);
    }
}
