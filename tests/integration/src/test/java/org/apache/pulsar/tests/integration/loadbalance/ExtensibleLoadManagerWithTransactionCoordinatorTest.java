/*
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
package org.apache.pulsar.tests.integration.loadbalance;

import java.util.Map;
import java.util.function.Supplier;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.tests.integration.messaging.TopicMessagingBase;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

@Test(singleThreaded = true)
public class ExtensibleLoadManagerWithTransactionCoordinatorTest extends TopicMessagingBase {

    @Factory(dataProvider = "BrokerConfigs")
    public ExtensibleLoadManagerWithTransactionCoordinatorTest(Map<String, String> brokerEnvs) {
        super();
        this.brokerEnvs.putAll(brokerEnvs);
    }

    @DataProvider(name = "BrokerConfigs")
    public static Object[][] brokerConfigs() {
        return new Object[][] {
                {
                        Map.of("loadManagerClassName", "org.apache.pulsar.broker.loadbalance.ExtensibleLoadManagerImpl",
                                "loadBalancerLoadSheddingStrategy",
                                "org.apache.pulsar.broker.loadbalance.extensions.scheduler.TransferShedder"),
                },
                {
                        Map.of("loadManagerClassName", "org.apache.pulsar.broker.loadbalance.ExtensibleLoadManagerImpl",
                                "loadBalancerLoadSheddingStrategy",
                                "org.apache.pulsar.broker.loadbalance.extensions.scheduler.TransferShedder",
                                "transactionCoordinatorEnabled", "true"),
                },
        };
    }

    @Test(dataProvider = "ServiceUrlAndTopicDomain")
    public void testNonPartitionedTopicMessagingWithExclusive(Supplier<String> serviceUrl, TopicDomain topicDomain)
            throws Exception {
        nonPartitionedTopicSendAndReceiveWithExclusive(serviceUrl.get(), TopicDomain.persistent.equals(topicDomain));
    }

    @Test(dataProvider = "ServiceUrlAndTopicDomain")
    public void testPartitionedTopicMessagingWithExclusive(Supplier<String> serviceUrl, TopicDomain topicDomain)
            throws Exception {
        partitionedTopicSendAndReceiveWithExclusive(serviceUrl.get(), TopicDomain.persistent.equals(topicDomain));
    }

    @Test(dataProvider = "ServiceUrlAndTopicDomain")
    public void testNonPartitionedTopicMessagingWithFailover(Supplier<String> serviceUrl, TopicDomain topicDomain) throws Exception {
        nonPartitionedTopicSendAndReceiveWithFailover(serviceUrl.get(), TopicDomain.persistent.equals(topicDomain));
    }

    @Test(dataProvider = "ServiceUrlAndTopicDomain")
    public void testPartitionedTopicMessagingWithFailover(Supplier<String> serviceUrl, TopicDomain topicDomain) throws Exception {
        partitionedTopicSendAndReceiveWithFailover(serviceUrl.get(), TopicDomain.persistent.equals(topicDomain));
    }

    @Test(dataProvider = "ServiceUrlAndTopicDomain")
    public void testNonPartitionedTopicMessagingWithShared(Supplier<String> serviceUrl, TopicDomain topicDomain) throws Exception {
        nonPartitionedTopicSendAndReceiveWithShared(serviceUrl.get(), TopicDomain.persistent.equals(topicDomain));
    }

    @Test(dataProvider = "ServiceUrlAndTopicDomain")
    public void testPartitionedTopicMessagingWithShared(Supplier<String> serviceUrl, TopicDomain topicDomain) throws Exception {
        partitionedTopicSendAndReceiveWithShared(serviceUrl.get(), TopicDomain.persistent.equals(topicDomain));
    }

    @Test(dataProvider = "ServiceUrlAndTopicDomain")
    public void testNonPartitionedTopicMessagingWithKeyShared(Supplier<String> serviceUrl, TopicDomain topicDomain) throws Exception {
        nonPartitionedTopicSendAndReceiveWithKeyShared(serviceUrl.get(), TopicDomain.persistent.equals(topicDomain));
    }

    @Test(dataProvider = "ServiceUrlAndTopicDomain")
    public void testPartitionedTopicMessagingWithKeyShared(Supplier<String> serviceUrl, TopicDomain topicDomain) throws Exception {
        partitionedTopicSendAndReceiveWithKeyShared(serviceUrl.get(), TopicDomain.persistent.equals(topicDomain));
    }
}
