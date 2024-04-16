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
package org.apache.pulsar.tests.integration.messaging;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.pulsar.broker.loadbalance.extensions.ExtensibleLoadManagerImpl;
import org.apache.pulsar.broker.loadbalance.extensions.scheduler.TransferShedder;
import org.apache.pulsar.common.naming.TopicDomain;
import org.testng.ITest;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

public class MessagingSmokeTest extends TopicMessagingBase implements ITest {

    @Factory
    public static Object[] messagingTests() {
        List<?> tests = List.of(
                new MessagingSmokeTest("Extensible Load Manager",
                        Map.of("loadManagerClassName", ExtensibleLoadManagerImpl.class.getName(),
                                "loadBalancerLoadSheddingStrategy", TransferShedder.class.getName())),
                new MessagingSmokeTest("Extensible Load Manager with TX Coordinator",
                        Map.of("loadManagerClassName", ExtensibleLoadManagerImpl.class.getName(),
                                "loadBalancerLoadSheddingStrategy", TransferShedder.class.getName(),
                                "transactionCoordinatorEnabled", "true"))
        );
        return tests.toArray();
    }

    private final String name;

    public MessagingSmokeTest(String name, Map<String, String> brokerEnvs) {
        super();
        this.brokerEnvs.putAll(brokerEnvs);
        this.name = name;
    }

    @Override
    public String getTestName() {
        return name;
    }

    @Test(dataProvider = "serviceUrlAndTopicDomain")
    public void testNonPartitionedTopicMessagingWithExclusive(Supplier<String> serviceUrl, TopicDomain topicDomain)
            throws Exception {
        nonPartitionedTopicSendAndReceiveWithExclusive(serviceUrl.get(), TopicDomain.persistent.equals(topicDomain));
    }

    @Test(dataProvider = "serviceUrlAndTopicDomain")
    public void testPartitionedTopicMessagingWithExclusive(Supplier<String> serviceUrl, TopicDomain topicDomain)
            throws Exception {
        partitionedTopicSendAndReceiveWithExclusive(serviceUrl.get(), TopicDomain.persistent.equals(topicDomain));
    }

    @Test(dataProvider = "serviceUrlAndTopicDomain")
    public void testNonPartitionedTopicMessagingWithFailover(Supplier<String> serviceUrl, TopicDomain topicDomain)
            throws Exception {
        nonPartitionedTopicSendAndReceiveWithFailover(serviceUrl.get(), TopicDomain.persistent.equals(topicDomain));
    }

    @Test(dataProvider = "serviceUrlAndTopicDomain")
    public void testPartitionedTopicMessagingWithFailover(Supplier<String> serviceUrl, TopicDomain topicDomain)
            throws Exception {
        partitionedTopicSendAndReceiveWithFailover(serviceUrl.get(), TopicDomain.persistent.equals(topicDomain));
    }

    @Test(dataProvider = "serviceUrlAndTopicDomain")
    public void testNonPartitionedTopicMessagingWithShared(Supplier<String> serviceUrl, TopicDomain topicDomain)
            throws Exception {
        nonPartitionedTopicSendAndReceiveWithShared(serviceUrl.get(), TopicDomain.persistent.equals(topicDomain));
    }

    @Test(dataProvider = "serviceUrlAndTopicDomain")
    public void testPartitionedTopicMessagingWithShared(Supplier<String> serviceUrl, TopicDomain topicDomain)
            throws Exception {
        partitionedTopicSendAndReceiveWithShared(serviceUrl.get(), TopicDomain.persistent.equals(topicDomain));
    }

    @Test(dataProvider = "serviceUrlAndTopicDomain")
    public void testNonPartitionedTopicMessagingWithKeyShared(Supplier<String> serviceUrl, TopicDomain topicDomain)
            throws Exception {
        nonPartitionedTopicSendAndReceiveWithKeyShared(serviceUrl.get(), TopicDomain.persistent.equals(topicDomain));
    }

    @Test(dataProvider = "serviceUrlAndTopicDomain")
    public void testPartitionedTopicMessagingWithKeyShared(Supplier<String> serviceUrl, TopicDomain topicDomain)
            throws Exception {
        partitionedTopicSendAndReceiveWithKeyShared(serviceUrl.get(), TopicDomain.persistent.equals(topicDomain));
    }
}
