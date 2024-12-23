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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.pulsar.broker.loadbalance.extensions.ExtensibleLoadManagerImpl;
import org.apache.pulsar.broker.loadbalance.extensions.scheduler.TransferShedder;
import org.apache.pulsar.common.naming.TopicDomain;
import org.testng.ITest;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

public class MessagingSmokeTest extends MessagingBase implements ITest {

    TopicMessaging test;

    @Factory
    public static Object[] messagingTests() throws IOException {
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

    public MessagingSmokeTest(String name, Map<String, String> brokerEnvs) throws IOException {
        super();
        this.brokerEnvs.putAll(brokerEnvs);
        this.name = name;

    }

    @BeforeClass(alwaysRun = true)
    public void setupTest() throws Exception {
        this.test = new TopicMessaging(getPulsarClient(), getPulsarAdmin());
    }

    @AfterClass(alwaysRun = true)
    public void closeTest() throws Exception {
        this.test.close();
    }

    @Override
    public String getTestName() {
        return name;
    }

    @Test(dataProvider = "topicDomain")
    public void testNonPartitionedTopicMessagingWithExclusive(TopicDomain topicDomain)
            throws Exception {
        test.nonPartitionedTopicSendAndReceiveWithExclusive(TopicDomain.persistent.equals(topicDomain));
    }

    @Test(dataProvider = "topicDomain")
    public void testPartitionedTopicMessagingWithExclusive(TopicDomain topicDomain)
            throws Exception {
        test.partitionedTopicSendAndReceiveWithExclusive(TopicDomain.persistent.equals(topicDomain));
    }

    @Test(dataProvider = "topicDomain")
    public void testNonPartitionedTopicMessagingWithFailover(TopicDomain topicDomain)
            throws Exception {
        test.nonPartitionedTopicSendAndReceiveWithFailover(TopicDomain.persistent.equals(topicDomain));
    }

    @Test(dataProvider = "topicDomain")
    public void testPartitionedTopicMessagingWithFailover(TopicDomain topicDomain)
            throws Exception {
        test.partitionedTopicSendAndReceiveWithFailover(TopicDomain.persistent.equals(topicDomain));
    }

    @Test(dataProvider = "topicDomain")
    public void testNonPartitionedTopicMessagingWithShared(TopicDomain topicDomain)
            throws Exception {
        test.nonPartitionedTopicSendAndReceiveWithShared(TopicDomain.persistent.equals(topicDomain));
    }

    @Test(dataProvider = "topicDomain")
    public void testPartitionedTopicMessagingWithShared(TopicDomain topicDomain)
            throws Exception {
        test.partitionedTopicSendAndReceiveWithShared(TopicDomain.persistent.equals(topicDomain));
    }

    @Test(dataProvider = "topicDomain")
    public void testNonPartitionedTopicMessagingWithKeyShared(TopicDomain topicDomain)
            throws Exception {
        test.nonPartitionedTopicSendAndReceiveWithKeyShared(TopicDomain.persistent.equals(topicDomain));
    }

    @Test(dataProvider = "topicDomain")
    public void testPartitionedTopicMessagingWithKeyShared(TopicDomain topicDomain)
            throws Exception {
        test.partitionedTopicSendAndReceiveWithKeyShared(TopicDomain.persistent.equals(topicDomain));
    }
}
