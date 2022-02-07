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
package org.apache.pulsar.broker.service;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.AutoSubscriptionCreationOverride;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class BrokerServiceAutoSubscriptionCreationTest extends BrokerTestBase {

    private final AtomicInteger testId = new AtomicInteger(0);

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.baseSetup();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @AfterMethod(alwaysRun = true)
    protected void cleanupTest() throws Exception {
        pulsar.getAdminClient().namespaces().removeAutoSubscriptionCreation("prop/ns-abc");
    }

    @Test
    public void testAutoSubscriptionCreationDisable() throws Exception {
        pulsar.getConfiguration().setAllowAutoSubscriptionCreation(false);

        final String topicName = "persistent://prop/ns-abc/test-subtopic-" + testId.getAndIncrement();
        final String subscriptionName = "test-subtopic-sub";

        admin.topics().createNonPartitionedTopic(topicName);

        try {
            pulsarClient.newConsumer().topic(topicName).subscriptionName(subscriptionName).subscribe();
            fail("Subscribe operation should have failed");
        } catch (Exception e) {
            assertTrue(e instanceof PulsarClientException);
        }
        assertFalse(admin.topics().getSubscriptions(topicName).contains(subscriptionName));
    }

    @Test
    public void testSubscriptionCreationWithAutoCreationDisable() throws Exception {
        pulsar.getConfiguration().setAllowAutoSubscriptionCreation(false);

        final String topicName = "persistent://prop/ns-abc/test-subtopic-" + testId.getAndIncrement();
        final String subscriptionName = "test-subtopic-sub-1";

        admin.topics().createNonPartitionedTopic(topicName);
        assertFalse(admin.topics().getSubscriptions(topicName).contains(subscriptionName));

        // Create the subscription by PulsarAdmin
        admin.topics().createSubscription(topicName, subscriptionName, MessageId.earliest);
        assertTrue(admin.topics().getSubscriptions(topicName).contains(subscriptionName));

        // Subscribe operation should be successful
        pulsarClient.newConsumer().topic(topicName).subscriptionName(subscriptionName).subscribe();
    }

    @Test
    public void testAutoSubscriptionCreationNamespaceAllowOverridesBroker() throws Exception {
        final String topic = "persistent://prop/ns-abc/test-subtopic-" + testId.getAndIncrement();
        final String subscriptionName = "test-subtopic-sub-2";
        final TopicName topicName = TopicName.get(topic);

        admin.topics().createNonPartitionedTopic(topicName.toString());

        pulsar.getConfiguration().setAllowAutoSubscriptionCreation(false);
        AutoSubscriptionCreationOverride autoSubscriptionCreationOverride = AutoSubscriptionCreationOverride.builder()
                .allowAutoSubscriptionCreation(true)
                .build();
        pulsar.getAdminClient().namespaces().setAutoSubscriptionCreation(topicName.getNamespace(),
                autoSubscriptionCreationOverride);
        Assert.assertEquals(pulsar.getAdminClient().namespaces().getAutoSubscriptionCreation(topicName.getNamespace()),
                autoSubscriptionCreationOverride);

        // Subscribe operation should be successful
        pulsarClient.newConsumer().topic(topicName.toString()).subscriptionName(subscriptionName).subscribe();
        assertTrue(admin.topics().getSubscriptions(topicName.toString()).contains(subscriptionName));
    }

    @Test
    public void testAutoSubscriptionCreationNamespaceDisallowOverridesBroker() throws Exception {
        final String topic = "persistent://prop/ns-abc/test-subtopic-" + testId.getAndIncrement();
        final String subscriptionName = "test-subtopic-sub-3";
        final TopicName topicName = TopicName.get(topic);

        admin.topics().createNonPartitionedTopic(topicName.toString());

        pulsar.getConfiguration().setAllowAutoSubscriptionCreation(true);
        AutoSubscriptionCreationOverride autoSubscriptionCreationOverride = AutoSubscriptionCreationOverride.builder()
                .allowAutoSubscriptionCreation(false)
                .build();
        pulsar.getAdminClient().namespaces().setAutoSubscriptionCreation(topicName.getNamespace(),
                autoSubscriptionCreationOverride);
        Assert.assertEquals(pulsar.getAdminClient().namespaces().getAutoSubscriptionCreation(topicName.getNamespace()),
                autoSubscriptionCreationOverride);

        try {
            pulsarClient.newConsumer().topic(topicName.toString()).subscriptionName(subscriptionName).subscribe();
            fail("Subscribe operation should have failed");
        } catch (Exception e) {
            assertTrue(e instanceof PulsarClientException);
        }
        assertFalse(admin.topics().getSubscriptions(topicName.toString()).contains(subscriptionName));
    }

    @Test
    public void testNonPersistentTopicSubscriptionCreationWithAutoCreationDisable() throws Exception {
        pulsar.getConfiguration().setAllowAutoSubscriptionCreation(false);

        final String topicName = "non-persistent://prop/ns-abc/test-subtopic-" + testId.getAndIncrement();
        final String subscriptionName = "test-subtopic-sub";

        admin.topics().createNonPartitionedTopic(topicName);

        // Subscribe operation should be successful
        pulsarClient.newConsumer().topic(topicName).subscriptionName(subscriptionName).subscribe();
        assertTrue(admin.topics().getSubscriptions(topicName).contains(subscriptionName));
    }

}
