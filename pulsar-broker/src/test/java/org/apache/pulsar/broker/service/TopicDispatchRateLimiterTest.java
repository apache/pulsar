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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import lombok.Cleanup;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.common.policies.data.DispatchRate;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class TopicDispatchRateLimiterTest extends BrokerTestBase {
    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        conf.setDispatchThrottlingRatePerTopicInMsg(0);
        conf.setDispatchThrottlingRatePerTopicInByte(0L);
        conf.setSystemTopicEnabled(true);
        conf.setTopicLevelPoliciesEnabled(true);
        super.baseSetup();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testTopicDispatchRateLimiterPerTopicInMsgOnlyBrokerLevel() throws Exception {
        final String topicName = "persistent://" + newTopicName();

        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).create();
        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getOrCreateTopic(topicName).get();
        assertNotNull(topic);
        assertFalse(topic.getDispatchRateLimiter().isPresent());

        admin.brokers().updateDynamicConfiguration("dispatchThrottlingRatePerTopicInMsg", "100");
        Awaitility.await().untilAsserted(() ->
            assertEquals(pulsar.getConfig().getDispatchThrottlingRatePerTopicInMsg(), 100));

        Awaitility.await().untilAsserted(() ->
            assertTrue(topic.getDispatchRateLimiter().isPresent()));
        assertEquals(topic.getDispatchRateLimiter().get().getAvailableDispatchRateLimitOnMsg(), 100L);
    }

    @Test
    public void testTopicDispatchRateLimiterPerTopicInByteOnlyBrokerLevel() throws Exception {
        final String topicName = "persistent://" + newTopicName();

        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).create();
        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getOrCreateTopic(topicName).get();
        assertNotNull(topic);
        assertFalse(topic.getDispatchRateLimiter().isPresent());

        admin.brokers().updateDynamicConfiguration("dispatchThrottlingRatePerTopicInByte", "1000");
        Awaitility.await().untilAsserted(() ->
            assertEquals(pulsar.getConfig().getDispatchThrottlingRatePerTopicInByte(), 1000L));

        Awaitility.await().untilAsserted(() ->
            assertTrue(topic.getDispatchRateLimiter().isPresent()));
        assertEquals(topic.getDispatchRateLimiter().get().getAvailableDispatchRateLimitOnByte(), 1000L);
    }

    @Test
    public void testTopicDispatchRateLimiterOnlyNamespaceLevel() throws Exception {
        final String topicName = "persistent://" + newTopicName();

        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).create();
        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getOrCreateTopic(topicName).get();
        assertNotNull(topic);
        assertFalse(topic.getDispatchRateLimiter().isPresent());

        DispatchRate dispatchRate = DispatchRate
            .builder()
            .dispatchThrottlingRateInMsg(100)
            .dispatchThrottlingRateInByte(1000L)
            .build();
        admin.namespaces().setDispatchRate("prop/ns-abc", dispatchRate);

        Awaitility.await().untilAsserted(() -> {
            assertNotNull(admin.namespaces().getDispatchRate("prop/ns-abc"));
            assertEquals(admin.namespaces().getDispatchRate("prop/ns-abc").getDispatchThrottlingRateInMsg(), 100);
            assertEquals(admin.namespaces().getDispatchRate("prop/ns-abc").getDispatchThrottlingRateInByte(), 1000L);
        });

        Awaitility.await().untilAsserted(() -> assertTrue(topic.getDispatchRateLimiter().isPresent()));
        assertEquals(topic.getDispatchRateLimiter().get().getAvailableDispatchRateLimitOnMsg(), 100);
        assertEquals(topic.getDispatchRateLimiter().get().getAvailableDispatchRateLimitOnByte(), 1000L);
    }

    @Test
    public void testTopicDispatchRateLimiterOnlyTopicLevel() throws Exception {
        final String topicName = "persistent://" + newTopicName();

        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).create();
        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getOrCreateTopic(topicName).get();
        assertNotNull(topic);
        assertFalse(topic.getDispatchRateLimiter().isPresent());

        DispatchRate dispatchRate = DispatchRate
            .builder()
            .dispatchThrottlingRateInMsg(100)
            .dispatchThrottlingRateInByte(1000L)
            .build();
        admin.topicPolicies().setDispatchRate(topicName, dispatchRate);

        Awaitility.await().untilAsserted(() -> {
            assertNotNull(admin.topicPolicies().getDispatchRate(topicName));
            assertEquals(admin.topicPolicies().getDispatchRate(topicName).getDispatchThrottlingRateInMsg(), 100);
            assertEquals(admin.topicPolicies().getDispatchRate(topicName).getDispatchThrottlingRateInByte(), 1000L);
        });

        Awaitility.await().untilAsserted(() ->  assertTrue(topic.getDispatchRateLimiter().isPresent()));
        assertEquals(topic.getDispatchRateLimiter().get().getAvailableDispatchRateLimitOnMsg(), 100);
        assertEquals(topic.getDispatchRateLimiter().get().getAvailableDispatchRateLimitOnByte(), 1000L);
    }
}
