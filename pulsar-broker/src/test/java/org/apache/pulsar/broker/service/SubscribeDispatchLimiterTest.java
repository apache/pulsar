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
import java.util.Optional;
import lombok.Cleanup;
import org.apache.pulsar.broker.service.persistent.DispatchRateLimiter;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.Consumer;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class SubscribeDispatchLimiterTest extends BrokerTestBase {
    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        conf.setDispatchThrottlingRatePerSubscriptionInMsg(0);
        conf.setDispatchThrottlingRatePerSubscriptionInByte(0L);
        super.baseSetup();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testDispatchRateLimiterPerSubscriptionInMsgOnlyBrokerLevel() throws Exception {
        final String topicName = "persistent://" + newTopicName();
        final String subscribeName = "cg_testDispatchRateLimiterPerSubscriptionInMsgOnlyBrokerLevel";

        @Cleanup
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
            .topic(topicName)
            .subscriptionName(subscribeName)
            .subscribe();

        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getOrCreateTopic(topicName).get();
        assertNotNull(topic);

        PersistentSubscription subscription = topic.getSubscriptions().get(subscribeName);
        assertNotNull(subscription);
        assertFalse(subscription.getDispatcher().getRateLimiter().isPresent());

        admin.brokers().updateDynamicConfiguration("dispatchThrottlingRatePerSubscriptionInMsg", "100");
        Awaitility.await().untilAsserted(() ->
            assertEquals(pulsar.getConfig().getDispatchThrottlingRatePerSubscriptionInMsg(), 100)
        );
        Awaitility.await().untilAsserted(() -> {
            Optional<DispatchRateLimiter> limiterOpt = subscription.getDispatcher().getRateLimiter();
            assertTrue(limiterOpt.isPresent());
            assertEquals(limiterOpt.get().getDispatchRateOnMsg(), 100);
        });
    }

    @Test
    public void testDispatchRateLimiterPerSubscriptionInByteOnlyBrokerLevel() throws Exception {
        final String topicName = "persistent://" + newTopicName();
        final String subscribeName = "testDispatchRateLimiterPerSubscriptionInByteOnlyBrokerLevel";

        @Cleanup
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
            .topic(topicName)
            .subscriptionName(subscribeName)
            .subscribe();

        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getOrCreateTopic(topicName).get();
        assertNotNull(topic);

        PersistentSubscription subscription = topic.getSubscriptions().get(subscribeName);
        assertNotNull(subscription);
        assertFalse(subscription.getDispatcher().getRateLimiter().isPresent());

        admin.brokers().updateDynamicConfiguration("dispatchThrottlingRatePerSubscriptionInByte", "1000");
        Awaitility.await().untilAsserted(() ->
            assertEquals(pulsar.getConfig().getDispatchThrottlingRatePerSubscriptionInByte(), 1000)
        );
        Awaitility.await().untilAsserted(() -> {
            Optional<DispatchRateLimiter> limiterOpt = subscription.getDispatcher().getRateLimiter();
            assertTrue(limiterOpt.isPresent());
            assertEquals(limiterOpt.get().getDispatchRateOnByte(), 1000);
        });
    }
}
