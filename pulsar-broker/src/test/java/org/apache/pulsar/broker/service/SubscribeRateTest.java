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

import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import java.util.Optional;
import lombok.Cleanup;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.service.persistent.SubscribeRateLimiter;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.common.policies.data.SubscribeRate;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class SubscribeRateTest extends BrokerTestBase {
    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.baseSetup();
        conf.setTopicLevelPoliciesEnabled(true);
        conf.setSystemTopicEnabled(true);
        conf.setMaxPendingPublishRequestsPerConnection(0);
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testBrokerLevelSubscribeRateDynamicUpdate() throws Exception {
        final String topic = "persistent://prop/ns-abc/testBrokerLevelSubscribeRateDynamicUpdate";
        Producer<byte[]> producer = pulsarClient.newProducer()
            .topic(topic)
            .producerName("producer-name")
            .create();

        Topic topicRef = pulsar.getBrokerService().getTopicReference(topic).get();
        Assert.assertNotNull(topicRef);
        Assert.assertFalse(topicRef.getSubscribeRateLimiter().isPresent());

        final int ratePerConsumer = 10;
        final int ratePeriod = 60;

        String defaultRatePerConsumer = admin.brokers().getRuntimeConfigurations().get("subscribeThrottlingRatePerConsumer");
        String defaultRatePeriod = admin.brokers().getRuntimeConfigurations().get("subscribeRatePeriodPerConsumerInSecond");
        Assert.assertNotNull(defaultRatePerConsumer);
        Assert.assertNotNull(defaultRatePeriod);
        Assert.assertNotEquals(ratePerConsumer, Integer.parseInt(defaultRatePerConsumer));
        Assert.assertNotEquals(ratePeriod, Integer.parseInt(defaultRatePeriod));

        // subscribeThrottlingRatePerConsumer
        admin.brokers().updateDynamicConfiguration("subscribeThrottlingRatePerConsumer", ratePerConsumer + "");
        Awaitility.await().untilAsserted(() -> Assert.assertTrue(topicRef.getSubscribeRateLimiter().isPresent()));
        SubscribeRateLimiter limiter = topicRef.getSubscribeRateLimiter().get();
        Assert.assertEquals(limiter.getSubscribeRate().subscribeThrottlingRatePerConsumer, ratePerConsumer);
        Assert.assertEquals(limiter.getSubscribeRate().ratePeriodInSecond, 30);

        // subscribeRatePeriodPerConsumerInSecond
        admin.brokers().updateDynamicConfiguration("subscribeRatePeriodPerConsumerInSecond", ratePeriod + "");
        Awaitility.await().untilAsserted(() -> Assert.assertEquals(limiter.getSubscribeRate().ratePeriodInSecond, ratePeriod));
        Assert.assertEquals(limiter.getSubscribeRate().subscribeThrottlingRatePerConsumer, ratePerConsumer);

        producer.close();
    }

    @Test
    public void testUpdateSubscribeRateLimiter() throws Exception {

        final String topic = "persistent://prop/ns-abc/testUpdateSubscribeRateLimiter";

        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer()
            .topic(topic)
            .producerName("producer-name")
            .create();

        Topic topicRef = pulsar.getBrokerService().getTopicReference(topic).get();
        Assert.assertNotNull(topicRef);
        Assert.assertTrue(topicRef instanceof PersistentTopic);
        Assert.assertFalse(topicRef.getSubscribeRateLimiter().isPresent());

        // init
        PersistentTopic persistentTopic = spy(((PersistentTopic) topicRef));
        when(persistentTopic.getSubscribeRate()).thenReturn(new SubscribeRate(10, 60));
        persistentTopic.updateSubscribeRateLimiter();

        Optional<SubscribeRateLimiter> limiter1 = persistentTopic.getSubscribeRateLimiter();
        Assert.assertTrue(limiter1.isPresent());
        Assert.assertEquals(limiter1.get().getSubscribeRate(), new SubscribeRate(10, 60));

        // update
        when(persistentTopic.getSubscribeRate()).thenReturn(new SubscribeRate(20, 120));
        persistentTopic.updateSubscribeRateLimiter();

        Optional<SubscribeRateLimiter> limiter2 = persistentTopic.getSubscribeRateLimiter();
        Assert.assertTrue(limiter2.isPresent());
        Assert.assertEquals(limiter2.get().getSubscribeRate(), new SubscribeRate(20, 120));

        Assert.assertSame(limiter1, limiter2);

        // disable
        when(persistentTopic.getSubscribeRate()).thenReturn(new SubscribeRate(0, 0));
        persistentTopic.updateSubscribeRateLimiter();

        Optional<SubscribeRateLimiter> limiter3 = persistentTopic.getSubscribeRateLimiter();
        Assert.assertFalse(limiter3.isPresent());
    }
}
