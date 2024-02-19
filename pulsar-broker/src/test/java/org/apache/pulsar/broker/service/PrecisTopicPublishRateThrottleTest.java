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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Function;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.common.policies.data.PublishRate;
import org.apache.pulsar.common.util.RateLimiter;
import org.awaitility.Awaitility;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test(groups = "broker")
@Slf4j
public class PrecisTopicPublishRateThrottleTest extends BrokerTestBase{

    @Override
    protected void setup() throws Exception {
        //No-op
    }

    @Override
    protected void cleanup() throws Exception {
        //No-op
    }

    @Test
    public void testPrecisTopicPublishRateLimitingDisabled() throws Exception {
        PublishRate publishRate = new PublishRate(1,10);
        // disable precis topic publish rate limiting
        conf.setPreciseTopicPublishRateLimiterEnable(false);
        conf.setMaxPendingPublishRequestsPerConnection(0);
        super.baseSetup();
        admin.namespaces().setPublishRate("prop/ns-abc", publishRate);
        final String topic = "persistent://prop/ns-abc/testPrecisTopicPublishRateLimiting";
        org.apache.pulsar.client.api.Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topic)
                .producerName("producer-name")
                .create();

        Topic topicRef = pulsar.getBrokerService().getTopicReference(topic).get();
        Assert.assertNotNull(topicRef);
        MessageId messageId = null;
        try {
            // first will be success
            messageId = producer.sendAsync(new byte[10]).get(500, TimeUnit.MILLISECONDS);
            Assert.assertNotNull(messageId);
            // second will be success
            messageId = producer.sendAsync(new byte[10]).get(500, TimeUnit.MILLISECONDS);
            Assert.assertNotNull(messageId);
        } catch (TimeoutException e) {
            // No-op
        }
        Thread.sleep(1000);
        try {
            messageId = producer.sendAsync(new byte[10]).get(1, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            // No-op
        }
        Assert.assertNotNull(messageId);
        super.internalCleanup();
    }

    @Test
    public void testProducerBlockedByPrecisTopicPublishRateLimiting() throws Exception {
        PublishRate publishRate = new PublishRate(1,10);
        conf.setPreciseTopicPublishRateLimiterEnable(true);
        conf.setMaxPendingPublishRequestsPerConnection(0);
        super.baseSetup();
        admin.namespaces().setPublishRate("prop/ns-abc", publishRate);
        final String topic = "persistent://prop/ns-abc/testPrecisTopicPublishRateLimiting";
        org.apache.pulsar.client.api.Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topic)
                .producerName("producer-name")
                .create();

        Topic topicRef = pulsar.getBrokerService().getTopicReference(topic).get();
        Assert.assertNotNull(topicRef);
        MessageId messageId = null;
        try {
            // first will be success, and will set auto read to false
            messageId = producer.sendAsync(new byte[10]).get(500, TimeUnit.MILLISECONDS);
            Assert.assertNotNull(messageId);
            // second will be blocked
            producer.sendAsync(new byte[10]).get(500, TimeUnit.MILLISECONDS);
            Assert.fail("should failed, because producer blocked by topic publish rate limiting");
        } catch (TimeoutException e) {
            // No-op
        }
        super.internalCleanup();
    }

    @Test
    public void testPrecisTopicPublishRateLimitingProduceRefresh() throws Exception {
        PublishRate publishRate = new PublishRate(1,10);
        conf.setPreciseTopicPublishRateLimiterEnable(true);
        conf.setMaxPendingPublishRequestsPerConnection(0);
        super.baseSetup();
        admin.namespaces().setPublishRate("prop/ns-abc", publishRate);
        final String topic = "persistent://prop/ns-abc/testPrecisTopicPublishRateLimiting";
        org.apache.pulsar.client.api.Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topic)
                .producerName("producer-name")
                .create();

        Topic topicRef = pulsar.getBrokerService().getTopicReference(topic).get();
        Assert.assertNotNull(topicRef);
        MessageId messageId = null;
        try {
            // first will be success, and will set auto read to false
            messageId = producer.sendAsync(new byte[10]).get(500, TimeUnit.MILLISECONDS);
            Assert.assertNotNull(messageId);
            // second will be blocked
            producer.sendAsync(new byte[10]).get(500, TimeUnit.MILLISECONDS);
            Assert.fail("should failed, because producer blocked by topic publish rate limiting");
        } catch (TimeoutException e) {
            // No-op
        }
        Thread.sleep(1000);
        try {
            messageId = producer.sendAsync(new byte[10]).get(1, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            // No-op
        }
        Assert.assertNotNull(messageId);
        super.internalCleanup();
    }

    @Test
    public void testBrokerLevelPublishRateDynamicUpdate() throws Exception{
        conf.setPreciseTopicPublishRateLimiterEnable(true);
        conf.setTopicLevelPoliciesEnabled(true);
        conf.setSystemTopicEnabled(true);
        conf.setMaxPendingPublishRequestsPerConnection(0);
        super.baseSetup();
        final String topic = "persistent://prop/ns-abc/testMultiLevelPublishRate";
        org.apache.pulsar.client.api.Producer<byte[]> producer = pulsarClient.newProducer()
            .topic(topic)
            .producerName("producer-name")
            .create();

        final int rateInMsg = 10;
        final long rateInByte = 20;

        // maxPublishRatePerTopicInMessages
        admin.brokers().updateDynamicConfiguration("maxPublishRatePerTopicInMessages", "" + rateInMsg);
        Awaitility.await()
            .untilAsserted(() ->
                Assert.assertEquals(admin.brokers().getAllDynamicConfigurations().get("maxPublishRatePerTopicInMessages"),
                    "" + rateInMsg));
        Topic topicRef = pulsar.getBrokerService().getTopicReference(topic).get();
        Assert.assertNotNull(topicRef);
        PrecisPublishLimiter limiter = ((PrecisPublishLimiter) ((AbstractTopic) topicRef).topicPublishRateLimiter);
        Awaitility.await().untilAsserted(() -> Assert.assertEquals(limiter.publishMaxMessageRate, rateInMsg));
        Assert.assertEquals(limiter.publishMaxByteRate, 0);

        // maxPublishRatePerTopicInBytes
        admin.brokers().updateDynamicConfiguration("maxPublishRatePerTopicInBytes", "" + rateInByte);
        Awaitility.await()
            .untilAsserted(() ->
                Assert.assertEquals(admin.brokers().getAllDynamicConfigurations().get("maxPublishRatePerTopicInBytes"),
                    "" + rateInByte));
        Awaitility.await().untilAsserted(() -> Assert.assertEquals(limiter.publishMaxByteRate, rateInByte));
        Assert.assertEquals(limiter.publishMaxMessageRate, rateInMsg);

        producer.close();
        super.internalCleanup();
    }

    @Test
    public void testWithConcurrentUpdate() throws Exception {
        PublishRate publishRate = new PublishRate(-1,10);
        conf.setPreciseTopicPublishRateLimiterEnable(true);
        conf.setMaxPendingPublishRequestsPerConnection(1000);
        super.baseSetup();
        admin.namespaces().setPublishRate("prop/ns-abc", publishRate);
        final String topic = "persistent://prop/ns-abc/testWithConcurrentUpdate";
        @Cleanup
        org.apache.pulsar.client.api.Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topic)
                .producerName("producer-name")
                .create();

        AbstractTopic topicRef = (AbstractTopic) pulsar.getBrokerService().getTopicReference(topic).get();
        Assert.assertNotNull(topicRef);

        PublishRateLimiter topicPublishRateLimiter = Mockito.spy(topicRef.getTopicPublishRateLimiter());

        AtomicBoolean blocking = new AtomicBoolean(false);
        BiFunction<Function<Long, Boolean>, Long, Boolean> blockFunc = (function, acquirePermit) -> {
            blocking.set(true);
            while (blocking.get()) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            return function.apply(acquirePermit);
        };

        Mockito.doAnswer(invocation -> {
            log.info("tryAcquire: {}, {}", invocation.getArgument(0), invocation.getArgument(1));
            RateLimiter publishRateLimiterOnMessage =
                    (RateLimiter) FieldUtils.readDeclaredField(topicPublishRateLimiter,
                            "topicPublishRateLimiterOnMessage", true);
            RateLimiter publishRateLimiterOnByte =
                    (RateLimiter) FieldUtils.readDeclaredField(topicPublishRateLimiter,
                            "topicPublishRateLimiterOnByte", true);
            int numbers = invocation.getArgument(0);
            long bytes = invocation.getArgument(1);
            return (publishRateLimiterOnMessage == null || publishRateLimiterOnMessage.tryAcquire(numbers))
            && (publishRateLimiterOnByte == null || blockFunc.apply(publishRateLimiterOnByte::tryAcquire, bytes));
        }).when(topicPublishRateLimiter).tryAcquire(Mockito.anyInt(), Mockito.anyLong());

        FieldUtils.writeField(topicRef, "topicPublishRateLimiter", topicPublishRateLimiter, true);

        CompletableFuture<MessageId> sendFuture = producer.sendAsync(new byte[10]);

        Awaitility.await().untilAsserted(() -> Assert.assertTrue(blocking.get()));
        publishRate.publishThrottlingRateInByte = 20;
        admin.namespaces().setPublishRate("prop/ns-abc", publishRate);
        blocking.set(false);

        sendFuture.join();

        super.internalCleanup();
    }
}
