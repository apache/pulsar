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
package org.apache.pulsar.broker.service;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.common.policies.data.PublishRate;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class TopicPublishRateThrottleTest extends BrokerTestBase{
    @BeforeMethod(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        // no-op, each test will call baseSetup
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testProducerBlockedByPrecisTopicPublishRateLimiting() throws Exception {
        PublishRate publishRate = new PublishRate(1, 10);
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
        removePublishRateAndCloseClient();
    }

    @Test
    public void testResumeEvenProducerClosed() throws Exception {
        PublishRate publishRate = new PublishRate(1, 10);
        conf.setMaxPendingPublishRequestsPerConnection(0);
        super.baseSetup();
        admin.namespaces().setPublishRate("prop/ns-abc", publishRate);
        final String topic = BrokerTestUtil.newUniqueName("persistent://prop/ns-abc/tp");
        org.apache.pulsar.client.api.Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topic).create();

        Topic topicRef = pulsar.getBrokerService().getTopicReference(topic).get();
        Assert.assertNotNull(topicRef);
        TransportCnx cnx = topicRef.getProducers().values().iterator().next().getCnx();
        MessageId messageId = null;
        // first will be success, and the second will fail, will set auto read to false.
        messageId = producer.sendAsync(new byte[10]).get(500, TimeUnit.MILLISECONDS);
        Assert.assertNotNull(messageId);
        // second will be blocked
        producer.sendAsync(new byte[10]);

        // Verify: even through the producer was closed before the unblock, the state should be unblocked at the next
        // period of rate limiter.
        producer.close();
        Awaitility.await().untilAsserted(() -> Assert.assertEquals(getThrottledCount(cnx), 0));
        org.apache.pulsar.client.api.Producer<byte[]> producer2 = pulsarClient.newProducer()
                .topic(topic).create();
        producer2.sendAsync(new byte[2]).get(500, TimeUnit.MILLISECONDS);

        removePublishRateAndCloseClient();
    }

    @Test
    public void testSystemTopicPublishNonBlock() throws Exception {
        super.baseSetup();
        PublishRate publishRate = new PublishRate(1, 10);
        admin.namespaces().setPublishRate("prop/ns-abc", publishRate);
        final String topic = BrokerTestUtil.newUniqueName("persistent://prop/ns-abc/tp");
        PulsarAdmin admin1 = PulsarAdmin.builder().serviceHttpUrl(brokerUrl != null
            ? brokerUrl.toString() : brokerUrlTls.toString()).readTimeout(5, TimeUnit.SECONDS).build();
        admin1.topics().createNonPartitionedTopic(topic);
        admin1.topicPolicies().setDeduplicationStatus(topic, true);
        admin1.topicPolicies().setDeduplicationStatus(topic, false);
        // cleanup.
        admin.namespaces().removePublishRate("prop/ns-abc");
        admin1.close();
    }

    @Test
    public void testPrecisTopicPublishRateLimitingProduceRefresh() throws Exception {
        PublishRate publishRate = new PublishRate(1, 10);
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
        CompletableFuture<MessageId> blockedSend = null;
        try {
            // first will be success, and will set auto read to false
            messageId = producer.sendAsync(new byte[10]).get(500, TimeUnit.MILLISECONDS);
            Assert.assertNotNull(messageId);
            // second will be blocked
            blockedSend = producer.sendAsync(new byte[10]);
            blockedSend.get(500, TimeUnit.MILLISECONDS);
            Assert.fail("should failed, because producer blocked by topic publish rate limiting");
        } catch (TimeoutException e) {
            // No-op
        }
        // The blocked message is published once the rate limiter has tokens again
        Assert.assertNotNull(blockedSend.get(10, TimeUnit.SECONDS));
        removePublishRateAndCloseClient();
    }

    @Test
    public void testBrokerLevelPublishRateDynamicUpdate() throws Exception{
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
                Assert.assertEquals(admin.brokers()
                                .getAllDynamicConfigurations().get("maxPublishRatePerTopicInMessages"),
                    "" + rateInMsg));
        Topic topicRef = pulsar.getBrokerService().getTopicReference(topic).get();
        Assert.assertNotNull(topicRef);
        PublishRateLimiterImpl limiter = ((PublishRateLimiterImpl) ((AbstractTopic) topicRef).topicPublishRateLimiter);
        Awaitility.await()
                .untilAsserted(() -> Assert.assertEquals(limiter.getTokenBucketOnMessage().getRate(), rateInMsg));
        Assert.assertNull(limiter.getTokenBucketOnByte());

        // maxPublishRatePerTopicInBytes
        admin.brokers().updateDynamicConfiguration("maxPublishRatePerTopicInBytes", "" + rateInByte);
        Awaitility.await()
            .untilAsserted(() ->
                Assert.assertEquals(admin.brokers().getAllDynamicConfigurations().get("maxPublishRatePerTopicInBytes"),
                    "" + rateInByte));
        Awaitility.await()
                .untilAsserted(() -> Assert.assertEquals(limiter.getTokenBucketOnByte().getRate(), rateInByte));
        Assert.assertEquals(limiter.getTokenBucketOnMessage().getRate(), rateInMsg);

        producer.close();
    }

    // The throttle tracker belongs to the connection's I/O thread: read it there
    private static int getThrottledCount(TransportCnx cnx) throws Exception {
        CompletableFuture<Integer> throttledCount = new CompletableFuture<>();
        cnx.execute(() -> throttledCount.complete(cnx.getThrottleTracker().throttledCount()));
        return throttledCount.get(5, TimeUnit.SECONDS);
    }

    // A throttled connection reads the producers' close requests only once the pending messages' publish rate debt
    // is paid, a few seconds per message: remove the publish rate first
    private void removePublishRateAndCloseClient() throws Exception {
        admin.namespaces().removePublishRate("prop/ns-abc");
        // Close the PulsarClient gracefully to avoid ByteBuf leak
        pulsarClient.close();
    }
}
