package org.apache.pulsar.broker.service;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.PublishRate;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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
        conf.setMaxPendingPublishdRequestsPerConnection(0);
        super.baseSetup();
        final String topic = "persistent://prop/ns-abc/testPrecisTopicPublishRateLimiting";
        org.apache.pulsar.client.api.Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topic)
                .producerName("producer-name")
                .create();
        Policies policies = new Policies();
        policies.publishMaxMessageRate = new HashMap<>();
        policies.publishMaxMessageRate.put("test", publishRate);

        Topic topicRef = pulsar.getBrokerService().getTopicReference(topic).get();
        Assert.assertNotNull(topicRef);
        ((AbstractTopic)topicRef).updateMaxPublishRate(policies);
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
        conf.setMaxPendingPublishdRequestsPerConnection(0);
        super.baseSetup();
        final String topic = "persistent://prop/ns-abc/testPrecisTopicPublishRateLimiting";
        org.apache.pulsar.client.api.Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topic)
                .producerName("producer-name")
                .create();
        Policies policies = new Policies();
        policies.publishMaxMessageRate = new HashMap<>();
        policies.publishMaxMessageRate.put("test", publishRate);

        Topic topicRef = pulsar.getBrokerService().getTopicReference(topic).get();
        Assert.assertNotNull(topicRef);
        ((AbstractTopic)topicRef).updateMaxPublishRate(policies);
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
        conf.setMaxPendingPublishdRequestsPerConnection(0);
        super.baseSetup();
        final String topic = "persistent://prop/ns-abc/testPrecisTopicPublishRateLimiting";
        org.apache.pulsar.client.api.Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topic)
                .producerName("producer-name")
                .create();
        Policies policies = new Policies();
        policies.publishMaxMessageRate = new HashMap<>();
        policies.publishMaxMessageRate.put("test", publishRate);

        Topic topicRef = pulsar.getBrokerService().getTopicReference(topic).get();
        Assert.assertNotNull(topicRef);
        ((AbstractTopic)topicRef).updateMaxPublishRate(policies);
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
}
