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
package org.apache.pulsar.client.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertTrue;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AtomicDouble;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Cleanup;
import org.apache.pulsar.broker.service.Producer;
import org.apache.pulsar.broker.service.PublishRateLimiter;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.common.policies.data.PublishRate;
import org.awaitility.Awaitility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test
public class MessagePublishThrottlingTest extends ProducerConsumerBase {
    private static final Logger log = LoggerFactory.getLogger(MessagePublishThrottlingTest.class);

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        this.conf.setClusterName("test");
        this.conf.setTopicPublisherThrottlingTickTimeMillis(1);
        this.conf.setBrokerPublisherThrottlingTickTimeMillis(1);
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
        super.resetConfig();
    }

    /**
     * Verifies publish rate limiting by setting rate-limiting on number of published messages.
     *
     * @throws Exception
     */
    @Test
    public void testSimplePublishMessageThrottling() throws Exception {
        log.info("-- Starting {} test --", methodName);

        final String namespace = "my-property/throttling_publish";
        final String topicName = "persistent://" + namespace + "/throttlingMessageBlock";

        admin.namespaces().createNamespace(namespace, Sets.newHashSet("test"));
        PublishRate publishMsgRate = new PublishRate();
        publishMsgRate.publishThrottlingRateInMsg = 10;

        // create producer and topic
        ProducerImpl<byte[]> producer = (ProducerImpl<byte[]>) pulsarClient.newProducer().topic(topicName)
                .maxPendingMessages(30000).create();
        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getTopicIfExists(topicName).get().get();
        // (1) verify message-rate is -1 initially
        Assert.assertEquals(topic.getTopicPublishRateLimiter(), PublishRateLimiter.DISABLED_RATE_LIMITER);

        // enable throttling
        admin.namespaces().setPublishRate(namespace, publishMsgRate);
        retryStrategically((test) ->
                !topic.getTopicPublishRateLimiter().equals(PublishRateLimiter.DISABLED_RATE_LIMITER),
            5,
            200);
        Assert.assertNotEquals(topic.getTopicPublishRateLimiter(), PublishRateLimiter.DISABLED_RATE_LIMITER);

        Producer prod = topic.getProducers().values().iterator().next();
        // reset counter
        prod.updateRates();
        int total = 200;
        for (int i = 0; i < total; i++) {
            producer.send(new byte[80]);
        }
        // calculate rates and due to throttling rate should be < total per-second
        prod.updateRates();
        double rateIn = prod.getStats().msgRateIn;
        assertTrue(rateIn < total);

        // disable throttling
        publishMsgRate.publishThrottlingRateInMsg = -1;
        admin.namespaces().setPublishRate(namespace, publishMsgRate);
        retryStrategically((test) ->
                topic.getTopicPublishRateLimiter().equals(PublishRateLimiter.DISABLED_RATE_LIMITER),
            5,
            200);
        Assert.assertEquals(topic.getTopicPublishRateLimiter(), PublishRateLimiter.DISABLED_RATE_LIMITER);

        // reset counter
        prod.updateRates();
        for (int i = 0; i < total; i++) {
            producer.send(new byte[80]);
        }

        prod.updateRates();
        rateIn = prod.getStats().msgRateIn;
        assertTrue(rateIn > total);

        producer.close();
    }

    /**
     * Verifies publish rate limiting by setting rate-limiting on number of publish bytes.
     *
     * @throws Exception
     */
    @Test
    public void testSimplePublishByteThrottling() throws Exception {
        log.info("-- Starting {} test --", methodName);

        final String namespace = "my-property/throttling_publish";
        final String topicName = "persistent://" + namespace + "/throttlingRateBlock";

        admin.namespaces().createNamespace(namespace, Sets.newHashSet("test"));
        PublishRate publishMsgRate = new PublishRate();
        publishMsgRate.publishThrottlingRateInByte = 400;

        // create producer and topic
        ProducerImpl<byte[]> producer = (ProducerImpl<byte[]>) pulsarClient.newProducer().topic(topicName).create();
        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getOrCreateTopic(topicName).get();
        // (1) verify message-rate is -1 initially
        Assert.assertEquals(topic.getTopicPublishRateLimiter(), PublishRateLimiter.DISABLED_RATE_LIMITER);

        // enable throttling
        admin.namespaces().setPublishRate(namespace, publishMsgRate);
        retryStrategically((test) ->
                !topic.getTopicPublishRateLimiter().equals(PublishRateLimiter.DISABLED_RATE_LIMITER),
            5,
            200);
        Assert.assertNotEquals(topic.getTopicPublishRateLimiter(), PublishRateLimiter.DISABLED_RATE_LIMITER);

        Producer prod = topic.getProducers().values().iterator().next();
        // reset counter
        prod.updateRates();
        int total = 100;
        for (int i = 0; i < total; i++) {
            producer.send(new byte[1]);
        }
        // calculate rates and due to throttling rate should be < total per-second
        prod.updateRates();
        double rateIn = prod.getStats().msgRateIn;
        assertTrue(rateIn < total);

        // disable throttling
        publishMsgRate.publishThrottlingRateInByte = -1;
        admin.namespaces().setPublishRate(namespace, publishMsgRate);
        retryStrategically((test) -> topic.getTopicPublishRateLimiter().equals(PublishRateLimiter.DISABLED_RATE_LIMITER), 5,
                200);
        Assert.assertEquals(topic.getTopicPublishRateLimiter(), PublishRateLimiter.DISABLED_RATE_LIMITER);

        // reset counter
        prod.updateRates();
        for (int i = 0; i < total; i++) {
            producer.send(new byte[1]);
        }

        prod.updateRates();
        rateIn = prod.getStats().msgRateIn;
        assertTrue(rateIn > total);

        producer.close();
    }

    /**
     * Verifies publish rate limiting by setting rate-limiting on number of published messages.
     * Broker publish throttle enabled / topic publish throttle disabled
     * @throws Exception
     */
    @Test
    public void testBrokerPublishMessageThrottling() throws Exception {
        log.info("-- Starting {} test --", methodName);

        final String namespace = "my-property/throttling_publish";
        final String topicName = "persistent://" + namespace + "/brokerThrottlingMessageBlock";

        admin.namespaces().createNamespace(namespace, Sets.newHashSet("test"));
        int messageRate = 10;

        // create producer and topic
        ProducerImpl<byte[]> producer = (ProducerImpl<byte[]>) pulsarClient.newProducer()
            .topic(topicName)
            .enableBatching(false)
            .maxPendingMessages(30000).create();
        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getTopicIfExists(topicName).get().get();
        // (1) verify message-rate is -1 initially
        Assert.assertEquals(topic.getBrokerPublishRateLimiter(), PublishRateLimiter.DISABLED_RATE_LIMITER);

        // enable throttling
        admin.brokers().
            updateDynamicConfiguration(
                "brokerPublisherThrottlingMaxMessageRate",
                Integer.toString(messageRate));

        retryStrategically(
            (test) ->
                (topic.getBrokerPublishRateLimiter() != PublishRateLimiter.DISABLED_RATE_LIMITER),
            5,
            200);

        log.info("Get broker configuration: brokerTick {},  MaxMessageRate {}, MaxByteRate {}",
            pulsar.getConfiguration().getBrokerPublisherThrottlingTickTimeMillis(),
            pulsar.getConfiguration().getBrokerPublisherThrottlingMaxMessageRate(),
            pulsar.getConfiguration().getBrokerPublisherThrottlingMaxByteRate());

        Assert.assertNotEquals(topic.getBrokerPublishRateLimiter(), PublishRateLimiter.DISABLED_RATE_LIMITER);

        Producer prod = topic.getProducers().values().iterator().next();
        // reset counter
        prod.updateRates();
        int total = 100;
        for (int i = 0; i < total; i++) {
            producer.send(new byte[80]);
        }
        // calculate rates and due to throttling rate should be < total per-second
        prod.updateRates();
        double rateIn = prod.getStats().msgRateIn;
        log.info("1-st rate in: {}, total: {} ", rateIn, total);
        assertTrue(rateIn < total);

        // disable throttling
        admin.brokers()
            .updateDynamicConfiguration("brokerPublisherThrottlingMaxMessageRate", Integer.toString(0));
        retryStrategically((test) ->
                topic.getBrokerPublishRateLimiter().equals(PublishRateLimiter.DISABLED_RATE_LIMITER),
            5,
            200);
        Assert.assertEquals(topic.getBrokerPublishRateLimiter(), PublishRateLimiter.DISABLED_RATE_LIMITER);

        // reset counter
        prod.updateRates();
        for (int i = 0; i < total; i++) {
            producer.send(new byte[80]);
        }

        prod.updateRates();
        rateIn = prod.getStats().msgRateIn;
        log.info("2-nd rate in: {}, total: {} ", rateIn, total);
        assertTrue(rateIn > total);

        producer.close();
    }

    /**
     * Verifies publish rate limiting by setting rate-limiting on number of publish bytes.
     * Broker publish throttle enabled / topic publish throttle disabled
     * @throws Exception
     */
    @Test
    public void testBrokerPublishByteThrottling() throws Exception {
        log.info("-- Starting {} test --", methodName);

        final String namespace = "my-property/throttling_publish";
        final String topicName = "persistent://" + namespace + "/brokerThrottlingByteBlock";

        admin.namespaces().createNamespace(namespace, Sets.newHashSet("test"));
        long byteRate = 400;

        // create producer and topic
        ProducerImpl<byte[]> producer = (ProducerImpl<byte[]>) pulsarClient.newProducer()
            .topic(topicName)
            .enableBatching(false)
            .maxPendingMessages(30000).create();
        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getTopicIfExists(topicName).get().get();
        // (1) verify byte-rate is -1 disabled
        Assert.assertEquals(topic.getBrokerPublishRateLimiter(), PublishRateLimiter.DISABLED_RATE_LIMITER);

        // enable throttling
        admin.brokers()
            .updateDynamicConfiguration("brokerPublisherThrottlingMaxByteRate", Long.toString(byteRate));

        retryStrategically(
            (test) ->
                (topic.getBrokerPublishRateLimiter() != PublishRateLimiter.DISABLED_RATE_LIMITER),
            5,
            200);

        log.info("Get broker configuration after enable: brokerTick {},  MaxMessageRate {}, MaxByteRate {}",
            pulsar.getConfiguration().getBrokerPublisherThrottlingTickTimeMillis(),
            pulsar.getConfiguration().getBrokerPublisherThrottlingMaxMessageRate(),
            pulsar.getConfiguration().getBrokerPublisherThrottlingMaxByteRate());

        Assert.assertNotEquals(topic.getBrokerPublishRateLimiter(), PublishRateLimiter.DISABLED_RATE_LIMITER);

        Producer prod = topic.getProducers().values().iterator().next();
        // reset counter
        prod.updateRates();
        int numMessage = 20;
        int msgBytes = 80;

        for (int i = 0; i < numMessage; i++) {
            producer.send(new byte[msgBytes]);
        }
        // calculate rates and due to throttling rate should be < total per-second
        prod.updateRates();
        double rateIn = prod.getStats().msgThroughputIn;
        log.info("1-st byte rate in: {}, total: {} ", rateIn, numMessage * msgBytes);
        assertTrue(rateIn < numMessage * msgBytes);

        // disable throttling
        admin.brokers()
            .updateDynamicConfiguration("brokerPublisherThrottlingMaxByteRate", Long.toString(0));
        retryStrategically((test) ->
                topic.getBrokerPublishRateLimiter().equals(PublishRateLimiter.DISABLED_RATE_LIMITER),
            5,
            200);

        log.info("Get broker configuration after disable: brokerTick {},  MaxMessageRate {}, MaxByteRate {}",
            pulsar.getConfiguration().getBrokerPublisherThrottlingTickTimeMillis(),
            pulsar.getConfiguration().getBrokerPublisherThrottlingMaxMessageRate(),
            pulsar.getConfiguration().getBrokerPublisherThrottlingMaxByteRate());

        Assert.assertEquals(topic.getBrokerPublishRateLimiter(), PublishRateLimiter.DISABLED_RATE_LIMITER);

        // reset counter
        prod.updateRates();
        for (int i = 0; i < numMessage; i++) {
            producer.send(new byte[msgBytes]);
        }

        prod.updateRates();
        rateIn = prod.getStats().msgThroughputIn;
        log.info("2-nd byte rate in: {}, total: {} ", rateIn, numMessage * msgBytes);
        assertTrue(rateIn > numMessage * msgBytes);

        producer.close();
    }

    /**
     * Verifies publish rate limiting by setting rate-limiting on number of publish bytes.
     * Broker publish throttle / topic publish throttle both enabled.
     * 1. set brokerByteRate > topicByteRate,
     * 2. with 1 topic, topicByteRate first take effective, then brokerByteRate take effective, the former rate is less.
     * 3. create 3 topics with same rate limit, publish should throttle by broker and topic limit.
     * @throws Exception
     */
    @Test
    public void testBrokerTopicPublishByteThrottling() throws Exception {
        log.info("-- Starting {} test --", methodName);

        final String namespace = "my-property/throttling_publish";
        final String topicName = "persistent://" + namespace + "/brokerTopicThrottlingByteBlock";

        admin.namespaces().createNamespace(namespace, Sets.newHashSet("test"));
        PublishRate topicPublishMsgRate = new PublishRate();
        long topicByteRate = 400;
        long brokerByteRate = 800;
        topicPublishMsgRate.publishThrottlingRateInByte = topicByteRate;

        // create producer and topic
        @Cleanup
        ProducerImpl<byte[]> producer = (ProducerImpl<byte[]>) pulsarClient.newProducer()
            .topic(topicName)
            .enableBatching(false)
            .maxPendingMessages(30000).create();
        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getTopicIfExists(topicName).get().get();
        // (1) verify both broker and topic limiter is disabled
        Assert.assertEquals(topic.getBrokerPublishRateLimiter(), PublishRateLimiter.DISABLED_RATE_LIMITER);
        Assert.assertEquals(topic.getTopicPublishRateLimiter(), PublishRateLimiter.DISABLED_RATE_LIMITER);

        // enable broker and topic throttling
        admin.namespaces().setPublishRate(namespace, topicPublishMsgRate);
        Awaitility.await().untilAsserted(() -> {
            assertNotEquals(topic.getTopicPublishRateLimiter(), PublishRateLimiter.DISABLED_RATE_LIMITER);
        });

        admin.brokers().updateDynamicConfiguration("brokerPublisherThrottlingMaxByteRate",
                Long.toString(brokerByteRate));
        Awaitility.await().untilAsserted(() -> {
            assertNotSame(topic.getBrokerPublishRateLimiter(), PublishRateLimiter.DISABLED_RATE_LIMITER);
        });

        log.info("Get broker configuration after enable: brokerTick {},  MaxMessageRate {}, MaxByteRate {}",
            pulsar.getConfiguration().getBrokerPublisherThrottlingTickTimeMillis(),
            pulsar.getConfiguration().getBrokerPublisherThrottlingMaxMessageRate(),
            pulsar.getConfiguration().getBrokerPublisherThrottlingMaxByteRate());

        Producer prod = topic.getProducers().values().iterator().next();
        // reset counter
        prod.updateRates();
        int numMessage = 40;
        int msgBytes = 80;

        for (int i = 0; i < numMessage; i++) {
            producer.send(new byte[msgBytes]);
        }
        // calculate rates and due to throttling rate should be < total per-second
        prod.updateRates();
        double rateIn = prod.getStats().msgThroughputIn;
        log.info("1-st byte rate in 1: {}, total: {} ", rateIn, numMessage * msgBytes);
        assertTrue(rateIn < numMessage * msgBytes);

        // create other topics, and count the produce rate, this should be throttle by both topic and broker limit.
        int topicNumber = 3;
        final String topicNameBase = "persistent://" + namespace + "/brokerTopicThrottlingByteBlock";
        List<ProducerImpl<byte[]>> producers = Lists.newArrayListWithExpectedSize(topicNumber);
        List<PersistentTopic> topics = Lists.newArrayListWithExpectedSize(topicNumber);

        for (int i = 0 ; i < topicNumber; i ++) {
            String iTopicName = topicNameBase + i;
            ProducerImpl<byte[]> iProducer = (ProducerImpl<byte[]>) pulsarClient.newProducer()
                .topic(iTopicName)
                .enableBatching(false)
                .maxPendingMessages(30000)
                .create();
            PersistentTopic iTopic = (PersistentTopic) pulsar.getBrokerService()
                .getTopicIfExists(iTopicName).get().get();

            producers.add(iProducer);
            topics.add(iTopic);

            // verify both broker and topic limiter is enabled
            Assert.assertNotEquals(iTopic.getBrokerPublishRateLimiter(), PublishRateLimiter.DISABLED_RATE_LIMITER);

            admin.namespaces().setPublishRate(namespace, topicPublishMsgRate);
            retryStrategically((test) ->
                    !iTopic.getTopicPublishRateLimiter().equals(PublishRateLimiter.DISABLED_RATE_LIMITER),
                5,
                200);
            Assert.assertNotEquals(iTopic.getTopicPublishRateLimiter(), PublishRateLimiter.DISABLED_RATE_LIMITER);
        }

        List<Callable<Void>> topicRatesCounter = Lists.newArrayListWithExpectedSize(3);
        @Cleanup("shutdownNow")
        ExecutorService executor = Executors.newSingleThreadExecutor();
        final AtomicDouble topicsRateIn = new AtomicDouble(0);
        final AtomicInteger index = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(topicNumber);

        for (int i = 0; i < topicNumber; i ++) {
            topicRatesCounter.add(() -> {
                int id = index.incrementAndGet();
                ProducerImpl<byte[]> iProducer = producers.get(id);
                PersistentTopic iTopic = topics.get(id);
                Producer iProd = iTopic.getProducers().values().iterator().next();
                // reset counter
                iProd.updateRates();

                for (int j = 0; j < numMessage; j++) {
                    iProducer.send(new byte[msgBytes]);
                }
                iProd.updateRates();
                topicsRateIn.addAndGet(iProd.getStats().msgThroughputIn);
                latch.countDown();
                return null;
            });
        }
        executor.invokeAll(topicRatesCounter);
        latch.await(2, TimeUnit.SECONDS);
        log.info("2-nd rate in: {}, total: {} ", topicsRateIn.get(), topicNumber * numMessage * msgBytes);
        assertTrue(rateIn < topicsRateIn.get());
        assertTrue(rateIn < topicNumber * numMessage * msgBytes);

        // disable topic throttling, it will use broker throttling, expected rateIn bigger than before.
        topicPublishMsgRate.publishThrottlingRateInByte = -1;
        admin.namespaces().setPublishRate(namespace, topicPublishMsgRate);

        Awaitility.await().untilAsserted(() ->
            assertEquals(topic.getTopicPublishRateLimiter(), PublishRateLimiter.DISABLED_RATE_LIMITER)
        );

        // reset counter
        prod.updateRates();
        for (int i = 0; i < numMessage; i++) {
            producer.send(new byte[msgBytes]);
        }
        // calculate rates and due to use broker throttling, expected rateIn bigger than topic throttling.
        prod.updateRates();
        double rateIn2 = prod.getStats().msgThroughputIn;
        log.info("3-rd byte rate in: {}, rate in 2: {},  total: {} ", rateIn, rateIn2, numMessage * msgBytes);
        assertTrue(rateIn < rateIn2);
        assertTrue(rateIn2 < numMessage * msgBytes);

        // disable broker throttling, expected no throttling.
        admin.brokers()
            .updateDynamicConfiguration("brokerPublisherThrottlingMaxByteRate", Long.toString(0));
        retryStrategically((test) ->
                topic.getBrokerPublishRateLimiter().equals(PublishRateLimiter.DISABLED_RATE_LIMITER),
            5,
            200);

        log.info("Get broker configuration after disable: brokerTick {},  MaxMessageRate {}, MaxByteRate {}",
            pulsar.getConfiguration().getBrokerPublisherThrottlingTickTimeMillis(),
            pulsar.getConfiguration().getBrokerPublisherThrottlingMaxMessageRate(),
            pulsar.getConfiguration().getBrokerPublisherThrottlingMaxByteRate());

        Assert.assertEquals(topic.getBrokerPublishRateLimiter(), PublishRateLimiter.DISABLED_RATE_LIMITER);

        // reset counter
        prod.updateRates();
        for (int i = 0; i < numMessage; i++) {
            producer.send(new byte[msgBytes]);
        }

        prod.updateRates();
        rateIn = prod.getStats().msgThroughputIn;
        log.info("4-th byte rate in: {}, total: {} ", rateIn, numMessage * msgBytes);
        assertTrue(rateIn > numMessage * msgBytes);
    }
}
