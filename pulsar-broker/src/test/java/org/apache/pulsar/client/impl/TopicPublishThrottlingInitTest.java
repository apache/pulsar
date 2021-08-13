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

import static org.testng.Assert.assertTrue;
import com.google.common.collect.Sets;
import org.apache.pulsar.broker.service.Producer;
import org.apache.pulsar.broker.service.PublishRateLimiter;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.awaitility.Awaitility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker-impl")
public class TopicPublishThrottlingInitTest extends ProducerConsumerBase {
    private static final Logger log = LoggerFactory.getLogger(TopicPublishThrottlingInitTest.class);

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        this.conf.setClusterName("test");
        this.conf.setTopicPublisherThrottlingTickTimeMillis(1);
        // set these 2 config to make broker throttling enabled when start.
        this.conf.setBrokerPublisherThrottlingTickTimeMillis(1);
        this.conf.setBrokerPublisherThrottlingMaxMessageRate(10);
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
     * Verifies Broker publish rate limiting enabled by broker conf.
     * Broker publish throttle enabled / topic publish throttle disabled
     * @throws Exception
     */
    @Test
    public void testBrokerPublishMessageThrottlingInit() throws Exception {
        log.info("-- Starting {} test --", methodName);

        final String namespace = "my-property/throttling_publish_init";
        final String topicName = "persistent://" + namespace + "/brokerThrottlingMessageBlock";

        admin.namespaces().createNamespace(namespace, Sets.newHashSet("test"));

        // create producer and topic
        ProducerImpl<byte[]> producer = (ProducerImpl<byte[]>) pulsarClient.newProducer()
            .topic(topicName)
            .enableBatching(false)
            .maxPendingMessages(30000).create();
        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getTopicIfExists(topicName).get().get();
        // (1) verify message-rate is initialized when value configured in broker
        Assert.assertNotEquals(topic.getBrokerPublishRateLimiter(), PublishRateLimiter.DISABLED_RATE_LIMITER);

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
        Awaitility.await().untilAsserted(() ->
                Assert.assertEquals(topic.getBrokerPublishRateLimiter(), PublishRateLimiter.DISABLED_RATE_LIMITER));

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

}
