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

import static org.apache.pulsar.broker.stats.prometheus.PrometheusMetricsClient.parseMetrics;
import static org.testng.Assert.assertEquals;
import com.google.common.collect.Multimap;
import java.io.ByteArrayOutputStream;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.pulsar.PrometheusMetricsTestUtil;
import org.apache.pulsar.broker.stats.prometheus.PrometheusMetricsClient;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class BrokerDispatchRateLimiterTest extends BrokerTestBase {
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

    @Test
    public void testUpdateBrokerDispatchRateLimiter() throws PulsarAdminException {
        BrokerService service = pulsar.getBrokerService();
        assertEquals(service.getBrokerDispatchRateLimiter().getAvailableDispatchRateLimitOnByte(), -1L);
        assertEquals(service.getBrokerDispatchRateLimiter().getAvailableDispatchRateLimitOnMsg(), -1L);

        admin.brokers().updateDynamicConfiguration("dispatchThrottlingRateInByte", "100");
        Awaitility.await().untilAsserted(() ->
            assertEquals(service.getBrokerDispatchRateLimiter().getAvailableDispatchRateLimitOnByte(), 100L));
        assertEquals(service.getBrokerDispatchRateLimiter().getAvailableDispatchRateLimitOnMsg(), -1L);

        admin.brokers().updateDynamicConfiguration("dispatchThrottlingRateInMsg", "100");
        Awaitility.await().untilAsserted(() ->
            assertEquals(service.getBrokerDispatchRateLimiter().getAvailableDispatchRateLimitOnMsg(), 100L));
        assertEquals(service.getBrokerDispatchRateLimiter().getAvailableDispatchRateLimitOnMsg(), 100L);
    }

    @Test
    public void testBrokerDispatchThrottledMetrics() throws Exception {

        BrokerService service = pulsar.getBrokerService();
        admin.brokers().updateDynamicConfiguration("dispatchThrottlingRateInMsg", "10");
        admin.brokers().updateDynamicConfiguration("dispatchThrottlingRateInByte", "1024");
        Awaitility.await().untilAsserted(() ->
                assertEquals(service.getBrokerDispatchRateLimiter().getAvailableDispatchRateLimitOnMsg(), 10L));
        Awaitility.await().untilAsserted(() ->
                assertEquals(service.getBrokerDispatchRateLimiter().getAvailableDispatchRateLimitOnByte(), 1024L));

        final String topic = "persistent://" + newTopicName();
        final String subName = "my-sub";

        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topic)
                .enableBatching(false)
                .create();

        @Cleanup
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionType(SubscriptionType.Exclusive)
                .subscriptionName(subName)
                .subscribe();

        for (int i = 0; i < 100; i++) {
            producer.newMessage().value(UUID.randomUUID().toString()).send();
        }

        for (int i = 0; i < 100; i++) {
            Message<String> message = consumer.receive(100, TimeUnit.SECONDS);
            Assert.assertNotNull(message);
            consumer.acknowledge(message);
        }

        // Assert broker metrics
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        PrometheusMetricsTestUtil.generate(pulsar, true, false, false, output);
        String metricsStr = output.toString();
        Multimap<String, PrometheusMetricsClient.Metric> metrics = parseMetrics(metricsStr);

        // Assert subscription metrics reason by broker limit
        Collection<PrometheusMetricsClient.Metric> subscriptionDispatchThrottledMsgCountMetrics =
                metrics.get("pulsar_subscription_dispatch_throttled_msg_events");
        Assert.assertFalse(subscriptionDispatchThrottledMsgCountMetrics.isEmpty());
        double subscriptionDispatchThrottledMsgCount = subscriptionDispatchThrottledMsgCountMetrics.stream()
                .filter(m -> m.tags.get("subscription").equals(subName)
                        && m.tags.get("topic").equals(topic) && m.tags.get("reason").equals("broker"))
                .mapToDouble(m-> m.value).sum();
        Assert.assertTrue(subscriptionDispatchThrottledMsgCount > 0);
        double brokerAllDispatchThrottledMsgCount = subscriptionDispatchThrottledMsgCountMetrics.stream()
                .filter(m -> m.tags.get("reason").equals("broker"))
                .mapToDouble(m-> m.value).sum();
        Assert.assertEquals(subscriptionDispatchThrottledMsgCount, brokerAllDispatchThrottledMsgCount);

        Collection<PrometheusMetricsClient.Metric> subscriptionDispatchThrottledBytesCountMetrics =
                metrics.get("pulsar_subscription_dispatch_throttled_bytes_events");
        Assert.assertFalse(subscriptionDispatchThrottledBytesCountMetrics.isEmpty());
        double subscriptionDispatchThrottledBytesCount = subscriptionDispatchThrottledBytesCountMetrics.stream()
                .filter(m -> m.tags.get("subscription").equals(subName)
                        && m.tags.get("topic").equals(topic) && m.tags.get("reason").equals("broker"))
                .mapToDouble(m-> m.value).sum();
        Assert.assertTrue(subscriptionDispatchThrottledBytesCount > 0);
        double brokerAllDispatchThrottledBytesCount = subscriptionDispatchThrottledBytesCountMetrics.stream()
                .filter(m -> m.tags.get("reason").equals("broker"))
                .mapToDouble(m-> m.value).sum();
        Assert.assertEquals(subscriptionDispatchThrottledBytesCount, brokerAllDispatchThrottledBytesCount);
    }

}
