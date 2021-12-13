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
package org.apache.pulsar.broker.stats;

import static com.google.common.base.Preconditions.checkArgument;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import com.google.common.base.MoreObjects;
import com.google.common.base.Splitter;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import io.jsonwebtoken.SignatureAlgorithm;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.crypto.SecretKey;
import javax.naming.AuthenticationException;
import lombok.Cleanup;
import org.apache.commons.io.IOUtils;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.authentication.AuthenticationProviderToken;
import org.apache.pulsar.broker.authentication.utils.AuthTokenUtils;
import org.apache.pulsar.broker.service.BrokerTestBase;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentMessageExpiryMonitor;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.stats.prometheus.PrometheusMetricsGenerator;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.compaction.Compactor;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "flaky")
public class PrometheusMetricsTest extends BrokerTestBase {

    @BeforeMethod(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        super.baseSetup();
        AuthenticationProviderToken.resetMetrics();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testMetricsTopicCount() throws Exception {
        String ns1 = "prop/ns-abc1";
        String ns2 = "prop/ns-abc2";
        admin.namespaces().createNamespace(ns1);
        admin.namespaces().createNamespace(ns2);
        String baseTopic1 = "persistent://" + ns1 + "/testMetricsTopicCount";
        String baseTopic2 = "persistent://" + ns2 + "/testMetricsTopicCount";
        for (int i = 0; i < 6; i++) {
            admin.topics().createNonPartitionedTopic(baseTopic1 + UUID.randomUUID());
        }
        for (int i = 0; i < 3; i++) {
            admin.topics().createNonPartitionedTopic(baseTopic2 + UUID.randomUUID());
        }
        Thread.sleep(ASYNC_EVENT_COMPLETION_WAIT);
        @Cleanup
        ByteArrayOutputStream statsOut = new ByteArrayOutputStream();
        PrometheusMetricsGenerator.generate(pulsar, true, false, false, statsOut);
        String metricsStr = statsOut.toString();
        Multimap<String, Metric> metrics = parseMetrics(metricsStr);
        Collection<Metric> metric = metrics.get("pulsar_topics_count");
        metric.forEach(item -> {
            if (ns1.equals(item.tags.get("namespace"))) {
                assertEquals(item.value, 6.0);
            }
            if (ns2.equals(item.tags.get("namespace"))) {
                assertEquals(item.value, 3.0);
            }
        });
    }

    @Test
    public void testMetricsAvgMsgSize2() throws Exception {
        String ns1 = "prop/ns-abc1";
        admin.namespaces().createNamespace(ns1, 1);
        String baseTopic1 = "persistent://" + ns1 + "/testMetricsTopicCount";
        String topicName = baseTopic1 + UUID.randomUUID();
        Producer producer = pulsarClient.newProducer().producerName("my-pub")
                .topic(topicName).create();
        PersistentTopic persistentTopic = (PersistentTopic) pulsar.getBrokerService()
                .getTopic(topicName, false).get().get();
        org.apache.pulsar.broker.service.Producer producerInServer = persistentTopic.getProducers().get("my-pub");
        producerInServer.getStats().msgRateIn = 10;
        producerInServer.getStats().msgThroughputIn = 100;
        @Cleanup
        ByteArrayOutputStream statsOut = new ByteArrayOutputStream();
        PrometheusMetricsGenerator.generate(pulsar, true, false, true, statsOut);
        String metricsStr = statsOut.toString();
        Multimap<String, Metric> metrics = parseMetrics(metricsStr);
        assertTrue(metrics.containsKey("pulsar_average_msg_size"));
        assertEquals(metrics.get("pulsar_average_msg_size").size(), 1);
        Collection<Metric> avgMsgSizes = metrics.get("pulsar_average_msg_size");
        avgMsgSizes.forEach(item -> {
            if (ns1.equals(item.tags.get("namespace"))) {
                assertEquals(item.value, 10);
            }
        });
        producer.close();
    }

    @Test
    public void testPerTopicStats() throws Exception {
        Producer<byte[]> p1 = pulsarClient.newProducer().topic("persistent://my-property/use/my-ns/my-topic1").create();
        Producer<byte[]> p2 = pulsarClient.newProducer().topic("persistent://my-property/use/my-ns/my-topic2").create();

        Consumer<byte[]> c1 = pulsarClient.newConsumer()
                .topic("persistent://my-property/use/my-ns/my-topic1")
                .subscriptionName("test")
                .subscribe();

        Consumer<byte[]> c2 = pulsarClient.newConsumer()
                .topic("persistent://my-property/use/my-ns/my-topic2")
                .subscriptionName("test")
                .subscribe();

        final int messages = 10;

        for (int i = 0; i < messages; i++) {
            String message = "my-message-" + i;
            p1.send(message.getBytes());
            p2.send(message.getBytes());
        }

        for (int i = 0; i < messages; i++) {
            c1.acknowledge(c1.receive());
            c2.acknowledge(c2.receive());
        }

        ByteArrayOutputStream statsOut = new ByteArrayOutputStream();
        PrometheusMetricsGenerator.generate(pulsar, true, false, false, statsOut);
        String metricsStr = statsOut.toString();
        Multimap<String, Metric> metrics = parseMetrics(metricsStr);

        metrics.entries().forEach(e -> {
            System.out.println(e.getKey() + ": " + e.getValue());
        });

        // There should be 2 metrics with different tags for each topic
        List<Metric> cm = (List<Metric>) metrics.get("pulsar_storage_write_latency_le_1");
        assertEquals(cm.size(), 2);
        assertEquals(cm.get(0).tags.get("topic"), "persistent://my-property/use/my-ns/my-topic2");
        assertEquals(cm.get(0).tags.get("namespace"), "my-property/use/my-ns");
        assertEquals(cm.get(1).tags.get("topic"), "persistent://my-property/use/my-ns/my-topic1");
        assertEquals(cm.get(1).tags.get("namespace"), "my-property/use/my-ns");

        cm = (List<Metric>) metrics.get("pulsar_producers_count");
        assertEquals(cm.size(), 3);
        assertEquals(cm.get(1).tags.get("topic"), "persistent://my-property/use/my-ns/my-topic2");
        assertEquals(cm.get(1).tags.get("namespace"), "my-property/use/my-ns");
        assertEquals(cm.get(2).tags.get("topic"), "persistent://my-property/use/my-ns/my-topic1");
        assertEquals(cm.get(2).tags.get("namespace"), "my-property/use/my-ns");

        cm = (List<Metric>) metrics.get("topic_load_times_count");
        assertEquals(cm.size(), 1);
        assertEquals(cm.get(0).tags.get("cluster"), "test");

        cm = (List<Metric>) metrics.get("pulsar_in_bytes_total");
        assertEquals(cm.size(), 2);
        assertEquals(cm.get(0).tags.get("topic"), "persistent://my-property/use/my-ns/my-topic2");
        assertEquals(cm.get(0).tags.get("namespace"), "my-property/use/my-ns");
        assertEquals(cm.get(1).tags.get("topic"), "persistent://my-property/use/my-ns/my-topic1");
        assertEquals(cm.get(1).tags.get("namespace"), "my-property/use/my-ns");

        cm = (List<Metric>) metrics.get("pulsar_in_messages_total");
        assertEquals(cm.size(), 2);
        assertEquals(cm.get(0).tags.get("topic"), "persistent://my-property/use/my-ns/my-topic2");
        assertEquals(cm.get(0).tags.get("namespace"), "my-property/use/my-ns");
        assertEquals(cm.get(1).tags.get("topic"), "persistent://my-property/use/my-ns/my-topic1");
        assertEquals(cm.get(1).tags.get("namespace"), "my-property/use/my-ns");

        cm = (List<Metric>) metrics.get("pulsar_out_bytes_total");
        assertEquals(cm.size(), 2);
        assertEquals(cm.get(0).tags.get("topic"), "persistent://my-property/use/my-ns/my-topic2");
        assertEquals(cm.get(0).tags.get("namespace"), "my-property/use/my-ns");
        assertEquals(cm.get(0).tags.get("subscription"), "test");
        assertEquals(cm.get(1).tags.get("topic"), "persistent://my-property/use/my-ns/my-topic1");
        assertEquals(cm.get(1).tags.get("namespace"), "my-property/use/my-ns");
        assertEquals(cm.get(1).tags.get("subscription"), "test");

        cm = (List<Metric>) metrics.get("pulsar_out_messages_total");
        assertEquals(cm.size(), 2);
        assertEquals(cm.get(0).tags.get("topic"), "persistent://my-property/use/my-ns/my-topic2");
        assertEquals(cm.get(0).tags.get("namespace"), "my-property/use/my-ns");
        assertEquals(cm.get(0).tags.get("subscription"), "test");
        assertEquals(cm.get(1).tags.get("topic"), "persistent://my-property/use/my-ns/my-topic1");
        assertEquals(cm.get(1).tags.get("namespace"), "my-property/use/my-ns");
        assertEquals(cm.get(1).tags.get("subscription"), "test");

        p1.close();
        p2.close();
        c1.close();
        c2.close();
    }

    @Test
    public void testPerTopicExpiredStat() throws Exception {
        String ns = "prop/ns-abc1";
        admin.namespaces().createNamespace(ns);
        String topic1 = "persistent://" + ns + "/testPerTopicExpiredStat1";
        String topic2 = "persistent://" + ns + "/testPerTopicExpiredStat2";
        List<String> topicList = Arrays.asList(topic2, topic1);
        Producer<byte[]> p1 = pulsarClient.newProducer().topic(topic1).create();
        Producer<byte[]> p2 = pulsarClient.newProducer().topic(topic2).create();
        final String subName = "test";
        for (String topic : topicList) {
            pulsarClient.newConsumer()
                    .topic(topic)
                    .subscriptionName(subName)
                    .subscribe().close();
        }

        final int messages = 10;

        for (int i = 0; i < messages; i++) {
            String message = "my-message-" + i;
            p1.send(message.getBytes());
            p2.send(message.getBytes());
        }

        p1.close();
        p2.close();
        // Let the message expire
        for (String topic : topicList) {
            PersistentTopic persistentTopic = (PersistentTopic) pulsar.getBrokerService().getTopicIfExists(topic).get().get();
            persistentTopic.getBrokerService().getPulsar().getConfiguration().setTtlDurationDefaultInSeconds(-1);
            persistentTopic.getHierarchyTopicPolicies().getMessageTTLInSeconds().updateBrokerValue(-1);
        }
        pulsar.getBrokerService().forEachTopic(Topic::checkMessageExpiry);
        //wait for checkMessageExpiry
        PersistentSubscription sub = (PersistentSubscription)
                pulsar.getBrokerService().getTopicIfExists(topic1).get().get().getSubscription(subName);
        PersistentSubscription sub2 = (PersistentSubscription)
                pulsar.getBrokerService().getTopicIfExists(topic2).get().get().getSubscription(subName);
        Awaitility.await().until(() -> sub.getExpiredMessageRate() != 0.0);
        Awaitility.await().until(() -> sub2.getExpiredMessageRate() != 0.0);

        ByteArrayOutputStream statsOut = new ByteArrayOutputStream();
        PrometheusMetricsGenerator.generate(pulsar, true, false, false, statsOut);
        String metricsStr = statsOut.toString();
        Multimap<String, Metric> metrics = parseMetrics(metricsStr);
        // There should be 2 metrics with different tags for each topic
        List<Metric> cm = (List<Metric>) metrics.get("pulsar_subscription_last_expire_timestamp");
        assertEquals(cm.size(), 2);
        assertEquals(cm.get(0).tags.get("topic"), topic2);
        assertEquals(cm.get(0).tags.get("namespace"), ns);
        assertEquals(cm.get(1).tags.get("topic"), topic1);
        assertEquals(cm.get(1).tags.get("namespace"), ns);

        //check value
        Field field = PersistentSubscription.class.getDeclaredField("lastExpireTimestamp");
        field.setAccessible(true);
        for (int i = 0; i < topicList.size(); i++) {
            PersistentSubscription subscription = (PersistentSubscription) pulsar.getBrokerService()
                    .getTopicIfExists(topicList.get(i)).get().get().getSubscription(subName);
            assertEquals((long) field.get(subscription), (long) cm.get(i).value);
        }

        cm = (List<Metric>) metrics.get("pulsar_subscription_msg_rate_expired");
        assertEquals(cm.size(), 2);
        assertEquals(cm.get(0).tags.get("topic"), topic2);
        assertEquals(cm.get(0).tags.get("namespace"), ns);
        assertEquals(cm.get(1).tags.get("topic"), topic1);
        assertEquals(cm.get(1).tags.get("namespace"), ns);
        //check value
        field = PersistentSubscription.class.getDeclaredField("expiryMonitor");
        field.setAccessible(true);
        NumberFormat nf = NumberFormat.getNumberInstance();
        nf.setMaximumFractionDigits(3);
        nf.setRoundingMode(RoundingMode.DOWN);
        for (int i = 0; i < topicList.size(); i++) {
            PersistentSubscription subscription = (PersistentSubscription) pulsar.getBrokerService()
                    .getTopicIfExists(topicList.get(i)).get().get().getSubscription(subName);
            PersistentMessageExpiryMonitor monitor = (PersistentMessageExpiryMonitor) field.get(subscription);
            assertEquals(Double.valueOf(nf.format(monitor.getMessageExpiryRate())).doubleValue(), cm.get(i).value);
        }

        cm = (List<Metric>) metrics.get("pulsar_subscription_total_msg_expired");
        assertEquals(cm.size(), 2);
        assertEquals(cm.get(0).tags.get("topic"), topic2);
        assertEquals(cm.get(0).tags.get("namespace"), ns);
        assertEquals(cm.get(1).tags.get("topic"), topic1);
        assertEquals(cm.get(1).tags.get("namespace"), ns);
        //check value
        for (int i = 0; i < topicList.size(); i++) {
            assertEquals(messages, (long) cm.get(i).value);
        }

    }

    @Test
    public void testPerNamespaceStats() throws Exception {
        Producer<byte[]> p1 = pulsarClient.newProducer().topic("persistent://my-property/use/my-ns/my-topic1").create();
        Producer<byte[]> p2 = pulsarClient.newProducer().topic("persistent://my-property/use/my-ns/my-topic2").create();

        Consumer<byte[]> c1 = pulsarClient.newConsumer()
                .topic("persistent://my-property/use/my-ns/my-topic1")
                .subscriptionName("test")
                .subscribe();

        Consumer<byte[]> c2 = pulsarClient.newConsumer()
                .topic("persistent://my-property/use/my-ns/my-topic2")
                .subscriptionName("test")
                .subscribe();

        final int messages = 10;

        for (int i = 0; i < messages; i++) {
            String message = "my-message-" + i;
            p1.send(message.getBytes());
            p2.send(message.getBytes());
        }

        for (int i = 0; i < messages; i++) {
            c1.acknowledge(c1.receive());
            c2.acknowledge(c2.receive());
        }

        ByteArrayOutputStream statsOut = new ByteArrayOutputStream();
        PrometheusMetricsGenerator.generate(pulsar, false, false, false, statsOut);
        String metricsStr = statsOut.toString();

        Multimap<String, Metric> metrics = parseMetrics(metricsStr);

        metrics.entries().forEach(e -> {
            System.out.println(e.getKey() + ": " + e.getValue());
        });

        // There should be 1 metric aggregated per namespace
        List<Metric> cm = (List<Metric>) metrics.get("pulsar_storage_write_latency_le_1");
        assertEquals(cm.size(), 1);
        assertNull(cm.get(0).tags.get("topic"));
        assertEquals(cm.get(0).tags.get("namespace"), "my-property/use/my-ns");

        cm = (List<Metric>) metrics.get("pulsar_producers_count");
        assertEquals(cm.size(), 2);
        assertNull(cm.get(1).tags.get("topic"));
        assertEquals(cm.get(1).tags.get("namespace"), "my-property/use/my-ns");

        cm = (List<Metric>) metrics.get("pulsar_in_bytes_total");
        assertEquals(cm.size(), 1);
        assertEquals(cm.get(0).tags.get("namespace"), "my-property/use/my-ns");

        cm = (List<Metric>) metrics.get("pulsar_in_messages_total");
        assertEquals(cm.size(), 1);
        assertEquals(cm.get(0).tags.get("namespace"), "my-property/use/my-ns");

        cm = (List<Metric>) metrics.get("pulsar_out_bytes_total");
        assertEquals(cm.size(), 1);
        assertEquals(cm.get(0).tags.get("namespace"), "my-property/use/my-ns");

        cm = (List<Metric>) metrics.get("pulsar_out_messages_total");
        assertEquals(cm.size(), 1);
        assertEquals(cm.get(0).tags.get("namespace"), "my-property/use/my-ns");

        p1.close();
        p2.close();
        c1.close();
        c2.close();
    }

    @Test
    public void testPerProducerStats() throws Exception {
        Producer<byte[]> p1 = pulsarClient.newProducer().topic("persistent://my-property/use/my-ns/my-topic1")
                .producerName("producer1").create();
        Producer<byte[]> p2 = pulsarClient.newProducer().topic("persistent://my-property/use/my-ns/my-topic2")
                .producerName("producer2").create();

        Consumer<byte[]> c1 = pulsarClient.newConsumer()
                .topic("persistent://my-property/use/my-ns/my-topic1")
                .subscriptionName("Test")
                .subscribe();

        Consumer<byte[]> c2 = pulsarClient.newConsumer()
                .topic("persistent://my-property/use/my-ns/my-topic2")
                .subscriptionName("Test")
                .subscribe();

        final int messages = 10;

        for (int i = 0; i < messages; i++) {
            String message = "my-message-" + i;
            p1.send(message.getBytes());
            p2.send(message.getBytes());
        }

        for (int i = 0; i < messages; i++) {
            c1.acknowledge(c1.receive());
            c2.acknowledge(c2.receive());
        }

        ByteArrayOutputStream statsOut = new ByteArrayOutputStream();
        PrometheusMetricsGenerator.generate(pulsar, true, false, true, statsOut);
        String metricsStr = statsOut.toString();

        Multimap<String, Metric> metrics = parseMetrics(metricsStr);

        metrics.entries().forEach(e -> {
            System.out.println(e.getKey() + ": " + e.getValue());
        });

        List<Metric> cm = (List<Metric>) metrics.get("pulsar_producer_msg_rate_in");
        assertEquals(cm.size(), 2);
        assertEquals(cm.get(0).tags.get("namespace"), "my-property/use/my-ns");
        assertEquals(cm.get(0).tags.get("topic"), "persistent://my-property/use/my-ns/my-topic2");
        assertEquals(cm.get(0).tags.get("producer_name"), "producer2");
        assertEquals(cm.get(0).tags.get("producer_id"), "1");

        assertEquals(cm.get(1).tags.get("namespace"), "my-property/use/my-ns");
        assertEquals(cm.get(1).tags.get("topic"), "persistent://my-property/use/my-ns/my-topic1");
        assertEquals(cm.get(1).tags.get("producer_name"), "producer1");
        assertEquals(cm.get(1).tags.get("producer_id"), "0");

        cm = (List<Metric>) metrics.get("pulsar_producer_msg_throughput_in");
        assertEquals(cm.size(), 2);
        assertEquals(cm.get(0).tags.get("namespace"), "my-property/use/my-ns");
        assertEquals(cm.get(0).tags.get("topic"), "persistent://my-property/use/my-ns/my-topic2");
        assertEquals(cm.get(0).tags.get("producer_name"), "producer2");
        assertEquals(cm.get(0).tags.get("producer_id"), "1");

        assertEquals(cm.get(1).tags.get("namespace"), "my-property/use/my-ns");
        assertEquals(cm.get(1).tags.get("topic"), "persistent://my-property/use/my-ns/my-topic1");
        assertEquals(cm.get(1).tags.get("producer_name"), "producer1");
        assertEquals(cm.get(1).tags.get("producer_id"), "0");

        p1.close();
        p2.close();
        c1.close();
        c2.close();
    }

    @Test
    public void testPerConsumerStats() throws Exception {
        Producer<byte[]> p1 = pulsarClient.newProducer().topic("persistent://my-property/use/my-ns/my-topic1").create();
        Producer<byte[]> p2 = pulsarClient.newProducer().topic("persistent://my-property/use/my-ns/my-topic2").create();

        Consumer<byte[]> c1 = pulsarClient.newConsumer()
                .topic("persistent://my-property/use/my-ns/my-topic1")
                .subscriptionName("test")
                .subscribe();

        Consumer<byte[]> c2 = pulsarClient.newConsumer()
                .topic("persistent://my-property/use/my-ns/my-topic2")
                .subscriptionName("test")
                .subscribe();

        final int messages = 10;

        for (int i = 0; i < messages; i++) {
            String message = "my-message-" + i;
            p1.send(message.getBytes());
            p2.send(message.getBytes());
        }

        for (int i = 0; i < messages; i++) {
            c1.acknowledge(c1.receive());
            c2.acknowledge(c2.receive());
        }

        ByteArrayOutputStream statsOut = new ByteArrayOutputStream();
        PrometheusMetricsGenerator.generate(pulsar, true, true, false, statsOut);
        String metricsStr = statsOut.toString();

        Multimap<String, Metric> metrics = parseMetrics(metricsStr);

        metrics.entries().forEach(e -> {
            System.out.println(e.getKey() + ": " + e.getValue());
        });

        // There should be 1 metric aggregated per namespace
        List<Metric> cm = (List<Metric>) metrics.get("pulsar_out_bytes_total");
        assertEquals(cm.size(), 4);
        assertEquals(cm.get(0).tags.get("namespace"), "my-property/use/my-ns");
        assertEquals(cm.get(0).tags.get("topic"), "persistent://my-property/use/my-ns/my-topic2");
        assertEquals(cm.get(0).tags.get("subscription"), "test");

        assertEquals(cm.get(1).tags.get("namespace"), "my-property/use/my-ns");
        assertEquals(cm.get(1).tags.get("topic"), "persistent://my-property/use/my-ns/my-topic2");
        assertEquals(cm.get(1).tags.get("subscription"), "test");
        assertEquals(cm.get(1).tags.get("consumer_id"), "1");

        assertEquals(cm.get(2).tags.get("namespace"), "my-property/use/my-ns");
        assertEquals(cm.get(2).tags.get("topic"), "persistent://my-property/use/my-ns/my-topic1");
        assertEquals(cm.get(2).tags.get("subscription"), "test");

        assertEquals(cm.get(3).tags.get("namespace"), "my-property/use/my-ns");
        assertEquals(cm.get(3).tags.get("topic"), "persistent://my-property/use/my-ns/my-topic1");
        assertEquals(cm.get(3).tags.get("subscription"), "test");
        assertEquals(cm.get(3).tags.get("consumer_id"), "0");

        cm = (List<Metric>) metrics.get("pulsar_out_messages_total");
        assertEquals(cm.size(), 4);
        assertEquals(cm.get(0).tags.get("namespace"), "my-property/use/my-ns");
        assertEquals(cm.get(0).tags.get("topic"), "persistent://my-property/use/my-ns/my-topic2");
        assertEquals(cm.get(0).tags.get("subscription"), "test");

        assertEquals(cm.get(1).tags.get("namespace"), "my-property/use/my-ns");
        assertEquals(cm.get(1).tags.get("topic"), "persistent://my-property/use/my-ns/my-topic2");
        assertEquals(cm.get(1).tags.get("subscription"), "test");
        assertEquals(cm.get(1).tags.get("consumer_id"), "1");

        assertEquals(cm.get(2).tags.get("namespace"), "my-property/use/my-ns");
        assertEquals(cm.get(2).tags.get("topic"), "persistent://my-property/use/my-ns/my-topic1");
        assertEquals(cm.get(2).tags.get("subscription"), "test");

        assertEquals(cm.get(3).tags.get("namespace"), "my-property/use/my-ns");
        assertEquals(cm.get(3).tags.get("topic"), "persistent://my-property/use/my-ns/my-topic1");
        assertEquals(cm.get(3).tags.get("subscription"), "test");
        assertEquals(cm.get(3).tags.get("consumer_id"), "0");

        p1.close();
        p2.close();
        c1.close();
        c2.close();
    }

    /** Checks for duplicate type definitions for a metric in the Prometheus metrics output. If the Prometheus parser
     finds a TYPE definition for the same metric more than once, it errors out:
     https://github.com/prometheus/prometheus/blob/f04b1b5559a80a4fd1745cf891ce392a056460c9/vendor/github.com/prometheus/common/expfmt/text_parse.go#L499-L502
     This can happen when including topic metrics, since the same metric is reported multiple times with different labels. For example:

     # TYPE pulsar_subscriptions_count gauge
     pulsar_subscriptions_count{cluster="standalone"} 0 1556372982118
     pulsar_subscriptions_count{cluster="standalone",namespace="public/functions",topic="persistent://public/functions/metadata"} 1.0 1556372982118
     pulsar_subscriptions_count{cluster="standalone",namespace="public/functions",topic="persistent://public/functions/coordinate"} 1.0 1556372982118
     pulsar_subscriptions_count{cluster="standalone",namespace="public/functions",topic="persistent://public/functions/assignments"} 1.0 1556372982118

     **/
    // Running the test twice to make sure types are present when generated multiple times
    @Test(invocationCount = 2)
    public void testDuplicateMetricTypeDefinitions() throws Exception {
        Producer<byte[]> p1 = pulsarClient.newProducer().topic("persistent://my-property/use/my-ns/my-topic1").create();
        Producer<byte[]> p2 = pulsarClient.newProducer().topic("persistent://my-property/use/my-ns/my-topic2").create();
        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            p1.send(message.getBytes());
            p2.send(message.getBytes());
        }

        ByteArrayOutputStream statsOut = new ByteArrayOutputStream();
        PrometheusMetricsGenerator.generate(pulsar, false, false, false, statsOut);
        String metricsStr = statsOut.toString();

        Map<String, String> typeDefs = new HashMap<>();
        Map<String, String> metricNames = new HashMap<>();

        Pattern typePattern = Pattern.compile("^#\\s+TYPE\\s+(\\w+)\\s+(\\w+)");
        Pattern metricNamePattern = Pattern.compile("^(\\w+)\\{.+");

        Splitter.on("\n").split(metricsStr).forEach(line -> {
            if (line.isEmpty()) {
                return;
            }
            if (line.startsWith("#")) {
                // Check for duplicate type definitions
                Matcher typeMatcher = typePattern.matcher(line);
                checkArgument(typeMatcher.matches());
                String metricName = typeMatcher.group(1);
                String type = typeMatcher.group(2);

                // From https://github.com/prometheus/docs/blob/master/content/docs/instrumenting/exposition_formats.md
                // "Only one TYPE line may exist for a given metric name."
                if (!typeDefs.containsKey(metricName)) {
                    typeDefs.put(metricName, type);
                } else {
                    fail("Duplicate type definition found for TYPE definition " + metricName);
                    System.out.println(metricsStr);

                }
                // From https://github.com/prometheus/docs/blob/master/content/docs/instrumenting/exposition_formats.md
                // "The TYPE line for a metric name must appear before the first sample is reported for that metric name."
                if (metricNames.containsKey(metricName)) {
                    System.out.println(metricsStr);
                    fail("TYPE definition for " + metricName + " appears after first sample");

                }
            } else {
                Matcher metricMatcher = metricNamePattern.matcher(line);
                checkArgument(metricMatcher.matches());
                String metricName = metricMatcher.group(1);
                metricNames.put(metricName, metricName);
            }
        });

        // Metrics with no type definition
        for (String metricName : metricNames.keySet()) {

            if (!typeDefs.containsKey(metricName)) {
                // This may be OK if this is a _sum or _count metric from a summary
                if (metricName.endsWith("_sum")) {
                    String summaryMetricName = metricName.substring(0, metricName.indexOf("_sum"));
                    if (!typeDefs.containsKey(summaryMetricName)) {
                        fail("Metric " + metricName + " does not have a corresponding summary type definition");
                    }
                } else if (metricName.endsWith("_count")) {
                    String summaryMetricName = metricName.substring(0, metricName.indexOf("_count"));
                    if (!typeDefs.containsKey(summaryMetricName)) {
                        fail("Metric " + metricName + " does not have a corresponding summary type definition");
                    }
                } else if (metricName.endsWith("_bucket")) {
                    String summaryMetricName = metricName.substring(0, metricName.indexOf("_bucket"));
                    if (!typeDefs.containsKey(summaryMetricName)) {
                        fail("Metric " + metricName + " does not have a corresponding summary type definition");
                    }
                } else {
                    fail("Metric " + metricName + " does not have a type definition");
                }

            }
        }

        p1.close();
        p2.close();
    }

    @Test
    public void testManagedLedgerCacheStats() throws Exception {
        Producer<byte[]> p1 = pulsarClient.newProducer().topic("persistent://my-property/use/my-ns/my-topic1").create();
        Producer<byte[]> p2 = pulsarClient.newProducer().topic("persistent://my-property/use/my-ns/my-topic2").create();
        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            p1.send(message.getBytes());
            p2.send(message.getBytes());
        }

        ByteArrayOutputStream statsOut = new ByteArrayOutputStream();
        PrometheusMetricsGenerator.generate(pulsar, false, false, false, statsOut);
        String metricsStr = statsOut.toString();

        Multimap<String, Metric> metrics = parseMetrics(metricsStr);

        metrics.entries().forEach(e ->
                System.out.println(e.getKey() + ": " + e.getValue())
        );

        List<Metric> cm = (List<Metric>) metrics.get("pulsar_ml_cache_evictions");
        assertEquals(cm.size(), 1);
        assertEquals(cm.get(0).tags.get("cluster"), "test");

        cm = (List<Metric>) metrics.get("pulsar_ml_cache_hits_rate");
        assertEquals(cm.size(), 1);
        assertEquals(cm.get(0).tags.get("cluster"), "test");

        p1.close();
        p2.close();
    }

    @Test
    public void testManagedLedgerStats() throws Exception {
        Producer<byte[]> p1 = pulsarClient.newProducer().topic("persistent://my-property/use/my-ns/my-topic1").create();
        Producer<byte[]> p2 = pulsarClient.newProducer().topic("persistent://my-property/use/my-ns/my-topic2").create();
        Producer<byte[]> p3 = pulsarClient.newProducer().topic("persistent://my-property/use/my-ns2/my-topic1").create();
        Producer<byte[]> p4 = pulsarClient.newProducer().topic("persistent://my-property/use/my-ns2/my-topic2").create();
        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            p1.send(message.getBytes());
            p2.send(message.getBytes());
            p3.send(message.getBytes());
            p4.send(message.getBytes());
        }

        ByteArrayOutputStream statsOut = new ByteArrayOutputStream();
        PrometheusMetricsGenerator.generate(pulsar, false, false, false, statsOut);
        String metricsStr = statsOut.toString();

        Multimap<String, Metric> metrics = parseMetrics(metricsStr);

        metrics.entries().forEach(e ->
                System.out.println(e.getKey() + ": " + e.getValue())
        );

        Map<String, String> typeDefs = new HashMap<>();
        Map<String, String> metricNames = new HashMap<>();

        Pattern typePattern = Pattern.compile("^#\\s+TYPE\\s+(\\w+)\\s+(\\w+)");
        Pattern metricNamePattern = Pattern.compile("^(\\w+)\\{.+");

        Splitter.on("\n").split(metricsStr).forEach(line -> {
            if (line.isEmpty()) {
                return;
            }
            if (line.startsWith("#")) {
                // Check for duplicate type definitions
                Matcher typeMatcher = typePattern.matcher(line);
                checkArgument(typeMatcher.matches());
                String metricName = typeMatcher.group(1);
                String type = typeMatcher.group(2);

                // From https://github.com/prometheus/docs/blob/master/content/docs/instrumenting/exposition_formats.md
                // "Only one TYPE line may exist for a given metric name."
                if (!typeDefs.containsKey(metricName)) {
                    typeDefs.put(metricName, type);
                } else {
                    fail("Duplicate type definition found for TYPE definition " + metricName);
                }
                // From https://github.com/prometheus/docs/blob/master/content/docs/instrumenting/exposition_formats.md
                // "The TYPE line for a metric name must appear before the first sample is reported for that metric name."
                if (metricNames.containsKey(metricName)) {
                    fail("TYPE definition for " + metricName + " appears after first sample");
                }
            } else {
                Matcher metricMatcher = metricNamePattern.matcher(line);
                checkArgument(metricMatcher.matches());
                String metricName = metricMatcher.group(1);
                metricNames.put(metricName, metricName);
            }
        });

        List<Metric> cm = (List<Metric>) metrics.get("pulsar_ml_AddEntryBytesRate");
        assertEquals(cm.size(), 2);
        assertEquals(cm.get(0).tags.get("cluster"), "test");
        String ns = cm.get(0).tags.get("namespace");
        assertTrue(ns.equals("my-property/use/my-ns") || ns.equals("my-property/use/my-ns2"));

        cm = (List<Metric>) metrics.get("pulsar_ml_AddEntryMessagesRate");
        assertEquals(cm.size(), 2);
        assertEquals(cm.get(0).tags.get("cluster"), "test");
        ns = cm.get(0).tags.get("namespace");
        assertTrue(ns.equals("my-property/use/my-ns") || ns.equals("my-property/use/my-ns2"));

        p1.close();
        p2.close();
        p3.close();
        p4.close();
    }

    @Test
    public void testManagedLedgerBookieClientStats() throws Exception {
        @Cleanup
        Producer<byte[]> p1 = pulsarClient.newProducer().topic("persistent://my-property/use/my-ns/my-topic1").create();

        @Cleanup
        Producer<byte[]> p2 = pulsarClient.newProducer().topic("persistent://my-property/use/my-ns/my-topic2").create();
        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            p1.send(message.getBytes());
            p2.send(message.getBytes());
        }

        ByteArrayOutputStream statsOut = new ByteArrayOutputStream();
        PrometheusMetricsGenerator.generate(pulsar, false, false, false, statsOut);
        String metricsStr = statsOut.toString();

        Multimap<String, Metric> metrics = parseMetrics(metricsStr);

        metrics.entries().forEach(e ->
                System.out.println(e.getKey() + ": " + e.getValue())
        );

        List<Metric> cm = (List<Metric>) metrics.get("pulsar_managedLedger_client_bookkeeper_ml_scheduler_completed_tasks_0");
        assertEquals(cm.size(), 1);
        assertEquals(cm.get(0).tags.get("cluster"), "test");

        cm = (List<Metric>) metrics.get("pulsar_managedLedger_client_bookkeeper_ml_scheduler_queue_0");
        assertEquals(cm.size(), 1);
        assertEquals(cm.get(0).tags.get("cluster"), "test");

        cm = (List<Metric>) metrics.get("pulsar_managedLedger_client_bookkeeper_ml_scheduler_total_tasks_0");
        assertEquals(cm.size(), 1);
        assertEquals(cm.get(0).tags.get("cluster"), "test");

        cm = (List<Metric>) metrics.get("pulsar_managedLedger_client_bookkeeper_ml_workers_completed_tasks_0");
        assertEquals(cm.size(), 0);

        cm = (List<Metric>) metrics.get("pulsar_managedLedger_client_bookkeeper_ml_workers_task_execution_count");
        assertEquals(cm.size(), 0);
    }

    @Test
    public void testAuthMetrics() throws IOException, AuthenticationException {
        SecretKey secretKey = AuthTokenUtils.createSecretKey(SignatureAlgorithm.HS256);

        AuthenticationProviderToken provider = new AuthenticationProviderToken();

        Properties properties = new Properties();
        properties.setProperty("tokenSecretKey", AuthTokenUtils.encodeKeyBase64(secretKey));

        ServiceConfiguration conf = new ServiceConfiguration();
        conf.setProperties(properties);
        provider.initialize(conf);

        String authExceptionMessage = "";

        try {
            provider.authenticate(new AuthenticationDataSource() {
            });
            fail("Should have failed");
        } catch (AuthenticationException e) {
            // expected, no credential passed
            authExceptionMessage = e.getMessage();
        }

        String token = AuthTokenUtils.createToken(secretKey, "subject", Optional.empty());

        // Pulsar protocol auth
        String subject = provider.authenticate(new AuthenticationDataSource() {
            @Override
            public boolean hasDataFromCommand() {
                return true;
            }

            @Override
            public String getCommandData() {
                return token;
            }
        });

        ByteArrayOutputStream statsOut = new ByteArrayOutputStream();
        PrometheusMetricsGenerator.generate(pulsar, false, false, false, statsOut);
        String metricsStr = statsOut.toString();
        Multimap<String, Metric> metrics = parseMetrics(metricsStr);
        List<Metric> cm = (List<Metric>) metrics.get("pulsar_authentication_success_count");
        boolean haveSucceed = false;
        for (Metric metric : cm) {
            if (Objects.equals(metric.tags.get("auth_method"), "token")
                    && Objects.equals(metric.tags.get("provider_name"), provider.getClass().getSimpleName())) {
                haveSucceed = true;
            }
        }
        Assert.assertTrue(haveSucceed);

        cm = (List<Metric>) metrics.get("pulsar_authentication_failures_count");

        boolean haveFailed = false;
        for (Metric metric : cm) {
            if (Objects.equals(metric.tags.get("auth_method"), "token")
                    && Objects.equals(metric.tags.get("reason"), authExceptionMessage)
                    && Objects.equals(metric.tags.get("provider_name"), provider.getClass().getSimpleName())) {
                haveFailed = true;
            }
        }
        Assert.assertTrue(haveFailed);
    }

    @Test
    public void testExpiredTokenMetrics() throws Exception {
        SecretKey secretKey = AuthTokenUtils.createSecretKey(SignatureAlgorithm.HS256);

        AuthenticationProviderToken provider = new AuthenticationProviderToken();

        Properties properties = new Properties();
        properties.setProperty("tokenSecretKey", AuthTokenUtils.encodeKeyBase64(secretKey));

        ServiceConfiguration conf = new ServiceConfiguration();
        conf.setProperties(properties);
        provider.initialize(conf);

        Date expiredDate = new Date(System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1));
        String expiredToken = AuthTokenUtils.createToken(secretKey, "subject", Optional.of(expiredDate));

        try {
            provider.authenticate(new AuthenticationDataSource() {
                @Override
                public boolean hasDataFromCommand() {
                    return true;
                }

                @Override
                public String getCommandData() {
                    return expiredToken;
                }
            });
            fail("Should have failed");
        } catch (AuthenticationException e) {
            // expected, token was expired
        }

        ByteArrayOutputStream statsOut = new ByteArrayOutputStream();
        PrometheusMetricsGenerator.generate(pulsar, false, false, false, statsOut);
        String metricsStr = statsOut.toString();
        Multimap<String, Metric> metrics = parseMetrics(metricsStr);
        List<Metric> cm = (List<Metric>) metrics.get("pulsar_expired_token_count");
        assertEquals(cm.size(), 1);

        provider.close();
    }

    @Test
    public void testExpiringTokenMetrics() throws Exception {
        SecretKey secretKey = AuthTokenUtils.createSecretKey(SignatureAlgorithm.HS256);

        AuthenticationProviderToken provider = new AuthenticationProviderToken();

        Properties properties = new Properties();
        properties.setProperty("tokenSecretKey", AuthTokenUtils.encodeKeyBase64(secretKey));

        ServiceConfiguration conf = new ServiceConfiguration();
        conf.setProperties(properties);
        provider.initialize(conf);

        int[] tokenRemainTime = new int[]{3, 7, 40, 100, 400};

        for (int remainTime : tokenRemainTime) {
            Date expiredDate = new Date(System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(remainTime));
            String expiringToken = AuthTokenUtils.createToken(secretKey, "subject", Optional.of(expiredDate));
            provider.authenticate(new AuthenticationDataSource() {
                @Override
                public boolean hasDataFromCommand() {
                    return true;
                }

                @Override
                public String getCommandData() {
                    return expiringToken;
                }
            });
        }

        ByteArrayOutputStream statsOut = new ByteArrayOutputStream();
        PrometheusMetricsGenerator.generate(pulsar, false, false, false, statsOut);
        String metricsStr = statsOut.toString();
        Multimap<String, Metric> metrics = parseMetrics(metricsStr);
        Metric countMetric = ((List<Metric>) metrics.get("pulsar_expiring_token_minutes_count")).get(0);
        assertEquals(countMetric.value, tokenRemainTime.length);
        List<Metric> cm = (List<Metric>) metrics.get("pulsar_expiring_token_minutes_bucket");
        assertEquals(cm.size(), 5);
        cm.forEach((e) -> {
            switch (e.tags.get("le")) {
                case "5.0":
                    assertEquals(e.value, 1);
                    break;
                case "10.0":
                    assertEquals(e.value, 2);
                    break;
                case "60.0":
                    assertEquals(e.value, 3);
                    break;
                case "240.0":
                    assertEquals(e.value, 4);
                    break;
                default:
                    assertEquals(e.value, 5);
                    break;
            }
        });
        provider.close();
    }

    @Test
    public void testParsingWithPositiveInfinityValue() {
        Multimap<String, Metric> metrics = parseMetrics("pulsar_broker_publish_latency{cluster=\"test\",quantile=\"0.0\"} +Inf");
        List<Metric> cm = (List<Metric>) metrics.get("pulsar_broker_publish_latency");
        assertEquals(cm.size(), 1);
        assertEquals(cm.get(0).tags.get("cluster"), "test");
        assertEquals(cm.get(0).tags.get("quantile"), "0.0");
        assertEquals(cm.get(0).value, Double.POSITIVE_INFINITY);
    }

    @Test
    public void testParsingWithNegativeInfinityValue() {
        Multimap<String, Metric> metrics = parseMetrics("pulsar_broker_publish_latency{cluster=\"test\",quantile=\"0.0\"} -Inf");
        List<Metric> cm = (List<Metric>) metrics.get("pulsar_broker_publish_latency");
        assertEquals(cm.size(), 1);
        assertEquals(cm.get(0).tags.get("cluster"), "test");
        assertEquals(cm.get(0).tags.get("quantile"), "0.0");
        assertEquals(cm.get(0).value, Double.NEGATIVE_INFINITY);
    }

    @Test
    public void testManagedCursorPersistStats() throws Exception {
        final String subName = "my-sub";
        final String topicName = "persistent://my-namespace/use/my-ns/my-topic1";
        final int messageSize = 10;

        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(topicName)
                .subscriptionType(SubscriptionType.Shared)
                .ackTimeout(1, TimeUnit.SECONDS)
                .subscriptionName(subName)
                .subscribe();

        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topicName)
                .create();
        for (int i = 0; i < messageSize; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
            consumer.acknowledge(consumer.receive().getMessageId());
        }

        // enable ExposeManagedCursorMetricsInPrometheus
        pulsar.getConfiguration().setExposeManagedCursorMetricsInPrometheus(true);
        ByteArrayOutputStream statsOut = new ByteArrayOutputStream();
        PrometheusMetricsGenerator.generate(pulsar, true, false, false, statsOut);
        String metricsStr = statsOut.toString();

        Multimap<String, Metric> metrics = parseMetrics(metricsStr);

        List<Metric> cm = (List<Metric>) metrics.get("pulsar_ml_cursor_persistLedgerSucceed");
        assertEquals(cm.size(), 1);
        assertEquals(cm.get(0).tags.get("cluster"), "test");
        assertEquals(cm.get(0).tags.get("cursor_name"), subName);

        // disable ExposeManagedCursorMetricsInPrometheus
        pulsar.getConfiguration().setExposeManagedCursorMetricsInPrometheus(false);
        ByteArrayOutputStream statsOut2 = new ByteArrayOutputStream();
        PrometheusMetricsGenerator.generate(pulsar, true, false, false, statsOut2);
        String metricsStr2 = statsOut2.toString();
        Multimap<String, Metric> metrics2 = parseMetrics(metricsStr2);
        List<Metric> cm2 = (List<Metric>) metrics2.get("pulsar_ml_cursor_persistLedgerSucceed");
        assertEquals(cm2.size(), 0);

        producer.close();
        consumer.close();
    }

    @Test
    public void testBrokerConnection() throws Exception {
        final String topicName = "persistent://my-namespace/use/my-ns/my-topic1";

        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topicName)
                .create();

        ByteArrayOutputStream statsOut = new ByteArrayOutputStream();
        PrometheusMetricsGenerator.generate(pulsar, true, false, false, statsOut);
        String metricsStr = statsOut.toString();
        Multimap<String, Metric> metrics = parseMetrics(metricsStr);
        List<Metric> cm = (List<Metric>) metrics.get("pulsar_connection_created_total_count");
        compareBrokerConnectionStateCount(cm, 1.0);

        cm = (List<Metric>) metrics.get("pulsar_connection_create_success_count");
        compareBrokerConnectionStateCount(cm, 1.0);

        cm = (List<Metric>) metrics.get("pulsar_connection_closed_total_count");
        compareBrokerConnectionStateCount(cm, 0.0);

        cm = (List<Metric>) metrics.get("pulsar_active_connections");
        compareBrokerConnectionStateCount(cm, 1.0);

        pulsarClient.close();
        statsOut = new ByteArrayOutputStream();
        PrometheusMetricsGenerator.generate(pulsar, true, false, false, statsOut);
        metricsStr = statsOut.toString();

        metrics = parseMetrics(metricsStr);
        cm = (List<Metric>) metrics.get("pulsar_connection_closed_total_count");
        compareBrokerConnectionStateCount(cm, 1.0);

        pulsar.getConfiguration().setAuthenticationEnabled(true);

        replacePulsarClient(PulsarClient.builder().serviceUrl(lookupUrl.toString())
                .operationTimeout(1, TimeUnit.MILLISECONDS));

        try {
            pulsarClient.newProducer()
                    .topic(topicName)
                    .create();
            fail();
        } catch (Exception e) {
            assertTrue(e instanceof PulsarClientException.AuthenticationException);
        }

        pulsarClient.close();
        statsOut = new ByteArrayOutputStream();
        PrometheusMetricsGenerator.generate(pulsar, true, false, false, statsOut);
        metricsStr = statsOut.toString();

        metrics = parseMetrics(metricsStr);
        cm = (List<Metric>) metrics.get("pulsar_connection_closed_total_count");
        compareBrokerConnectionStateCount(cm, 2.0);

        cm = (List<Metric>) metrics.get("pulsar_connection_create_fail_count");
        compareBrokerConnectionStateCount(cm, 1.0);

        cm = (List<Metric>) metrics.get("pulsar_connection_create_success_count");
        compareBrokerConnectionStateCount(cm, 1.0);

        cm = (List<Metric>) metrics.get("pulsar_active_connections");
        compareBrokerConnectionStateCount(cm, 0.0);

        cm = (List<Metric>) metrics.get("pulsar_connection_created_total_count");
        compareBrokerConnectionStateCount(cm, 2.0);
    }

    private void compareBrokerConnectionStateCount(List<Metric> cm, double count) {
        assertEquals(cm.size(), 1);
        assertEquals(cm.get(0).tags.get("cluster"), "test");
        assertEquals(cm.get(0).tags.get("broker"), "localhost");
        assertEquals(cm.get(0).value, count);
    }

    @Test
    void testParseMetrics() throws IOException {
        String sampleMetrics = IOUtils.toString(getClass().getClassLoader()
                .getResourceAsStream("prometheus_metrics_sample.txt"), StandardCharsets.UTF_8);
        parseMetrics(sampleMetrics);
    }

    @Test
    public void testCompaction() throws Exception {
        final String topicName = "persistent://my-namespace/use/my-ns/my-compaction1";

        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topicName)
                .enableBatching(false)
                .messageRoutingMode(MessageRoutingMode.SinglePartition)
                .create();
        ByteArrayOutputStream statsOut = new ByteArrayOutputStream();
        PrometheusMetricsGenerator.generate(pulsar, true, false, false, statsOut);
        String metricsStr = statsOut.toString();
        Multimap<String, Metric> metrics = parseMetrics(metricsStr);
        List<Metric> cm = (List<Metric>) metrics.get("pulsar_compaction_removed_event_count");
        assertEquals(cm.size(), 0);
        cm = (List<Metric>) metrics.get("pulsar_compaction_succeed_count");
        assertEquals(cm.size(), 0);
        cm = (List<Metric>) metrics.get("pulsar_compaction_failed_count");
        assertEquals(cm.size(), 0);
        cm = (List<Metric>) metrics.get("pulsar_compaction_duration_time_in_mills");
        assertEquals(cm.size(), 0);
        cm = (List<Metric>) metrics.get("pulsar_compaction_read_throughput");
        assertEquals(cm.size(), 0);
        cm = (List<Metric>) metrics.get("pulsar_compaction_write_throughput");
        assertEquals(cm.size(), 0);
        cm = (List<Metric>) metrics.get("pulsar_compaction_compacted_entries_count");
        assertEquals(cm.size(), 0);
        cm = (List<Metric>) metrics.get("pulsar_compaction_compacted_entries_size");
        assertEquals(cm.size(), 0);
        //
        final int numMessages = 1000;
        final int maxKeys = 10;
        Random r = new Random(0);
        for (int j = 0; j < numMessages; j++) {
            int keyIndex = r.nextInt(maxKeys);
            String key = "key"+keyIndex;
            byte[] data = ("my-message-" + key + "-" + j).getBytes();
            producer.newMessage()
                    .key(key)
                    .value(data)
                    .send();
        }
        Compactor compactor = pulsar.getCompactor(true);
        compactor.compact(topicName).get();
        statsOut = new ByteArrayOutputStream();
        PrometheusMetricsGenerator.generate(pulsar, true, false, false, statsOut);
        metricsStr = statsOut.toString();
        metrics = parseMetrics(metricsStr);
        cm = (List<Metric>) metrics.get("pulsar_compaction_removed_event_count");
        assertEquals(cm.size(), 1);
        assertEquals(cm.get(0).value, 990);
        cm = (List<Metric>) metrics.get("pulsar_compaction_succeed_count");
        assertEquals(cm.size(), 1);
        assertEquals(cm.get(0).value, 1);
        cm = (List<Metric>) metrics.get("pulsar_compaction_failed_count");
        assertEquals(cm.size(), 1);
        assertEquals(cm.get(0).value, 0);
        cm = (List<Metric>) metrics.get("pulsar_compaction_duration_time_in_mills");
        assertEquals(cm.size(), 1);
        assertTrue(cm.get(0).value > 0);
        cm = (List<Metric>) metrics.get("pulsar_compaction_read_throughput");
        assertEquals(cm.size(), 1);
        assertTrue(cm.get(0).value > 0);
        cm = (List<Metric>) metrics.get("pulsar_compaction_write_throughput");
        assertEquals(cm.size(), 1);
        assertTrue(cm.get(0).value > 0);
        cm = (List<Metric>) metrics.get("pulsar_compaction_compacted_entries_count");
        assertEquals(cm.size(), 1);
        assertEquals(cm.get(0).value, 10);
        cm = (List<Metric>) metrics.get("pulsar_compaction_compacted_entries_size");
        assertEquals(cm.size(), 1);
        assertEquals(cm.get(0).value, 870);

        pulsarClient.close();
    }

    @Test
    public void testSplitTopicAndPartitionLabel() throws Exception {
        String ns1 = "prop/ns-abc1";
        String ns2 = "prop/ns-abc2";
        admin.namespaces().createNamespace(ns1);
        admin.namespaces().createNamespace(ns2);
        String baseTopic1 = "persistent://" + ns1 + "/testMetricsTopicCount";
        String baseTopic2 = "persistent://" + ns2 + "/testMetricsTopicCount";
        for (int i = 0; i < 6; i++) {
            admin.topics().createNonPartitionedTopic(baseTopic1 + UUID.randomUUID());
        }
        for (int i = 0; i < 3; i++) {
            admin.topics().createPartitionedTopic(baseTopic2 + UUID.randomUUID(), 3);
        }
        Consumer<byte[]> consumer1 = pulsarClient.newConsumer()
                .topicsPattern("persistent://" + ns1 + "/.*")
                .subscriptionName("sub")
                .subscribe();
        Consumer<byte[]> consumer2 = pulsarClient.newConsumer()
                .topicsPattern("persistent://" + ns2 + "/.*")
                .subscriptionName("sub")
                .subscribe();
        @Cleanup
        ByteArrayOutputStream statsOut = new ByteArrayOutputStream();
        PrometheusMetricsGenerator.generate(pulsar, true, false, false, true,  statsOut);
        String metricsStr = statsOut.toString();
        Multimap<String, Metric> metrics = parseMetrics(metricsStr);
        Collection<Metric> metric = metrics.get("pulsar_consumers_count");
        assertTrue(metric.size() >= 15);
        metric.forEach(item -> {
            if (ns1.equals(item.tags.get("namespace"))) {
                assertEquals(item.tags.get("partition"), "-1");
            }
            if (ns2.equals(item.tags.get("namespace"))) {
                System.out.println(item);
                assertTrue(Integer.parseInt(item.tags.get("partition")) >= 0);
            }
        });
        consumer1.close();
        consumer2.close();
    }

    private void compareCompactionStateCount(List<Metric> cm, double count) {
        assertEquals(cm.size(), 1);
        assertEquals(cm.get(0).tags.get("cluster"), "test");
        assertEquals(cm.get(0).tags.get("broker"), "localhost");
        assertEquals(cm.get(0).value, count);
    }

    /**
     * Hacky parsing of Prometheus text format. Should be good enough for unit tests
     */
    public static Multimap<String, Metric> parseMetrics(String metrics) {
        Multimap<String, Metric> parsed = ArrayListMultimap.create();

        // Example of lines are
        // jvm_threads_current{cluster="standalone",} 203.0
        // or
        // pulsar_subscriptions_count{cluster="standalone", namespace="sample/standalone/ns1",
        // topic="persistent://sample/standalone/ns1/test-2"} 0.0 1517945780897
        Pattern pattern = Pattern.compile("^(\\w+)\\{([^\\}]+)\\}\\s([+-]?[\\d\\w\\.-]+)(\\s(\\d+))?$");
        Pattern tagsPattern = Pattern.compile("(\\w+)=\"([^\"]+)\"(,\\s?)?");

        Splitter.on("\n").split(metrics).forEach(line -> {
            if (line.isEmpty() || line.startsWith("#")) {
                return;
            }

            Matcher matcher = pattern.matcher(line);
            assertTrue(matcher.matches(), "line " + line + " does not match pattern " + pattern);
            String name = matcher.group(1);

            Metric m = new Metric();
            String numericValue = matcher.group(3);
            if (numericValue.equalsIgnoreCase("-Inf")) {
                m.value = Double.NEGATIVE_INFINITY;
            } else if (numericValue.equalsIgnoreCase("+Inf")) {
                m.value = Double.POSITIVE_INFINITY;
            } else {
                m.value = Double.parseDouble(numericValue);
            }
            String tags = matcher.group(2);
            Matcher tagsMatcher = tagsPattern.matcher(tags);
            while (tagsMatcher.find()) {
                String tag = tagsMatcher.group(1);
                String value = tagsMatcher.group(2);
                m.tags.put(tag, value);
            }

            parsed.put(name, m);
        });

        return parsed;
    }

    static class Metric {
        Map<String, String> tags = new TreeMap<>();
        double value;

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this).add("tags", tags).add("value", value).toString();
        }
    }

}
