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

import static org.apache.pulsar.broker.BrokerTestUtil.spyWithClassAndConstructorArgs;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertNotEquals;
import static org.testng.AssertJUnit.assertEquals;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.service.plugin.EntryFilter;
import org.apache.pulsar.broker.service.plugin.EntryFilterProducerTest;
import org.apache.pulsar.broker.service.plugin.EntryFilterWithClassLoader;
import org.apache.pulsar.broker.stats.prometheus.PrometheusMetricsGenerator;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageListener;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.nar.NarClassLoader;
import org.apache.pulsar.common.policies.data.ConsumerStats;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.apache.pulsar.common.policies.data.stats.ConsumerStatsImpl;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker")
public class ConsumerStatsTest extends ProducerConsumerBase {

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        conf.setMaxUnackedMessagesPerConsumer(0);
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testConsumerStatsOnZeroMaxUnackedMessagesPerConsumer() throws PulsarClientException, InterruptedException, PulsarAdminException {
        Assert.assertEquals(pulsar.getConfiguration().getMaxUnackedMessagesPerConsumer(), 0);
        final String topicName = "persistent://my-property/my-ns/testConsumerStatsOnZeroMaxUnackedMessagesPerConsumer";

        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(topicName)
                .subscriptionType(SubscriptionType.Shared)
                .ackTimeout(1, TimeUnit.SECONDS)
                .subscriptionName("sub")
                .subscribe();

        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topicName)
                .create();

        final int messages = 10;
        for (int i = 0; i < messages; i++) {
            producer.send(("message-" + i).getBytes());
        }

        int received = 0;
        for (int i = 0; i < messages; i++) {
            // don't ack messages here
            consumer.receive();
            received++;
        }

        Assert.assertEquals(received, messages);
        received = 0;

        TopicStats stats = admin.topics().getStats(topicName);
        Assert.assertEquals(stats.getSubscriptions().size(), 1);
        Assert.assertEquals(stats.getSubscriptions().entrySet().iterator().next().getValue().getConsumers().size(), 1);
        Assert.assertFalse(stats.getSubscriptions().entrySet().iterator().next().getValue().getConsumers().get(0).isBlockedConsumerOnUnackedMsgs());
        Assert.assertEquals(stats.getSubscriptions().entrySet().iterator().next().getValue().getConsumers().get(0).getUnackedMessages(), messages);

        for (int i = 0; i < messages; i++) {
            consumer.acknowledge(consumer.receive());
            received++;
        }

        Assert.assertEquals(received, messages);

        // wait acknowledge send
        Thread.sleep(2000);

        stats = admin.topics().getStats(topicName);

        Assert.assertFalse(stats.getSubscriptions().entrySet().iterator().next().getValue().getConsumers().get(0).isBlockedConsumerOnUnackedMsgs());
        Assert.assertEquals(stats.getSubscriptions().entrySet().iterator().next().getValue().getConsumers().get(0).getUnackedMessages(), 0);
    }

    @Test
    public void testAckStatsOnPartitionedTopicForExclusiveSubscription() throws PulsarAdminException, PulsarClientException, InterruptedException {
        final String topic = "persistent://my-property/my-ns/testAckStatsOnPartitionedTopicForExclusiveSubscription";
        admin.topics().createPartitionedTopic(topic, 3);
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(topic)
                .subscriptionType(SubscriptionType.Exclusive)
                .subscriptionName("sub")
                .subscribe();

        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topic)
                .create();

        final int messages = 10;
        for (int i = 0; i < messages; i++) {
            producer.send(("message-" + i).getBytes());
        }

        int received = 0;
        for (int i = 0; i < messages; i++) {
            consumer.acknowledge(consumer.receive());
            received++;
        }
        Assert.assertEquals(messages, received);

        // wait acknowledge send
        Thread.sleep(2000);

        for (int i = 0; i < 3; i++) {
            TopicStats stats = admin.topics().getStats(topic + "-partition-" + i);
            Assert.assertEquals(stats.getSubscriptions().size(), 1);
            Assert.assertEquals(stats.getSubscriptions().entrySet().iterator().next().getValue().getConsumers().size(), 1);
            Assert.assertEquals(stats.getSubscriptions().entrySet().iterator().next().getValue().getConsumers().get(0).getUnackedMessages(), 0);
        }
    }

    @Test
    public void testUpdateStatsForActiveConsumerAndSubscription() throws Exception {
        final String topicName = "persistent://prop/use/ns-abc/testUpdateStatsForActiveConsumerAndSubscription";
        pulsarClient.newConsumer()
                .topic(topicName)
                .subscriptionType(SubscriptionType.Shared)
                .subscriptionName("my-subscription")
                .subscribe();

        PersistentTopic topicRef = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName).get();
        Assert.assertNotNull(topicRef);
        Assert.assertEquals(topicRef.getSubscriptions().size(), 1);
        List<org.apache.pulsar.broker.service.Consumer> consumers = topicRef.getSubscriptions()
                .get("my-subscription").getConsumers();
        Assert.assertEquals(consumers.size(), 1);
        ConsumerStatsImpl consumerStats = new ConsumerStatsImpl();
        consumerStats.msgOutCounter = 10;
        consumerStats.bytesOutCounter = 1280;
        consumers.get(0).updateStats(consumerStats);
        ConsumerStats updatedStats = consumers.get(0).getStats();

        Assert.assertEquals(updatedStats.getMsgOutCounter(), 10);
        Assert.assertEquals(updatedStats.getBytesOutCounter(), 1280);
    }

    @Test
    public void testConsumerStatsOutput() throws Exception {
        Set<String> allowedFields = Sets.newHashSet(
                "msgRateOut",
                "msgThroughputOut",
                "bytesOutCounter",
                "msgOutCounter",
                "messageAckRate",
                "msgRateRedeliver",
                "chunkedMessageRate",
                "consumerName",
                "availablePermits",
                "unackedMessages",
                "avgMessagesPerEntry",
                "blockedConsumerOnUnackedMsgs",
                "readPositionWhenJoining",
                "lastAckedTimestamp",
                "lastConsumedTimestamp",
                "keyHashRanges",
                "metadata",
                "address",
                "connectedSince",
                "clientVersion");

        final String topicName = "persistent://prop/use/ns-abc/testConsumerStatsOutput";
        final String subName = "my-subscription";

        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(topicName)
                .subscriptionType(SubscriptionType.Shared)
                .subscriptionName(subName)
                .subscribe();

        TopicStats stats = admin.topics().getStats(topicName);
        ObjectMapper mapper = ObjectMapperFactory.create();
        JsonNode node = mapper.readTree(mapper.writer().writeValueAsString(stats.getSubscriptions()
                .get(subName).getConsumers().get(0)));
        Iterator<String> itr = node.fieldNames();
        while (itr.hasNext()) {
            String field = itr.next();
            Assert.assertTrue(allowedFields.contains(field), field + " should not be exposed");
        }

        consumer.close();
    }


    @Test
    public void testPersistentTopicMessageAckRateMetricTopicLevel() throws Exception {
        String topicName = "persistent://public/default/msg_ack_rate" + UUID.randomUUID();
        testMessageAckRateMetric(topicName, true);
    }

    @Test
    public void testPersistentTopicMessageAckRateMetricNamespaceLevel() throws Exception {
        String topicName = "persistent://public/default/msg_ack_rate" + UUID.randomUUID();
        testMessageAckRateMetric(topicName, false);
    }

    private void testMessageAckRateMetric(String topicName, boolean exposeTopicLevelMetrics)
            throws Exception {
        final int messages = 1000;
        String subName = "test_sub";
        CountDownLatch latch = new CountDownLatch(messages);

        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topicName)
                .enableBatching(true).batchingMaxMessages(10).create();

        MessageListener<String> listener = (consumer, msg) -> {
            try {
                consumer.acknowledge(msg);
                latch.countDown();
            } catch (PulsarClientException e) {
                //ignore
            }
        };
        @Cleanup
        Consumer<String> c1 = pulsarClient.newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionName(subName)
                .subscriptionType(SubscriptionType.Shared)
                .messageListener(listener)
                .subscribe();
        @Cleanup
        Consumer<String> c2 = pulsarClient.newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionName(subName)
                .subscriptionType(SubscriptionType.Shared)
                .messageListener(listener)
                .subscribe();

        String namespace = TopicName.get(topicName).getNamespace();

        for (int i = 0; i < messages; i++) {
            producer.sendAsync(UUID.randomUUID().toString());
        }
        producer.flush();

        latch.await(20, TimeUnit.SECONDS);
        TimeUnit.SECONDS.sleep(1);

        Topic topic = pulsar.getBrokerService().getTopic(topicName, false).get().get();
        Subscription subscription = topic.getSubscription(subName);
        List<org.apache.pulsar.broker.service.Consumer> consumers = subscription.getConsumers();
        Assert.assertEquals(consumers.size(), 2);
        org.apache.pulsar.broker.service.Consumer consumer1 = consumers.get(0);
        org.apache.pulsar.broker.service.Consumer consumer2 = consumers.get(1);
        consumer1.updateRates();
        consumer2.updateRates();

        ByteArrayOutputStream output = new ByteArrayOutputStream();
        PrometheusMetricsGenerator.generate(pulsar, exposeTopicLevelMetrics, true, true, output);
        String metricStr = output.toString(StandardCharsets.UTF_8.name());

        Multimap<String, PrometheusMetricsTest.Metric> metricsMap = PrometheusMetricsTest.parseMetrics(metricStr);
        Collection<PrometheusMetricsTest.Metric> ackRateMetric = metricsMap.get("pulsar_consumer_msg_ack_rate");

        String rateOutMetricName = exposeTopicLevelMetrics ? "pulsar_consumer_msg_rate_out" : "pulsar_rate_out";
        Collection<PrometheusMetricsTest.Metric> rateOutMetric = metricsMap.get(rateOutMetricName);
        Assert.assertTrue(ackRateMetric.size() > 0);
        Assert.assertTrue(rateOutMetric.size() > 0);

        if (exposeTopicLevelMetrics) {
            String consumer1Name = consumer1.consumerName();
            String consumer2Name = consumer2.consumerName();
            double totalAckRate = ackRateMetric.stream()
                    .filter(metric -> metric.tags.get("consumer_name").equals(consumer1Name)
                            || metric.tags.get("consumer_name").equals(consumer2Name))
                    .mapToDouble(metric -> metric.value).sum();
            double totalRateOut = rateOutMetric.stream()
                    .filter(metric -> metric.tags.get("consumer_name").equals(consumer1Name)
                            || metric.tags.get("consumer_name").equals(consumer2Name))
                    .mapToDouble(metric -> metric.value).sum();

            Assert.assertTrue(totalAckRate > 0D);
            Assert.assertTrue(totalRateOut > 0D);
            Assert.assertEquals(totalAckRate, totalRateOut, totalRateOut * 0.1D);
        } else {
            double totalAckRate = ackRateMetric.stream()
                    .filter(metric -> namespace.equals(metric.tags.get("namespace")))
                    .mapToDouble(metric -> metric.value).sum();
            double totalRateOut = rateOutMetric.stream()
                    .filter(metric -> namespace.equals(metric.tags.get("namespace")))
                    .mapToDouble(metric -> metric.value).sum();

            Assert.assertTrue(totalAckRate > 0D);
            Assert.assertTrue(totalRateOut > 0D);
            Assert.assertEquals(totalAckRate, totalRateOut, totalRateOut * 0.1D);
        }
    }

    @Override
    protected PulsarService newPulsarService(ServiceConfiguration conf) throws Exception {
        return new PulsarService(conf) {
            @Override
            protected BrokerService newBrokerService(PulsarService pulsar) throws Exception {
                return spy(new BrokerService(this, ioEventLoopGroup));
            }
        };
    }

    @Test
    public void testAvgMessagesPerEntry() throws Exception {
        final String topic = "persistent://public/default/testFilterState";
        String subName = "sub";

        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .producerName("producer1")
                .enableBatching(true).topic(topic)
                .batchingMaxMessages(20)
                .batchingMaxPublishDelay(5, TimeUnit.SECONDS)
                .batchingMaxBytes(Integer.MAX_VALUE)
                .create();

        producer.send("first-message");
        List<CompletableFuture<MessageId>> futures = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            futures.add(producer.sendAsync("message"));
        }
        FutureUtil.waitForAll(futures);
        producer.close();

        Producer<String> producer2 = pulsarClient.newProducer(Schema.STRING)
                .producerName("producer2")
                .enableBatching(false).topic(topic)
                .create();
        producer2.newMessage().value("producer2-message").send();
        producer2.close();

        // mock entry filters
        NarClassLoader narClassLoader = mock(NarClassLoader.class);
        EntryFilter filter = new EntryFilterProducerTest();
        EntryFilterWithClassLoader
                loader = spyWithClassAndConstructorArgs(EntryFilterWithClassLoader.class, filter,
                narClassLoader);
        ImmutableMap<String, EntryFilterWithClassLoader> entryFilters = ImmutableMap.of("filter", loader);
        BrokerService brokerService = pulsar.getBrokerService();
        doReturn(entryFilters).when(brokerService).getEntryFilters();

        Map<String, String> metadataConsumer = new HashMap<>();
        metadataConsumer.put("matchValueAccept", "producer1");
        metadataConsumer.put("matchValueReschedule", "producer2");
        @Cleanup
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING).topic(topic).properties(metadataConsumer)
                .subscriptionName(subName).subscriptionInitialPosition(SubscriptionInitialPosition.Earliest).subscribe();

        int counter = 0;
        while (true) {
            Message<String> message = consumer.receive(10, TimeUnit.SECONDS);
            if (message != null) {
                counter++;
                assertNotEquals(message.getValue(), "producer2-message");
                consumer.acknowledge(message);
            } else {
                break;
            }
        }

        assertEquals(21, counter);

        ConsumerStats consumerStats =
                admin.topics().getStats(topic).getSubscriptions().get(subName).getConsumers().get(0);

        assertEquals(21, consumerStats.getMsgOutCounter());

        // Math.round(1 * 0.9 + 0.1 * (20 / 1))
        int avgMessagesPerEntry = consumerStats.getAvgMessagesPerEntry();
        assertEquals(3, avgMessagesPerEntry);
    }
}
