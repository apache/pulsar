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
package org.apache.pulsar.broker.service.persistent;

import static org.apache.bookkeeper.mledger.impl.ManagedCursorImpl.CURSOR_INTERNAL_PROPERTY_PREFIX;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import com.google.common.collect.Multimap;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.delayed.BucketDelayedDeliveryTrackerFactory;
import org.apache.pulsar.broker.service.Dispatcher;
import org.apache.pulsar.broker.stats.PrometheusMetricsTest;
import org.apache.pulsar.broker.stats.prometheus.PrometheusMetricsGenerator;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class BucketDelayedDeliveryTest extends DelayedDeliveryTest {

    @BeforeClass
    @Override
    public void setup() throws Exception {
        conf.setDelayedDeliveryTrackerFactoryClassName(BucketDelayedDeliveryTrackerFactory.class.getName());
        conf.setDelayedDeliveryMaxNumBuckets(10);
        conf.setDelayedDeliveryMaxTimeStepPerBucketSnapshotSegmentSeconds(1);
        conf.setDelayedDeliveryMaxIndexesPerBucketSnapshotSegment(10);
        conf.setDelayedDeliveryMinIndexCountPerBucket(50);
        conf.setManagedLedgerMaxEntriesPerLedger(50);
        conf.setManagedLedgerMinLedgerRolloverTimeMinutes(0);
        super.setup();
    }

    @Override
    @AfterClass(alwaysRun = true)
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testBucketDelayedDeliveryWithAllConsumersDisconnecting() throws Exception {
        String topic = BrokerTestUtil.newUniqueName("persistent://public/default/testDelaysWithAllConsumerDis");

        Consumer<String> c1 = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("sub")
                .subscriptionType(SubscriptionType.Shared)
                .subscribe();

        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topic)
                .create();

        for (int i = 0; i < 1000; i++) {
            producer.newMessage()
                    .value("msg")
                    .deliverAfter(1, TimeUnit.HOURS)
                    .send();
        }

        Dispatcher dispatcher = pulsar.getBrokerService().getTopicReference(topic).get().getSubscription("sub").getDispatcher();
        Awaitility.await().untilAsserted(() -> Assert.assertEquals(dispatcher.getNumberOfDelayedMessages(), 1000));
        List<String> bucketKeys =
                ((PersistentDispatcherMultipleConsumers) dispatcher).getCursor().getCursorProperties().keySet().stream()
                        .filter(x -> x.startsWith(ManagedCursorImpl.CURSOR_INTERNAL_PROPERTY_PREFIX)).toList();

        c1.close();

        // Attach a new consumer. Since there are no consumers connected, this will trigger the cursor rewind
        @Cleanup
        Consumer<String> c2 = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("sub")
                .subscriptionType(SubscriptionType.Shared)
                .subscribe();

        Dispatcher dispatcher2 = pulsar.getBrokerService().getTopicReference(topic).get().getSubscription("sub").getDispatcher();
        List<String> bucketKeys2 =
                ((PersistentDispatcherMultipleConsumers) dispatcher2).getCursor().getCursorProperties().keySet().stream()
                        .filter(x -> x.startsWith(ManagedCursorImpl.CURSOR_INTERNAL_PROPERTY_PREFIX)).toList();

        Awaitility.await().untilAsserted(() -> Assert.assertEquals(dispatcher2.getNumberOfDelayedMessages(), 1000));
        Assert.assertEquals(bucketKeys, bucketKeys2);
    }


    @Test
    public void testUnsubscribe() throws Exception {
        String topic = BrokerTestUtil.newUniqueName("persistent://public/default/testUnsubscribes");

        @Cleanup
        Consumer<String> c1 = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("sub")
                .subscriptionType(SubscriptionType.Shared)
                .subscribe();

        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topic)
                .create();

        for (int i = 0; i < 1000; i++) {
            producer.newMessage()
                    .value("msg")
                    .deliverAfter(1, TimeUnit.HOURS)
                    .send();
        }

        Dispatcher dispatcher = pulsar.getBrokerService().getTopicReference(topic).get().getSubscription("sub").getDispatcher();
        Awaitility.await().untilAsserted(() -> Assert.assertEquals(dispatcher.getNumberOfDelayedMessages(), 1000));

        Map<String, String> cursorProperties =
                ((PersistentDispatcherMultipleConsumers) dispatcher).getCursor().getCursorProperties();
        List<Long> bucketIds = cursorProperties.entrySet().stream()
                .filter(x -> x.getKey().startsWith(CURSOR_INTERNAL_PROPERTY_PREFIX + "delayed.bucket")).map(
                        x -> Long.valueOf(x.getValue())).toList();

        assertTrue(bucketIds.size() > 0);

        c1.close();

        restartBroker();

        admin.topics().deleteSubscription(topic, "sub");

        for (Long bucketId : bucketIds) {
            try {
                LedgerHandle ledgerHandle =
                        pulsarTestContext.getBookKeeperClient()
                                .openLedger(bucketId, BookKeeper.DigestType.CRC32C, new byte[]{});
                Assert.fail("Should fail");
            } catch (BKException.BKNoSuchLedgerExistsException e) {
                // ignore it
            }
        }
    }


    @Test
    public void testBucketDelayedIndexMetrics() throws Exception {
        cleanup();
        setup();

        String topic = BrokerTestUtil.newUniqueName("persistent://public/default/testBucketDelayedIndexMetrics");

        @Cleanup
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("test_sub")
                .subscriptionType(SubscriptionType.Shared)
                .subscribe();

        @Cleanup
        Consumer<String> consumer2 = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("test_sub2")
                .subscriptionType(SubscriptionType.Shared)
                .subscribe();

        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topic)
                .create();

        final int N = 101;

        for (int i = 0; i < N; i++) {
            producer.newMessage()
                    .value("msg-" + i)
                    .deliverAfter(3600 + i, TimeUnit.SECONDS)
                    .sendAsync();
        }
        producer.flush();

        Thread.sleep(2000);

        ByteArrayOutputStream output = new ByteArrayOutputStream();
        PrometheusMetricsGenerator.generate(pulsar, true, true, true, output);
        String metricsStr = output.toString(StandardCharsets.UTF_8);
        Multimap<String, PrometheusMetricsTest.Metric> metricsMap = PrometheusMetricsTest.parseMetrics(metricsStr);

        List<PrometheusMetricsTest.Metric> bucketsMetrics =
                metricsMap.get("pulsar_delayed_message_index_bucket_total").stream()
                        .filter(metric -> metric.tags.get("topic").equals(topic)).toList();
        MutableInt bucketsSum = new MutableInt();
        bucketsMetrics.stream().filter(metric -> metric.tags.containsKey("subscription")).forEach(metric -> {
            assertEquals(3, metric.value);
            bucketsSum.add(metric.value);
        });
        assertEquals(6, bucketsSum.intValue());
        Optional<PrometheusMetricsTest.Metric> bucketsTopicMetric =
                bucketsMetrics.stream().filter(metric -> !metric.tags.containsKey("subscription")).findFirst();
        assertTrue(bucketsTopicMetric.isPresent());
        assertEquals(bucketsSum.intValue(), bucketsTopicMetric.get().value);

        List<PrometheusMetricsTest.Metric> loadedIndexMetrics =
                metricsMap.get("pulsar_delayed_message_index_loaded").stream()
                        .filter(metric -> metric.tags.get("topic").equals(topic)).toList();
        MutableInt loadedIndexSum = new MutableInt();
        long count = loadedIndexMetrics.stream().filter(metric -> metric.tags.containsKey("subscription")).peek(metric -> {
            assertTrue(metric.value > 0 && metric.value <= N);
            loadedIndexSum.add(metric.value);
        }).count();
        assertEquals(2, count);
        Optional<PrometheusMetricsTest.Metric> loadedIndexTopicMetrics =
                bucketsMetrics.stream().filter(metric -> !metric.tags.containsKey("subscription")).findFirst();
        assertTrue(loadedIndexTopicMetrics.isPresent());
        assertEquals(loadedIndexSum.intValue(), loadedIndexTopicMetrics.get().value);

        List<PrometheusMetricsTest.Metric> snapshotSizeBytesMetrics =
                metricsMap.get("pulsar_delayed_message_index_bucket_snapshot_size_bytes").stream()
                        .filter(metric -> metric.tags.get("topic").equals(topic)).toList();
        MutableInt snapshotSizeBytesSum = new MutableInt();
        count = snapshotSizeBytesMetrics.stream().filter(metric -> metric.tags.containsKey("subscription"))
                .peek(metric -> {
                    assertTrue(metric.value > 0);
                    snapshotSizeBytesSum.add(metric.value);
                }).count();
        assertEquals(2, count);
        Optional<PrometheusMetricsTest.Metric> snapshotSizeBytesTopicMetrics =
                snapshotSizeBytesMetrics.stream().filter(metric -> !metric.tags.containsKey("subscription")).findFirst();
        assertTrue(snapshotSizeBytesTopicMetrics.isPresent());
        assertEquals(snapshotSizeBytesSum.intValue(), snapshotSizeBytesTopicMetrics.get().value);

        List<PrometheusMetricsTest.Metric> opCountMetrics =
                metricsMap.get("pulsar_delayed_message_index_bucket_op_count").stream()
                        .filter(metric -> metric.tags.get("topic").equals(topic)).toList();
        MutableInt opCountMetricsSum = new MutableInt();
        count = opCountMetrics.stream()
                .filter(metric -> metric.tags.get("state").equals("succeed") && metric.tags.get("type").equals("create")
                        && metric.tags.containsKey("subscription"))
                .peek(metric -> {
                    assertTrue(metric.value >= 2);
                    opCountMetricsSum.add(metric.value);
                }).count();
        assertEquals(2, count);
        Optional<PrometheusMetricsTest.Metric> opCountTopicMetrics =
                opCountMetrics.stream()
                        .filter(metric -> metric.tags.get("state").equals("succeed") && metric.tags.get("type")
                                .equals("create") && !metric.tags.containsKey("subscription")).findFirst();
        assertTrue(opCountTopicMetrics.isPresent());
        assertEquals(opCountMetricsSum.intValue(), opCountTopicMetrics.get().value);

        List<PrometheusMetricsTest.Metric> opLatencyMetrics =
                metricsMap.get("pulsar_delayed_message_index_bucket_op_latency_ms").stream()
                        .filter(metric -> metric.tags.get("topic").equals(topic)).toList();
        MutableInt opLatencyMetricsSum = new MutableInt();
        count = opLatencyMetrics.stream()
                .filter(metric -> metric.tags.get("type").equals("create")
                        && metric.tags.containsKey("subscription"))
                .peek(metric -> {
                    assertTrue(metric.tags.containsKey("quantile"));
                    opLatencyMetricsSum.add(metric.value);
                }).count();
        assertTrue(count >= 2);
        Optional<PrometheusMetricsTest.Metric> opLatencyTopicMetrics =
                opCountMetrics.stream()
                        .filter(metric -> metric.tags.get("type").equals("create")
                                && !metric.tags.containsKey("subscription")).findFirst();
        assertTrue(opLatencyTopicMetrics.isPresent());
        assertEquals(opLatencyMetricsSum.intValue(), opLatencyTopicMetrics.get().value);

        ByteArrayOutputStream namespaceOutput = new ByteArrayOutputStream();
        PrometheusMetricsGenerator.generate(pulsar, false, true, true, namespaceOutput);
        Multimap<String, PrometheusMetricsTest.Metric> namespaceMetricsMap = PrometheusMetricsTest.parseMetrics(namespaceOutput.toString(StandardCharsets.UTF_8));

        Optional<PrometheusMetricsTest.Metric> namespaceMetric =
                namespaceMetricsMap.get("pulsar_delayed_message_index_bucket_total").stream().findFirst();
        assertTrue(namespaceMetric.isPresent());
        assertEquals(6, namespaceMetric.get().value);
    }

    @Test
    public void testDelete() throws Exception {
        String topic = BrokerTestUtil.newUniqueName("persistent://public/default/testDelete");

        @Cleanup
        Consumer<String> c1 = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("sub")
                .subscriptionType(SubscriptionType.Shared)
                .subscribe();

        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topic)
                .create();

        for (int i = 0; i < 1000; i++) {
            producer.newMessage()
                    .value("msg")
                    .deliverAfter(1, TimeUnit.HOURS)
                    .send();
        }

        Dispatcher dispatcher = pulsar.getBrokerService().getTopicReference(topic).get().getSubscription("sub").getDispatcher();
        Awaitility.await().untilAsserted(() -> Assert.assertEquals(dispatcher.getNumberOfDelayedMessages(), 1000));

        Map<String, String> cursorProperties =
                ((PersistentDispatcherMultipleConsumers) dispatcher).getCursor().getCursorProperties();
        List<Long> bucketIds = cursorProperties.entrySet().stream()
                .filter(x -> x.getKey().startsWith(CURSOR_INTERNAL_PROPERTY_PREFIX + "delayed.bucket")).map(
                        x -> Long.valueOf(x.getValue())).toList();

        assertTrue(bucketIds.size() > 0);

        admin.topics().delete(topic, true);

        for (Long bucketId : bucketIds) {
            try {
                LedgerHandle ledgerHandle =
                        pulsarTestContext.getBookKeeperClient()
                                .openLedger(bucketId, BookKeeper.DigestType.CRC32C, new byte[]{});
                Assert.fail("Should fail");
            } catch (BKException.BKNoSuchLedgerExistsException e) {
                // ignore it
            }
        }
    }
}
