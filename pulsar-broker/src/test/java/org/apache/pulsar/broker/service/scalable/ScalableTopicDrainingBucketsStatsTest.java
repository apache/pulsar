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
package org.apache.pulsar.broker.service.scalable;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.service.SharedPulsarBaseTest;
import org.apache.pulsar.broker.service.StickyKeyConsumerSelector;
import org.apache.pulsar.broker.service.persistent.PersistentEntryBucketDispatcherMultipleConsumers;
import org.apache.pulsar.broker.service.persistent.PersistentStickyKeyDispatcherMultipleConsumers;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.KeySharedPolicy;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.Range;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.ConsumerBuilderImpl;
import org.apache.pulsar.client.impl.ProducerBuilderImpl;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.policies.data.AutoScalePolicyOverride;
import org.awaitility.Awaitility;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/** End-to-end coverage distinguishing classic per-key drains from actual entry-bucket handoffs. */
@Test(groups = "broker")
public class ScalableTopicDrainingBucketsStatsTest extends SharedPulsarBaseTest {

    @DataProvider
    public Object[][] dispatchModes() {
        return new Object[][]{{false}, {true}};
    }

    @Test(dataProvider = "dispatchModes", timeOut = 60_000)
    public void testDrainingBucketsReflectActualDispatcher(boolean entryBucketDispatch) throws Exception {
        String base = getNamespace() + "/handoff";
        String scalable = "topic://" + base;
        String backingTopic;
        List<Range> buckets = List.of();
        if (entryBucketDispatch) {
            admin.scalableTopics().createScalableTopic(scalable, 1);
            admin.scalableTopics().setAutoScalePolicy(scalable,
                    AutoScalePolicyOverride.builder().enabled(false).build());
            backingTopic = admin.scalableTopics().getStats(scalable).getLayout().getSegments().get(0L).getName();
            List<Integer> splits = admin.scalableTopics().getMetadata(scalable)
                    .getSegments().get(0L).getEntryBucketSplits();
            assertThat(splits).as("native positive control needs multiple entry buckets").isNotEmpty();
            buckets = bucketRanges(splits);
        } else {
            backingTopic = "persistent://" + base;
            admin.topics().createNonPartitionedTopic(backingTopic);
        }
        String subscription = "handoff-sub";
        try (Producer<String> producer = producer(backingTopic, entryBucketDispatch);
             Consumer<String> first = consumer(backingTopic, subscription, "first", buckets)) {
            PersistentTopic topic = (PersistentTopic) getTopicReference(backingTopic).orElseThrow();
            var dispatcher = (PersistentStickyKeyDispatcherMultipleConsumers)
                    topic.getSubscription(subscription).getDispatcher();
            assertThat(dispatcher instanceof PersistentEntryBucketDispatcherMultipleConsumers)
                    .isEqualTo(entryBucketDispatch);

            // Discover an actual hash assigned to the second consumer, then leave and rejoin
            // with the same name. This creates a deterministic pending-ack handoff without
            // inserting private drain-tracker state or guessing which key a join will move.
            String key;
            try (Consumer<String> probe = consumer(backingTopic, subscription, "second", buckets)) {
                key = keyForConsumer(dispatcher.getSelector(), "second");
            }
            Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                    assertThat(topic.getSubscription(subscription).getConsumers()).hasSize(1));
            producer.newMessage().key(key).value("pending").send();
            Message<String> pending = first.receive(10, TimeUnit.SECONDS);
            assertThat(pending).isNotNull();
            assertThat(pending.getKey()).isEqualTo(key);

            try (Consumer<String> second = consumer(backingTopic, subscription, "second", buckets)) {
                Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                        assertThat(drainingHashes(topic, subscription)).isPositive());
                int classicDrains = drainingHashes(topic, subscription);
                if (!entryBucketDispatch) {
                    // Force migration retains the active classic AUTO_SPLIT subscription on the
                    // legacy segment. Its per-key hash drains must not become bucket diagnostics.
                    admin.scalableTopics().migrateToScalable(base, true);
                    admin.scalableTopics().setAutoScalePolicy(scalable,
                            AutoScalePolicyOverride.builder().enabled(false).build());
                }

                int expectedBuckets = entryBucketDispatch ? classicDrains : 0;
                assertThat(admin.scalableTopics().getSegmentStats(scalable, 0L)
                        .getSubscriptions().get(subscription).getDrainingBuckets()).isEqualTo(expectedBuckets);
                assertThat(admin.scalableTopics().getStats(scalable).getSubscriptions().get(subscription)
                        .getSegments().get(0L).getDrainingBuckets()).isEqualTo(expectedBuckets);
                assertThat(drainingHashes(topic, subscription))
                        .as("stats must not mutate the actual handoff").isEqualTo(classicDrains);
                assertThat(topic.getSubscription(subscription).getDispatcher()).isSameAs(dispatcher);

                first.acknowledge(pending);
                Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
                    assertThat(drainingHashes(topic, subscription)).isZero();
                    assertThat(admin.scalableTopics().getSegmentStats(scalable, 0L)
                            .getSubscriptions().get(subscription).getDrainingBuckets()).isZero();
                });
            }
        }
    }

    private static int drainingHashes(PersistentTopic topic, String subscription) throws Exception {
        return topic.asyncGetStats(ScalableTopicService.SEGMENT_STATS_OPTIONS).get(10, TimeUnit.SECONDS)
                .getSubscriptions().get(subscription).getDrainingHashesCount();
    }

    private Producer<String> producer(String topic, boolean entryBucketDispatch) throws Exception {
        ProducerBuilder<String> builder = pulsarClient.newProducer(Schema.STRING)
                .topic(topic).enableBatching(false);
        if (!entryBucketDispatch) {
            return builder.create();
        }
        // Use the same segment-specific client hook as the V5 SDK; the public V4 SDK
        // intentionally rejects segment:// topics before making a broker request.
        return ((PulsarClientImpl) pulsarClient)
                .createSegmentProducerAsync(((ProducerBuilderImpl<String>) builder).getConf(), Schema.STRING)
                .get(10, TimeUnit.SECONDS);
    }

    private Consumer<String> consumer(String topic, String subscription, String name, List<Range> buckets)
            throws Exception {
        ConsumerBuilder<String> builder = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic).subscriptionName(subscription).consumerName(name)
                .subscriptionType(SubscriptionType.Key_Shared)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .acknowledgmentGroupTime(0, TimeUnit.MILLISECONDS);
        if (buckets.isEmpty()) {
            return builder.keySharedPolicy(KeySharedPolicy.autoSplitHashRange()).subscribe();
        }
        builder.keySharedPolicy(KeySharedPolicy.stickyHashRange().ranges(buckets.toArray(Range[]::new)));
        var conf = ((ConsumerBuilderImpl<String>) builder).getConf();
        conf.setEntryBucketDispatch(true);
        return ((PulsarClientImpl) pulsarClient)
                .subscribeSegmentAsync(conf, Schema.STRING)
                .get(10, TimeUnit.SECONDS);
    }

    private static String keyForConsumer(StickyKeyConsumerSelector selector, String consumerName) {
        for (int i = 0; i < 100_000; i++) {
            String key = "key-" + i;
            var selected = selector.select(selector.makeStickyKeyHash(key.getBytes(UTF_8)));
            if (selected != null && consumerName.equals(selected.consumerName())) {
                return key;
            }
        }
        throw new AssertionError("No key assigned to " + consumerName);
    }

    private static List<Range> bucketRanges(List<Integer> splits) {
        List<Range> ranges = new ArrayList<>();
        int start = 0;
        for (int split : splits) {
            ranges.add(Range.of(start, split - 1));
            start = split;
        }
        ranges.add(Range.of(start, 0xffff));
        return ranges;
    }
}
