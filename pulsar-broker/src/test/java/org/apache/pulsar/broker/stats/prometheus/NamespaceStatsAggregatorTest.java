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
package org.apache.pulsar.broker.stats.prometheus;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Cleanup;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerMBeanImpl;
import org.apache.bookkeeper.mledger.util.StatsBuckets;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.GetStatsOptions;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.service.persistent.PersistentTopicMetrics;
import org.apache.pulsar.broker.service.persistent.SystemTopic;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.stats.ConsumerStatsImpl;
import org.apache.pulsar.common.policies.data.stats.SubscriptionStatsImpl;
import org.apache.pulsar.common.policies.data.stats.TopicMetricBean;
import org.apache.pulsar.common.policies.data.stats.TopicStatsImpl;
import org.apache.pulsar.common.util.SimpleTextOutputStream;
import org.apache.pulsar.metadata.impl.ZKMetadataStore;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class NamespaceStatsAggregatorTest {
    protected PulsarService pulsar;
    private BrokerService broker;
    private Map<String, Map<String, Map<String, Topic>>> multiLayerTopicsMap;

    @BeforeMethod(alwaysRun = true)
    public void setup() throws Exception {
        multiLayerTopicsMap = new ConcurrentHashMap<>();
        pulsar = Mockito.mock(PulsarService.class);
        broker = Mockito.mock(BrokerService.class);
        doReturn(multiLayerTopicsMap).when(broker).getMultiLayerTopicsMap();
        Mockito.when(pulsar.getLocalMetadataStore()).thenReturn(Mockito.mock(ZKMetadataStore.class));
        ServiceConfiguration mockConfig = Mockito.mock(ServiceConfiguration.class);
        doReturn(mockConfig).when(pulsar).getConfiguration();
        doReturn(broker).when(pulsar).getBrokerService();
    }

    @DataProvider
    public Object[][] persistentTopicMetrics() {
        return new Object[][] {
                {PersistentTopic.class, true, true},
                {PersistentTopic.class, true, false},
                {SystemTopic.class, true, true},
                {SystemTopic.class, true, false},
                {CustomPersistentTopic.class, false, true},
                {CustomPersistentTopic.class, false, false}
        };
    }

    @Test(dataProvider = "persistentTopicMetrics")
    public void testGenerateSubscriptionsStats(Class<? extends PersistentTopic> topicClass,
                                               boolean reuseTopicBacklogSize, boolean includeTopicMetrics) {
        // given
        final String namespace = "tenant/cluster/ns";
        final long ledgerBacklogSize = 321L;
        final long topicBacklogSize = 987L;

        // prepare multi-layer topic map
        final var bundlesMap = new ConcurrentHashMap<String, Map<String, Topic>>();
        final var topicsMap = new ConcurrentHashMap<String, Topic>();
        final var subscriptionsMaps = new ConcurrentHashMap<String, PersistentSubscription>();
        bundlesMap.put("my-bundle", topicsMap);
        multiLayerTopicsMap.put(namespace, bundlesMap);

        // Prepare managed ledger
        ManagedLedger ml = Mockito.mock(ManagedLedger.class);
        ManagedLedgerMBeanImpl mlBeanStats = Mockito.mock(ManagedLedgerMBeanImpl.class);
        StatsBuckets statsBuckets = new StatsBuckets(ManagedLedgerMBeanImpl.ENTRY_LATENCY_BUCKETS_USEC);
        when(mlBeanStats.getInternalAddEntryLatencyBuckets()).thenReturn(statsBuckets);
        when(mlBeanStats.getInternalLedgerAddEntryLatencyBuckets()).thenReturn(statsBuckets);
        when(mlBeanStats.getInternalEntrySizeBuckets()).thenReturn(
                new StatsBuckets(ManagedLedgerMBeanImpl.ENTRY_SIZE_BUCKETS_BYTES));
        when(ml.getStats()).thenReturn(mlBeanStats);
        when(ml.getEstimatedBacklogSize()).thenReturn(ledgerBacklogSize);

        // Prepare topic and subscription
        PersistentTopic topic = Mockito.mock(topicClass);
        PersistentSubscription subscription = Mockito.mock(PersistentSubscription.class);
        Consumer consumer = Mockito.mock(Consumer.class);
        ConsumerStatsImpl consumerStats = new ConsumerStatsImpl();
        when(consumer.getStats()).thenReturn(consumerStats);
        when(subscription.getConsumers()).thenReturn(List.of(consumer));
        subscriptionsMaps.put("my-subscription", subscription);
        SubscriptionStatsImpl subStats = new SubscriptionStatsImpl();
        TopicStatsImpl topicStats = new TopicStatsImpl();
        topicStats.backlogSize = topicBacklogSize;
        topicStats.subscriptions.put("my-subscription", subStats);
        topicStats.bytesInCounter = 9007199254740993L;
        topicStats.bytesOutCounter = 9007199254740993L;
        topicStats.bucketDelayedIndexStats.put("bucket", new TopicMetricBean(
                "pulsar_test_delayed_bucket", 2.5, new String[]{"bucket", "bucket-a"}));
        GetStatsOptions statsOptions = GetStatsOptions.builder().excludeLastPublishTime(true).build();
        when(topic.getStats(statsOptions)).thenReturn(topicStats);
        when(topic.getBrokerService()).thenReturn(broker);
        when(topic.getSubscriptions()).thenReturn(subscriptionsMaps);
        when(topic.getReplicators()).thenReturn(new ConcurrentHashMap<>());
        when(topic.getManagedLedger()).thenReturn(ml);
        when(topic.getBacklogQuota(Mockito.any())).thenReturn(Mockito.mock(BacklogQuota.class));
        PersistentTopicMetrics persistentTopicMetrics = new PersistentTopicMetrics();
        when(topic.getPersistentTopicMetrics()).thenReturn(persistentTopicMetrics);
        topicsMap.put("my-topic", topic);
        @Cleanup("releaseAll")
        PrometheusMetricStreams metricStreams = new PrometheusMetricStreams();

        // Populate subscriptions stats
        ServiceConfiguration config = pulsar.getConfiguration();
        doReturn(true).when(config).isExposeSubscriptionBacklogAgeInPrometheus();
        subStats.blockedSubscriptionOnUnackedMsgs = true;
        subStats.oldestBacklogMessageAgeSeconds = 123;
        consumerStats.blockedConsumerOnUnackedMsgs = false; // should not affect blockedSubscriptionOnUnackedMsgs
        consumerStats.unackedMessages = 1;
        consumerStats.msgRateRedeliver = 0.7;
        subStats.consumers.add(0, consumerStats);

        // when
        NamespaceStatsAggregator.generate(pulsar, includeTopicMetrics, true,
                true, true, metricStreams);

        // then
        Mockito.verify(topic).getStats(statsOptions);
        Mockito.verify(ml, Mockito.times(reuseTopicBacklogSize ? 0 : 1)).getEstimatedBacklogSize();
        long expectedBacklogSize = reuseTopicBacklogSize ? topicBacklogSize : ledgerBacklogSize;
        ByteBuf buffer = Unpooled.buffer();
        try {
            metricStreams.flushAllToStream(new SimpleTextOutputStream(buffer));
            List<String> lines = buffer.toString(StandardCharsets.UTF_8).lines().toList();
            String namespaceLabels = "{cluster=\"\",namespace=\"tenant/cluster/ns\"";
            if (includeTopicMetrics) {
                String topicLabels = namespaceLabels + ",topic=\"my-topic\",partition=\"-1\"";
                String subscriptionLabels = topicLabels + ",subscription=\"my-subscription\"";
                assertThat(lines).containsOnlyOnce(
                        "pulsar_storage_backlog_size" + topicLabels + "} " + expectedBacklogSize,
                        "pulsar_subscription_blocked_on_unacked_messages" + subscriptionLabels + "} 1",
                        "pulsar_consumer_blocked_on_unacked_messages" + subscriptionLabels
                                + ",consumer_name=\"\",consumer_id=\"0\"} 0",
                        "pulsar_subscription_msg_rate_redeliver" + subscriptionLabels + "} 0.700",
                        "pulsar_subscription_unacked_messages" + subscriptionLabels + "} 1",
                        "pulsar_subscription_storage_backlog_age_seconds" + subscriptionLabels + "} 123");
            } else {
                assertThat(lines).containsOnlyOnce(
                        "pulsar_storage_backlog_size" + namespaceLabels + "} " + expectedBacklogSize,
                        "pulsar_test_delayed_bucket" + namespaceLabels + ",bucket=\"bucket-a\"} 2.500");
            }
            assertThat(lines).containsOnlyOnce(
                    "pulsar_broker_in_bytes_total{cluster=\"\",system_topic=\"false\"} 9007199254740993",
                    "pulsar_broker_in_bytes_total{cluster=\"\",system_topic=\"true\"} 0",
                    "pulsar_broker_out_bytes_total{cluster=\"\",system_subscription=\"false\"} 9007199254740993",
                    "pulsar_broker_out_bytes_total{cluster=\"\",system_subscription=\"true\"} 0");
        } finally {
            buffer.release();
        }
    }

    private static final class CustomPersistentTopic extends PersistentTopic {
        private CustomPersistentTopic(String topic, ManagedLedger ledger, BrokerService brokerService) {
            super(topic, ledger, brokerService);
        }
    }
}
