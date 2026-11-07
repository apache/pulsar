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
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.service.persistent.PersistentTopicMetrics;
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
    public Object[][] topicMetrics() {
        return new Object[][] {{true}, {false}};
    }

    @Test(dataProvider = "topicMetrics")
    public void testGenerateSubscriptionsStats(boolean includeTopicMetrics) {
        // given
        final String namespace = "tenant/cluster/ns";

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

        // Prepare topic and subscription
        PersistentTopic topic = Mockito.mock(PersistentTopic.class);
        PersistentSubscription subscription = Mockito.mock(PersistentSubscription.class);
        Consumer consumer = Mockito.mock(Consumer.class);
        ConsumerStatsImpl consumerStats = new ConsumerStatsImpl();
        when(consumer.getStats()).thenReturn(consumerStats);
        when(subscription.getConsumers()).thenReturn(List.of(consumer));
        subscriptionsMaps.put("my-subscription", subscription);
        SubscriptionStatsImpl subStats = new SubscriptionStatsImpl();
        TopicStatsImpl topicStats = new TopicStatsImpl();
        topicStats.subscriptions.put("my-subscription", subStats);
        topicStats.bytesInCounter = 9007199254740993L;
        topicStats.bytesOutCounter = 9007199254740993L;
        topicStats.bucketDelayedIndexStats.put("bucket", new TopicMetricBean(
                "pulsar_test_delayed_bucket", 2.5, new String[]{"bucket", "bucket-a"}));
        when(topic.getStats(false, false, false)).thenReturn(topicStats);
        when(topic.getBrokerService()).thenReturn(broker);
        when(topic.getSubscriptions()).thenReturn(subscriptionsMaps);
        when(topic.getReplicators()).thenReturn(new ConcurrentHashMap<>());
        when(topic.getManagedLedger()).thenReturn(ml);
        when(topic.getBacklogQuota(Mockito.any())).thenReturn(Mockito.mock(BacklogQuota.class));
        PersistentTopicMetrics persistentTopicMetrics = new PersistentTopicMetrics();
        when(topic.getPersistentTopicMetrics()).thenReturn(persistentTopicMetrics);
        topicsMap.put("my-topic", topic);
        @Cleanup("releaseAll")
        PrometheusMetricStreams metricStreams = Mockito.spy(new PrometheusMetricStreams());

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
        ByteBuf buffer = Unpooled.buffer();
        try {
            metricStreams.flushAllToStream(new SimpleTextOutputStream(buffer));
            String metrics = buffer.toString(StandardCharsets.UTF_8);
            if (includeTopicMetrics) {
                String subscriptionLabels = "{cluster=\"\",namespace=\"tenant/cluster/ns\",topic=\"my-topic\","
                        + "partition=\"-1\",subscription=\"my-subscription\"";
                assertThat(metrics).contains(
                        "pulsar_subscription_blocked_on_unacked_messages" + subscriptionLabels + "} 1\n",
                        "pulsar_consumer_blocked_on_unacked_messages" + subscriptionLabels
                                + ",consumer_name=\"\",consumer_id=\"0\"} 0\n",
                        "pulsar_subscription_msg_rate_redeliver" + subscriptionLabels + "} 0.700\n",
                        "pulsar_subscription_unacked_messages" + subscriptionLabels + "} 1\n",
                        "pulsar_subscription_storage_backlog_age_seconds" + subscriptionLabels + "} 123\n");
            } else {
                assertThat(metrics).contains("pulsar_test_delayed_bucket{cluster=\"\","
                        + "namespace=\"tenant/cluster/ns\",bucket=\"bucket-a\"} 2.500\n");
            }
            assertThat(metrics).contains(
                    "pulsar_broker_in_bytes_total{cluster=\"\",system_topic=\"false\"} 9007199254740993\n",
                    "pulsar_broker_in_bytes_total{cluster=\"\",system_topic=\"true\"} 0\n",
                    "pulsar_broker_out_bytes_total{cluster=\"\",system_subscription=\"false\"} 9007199254740993\n",
                    "pulsar_broker_out_bytes_total{cluster=\"\",system_subscription=\"true\"} 0\n");
        } finally {
            buffer.release();
        }
    }
}
