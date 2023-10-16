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

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;
import java.util.List;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerMBeanImpl;
import org.apache.bookkeeper.mledger.util.StatsBuckets;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.Replicator;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.stats.ConsumerStatsImpl;
import org.apache.pulsar.common.policies.data.stats.SubscriptionStatsImpl;
import org.apache.pulsar.common.policies.data.stats.TopicStatsImpl;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashMap;
import org.apache.pulsar.metadata.impl.ZKMetadataStore;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class NamespaceStatsAggregatorTest {
    protected PulsarService pulsar;
    private BrokerService broker;
    private ConcurrentOpenHashMap<String, ConcurrentOpenHashMap<String, ConcurrentOpenHashMap<String, Topic>>>
            multiLayerTopicsMap;

    @BeforeMethod(alwaysRun = true)
    public void setup() throws Exception {
        multiLayerTopicsMap = ConcurrentOpenHashMap.<String,
                        ConcurrentOpenHashMap<String, ConcurrentOpenHashMap<String, Topic>>>newBuilder()
                .build();
        pulsar = Mockito.mock(PulsarService.class);
        broker = Mockito.mock(BrokerService.class);
        doReturn(multiLayerTopicsMap).when(broker).getMultiLayerTopicMap();
        Mockito.when(pulsar.getLocalMetadataStore()).thenReturn(Mockito.mock(ZKMetadataStore.class));
        ServiceConfiguration mockConfig = Mockito.mock(ServiceConfiguration.class);
        doReturn(mockConfig).when(pulsar).getConfiguration();
        doReturn(broker).when(pulsar).getBrokerService();
    }

    @Test
    public void testGenerateSubscriptionsStats() {
        // given
        final String namespace = "tenant/cluster/ns";

        // prepare multi-layer topic map
        ConcurrentOpenHashMap bundlesMap = ConcurrentOpenHashMap.newBuilder().build();
        ConcurrentOpenHashMap topicsMap = ConcurrentOpenHashMap.newBuilder().build();
        ConcurrentOpenHashMap subscriptionsMaps = ConcurrentOpenHashMap.newBuilder().build();
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
        Subscription subscription = Mockito.mock(Subscription.class);
        Consumer consumer = Mockito.mock(Consumer.class);
        ConsumerStatsImpl consumerStats = new ConsumerStatsImpl();
        when(consumer.getStats()).thenReturn(consumerStats);
        when(subscription.getConsumers()).thenReturn(List.of(consumer));
        subscriptionsMaps.put("my-subscription", subscription);
        SubscriptionStatsImpl subStats = new SubscriptionStatsImpl();
        TopicStatsImpl topicStats = new TopicStatsImpl();
        topicStats.subscriptions.put("my-subscription", subStats);
        when(topic.getStats(false, false, false)).thenReturn(topicStats);
        when(topic.getBrokerService()).thenReturn(broker);
        when(topic.getSubscriptions()).thenReturn(subscriptionsMaps);
        when(topic.getReplicators()).thenReturn(ConcurrentOpenHashMap.<String,Replicator>newBuilder().build());
        when(topic.getManagedLedger()).thenReturn(ml);
        when(topic.getBacklogQuota(Mockito.any())).thenReturn(Mockito.mock(BacklogQuota.class));
        topicsMap.put("my-topic", topic);
        PrometheusMetricStreams metricStreams = Mockito.spy(new PrometheusMetricStreams());

        // Populate subscriptions stats
        subStats.blockedSubscriptionOnUnackedMsgs = true;
        consumerStats.blockedConsumerOnUnackedMsgs = false; // should not affect blockedSubscriptionOnUnackedMsgs
        consumerStats.unackedMessages = 1;
        consumerStats.msgRateRedeliver = 0.7;
        subStats.consumers.add(0, consumerStats);

        // when
        NamespaceStatsAggregator.generate(pulsar, true, true,
                true, true, metricStreams);

        // then
        verifySubscriptionMetric(metricStreams, "pulsar_subscription_blocked_on_unacked_messages", 1);
        verifyConsumerMetric(metricStreams, "pulsar_consumer_blocked_on_unacked_messages", 0);

        verifySubscriptionMetric(metricStreams, "pulsar_subscription_msg_rate_redeliver", 0.7);
        verifySubscriptionMetric(metricStreams, "pulsar_subscription_unacked_messages", 1L);
    }

    private void verifySubscriptionMetric(PrometheusMetricStreams metricStreams, String metricName, Number value) {
        Mockito.verify(metricStreams).writeSample(metricName,
                value,
                "cluster",
                null,
                "namespace",
                "tenant/cluster/ns",
                "topic",
                "my-topic",
                "partition",
                "-1",
                "subscription",
                "my-subscription");
    }

    private void verifyConsumerMetric(PrometheusMetricStreams metricStreams, String metricName, Number value) {
        Mockito.verify(metricStreams).writeSample(metricName,
                value,
                "cluster",
                null,
                "namespace",
                "tenant/cluster/ns",
                "topic",
                "my-topic",
                "partition",
                "-1",
                "subscription",
                "my-subscription",
                "consumer_name",
                null,
                "consumer_id",
                "0");
    }
}
