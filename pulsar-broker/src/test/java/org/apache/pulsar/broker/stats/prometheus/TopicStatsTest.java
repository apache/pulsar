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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.common.policies.data.stats.TopicMetricBean;
import org.apache.pulsar.common.util.SimpleTextOutputStream;
import org.apache.pulsar.compaction.CompactionRecord;
import org.apache.pulsar.compaction.CompactorMXBean;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class TopicStatsTest {
    private static final String TOPIC = "persistent://tenant/ns/topic-partition-7";
    private static final String BASE_LABELS = "cluster=\"cluster\",namespace=\"tenant/ns\","
            + "topic=\"persistent://tenant/ns/topic\",partition=\"7\"";

    @DataProvider
    public Object[][] topicLabels() {
        return new Object[][] {
                {TOPIC, false, "topic=\"" + TOPIC + "\""},
                {TOPIC, true, "topic=\"persistent://tenant/ns/topic\",partition=\"7\""},
                {"persistent://tenant/ns/topic", true,
                        "topic=\"persistent://tenant/ns/topic\",partition=\"-1\""},
                {"persistent://tenant/ns/topic-partition-7-partition-9", true,
                        "topic=\"persistent://tenant/ns/topic\",partition=\"7-partition-9\""},
                {"persistent://tenant/ns/topic-partition-", true,
                        "topic=\"persistent://tenant/ns/topic\",partition=\"\""},
                {"-partition-7", true, "topic=\"-partition-7\",partition=\"-1\""}
        };
    }

    @Test(dataProvider = "topicLabels")
    public void testTopicLabels(String topic, boolean split, String expectedTopicLabels) {
        String expectedLabels = "cluster=\"cluster\",namespace=\"tenant/ns\"," + expectedTopicLabels
                + ",rack=\"rack\\\"a\",empty=\"\"";
        String[] customLabels = {"rack", "rack\"a", "empty", null};

        String output = render(new TopicStats(), topic, split, false, customLabels, null);

        assertThat(samples(output)).as("All topic samples use the same ordered labels")
                .isNotEmpty()
                .allSatisfy(line -> assertThat(line).contains("{" + expectedLabels));
        assertThat(samples(output)).contains(
                "pulsar_subscriptions_count{" + expectedLabels + "} 0",
                "pulsar_rate_in{" + expectedLabels + "} 0.0");
        assertThat(customLabels).containsExactly("rack", "rack\"a", "empty", null);
    }

    @Test
    public void testNullAndEmptyCustomLabelsProduceIdenticalOutput() {
        TopicStats stats = populatedStats();

        assertThat(render(stats, TOPIC, true, true, null, null))
                .isEqualTo(render(stats, TOPIC, true, true, new String[0], null));
    }

    @Test
    public void testNestedLabelsAndValues() {
        TopicStats stats = populatedStats();
        CompactorMXBean compactor = mock(CompactorMXBean.class);
        when(compactor.getCompactionRecordForTopic(TOPIC)).thenReturn(Optional.of(new CompactionRecord()));
        String labels = BASE_LABELS + ",rack=\"rack-a\"";
        String subscriptionLabels = labels + ",subscription=\"sub\\\"a\"";
        String consumerLabels = subscriptionLabels + ",consumer_name=\"consumer\\\"a\",consumer_id=\"9876543210\"";
        String producerLabels = labels + ",producer_name=\"producer\\\"a\",producer_id=\"1234567890\"";

        String output = render(stats, TOPIC, true, true, new String[]{"rack", "rack-a"}, compactor);
        List<String> samples = samples(output);

        assertThat(samples).contains(
                "pulsar_subscriptions_count{" + labels + "} 1",
                "pulsar_in_bytes_total{" + labels + "} 9223372036854775807",
                "pulsar_in_messages_total{" + labels + "} 9007199254740993",
                "pulsar_rate_in{" + labels + "} 1.250",
                "pulsar_storage_backlog_quota_exceeded_evictions_total{" + labels + ",quota_type=\"size\"} 11",
                "pulsar_storage_backlog_quota_exceeded_evictions_total{" + labels + ",quota_type=\"time\"} 12",
                "pulsar_producer_msg_rate_in{" + producerLabels + "} 2.500",
                "pulsar_producer_msg_throughput_in{" + producerLabels + "} 3.750",
                "pulsar_producer_msg_average_Size{" + producerLabels + "} 4.0",
                "pulsar_subscription_back_log{" + subscriptionLabels + "} 9007199254740993",
                "pulsar_subscription_storage_backlog_age_seconds{" + subscriptionLabels + "} 17",
                "pulsar_subscription_blocked_on_unacked_messages{" + subscriptionLabels + "} 1",
                "pulsar_out_bytes_total{" + subscriptionLabels + "} 9007199254740993",
                "pulsar_consumer_unacked_messages{" + consumerLabels + "} 9007199254740993",
                "pulsar_consumer_msg_rate_out{" + consumerLabels + "} 5.250",
                "pulsar_consumer_blocked_on_unacked_messages{" + consumerLabels + "} 0",
                "pulsar_out_bytes_total{" + consumerLabels + "} 9223372036854775807",
                "pulsar_replication_backlog{" + labels + ",remote_cluster=\"remote\\\"a\"} 9007199254740993",
                "pulsar_compaction_removed_event_count{" + labels + "} 19",
                "pulsar_compaction_read_throughput{" + labels + "} 6.500",
                "pulsar_test_subscription_bucket{" + subscriptionLabels + ",bucket=\"bucket\\\"a\"} 8.250");
        assertThat(samples).containsOnlyOnce("pulsar_test_subscription_bucket{" + subscriptionLabels
                        + ",bucket=\"bucket\\\"a\"} 8.250");
        assertThat(samples.stream().filter(line -> line.equals("pulsar_test_topic_bucket{" + labels
                + ",bucket=\"bucket\\\"a\"} 7.500")).count())
                .as("Compaction currently repeats the topic delayed-index samples").isEqualTo(2);
        assertThat(samples).contains(
                "pulsar_subscription_dispatch_throttled_msg_events{" + subscriptionLabels
                        + ",reason=\"subscription\"} 21",
                "pulsar_subscription_dispatch_throttled_bytes_events{" + subscriptionLabels
                        + ",reason=\"subscription\"} 22",
                "pulsar_subscription_dispatch_throttled_msg_events{" + subscriptionLabels
                        + ",reason=\"topic\"} 23",
                "pulsar_subscription_dispatch_throttled_bytes_events{" + subscriptionLabels
                        + ",reason=\"topic\"} 24",
                "pulsar_subscription_dispatch_throttled_msg_events{" + subscriptionLabels
                        + ",reason=\"broker\"} 25",
                "pulsar_subscription_dispatch_throttled_bytes_events{" + subscriptionLabels
                        + ",reason=\"broker\"} 26");
        assertThat(output.lines().filter(line -> line.startsWith("# TYPE ")).toList())
                .as("One TYPE header per metric, including shared consumer and subscription counters")
                .doesNotHaveDuplicates();
    }

    @DataProvider
    public Object[][] doubleValues() {
        return new Object[][] {
                {1.25, "1.250"},
                {-1.25, "-1.250"},
                {-0.0, "0.0"},
                {Double.NaN, "0.0"}
        };
    }

    @Test(dataProvider = "doubleValues")
    public void testDoubleFormatting(double value, String expectedValue) {
        TopicStats stats = new TopicStats();
        stats.rateIn = value;

        assertThat(samples(render(stats, TOPIC, true, false, null, null)))
                .contains("pulsar_rate_in{" + BASE_LABELS + "} " + expectedValue);
    }

    @Test
    public void testSubscriptionBacklogAgeIsOptional() {
        TopicStats stats = populatedStats();
        assertThat(render(stats, TOPIC, true, false, null, null))
                .doesNotContain("pulsar_subscription_storage_backlog_age_seconds");
        stats.subscriptionStats.values().iterator().next().backlogAgeSeconds = -1;
        assertThat(render(stats, TOPIC, true, true, null, null))
                .doesNotContain("pulsar_subscription_storage_backlog_age_seconds");
    }

    private static TopicStats populatedStats() {
        TopicStats stats = new TopicStats();
        stats.subscriptionsCount = 1;
        stats.rateIn = 1.25;
        stats.bytesInCounter = Long.MAX_VALUE;
        stats.msgInCounter = 9007199254740993L;
        stats.sizeBasedBacklogQuotaExceededEvictionCount = 11;
        stats.timeBasedBacklogQuotaExceededEvictionCount = 12;
        stats.compactionRemovedEventCount = 19;
        stats.compactionReadThroughput = 6.5;
        stats.bucketDelayedIndexStats.put("topic-bucket", new TopicMetricBean("pulsar_test_topic_bucket",
                7.5, new String[]{"bucket", "bucket\"a"}));

        AggregatedProducerStats producer = new AggregatedProducerStats();
        producer.producerId = 1234567890L;
        producer.msgRateIn = 2.5;
        producer.msgThroughputIn = 3.75;
        producer.averageMsgSize = 4;
        stats.producerStats.put("producer\"a", producer);

        Consumer consumer = mock(Consumer.class);
        when(consumer.consumerName()).thenReturn("consumer\"a");
        when(consumer.consumerId()).thenReturn(9876543210L);
        AggregatedConsumerStats consumerStats = new AggregatedConsumerStats();
        consumerStats.unackedMessages = 9007199254740993L;
        consumerStats.msgRateOut = 5.25;
        consumerStats.bytesOutCounter = Long.MAX_VALUE;
        AggregatedSubscriptionStats subscription = new AggregatedSubscriptionStats();
        subscription.msgBacklog = 9007199254740993L;
        subscription.backlogAgeSeconds = 17;
        subscription.bytesOutCounter = 9007199254740993L;
        subscription.blockedSubscriptionOnUnackedMsgs = true;
        subscription.dispatchThrottledMsgEventsBySubscriptionLimit = 21;
        subscription.dispatchThrottledBytesEventsBySubscriptionLimit = 22;
        subscription.dispatchThrottledMsgEventsByTopicLimit = 23;
        subscription.dispatchThrottledBytesEventsByTopicLimit = 24;
        subscription.dispatchThrottledMsgEventsByBrokerLimit = 25;
        subscription.dispatchThrottledBytesEventsByBrokerLimit = 26;
        subscription.consumerStat.put(consumer, consumerStats);
        subscription.bucketDelayedIndexStats.put("subscription-bucket",
                new TopicMetricBean("pulsar_test_subscription_bucket", 8.25, new String[]{"bucket", "bucket\"a"}));
        stats.subscriptionStats.put("sub\"a", subscription);

        AggregatedReplicationStats replication = new AggregatedReplicationStats();
        replication.replicationBacklog = 9007199254740993L;
        stats.replicationStats.put("remote\"a", replication);
        return stats;
    }

    private static List<String> samples(String output) {
        return output.lines().filter(line -> !line.startsWith("#")).toList();
    }

    private static String render(TopicStats stats, String topic, boolean split, boolean exposeBacklogAge,
                                 String[] customLabels, CompactorMXBean compactor) {
        PrometheusMetricStreams streams = new PrometheusMetricStreams();
        ByteBuf buffer = Unpooled.buffer();
        try {
            TopicStats.printTopicStats(streams, stats, Optional.ofNullable(compactor), "cluster", "tenant/ns",
                    topic, split, exposeBacklogAge, customLabels);
            streams.flushAllToStream(new SimpleTextOutputStream(buffer));
            return buffer.toString(StandardCharsets.UTF_8);
        } finally {
            buffer.release();
            streams.releaseAll();
        }
    }
}
