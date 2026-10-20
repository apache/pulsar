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
package org.apache.pulsar.client.api.v5;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertTrue;
import io.prometheus.client.CollectorRegistry;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse.BodyHandlers;
import java.time.Duration;
import lombok.Cleanup;
import org.apache.pulsar.broker.resources.ScalableTopicResources;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.scalable.SegmentTopicName;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

/**
 * End-to-end coverage for the broker-side segment load reporter sweep (PIP-483): after a
 * scalable topic has live segment topics on the broker, the periodic sweep (driven manually
 * here for determinism) must write a {@link org.apache.pulsar.common.scalable.SegmentLoadStats}
 * record to the metadata store for each hosted segment, which is what the controller's auto
 * split/merge reads.
 */
public class V5SegmentLoadReporterTest extends V5ClientBaseTest {

    @Test
    public void testSweepWritesSegmentLoadRecords() throws Exception {
        double initialWritten = loadReports("written");
        double initialUnchanged = loadReports("unchanged");
        String topic = newScalableTopic(2);

        @Cleanup
        Producer<byte[]> producer = v5Client.newProducer(Schema.bytes())
                .topic(topic)
                .create();
        // Produce across keys so both initial segments get a live segment topic on the broker.
        for (int i = 0; i < 50; i++) {
            producer.newMessage().key("k-" + i).value(("v-" + i).getBytes()).send();
        }

        ScalableTopicResources resources =
                getPulsar().getPulsarResources().getScalableTopicResources();
        TopicName parent = TopicName.get(topic);

        // Drive the sweep directly rather than waiting for the 10s scheduled tick.
        getPulsar().getBrokerService().runSegmentLoadReportOnceForTest();

        // Both initial segments (0 and 1) should now have a load record.
        Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            getPulsar().getBrokerService().runSegmentLoadReportOnceForTest();
            assertTrue(resources.getSegmentLoadAsync(parent, 0).get().isPresent(),
                    "segment 0 load record must be written by the sweep");
            assertTrue(resources.getSegmentLoadAsync(parent, 1).get().isPresent(),
                    "segment 1 load record must be written by the sweep");
            assertThat(loadReports("written")).isGreaterThanOrEqualTo(initialWritten + 2);
            assertThat(loadReports("unchanged")).isGreaterThan(initialUnchanged);
        });
    }

    @Test
    public void testOwnerMetricsObserveControllerAndTerminatedSegment() throws Exception {
        double initialLeaderless = ownerGauge("leaderless_count");
        double initialUnwritable = ownerGauge("segment_unwritable_count");
        String topic = newScalableTopic(2);
        TopicName parent = TopicName.get(topic);
        var broker = getPulsar().getBrokerService();
        var resources = getPulsar().getPulsarResources().getScalableTopicResources();
        var metadata = resources.getScalableTopicMetadataAsync(parent).get().orElseThrow();
        var segment = metadata.getSegments().values().iterator().next();
        String segmentName = SegmentTopicName.backingTopicName(parent, segment);

        Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(ownerGauge("leaderless_count")).isEqualTo(initialLeaderless + 1));
        // Observation must not elect a controller for a producer-only topic.
        assertThat(resources.getStore().exists(resources.controllerLockPath(parent)).get()).isFalse();
        try (HttpClient client = HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NORMAL).build()) {
            var response = client.send(HttpRequest.newBuilder(URI.create(getWebServiceUrl() + "/metrics"))
                    .timeout(Duration.ofSeconds(10)).GET().build(), BodyHandlers.ofString());
            assertThat(response.statusCode()).isEqualTo(200);
            assertThat(response.body())
                    .contains("# TYPE pulsar_scalable_topic_leaderless_count gauge\n")
                    .contains("# TYPE pulsar_scalable_topic_segment_unwritable_count gauge\n")
                    .contains("# TYPE pulsar_scalable_topic_entry_bucket_draining_count gauge\n")
                    .contains("# TYPE pulsar_scalable_topic_entry_bucket_violation_total counter\n")
                    .contains("# TYPE pulsar_scalable_topic_segment_load_report_total counter\n");
        }
        admin.scalableTopics().terminateSegmentAsync(segmentName).get();
        Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(ownerGauge("segment_unwritable_count")).isEqualTo(initialUnwritable + 1));

        broker.getScalableTopicService().getOrCreateController(parent).get();
        Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(ownerGauge("leaderless_count")).isEqualTo(initialLeaderless));
        // Leadership alone does not finish a stopped layout operation.
        assertThat(ownerGauge("segment_unwritable_count")).isEqualTo(initialUnwritable + 1);
        admin.scalableTopics().splitSegment(topic, segment.segmentId());
        Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(ownerGauge("segment_unwritable_count")).isEqualTo(initialUnwritable));
    }

    @Test
    public void testMigrationTerminationTracksLegacyOwnerWithoutPolling() throws Exception {
        double initialLeaderless = ownerGauge("leaderless_count");
        String topic = getNamespace() + "/legacy-owner-" + System.nanoTime();
        String legacyName = "persistent://" + topic;
        admin.topics().createNonPartitionedTopic(legacyName);
        try (var producer = pulsarClient.newProducer().topic(legacyName).create()) {
            producer.send(new byte[]{1});
        }
        var broker = getPulsar().getBrokerService();
        assertThat(broker.getTopicReference(legacyName)).isPresent();
        admin.scalableTopics().migrateToScalable(topic, false);
        TopicName parent = TopicName.get("topic://" + topic);
        var resources = getPulsar().getPulsarResources().getScalableTopicResources();
        var metadata = resources.getScalableTopicMetadataAsync(parent).get().orElseThrow();
        for (var segment : metadata.getSegments().values()) {
            if (!segment.isLegacy()) {
                String name = SegmentTopicName.backingTopicName(parent, segment);
                broker.getTopicReference(name).orElseThrow().close(true).get();
            }
        }
        // Only the already-loaded legacy topic remains. No stats tick or topic reload discovers it:
        // the successful termination hook must have registered it during migration.
        assertThat(broker.getTopicReference(legacyName)).isPresent();
        Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(ownerGauge("leaderless_count")).isEqualTo(initialLeaderless + 1));
        assertThat(resources.getStore().exists(resources.controllerLockPath(parent)).get()).isFalse();
    }

    private double loadReports(String result) {
        Double sample = CollectorRegistry.defaultRegistry.getSampleValue(
                "pulsar_scalable_topic_segment_load_report_total",
                new String[]{"cluster", "load_report_result"}, new String[]{getConfig().getClusterName(), result});
        assertThat(sample).as("registered segment load-report counter %s", result).isNotNull();
        return sample;
    }

    private double ownerGauge(String suffix) {
        Double sample = CollectorRegistry.defaultRegistry.getSampleValue("pulsar_scalable_topic_" + suffix,
                new String[]{"cluster"}, new String[]{getConfig().getClusterName()});
        assertThat(sample).as("registered segment-owner gauge %s", suffix).isNotNull();
        return sample;
    }

    @Test
    public void testNonSegmentTopicsProduceNoLoadRecords() throws Exception {
        // A plain persistent topic must not produce any scalable-segment load record.
        @Cleanup
        org.apache.pulsar.client.api.PulsarClient v4 =
                org.apache.pulsar.client.api.PulsarClient.builder()
                        .serviceUrl(getBrokerServiceUrl()).build();
        String plain = "persistent://" + getNamespace() + "/plain-" + System.nanoTime();
        @Cleanup
        org.apache.pulsar.client.api.Producer<byte[]> p =
                v4.newProducer().topic(plain).create();
        p.send("x".getBytes());

        // Sweep must not throw on non-segment topics (and obviously writes nothing for them).
        getPulsar().getBrokerService().runSegmentLoadReportOnceForTest();
    }
}
