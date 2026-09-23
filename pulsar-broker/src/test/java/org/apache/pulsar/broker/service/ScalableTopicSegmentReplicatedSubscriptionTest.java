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

import static org.assertj.core.api.Assertions.assertThat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import lombok.Cleanup;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ScalableTopicMetadata.SegmentInfo;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.scalable.HashRange;
import org.apache.pulsar.common.scalable.SegmentTopicName;
import org.assertj.core.api.SoftAssertions;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * The segment DAG of a scalable topic is independent per cluster, so no remote cluster can take part in a
 * replicated-subscription snapshot of a {@code segment://} topic. A subscription that asks for its state to be
 * replicated must therefore not turn on the replicated subscriptions controller of the segment, which would
 * otherwise keep writing snapshot request markers into it that nothing can ever answer.
 */
@Test(groups = "broker-replication")
public class ScalableTopicSegmentReplicatedSubscriptionTest extends OneWayReplicatorTestBase {

    @Override
    @BeforeClass(alwaysRun = true, timeOut = 300000)
    public void setup() throws Exception {
        super.setup();
    }

    @Override
    @AfterClass(alwaysRun = true, timeOut = 300000)
    public void cleanup() throws Exception {
        super.cleanup();
    }

    private String singleSegmentTopic(String scalableTopic) throws Exception {
        List<String> segmentTopics = new ArrayList<>();
        for (SegmentInfo segment : admin1.scalableTopics().getMetadata(scalableTopic).getSegments().values()) {
            HashRange range = HashRange.of(segment.getHashRange().getStart(), segment.getHashRange().getEnd());
            segmentTopics.add(SegmentTopicName.fromParent(TopicName.get(scalableTopic), range,
                    segment.getSegmentId()).toString());
        }
        assertThat(segmentTopics).hasSize(1);
        return segmentTopics.get(0);
    }

    private static List<Integer> markerTypes(ManagedLedger ledger) throws Exception {
        List<Integer> markerTypes = new ArrayList<>();
        ManagedCursor cursor = ledger.newNonDurableCursor(PositionFactory.EARLIEST);
        try {
            while (cursor.hasMoreEntries()) {
                for (Entry entry : cursor.readEntries(100)) {
                    try {
                        MessageMetadata metadata = Commands.parseMessageMetadata(entry.getDataBuffer());
                        if (metadata.hasMarkerType()) {
                            markerTypes.add(metadata.getMarkerType());
                        }
                    } finally {
                        entry.release();
                    }
                }
            }
        } finally {
            ledger.deleteCursor(cursor.getName());
        }
        return markerTypes;
    }

    @Test(timeOut = 60000)
    public void testSegmentTopicDoesNotEnableReplicatedSubscriptionsController() throws Exception {
        final String scalableTopic = BrokerTestUtil.newUniqueName("topic://" + replicatedNamespace + "/tp_");
        admin1.scalableTopics().createScalableTopic(scalableTopic, 1);
        final String segmentTopic = singleSegmentTopic(scalableTopic);
        // The per-segment producer and consumer a V5 client creates under the hood. Like a V5 client, it has to
        // use the binary protocol: a segment topic cannot be looked up over HTTP.
        @Cleanup
        PulsarClientImpl segmentClient = (PulsarClientImpl) PulsarClient.builder()
                .serviceUrl(pulsar1.getBrokerServiceUrl())
                .build();

        // The controller only snapshots a topic that has data, so give the segment a message.
        ProducerConfigurationData producerConf = new ProducerConfigurationData();
        producerConf.setTopicName(segmentTopic);
        @Cleanup
        Producer<byte[]> producer = segmentClient.createSegmentProducerAsync(producerConf, Schema.BYTES).get();
        producer.send("msg".getBytes());

        ConsumerConfigurationData<byte[]> consumerConf = new ConsumerConfigurationData<>();
        consumerConf.getTopicNames().add(segmentTopic);
        consumerConf.setSubscriptionName("replicated-sub");
        consumerConf.setReplicateSubscriptionState(true);
        @Cleanup
        Consumer<byte[]> consumer = segmentClient.subscribeSegmentAsync(consumerConf, Schema.BYTES).get();

        PersistentTopic persistentTopic = (PersistentTopic) pulsar1.getBrokerService()
                .getTopic(segmentTopic, false).get().orElseThrow();
        // Everything that enables the controller of a regular topic holds for the segment as well, so it is only
        // the topic being a segment that keeps it off. The subscribe request is not rejected for asking: the
        // subscription is created and stays flagged as replicated.
        assertThat(pulsar1.getConfiguration().isEnableReplicatedSubscriptions()).isTrue();
        assertThat(persistentTopic.getHierarchyTopicPolicies().getReplicationClusters().get())
                .containsExactlyInAnyOrder(cluster1, cluster2);
        assertThat(persistentTopic.getSubscription("replicated-sub").isReplicated()).isTrue();

        // Several snapshot periods, the first of which starts as soon as a controller is created.
        Awaitility.await()
                .during(Duration.ofMillis(3L * config1.getReplicatedSubscriptionsSnapshotFrequencyMillis()))
                .atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> {
                    List<Integer> markerTypes = markerTypes(persistentTopic.getManagedLedger());
                    SoftAssertions.assertSoftly(softly -> {
                        softly.assertThat(persistentTopic.getReplicatedSubscriptionController())
                                .as("replicated subscriptions controller of %s", segmentTopic)
                                .isEmpty();
                        softly.assertThat(markerTypes)
                                .as("marker messages in %s", segmentTopic)
                                .isEmpty();
                    });
                });
    }
}
