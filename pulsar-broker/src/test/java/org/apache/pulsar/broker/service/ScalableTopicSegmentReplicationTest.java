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
import java.util.ArrayList;
import java.util.List;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ScalableTopicMetadata.SegmentInfo;
import org.apache.pulsar.common.scalable.HashRange;
import org.apache.pulsar.common.scalable.SegmentTopicName;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * The segment DAG of a scalable topic is independent per cluster, so the {@code segment://} topics backing it
 * must not be picked up by the classic geo-replication of the namespace they live in: there is no same-named
 * segment to replicate into on the remote cluster.
 */
@Test(groups = "broker-replication")
public class ScalableTopicSegmentReplicationTest extends OneWayReplicatorTestBase {

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

    private List<String> segmentTopics(String scalableTopic) throws Exception {
        List<String> segmentTopics = new ArrayList<>();
        for (SegmentInfo segment : admin1.scalableTopics().getMetadata(scalableTopic).getSegments().values()) {
            HashRange range = HashRange.of(segment.getHashRange().getStart(), segment.getHashRange().getEnd());
            segmentTopics.add(SegmentTopicName.fromParent(TopicName.get(scalableTopic), range,
                    segment.getSegmentId()).toString());
        }
        return segmentTopics;
    }

    @Test(timeOut = 60000)
    public void testSegmentTopicDoesNotStartGeoReplicator() throws Exception {
        final String scalableTopic = BrokerTestUtil.newUniqueName("topic://" + replicatedNamespace + "/tp_");
        // Creating the scalable topic loads its initial segment topics, which runs their replication check.
        admin1.scalableTopics().createScalableTopic(scalableTopic, 2);

        List<String> segmentTopics = segmentTopics(scalableTopic);
        assertThat(segmentTopics).hasSize(2);
        for (String segmentTopic : segmentTopics) {
            PersistentTopic persistentTopic = (PersistentTopic) pulsar1.getBrokerService()
                    .getTopic(segmentTopic, false).get().orElseThrow();
            // The replication clusters of the namespace do resolve for the segment topic, so it is only the
            // topic being a segment that keeps a replicator from being started.
            assertThat(persistentTopic.getHierarchyTopicPolicies().getReplicationClusters().get())
                    .containsExactlyInAnyOrder(cluster1, cluster2);

            // The check that runs when the topic is loaded, and again on every policies update.
            persistentTopic.checkReplication().get();

            assertThat(persistentTopic.getReplicators().keySet())
                    .as("replicators of %s", segmentTopic)
                    .isEmpty();
            assertThat(persistentTopic.getManagedLedger().getCursors())
                    .extracting(ManagedCursor::getName)
                    .as("cursors of %s", segmentTopic)
                    .noneMatch(cursorName -> cursorName.startsWith(persistentTopic.getReplicatorPrefix()));
        }
    }
}
