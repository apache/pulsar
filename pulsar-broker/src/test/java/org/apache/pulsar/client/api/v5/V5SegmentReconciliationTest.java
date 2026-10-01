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

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.scalable.HashRange;
import org.apache.pulsar.common.scalable.SegmentTopicName;
import org.testng.annotations.Test;

/**
 * Coverage for the segment-topic auto-create reconciliation rule (PIP-468/483): a
 * {@code segment://} topic may be auto-created on connect <b>only</b> when the parent DAG
 * still lists that segment as ACTIVE. This is what lets an active segment whose backing topic
 * is missing (e.g. a {@code createScalableTopic} that wrote metadata then failed to
 * materialize segments) self-heal when a client connects — while still refusing to resurrect
 * a sealed/pruned segment that a producer/consumer might be racing a delete on.
 *
 * <p>Exercised directly through {@code BrokerService.isAllowAutoTopicCreationAsync}, which is
 * the gate every producer/consumer connect funnels through.
 */
public class V5SegmentReconciliationTest extends V5ClientBaseTest {

    private boolean segmentAutoCreatable(String segmentTopic) throws Exception {
        return getPulsar().getBrokerService().isAllowAutoTopicCreationAsync(segmentTopic).get();
    }

    private String segmentName(String parentTopic, HashRange range, long segmentId) {
        return SegmentTopicName.fromParent(TopicName.get(parentTopic), range, segmentId).toString();
    }

    @Test
    public void testActiveSegmentIsAutoCreatable() throws Exception {
        String topic = newScalableTopic(1);
        // The single initial segment covers the full hash range and is ACTIVE.
        String seg0 = segmentName(topic, HashRange.of(0x0000, 0xFFFF), 0);
        assertTrue(segmentAutoCreatable(seg0),
                "an active segment in the DAG must be auto-creatable so a connect can "
                        + "reconcile a missing backing topic");
    }

    @Test
    public void testSealedSegmentIsNotAutoCreatable() throws Exception {
        String topic = newScalableTopic(1);
        String seg0 = segmentName(topic, HashRange.of(0x0000, 0xFFFF), 0);

        // Split seals segment 0 (its range/id are unchanged; only its state flips to SEALED).
        admin.scalableTopics().splitSegment(topic, 0);

        assertFalse(segmentAutoCreatable(seg0),
                "a sealed segment must NOT be auto-created — a connect racing its teardown "
                        + "must not resurrect it");
    }

    @Test
    public void testUnknownSegmentIsNotAutoCreatable() throws Exception {
        String topic = newScalableTopic(1);
        // A segment id that isn't in the DAG at all.
        String ghost = segmentName(topic, HashRange.of(0x0000, 0xFFFF), 999);
        assertFalse(segmentAutoCreatable(ghost),
                "a segment id absent from the DAG must not be auto-created");
    }

    @Test
    public void testSegmentUnderMissingParentIsNotAutoCreatable() throws Exception {
        // Parent scalable topic was never created.
        String missingParent = "topic://" + getNamespace() + "/never-created-"
                + java.util.UUID.randomUUID().toString().substring(0, 8);
        String seg0 = segmentName(missingParent, HashRange.of(0x0000, 0xFFFF), 0);
        assertFalse(segmentAutoCreatable(seg0),
                "a segment under a non-existent scalable topic must not be auto-created");
    }
}
