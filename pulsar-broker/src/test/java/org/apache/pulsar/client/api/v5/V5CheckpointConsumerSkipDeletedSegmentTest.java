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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import java.time.Duration;
import lombok.Cleanup;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.apache.pulsar.common.scalable.SegmentTopicName;
import org.testng.annotations.Test;

/**
 * Coverage for {@link CheckpointConsumer} survival when a segment's backing topic
 * is gone. The controller's GC eventually deletes pruned segment topics after
 * their retention window expires; a checkpoint consumer doesn't dictate retention,
 * so when it restores against a DAG that still references a now-deleted segment
 * the segment's data is genuinely gone and the consumer must silently skip it,
 * delivering everything still on disk — the same observable shape as in-segment
 * retention.
 */
public class V5CheckpointConsumerSkipDeletedSegmentTest extends V5ClientBaseTest {

    /**
     * Force-delete one of two segments and open a checkpoint consumer: it must
     * subscribe to the survivor and skip the deleted segment silently, delivering
     * all messages still on disk.
     *
     * <p>Segment topics are never auto-created (only the controller materializes
     * them, see {@code BrokerService.isAllowAutoTopicCreationAsync}), so a delete
     * sticks — the consumer's resubscribe attempt against the gone segment
     * surfaces as {@code TopicDoesNotExist}, which the consumer must swallow.
     */
    @Test
    public void testConsumerSkipsDeletedSegmentSilently() throws Exception {
        String topic = newScalableTopic(2);
        long deletedSegmentId = 0L;
        long survivorSegmentId = 1L;

        // Produce a batch with enough variety in keys that both segments end up
        // with messages. After producing, we ask the broker which messages live
        // on the survivor segment by counting its in-counter.
        int n = 100;
        @Cleanup
        Producer<byte[]> producer = v5Client.newProducer(Schema.bytes())
                .topic(topic)
                .create();
        for (int i = 0; i < n; i++) {
            producer.newMessage().key("k-" + i).value(("msg-" + i).getBytes()).send();
        }
        producer.close();

        // Ground truth: how many messages should reach the consumer after delete.
        long survivorBacklog = segmentBacklog(topic, survivorSegmentId);
        long deletedBacklog = segmentBacklog(topic, deletedSegmentId);
        assertTrue(survivorBacklog > 0,
                "test setup: hash distribution put no messages on the survivor segment");
        assertTrue(deletedBacklog > 0,
                "test setup: hash distribution put no messages on the segment to be deleted");

        // Force-delete one segment's backing topic — the deterministic stand-in
        // for the controller's GC pruning it after retention expired.
        String victimTopicName = SegmentTopicName.fromParent(
                org.apache.pulsar.common.naming.TopicName.get(topic),
                segmentHashRange(topic, deletedSegmentId), deletedSegmentId).toString();
        admin.scalableTopics().deleteSegment(victimTopicName, true);

        // Open the consumer. The DAG still has segment 0 (we only deleted the topic,
        // not the metadata), so the consumer attempts to subscribe to it and gets
        // TopicDoesNotExist — which the fix swallows, leaving the survivor reader
        // to keep delivering. Must NOT throw.
        @Cleanup
        CheckpointConsumer<byte[]> consumer = v5Client.newCheckpointConsumer(Schema.bytes())
                .topic(topic)
                .startPosition(Checkpoint.earliest())
                .create();

        int received = 0;
        long deadline = System.currentTimeMillis() + 30_000L;
        while (received < survivorBacklog && System.currentTimeMillis() < deadline) {
            Message<byte[]> msg = consumer.receive(Duration.ofMillis(500));
            if (msg != null) {
                received++;
            }
        }
        assertEquals(received, survivorBacklog,
                "every survivor-segment message must still be delivered");
    }

    // --- Helpers ---

    /** Total messages currently on a segment's backing topic — used as ground
     * truth for "what we expect the consumer to deliver." Goes through the
     * broker's in-process topic reference because segment topics aren't exposed
     * via the regular {@code /admin/v2/persistent/.../stats} endpoint. */
    private long segmentBacklog(String topic, long segmentId) throws Exception {
        String segmentTopic = SegmentTopicName.fromParent(
                org.apache.pulsar.common.naming.TopicName.get(topic),
                segmentHashRange(topic, segmentId), segmentId).toString();
        var ref = getTopicReference(segmentTopic).orElseThrow(
                () -> new AssertionError("segment topic not loaded: " + segmentTopic));
        return ref.getStats(false, false, false).getMsgInCounter();
    }

    private org.apache.pulsar.common.scalable.HashRange segmentHashRange(String topic, long segmentId)
            throws Exception {
        var meta = admin.scalableTopics().getMetadata(topic);
        var seg = meta.getSegments().get(segmentId);
        if (seg == null) {
            throw new AssertionError("segment " + segmentId + " not found in DAG of " + topic);
        }
        // Admin-client SegmentInfo / HashRange are wire-payload @Data classes, distinct
        // from the broker-internal records — convert across the boundary.
        var adminRange = seg.getHashRange();
        return org.apache.pulsar.common.scalable.HashRange.of(
                adminRange.getStart(), adminRange.getEnd());
    }
}
