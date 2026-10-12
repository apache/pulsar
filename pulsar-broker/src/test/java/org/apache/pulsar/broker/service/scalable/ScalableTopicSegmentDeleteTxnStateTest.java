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

import static org.assertj.core.api.Assertions.assertThat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import lombok.Cleanup;
import org.apache.pulsar.broker.transaction.metadata.TxnMetadataStore;
import org.apache.pulsar.broker.transaction.metadata.TxnPaths;
import org.apache.pulsar.client.api.v5.Producer;
import org.apache.pulsar.client.api.v5.PulsarClient;
import org.apache.pulsar.client.api.v5.Transaction;
import org.apache.pulsar.client.api.v5.V5ClientBaseTest;
import org.apache.pulsar.client.api.v5.config.TransactionPolicy;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.AutoScalePolicyOverride;
import org.apache.pulsar.common.scalable.HashRange;
import org.apache.pulsar.common.scalable.SegmentTopicName;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.ScanConsumer;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

/**
 * Deleting the backing topic of a segment must delete the transaction state that the segment's
 * transaction buffer keeps in the metadata store under {@link TxnPaths#TXN_SEGMENT_STATE_PREFIX}: the
 * watermark record, written once anything is published on the segment, and one record per aborted
 * transaction whose data is still in the segment. Nothing reads or deletes them once the segment is
 * gone.
 */
public class ScalableTopicSegmentDeleteTxnStateTest extends V5ClientBaseTest {

    @Test
    public void testDeleteScalableTopicDeletesSegmentTxnState() throws Exception {
        String topic = newScalableTopic(1);
        PulsarClient client = newTxnClient();
        @Cleanup
        Producer<String> producer = client.newProducer(Schema.string())
                .topic(topic)
                .create();
        String parent = abortThenSplit(topic, client, producer);
        List<String> segments = new ArrayList<>(childrenWithTxnState(topic));
        segments.add(parent);
        producer.close();

        admin.scalableTopics().deleteScalableTopic(topic, true);

        for (String segment : segments) {
            assertThat(txnState(segment))
                    .as("transaction state of %s after deleting %s", segment, topic)
                    .isEmpty();
        }
    }

    @Test
    public void testDeleteSegmentDeletesTxnState() throws Exception {
        String topic = newScalableTopic(1);
        PulsarClient client = newTxnClient();
        @Cleanup
        Producer<String> producer = client.newProducer(Schema.string())
                .topic(topic)
                .create();
        String parent = abortThenSplit(topic, client, producer);
        List<String> children = childrenWithTxnState(topic);

        admin.scalableTopics().deleteSegment(parent, true);

        assertThat(txnState(parent))
                .as("transaction state of %s after deleting its backing topic", parent)
                .isEmpty();
        assertChildrenKeepTxnState(children);
    }

    @Test
    public void testGcPruneDeletesSegmentTxnState() throws Exception {
        String topic = newScalableTopic(1);
        PulsarClient client = newTxnClient();
        @Cleanup
        Producer<String> producer = client.newProducer(Schema.string())
                .topic(topic)
                .create();
        String parent = abortThenSplit(topic, client, producer);
        List<String> children = childrenWithTxnState(topic);

        // The broker default retention (defaultRetentionTimeInMinutes = 0) makes the sealed parent
        // eligible right away, and with no subscriptions there is nothing left to drain.
        ScalableTopicController controller = getPulsar().getBrokerService().getScalableTopicService()
                .getOrCreateController(TopicName.get(topic)).get();
        controller.runGcTickAsync().get();

        Awaitility.await().untilAsserted(() -> assertThat(getPulsar().getPulsarResources().getTopicResources()
                .persistentTopicExists(TopicName.get(parent)).get())
                .as("the GC tick must delete the backing topic of the sealed parent %s", parent)
                .isFalse());
        assertThat(txnState(parent))
                .as("transaction state of %s after the GC tick pruned it", parent)
                .isEmpty();
        assertChildrenKeepTxnState(children);
    }

    private PulsarClient newTxnClient() throws Exception {
        return track(PulsarClient.builder()
                .serviceUrl(getBrokerServiceUrl())
                .transactionPolicy(TransactionPolicy.builder().timeout(Duration.ofMinutes(2)).build())
                .build());
    }

    /**
     * Aborts a transaction that wrote to the single initial segment, waits until the segment has
     * persisted its watermark and the aborted-transaction record, then splits the segment and
     * publishes on the children. Returns the name of the sealed parent's backing topic.
     */
    private String abortThenSplit(String topic, PulsarClient client, Producer<String> producer)
            throws Exception {
        admin.scalableTopics().setAutoScalePolicy(topic,
                AutoScalePolicyOverride.builder().enabled(false).build());
        long parentId = singleActiveSegmentId(topic);
        String parent = segmentTopicName(topic, parentId);

        Transaction txn = client.newTransaction();
        producer.newMessage().key("k").transaction(txn).value("aborted").send();
        txn.abort();
        Awaitility.await().untilAsserted(() -> assertThat(txnState(parent))
                .as("transaction state of %s after aborting a transaction that wrote to it", parent)
                .contains("watermark")
                .anyMatch(record -> record.startsWith("aborted:")));

        admin.scalableTopics().splitSegment(topic, parentId);
        Awaitility.await().untilAsserted(() -> assertThat(activeSegmentIds(topic))
                .as("active segments after splitting segment %s", parentId)
                .hasSize(2));
        producer.newMessage().key("k").value("post").send();
        return parent;
    }

    /** The active segments that have persisted a watermark for what was published on them. */
    private List<String> childrenWithTxnState(String topic) throws Exception {
        List<String> children = new ArrayList<>();
        for (long id : activeSegmentIds(topic)) {
            children.add(segmentTopicName(topic, id));
        }
        List<String> withState = new ArrayList<>();
        Awaitility.await().untilAsserted(() -> {
            withState.clear();
            for (String child : children) {
                if (!txnState(child).isEmpty()) {
                    withState.add(child);
                }
            }
            assertThat(withState).as("children of the split with transaction state").isNotEmpty();
        });
        return withState;
    }

    private void assertChildrenKeepTxnState(List<String> children) throws Exception {
        for (String child : children) {
            assertThat(txnState(child))
                    .as("transaction state of the active segment %s after deleting its sealed parent", child)
                    .contains("watermark");
        }
    }

    /**
     * The records the segment's transaction buffer keeps under {@link TxnPaths#TXN_SEGMENT_STATE_PREFIX}:
     * {@code "watermark"} for the watermark record and {@code "aborted:<txnId>"} for each aborted
     * transaction.
     */
    private List<String> txnState(String segment) throws Exception {
        TxnMetadataStore txnStore = new TxnMetadataStore(getPulsar().getLocalMetadataStore());
        List<String> records = Collections.synchronizedList(new ArrayList<>());
        if (txnStore.getSegmentWatermark(segment).get().isPresent()) {
            records.add("watermark");
        }
        txnStore.scanAbortedTxns(segment,
                TxnPaths.abortedByPositionSegmentLowerBound(segment),
                TxnPaths.abortedByPositionSegmentUpperBound(segment),
                new ScanConsumer() {
                    @Override
                    public void onNext(GetResult r) {
                        records.add("aborted:" + TxnPaths.txnIdFromAbortedPath(r.getStat().getPath()));
                    }

                    @Override
                    public void onError(Throwable throwable) {
                    }

                    @Override
                    public void onCompleted() {
                    }
                }).get();
        return records;
    }

    private long singleActiveSegmentId(String topic) throws Exception {
        List<Long> active = activeSegmentIds(topic);
        assertThat(active).as("active segments of %s", topic).hasSize(1);
        return active.get(0);
    }

    private List<Long> activeSegmentIds(String topic) throws Exception {
        return admin.scalableTopics().getMetadata(topic).getSegments().values().stream()
                .filter(seg -> seg.isActive())
                .map(seg -> seg.getSegmentId())
                .toList();
    }

    private String segmentTopicName(String topic, long segmentId) throws Exception {
        var range = admin.scalableTopics().getMetadata(topic).getSegments().get(segmentId).getHashRange();
        return SegmentTopicName.fromParent(TopicName.get(topic),
                HashRange.of(range.getStart(), range.getEnd()), segmentId).toString();
    }
}
