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
import org.apache.pulsar.client.api.v5.Message;
import org.apache.pulsar.client.api.v5.Producer;
import org.apache.pulsar.client.api.v5.PulsarClient;
import org.apache.pulsar.client.api.v5.QueueConsumer;
import org.apache.pulsar.client.api.v5.Transaction;
import org.apache.pulsar.client.api.v5.V5ClientBaseTest;
import org.apache.pulsar.client.api.v5.config.SubscriptionInitialPosition;
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
 * gone. The same goes for the {@link TxnPaths#TXN_OP_PREFIX} write records of the transactions still
 * open on the segment, which the buffer deletes as it applies their outcome, and for their ack records
 * on the segment's subscriptions, which the pending-ack store of each subscription deletes the same way.
 * Except while the segment is still active in the layout of its topic: a client can then load it again.
 * Deleting just a subscription must delete its ack records too.
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
    public void testDeleteActiveSegmentKeepsTxnState() throws Exception {
        // A client can load a segment still active in the layout again right after its backing topic is deleted,
        // and the new transaction buffer recovers from these records: they are left to the delete that takes the
        // segment out of the layout. The re-created segment then follows the transaction still open on it.
        String topic = newScalableTopic(1);
        String segment = segmentTopicName(topic, singleActiveSegmentId(topic));
        PulsarClient client = newTxnClient();
        try (Producer<String> producer = client.newProducer(Schema.string()).topic(topic).create()) {
            producer.newMessage().key("k").value("before").send();
        }
        Awaitility.await().untilAsserted(() -> assertThat(txnState(segment))
                .as("transaction state of %s after a publish", segment)
                .contains("watermark"));
        Transaction txn = writeInOpenTransaction(topic, client);

        admin.scalableTopics().deleteSegment(segment, true);

        assertThat(txnState(segment))
                .as("transaction state of the active segment %s after deleting its backing topic", segment)
                .contains("watermark");
        assertThat(writeRecords(segment))
                .as("write records on the active segment %s after deleting its backing topic", segment)
                .hasSize(1);

        @Cleanup
        QueueConsumer<String> consumer = client.newQueueConsumer(Schema.string())
                .topic(topic)
                .subscriptionName("sub")
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .subscribe();
        @Cleanup
        Producer<String> producer = client.newProducer(Schema.string())
                .topic(topic)
                .create();
        producer.newMessage().key("k").value("after").send();
        txn.abort();

        Message<String> msg = consumer.receive(Duration.ofSeconds(10));
        assertThat(msg)
                .as("message on the re-created segment %s once the transaction open on it is aborted", segment)
                .isNotNull();
        assertThat(msg.value()).isEqualTo("after");
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

    @Test
    public void testDeleteScalableTopicDeletesWriteRecordsOfOpenTransaction() throws Exception {
        String topic = newScalableTopic(1);
        String segment = segmentTopicName(topic, singleActiveSegmentId(topic));
        PulsarClient client = newTxnClient();
        Transaction txn = writeInOpenTransaction(topic, client);
        assertThat(writeRecords(segment))
                .as("write records on %s of a transaction still open", segment)
                .hasSize(1);

        admin.scalableTopics().deleteScalableTopic(topic, true);

        assertThat(writeRecords(segment))
                .as("write records on %s of a transaction still open after deleting %s", segment, topic)
                .isEmpty();
        txn.abort();
    }

    @Test
    public void testRecreatedTopicNotHeldBackByOpenTransactionOfDeletedTopic() throws Exception {
        // A write record left behind would make the transaction buffer of the re-created segment, named like the
        // deleted one, recover the transaction as open, and hold back every message until the transaction ends.
        String topic = newScalableTopic(1);
        String segment = segmentTopicName(topic, singleActiveSegmentId(topic));
        PulsarClient client = newTxnClient();
        Transaction txn = writeInOpenTransaction(topic, client);
        admin.scalableTopics().deleteScalableTopic(topic, true);
        admin.scalableTopics().createScalableTopic(topic, 1);
        assertThat(segmentTopicName(topic, singleActiveSegmentId(topic)))
                .as("segment of %s re-created with the same name", topic)
                .isEqualTo(segment);

        @Cleanup
        QueueConsumer<String> consumer = client.newQueueConsumer(Schema.string())
                .topic(topic)
                .subscriptionName("sub")
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .subscribe();
        @Cleanup
        Producer<String> producer = client.newProducer(Schema.string())
                .topic(topic)
                .create();
        producer.newMessage().key("k").value("v").send();

        Message<String> msg = consumer.receive(Duration.ofSeconds(10));
        assertThat(msg)
                .as("message on %s, re-created while a transaction that wrote to it is still open", topic)
                .isNotNull();
        assertThat(msg.value()).isEqualTo("v");
        txn.abort();
    }

    @Test
    public void testDeleteScalableTopicDeletesAckRecordsOfOpenTransaction() throws Exception {
        String topic = newScalableTopic(1);
        String segment = segmentTopicName(topic, singleActiveSegmentId(topic));
        PulsarClient client = newTxnClient();
        Transaction txn = client.newTransaction();
        ackInTransaction(topic, client, "sub", txn);

        admin.scalableTopics().deleteScalableTopic(topic, true);

        assertThat(ackRecords(segment, "sub"))
                .as("ack records on %s of a transaction still open after deleting %s", segment, topic)
                .isEmpty();
        txn.abort();
    }

    @Test
    public void testDeleteSubscriptionDeletesAckRecordsOfOpenTransaction() throws Exception {
        String topic = newScalableTopic(1);
        String segment = segmentTopicName(topic, singleActiveSegmentId(topic));
        PulsarClient client = newTxnClient();
        Transaction txn = client.newTransaction();
        ackInTransaction(topic, client, "sub", txn);
        ackInTransaction(topic, client, "sub2", txn);

        admin.scalableTopics().deleteSubscription(topic, "sub");

        assertThat(ackRecords(segment, "sub"))
                .as("ack records on %s of a transaction still open after deleting the subscription", segment)
                .isEmpty();
        assertThat(ackRecords(segment, "sub2"))
                .as("ack records on %s of the same transaction on another subscription", segment)
                .hasSize(1);
        txn.abort();
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

    /** Publishes a message on {@code topic} in a transaction left open, and returns the transaction. */
    private Transaction writeInOpenTransaction(String topic, PulsarClient client) throws Exception {
        Transaction txn = client.newTransaction();
        try (Producer<String> producer = client.newProducer(Schema.string()).topic(topic).create()) {
            producer.newMessage().key("k").transaction(txn).value("open").send();
        }
        return txn;
    }

    /** Publishes a message on {@code topic}, and acknowledges a message on {@code subscription} in {@code txn}. */
    private void ackInTransaction(String topic, PulsarClient client, String subscription, Transaction txn)
            throws Exception {
        String segment = segmentTopicName(topic, singleActiveSegmentId(topic));
        try (QueueConsumer<String> consumer = client.newQueueConsumer(Schema.string())
                     .topic(topic)
                     .subscriptionName(subscription)
                     .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                     .subscribe();
             Producer<String> producer = client.newProducer(Schema.string()).topic(topic).create()) {
            producer.newMessage().key("k").value("acked").send();
            Message<String> msg = consumer.receive(Duration.ofSeconds(10));
            assertThat(msg).as("message on %s", topic).isNotNull();
            consumer.acknowledge(msg.id(), txn);
            // The acknowledgment is sent asynchronously: wait until the subscription has recorded it.
            Awaitility.await().untilAsserted(() -> assertThat(ackRecords(segment, subscription))
                    .as("ack records of %s on %s", subscription, segment)
                    .hasSize(1));
        }
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

    /** The paths of the {@link TxnPaths#TXN_OP_PREFIX} write records on the segment. */
    private List<String> writeRecords(String segment) throws Exception {
        TxnMetadataStore txnStore = new TxnMetadataStore(getPulsar().getLocalMetadataStore());
        List<String> paths = Collections.synchronizedList(new ArrayList<>());
        txnStore.listWritesBySegment(segment, new ScanConsumer() {
            @Override
            public void onNext(GetResult r) {
                paths.add(r.getStat().getPath());
            }

            @Override
            public void onError(Throwable throwable) {
            }

            @Override
            public void onCompleted() {
            }
        }).get();
        return paths;
    }

    /** The paths of the {@link TxnPaths#TXN_OP_PREFIX} ack records of {@code subscription} on the segment. */
    private List<String> ackRecords(String segment, String subscription) throws Exception {
        TxnMetadataStore txnStore = new TxnMetadataStore(getPulsar().getLocalMetadataStore());
        List<String> paths = Collections.synchronizedList(new ArrayList<>());
        txnStore.listAcksBySegmentSubscription(segment, subscription, new ScanConsumer() {
            @Override
            public void onNext(GetResult r) {
                paths.add(r.getStat().getPath());
            }

            @Override
            public void onError(Throwable throwable) {
            }

            @Override
            public void onCompleted() {
            }
        }).get();
        return paths;
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
