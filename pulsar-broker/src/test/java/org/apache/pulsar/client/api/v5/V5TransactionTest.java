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
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import lombok.Cleanup;
import org.apache.pulsar.client.api.v5.config.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.v5.config.TransactionPolicy;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

/**
 * Coverage for the V5 transaction wire: opening a transaction on a transaction-enabled
 * client, sending and acknowledging transactional messages through the V5 producer
 * and consumer on a scalable topic, and committing or aborting the transaction.
 */
public class V5TransactionTest extends V5ClientBaseTest {

    private PulsarClient newTxnClient() throws Exception {
        return track(PulsarClient.builder()
                .serviceUrl(getBrokerServiceUrl())
                .transactionPolicy(new TransactionPolicy(Duration.ofMinutes(1)))
                .build());
    }

    @Test
    public void testAbortMovesTransactionToAbortedState() throws Exception {
        // Open a transaction-enabled V5 client, send transactional messages through the
        // V5 producer to a scalable topic, then abort. The transaction state must move
        // OPEN → ABORTED — exercises the V5 newTransaction / message-builder
        // transaction / Transaction.abort path end-to-end.
        PulsarClient client = newTxnClient();
        String topic = newScalableTopic(1);

        @Cleanup
        Producer<String> producer = client.newProducer(Schema.string())
                .topic(topic)
                .create();

        Transaction txn = client.newTransaction();
        assertEquals(txn.state(), Transaction.State.OPEN);
        for (int i = 0; i < 5; i++) {
            producer.newMessage().transaction(txn).value("v-" + i).send();
        }
        txn.abort();
        assertEquals(txn.state(), Transaction.State.ABORTED);
    }

    @Test
    public void testCommitMakesProducedMessagesVisible() throws Exception {
        PulsarClient client = newTxnClient();
        String topic = newScalableTopic(1);

        @Cleanup
        Producer<String> producer = client.newProducer(Schema.string())
                .topic(topic)
                .create();
        @Cleanup
        QueueConsumer<String> consumer = client.newQueueConsumer(Schema.string())
                .topic(topic)
                .subscriptionName("txn-commit-sub")
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .subscribe();

        Transaction txn = client.newTransaction();
        Set<String> sent = new HashSet<>();
        for (int i = 0; i < 5; i++) {
            String v = "v-" + i;
            producer.newMessage().transaction(txn).value(v).send();
            sent.add(v);
        }
        // Pre-commit: nothing visible yet.
        assertNull(consumer.receive(Duration.ofMillis(500)),
                "transactional sends must not be visible before commit");

        txn.commit();
        assertEquals(txn.state(), Transaction.State.COMMITTED);

        Set<String> received = new HashSet<>();
        for (int i = 0; i < 5; i++) {
            Message<String> msg = consumer.receive(Duration.ofSeconds(5));
            assertNotNull(msg, "missed committed message #" + i);
            received.add(msg.value());
            consumer.acknowledge(msg.id());
        }
        assertEquals(received, sent, "every committed message must be delivered");
    }

    @Test
    public void testAbortedMessagesAreNeverDelivered() throws Exception {
        // After abort, transactional messages must never be delivered. Use a non-txn
        // sentinel published after the abort to prove that the consumer is alive and
        // delivering — and that the only message it sees is the sentinel.
        PulsarClient client = newTxnClient();
        String topic = newScalableTopic(1);

        @Cleanup
        Producer<String> producer = client.newProducer(Schema.string())
                .topic(topic)
                .create();
        @Cleanup
        QueueConsumer<String> consumer = client.newQueueConsumer(Schema.string())
                .topic(topic)
                .subscriptionName("txn-abort-sub")
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .subscribe();

        Transaction txn = client.newTransaction();
        for (int i = 0; i < 5; i++) {
            producer.newMessage().transaction(txn).value("v-" + i).send();
        }
        txn.abort();
        assertEquals(txn.state(), Transaction.State.ABORTED);

        producer.newMessage().value("sentinel").send();

        Message<String> msg = consumer.receive(Duration.ofSeconds(5));
        assertNotNull(msg, "non-transactional sentinel must be delivered");
        assertEquals(msg.value(), "sentinel", "aborted txn messages must not precede the sentinel");
        consumer.acknowledge(msg.id());

        assertNull(consumer.receive(Duration.ofMillis(500)),
                "no further messages after the sentinel — the aborted txn must be discarded");
    }

    @Test
    public void testCommitAcrossMultipleTopics() throws Exception {
        // A single transaction writing to two distinct scalable topics commits atomically:
        // both topics see their messages only after commit.
        PulsarClient client = newTxnClient();
        String topicA = newScalableTopic(1);
        String topicB = newScalableTopic(1);

        @Cleanup
        Producer<String> prodA = client.newProducer(Schema.string()).topic(topicA).create();
        @Cleanup
        Producer<String> prodB = client.newProducer(Schema.string()).topic(topicB).create();

        @Cleanup
        QueueConsumer<String> consA = client.newQueueConsumer(Schema.string())
                .topic(topicA).subscriptionName("multi-topic-a")
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .subscribe();
        @Cleanup
        QueueConsumer<String> consB = client.newQueueConsumer(Schema.string())
                .topic(topicB).subscriptionName("multi-topic-b")
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .subscribe();

        Transaction txn = client.newTransaction();
        prodA.newMessage().transaction(txn).value("a-1").send();
        prodB.newMessage().transaction(txn).value("b-1").send();

        assertNull(consA.receive(Duration.ofMillis(500)), "topic A: nothing visible pre-commit");
        assertNull(consB.receive(Duration.ofMillis(500)), "topic B: nothing visible pre-commit");

        txn.commit();
        assertEquals(txn.state(), Transaction.State.COMMITTED);

        Message<String> ma = consA.receive(Duration.ofSeconds(5));
        assertNotNull(ma, "topic A: expected message after commit");
        assertEquals(ma.value(), "a-1");

        Message<String> mb = consB.receive(Duration.ofSeconds(5));
        assertNotNull(mb, "topic B: expected message after commit");
        assertEquals(mb.value(), "b-1");
    }

    // Disabled: documents a real broker gap in the per-segment TransactionBuffer model.
    // After split, the parent segment is sealed; the txn coordinator's COMMIT END-TXN to
    // the sealed parent never returns and the commit RPC times out (~30s). The fix
    // requires moving transaction tracking up to the scalable-topic level (so layout
    // changes don't strand in-flight transactions on now-sealed segments) — that's
    // beyond the scope of this commit.
    @Test(enabled = false)
    public void testCommitSpansSplit() throws Exception {
        // A single transaction whose lifetime spans a layout-changing split must commit
        // atomically: pre-split writes (on the now-sealed parent) and post-split writes
        // (on the new children) all become visible together at commit. Exercises commit
        // markers landing on a sealed segment as well as the live children.
        PulsarClient client = newTxnClient();
        String topic = newScalableTopic(1);

        @Cleanup
        Producer<String> producer = client.newProducer(Schema.string()).topic(topic).create();
        @Cleanup
        QueueConsumer<String> consumer = client.newQueueConsumer(Schema.string())
                .topic(topic).subscriptionName("split-txn-sub")
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .subscribe();

        Transaction txn = client.newTransaction();

        // First batch: lands on the only initial segment.
        Set<String> sent = new HashSet<>();
        for (int i = 0; i < 5; i++) {
            String v = "before-split-" + i;
            producer.newMessage().key("k-" + i).transaction(txn).value(v).send();
            sent.add(v);
        }

        long activeSegmentId = -1;
        var meta = admin.scalableTopics().getMetadata(topic);
        for (var seg : meta.getSegments().values()) {
            if (seg.isActive()) {
                activeSegmentId = seg.getSegmentId();
                break;
            }
        }
        assertTrue(activeSegmentId >= 0, "expected exactly one active segment before split");
        admin.scalableTopics().splitSegment(topic, activeSegmentId);

        // Wait for the producer's view to reflect the new layout.
        Awaitility.await().untilAsserted(() -> {
            int active = 0;
            var m = admin.scalableTopics().getMetadata(topic);
            for (var seg : m.getSegments().values()) {
                if (seg.isActive()) {
                    active++;
                }
            }
            assertEquals(active, 2, "split must produce 2 active children");
        });

        // Second batch: lands on the new children, still inside the same transaction.
        for (int i = 0; i < 5; i++) {
            String v = "after-split-" + i;
            producer.newMessage().key("k-after-" + i).transaction(txn).value(v).send();
            sent.add(v);
        }

        assertNull(consumer.receive(Duration.ofMillis(500)),
                "transactional sends must not be visible before commit");

        txn.commit();
        assertEquals(txn.state(), Transaction.State.COMMITTED);

        Set<String> received = new HashSet<>();
        for (int i = 0; i < sent.size(); i++) {
            Message<String> msg = consumer.receive(Duration.ofSeconds(10));
            assertNotNull(msg, "missed message #" + i + " (received so far: " + received.size() + ")");
            received.add(msg.value());
            consumer.acknowledge(msg.id());
        }
        assertEquals(received, sent, "all txn messages across the split must be delivered after commit");
    }

    // Disabled: same broker gap as testCommitSpansSplit. After merge, both source
    // segments are sealed and the COMMIT marker can't be delivered, so the END-TXN
    // request times out.
    @Test(enabled = false)
    public void testCommitSpansMerge() throws Exception {
        // A single transaction whose lifetime spans a layout-changing merge must commit
        // atomically: writes to the two pre-merge segments and writes to the post-merge
        // segment all become visible together.
        PulsarClient client = newTxnClient();
        String topic = newScalableTopic(2);

        @Cleanup
        Producer<String> producer = client.newProducer(Schema.string()).topic(topic).create();
        @Cleanup
        QueueConsumer<String> consumer = client.newQueueConsumer(Schema.string())
                .topic(topic).subscriptionName("merge-txn-sub")
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .subscribe();

        Transaction txn = client.newTransaction();

        // First batch: lands on the two initial segments.
        Set<String> sent = new HashSet<>();
        for (int i = 0; i < 10; i++) {
            String v = "before-merge-" + i;
            producer.newMessage().key("k-" + i).transaction(txn).value(v).send();
            sent.add(v);
        }

        var meta = admin.scalableTopics().getMetadata(topic);
        List<Long> activeIds = new ArrayList<>();
        for (var seg : meta.getSegments().values()) {
            if (seg.isActive()) {
                activeIds.add(seg.getSegmentId());
            }
        }
        assertEquals(activeIds.size(), 2, "expected 2 active segments before merge");
        admin.scalableTopics().mergeSegments(topic, activeIds.get(0), activeIds.get(1));

        Awaitility.await().untilAsserted(() -> {
            int active = 0;
            var m = admin.scalableTopics().getMetadata(topic);
            for (var seg : m.getSegments().values()) {
                if (seg.isActive()) {
                    active++;
                }
            }
            assertEquals(active, 1, "merge must collapse to 1 active segment");
        });

        // Second batch: lands on the new sole segment, still inside the same transaction.
        for (int i = 0; i < 5; i++) {
            String v = "after-merge-" + i;
            producer.newMessage().key("k-after-" + i).transaction(txn).value(v).send();
            sent.add(v);
        }

        assertNull(consumer.receive(Duration.ofMillis(500)),
                "transactional sends must not be visible before commit");

        txn.commit();
        assertEquals(txn.state(), Transaction.State.COMMITTED);

        Set<String> received = new HashSet<>();
        for (int i = 0; i < sent.size(); i++) {
            Message<String> msg = consumer.receive(Duration.ofSeconds(10));
            assertNotNull(msg, "missed message #" + i + " (received so far: " + received.size() + ")");
            received.add(msg.value());
            consumer.acknowledge(msg.id());
        }
        assertEquals(received, sent, "all txn messages across the merge must be delivered after commit");
    }

    @Test
    public void testTransactionalConsumeAndProduce() throws Exception {
        // Classic consume-transform-produce: ack input + produce derived output in one
        // transaction. The output is invisible pre-commit; commit makes it visible.
        PulsarClient client = newTxnClient();
        String inputTopic = newScalableTopic(1);
        String outputTopic = newScalableTopic(1);

        // Seed an input message non-transactionally.
        @Cleanup
        Producer<String> seed = client.newProducer(Schema.string()).topic(inputTopic).create();
        seed.newMessage().value("hello").send();

        @Cleanup
        QueueConsumer<String> input = client.newQueueConsumer(Schema.string())
                .topic(inputTopic).subscriptionName("input-sub")
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .subscribe();
        @Cleanup
        Producer<String> output = client.newProducer(Schema.string()).topic(outputTopic).create();
        @Cleanup
        QueueConsumer<String> verify = client.newQueueConsumer(Schema.string())
                .topic(outputTopic).subscriptionName("verify-sub")
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .subscribe();

        Message<String> in = input.receive(Duration.ofSeconds(5));
        assertNotNull(in, "seed input message must be delivered");

        Transaction txn = client.newTransaction();
        output.newMessage().transaction(txn).value(in.value().toUpperCase()).send();
        input.acknowledge(in.id(), txn);

        assertNull(verify.receive(Duration.ofMillis(500)),
                "transactional output must not be visible before commit");

        txn.commit();
        assertEquals(txn.state(), Transaction.State.COMMITTED);

        Message<String> out = verify.receive(Duration.ofSeconds(5));
        assertNotNull(out, "committed output must be delivered");
        assertEquals(out.value(), "HELLO");
    }
}
