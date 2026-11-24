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
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import lombok.Cleanup;
import org.apache.pulsar.client.api.v5.config.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.v5.config.TransactionPolicy;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.apache.pulsar.common.policies.data.ScalableTopicMetadata;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

/**
 * Extended coverage for V5 transactions on scalable topics: multi-segment producers, many
 * concurrent transactions with mixed commit/abort, abort across layout changes (split / merge),
 * large transactions, transactional acknowledgement lifecycle, per-subscription isolation, and
 * read-committed visibility while a transaction is still open.
 *
 * <p>Complements {@link V5TransactionTest} (single-segment happy paths + commit-across-split/merge).
 */
public class V5TransactionScalableTest extends V5ClientBaseTest {

    private PulsarClient newTxnClient() throws Exception {
        return track(PulsarClient.builder()
                .serviceUrl(getBrokerServiceUrl())
                .transactionPolicy(TransactionPolicy.builder().timeout(Duration.ofMinutes(2)).build())
                .build());
    }

    private QueueConsumer<String> subscribe(PulsarClient client, String topic, String sub) throws Exception {
        return client.newQueueConsumer(Schema.string())
                .topic(topic)
                .subscriptionName(sub)
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .subscribe();
    }

    /** Receive exactly {@code count} messages (acking each), failing if any is missing. */
    private Set<String> receiveValues(QueueConsumer<String> consumer, int count, Duration perMessage)
            throws Exception {
        Set<String> values = new HashSet<>();
        for (int i = 0; i < count; i++) {
            Message<String> msg = consumer.receive(perMessage);
            assertNotNull(msg, "missing message #" + i + " (received so far: " + values.size() + ")");
            values.add(msg.value());
            consumer.acknowledge(msg.id());
        }
        return values;
    }

    private long firstActiveSegment(String topic) throws Exception {
        ScalableTopicMetadata meta = admin.scalableTopics().getMetadata(topic);
        for (ScalableTopicMetadata.SegmentInfo seg : meta.getSegments().values()) {
            if (seg.isActive()) {
                return seg.getSegmentId();
            }
        }
        throw new IllegalStateException("no active segment for " + topic);
    }

    private List<Long> activeSegments(String topic) throws Exception {
        ScalableTopicMetadata meta = admin.scalableTopics().getMetadata(topic);
        List<Long> ids = new ArrayList<>();
        for (ScalableTopicMetadata.SegmentInfo seg : meta.getSegments().values()) {
            if (seg.isActive()) {
                ids.add(seg.getSegmentId());
            }
        }
        return ids;
    }

    private void awaitActiveSegmentCount(String topic, int expected) {
        Awaitility.await().untilAsserted(() -> assertEquals(activeSegments(topic).size(), expected));
    }

    @Test
    public void testMultiSegmentTransactionCommit() throws Exception {
        // A single transaction whose keyed writes spread across all segments of a multi-segment
        // topic commits atomically: every message becomes visible together.
        PulsarClient client = newTxnClient();
        String topic = newScalableTopic(3);

        @Cleanup
        Producer<String> producer = client.newProducer(Schema.string()).topic(topic).create();
        @Cleanup
        QueueConsumer<String> consumer = subscribe(client, topic, "multi-seg-sub");

        Transaction txn = client.newTransaction();
        Set<String> sent = new HashSet<>();
        for (int i = 0; i < 60; i++) {
            String v = "v-" + i;
            producer.newMessage().key("k-" + i).transaction(txn).value(v).send();
            sent.add(v);
        }
        assertNull(consumer.receive(Duration.ofMillis(500)), "nothing visible before commit");

        txn.commit();
        assertEquals(receiveValues(consumer, sent.size(), Duration.ofSeconds(10)), sent,
                "every committed message across all segments must be delivered");
        assertNull(consumer.receive(Duration.ofMillis(500)), "no extra messages");
    }

    @Test
    public void testConcurrentTransactionsMixedCommitAbort() throws Exception {
        // Many open transactions on one topic, interleaved; half commit, half abort. Only the
        // committed transactions' messages are ever delivered. While any transaction stays open the
        // buffer pins visibility, so nothing is visible until all resolve.
        PulsarClient client = newTxnClient();
        String topic = newScalableTopic(2);

        @Cleanup
        Producer<String> producer = client.newProducer(Schema.string()).topic(topic).create();
        @Cleanup
        QueueConsumer<String> consumer = subscribe(client, topic, "concurrent-sub");

        int numTxns = 6;
        int perTxn = 10;
        List<Transaction> txns = new ArrayList<>();
        for (int t = 0; t < numTxns; t++) {
            txns.add(client.newTransaction());
        }
        Set<String> committedExpected = new HashSet<>();
        for (int i = 0; i < perTxn; i++) {
            for (int t = 0; t < numTxns; t++) {
                String v = "t" + t + "-m" + i;
                producer.newMessage().key("t" + t).transaction(txns.get(t)).value(v).send();
                if (t % 2 == 0) {
                    committedExpected.add(v);
                }
            }
        }
        assertNull(consumer.receive(Duration.ofMillis(500)), "nothing visible while transactions are open");

        for (int t = 0; t < numTxns; t++) {
            if (t % 2 == 0) {
                txns.get(t).commit();
            } else {
                txns.get(t).abort();
            }
        }

        assertEquals(receiveValues(consumer, committedExpected.size(), Duration.ofSeconds(10)),
                committedExpected, "exactly the committed transactions' messages must be delivered");
        assertNull(consumer.receive(Duration.ofSeconds(1)), "aborted transactions' messages must never appear");
    }

    @Test
    public void testAbortSpansSplit() throws Exception {
        // A transaction spanning a split that aborts must leave nothing visible from either the
        // sealed parent or the new children. A non-transactional sentinel proves the consumer is
        // live and that only it is delivered.
        PulsarClient client = newTxnClient();
        String topic = newScalableTopic(1);

        @Cleanup
        Producer<String> producer = client.newProducer(Schema.string()).topic(topic).create();
        @Cleanup
        QueueConsumer<String> consumer = subscribe(client, topic, "abort-split-sub");

        Transaction txn = client.newTransaction();
        for (int i = 0; i < 5; i++) {
            producer.newMessage().key("k-" + i).transaction(txn).value("before-split-" + i).send();
        }
        admin.scalableTopics().splitSegment(topic, firstActiveSegment(topic));
        awaitActiveSegmentCount(topic, 2);
        for (int i = 0; i < 5; i++) {
            producer.newMessage().key("k-after-" + i).transaction(txn).value("after-split-" + i).send();
        }
        txn.abort();

        producer.newMessage().value("sentinel").send();
        Message<String> msg = consumer.receive(Duration.ofSeconds(5));
        assertNotNull(msg, "non-transactional sentinel must be delivered");
        assertEquals(msg.value(), "sentinel", "aborted txn messages must not precede the sentinel");
        consumer.acknowledge(msg.id());
        assertNull(consumer.receive(Duration.ofMillis(500)), "no aborted messages after the sentinel");
    }

    @Test
    public void testAbortSpansMerge() throws Exception {
        // Same as the split case but across a merge: an aborted transaction whose lifetime spans a
        // merge leaves nothing visible.
        PulsarClient client = newTxnClient();
        String topic = newScalableTopic(2);

        @Cleanup
        Producer<String> producer = client.newProducer(Schema.string()).topic(topic).create();
        @Cleanup
        QueueConsumer<String> consumer = subscribe(client, topic, "abort-merge-sub");

        Transaction txn = client.newTransaction();
        for (int i = 0; i < 10; i++) {
            producer.newMessage().key("k-" + i).transaction(txn).value("before-merge-" + i).send();
        }
        List<Long> active = activeSegments(topic);
        assertEquals(active.size(), 2, "expected 2 active segments before merge");
        admin.scalableTopics().mergeSegments(topic, active.get(0), active.get(1));
        awaitActiveSegmentCount(topic, 1);
        for (int i = 0; i < 5; i++) {
            producer.newMessage().key("k-after-" + i).transaction(txn).value("after-merge-" + i).send();
        }
        txn.abort();

        producer.newMessage().value("sentinel").send();
        Message<String> msg = consumer.receive(Duration.ofSeconds(5));
        assertNotNull(msg, "non-transactional sentinel must be delivered");
        assertEquals(msg.value(), "sentinel");
        consumer.acknowledge(msg.id());
        assertNull(consumer.receive(Duration.ofMillis(500)), "no aborted messages after the sentinel");
    }

    @Test
    public void testLargeTransaction() throws Exception {
        // A large transaction commits atomically and every message is delivered exactly once.
        PulsarClient client = newTxnClient();
        String topic = newScalableTopic(2);

        @Cleanup
        Producer<String> producer = client.newProducer(Schema.string()).topic(topic).create();
        @Cleanup
        QueueConsumer<String> consumer = subscribe(client, topic, "large-txn-sub");

        int n = 500;
        Transaction txn = client.newTransaction();
        Set<String> sent = new HashSet<>();
        for (int i = 0; i < n; i++) {
            String v = "v-" + i;
            producer.newMessage().key("k-" + (i % 50)).transaction(txn).value(v).send();
            sent.add(v);
        }
        txn.commit();

        assertEquals(receiveValues(consumer, n, Duration.ofSeconds(15)), sent,
                "all messages in the large transaction must be delivered");
        assertNull(consumer.receive(Duration.ofSeconds(1)), "no extra messages");
    }

    @Test
    public void testTransactionalAckAbortRedeliversThenCommitSticks() throws Exception {
        // Acknowledge messages inside a transaction that aborts -> the acks roll back and the
        // messages are redelivered. Acknowledging the redelivered batch in a committed transaction
        // makes the acks durable -> no further redelivery.
        PulsarClient client = newTxnClient();
        String topic = newScalableTopic(1);

        @Cleanup
        Producer<String> producer = client.newProducer(Schema.string()).topic(topic).create();
        int n = 10;
        Set<String> produced = new HashSet<>();
        for (int i = 0; i < n; i++) {
            String v = "m-" + i;
            producer.newMessage().value(v).send();
            produced.add(v);
        }

        @Cleanup
        QueueConsumer<String> consumer = subscribe(client, topic, "ack-lifecycle-sub");

        // Consume all, ack inside a transaction we then abort.
        List<Message<String>> first = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            Message<String> m = consumer.receive(Duration.ofSeconds(5));
            assertNotNull(m, "initial delivery #" + i);
            first.add(m);
        }
        Transaction abortTxn = client.newTransaction();
        for (Message<String> m : first) {
            consumer.acknowledge(m.id(), abortTxn);
        }
        abortTxn.abort();

        // After abort the messages must be redelivered. Collect them in a committed transaction.
        Transaction commitTxn = client.newTransaction();
        Set<String> redelivered = new HashSet<>();
        long deadline = System.currentTimeMillis() + 30_000;
        while (redelivered.size() < n && System.currentTimeMillis() < deadline) {
            Message<String> m = consumer.receive(Duration.ofSeconds(2));
            if (m == null) {
                continue;
            }
            redelivered.add(m.value());
            consumer.acknowledge(m.id(), commitTxn);
        }
        assertEquals(redelivered, produced, "aborting a transactional ack must redeliver every message");
        commitTxn.commit();

        assertNull(consumer.receive(Duration.ofSeconds(2)),
                "messages acked in a committed transaction must not be redelivered");
    }

    @Test
    public void testMultipleSubscriptionsIndependentTransactionalAcks() throws Exception {
        // A committed transactional ack on one subscription must not affect another subscription on
        // the same scalable topic.
        PulsarClient client = newTxnClient();
        String topic = newScalableTopic(1);

        @Cleanup
        Producer<String> producer = client.newProducer(Schema.string()).topic(topic).create();
        int n = 10;
        Set<String> produced = new HashSet<>();
        for (int i = 0; i < n; i++) {
            String v = "m-" + i;
            producer.newMessage().value(v).send();
            produced.add(v);
        }

        @Cleanup
        QueueConsumer<String> subA = subscribe(client, topic, "sub-a");
        @Cleanup
        QueueConsumer<String> subB = subscribe(client, topic, "sub-b");

        Transaction txn = client.newTransaction();
        for (int i = 0; i < n; i++) {
            Message<String> m = subA.receive(Duration.ofSeconds(5));
            assertNotNull(m, "sub-a delivery #" + i);
            subA.acknowledge(m.id(), txn);
        }
        txn.commit();
        assertNull(subA.receive(Duration.ofSeconds(2)), "sub-a committed acks must stick");

        // sub-b is independent and must still see everything.
        assertEquals(receiveValues(subB, n, Duration.ofSeconds(5)), produced,
                "the other subscription must be unaffected by sub-a's transactional acks");
    }

    @Test
    public void testOpenTransactionPinsLaterNonTransactionalWrites() throws Exception {
        // Read-committed visibility: while a transaction is open, the buffer's max-read position is
        // pinned below its first write, so even a non-transactional message published afterwards
        // stays invisible until the transaction resolves. After abort, the non-transactional message
        // becomes visible and the aborted ones are filtered.
        PulsarClient client = newTxnClient();
        String topic = newScalableTopic(1);

        @Cleanup
        Producer<String> producer = client.newProducer(Schema.string()).topic(topic).create();
        @Cleanup
        QueueConsumer<String> consumer = subscribe(client, topic, "pin-sub");

        Transaction txn = client.newTransaction();
        for (int i = 0; i < 5; i++) {
            producer.newMessage().transaction(txn).value("txn-" + i).send();
        }
        assertNull(consumer.receive(Duration.ofSeconds(1)), "uncommitted writes are invisible");

        producer.newMessage().value("sentinel").send();
        assertNull(consumer.receive(Duration.ofSeconds(1)),
                "a non-transactional write after an open txn is pinned until the txn resolves");

        txn.abort();
        Message<String> msg = consumer.receive(Duration.ofSeconds(5));
        assertNotNull(msg, "after abort the non-transactional write becomes visible");
        assertEquals(msg.value(), "sentinel", "only the non-transactional write is delivered");
        consumer.acknowledge(msg.id());
        assertNull(consumer.receive(Duration.ofMillis(500)), "aborted txn messages must stay filtered");
    }
}
