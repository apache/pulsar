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
package org.apache.pulsar.tests.integration.transaction;

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
import lombok.CustomLog;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.v5.Message;
import org.apache.pulsar.client.api.v5.Producer;
import org.apache.pulsar.client.api.v5.PulsarClient;
import org.apache.pulsar.client.api.v5.QueueConsumer;
import org.apache.pulsar.client.api.v5.Transaction;
import org.apache.pulsar.client.api.v5.config.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.v5.config.TransactionPolicy;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.apache.pulsar.common.policies.data.ScalableTopicMetadata;
import org.apache.pulsar.tests.integration.containers.BrokerContainer;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

/**
 * End-to-end integration coverage for V5 transactions producing and consuming data on scalable
 * ({@code topic://}) topics across a multi-broker docker cluster: commit visibility, abort filtering,
 * multi-topic atomic commit, consume-transform-produce, survival of a broker failover mid-transaction,
 * and transactions whose lifetime spans a layout-changing split / merge.
 */
@CustomLog
public class V5ScalableTopicTransactionTest extends TcMetadataDiscoveryTestBase {

    private static final int OP_TIMEOUT_SECONDS = 30;

    private PulsarAdmin newAdmin() throws Exception {
        return PulsarAdmin.builder().serviceHttpUrl(pulsarCluster.getHttpServiceUrl()).build();
    }

    private PulsarClient newTxnClient() throws Exception {
        return PulsarClient.builder()
                .serviceUrl(pulsarCluster.getPlainTextServiceUrl())
                .transactionPolicy(TransactionPolicy.builder().timeout(Duration.ofMinutes(5)).build())
                .build();
    }

    private String scalableTopicName() {
        return "topic://public/default/scalable-" + randomName(8);
    }

    private QueueConsumer<String> subscribe(PulsarClient client, String topic, String sub) throws Exception {
        return client.newQueueConsumer(Schema.string())
                .topic(topic)
                .subscriptionName(sub)
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .subscribe();
    }

    private Set<String> drain(QueueConsumer<String> consumer, int count) throws Exception {
        Set<String> values = new HashSet<>();
        for (int i = 0; i < count; i++) {
            Message<String> msg = consumer.receive(Duration.ofSeconds(OP_TIMEOUT_SECONDS));
            assertNotNull(msg, "missing message #" + i + " (received so far: " + values.size() + ")");
            values.add(msg.value());
            consumer.acknowledge(msg.id());
        }
        return values;
    }

    private List<Long> activeSegments(PulsarAdmin admin, String topic) throws Exception {
        ScalableTopicMetadata meta = admin.scalableTopics().getMetadata(topic);
        List<Long> ids = new ArrayList<>();
        for (ScalableTopicMetadata.SegmentInfo seg : meta.getSegments().values()) {
            if (seg.isActive()) {
                ids.add(seg.getSegmentId());
            }
        }
        return ids;
    }

    @Test(timeOut = 300_000)
    public void testProduceConsumeCommitAndAbort() throws Exception {
        @Cleanup
        PulsarAdmin admin = newAdmin();
        @Cleanup
        PulsarClient client = newTxnClient();
        String topic = scalableTopicName();
        admin.scalableTopics().createScalableTopic(topic, 2);

        @Cleanup
        Producer<String> producer = client.newProducer(Schema.string()).topic(topic).create();
        @Cleanup
        QueueConsumer<String> consumer = subscribe(client, topic, "ce-sub");

        // Commit path.
        Transaction commit = client.newTransaction();
        Set<String> committed = new HashSet<>();
        for (int i = 0; i < 20; i++) {
            String v = "commit-" + i;
            producer.newMessage().key("k-" + i).transaction(commit).value(v).send();
            committed.add(v);
        }
        assertNull(consumer.receive(Duration.ofSeconds(2)), "nothing visible before commit");
        commit.commit();
        assertEquals(drain(consumer, committed.size()), committed, "committed messages must all be delivered");

        // Abort path: aborted messages never delivered; a sentinel proves the consumer is live.
        Transaction abort = client.newTransaction();
        for (int i = 0; i < 20; i++) {
            producer.newMessage().key("k-" + i).transaction(abort).value("abort-" + i).send();
        }
        abort.abort();
        producer.newMessage().value("sentinel").send();
        Message<String> msg = consumer.receive(Duration.ofSeconds(OP_TIMEOUT_SECONDS));
        assertNotNull(msg, "sentinel must be delivered");
        assertEquals(msg.value(), "sentinel", "aborted messages must not precede the sentinel");
        consumer.acknowledge(msg.id());
        assertNull(consumer.receive(Duration.ofSeconds(2)), "no aborted messages after the sentinel");
    }

    @Test(timeOut = 300_000)
    public void testMultiTopicAtomicCommit() throws Exception {
        @Cleanup
        PulsarAdmin admin = newAdmin();
        @Cleanup
        PulsarClient client = newTxnClient();
        String topicA = scalableTopicName();
        String topicB = scalableTopicName();
        admin.scalableTopics().createScalableTopic(topicA, 2);
        admin.scalableTopics().createScalableTopic(topicB, 2);

        @Cleanup
        Producer<String> prodA = client.newProducer(Schema.string()).topic(topicA).create();
        @Cleanup
        Producer<String> prodB = client.newProducer(Schema.string()).topic(topicB).create();
        @Cleanup
        QueueConsumer<String> consA = subscribe(client, topicA, "multi-a");
        @Cleanup
        QueueConsumer<String> consB = subscribe(client, topicB, "multi-b");

        Transaction txn = client.newTransaction();
        prodA.newMessage().value("a-1").send();
        prodB.newMessage().value("b-1").send();
        // The above are non-transactional sends to confirm the topics work; now the transactional ones:
        prodA.newMessage().transaction(txn).value("a-txn").send();
        prodB.newMessage().transaction(txn).value("b-txn").send();

        // The non-transactional a-1/b-1 are pinned behind the open transaction's first write only if
        // they were published after it; here they were published before, so they are visible.
        assertEquals(consA.receive(Duration.ofSeconds(OP_TIMEOUT_SECONDS)).value(), "a-1");
        assertEquals(consB.receive(Duration.ofSeconds(OP_TIMEOUT_SECONDS)).value(), "b-1");
        assertNull(consA.receive(Duration.ofSeconds(2)), "txn message on A invisible pre-commit");
        assertNull(consB.receive(Duration.ofSeconds(2)), "txn message on B invisible pre-commit");

        txn.commit();
        assertEquals(consA.receive(Duration.ofSeconds(OP_TIMEOUT_SECONDS)).value(), "a-txn");
        assertEquals(consB.receive(Duration.ofSeconds(OP_TIMEOUT_SECONDS)).value(), "b-txn");
    }

    @Test(timeOut = 300_000)
    public void testConsumeTransformProduce() throws Exception {
        @Cleanup
        PulsarAdmin admin = newAdmin();
        @Cleanup
        PulsarClient client = newTxnClient();
        String input = scalableTopicName();
        String output = scalableTopicName();
        admin.scalableTopics().createScalableTopic(input, 1);
        admin.scalableTopics().createScalableTopic(output, 1);

        @Cleanup
        Producer<String> seed = client.newProducer(Schema.string()).topic(input).create();
        seed.newMessage().value("hello").send();

        @Cleanup
        QueueConsumer<String> in = subscribe(client, input, "in-sub");
        @Cleanup
        Producer<String> out = client.newProducer(Schema.string()).topic(output).create();
        @Cleanup
        QueueConsumer<String> verify = subscribe(client, output, "verify-sub");

        Message<String> seedMsg = in.receive(Duration.ofSeconds(OP_TIMEOUT_SECONDS));
        assertNotNull(seedMsg, "seed message must be delivered");

        Transaction txn = client.newTransaction();
        out.newMessage().transaction(txn).value(seedMsg.value().toUpperCase()).send();
        in.acknowledge(seedMsg.id(), txn);
        assertNull(verify.receive(Duration.ofSeconds(2)), "output invisible before commit");

        txn.commit();
        Message<String> outMsg = verify.receive(Duration.ofSeconds(OP_TIMEOUT_SECONDS));
        assertNotNull(outMsg, "committed output must be delivered");
        assertEquals(outMsg.value(), "HELLO");
    }

    @Test(timeOut = 360_000)
    public void testTransactionSurvivesBrokerFailover() throws Exception {
        @Cleanup
        PulsarAdmin admin = newAdmin();
        @Cleanup
        PulsarClient client = newTxnClient();
        String topic = scalableTopicName();
        // Several segments so they spread across both brokers.
        admin.scalableTopics().createScalableTopic(topic, 4);

        @Cleanup
        Producer<String> producer = client.newProducer(Schema.string()).topic(topic).create();
        @Cleanup
        QueueConsumer<String> consumer = subscribe(client, topic, "failover-sub");

        Transaction txn = client.newTransaction();
        Set<String> sent = new HashSet<>();
        for (int i = 0; i < 10; i++) {
            String v = "before-kill-" + i;
            producer.newMessage().key("k-" + i).transaction(txn).value(v).send();
            sent.add(v);
        }

        // Kill one broker mid-transaction; coordinator partitions and segments it led are reassigned.
        BrokerContainer victim = pulsarCluster.getBrokers().iterator().next();
        log.info().attr("broker", victim.getContainerName()).log("Stopping broker mid-transaction");
        victim.stop();

        // Continue producing in the same transaction; the producer reconnects to the survivor. Retry
        // each send within a bounded window while reassignment/reconnection settles.
        for (int i = 0; i < 10; i++) {
            String v = "after-kill-" + i;
            sendWithRetry(producer, txn, "k-after-" + i, v);
            sent.add(v);
        }

        txn.commit();
        assertEquals(drain(consumer, sent.size()), sent,
                "every message in a transaction that spanned a broker failover must be delivered");
    }

    @Test(timeOut = 360_000)
    public void testCommitSpansSplit() throws Exception {
        @Cleanup
        PulsarAdmin admin = newAdmin();
        @Cleanup
        PulsarClient client = newTxnClient();
        String topic = scalableTopicName();
        admin.scalableTopics().createScalableTopic(topic, 1);

        @Cleanup
        Producer<String> producer = client.newProducer(Schema.string()).topic(topic).create();
        @Cleanup
        QueueConsumer<String> consumer = subscribe(client, topic, "split-sub");

        Transaction txn = client.newTransaction();
        Set<String> sent = new HashSet<>();
        for (int i = 0; i < 5; i++) {
            String v = "before-split-" + i;
            producer.newMessage().key("k-" + i).transaction(txn).value(v).send();
            sent.add(v);
        }
        List<Long> active = activeSegments(admin, topic);
        assertEquals(active.size(), 1, "expected one active segment before split");
        admin.scalableTopics().splitSegment(topic, active.get(0));
        Awaitility.await().untilAsserted(() -> assertEquals(activeSegments(admin, topic).size(), 2));

        for (int i = 0; i < 5; i++) {
            String v = "after-split-" + i;
            sendWithRetry(producer, txn, "k-after-" + i, v);
            sent.add(v);
        }
        assertNull(consumer.receive(Duration.ofSeconds(2)), "nothing visible before commit");
        txn.commit();
        assertEquals(drain(consumer, sent.size()), sent, "all messages across the split must be delivered");
    }

    @Test(timeOut = 360_000)
    public void testCommitSpansMerge() throws Exception {
        @Cleanup
        PulsarAdmin admin = newAdmin();
        @Cleanup
        PulsarClient client = newTxnClient();
        String topic = scalableTopicName();
        admin.scalableTopics().createScalableTopic(topic, 2);

        @Cleanup
        Producer<String> producer = client.newProducer(Schema.string()).topic(topic).create();
        @Cleanup
        QueueConsumer<String> consumer = subscribe(client, topic, "merge-sub");

        Transaction txn = client.newTransaction();
        Set<String> sent = new HashSet<>();
        for (int i = 0; i < 10; i++) {
            String v = "before-merge-" + i;
            producer.newMessage().key("k-" + i).transaction(txn).value(v).send();
            sent.add(v);
        }
        List<Long> active = activeSegments(admin, topic);
        assertEquals(active.size(), 2, "expected two active segments before merge");
        admin.scalableTopics().mergeSegments(topic, active.get(0), active.get(1));
        Awaitility.await().untilAsserted(() -> assertEquals(activeSegments(admin, topic).size(), 1));

        for (int i = 0; i < 5; i++) {
            String v = "after-merge-" + i;
            sendWithRetry(producer, txn, "k-after-" + i, v);
            sent.add(v);
        }
        assertNull(consumer.receive(Duration.ofSeconds(2)), "nothing visible before commit");
        txn.commit();
        assertEquals(drain(consumer, sent.size()), sent, "all messages across the merge must be delivered");
    }

    /** Send a transactional message, retrying within a bounded window while the cluster re-settles. */
    private void sendWithRetry(Producer<String> producer, Transaction txn, String key, String value)
            throws Exception {
        long deadline = System.currentTimeMillis() + 60_000;
        Exception last = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                producer.newMessage().key(key).transaction(txn).value(value).send();
                return;
            } catch (Exception e) {
                last = e;
                Thread.sleep(1000);
            }
        }
        assertTrue(false, "send did not succeed within the retry window: " + (last == null ? "?" : last));
    }
}
