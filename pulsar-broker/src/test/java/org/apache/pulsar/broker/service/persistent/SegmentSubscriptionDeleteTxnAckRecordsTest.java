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
package org.apache.pulsar.broker.service.persistent;

import static org.assertj.core.api.Assertions.assertThat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
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
import org.apache.pulsar.common.scalable.HashRange;
import org.apache.pulsar.common.scalable.SegmentTopicName;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.ScanConsumer;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

/**
 * Deleting a subscription of a segment deletes the {@link TxnPaths#TXN_OP_PREFIX} ack records of the transactions
 * still open on it, and only those: the cursor delete lets a subscription with the same name be created again, and
 * the records of the new subscription must stay.
 */
public class SegmentSubscriptionDeleteTxnAckRecordsTest extends V5ClientBaseTest {

    @Test
    public void testDeleteSubscriptionKeepsAckRecordsOfSubscriptionRecreatedWithSameName() throws Exception {
        String topic = newScalableTopic(1);
        String segment = singleSegment(topic);
        PulsarClient client = track(PulsarClient.builder()
                .serviceUrl(getBrokerServiceUrl())
                .transactionPolicy(TransactionPolicy.builder().timeout(Duration.ofMinutes(2)).build())
                .build());
        Transaction oldTxn = client.newTransaction();
        ackInTransaction(topic, client, "sub", oldTxn);
        List<String> oldRecords = ackRecords(segment, "sub");
        assertThat(oldRecords).as("ack records of the subscription to delete").hasSize(1);

        // Hold the delete once the cursor is deleted, before the ack records are.
        PersistentTopic segmentTopic = (PersistentTopic) getTopicReference(segment).orElseThrow();
        CompletableFuture<Void> gate = new CompletableFuture<>();
        segmentTopic.getSubscription("sub").setTxnAckRecordsDeleteGate(gate);
        CompletableFuture<Void> delete = admin.scalableTopics().deleteSubscriptionAsync(topic, "sub");
        Awaitility.await().untilAsserted(() -> assertThat(segmentTopic.getSubscription("sub"))
                .as("subscription of %s once its cursor is deleted", segment)
                .isNull());

        // A subscription created again with the same name acknowledges in a transaction of its own.
        Transaction newTxn = client.newTransaction();
        ackInTransaction(topic, client, "sub", newTxn);
        gate.complete(null);
        delete.get(30, TimeUnit.SECONDS);

        assertThat(ackRecords(segment, "sub"))
                .as("ack records on %s of the subscription created again with the same name", segment)
                .hasSize(1)
                .doesNotContainAnyElementsOf(oldRecords);
        oldTxn.abort();
        newTxn.abort();
    }

    /**
     * Publishes a message on {@code topic}, and acknowledges a message on {@code subscription} in {@code txn},
     * waiting until the subscription has recorded it.
     */
    private void ackInTransaction(String topic, PulsarClient client, String subscription, Transaction txn)
            throws Exception {
        String segment = singleSegment(topic);
        List<String> before = ackRecords(segment, subscription);
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
            // The acknowledgment is sent asynchronously.
            Awaitility.await().untilAsserted(() -> assertThat(ackRecords(segment, subscription))
                    .as("ack records of %s on %s", subscription, segment)
                    .hasSize(before.size() + 1));
        }
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

    private String singleSegment(String topic) throws Exception {
        var segments = admin.scalableTopics().getMetadata(topic).getSegments().values().stream()
                .filter(seg -> seg.isActive())
                .toList();
        assertThat(segments).as("active segments of %s", topic).hasSize(1);
        var range = segments.get(0).getHashRange();
        return SegmentTopicName.fromParent(TopicName.get(topic),
                HashRange.of(range.getStart(), range.getEnd()), segments.get(0).getSegmentId()).toString();
    }
}
