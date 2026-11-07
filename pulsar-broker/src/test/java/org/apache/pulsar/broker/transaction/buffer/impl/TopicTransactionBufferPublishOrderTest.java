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
package org.apache.pulsar.broker.transaction.buffer.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.pulsar.broker.service.SharedPulsarBaseTest;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.transaction.buffer.AbortedTxnProcessor;
import org.apache.pulsar.broker.transaction.buffer.TransactionBufferProvider;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.client.impl.transaction.TransactionImpl;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * A producer mixing transactional and plain messages on a topic whose transaction buffer holds back the
 * transactional ones, while it recovers or takes its first snapshot: the messages must still be stored, and
 * acknowledged, in the order they were sent. A producer acknowledged out of order drops its connection and re-sends
 * its pending messages, which the broker writes again unless deduplication is on.
 */
@Test(groups = "broker")
public class TopicTransactionBufferPublishOrderTest extends SharedPulsarBaseTest {

    @DataProvider
    public Object[][] recovering() {
        return new Object[][] {{false}, {true}};
    }

    @Test(dataProvider = "recovering", timeOut = 60_000)
    public void testPlainMessageSentAfterTransactionalMessageIsStoredAfterIt(boolean recovering) throws Exception {
        String topic = newTopicName();
        // Without deduplication, a message the producer re-sends is written again.
        admin.namespaces().setDeduplicationStatus(getNamespace(), false);
        // The topic's transaction buffer holds back transactional messages while it recovers, and while it takes the
        // first snapshot that the first transactional message on the topic triggers. The test holds back either.
        CompletableFuture<Position> recovery = new CompletableFuture<>();
        CompletableFuture<Void> firstSnapshot = new CompletableFuture<>();
        AbortedTxnProcessor processor = mock(AbortedTxnProcessor.class);
        when(processor.recoverFromSnapshot())
                .thenReturn(recovering ? recovery : CompletableFuture.completedFuture(null));
        when(processor.takeAbortedTxnsSnapshot(any()))
                .thenReturn(recovering ? CompletableFuture.completedFuture(null) : firstSnapshot);
        when(processor.clearAbortedTxnSnapshot()).thenReturn(CompletableFuture.completedFuture(null));
        when(processor.closeAsync()).thenReturn(CompletableFuture.completedFuture(null));
        CountDownLatch plainMessagePublished = new CountDownLatch(1);
        TransactionBufferProvider originalProvider = getPulsar().getTransactionBufferProvider();
        getPulsar().setTransactionBufferProvider(originTopic -> {
            if (!originTopic.getName().equals(topic)) {
                return originalProvider.newTransactionBuffer(originTopic);
            }
            return new TopicTransactionBuffer((PersistentTopic) originTopic, processor,
                    AbortedTxnProcessor.SnapshotType.Single) {
                @Override
                public void addEntryInPublishOrder(ManagedLedger ledger, ByteBuf headersAndPayload,
                                                   int numberOfMessages, AsyncCallbacks.AddEntryCallback callback,
                                                   Object ctx) {
                    super.addEntryInPublishOrder(ledger, headersAndPayload, numberOfMessages, callback, ctx);
                    plainMessagePublished.countDown();
                }
            };
        });
        try {
            @Cleanup
            PulsarClient client = PulsarClient.builder()
                    .serviceUrl(getBrokerServiceUrl())
                    .enableTransaction(true)
                    .build();
            // Without batching, each message is an entry of its own, published and acknowledged on its own.
            @Cleanup
            Producer<String> producer = client.newProducer(Schema.STRING)
                    .topic(topic)
                    .enableBatching(false)
                    .create();
            @Cleanup
            Consumer<String> consumer = client.newConsumer(Schema.STRING)
                    .topic(topic)
                    .subscriptionName("sub")
                    .subscribe();
            if (!recovering) {
                // The buffer finds that the topic has no transactions yet.
                PersistentTopic persistentTopic = (PersistentTopic) getTopicIfExists(topic).get().orElseThrow();
                persistentTopic.getTransactionBuffer().checkIfTBRecoverCompletely().get(10, TimeUnit.SECONDS);
            }
            Transaction txn = client.newTransaction()
                    .withTransactionTimeout(1, TimeUnit.MINUTES)
                    .build()
                    .get(10, TimeUnit.SECONDS);
            // The first message of a transaction to a topic waits for the topic to be registered with the
            // transaction before it's sent, so a message sent right after it would go first: register it beforehand.
            ((TransactionImpl) txn).registerProducedTopic(topic).get(10, TimeUnit.SECONDS);

            CompletableFuture<MessageId> txnSend = producer.newMessage(txn).value("txn").sendAsync();
            CompletableFuture<MessageId> plainSend = producer.newMessage().value("plain").sendAsync();
            // Let the buffer hand the transactional message to the ledger once the plain one reached it.
            assertThat(plainMessagePublished.await(10, TimeUnit.SECONDS)).isTrue();
            if (recovering) {
                recovery.complete(PositionFactory.EARLIEST);
            } else {
                firstSnapshot.complete(null);
            }
            assertThat(txnSend.get(10, TimeUnit.SECONDS)).isLessThan(plainSend.get(10, TimeUnit.SECONDS));
            txn.commit().get(10, TimeUnit.SECONDS);

            // Each message is stored once, in the order it was sent.
            List<String> received = new ArrayList<>();
            for (Message<String> msg = consumer.receive(10, TimeUnit.SECONDS); msg != null;
                    msg = consumer.receive(1, TimeUnit.SECONDS)) {
                received.add(msg.getValue());
            }
            assertThat(received).containsExactly("txn", "plain");
        } finally {
            recovery.complete(null);
            firstSnapshot.complete(null);
            getPulsar().setTransactionBufferProvider(originalProvider);
        }
    }
}
