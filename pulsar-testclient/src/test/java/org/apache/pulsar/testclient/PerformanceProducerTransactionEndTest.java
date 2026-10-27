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
package org.apache.pulsar.testclient;

import static org.assertj.core.api.Assertions.assertThat;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * How a {@code produce -txn} run ends its transactions, against a fake client whose sends and
 * transaction ends complete when each test says. Whether {@code -m} ends the run on a transaction
 * boundary or inside a transaction, the producer must end every transaction it opens, open none after
 * the last message, and count every end before it closes the client.
 */
public class PerformanceProducerTransactionEndTest {

    // One thread, so sends complete in the order they were issued, like acknowledgements on a connection.
    private ScheduledExecutorService completions;

    @BeforeMethod
    public void setup() {
        completions = Executors.newSingleThreadScheduledExecutor();
    }

    @AfterMethod(alwaysRun = true)
    public void cleanup() {
        completions.shutdownNow();
    }

    @DataProvider
    public Object[][] awaitSends() {
        return new Object[][] {{true}, {false}};
    }

    /**
     * 100 messages fill two transactions of 50 (the {@code -nmt} default). The sends and the ends
     * complete a little later, as on a real client: without awaitSendsBeforeEndingTransaction() (the
     * v4 client), the last acknowledgements are still in flight when the second transaction ends.
     */
    @Test(dataProvider = "awaitSends", timeOut = 30000)
    public void endsEveryTransactionWhenTheLastMessageFillsATransaction(boolean awaitSends) {
        FakeTransactionalProducer producer =
                new FakeTransactionalProducer(awaitSends, () -> later(5), () -> later(50));

        producer.produce(100);

        assertThat(producer.opened).as("transactions opened").containsExactly(1, 2);
        assertThat(producer.ended).as("transactions ended").containsExactly(1, 2);
        assertThat(producer.endsCountedWhenClientClosed)
                .as("transaction ends counted when the client was closed")
                .isEqualTo(2);
    }

    /** 75 messages stop the run 25 messages into the second transaction, which must be ended too. */
    @Test(dataProvider = "awaitSends", timeOut = 30000)
    public void endsTheTransactionTheRunStopsIn(boolean awaitSends) {
        FakeTransactionalProducer producer = new FakeTransactionalProducer(awaitSends,
                () -> CompletableFuture.completedFuture(null), () -> later(50));

        producer.produce(75);

        assertThat(producer.opened).as("transactions opened").containsExactly(1, 2);
        assertThat(producer.ended).as("transactions ended").containsExactly(1, 2);
        assertThat(producer.endsCountedWhenClientClosed)
                .as("transaction ends counted when the client was closed")
                .isEqualTo(2);
    }

    /** An end that never completes holds the run up for {@code --txn-timeout}, not forever. */
    @Test(timeOut = 30000)
    public void givesUpOnTransactionEndsAfterTheTransactionTimeout() {
        FakeTransactionalProducer producer = new FakeTransactionalProducer(true,
                () -> CompletableFuture.completedFuture(null), CompletableFuture::new);
        producer.arguments.transactionTimeout = 1;

        producer.produce(100);

        assertThat(producer.ended).as("transactions ended").containsExactly(1, 2);
        assertThat(producer.endsCountedWhenClientClosed)
                .as("transaction ends counted when the client was closed")
                .isZero();
    }

    /** Stopping the tool interrupts its test threads, which must not keep waiting for the ends. */
    @Test(timeOut = 30000)
    public void stopsWaitingForTransactionEndsWhenInterrupted() throws Exception {
        CountDownLatch endsRequested = new CountDownLatch(2);
        FakeTransactionalProducer producer = new FakeTransactionalProducer(true,
                () -> CompletableFuture.completedFuture(null), () -> {
                    endsRequested.countDown();
                    return new CompletableFuture<>();
                });
        // Long enough that only the interrupt can end the wait within the test's timeout.
        producer.arguments.transactionTimeout = 600;
        Thread worker = new Thread(() -> producer.produce(100), "perf-producer-worker");
        worker.start();
        assertThat(endsRequested.await(10, TimeUnit.SECONDS)).as("both transactions ended").isTrue();

        worker.interrupt();
        worker.join(TimeUnit.SECONDS.toMillis(10));

        assertThat(worker.isAlive()).as("still waiting for the transaction ends after an interrupt").isFalse();
        assertThat(producer.endsCountedWhenClientClosed)
                .as("transaction ends counted when the client was closed")
                .isZero();
    }

    private <T> CompletableFuture<T> later(long delayMillis) {
        CompletableFuture<T> future = new CompletableFuture<>();
        completions.schedule(() -> future.complete(null), delayMillis, TimeUnit.MILLISECONDS);
        return future;
    }

    /**
     * Runs one test thread of {@code produce -txn} on a fake client. Transactions are numbered from 1,
     * and each send and each transaction end gets the future its supplier returns.
     */
    private static final class FakeTransactionalProducer extends PerformanceProducerBase<Object, Object, Integer> {
        private final boolean awaitSends;
        private final Supplier<CompletableFuture<?>> sends;
        private final Supplier<CompletableFuture<Void>> ends;
        private final List<Integer> opened = new CopyOnWriteArrayList<>();
        private final List<Integer> ended = new CopyOnWriteArrayList<>();
        private volatile long endsCountedWhenClientClosed = -1;

        FakeTransactionalProducer(boolean awaitSends, Supplier<CompletableFuture<?>> sends,
                                  Supplier<CompletableFuture<Void>> ends) {
            super(new PerformanceProducer());
            arguments.isEnableTransaction = true;
            arguments.topics = List.of("my-topic");
            this.awaitSends = awaitSends;
            this.sends = sends;
            this.ends = ends;
        }

        void produce(long numMessages) {
            runProducer(0, numMessages, 1_000_000, List.of(), new byte[16], new CountDownLatch(1));
        }

        @Override
        protected Object createClient() {
            return new Object();
        }

        @Override
        protected void closeClient(Object client) {
            endsCountedWhenClientClosed = getTransactionsEndedSuccessfully();
        }

        @Override
        protected CompletableFuture<Object> createProducerAsync(Object client, int producerId, String topic) {
            return CompletableFuture.completedFuture(new Object());
        }

        @Override
        protected Integer newTransaction(Object client) {
            opened.add(opened.size() + 1);
            return opened.size();
        }

        @Override
        protected CompletableFuture<Void> commitTransaction(Integer transaction) {
            ended.add(transaction);
            return ends.get();
        }

        @Override
        protected CompletableFuture<Void> abortTransaction(Integer transaction) {
            return commitTransaction(transaction);
        }

        @Override
        protected CompletableFuture<?> sendMessage(Object producer, byte[] payload, Integer transaction,
                                                   String key, Long deliverAfterSeconds) {
            return sends.get();
        }

        @Override
        protected boolean isAlreadyClosedException(Throwable cause) {
            return false;
        }

        @Override
        protected boolean awaitSendsBeforeEndingTransaction() {
            return awaitSends;
        }
    }
}
