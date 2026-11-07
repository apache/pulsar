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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * How a {@code transaction} run ends its transactions, against a fake client whose sends and
 * transaction ends complete when each test says. Whether the run stops at {@code -ntxn} or at the
 * end of its test duration, the test threads must end every transaction they open, open none after
 * the last, and the run must count every end before it closes the client.
 */
public class PerformanceTransactionEndTest {

    private ScheduledExecutorService completions;
    private final List<Integer> exitCodes = new CopyOnWriteArrayList<>();

    @BeforeMethod
    public void setup() {
        completions = Executors.newSingleThreadScheduledExecutor();
        exitCodes.clear();
        PerfClientUtils.setExitProcedure(exitCodes::add);
    }

    @AfterMethod(alwaysRun = true)
    public void cleanup() {
        completions.shutdownNow();
    }

    @DataProvider
    public Object[][] awaitSends() {
        return new Object[][] {{true}, {false}};
    }

    /** {@code -ntxn 3} runs three transactions, each acknowledging one message and sending one. */
    @Test(dataProvider = "awaitSends", timeOut = 60000)
    public void opensNoTransactionAfterTheLast(boolean awaitSends) throws Exception {
        FakeTransactionalWorkers workers = new FakeTransactionalWorkers(awaitSends, () -> later(5), () -> later(50));
        workers.arguments.numTransactions = 3;

        workers.run();

        assertThat(workers.opened).as("transactions opened").containsExactly(1, 2, 3);
        assertThat(workers.ended).as("transactions ended").containsExactly(1, 2, 3);
        assertThat(workers.acknowledged).as("acknowledgements per transaction").containsOnly(
                Map.entry(1, 1), Map.entry(2, 1), Map.entry(3, 1));
        assertThat(workers.sent).as("sends per transaction").containsOnly(
                Map.entry(1, 1), Map.entry(2, 1), Map.entry(3, 1));
        assertThat(workers.violations).isEmpty();
        assertThat(workers.endsCountedWhenClientClosed)
                .as("transaction ends counted when the client was closed")
                .isEqualTo(3);
        assertThat(workers.getTransactionsOpenedSuccessfully())
                .as("transactions the summary reports as opened")
                .isEqualTo(3);
        assertThat(exitCodes).containsExactly(0);
    }

    /** When the test duration runs out, the transaction a test thread would run next is not opened. */
    @Test(timeOut = 60000)
    public void endsEveryTransactionWhenTheTestDurationRunsOut() throws Exception {
        FakeTransactionalWorkers workers = new FakeTransactionalWorkers(true, () -> later(2), () -> later(50));
        workers.arguments.testTime = 1;

        workers.run();

        assertThat(workers.opened).as("transactions opened").isNotEmpty();
        assertThat(workers.ended).as("transactions ended").containsExactlyElementsOf(workers.opened);
        assertThat(workers.violations).isEmpty();
        assertThat(workers.endsCountedWhenClientClosed)
                .as("transaction ends counted when the client was closed")
                .isEqualTo(workers.opened.size());
        assertThat(exitCodes).containsExactly(0);
    }

    /**
     * With a Failover subscription, the second test thread's consumer is a standby that never
     * receives a message, so it never finishes its transaction. The run must still stop once the
     * first thread has run the rest of {@code -ntxn}, and end the standby's transaction too.
     */
    @Test(timeOut = 60000)
    public void endsTheTransactionOfAThreadThatNeverReceives() throws Exception {
        FakeTransactionalWorkers workers = new FakeTransactionalWorkers(true, () -> later(5), () -> later(50));
        workers.standbyConsumer = 1;
        workers.arguments.numTestThreads = 2;
        workers.arguments.numTransactions = 3;
        workers.arguments.transactionTimeout = 1;

        workers.run();

        assertThat(workers.opened).as("transactions opened").hasSize(3);
        assertThat(workers.ended).as("transactions ended").containsExactlyInAnyOrderElementsOf(workers.opened);
        assertThat(workers.violations).isEmpty();
        assertThat(workers.endsCountedWhenClientClosed)
                .as("transaction ends counted when the client was closed")
                .isEqualTo(3);
        assertThat(exitCodes).containsExactly(0);
    }

    /** An end that never completes holds the run up for {@code --txn-timeout}, not forever. */
    @Test(timeOut = 60000)
    public void givesUpOnTransactionEndsAfterTheTransactionTimeout() throws Exception {
        FakeTransactionalWorkers workers = new FakeTransactionalWorkers(true,
                () -> CompletableFuture.completedFuture(null), CompletableFuture::new);
        workers.arguments.numTransactions = 2;
        workers.arguments.transactionTimeout = 1;

        workers.run();

        assertThat(workers.ended).as("transactions ended").containsExactly(1, 2);
        assertThat(workers.endsCountedWhenClientClosed)
                .as("transaction ends counted when the client was closed")
                .isZero();
        assertThat(exitCodes).containsExactly(0);
    }

    private <T> CompletableFuture<T> later(long delayMillis) {
        CompletableFuture<T> future = new CompletableFuture<>();
        completions.schedule(() -> future.complete(null), delayMillis, TimeUnit.MILLISECONDS);
        return future;
    }

    /**
     * Runs {@code transaction} on a fake client. Consumers are numbered from 0 in the order they
     * subscribe, and each receives a message whenever asked, except the standby, which never does.
     * Transactions are numbered from 1, and each send and each transaction end gets the future its
     * supplier returns.
     */
    private static final class FakeTransactionalWorkers
            extends PerformanceTransactionBase<Object, Integer, Integer, Integer, Integer> {
        private final boolean awaitSends;
        private final Supplier<CompletableFuture<?>> sends;
        private final Supplier<CompletableFuture<Void>> ends;
        private final AtomicInteger nextProducer = new AtomicInteger();
        private final AtomicInteger nextConsumer = new AtomicInteger();
        private final AtomicInteger nextMessage = new AtomicInteger();
        private final AtomicInteger nextTransaction = new AtomicInteger();
        private final List<Integer> opened = Collections.synchronizedList(new ArrayList<>());
        private final List<Integer> ended = Collections.synchronizedList(new ArrayList<>());
        private final Set<Integer> endedSet = ConcurrentHashMap.newKeySet();
        private final Map<Integer, Integer> acknowledged = new ConcurrentHashMap<>();
        private final Map<Integer, Integer> sent = new ConcurrentHashMap<>();
        private final List<String> violations = new CopyOnWriteArrayList<>();
        private volatile long endsCountedWhenClientClosed = -1;
        private volatile int standbyConsumer = -1;

        FakeTransactionalWorkers(boolean awaitSends, Supplier<CompletableFuture<?>> sends,
                                 Supplier<CompletableFuture<Void>> ends) {
            super(new PerformanceTransaction());
            this.awaitSends = awaitSends;
            this.sends = sends;
            this.ends = ends;
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
        protected CompletableFuture<Integer> createProducerAsync(Object client, String topic) {
            return CompletableFuture.completedFuture(nextProducer.getAndIncrement());
        }

        @Override
        protected CompletableFuture<Integer> subscribeAsync(Object client, String topic, String subscription) {
            return CompletableFuture.completedFuture(nextConsumer.getAndIncrement());
        }

        @Override
        protected Integer newTransaction(Object client) {
            int transaction = nextTransaction.incrementAndGet();
            opened.add(transaction);
            return transaction;
        }

        @Override
        protected CompletableFuture<Void> commitTransaction(Integer transaction) {
            ended.add(transaction);
            endedSet.add(transaction);
            return ends.get();
        }

        @Override
        protected CompletableFuture<Void> abortTransaction(Integer transaction) {
            return commitTransaction(transaction);
        }

        @Override
        protected Integer receive(Integer consumer) throws InterruptedException {
            if (consumer == standbyConsumer) {
                new CountDownLatch(1).await();
            }
            return nextMessage.incrementAndGet();
        }

        @Override
        protected CompletableFuture<Void> acknowledgeAsync(Integer consumer, Integer msg, Integer transaction) {
            record(acknowledged, "message " + msg + " acknowledged", transaction);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        protected CompletableFuture<?> sendMessage(Integer producer, byte[] payload, Integer transaction) {
            record(sent, "message sent", transaction);
            return sends.get();
        }

        private void record(Map<Integer, Integer> operations, String operation, Integer transaction) {
            if (transaction == null) {
                violations.add(operation + " outside of a transaction");
                return;
            }
            if (endedSet.contains(transaction)) {
                violations.add(operation + " under transaction " + transaction + " after it was ended");
            }
            operations.merge(transaction, 1, Integer::sum);
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
