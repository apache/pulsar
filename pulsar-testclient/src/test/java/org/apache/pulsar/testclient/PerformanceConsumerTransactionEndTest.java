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
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntFunction;
import java.util.function.Supplier;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * How a {@code consume -txn} run ends its transactions, against a fake client whose consumers are
 * each fed by a thread of their own, as the V5 poll loop does, and whose transaction ends complete
 * when each test says. Whether the run stops at {@code -ntxn} or inside a transaction, the consumer
 * must end every transaction it opens, open none after the last, and count every end before it
 * closes the client.
 */
public class PerformanceConsumerTransactionEndTest {

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
    public Object[][] consumers() {
        return new Object[][] {{1}, {2}};
    }

    /**
     * {@code -ntxn 2} with 5 messages per transaction, and more messages than that: the run ends
     * with the second transaction, so no third may be opened and nothing acknowledged after it.
     */
    @Test(dataProvider = "consumers", timeOut = 30000)
    public void opensNoTransactionAfterTheLast(int consumers) throws Exception {
        FakeTransactionalConsumer consumer = new FakeTransactionalConsumer(() -> later(50), endlessMessages());
        consumer.arguments.numConsumers = consumers;
        consumer.arguments.numMessagesPerTransaction = 5;
        consumer.arguments.totalNumTxn = 2;

        consumer.run();

        assertThat(consumer.opened).as("transactions opened").containsExactly(1, 2);
        assertThat(consumer.ended).as("transactions ended").containsExactly(1, 2);
        assertThat(consumer.acknowledged).as("acknowledgements per transaction").containsOnly(
                Map.entry(1, 5), Map.entry(2, 5));
        assertThat(consumer.violations).isEmpty();
        assertThat(consumer.stuckConsumers).as("consumers still running after the run").isEmpty();
        assertThat(consumer.endsCountedWhenClientClosed)
                .as("transaction ends counted when the client was closed")
                .isEqualTo(2);
        assertThat(exitCodes).containsOnly(0);
    }

    /**
     * {@code -m 8} with 5 messages per transaction stops the run 2 acknowledgements into the second
     * transaction (the 8th message trips the limit and is not acknowledged). That transaction must be
     * ended too, and counted as opened.
     */
    @Test(timeOut = 30000)
    public void endsTheTransactionTheRunStopsIn() throws Exception {
        FakeTransactionalConsumer consumer = new FakeTransactionalConsumer(() -> later(50), endlessMessages());
        consumer.arguments.numMessagesPerTransaction = 5;
        consumer.arguments.numMessages = 8;

        consumer.run();

        assertThat(consumer.opened).as("transactions opened").containsExactly(1, 2);
        assertThat(consumer.ended).as("transactions ended").containsExactly(1, 2);
        assertThat(consumer.acknowledged).as("acknowledgements per transaction").containsOnly(
                Map.entry(1, 5), Map.entry(2, 2));
        assertThat(consumer.violations).isEmpty();
        assertThat(consumer.endsCountedWhenClientClosed)
                .as("transaction ends counted when the client was closed")
                .isEqualTo(2);
        assertThat(consumer.getTransactionsOpenedSuccessfully())
                .as("transactions the summary reports as opened")
                .isEqualTo(2);
    }

    /**
     * One consumer is still acknowledging under the open transaction when the other one's message
     * trips {@code -m}: the transaction may only be ended once that acknowledgement is issued, as the
     * client rejects an acknowledgement under a transaction that is ending.
     */
    @Test(timeOut = 30000)
    public void endsTheOpenTransactionAfterTheAcknowledgementsInFlight() throws Exception {
        CountDownLatch acknowledging = new CountDownLatch(1);
        // Consumer 0 receives messages 1 and 2, and is held while it acknowledges message 2;
        // consumer 1 receives message 3, the one that trips -m, meanwhile.
        FakeTransactionalConsumer consumer = new FakeTransactionalConsumer(() -> later(50), c -> {
            if (c == 0) {
                List<Integer> messages = new ArrayList<>(List.of(1, 2));
                return () -> messages.isEmpty() ? null : messages.remove(0);
            }
            List<Integer> messages = new ArrayList<>(List.of(3));
            return () -> {
                awaitUninterruptibly(acknowledging);
                return messages.isEmpty() ? null : messages.remove(0);
            };
        });
        consumer.heldAcknowledgement = 2;
        consumer.acknowledging = acknowledging;
        consumer.arguments.numConsumers = 2;
        consumer.arguments.numMessagesPerTransaction = 10;
        consumer.arguments.numMessages = 3;

        consumer.run();

        assertThat(consumer.ended).as("transactions ended").containsExactly(1);
        assertThat(consumer.acknowledged).as("acknowledgements per transaction").containsOnly(Map.entry(1, 2));
        assertThat(consumer.violations).isEmpty();
        assertThat(consumer.endsCountedWhenClientClosed)
                .as("transaction ends counted when the client was closed")
                .isEqualTo(1);
    }

    /** An end that never completes holds the run up for {@code --txn-timeout}, not forever. */
    @Test(timeOut = 30000)
    public void givesUpOnTransactionEndsAfterTheTransactionTimeout() throws Exception {
        FakeTransactionalConsumer consumer = new FakeTransactionalConsumer(CompletableFuture::new, endlessMessages());
        consumer.arguments.numMessagesPerTransaction = 5;
        consumer.arguments.numMessages = 8;
        consumer.arguments.transactionTimeout = 1;

        consumer.run();

        assertThat(consumer.ended).as("transactions ended").containsExactly(1, 2);
        assertThat(consumer.endsCountedWhenClientClosed)
                .as("transaction ends counted when the client was closed")
                .isZero();
        assertThat(exitCodes).containsOnly(0);
    }

    /** Stopping the consumer that ends the run must not leave it waiting for the transaction ends. */
    @Test(timeOut = 30000)
    public void stopsWaitingForTransactionEndsWhenInterrupted() throws Exception {
        CountDownLatch endsRequested = new CountDownLatch(2);
        FakeTransactionalConsumer consumer = new FakeTransactionalConsumer(() -> {
            endsRequested.countDown();
            return new CompletableFuture<>();
        }, endlessMessages());
        consumer.arguments.numMessagesPerTransaction = 5;
        consumer.arguments.numMessages = 8;
        // Long enough that only the interrupt can end the wait within the test's timeout.
        consumer.arguments.transactionTimeout = 600;
        Thread run = new Thread(() -> {
            try {
                consumer.run();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, "perf-consumer-run");
        run.start();
        assertThat(endsRequested.await(10, TimeUnit.SECONDS)).as("both transactions ended").isTrue();

        consumer.feeders.get(0).interrupt();
        run.join(TimeUnit.SECONDS.toMillis(10));

        assertThat(run.isAlive()).as("still waiting for the transaction ends after an interrupt").isFalse();
        assertThat(consumer.endsCountedWhenClientClosed)
                .as("transaction ends counted when the client was closed")
                .isZero();
    }

    private static IntFunction<Supplier<Integer>> endlessMessages() {
        return c -> {
            AtomicInteger next = new AtomicInteger();
            return next::incrementAndGet;
        };
    }

    private static void awaitUninterruptibly(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private <T> CompletableFuture<T> later(long delayMillis) {
        CompletableFuture<T> future = new CompletableFuture<>();
        completions.schedule(() -> future.complete(null), delayMillis, TimeUnit.MILLISECONDS);
        return future;
    }

    /**
     * Runs {@code consume -txn} on a fake client. Consumers are numbered from 0 and each is fed by a
     * thread of its own from the messages its source gives, until the source runs dry (a
     * {@code null}) or the run is done. Transactions are numbered from 1, and each end gets the
     * future the {@code ends} supplier returns.
     */
    private static final class FakeTransactionalConsumer
            extends PerformanceConsumerBase<Object, Integer, Integer, Integer> {
        private final Supplier<CompletableFuture<Void>> ends;
        private final IntFunction<Supplier<Integer>> messages;
        private final AtomicInteger nextConsumer = new AtomicInteger();
        private final AtomicInteger nextTransaction = new AtomicInteger();
        private final List<Integer> opened = new CopyOnWriteArrayList<>();
        private final List<Integer> ended = new CopyOnWriteArrayList<>();
        private final Map<Integer, Integer> acknowledged = new ConcurrentHashMap<>();
        private final List<String> violations = new CopyOnWriteArrayList<>();
        private final List<Thread> feeders = new CopyOnWriteArrayList<>();
        private final List<String> stuckConsumers = new CopyOnWriteArrayList<>();
        private volatile long endsCountedWhenClientClosed = -1;
        /** A message whose acknowledgement is held until an end is requested, or for a second. */
        private volatile int heldAcknowledgement = -1;
        private volatile CountDownLatch acknowledging = new CountDownLatch(1);
        private final CountDownLatch endRequested = new CountDownLatch(1);

        FakeTransactionalConsumer(Supplier<CompletableFuture<Void>> ends, IntFunction<Supplier<Integer>> messages) {
            super(new PerformanceConsumer());
            arguments.isEnableTransaction = true;
            arguments.topics = List.of("my-topic");
            this.ends = ends;
            this.messages = messages;
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
            endRequested.countDown();
            return ends.get();
        }

        @Override
        protected CompletableFuture<Void> abortTransaction(Integer transaction) {
            return commitTransaction(transaction);
        }

        @Override
        protected int messageSize(Integer msg) {
            return 16;
        }

        @Override
        protected long publishTimeMillis(Integer msg) {
            return System.currentTimeMillis();
        }

        @Override
        protected void acknowledge(Integer consumer, Integer msg, Integer transaction) {
            if (transaction == null) {
                violations.add("message " + msg + " acknowledged outside of a transaction");
            } else if (ended.contains(transaction)) {
                violations.add("message " + msg + " acknowledged under transaction " + transaction
                        + " after it was ended");
            }
            if (msg == heldAcknowledgement) {
                acknowledging.countDown();
                try {
                    if (endRequested.await(1, TimeUnit.SECONDS)) {
                        violations.add("transaction " + transaction + " ended while message " + msg
                                + " was being acknowledged under it");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            if (transaction != null) {
                acknowledged.merge(transaction, 1, Integer::sum);
            }
            ackSucceeded();
        }

        /** One thread per consumer, as the V5 poll loop. */
        @Override
        protected void startConsuming(List<Integer> consumers) {
            for (Integer consumer : consumers) {
                Supplier<Integer> source = messages.apply(consumer);
                Thread feeder = new Thread(() -> {
                    while (!Thread.currentThread().isInterrupted()) {
                        if (checkDone()) {
                            return;
                        }
                        Integer msg = source.get();
                        if (msg == null || handleMessage(consumer, msg)) {
                            return;
                        }
                    }
                }, "fake-consumer-" + consumer);
                feeders.add(feeder);
                feeder.start();
            }
        }

        /**
         * Waits for the feeders without interrupting them, as nothing interrupts the v4 client's
         * listener threads either: each must find out by itself that the run is over.
         */
        @Override
        protected void stopConsuming() {
            // run() gets here interrupted, by the run reporting itself done.
            boolean interrupted = Thread.interrupted();
            try {
                for (Thread feeder : feeders) {
                    feeder.join(TimeUnit.SECONDS.toMillis(10));
                    if (feeder.isAlive()) {
                        stuckConsumers.add(feeder.getName());
                        feeder.interrupt();
                    }
                }
            } catch (InterruptedException e) {
                interrupted = true;
            } finally {
                if (interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }
}
