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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.annotations.Test;

/**
 * How a {@code produce} run ends, against a fake client whose sends complete at once. A run with a
 * message limit or a test duration must return as soon as its test threads are done, not at the next
 * tick of its 10 s reporting interval, and only once they have closed their clients.
 */
public class PerformanceProducerEndOfRunTest {

    @Test(timeOut = 30000)
    public void returnsOnceTheMessagesAreSent() throws Exception {
        FakeProducer producer = new FakeProducer();
        producer.arguments.numMessages = 100;

        long elapsedMillis = timeRun(producer);

        assertThat(producer.sends).as("messages sent").hasValue(100);
        assertThat(producer.clientsClosed).as("clients closed").hasValue(2);
        assertThat(producer.closesInterrupted).as("client closes interrupted").hasValue(0);
        assertThat(elapsedMillis).as("run time in ms").isLessThan(5000);
    }

    @Test(timeOut = 30000)
    public void returnsOnceTheTestDurationIsOver() throws Exception {
        FakeProducer producer = new FakeProducer();
        producer.arguments.testTime = 1;

        long elapsedMillis = timeRun(producer);

        assertThat(producer.sends.get()).as("messages sent").isPositive();
        assertThat(producer.clientsClosed).as("clients closed").hasValue(2);
        assertThat(producer.closesInterrupted).as("client closes interrupted").hasValue(0);
        assertThat(elapsedMillis).as("run time in ms").isBetween(1000L, 5000L);
    }

    private static long timeRun(FakeProducer producer) throws Exception {
        long start = System.nanoTime();
        producer.run();
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
    }

    /** Two test threads, each with its own client, and no transactions. */
    private static final class FakeProducer extends PerformanceProducerBase<Object, Object, Void> {
        // A close that takes a while, like a real client's, so that an interrupt would land in it.
        private static final long CLOSE_MILLIS = 500;

        private final AtomicInteger sends = new AtomicInteger();
        private final AtomicInteger clientsClosed = new AtomicInteger();
        private final AtomicInteger closesInterrupted = new AtomicInteger();

        FakeProducer() {
            super(new PerformanceProducer());
            arguments.topics = List.of("my-topic");
            arguments.numTestThreads = 2;
            arguments.msgRate = 10_000;
        }

        @Override
        protected Object createClient() {
            return new Object();
        }

        @Override
        protected void closeClient(Object client) {
            try {
                Thread.sleep(CLOSE_MILLIS);
                clientsClosed.incrementAndGet();
            } catch (InterruptedException e) {
                closesInterrupted.incrementAndGet();
                Thread.currentThread().interrupt();
            }
        }

        @Override
        protected CompletableFuture<Object> createProducerAsync(Object client, int producerId, String topic) {
            return CompletableFuture.completedFuture(new Object());
        }

        @Override
        protected Void newTransaction(Object client) {
            return null;
        }

        @Override
        protected CompletableFuture<Void> commitTransaction(Void transaction) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        protected CompletableFuture<Void> abortTransaction(Void transaction) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        protected CompletableFuture<?> sendMessage(Object producer, byte[] payload, Void transaction,
                                                   String key, Long deliverAfterSeconds) {
            sends.incrementAndGet();
            return CompletableFuture.completedFuture(null);
        }

        @Override
        protected boolean isAlreadyClosedException(Throwable cause) {
            return false;
        }
    }
}
