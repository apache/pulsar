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
package org.apache.pulsar.functions.runtime.thread;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import lombok.Cleanup;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.functions.instance.InstanceCache;
import org.apache.pulsar.functions.instance.InstanceConfig;
import org.apache.pulsar.functions.proto.FunctionDetails;
import org.apache.pulsar.functions.utils.functioncache.FunctionCacheManager;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.core.Source;
import org.apache.pulsar.io.core.SourceContext;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

public class ThreadRuntimeTest {

    @AfterClass(alwaysRun = true)
    public void cleanupInstanceCache() {
        InstanceCache.shutdown();
    }

    /**
     * A stop that arrives while the instance is in its setup interrupts the instance thread, but code that runs
     * in the setup can clear the interrupt (Netty's HashedWheelTimer.start() does when the setup schedules the
     * client's first timeout). The thread must still exit instead of blocking in its first read until stop() gives
     * up waiting for it after 10 s.
     */
    @Test(timeOut = 60_000)
    public void testStopDuringSetupThatClearsTheInterrupt() throws Exception {
        InterruptClearingSource.opening = new CompletableFuture<>();
        @Cleanup
        PulsarClient client = PulsarClient.builder().serviceUrl("pulsar://localhost:6650").build();
        ThreadRuntime runtime = createRuntime(client, StringIdentityFunction.class, InterruptClearingSource.class,
                DiscardingSink.class);
        runtime.start();
        InterruptClearingSource.opening.get(10, TimeUnit.SECONDS);

        long startNanos = System.nanoTime();
        runtime.stop();
        long stopMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);

        assertTrue(stopMillis < 5_000, "stop() took " + stopMillis + " ms");
        assertFalse(runtime.isAlive());
    }

    /**
     * A stop that arrives while a sync function processes a record ends the loop on the stop request, and the
     * function returns without consuming the stop's interrupt. The instance thread must clear it before it closes
     * the instance: on an interrupted thread, closing the producer cache gives up waiting for the output producers
     * to be flushed and closed.
     */
    @Test(timeOut = 60_000)
    public void testStopDuringProcessingClosesWithoutTheInterrupt() throws Exception {
        InterruptAwaitingFunction.processing = new CompletableFuture<>();
        InterruptRecordingSink.closingThread = null;
        @Cleanup
        PulsarClient client = PulsarClient.builder().serviceUrl("pulsar://localhost:6650").build();
        ThreadRuntime runtime = createRuntime(client, InterruptAwaitingFunction.class, SingleRecordSource.class,
                InterruptRecordingSink.class);
        runtime.start();
        Thread instanceThread = InterruptAwaitingFunction.processing.get(10, TimeUnit.SECONDS);

        runtime.stop();

        assertFalse(runtime.isAlive());
        assertSame(InterruptRecordingSink.closingThread, instanceThread);
        assertFalse(InterruptRecordingSink.closedInterrupted, "the instance thread closed the sink while interrupted");
    }

    private static ThreadRuntime createRuntime(PulsarClient client, Class<?> functionClass, Class<?> sourceClass,
                                               Class<?> sinkClass) throws Exception {
        FunctionDetails functionDetails = new FunctionDetails()
                .setTenant("public")
                .setNamespace("default")
                .setName("thread-runtime-test")
                .setClassName(functionClass.getName());
        functionDetails.setSource().setClassName(sourceClass.getName());
        functionDetails.setSink().setClassName(sinkClass.getName());
        InstanceConfig instanceConfig = new InstanceConfig();
        instanceConfig.setFunctionDetails(functionDetails);
        instanceConfig.setMaxBufferedTuples(1024);
        instanceConfig.setClusterName("test-cluster");
        return new ThreadRuntime(instanceConfig, mock(FunctionCacheManager.class), new ThreadGroup("test"), null,
                null, client, null, null, null, null, null, null, null, null, Optional.empty(), Optional.empty());
    }

    public static class StringIdentityFunction implements Function<String, String> {
        @Override
        public String process(String input, Context context) {
            return input;
        }
    }

    /**
     * A sync function that processes a record until its thread is interrupted, and leaves the interrupt set.
     */
    public static class InterruptAwaitingFunction implements Function<String, String> {
        static volatile CompletableFuture<Thread> processing;

        @Override
        public String process(String input, Context context) {
            processing.complete(Thread.currentThread());
            while (!Thread.currentThread().isInterrupted()) {
                LockSupport.park(this);
            }
            return input;
        }
    }

    /**
     * A source whose open() is the setup step the stop interrupts, and that clears the interrupt.
     */
    public static class InterruptClearingSource implements Source<String> {
        static volatile CompletableFuture<Void> opening;

        private final CompletableFuture<Record<String>> nextRecord = new CompletableFuture<>();

        @Override
        public void open(Map<String, Object> config, SourceContext sourceContext) {
            opening.complete(null);
            while (!Thread.interrupted()) {
                LockSupport.park(this);
            }
        }

        @Override
        public Record<String> read() throws Exception {
            // like Consumer.receive(): waits until a record arrives or the source is closed
            return nextRecord.get();
        }

        @Override
        public void close() {
            nextRecord.completeExceptionally(new IllegalStateException("The source is closed"));
        }
    }

    /**
     * A source with a single record, whose next read waits until the source is closed.
     */
    public static class SingleRecordSource implements Source<String> {
        private final CompletableFuture<Record<String>> nextRecord = new CompletableFuture<>();
        private boolean recordRead;

        @Override
        public void open(Map<String, Object> config, SourceContext sourceContext) {
        }

        @Override
        public Record<String> read() throws Exception {
            if (!recordRead) {
                recordRead = true;
                return () -> "record";
            }
            // like Consumer.receive(): waits until a record arrives or the source is closed
            return nextRecord.get();
        }

        @Override
        public void close() {
            nextRecord.completeExceptionally(new IllegalStateException("The source is closed"));
        }
    }

    public static class DiscardingSink implements Sink<String> {
        @Override
        public void open(Map<String, Object> config, SinkContext sinkContext) {
        }

        @Override
        public void write(Record<String> record) {
        }

        @Override
        public void close() {
        }
    }

    /**
     * A sink that records the thread that closes it, and whether that thread was interrupted.
     */
    public static class InterruptRecordingSink extends DiscardingSink {
        static volatile Thread closingThread;
        static volatile boolean closedInterrupted;

        @Override
        public void close() {
            closingThread = Thread.currentThread();
            closedInterrupted = Thread.currentThread().isInterrupted();
        }
    }
}
