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
        FunctionDetails functionDetails = new FunctionDetails()
                .setTenant("public")
                .setNamespace("default")
                .setName("stop-during-setup")
                .setClassName(StringIdentityFunction.class.getName());
        functionDetails.setSource().setClassName(InterruptClearingSource.class.getName());
        functionDetails.setSink().setClassName(DiscardingSink.class.getName());
        InstanceConfig instanceConfig = new InstanceConfig();
        instanceConfig.setFunctionDetails(functionDetails);
        instanceConfig.setMaxBufferedTuples(1024);
        instanceConfig.setClusterName("test-cluster");
        @Cleanup
        PulsarClient client = PulsarClient.builder().serviceUrl("pulsar://localhost:6650").build();
        ThreadRuntime runtime = new ThreadRuntime(instanceConfig, mock(FunctionCacheManager.class),
                new ThreadGroup("test"), null, null, client, null, null, null, null, null, null, null, null,
                Optional.empty(), Optional.empty());
        runtime.start();
        InterruptClearingSource.opening.get(10, TimeUnit.SECONDS);

        long startNanos = System.nanoTime();
        runtime.stop();
        long stopMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);

        assertTrue(stopMillis < 5_000, "stop() took " + stopMillis + " ms");
        assertFalse(runtime.isAlive());
    }

    public static class StringIdentityFunction implements Function<String, String> {
        @Override
        public String process(String input, Context context) {
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
}
