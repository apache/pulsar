/**
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
package org.apache.pulsar.client.impl;

import org.apache.pulsar.client.util.RetryUtil;
import org.apache.pulsar.common.util.FutureUtil;
import org.testng.annotations.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test(groups = "utils")
public class RetryUtilTest {


    @Test
    public void testFailAndRetry() throws Exception {
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        CompletableFuture<Boolean> callback = new CompletableFuture<>();
        AtomicInteger atomicInteger = new AtomicInteger(0);
        Backoff backoff = new BackoffBuilder()
                .setInitialTime(100, TimeUnit.MILLISECONDS)
                .setMax(2000, TimeUnit.MILLISECONDS)
                .setMandatoryStop(5000, TimeUnit.MILLISECONDS)
                .create();
        RetryUtil.retryAsynchronously(() -> {
            CompletableFuture<Boolean> future = new CompletableFuture<>();
            atomicInteger.incrementAndGet();
            if (atomicInteger.get() < 5) {
                future.completeExceptionally(new RuntimeException("fail"));
            } else {
                future.complete(true);
            }
            return future;
        }, backoff, executor, callback);
        assertTrue(callback.get());
        assertEquals(atomicInteger.get(), 5);
        executor.shutdownNow();
    }

    @Test
    public void testFail() throws Exception {
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        CompletableFuture<Boolean> callback = new CompletableFuture<>();
        Backoff backoff = new BackoffBuilder()
                .setInitialTime(500, TimeUnit.MILLISECONDS)
                .setMax(2000, TimeUnit.MILLISECONDS)
                .setMandatoryStop(5000, TimeUnit.MILLISECONDS)
                .create();
        long start = System.currentTimeMillis();
        RetryUtil.retryAsynchronously(() ->
                FutureUtil.failedFuture(new RuntimeException("fail")), backoff, executor, callback);
        try {
            callback.get();
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("fail"));
        }
        long time = System.currentTimeMillis() - start;
        assertTrue(time >= 5000 - 2000, "Duration:" + time);
        executor.shutdownNow();
    }
}
