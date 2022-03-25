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

package org.apache.pulsar.common.util;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import lombok.Cleanup;
import org.assertj.core.util.Lists;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class FutureUtilTest {

    @Test
    public void testCreateTimeoutException() {
        TimeoutException timeoutException = FutureUtil.createTimeoutException("hello world", getClass(), "test(...)");
        assertNotNull(timeoutException);
        assertEquals(timeoutException.getMessage(), "hello world");
        StringWriter stringWriter = new StringWriter();
        timeoutException.printStackTrace(new PrintWriter(stringWriter, true));
        assertEquals(stringWriter.toString(),
                "org.apache.pulsar.common.util.FutureUtil$LowOverheadTimeoutException: "
                + "hello world" + System.lineSeparator()
                + "\tat org.apache.pulsar.common.util.FutureUtilTest.test(...)(Unknown Source)" + System.lineSeparator());
    }

    @Test
    public void testTimeoutHandling() {
        CompletableFuture<Void> future = new CompletableFuture<>();
        @Cleanup("shutdownNow")
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
        Exception e = new Exception();
        try {
            FutureUtil.addTimeoutHandling(future, Duration.ofMillis(1), executor, () -> e);
            future.get();
            fail("Should have failed.");
        } catch (InterruptedException interruptedException) {
            fail("Shouldn't occur");
        } catch (ExecutionException executionException) {
            assertEquals(executionException.getCause(), e);
        }
    }

    @Test
    public void testTimeoutHandlingNoTimeout() throws ExecutionException, InterruptedException {
        CompletableFuture<Void> future = new CompletableFuture<>();
        @Cleanup("shutdownNow")
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
        FutureUtil.addTimeoutHandling(future, Duration.ofMillis(100), executor, () -> new Exception());
        future.complete(null);
        future.get();
    }

    @Test
    public void testCreatingFutureWithTimeoutHandling() {
        @Cleanup("shutdownNow")
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
        Exception e = new Exception();
        try {
            CompletableFuture<Void> future = FutureUtil.createFutureWithTimeout(Duration.ofMillis(1), executor,
                    () -> e);
            future.get();
            fail("Should have failed.");
        } catch (InterruptedException interruptedException) {
            fail("Shouldn't occur");
        } catch (ExecutionException executionException) {
            assertEquals(executionException.getCause(), e);
        }
    }

    @Test
    public void testGetOriginalException() {
        CompletableFuture<Void> future = CompletableFuture.completedFuture(null);
        CompletableFuture<Void> exceptionFuture = future.thenAccept(__ -> {
            throw new IllegalStateException("Illegal state");
        });
        assertTrue(exceptionFuture.isCompletedExceptionally());
        try {
            exceptionFuture.get();
        } catch (InterruptedException | ExecutionException e) {
            Throwable originalException = FutureUtil.unwrapCompletionException(e);
            assertTrue(originalException instanceof IllegalStateException);
        }
        CompletableFuture<Object> exceptionFuture2 = new CompletableFuture<>();
        exceptionFuture2.completeExceptionally(new IllegalStateException("Completed exception"));
        final List<Throwable> future2Exception = Lists.newArrayList();
        exceptionFuture2.exceptionally(ex -> {
            future2Exception.add(FutureUtil.unwrapCompletionException(ex));
            return null;
        });
        Awaitility.await()
                .untilAsserted(() -> {
                    assertEquals(future2Exception.size(), 1);
                    assertTrue(future2Exception.get(0) instanceof IllegalStateException);
                });
        final List<Throwable> future3Exception = Lists.newArrayList();
        CompletableFuture.completedFuture(null)
                .thenAccept(__ -> {
                    throw new IllegalStateException("Throw illegal exception");
                })
                .exceptionally(ex -> {
                    future3Exception.add(FutureUtil.unwrapCompletionException(ex));
                    return null;
                });
        Awaitility.await()
                .untilAsserted(() -> {
                    assertEquals(future3Exception.size(), 1);
                    assertTrue(future3Exception.get(0) instanceof IllegalStateException);
                });
    }
}