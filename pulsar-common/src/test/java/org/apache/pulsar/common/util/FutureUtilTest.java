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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.fail;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import lombok.Cleanup;
import org.testng.annotations.Test;

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
}