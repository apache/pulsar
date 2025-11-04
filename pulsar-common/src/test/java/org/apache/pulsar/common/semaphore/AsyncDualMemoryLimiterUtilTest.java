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
package org.apache.pulsar.common.semaphore;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pulsar.common.api.proto.BaseCommand;
import org.apache.pulsar.common.semaphore.AsyncDualMemoryLimiter.AsyncDualMemoryLimiterPermit;
import org.apache.pulsar.common.semaphore.AsyncDualMemoryLimiter.LimitType;
import org.apache.pulsar.common.semaphore.AsyncSemaphore.PermitAcquireCancelledException;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class AsyncDualMemoryLimiterUtilTest {

    private AsyncDualMemoryLimiterImpl limiter;

    @BeforeMethod
    public void setup() {
        limiter = new AsyncDualMemoryLimiterImpl(10000, 100, 5000, 10000, 100, 5000);
    }

    @AfterMethod(alwaysRun = true)
    public void cleanup() {
        if (limiter != null) {
            limiter.close();
            limiter = null;
        }
    }

    @Test
    public void testWithPermitsFutureSuccessfulExecution() throws Exception {
        CompletableFuture<AsyncDualMemoryLimiterPermit> permitFuture =
                limiter.acquire(100, LimitType.HEAP_MEMORY, () -> false);

        AtomicBoolean functionExecuted = new AtomicBoolean(false);
        AtomicBoolean permitReleased = new AtomicBoolean(false);

        CompletableFuture<String> result = AsyncDualMemoryLimiterUtil.withPermitsFuture(
                permitFuture,
                permit -> {
                    functionExecuted.set(true);
                    return CompletableFuture.completedFuture("success");
                },
                throwable -> CompletableFuture.completedFuture("error"),
                permit -> permitReleased.set(true)
        );

        String value = result.get(1, TimeUnit.SECONDS);
        assertEquals(value, "success");
        assertTrue(functionExecuted.get());
        assertTrue(permitReleased.get());
    }

    @Test
    public void testWithPermitsFuturePermitAcquireError() throws Exception {
        CompletableFuture<AsyncDualMemoryLimiterPermit> permitFuture =
                CompletableFuture.failedFuture(new RuntimeException("Permit acquire failed"));

        AtomicBoolean functionExecuted = new AtomicBoolean(false);
        AtomicBoolean errorHandlerExecuted = new AtomicBoolean(false);
        AtomicBoolean permitReleased = new AtomicBoolean(false);
        AtomicReference<Throwable> capturedError = new AtomicReference<>();

        CompletableFuture<String> result = AsyncDualMemoryLimiterUtil.withPermitsFuture(
                permitFuture,
                permit -> {
                    functionExecuted.set(true);
                    return CompletableFuture.completedFuture("success");
                },
                throwable -> {
                    errorHandlerExecuted.set(true);
                    capturedError.set(throwable);
                    return CompletableFuture.completedFuture("handled-error");
                },
                permit -> permitReleased.set(true)
        );

        String value = result.get(1, TimeUnit.SECONDS);
        assertEquals(value, "handled-error");
        assertTrue(errorHandlerExecuted.get());
        assertTrue(capturedError.get() instanceof RuntimeException);
        assertEquals(capturedError.get().getMessage(), "Permit acquire failed");
        assertTrue(!functionExecuted.get()); // Function should not be executed
        assertTrue(!permitReleased.get()); // Permits should not be released
    }

    @Test
    public void testWithPermitsFutureFunctionThrowsException() throws Exception {
        CompletableFuture<AsyncDualMemoryLimiterPermit> permitFuture =
                limiter.acquire(100, LimitType.HEAP_MEMORY, () -> false);

        AtomicBoolean permitReleased = new AtomicBoolean(false);

        CompletableFuture<String> result = AsyncDualMemoryLimiterUtil.withPermitsFuture(
                permitFuture,
                permit -> {
                    throw new RuntimeException("Function failed");
                },
                throwable -> CompletableFuture.completedFuture("error"),
                permit -> permitReleased.set(true)
        );

        try {
            result.get(1, TimeUnit.SECONDS);
            fail("Expected exception");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof RuntimeException);
            assertEquals(e.getCause().getMessage(), "Function failed");
        }

        // Permits should be released even when function throws
        assertTrue(permitReleased.get());
    }

    @Test
    public void testWithPermitsFutureFunctionReturnsFailedFuture() throws Exception {
        CompletableFuture<AsyncDualMemoryLimiterPermit> permitFuture =
                limiter.acquire(100, LimitType.HEAP_MEMORY, () -> false);

        AtomicBoolean permitReleased = new AtomicBoolean(false);

        CompletableFuture<String> result = AsyncDualMemoryLimiterUtil.withPermitsFuture(
                permitFuture,
                permit -> CompletableFuture.failedFuture(new RuntimeException("Async operation failed")),
                throwable -> CompletableFuture.completedFuture("error"),
                permit -> permitReleased.set(true)
        );

        try {
            result.get(1, TimeUnit.SECONDS);
            fail("Expected exception");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof RuntimeException);
            assertEquals(e.getCause().getMessage(), "Async operation failed");
        }

        // Permits should be released after the returned future completes
        assertTrue(permitReleased.get());
    }

    @Test
    public void testWithPermitsFutureReleasesPermitsAfterAsyncCompletion() throws Exception {
        CompletableFuture<AsyncDualMemoryLimiterPermit> permitFuture =
                limiter.acquire(100, LimitType.HEAP_MEMORY, () -> false);

        CompletableFuture<String> asyncOperation = new CompletableFuture<>();
        AtomicBoolean permitReleased = new AtomicBoolean(false);

        CompletableFuture<String> result = AsyncDualMemoryLimiterUtil.withPermitsFuture(
                permitFuture,
                permit -> asyncOperation,
                throwable -> CompletableFuture.completedFuture("error"),
                permit -> permitReleased.set(true)
        );

        // Permits should not be released yet
        Thread.sleep(100);
        assertTrue(!permitReleased.get());
        assertTrue(!result.isDone());

        // Complete the async operation
        asyncOperation.complete("async-result");

        String value = result.get(1, TimeUnit.SECONDS);
        assertEquals(value, "async-result");
        assertTrue(permitReleased.get());
    }

    @Test
    public void testWithPermitsFutureWithNullPermit() throws Exception {
        // Simulate a null permit scenario (edge case)
        CompletableFuture<AsyncDualMemoryLimiterPermit> permitFuture =
                CompletableFuture.completedFuture(null);

        AtomicBoolean functionExecuted = new AtomicBoolean(false);
        AtomicBoolean permitReleased = new AtomicBoolean(false);

        CompletableFuture<String> result = AsyncDualMemoryLimiterUtil.withPermitsFuture(
                permitFuture,
                permit -> {
                    functionExecuted.set(true);
                    return CompletableFuture.completedFuture("success");
                },
                throwable -> CompletableFuture.completedFuture("error"),
                permit -> permitReleased.set(true)
        );

        String value = result.get(1, TimeUnit.SECONDS);
        assertEquals(value, "success");
        assertTrue(functionExecuted.get());
        assertTrue(permitReleased.get());
    }

    @Test
    public void testAcquireDirectMemoryPermitsAndWriteAndFlushSuccess() throws Exception {
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        ChannelFuture channelFuture = mock(ChannelFuture.class);
        BaseCommand command = createTestCommand();

        AtomicReference<ByteBuf> capturedBuf = new AtomicReference<>();
        AtomicBoolean listenerCalled = new AtomicBoolean(false);

        when(ctx.writeAndFlush(any(ByteBuf.class))).thenAnswer(invocation -> {
            capturedBuf.set(invocation.getArgument(0));
            return channelFuture;
        });

        when(channelFuture.addListener(any())).thenAnswer(invocation -> {
            io.netty.util.concurrent.GenericFutureListener listener = invocation.getArgument(0);
            listenerCalled.set(true);
            // Simulate successful write
            listener.operationComplete(channelFuture);
            return channelFuture;
        });

        when(channelFuture.isSuccess()).thenReturn(true);

        AtomicBoolean errorHandlerCalled = new AtomicBoolean(false);

        CompletableFuture<Void> result = AsyncDualMemoryLimiterUtil.acquireDirectMemoryPermitsAndWriteAndFlush(
                ctx,
                limiter,
                () -> false,
                command,
                throwable -> errorHandlerCalled.set(true)
        );

        result.get(1, TimeUnit.SECONDS);

        verify(ctx, times(1)).writeAndFlush(any(ByteBuf.class));
        assertTrue(listenerCalled.get());
        assertTrue(!errorHandlerCalled.get());
        assertNotNull(capturedBuf.get());
    }

    @Test
    public void testAcquireDirectMemoryPermitsAndWriteAndFlushPermitAcquireError() throws Exception {
        // Fill up the limiter to cause permit acquisition to fail
        limiter.close();
        limiter = new AsyncDualMemoryLimiterImpl(100, 1, 100, 100, 1, 100);

        // Acquire all memory
        limiter.acquire(100, LimitType.DIRECT_MEMORY, () -> false).get(1, TimeUnit.SECONDS);

        // Try to acquire more - will be queued and timeout
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        BaseCommand command = createTestCommand();

        AtomicBoolean errorHandlerCalled = new AtomicBoolean(false);
        AtomicReference<Throwable> capturedError = new AtomicReference<>();

        CompletableFuture<Void> result = AsyncDualMemoryLimiterUtil.acquireDirectMemoryPermitsAndWriteAndFlush(
                ctx,
                limiter,
                () -> false,
                command,
                throwable -> {
                    errorHandlerCalled.set(true);
                    capturedError.set(throwable);
                }
        );

        // Wait for timeout
        Thread.sleep(200);

        assertTrue(errorHandlerCalled.get());
        assertNotNull(capturedError.get());
        verify(ctx, never()).writeAndFlush(any(ByteBuf.class));
    }

    @Test
    public void testAcquireDirectMemoryPermitsAndWriteAndFlushSerializationError() throws Exception {
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        BaseCommand command = mock(BaseCommand.class);

        // Mock the command to throw exception during serialization
        when(command.getSerializedSize()).thenReturn(100);

        AtomicBoolean errorHandlerCalled = new AtomicBoolean(false);

        CompletableFuture<Void> result = AsyncDualMemoryLimiterUtil.acquireDirectMemoryPermitsAndWriteAndFlush(
                ctx,
                limiter,
                () -> false,
                command,
                throwable -> errorHandlerCalled.set(true)
        );

        try {
            result.get(1, TimeUnit.SECONDS);
            fail("Expected exception");
        } catch (ExecutionException e) {
            // Expected exception from serialization
            assertTrue(e.getCause() != null);
        }
    }

    @Test
    public void testAcquireDirectMemoryPermitsAndWriteAndFlushWriteException() throws Exception {
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        BaseCommand command = createTestCommand();

        // Mock writeAndFlush to throw exception
        when(ctx.writeAndFlush(any(ByteBuf.class)))
                .thenThrow(new RuntimeException("Write failed"));

        AtomicBoolean errorHandlerCalled = new AtomicBoolean(false);

        CompletableFuture<Void> result = AsyncDualMemoryLimiterUtil.acquireDirectMemoryPermitsAndWriteAndFlush(
                ctx,
                limiter,
                () -> false,
                command,
                throwable -> errorHandlerCalled.set(true)
        );

        try {
            result.get(1, TimeUnit.SECONDS);
            fail("Expected exception");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof RuntimeException);
            assertEquals(e.getCause().getMessage(), "Write failed");
        }
    }

    @Test
    public void testAcquireDirectMemoryPermitsAndWriteAndFlushCancelled() throws Exception {
        // Fill up the limiter so permit acquisition will be queued
        limiter.close();
        limiter = new AsyncDualMemoryLimiterImpl(100, 10, 5000, 100, 10, 5000);
        AsyncDualMemoryLimiterPermit permits =
                limiter.acquire(100, LimitType.DIRECT_MEMORY, () -> false).get(1, TimeUnit.SECONDS);

        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        BaseCommand command = createTestCommand();

        AtomicBoolean cancelled = new AtomicBoolean(false);
        AtomicBoolean errorHandlerCalled = new AtomicBoolean(false);

        CompletableFuture<Void> result = AsyncDualMemoryLimiterUtil.acquireDirectMemoryPermitsAndWriteAndFlush(
                ctx,
                limiter,
                cancelled::get,
                command,
                throwable -> errorHandlerCalled.set(true)
        );

        // Cancel the request
        cancelled.set(true);

        limiter.release(permits);

        Awaitility.await().untilAsserted(() -> assertTrue(errorHandlerCalled.get()));
        verify(ctx, never()).writeAndFlush(any(ByteBuf.class));
        assertTrue(result.isCompletedExceptionally());
        assertThatThrownBy(() -> result.get()).hasCauseInstanceOf(PermitAcquireCancelledException.class);
    }

    @Test
    public void testAcquireDirectMemoryPermitsAndWriteAndFlushMultipleConcurrent() throws Exception {
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        ChannelFuture channelFuture = mock(ChannelFuture.class);

        when(ctx.writeAndFlush(any(ByteBuf.class))).thenReturn(channelFuture);
        when(channelFuture.addListener(any())).thenAnswer(invocation -> {
            io.netty.util.concurrent.GenericFutureListener listener = invocation.getArgument(0);
            listener.operationComplete(channelFuture);
            return channelFuture;
        });
        when(channelFuture.isSuccess()).thenReturn(true);

        int numRequests = 10;
        CompletableFuture<Void>[] futures = new CompletableFuture[numRequests];

        for (int i = 0; i < numRequests; i++) {
            BaseCommand command = createTestCommand();
            futures[i] = AsyncDualMemoryLimiterUtil.acquireDirectMemoryPermitsAndWriteAndFlush(
                    ctx,
                    limiter,
                    () -> false,
                    command,
                    throwable -> {
                    }
            );
        }

        // Wait for all to complete
        CompletableFuture.allOf(futures).get(5, TimeUnit.SECONDS);

        verify(ctx, times(numRequests)).writeAndFlush(any(ByteBuf.class));
    }

    @Test
    public void testAcquireDirectMemoryPermitsAndWriteAndFlushReleasesOnWriteFailure() throws Exception {
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        ChannelFuture channelFuture = mock(ChannelFuture.class);
        BaseCommand command = createTestCommand();

        when(ctx.writeAndFlush(any(ByteBuf.class))).thenReturn(channelFuture);
        when(channelFuture.addListener(any())).thenAnswer(invocation -> {
            io.netty.util.concurrent.GenericFutureListener listener = invocation.getArgument(0);
            // Simulate write failure
            when(channelFuture.isSuccess()).thenReturn(false);
            when(channelFuture.cause()).thenReturn(new RuntimeException("Write to socket failed"));
            listener.operationComplete(channelFuture);
            return channelFuture;
        });

        AtomicBoolean errorHandlerCalled = new AtomicBoolean(false);

        CompletableFuture<Void> result = AsyncDualMemoryLimiterUtil.acquireDirectMemoryPermitsAndWriteAndFlush(
                ctx,
                limiter,
                () -> false,
                command,
                throwable -> errorHandlerCalled.set(true)
        );

        result.get(1, TimeUnit.SECONDS);

        verify(ctx, times(1)).writeAndFlush(any(ByteBuf.class));
        // Error handler should not be called for write failures (only for permit acquire failures)
        assertTrue(!errorHandlerCalled.get());
    }

    @Test
    public void testWithPermitsFutureMultipleConcurrent() throws Exception {
        int numOperations = 20;
        CompletableFuture<String>[] futures = new CompletableFuture[numOperations];
        AtomicInteger releaseCount = new AtomicInteger(0);

        for (int i = 0; i < numOperations; i++) {
            final int index = i;
            CompletableFuture<AsyncDualMemoryLimiterPermit> permitFuture =
                    limiter.acquire(50, LimitType.HEAP_MEMORY, () -> false);

            futures[i] = AsyncDualMemoryLimiterUtil.withPermitsFuture(
                    permitFuture,
                    permit -> CompletableFuture.completedFuture("result-" + index),
                    throwable -> CompletableFuture.completedFuture("error-" + index),
                    permit -> releaseCount.incrementAndGet()
            );
        }

        CompletableFuture.allOf(futures).get(5, TimeUnit.SECONDS);

        assertEquals(releaseCount.get(), numOperations);

        for (int i = 0; i < numOperations; i++) {
            assertEquals(futures[i].get(), "result-" + i);
        }
    }

    @Test
    public void testWithAcquiredAndUpdatedPermitsDoesntLeakPermitsWhenUpdatedSizeIsLarger() throws Exception {
        for (int i = 0; i < 10_000; i++) {
            doTestWithAcquiredPermitsAndUpdatedPermits(50, 100);
        }
        assertThat(limiter.getLimiter(LimitType.HEAP_MEMORY).getAcquiredPermits()).isEqualTo(0);
        assertThat(limiter.getLimiter(LimitType.HEAP_MEMORY).getAvailablePermits()).isEqualTo(10000);
    }

    @Test
    public void testWithAcquiredAndUpdatedPermitsDoesntLeakPermitsWhenUpdatedSizeIsSmaller() throws Exception {
        for (int i = 0; i < 10_000; i++) {
            doTestWithAcquiredPermitsAndUpdatedPermits(100, 50);
        }
        assertThat(limiter.getLimiter(LimitType.HEAP_MEMORY).getAcquiredPermits()).isEqualTo(0);
        assertThat(limiter.getLimiter(LimitType.HEAP_MEMORY).getAvailablePermits()).isEqualTo(10000);
    }

    private void doTestWithAcquiredPermitsAndUpdatedPermits(int initialMemorySize, int newMemorySize) {
        CompletableFuture<Void> result = limiter.withAcquiredPermits(
                initialMemorySize,  // estimated permits
                LimitType.HEAP_MEMORY,
                () -> false,
                firstPermit -> {
                    // Simulate getting actual size and updating permits
                    return limiter.withUpdatedPermits(
                            firstPermit,
                            newMemorySize,
                            () -> false,
                            secondPermit -> {
                                return CompletableFuture.completedFuture(null);
                            },
                            throwable -> CompletableFuture.failedFuture(throwable)
                    );
                },
                throwable -> CompletableFuture.failedFuture(throwable)
        );
        assertThat(result).succeedsWithin(Duration.ofSeconds(1));
    }

    @Test
    public void testWithAcquiredPermitsDoesntLeakPermitsWhenExceptionIsThrown() throws Exception {
        assertThat(limiter.getLimiter(LimitType.HEAP_MEMORY).getAvailablePermits()).isEqualTo(10000);
        CompletableFuture<Void> result = limiter.withAcquiredPermits(
                90,  // estimated permits
                LimitType.HEAP_MEMORY,
                () -> false,
                firstPermit -> {
                    throw new RuntimeException("Exception in withAcquiredPermits");
                },
                throwable -> CompletableFuture.failedFuture(throwable)
        );
        assertThat(result).failsWithin(Duration.ofSeconds(1));
        assertThat(limiter.getLimiter(LimitType.HEAP_MEMORY).getAcquiredPermits()).isEqualTo(0);
        assertThat(limiter.getLimiter(LimitType.HEAP_MEMORY).getAvailablePermits()).isEqualTo(10000);
    }

    @Test
    public void testWithAcquiredAndUpdatedPermitsDoesntLeakPermitsWhenExceptionIsThrown() throws Exception {
        assertThat(limiter.getLimiter(LimitType.HEAP_MEMORY).getAvailablePermits()).isEqualTo(10000);
        CompletableFuture<Void> result = limiter.withAcquiredPermits(
                90,  // estimated permits
                LimitType.HEAP_MEMORY,
                () -> false,
                firstPermit -> {
                    // Simulate getting actual size and updating permits
                    return limiter.withUpdatedPermits(
                            firstPermit,
                            100,
                            () -> false,
                            secondPermit -> {
                                throw new RuntimeException("Exception in withUpdatedPermits");
                            },
                            throwable -> CompletableFuture.failedFuture(throwable)
                    );
                },
                throwable -> CompletableFuture.failedFuture(throwable)
        );
        assertThat(result).failsWithin(Duration.ofSeconds(1));
        assertThat(limiter.getLimiter(LimitType.HEAP_MEMORY).getAcquiredPermits()).isEqualTo(0);
        assertThat(limiter.getLimiter(LimitType.HEAP_MEMORY).getAvailablePermits()).isEqualTo(10000);
    }

    private BaseCommand createTestCommand() {
        BaseCommand command = new BaseCommand().setType(BaseCommand.Type.PING);
        command.setPing();
        return command;
    }
}