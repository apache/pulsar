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

import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.common.util.FutureUtil;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker-impl")
public class ConnectionHandlerTest extends ProducerConsumerBase {

    private static final Backoff BACKOFF = new BackoffBuilder().setInitialTime(1, TimeUnit.MILLISECONDS)
            .setMandatoryStop(1, TimeUnit.SECONDS)
            .setMax(3, TimeUnit.SECONDS).create();
    private final ExecutorService executor = Executors.newFixedThreadPool(4);

    @BeforeClass(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterClass
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
        executor.shutdown();
    }

    @Test(timeOut = 30000)
    public void testSynchronousGrabCnx() {
        for (int i = 0; i < 10; i++) {
            final CompletableFuture<Integer> future = new CompletableFuture<>();
            final int index = i;
            final ConnectionHandler handler = new ConnectionHandler(
                    new MockedHandlerState((PulsarClientImpl) pulsarClient, "my-topic"), BACKOFF,
                    cnx -> {
                        future.complete(index);
                        return CompletableFuture.completedFuture(null);
                    });
            handler.grabCnx();
            Assert.assertEquals(future.join().intValue(), i);
        }
    }

    @Test
    public void testConcurrentGrabCnx() {
        final AtomicInteger cnt = new AtomicInteger(0);
        final ConnectionHandler handler = new ConnectionHandler(
                new MockedHandlerState((PulsarClientImpl) pulsarClient, "my-topic"), BACKOFF,
                cnx -> {
                    cnt.incrementAndGet();
                    return CompletableFuture.completedFuture(null);
                });
        final int numGrab = 10;
        for (int i = 0; i < numGrab; i++) {
            handler.grabCnx();
        }
        Awaitility.await().atMost(Duration.ofSeconds(3)).until(() -> cnt.get() > 0);
        Assert.assertThrows(ConditionTimeoutException.class,
                () -> Awaitility.await().atMost(Duration.ofMillis(500)).until(() -> cnt.get() == numGrab));
        Assert.assertEquals(cnt.get(), 1);
    }

    @Test
    public void testDuringConnectInvokeCount() throws IllegalAccessException {
        // 1. connectionOpened completes with null
        final AtomicBoolean duringConnect = spy(new AtomicBoolean());
        final ConnectionHandler handler1 = new ConnectionHandler(
                new MockedHandlerState((PulsarClientImpl) pulsarClient, "my-topic"), BACKOFF,
                cnx -> CompletableFuture.completedFuture(null));
        FieldUtils.writeField(handler1, "duringConnect", duringConnect, true);
        handler1.grabCnx();
        Awaitility.await().atMost(Duration.ofSeconds(3)).until(() -> !duringConnect.get());
        verify(duringConnect, times(1)).compareAndSet(false, true);
        verify(duringConnect, times(1)).set(false);

        // 2. connectionFailed is called
        final ConnectionHandler handler2 = new ConnectionHandler(
                new MockedHandlerState((PulsarClientImpl) pulsarClient, null), new MockedBackoff(),
                cnx -> CompletableFuture.completedFuture(null));
        FieldUtils.writeField(handler2, "duringConnect", duringConnect, true);
        handler2.grabCnx();
        Awaitility.await().atMost(Duration.ofSeconds(3)).until(() -> !duringConnect.get());
        verify(duringConnect, times(2)).compareAndSet(false, true);
        verify(duringConnect, times(2)).set(false);

        // 3. connectionOpened completes exceptionally
        final ConnectionHandler handler3 = new ConnectionHandler(
                new MockedHandlerState((PulsarClientImpl) pulsarClient, "my-topic"), new MockedBackoff(),
                cnx -> FutureUtil.failedFuture(new RuntimeException("fail")));
        FieldUtils.writeField(handler3, "duringConnect", duringConnect, true);
        handler3.grabCnx();
        Awaitility.await().atMost(Duration.ofSeconds(3)).until(() -> !duringConnect.get());
        verify(duringConnect, times(3)).compareAndSet(false, true);
        verify(duringConnect, times(3)).set(false);
    }

    private static class MockedHandlerState extends HandlerState {

        public MockedHandlerState(PulsarClientImpl client, String topic) {
            super(client, topic);
        }

        @Override
        String getHandlerName() {
            return "mocked";
        }
    }

    private static class MockedBackoff extends Backoff {

        // Set a large backoff so that reconnection won't happen in tests
        public MockedBackoff() {
            super(1, TimeUnit.HOURS, 2, TimeUnit.HOURS, 1, TimeUnit.HOURS);
        }
    }
}
