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
package org.apache.pulsar.client.impl.v5;

import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.v5.MessageId;
import org.apache.pulsar.client.api.v5.Producer;
import org.apache.pulsar.client.api.v5.PulsarClient;
import org.apache.pulsar.client.api.v5.PulsarClientException;
import org.apache.pulsar.client.api.v5.V5ClientBaseTest;
import org.apache.pulsar.client.api.v5.config.BatchingPolicy;
import org.apache.pulsar.client.api.v5.config.ConnectionPolicy;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

/**
 * A send that fails because its client is closing must still complete its future. The failure is
 * delivered on the client's completion executor, which the client shuts down with
 * {@code shutdownNow()}, dropping whatever is still queued on it at that moment.
 */
public class V5ProducerClosingCompletionTest extends V5ClientBaseTest {

    /** One callback thread, so that holding it holds the producer's only completion executor. */
    private PulsarClient newClientWithOneCallbackThread() throws Exception {
        return track(PulsarClient.builder()
                .serviceUrl(getBrokerServiceUrl())
                .connectionPolicy(ConnectionPolicy.builder().ioThreads(1).callbackThreads(1).build())
                .build());
    }

    private static void awaitQuietly(CountDownLatch gate) {
        try {
            gate.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Test
    public void aSendFailedWhileTheClientIsClosingCompletesEvenIfItsCompletionWasQueued() throws Exception {
        PulsarClient client = newClientWithOneCallbackThread();
        PulsarClientImpl v4Client = ((PulsarClientV5) client).v4Client();
        String topic = newScalableTopic(1);
        Producer<byte[]> producer = track(client.newProducer(Schema.bytes())
                .topic(topic)
                .batchingPolicy(BatchingPolicy.ofDisabled())
                .create());
        // The per-segment producer exists, so a later send has nothing to create.
        producer.newMessage().value(new byte[16]).send();

        // Hold the only completion executor: a completion handed to it now stays queued behind this.
        CountDownLatch gate = new CountDownLatch(1);
        v4Client.externalExecutorProvider().getExecutor().execute(() -> awaitQuietly(gate));
        try {
            // Close on another thread: close() returns only once the client's executors are gone.
            CompletableFuture<Void> closing = CompletableFuture.runAsync(() -> {
                try {
                    client.close();
                } catch (Exception e) {
                    throw new CompletionException(e);
                }
            });
            Awaitility.await().pollDelay(Duration.ZERO).pollInterval(Duration.ofMillis(1))
                    .until(v4Client::isClosed);

            // Fails right away, the client is closing; its completion is handed to the held executor.
            CompletableFuture<MessageId> send = producer.async().newMessage().value(new byte[16]).send();

            // shutdownNow(): the held task is interrupted and whatever was queued behind it is dropped.
            closing.get(30, TimeUnit.SECONDS);
            gate.countDown();

            ExecutionException failure = expectThrows(ExecutionException.class,
                    () -> send.get(5, TimeUnit.SECONDS));
            assertTrue(failure.getCause() instanceof PulsarClientException.AlreadyClosedException,
                    "the send must fail because the client closed, got " + failure.getCause());
        } finally {
            gate.countDown();
        }
    }
}
