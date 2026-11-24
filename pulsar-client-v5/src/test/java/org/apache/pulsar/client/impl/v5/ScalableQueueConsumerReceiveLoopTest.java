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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import com.google.common.util.concurrent.MoreExecutors;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.util.ArrayDeque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException.AlreadyClosedException;
import org.apache.pulsar.client.api.PulsarClientException.TopicTerminatedException;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.client.util.ExecutorProvider;
import org.apache.pulsar.common.api.proto.ScalableTopicDAG;
import org.apache.pulsar.common.api.proto.SegmentState;
import org.apache.pulsar.common.naming.TopicName;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ScalableQueueConsumerReceiveLoopTest {
    private static final CompletableFuture<Void> READY = CompletableFuture.completedFuture(null);
    private CompletableFuture<Message<byte[]>> nextReceive;
    private CompletableFuture<Void> ready;
    private MessageSink<byte[]> sink;
    private Message<byte[]> message;
    private ScalableQueueConsumer<byte[]> consumer;
    private ExecutorService externalExecutor;
    private ExecutorService internalExecutor;
    private ArrayDeque<Runnable> retries;
    private int receives;
    private int closes;
    private int deliveries;

    @BeforeMethod
    @SuppressWarnings("unchecked")
    public void setup() {
        receives = 0;
        closes = 0;
        deliveries = 0;
        ready = READY;
        retries = new ArrayDeque<>();
        sink = msg -> {
            deliveries++;
            return ready;
        };
        externalExecutor = MoreExecutors.newDirectExecutorService();
        internalExecutor = mock(ExecutorService.class);
        doAnswer(invocation -> {
            retries.add(invocation.getArgument(0));
            return null;
        }).when(internalExecutor).execute(any());
        PulsarClientImpl v4 = mock(PulsarClientImpl.class);
        PulsarClientV5 client = mock(PulsarClientV5.class);
        ExecutorProvider external = mock(ExecutorProvider.class);
        when(client.v4Client()).thenReturn(v4);
        when(v4.getInternalExecutorService()).thenReturn(internalExecutor);
        when(v4.externalExecutorProvider()).thenReturn(external);
        when(external.getExecutor()).thenReturn(externalExecutor);
        message = mock(Message.class);
        when(message.getMessageId()).thenReturn(MessageId.earliest);
        Consumer<byte[]> endpoint = (Consumer<byte[]>) Proxy.newProxyInstance(
                Consumer.class.getClassLoader(), new Class<?>[]{Consumer.class}, (proxy, method, args) -> {
                    return switch (method.getName()) {
                        case "receiveAsync" -> {
                            receives++;
                            yield nextReceive = new CompletableFuture<>();
                        }
                        case "closeAsync" -> {
                            closes++;
                            nextReceive.completeExceptionally(new AlreadyClosedException("closed"));
                            yield READY;
                        }
                        default -> throw new UnsupportedOperationException(method.getName());
                    };
                });
        when(v4.<byte[]>subscribeSegmentAsync(any(), any()))
                .thenReturn(CompletableFuture.completedFuture(endpoint));
        TopicName topic = TopicName.get("topic://public/default/receive-loop-test");
        DagWatchClient watch = mock(DagWatchClient.class);
        when(watch.topicName()).thenReturn(topic);
        ScalableTopicDAG dag = new ScalableTopicDAG().setEpoch(0);
        dag.addSegment().setSegmentId(0).setHashStart(0).setHashEnd(0xffff)
                .setState(SegmentState.ACTIVE).setCreatedAtEpoch(0);
        ConsumerConfigurationData<byte[]> conf = new ConsumerConfigurationData<>();
        conf.setSubscriptionName("test");
        consumer = ScalableQueueConsumer.createAsyncImpl(client, Schema.bytes(), conf, watch,
                ClientSegmentLayout.fromProto(dag, topic), msg -> sink.accept(msg), null).join();
        assertEquals(receives, 1);
    }

    @AfterMethod(alwaysRun = true)
    public void cleanup() {
        if (consumer != null) {
            consumer.closeAsync().join();
            consumer = null;
        }
        if (externalExecutor != null) {
            externalExecutor.shutdown();
        }
    }

    @Test
    public void waitsForSinkReadinessBeforeReceivingAgain() {
        ready = new CompletableFuture<>();
        nextReceive.complete(message);
        assertEquals(deliveries, 1);
        assertEquals(receives, 1);
        ready.complete(null);
        assertEquals(receives, 2);
        nextReceive.complete(message);
        assertEquals(deliveries, 2);
        assertEquals(receives, 3);
    }

    @Test
    public void closeWhilePausedDoesNotRearm() {
        ready = new CompletableFuture<>();
        nextReceive.complete(message);
        consumer.closeAsync().join();
        ready.complete(null);
        assertEquals(receives, 1);
        assertEquals(closes, 1);
        assertTrue(retries.isEmpty());
    }

    @Test
    public void failedReadinessDoesNotRearm() {
        ready = new CompletableFuture<>();
        nextReceive.complete(message);
        ready.completeExceptionally(new IOException("sink readiness failed"));
        assertEquals(receives, 1);
        assertTrue(retries.isEmpty());
    }

    @Test
    public void transientReceiveErrorRetriesOnInternalExecutor() {
        nextReceive.completeExceptionally(new CompletionException(new IOException("transient")));
        assertEquals(receives, 1);
        assertEquals(retries.size(), 1);
        retries.remove().run();
        assertEquals(receives, 2);
        nextReceive.complete(message);
        assertEquals(deliveries, 1);
        assertEquals(receives, 3);
    }

    @DataProvider
    public Object[][] sinkFailures() {
        return new Object[][]{{false}, {true}};
    }

    @Test(dataProvider = "sinkFailures")
    public void throwingSinkRetriesOnInternalExecutor(boolean fatal) {
        sink = msg -> {
            if (fatal) {
                throw new AssertionError("sink failed");
            }
            throw new IllegalStateException("sink rejected message");
        };
        nextReceive.complete(message);
        assertEquals(receives, 1);
        assertEquals(retries.size(), 1);
        sink = msg -> {
            deliveries++;
            return READY;
        };
        retries.remove().run();
        nextReceive.complete(message);
        assertEquals(deliveries, 1);
        assertEquals(receives, 3);
    }

    @Test
    public void rejectedRetryDoesNotMutateReceiveFailure() {
        doThrow(new RejectedExecutionException("executor closed")).when(internalExecutor).execute(any());
        IOException failure = new IOException("receive failed");
        nextReceive.completeExceptionally(failure);
        assertEquals(failure.getSuppressed().length, 0);
        assertEquals(receives, 1);
        assertTrue(retries.isEmpty());
    }

    @DataProvider
    public Object[][] terminalErrors() {
        return new Object[][]{{new AlreadyClosedException("closed"), 0},
                {new TopicTerminatedException("sealed"), 1}};
    }

    @Test(dataProvider = "terminalErrors")
    public void terminalReceiveErrorsStopLoop(Throwable error, int expectedCloses) {
        nextReceive.completeExceptionally(error);
        assertEquals(receives, 1);
        assertEquals(closes, expectedCloses);
        assertTrue(retries.isEmpty());
    }
}
