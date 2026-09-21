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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;
import com.google.common.util.concurrent.MoreExecutors;
import java.lang.reflect.Proxy;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException.AlreadyClosedException;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.client.util.ExecutorProvider;
import org.apache.pulsar.common.api.proto.ScalableTopicDAG;
import org.apache.pulsar.common.api.proto.SegmentState;
import org.apache.pulsar.common.naming.TopicName;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

/** Measures actual V5 receive-loop callbacks with a controlled public V4 consumer endpoint. */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
public class V5ReceiveLoopAllocationBenchmark {
    private static final CompletableFuture<Void> READY = CompletableFuture.completedFuture(null);

    @Param({"false", "true"})
    public boolean paused;

    private CompletableFuture<Message<byte[]>> nextReceive;
    private CompletableFuture<Void> ready = READY;
    private Object delivered;
    private Message<byte[]> message;
    private ScalableQueueConsumer<byte[]> consumer;
    private ExecutorService executor;

    @Setup
    @SuppressWarnings("unchecked")
    public void setup() {
        executor = MoreExecutors.newDirectExecutorService();
        PulsarClientImpl v4 = mock(PulsarClientImpl.class, withSettings().stubOnly());
        PulsarClientV5 client = mock(PulsarClientV5.class, withSettings().stubOnly());
        ExecutorProvider external = mock(ExecutorProvider.class, withSettings().stubOnly());
        when(client.v4Client()).thenReturn(v4);
        when(v4.getInternalExecutorService()).thenReturn(executor);
        when(v4.externalExecutorProvider()).thenReturn(external);
        when(external.getExecutor()).thenReturn(executor);
        message = (Message<byte[]>) Proxy.newProxyInstance(
                Message.class.getClassLoader(), new Class<?>[]{Message.class}, (proxy, method, args) -> {
                    if ("getMessageId".equals(method.getName())) {
                        return MessageId.earliest;
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
        Consumer<byte[]> endpoint = (Consumer<byte[]>) Proxy.newProxyInstance(
                Consumer.class.getClassLoader(), new Class<?>[]{Consumer.class}, (proxy, method, args) -> {
                    return switch (method.getName()) {
                        case "receiveAsync" -> nextReceive = new CompletableFuture<>();
                        case "closeAsync" -> {
                            nextReceive.completeExceptionally(new AlreadyClosedException("benchmark closed"));
                            yield READY;
                        }
                        default -> throw new UnsupportedOperationException(method.getName());
                    };
                });
        when(v4.<byte[]>subscribeSegmentAsync(any(), any()))
                .thenReturn(CompletableFuture.completedFuture(endpoint));
        TopicName topic = TopicName.get("topic://public/default/receive-benchmark");
        DagWatchClient watch = mock(DagWatchClient.class, withSettings().stubOnly());
        when(watch.topicName()).thenReturn(topic);
        ScalableTopicDAG dag = new ScalableTopicDAG().setEpoch(0);
        dag.addSegment().setSegmentId(0).setHashStart(0).setHashEnd(0xffff)
                .setState(SegmentState.ACTIVE).setCreatedAtEpoch(0);
        ConsumerConfigurationData<byte[]> conf = new ConsumerConfigurationData<>();
        conf.setSubscriptionName("benchmark");
        consumer = ScalableQueueConsumer.createAsyncImpl(client, Schema.bytes(), conf, watch,
                ClientSegmentLayout.fromProto(dag, topic), msg -> {
                    delivered = msg;
                    return ready;
                }, null).join();
    }

    @Benchmark
    public Object receiveAndRearm() {
        ready = paused ? new CompletableFuture<>() : READY;
        if (!nextReceive.complete(message)) {
            throw new IllegalStateException("Receive loop was not rearmed");
        }
        if (paused) {
            ready.complete(null);
        }
        return delivered;
    }

    @TearDown
    public void tearDown() {
        consumer.closeAsync().join();
        executor.shutdown();
    }
}
