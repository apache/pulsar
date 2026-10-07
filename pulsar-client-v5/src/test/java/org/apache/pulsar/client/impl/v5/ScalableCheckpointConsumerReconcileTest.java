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
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import com.google.common.util.concurrent.MoreExecutors;
import io.netty.util.Timer;
import io.netty.util.TimerTask;
import java.lang.reflect.Proxy;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.v5.CheckpointConsumer;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.conf.ReaderConfigurationData;
import org.apache.pulsar.client.util.ExecutorProvider;
import org.apache.pulsar.common.api.proto.ScalableTopicDAG;
import org.apache.pulsar.common.api.proto.SegmentState;
import org.apache.pulsar.common.naming.TopicName;
import org.testng.annotations.Test;

public class ScalableCheckpointConsumerReconcileTest {

    private static final TopicName TOPIC = TopicName.get("topic://public/default/reconcile-test");

    @Test
    public void retriesAReaderThatFailedToAttach() throws Exception {
        Map<Long, Integer> attempts = new HashMap<>();
        ArrayDeque<TimerTask> retries = new ArrayDeque<>();
        ExecutorService externalExecutor = MoreExecutors.newDirectExecutorService();
        PulsarClientImpl v4 = mock(PulsarClientImpl.class);
        PulsarClientV5 client = mock(PulsarClientV5.class);
        ExecutorProvider external = mock(ExecutorProvider.class);
        Timer timer = mock(Timer.class);
        when(client.v4Client()).thenReturn(v4);
        when(v4.externalExecutorProvider()).thenReturn(external);
        when(external.getExecutor()).thenReturn(externalExecutor);
        when(v4.timer()).thenReturn(timer);
        when(timer.newTimeout(any(), anyLong(), any())).thenAnswer(invocation -> {
            retries.add(invocation.getArgument(0));
            return null;
        });
        // Segment 1's first reader fails to attach with a transient (not segment-gone) error.
        when(v4.<byte[]>createSegmentReaderAsync(any(), any())).thenAnswer(invocation -> {
            ReaderConfigurationData<byte[]> conf = invocation.getArgument(0);
            long segmentId = Long.parseLong(conf.getTopicName().substring(
                    conf.getTopicName().lastIndexOf('-') + 1));
            int attempt = attempts.merge(segmentId, 1, Integer::sum);
            return segmentId == 1 && attempt == 1
                    ? CompletableFuture.failedFuture(new PulsarClientException("transient"))
                    : CompletableFuture.completedFuture(reader());
        });
        DagWatchClient watch = mock(DagWatchClient.class);
        when(watch.topicName()).thenReturn(TOPIC);
        DagWatchClient.LayoutChangeListener[] listener = new DagWatchClient.LayoutChangeListener[1];
        doAnswer(invocation -> {
            listener[0] = invocation.getArgument(0);
            return null;
        }).when(watch).setListener(any());

        ClientSegmentLayout initial = layout(0, false);
        CheckpointConsumer<byte[]> consumer = ScalableCheckpointConsumer.createUnmanagedAsync(client,
                Schema.bytes(), watch, initial, CheckpointV5.EARLIEST, "test").join();
        try {
            // A split: the reader for child 1 fails to attach, the one for child 2 attaches.
            listener[0].onLayoutChange(layout(1, true), initial);
            assertEquals(attempts.get(1L), 1);
            assertEquals(attempts.get(2L), 1);
            assertEquals(retries.size(), 1, "the failed reader must be retried");

            retries.poll().run(null);
            assertEquals(attempts.get(1L), 2);
            assertEquals(attempts.get(2L), 1, "attached readers are not recreated");
            assertEquals(retries.size(), 0);
        } finally {
            consumer.close();
            externalExecutor.shutdown();
        }
    }

    /** Segment 0, or after a split, sealed segment 0 with children 1 and 2. */
    private static ClientSegmentLayout layout(long epoch, boolean split) {
        ScalableTopicDAG dag = new ScalableTopicDAG().setEpoch(epoch);
        dag.addSegment().setSegmentId(0).setHashStart(0).setHashEnd(0xffff)
                .setState(split ? SegmentState.SEALED : SegmentState.ACTIVE).setCreatedAtEpoch(0);
        if (split) {
            dag.addSegment().setSegmentId(1).setHashStart(0).setHashEnd(0x7fff)
                    .setState(SegmentState.ACTIVE).setCreatedAtEpoch(epoch).addParentId(0);
            dag.addSegment().setSegmentId(2).setHashStart(0x8000).setHashEnd(0xffff)
                    .setState(SegmentState.ACTIVE).setCreatedAtEpoch(epoch).addParentId(0);
        }
        return ClientSegmentLayout.fromProto(dag, TOPIC);
    }

    /** A reader with nothing to read: its pending read never completes, and segments stay unsealed. */
    @SuppressWarnings("unchecked")
    private static Reader<byte[]> reader() {
        return (Reader<byte[]>) Proxy.newProxyInstance(Reader.class.getClassLoader(),
                new Class<?>[]{Reader.class}, (proxy, method, args) -> switch (method.getName()) {
                    case "readNextAsync" -> new CompletableFuture<>();
                    case "hasMessageAvailableAsync" -> CompletableFuture.completedFuture(true);
                    case "closeAsync" -> CompletableFuture.completedFuture(null);
                    case "hashCode" -> System.identityHashCode(proxy);
                    case "equals" -> proxy == args[0];
                    case "toString" -> "reader";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }
}
