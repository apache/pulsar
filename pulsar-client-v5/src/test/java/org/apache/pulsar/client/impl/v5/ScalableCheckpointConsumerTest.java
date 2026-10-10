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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import com.google.common.util.concurrent.MoreExecutors;
import io.netty.util.Timer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.TopicMessageId;
import org.apache.pulsar.client.api.v5.CheckpointConsumer;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.conf.ReaderConfigurationData;
import org.apache.pulsar.client.impl.v5.SegmentRouter.ActiveSegment;
import org.apache.pulsar.client.util.ExecutorProvider;
import org.apache.pulsar.common.scalable.HashRange;
import org.testng.annotations.Test;

/**
 * Resolving a latest start position in {@link ScalableCheckpointConsumer}: the lookup goes through a reader that
 * is not tracked with the segment readers, so it must be closed before the lookup completes, and the position it
 * yields must only be recorded for a segment that still maps to the reader being created.
 */
public class ScalableCheckpointConsumerTest {

    private static final String TOPIC = "topic://public/default/checkpoint-consumer-test";
    private static final ActiveSegment SEGMENT = new ActiveSegment(0, HashRange.of(0, 0xffff),
            "segment://public/default/checkpoint-consumer-test/0000-ffff-0", null, List.of(), List.of());
    private static final List<TopicMessageId> LAST_MESSAGE_IDS = lastMessageIds(1);

    @Test
    public void testLookupCompletesOnceTheReaderIsClosed() {
        Reader<?> lookupReader = mock(Reader.class);
        when(lookupReader.getLastMessageIdsAsync()).thenReturn(CompletableFuture.completedFuture(LAST_MESSAGE_IDS));
        CompletableFuture<Void> closing = new CompletableFuture<>();
        when(lookupReader.closeAsync()).thenReturn(closing);

        var lookup = ScalableCheckpointConsumer.lastMessageIdsThenCloseAsync(lookupReader);

        verify(lookupReader).closeAsync();
        assertThat(lookup).as("lookup before the reader is closed").isNotDone();
        closing.complete(null);
        assertThat(lookup).isCompletedWithValue(LAST_MESSAGE_IDS);
    }

    @Test
    public void testFailedLookupFailsOnceTheReaderIsClosed() {
        Reader<?> lookupReader = mock(Reader.class);
        var lookupError = new PulsarClientException("lookup failed");
        when(lookupReader.getLastMessageIdsAsync()).thenReturn(CompletableFuture.failedFuture(lookupError));
        CompletableFuture<Void> closing = new CompletableFuture<>();
        when(lookupReader.closeAsync()).thenReturn(closing);

        var lookup = ScalableCheckpointConsumer.lastMessageIdsThenCloseAsync(lookupReader);

        verify(lookupReader).closeAsync();
        assertThat(lookup).as("lookup before the reader is closed").isNotDone();
        // The lookup's error is kept, not the close's.
        closing.completeExceptionally(new PulsarClientException("close failed"));
        assertThatThrownBy(lookup::join).isInstanceOf(CompletionException.class).cause().isSameAs(lookupError);
    }

    @Test
    public void testFailedCloseKeepsTheLookupResult() {
        Reader<?> lookupReader = mock(Reader.class);
        when(lookupReader.getLastMessageIdsAsync()).thenReturn(CompletableFuture.completedFuture(LAST_MESSAGE_IDS));
        when(lookupReader.closeAsync())
                .thenReturn(CompletableFuture.failedFuture(new PulsarClientException("close failed")));

        var lookup = ScalableCheckpointConsumer.lastMessageIdsThenCloseAsync(lookupReader);

        assertThat(lookup).isCompletedWithValue(LAST_MESSAGE_IDS);
    }

    @Test
    public void testLateLookupRecordsNoPositionForARemovedSegment() throws Exception {
        try (var member = new GroupMember()) {
            member.assign(SEGMENT);
            // The segment leaves the assignment while its lookup is in flight.
            member.assign();
            member.lookups.get(0).complete(lastMessageIds(1));

            assertThat(member.positions()).doesNotContainKey(SEGMENT.segmentId());
        }
    }

    @Test
    public void testLateLookupKeepsThePositionOfTheNextReader() throws Exception {
        try (var member = new GroupMember()) {
            member.assign(SEGMENT);
            member.assign();
            // Assigned again, with another reader and its own lookup, which completes first.
            member.assign(SEGMENT);
            member.lookups.get(1).complete(lastMessageIds(2));
            member.lookups.get(0).complete(lastMessageIds(1));

            assertThat(member.positions()).containsEntry(SEGMENT.segmentId(), lastMessageIds(2).get(0));
        }
    }

    private static List<TopicMessageId> lastMessageIds(long entryId) {
        return List.of(TopicMessageId.create(SEGMENT.segmentTopicName(), new MessageIdImpl(1, entryId, -1)));
    }

    /**
     * A member of a checkpoint consumer group started at latest, whose assignment changes and latest-start
     * lookups the test drives.
     */
    private static final class GroupMember implements AutoCloseable {

        private final List<CompletableFuture<List<TopicMessageId>>> lookups = new ArrayList<>();
        private final CheckpointConsumer<String> consumer;
        private ScalableConsumerClient.AssignmentChangeListener listener;
        private List<ActiveSegment> assignment = List.of();

        GroupMember() {
            PulsarClientImpl v4Client = mock(PulsarClientImpl.class);
            ExecutorProvider externalExecutor = mock(ExecutorProvider.class);
            when(externalExecutor.getExecutor()).thenReturn(MoreExecutors.newDirectExecutorService());
            when(v4Client.externalExecutorProvider()).thenReturn(externalExecutor);
            when(v4Client.timer()).thenReturn(mock(Timer.class));
            when(v4Client.createSegmentReaderAsync(any(), any())).thenAnswer(invocation -> {
                ReaderConfigurationData<?> conf = invocation.getArgument(0);
                Reader<?> reader = mock(Reader.class);
                when(reader.closeAsync()).thenReturn(CompletableFuture.completedFuture(null));
                if (MessageId.latest.equals(conf.getStartMessageId())) {
                    var lookup = new CompletableFuture<List<TopicMessageId>>();
                    lookups.add(lookup);
                    when(reader.getLastMessageIdsAsync()).thenReturn(lookup);
                } else {
                    when(reader.readNextAsync()).thenReturn(new CompletableFuture<>());
                }
                return CompletableFuture.completedFuture(reader);
            });
            PulsarClientV5 client = mock(PulsarClientV5.class);
            when(client.v4Client()).thenReturn(v4Client);
            ScalableConsumerClient session = mock(ScalableConsumerClient.class);
            doAnswer(invocation -> listener = invocation.getArgument(0)).when(session).setListener(any());
            consumer = ScalableCheckpointConsumer.createManagedAsync(client, Schema.string(), TOPIC, session,
                    List.of(), CheckpointV5.LATEST, "member").join();
        }

        void assign(ActiveSegment... segments) {
            List<ActiveSegment> previous = assignment;
            assignment = List.of(segments);
            listener.onAssignmentChange(assignment, previous);
        }

        Map<Long, MessageId> positions() {
            return ((CheckpointV5) consumer.checkpoint()).segmentPositions();
        }

        @Override
        public void close() throws Exception {
            consumer.close();
        }
    }
}
