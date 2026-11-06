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
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.TopicMessageId;
import org.apache.pulsar.client.api.v5.Checkpoint;
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
 * Checkpoint positions follow the assigned segments and advance only when the application receives a message.
 *
 * <p>Resolving a latest start position in {@link ScalableCheckpointConsumer}: the lookup goes through a reader that
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
    public void testFailedLatestLookupDoesNotRetainPositionTrackerAfterRevocation() throws Exception {
        try (var member = new GroupMember()) {
            var consumer = (ScalableCheckpointConsumer<?>) member.consumer;
            assertThat(consumer.checkpointPositionTrackerCount()).isZero();

            member.assign(SEGMENT);
            assertThat(member.lookups).hasSize(1);
            assertThat(member.positions()).as("a pending latest lookup has no public position yet").isEmpty();
            assertThat(consumer.checkpointPositionTrackerCount())
                    .as("a valid pending attachment retains exactly one tracker").isEqualTo(1);

            assertThat(member.lookups.get(0).completeExceptionally(new PulsarClientException("latest lookup failed")))
                    .isTrue();
            member.assign();

            assertThat(member.positions()).as("a revoked failed lookup has no public checkpoint position").isEmpty();
            assertThat(consumer.checkpointPositionTrackerCount())
                    .as("revocation also removes the failed attachment's unseeded tracker").isZero();
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

    @Test
    public void testReceivingBufferedMessageAdvancesAssignedSegment() throws Exception {
        try (var member = new GroupMember(CheckpointV5.EARLIEST)) {
            member.assign(SEGMENT);
            Message<String> message = message(1);
            assertThat(member.reads.get(0).complete(message)).isTrue();
            assertThat(member.reads).as("the read loop re-arms after buffering the message").hasSize(2);
            assertThat(member.positions()).as("prefetch must not advance the checkpoint")
                    .containsOnly(Map.entry(SEGMENT.segmentId(), MessageId.earliest));

            var received = member.consumer.receive(Duration.ZERO);

            assertThat(received).isNotNull();
            assertThat(received.id()).isEqualTo(new MessageIdV5(message.getMessageId(), SEGMENT.segmentId()));
            assertThat(member.positions()).as("receiving from an assigned segment advances its position")
                    .containsOnly(Map.entry(SEGMENT.segmentId(), message.getMessageId()));
        }
    }

    @Test
    public void testReceivingBufferedMessageDoesNotRestoreRevokedSegment() throws Exception {
        try (var member = new GroupMember(CheckpointV5.EARLIEST)) {
            member.assign(SEGMENT);
            Message<String> message = message(1);
            assertThat(member.reads.get(0).complete(message)).isTrue();
            assertThat(member.reads).as("the message is buffered before revocation").hasSize(2);
            assertThat(member.positions()).containsOnly(Map.entry(SEGMENT.segmentId(), MessageId.earliest));

            member.assign();
            assertThat(member.positions()).as("revocation removes the segment's checkpoint position").isEmpty();

            // A receive may return the buffered message or find it purged on revocation. In either case,
            // the detached segment must stay outside this consumer's checkpoint.
            var received = member.consumer.receive(Duration.ZERO);
            if (received != null) {
                assertThat(received.id()).isEqualTo(new MessageIdV5(message.getMessageId(), SEGMENT.segmentId()));
            }
            assertThat(member.positions()).as("receiving after revocation must not restore a detached segment")
                    .isEmpty();
        }
    }

    @Test
    public void testBufferedMessageFromPreviousAssignmentDoesNotOverwriteNewStartPosition() throws Exception {
        try (var member = new GroupMember()) {
            member.assign(SEGMENT);
            member.lookups.get(0).complete(lastMessageIds(10));
            Message<String> previousMessage = message(11);
            assertThat(member.reads.get(0).complete(previousMessage)).isTrue();
            assertThat(member.reads).as("the previous reader buffered its message").hasSize(2);

            member.assign();
            assertThat(member.positions()).isEmpty();
            member.assign(SEGMENT);
            member.lookups.get(1).complete(lastMessageIds(20));
            assertThat(member.reads).as("the new assignment has its own reader").hasSize(3);
            assertThat(member.positions()).containsOnly(Map.entry(SEGMENT.segmentId(), lastMessageIds(20).get(0)));

            var previous = member.consumer.receive(Duration.ZERO);
            if (previous != null) {
                assertThat(previous.id())
                        .isEqualTo(new MessageIdV5(previousMessage.getMessageId(), SEGMENT.segmentId()));
            }
            assertThat(member.positions()).as("a previous reader must not overwrite the new reader's start position")
                    .containsOnly(Map.entry(SEGMENT.segmentId(), lastMessageIds(20).get(0)));

            Message<String> currentMessage = message(21);
            assertThat(member.reads.get(2).complete(currentMessage)).isTrue();
            var current = member.consumer.receive(Duration.ZERO);
            assertThat(current).isNotNull();
            assertThat(current.id()).isEqualTo(new MessageIdV5(currentMessage.getMessageId(), SEGMENT.segmentId()));
            assertThat(member.positions()).as("the current reader still advances its position")
                    .containsOnly(Map.entry(SEGMENT.segmentId(), currentMessage.getMessageId()));
        }
    }

    private static Message<String> message(long entryId) {
        Message<String> message = mock(Message.class);
        when(message.getMessageId()).thenReturn(new MessageIdImpl(1, entryId, -1));
        return message;
    }

    private static List<TopicMessageId> lastMessageIds(long entryId) {
        return List.of(TopicMessageId.create(SEGMENT.segmentTopicName(), new MessageIdImpl(1, entryId, -1)));
    }

    /**
     * A member of a checkpoint consumer group whose assignment changes, latest-start lookups, and reader
     * deliveries the test drives. The direct executor makes each delivery finish buffering before it returns.
     */
    private static final class GroupMember implements AutoCloseable {

        private final List<CompletableFuture<List<TopicMessageId>>> lookups = new ArrayList<>();
        private final List<CompletableFuture<Message<String>>> reads = new ArrayList<>();
        private final CheckpointConsumer<String> consumer;
        private ScalableConsumerClient.AssignmentChangeListener listener;
        private List<ActiveSegment> assignment = List.of();

        GroupMember() {
            this(CheckpointV5.LATEST);
        }

        GroupMember(Checkpoint startPosition) {
            PulsarClientImpl v4Client = mock(PulsarClientImpl.class);
            ExecutorProvider externalExecutor = mock(ExecutorProvider.class);
            when(externalExecutor.getExecutor()).thenReturn(MoreExecutors.newDirectExecutorService());
            when(v4Client.externalExecutorProvider()).thenReturn(externalExecutor);
            when(v4Client.timer()).thenReturn(mock(Timer.class));
            when(v4Client.createSegmentReaderAsync(any(), any())).thenAnswer(invocation -> {
                ReaderConfigurationData<?> conf = invocation.getArgument(0);
                Reader<String> reader = mock(Reader.class);
                when(reader.closeAsync()).thenReturn(CompletableFuture.completedFuture(null));
                if (MessageId.latest.equals(conf.getStartMessageId())) {
                    var lookup = new CompletableFuture<List<TopicMessageId>>();
                    lookups.add(lookup);
                    when(reader.getLastMessageIdsAsync()).thenReturn(lookup);
                } else {
                    when(reader.readNextAsync()).thenAnswer(__ -> {
                        var read = new CompletableFuture<Message<String>>();
                        reads.add(read);
                        return read;
                    });
                }
                return CompletableFuture.completedFuture(reader);
            });
            PulsarClientV5 client = mock(PulsarClientV5.class);
            when(client.v4Client()).thenReturn(v4Client);
            ScalableConsumerClient session = mock(ScalableConsumerClient.class);
            doAnswer(invocation -> listener = invocation.getArgument(0)).when(session).setListener(any());
            consumer = ScalableCheckpointConsumer.createManagedAsync(client, Schema.string(), TOPIC, session,
                    List.of(), startPosition, "member").join();
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
