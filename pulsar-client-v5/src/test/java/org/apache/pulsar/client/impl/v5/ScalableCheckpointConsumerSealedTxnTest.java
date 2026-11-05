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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import com.google.common.util.concurrent.MoreExecutors;
import io.netty.util.Timeout;
import io.netty.util.Timer;
import io.netty.util.TimerTask;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException.AlreadyClosedException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.TopicMessageId;
import org.apache.pulsar.client.api.v5.Checkpoint;
import org.apache.pulsar.client.api.v5.CheckpointConsumer;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.apache.pulsar.client.impl.BatchMessageIdImpl;
import org.apache.pulsar.client.impl.ChunkMessageIdImpl;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.conf.ReaderConfigurationData;
import org.apache.pulsar.client.util.ExecutorProvider;
import org.apache.pulsar.common.api.proto.ScalableTopicDAG;
import org.apache.pulsar.common.api.proto.SegmentState;
import org.apache.pulsar.common.naming.TopicName;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/** A sealed segment may still have transactional messages behind its current read-committed frontier. */
public class ScalableCheckpointConsumerSealedTxnTest {

    private static final TopicName TOPIC = TopicName.get("topic://public/default/checkpoint-sealed-txn");

    @Test
    public void splitWaitsForCommittedPlainAndBatchTailBeforeReadingChildren() throws Exception {
        try (Fixture fixture = new Fixture(layout(0, false))) {
            ControlledReader parent = fixture.latest(0);
            fixture.update(layout(1, true));
            fixture.assertChildrenNotReading();
            assertThat(parent.closeCalls).as("an OPEN transaction is not an exhausted sealed parent").isZero();
            assertThat(fixture.timer.pending()).hasSize(1);
            fixture.timer.fireNext();
            assertThat(parent.closeCalls).isZero();
            fixture.assertChildrenNotReading();

            List<Message<String>> tail = transactionTail();
            parent.publish(tail);
            fixture.expectParentMessages(tail);
            fixture.assertChildrenNotReading();
            assertThat(parent.closeCalls).isZero();

            fixture.latest(1).enqueue(List.of(message("child-1", new MessageIdImpl(600, 0, -1))));
            fixture.latest(2).enqueue(List.of(message("child-2", new MessageIdImpl(601, 0, -1))));
            parent.endOfTopic = true;
            fixture.timer.fireNext();
            assertThat(parent.closed).isTrue();
            assertThat(List.of(fixture.receiveValue(), fixture.receiveValue()))
                    .containsExactlyInAnyOrder("child-1", "child-2");
        }
    }

    @Test
    public void initiallySealedParentKeepsItsReadPendingUntilItsTransactionResolves() throws Exception {
        try (Fixture fixture = new Fixture(layout(1, true))) {
            ControlledReader parent = fixture.latest(0);
            assertThat(parent.readCalls).as("the sealed read path must wait beyond an empty visible frontier")
                    .isEqualTo(1);
            assertThat(parent.closeCalls).isZero();
            fixture.assertChildrenNotReading();

            List<Message<String>> tail = transactionTail();
            parent.publish(tail);
            fixture.expectParentMessages(tail);
            assertThat(parent.closeCalls).isZero();
            fixture.assertChildrenNotReading();
            parent.endOfTopic = true;
            fixture.timer.fireNext();
            assertThat(fixture.latest(1).readCalls).isEqualTo(1);
            assertThat(fixture.latest(2).readCalls).isEqualTo(1);
        }
    }

    @Test
    public void endNotificationCannotDiscardBufferedBatchMembers() throws Exception {
        try (Fixture fixture = new Fixture(layout(0, false))) {
            ControlledReader parent = fixture.latest(0);
            List<Message<String>> tail = transactionTail();
            parent.enqueue(tail);
            parent.endOfTopic = true;
            fixture.update(layout(1, true));
            assertThat(parent.closeCalls).as("EOS does not discard the reader's buffered messages").isZero();
            fixture.assertChildrenNotReading();
            fixture.latest(1).enqueue(List.of(message("child", new MessageIdImpl(600, 0, -1))));

            // The pending wire read completes after the SEALED probe has seen the nonempty cache.
            parent.releasePending();
            fixture.expectParentMessages(tail);
            assertThat(fixture.receiveValue()).isEqualTo("child");
            assertThat(parent.closed).isTrue();
        }
    }

    @Test
    public void staleAvailabilityCannotAffectChildrenAfterTheirParentIsPruned() throws Exception {
        try (Fixture fixture = new Fixture(layout(0, false))) {
            ControlledReader old = fixture.latest(0);
            CompletableFuture<Boolean> oldAvailability = new CompletableFuture<>();
            old.availability.add(oldAvailability);
            fixture.update(layout(1, true));
            assertThat(oldAvailability).isNotDone();

            fixture.update(childrenOnlyLayout(2));
            int oldCloseCalls = old.closeCalls;
            old.endOfTopic = true;
            oldAvailability.complete(false);

            assertThat(old.closeCalls).as("a retired callback must have no further close effect")
                    .isEqualTo(oldCloseCalls);
            assertThat(fixture.latest(1).closeCalls).isZero();
            assertThat(fixture.latest(2).closeCalls).isZero();
            fixture.latest(1).publish(List.of(message("child-after-prune", new MessageIdImpl(700, 1, -1))));
            assertThat(fixture.receiveValue()).isEqualTo("child-after-prune");
        }
    }

    @Test
    public void lateMessageFromRevokedReaderCannotEnterTheCurrentConsumerQueue() throws Exception {
        try (Fixture fixture = new Fixture(layout(0, false))) {
            ControlledReader old = fixture.latest(0);
            old.completePendingOnClose = false;
            fixture.update(childrenOnlyLayout(1));
            var applicationReceive = fixture.consumer.async().receive();

            Message<String> stale = message("revoked", new MessageIdImpl(509, 28, -1));
            old.publish(List.of(stale));
            assertThat(applicationReceive).as("late data from a revoked reader must not be delivered").isNotDone();
            verify(stale).release();

            fixture.latest(1).publish(List.of(message("current-child", new MessageIdImpl(700, 0, -1))));
            assertThat(applicationReceive).isDone();
            assertThat(applicationReceive.join().value()).isEqualTo("current-child");
        }
    }

    @Test
    public void consumerCloseCancelsTheSealedProbeAndFencesAnAlreadyQueuedTimerCallback() throws Exception {
        try (Fixture fixture = new Fixture(layout(0, false))) {
            ControlledReader parent = fixture.latest(0);
            fixture.update(layout(1, true));
            assertThat(fixture.timer.pending()).hasSize(1);
            ControlledTimeout pending = fixture.timer.pending().get(0);
            fixture.consumer.close();
            assertThat(pending.isCancelled()).isTrue();
            int availabilityCalls = parent.availabilityCalls;
            int closeCalls = parent.closeCalls;
            pending.fireDespiteCancellation();
            assertThat(parent.availabilityCalls).isEqualTo(availabilityCalls);
            assertThat(parent.closeCalls).isEqualTo(closeCalls);
            assertThat(fixture.timer.pending()).isEmpty();
            fixture.assertChildrenNotReading();
        }
    }

    @Test
    public void consumerCloseFencesAnAvailabilityRequestStillInFlight() throws Exception {
        try (Fixture fixture = new Fixture(layout(0, false))) {
            ControlledReader parent = fixture.latest(0);
            CompletableFuture<Boolean> availability = new CompletableFuture<>();
            parent.availability.add(availability);
            fixture.update(layout(1, true));
            fixture.consumer.close();
            int closeCalls = parent.closeCalls;
            parent.endOfTopic = true;
            availability.complete(false);
            assertThat(parent.closeCalls).isEqualTo(closeCalls);
            assertThat(fixture.timer.pending()).isEmpty();
            fixture.assertChildrenNotReading();
        }
    }

    @Test
    public void anExternallyClosedReaderDoesNotProveItsOpenTransactionWasDrained() throws Exception {
        try (Fixture fixture = new Fixture(layout(0, false))) {
            ControlledReader parent = fixture.latest(0);
            fixture.update(layout(1, true));
            parent.reader.closeAsync().join();
            fixture.assertChildrenNotReading();
            fixture.timer.fireNext();
            fixture.assertChildrenNotReading();
            assertThat(fixture.timer.pending()).isEmpty();
        }
    }

    @Test
    public void repeatedMetadataDoesNotMultiplyProbesAndReliableEmptyEosReleasesChildren() throws Exception {
        try (Fixture fixture = new Fixture(layout(0, false))) {
            fixture.update(layout(1, true));
            fixture.update(layout(2, true));
            fixture.update(layout(3, true));
            assertThat(fixture.timer.pending()).hasSize(1);
            fixture.assertChildrenNotReading();

            fixture.latest(0).endOfTopic = true;
            fixture.timer.fireNext();
            assertThat(fixture.latest(0).closed).isTrue();
            assertThat(fixture.latest(1).readCalls).isEqualTo(1);
            assertThat(fixture.latest(2).readCalls).isEqualTo(1);
            assertThat(fixture.timer.pending()).isEmpty();
            fixture.update(layout(4, true));
            assertThat(fixture.readers.get(0L)).hasSize(1);
            assertThat(fixture.latest(1).readCalls).isEqualTo(1);
        }
    }

    @Test
    public void alreadySealedEmptyParentWithReliableEosNeedsNoPendingRead() throws Exception {
        try (Fixture fixture = new Fixture(layout(1, true), (segment, reader) -> {
            if (segment == 0) {
                reader.endOfTopic = true;
            }
        })) {
            assertThat(fixture.latest(0).readCalls).isZero();
            assertThat(fixture.latest(0).closed).isTrue();
            assertThat(fixture.latest(1).readCalls).isEqualTo(1);
            assertThat(fixture.latest(2).readCalls).isEqualTo(1);
            assertThat(fixture.timer.pending()).isEmpty();
        }
    }

    @DataProvider
    public Object[][] sealedReadPaths() {
        return new Object[][]{{false}, {true}};
    }

    @Test(dataProvider = "sealedReadPaths")
    public void anAvailabilityResultStartedBeforeEosCannotDiscardTheNowBufferedTail(boolean initiallySealed)
            throws Exception {
        CompletableFuture<Boolean> staleEmpty = new CompletableFuture<>();
        try (Fixture fixture = new Fixture(layout(initiallySealed ? 1 : 0, initiallySealed), (segment, reader) -> {
            if (initiallySealed && segment == 0) {
                reader.availability.add(staleEmpty);
            }
        })) {
            ControlledReader parent = fixture.latest(0);
            if (!initiallySealed) {
                parent.availability.add(staleEmpty);
                fixture.update(layout(1, true));
            }
            assertThat(staleEmpty).isNotDone();
            fixture.assertChildrenNotReading();
            List<Message<String>> tail = transactionTail();
            // The old query saw the OPEN transaction's empty frontier. Its false response is
            // delayed until after COMMIT, the tail entering the reader's cache and the EOS signal.
            parent.enqueue(tail);
            parent.endOfTopic = true;
            fixture.latest(1).enqueue(List.of(message("child-after-tail", new MessageIdImpl(600, 0, -1))));
            staleEmpty.complete(false);
            parent.releasePending();
            fixture.expectParentMessages(tail);
            assertThat(fixture.receiveValue()).isEqualTo("child-after-tail");
            assertThat(parent.closed).isTrue();
        }
    }

    @Test
    public void partialBatchCheckpointDropsOnlyTheProcessedPrefix() throws Exception {
        MessageId boundary = new BatchMessageIdImpl(509, 30, -1, 1, 4, null);
        try (Fixture fixture = new Fixture(layout(0, false), checkpoint(boundary), (segment, reader) -> { })) {
            fixture.assertInclusiveStart(boundary);
            List<Message<String>> batch = batchMessages(30);
            Message<String> later = message("next-entry", new MessageIdImpl(509, 31, -1));
            fixture.latest(0).publish(List.of(batch.get(0), batch.get(1), batch.get(2), batch.get(3), later));
            assertThat(fixture.position(0)).as("prefetch and internal drops cannot advance application state")
                    .isEqualTo(boundary);
            fixture.expectParentMessages(List.of(batch.get(2)));
            assertThat(fixture.position(0)).isEqualTo(batch.get(2).getMessageId());
            fixture.expectParentMessages(List.of(batch.get(3), later));
            verify(batch.get(0)).release();
            verify(batch.get(1)).release();
            assertThat(fixture.consumer.async().receive()).as("the processed batch prefix must not be replayed")
                    .isNotDone();
        }
    }

    @Test
    public void plainCheckpointAtAnActualBatchEntryPreservesTheLegacySiblingStart() throws Exception {
        MessageId boundary = new MessageIdImpl(509, 30, -1);
        try (Fixture fixture = new Fixture(layout(0, false), checkpoint(boundary), (segment, reader) -> { })) {
            fixture.assertInclusiveStart(boundary);
            List<Message<String>> batch = batchMessages(30);
            fixture.latest(0).publish(batch);
            assertThat(fixture.position(0)).isEqualTo(boundary);
            // The v4 batch filter compares actual indexes to the saved index: none are <= -1.
            // An index-less checkpoint must not acquire a new "whole batch processed" meaning.
            fixture.expectParentMessages(batch);
            assertThat(fixture.position(0)).isEqualTo(batch.get(3).getMessageId());
        }
    }

    @DataProvider
    public Object[][] alreadyProcessedEnds() {
        return new Object[][]{{false}, {true}};
    }

    @Test(dataProvider = "alreadyProcessedEnds")
    public void inclusiveReadOfAnAlreadyProcessedEndLetsASealedParentFinish(boolean batched) throws Exception {
        MessageId boundary = batched ? new BatchMessageIdImpl(509, 30, -1, 3, 4, null)
                : new MessageIdImpl(509, 30, -1);
        Message<String> processed = message("already-processed", boundary);
        try (Fixture fixture = new Fixture(layout(1, true), checkpoint(boundary), (segment, reader) -> {
            if (segment == 0) {
                reader.enqueue(List.of(processed));
                // The real reader ACKs its inclusive boundary before reporting the terminated topic's EOS.
                reader.onRead = ignored -> reader.endOfTopic = true;
            } else if (segment == 1) {
                reader.enqueue(List.of(message("child-after-known-end", new MessageIdImpl(600, 0, -1))));
            }
        })) {
            fixture.assertInclusiveStart(boundary);
            assertThat(fixture.latest(0).readCalls).as("the saved end must pass through the underlying reader")
                    .isEqualTo(1);
            assertThat(fixture.receiveValue()).isEqualTo("child-after-known-end");
            assertThat(fixture.position(0)).as("internal boundary reads are not new application receives")
                    .isEqualTo(boundary);
            assertThat(fixture.latest(0).closed).isTrue();
            verify(processed).release();
        }
    }

    @Test
    public void reconnectCannotReofferQueuedMessagesOrAdvanceTheApplicationCheckpoint() throws Exception {
        try (Fixture fixture = new Fixture(layout(0, false))) {
            ControlledReader reader = fixture.latest(0);
            MessageId initial = fixture.position(0);
            List<Message<String>> queued = transactionTail();
            reader.publish(queued);
            assertThat(fixture.position(0)).as("queued messages have not yet crossed the application boundary")
                    .isEqualTo(initial);

            Message<String> replayedPlain = message("replayed-plain", new MessageIdImpl(509, 29, -1));
            Message<String> replayedBatch = message("replayed-batch", new BatchMessageIdImpl(509, 30, -1, 3, 4, null));
            Message<String> newMessage = message("after-reconnect", new MessageIdImpl(509, 31, -1));
            reader.publish(List.of(replayedPlain, replayedBatch, newMessage));
            assertThat(fixture.position(0)).isEqualTo(initial);
            fixture.expectParentMessages(List.of(queued.get(0)));
            assertThat(fixture.position(0)).isEqualTo(queued.get(0).getMessageId());
            fixture.expectParentMessages(queued.subList(1, queued.size()));
            fixture.expectParentMessages(List.of(newMessage));
            verify(replayedPlain).release();
            verify(replayedBatch).release();
            assertThat(fixture.consumer.async().receive()).as("wire replay must not enter the V5 queue twice")
                    .isNotDone();
        }
    }

    @Test
    public void fullReceiveQueueKeepsApplicationStateBehindPrefetchAndReconnectReplay() throws Exception {
        try (Fixture fixture = new Fixture(layout(0, false))) {
            ControlledReader reader = fixture.latest(0);
            MessageId initial = fixture.position(0);
            List<Message<String>> queued = new ArrayList<>();
            // Fill the default receiver queue without recursive chains of immediately completed wire reads.
            for (int entry = 0; entry < 1000; entry++) {
                Message<String> message = message("queued-" + entry, new MessageIdImpl(509, entry, -1));
                queued.add(message);
                reader.publish(List.of(message));
            }
            assertThat(reader.readCalls).as("the full queue must pause the per-reader loop").isEqualTo(1000);
            assertThat(fixture.position(0)).isEqualTo(initial);
            Message<String> replayed = message("replayed-full-boundary", new MessageIdImpl(509, 999, -1));
            Message<String> next = message("after-full-queue", new MessageIdImpl(509, 1000, -1));
            reader.publish(List.of(replayed, next));
            assertThat(fixture.position(0)).as("neither queue admission nor capacity waiting is application receipt")
                    .isEqualTo(initial);
            fixture.expectParentMessages(List.of(queued.get(0)));
            assertThat(fixture.position(0)).isEqualTo(queued.get(0).getMessageId());
            fixture.expectParentMessages(queued.subList(1, queued.size()));
            fixture.expectParentMessages(List.of(next));
            verify(replayed).release();
            assertThat(fixture.consumer.async().receive()).isNotDone();
        }
    }

    @Test
    public void earliestHasNoRealBoundaryToWaitForOrDrop() throws Exception {
        try (Fixture fixture = new Fixture(layout(0, false))) {
            assertThat(fixture.latest(0).readCalls).isEqualTo(1);
            Message<String> first = message("first-ever", new MessageIdImpl(0, 0, -1));
            fixture.latest(0).publish(List.of(first));
            fixture.expectParentMessages(List.of(first));
        }
    }

    @Test
    public void emptyLatestLookupDoesNotRequireAnImpossibleBoundaryMessage() throws Exception {
        MessageId empty = new MessageIdImpl(701, -1, -1);
        try (Fixture fixture = new Fixture(layout(0, false), CheckpointV5.LATEST,
                (segment, reader) -> reader.lookupPosition = empty)) {
            assertThat(fixture.readers.get(0L)).hasSize(2);
            assertThat(fixture.readers.get(0L).get(0).closed).as("the dispatchable-position lookup is closed")
                    .isTrue();
            assertThat(fixture.configurations.get(0L).get(0).getStartMessageId()).isEqualTo(MessageId.latest);
            assertThat(fixture.configurations.get(0L).get(0).isResetIncludeHead()).isFalse();
            assertThat(fixture.latest(0).readCalls).isEqualTo(1);
            Message<String> first = message("created-after-empty-latest", new MessageIdImpl(701, 0, -1));
            fixture.latest(0).publish(List.of(first));
            fixture.expectParentMessages(List.of(first));
        }
    }

    @Test
    public void latestKeepsTheLookupBatchEndAsItsOriginalExclusiveBoundary() throws Exception {
        MessageId boundary = new BatchMessageIdImpl(509, 30, -1, 3, 4, null);
        try (Fixture fixture = new Fixture(layout(0, false), CheckpointV5.LATEST,
                (segment, reader) -> reader.lookupPosition = boundary)) {
            assertThat(fixture.readers.get(0L)).hasSize(2);
            assertThat(fixture.configurations.get(0L).get(0).isResetIncludeHead()).isFalse();
            fixture.assertInclusiveStart(boundary);
            Message<String> processed = message("latest-boundary", boundary);
            Message<String> committedTail = message("committed-after-lookup", new MessageIdImpl(509, 31, -1));
            fixture.latest(0).publish(List.of(processed, committedTail));
            assertThat(fixture.position(0)).isEqualTo(boundary);
            fixture.expectParentMessages(List.of(committedTail));
            verify(processed).release();
        }
    }

    @Test
    public void fullChunkBoundaryIsKeptWhenTheUnderlyingReaderStartsAtItsFirstChunk() throws Exception {
        MessageIdImpl firstChunk = new MessageIdImpl(509, 28, -1);
        MessageId boundary = new ChunkMessageIdImpl(firstChunk, new MessageIdImpl(509, 30, -1));
        try (Fixture fixture = new Fixture(layout(0, false), checkpoint(boundary),
                (segment, reader) -> reader.internalStartOverride = firstChunk)) {
            fixture.assertInclusiveStart(boundary);
            Message<String> processed = message("reassembled-old-chunk", boundary);
            Message<String> following = message("reassembled-next-chunk",
                    new ChunkMessageIdImpl(new MessageIdImpl(509, 31, -1), new MessageIdImpl(509, 33, -1)));
            fixture.latest(0).publish(List.of(processed, following));
            assertThat(fixture.position(0).toByteArray()).isEqualTo(boundary.toByteArray());
            fixture.expectParentMessages(List.of(following));
            assertThat(fixture.position(0).toByteArray()).isEqualTo(following.getMessageId().toByteArray());
            verify(processed).release();
        }
    }

    @Test
    public void metadataRevocationWhileReadingTheMessageIdCannotOfferStaleData() throws Exception {
        try (Fixture fixture = new Fixture(layout(0, false))) {
            ControlledReader parent = fixture.latest(0);
            MessageId position = new MessageIdImpl(509, 28, -1);
            AtomicBoolean revoked = new AtomicBoolean();
            var receive = fixture.consumer.async().receive();
            Message<String> stale = message("revoked-during-position-read", position);
            // Both the old wrapper path and the fixed path read the public message id before enqueueing.
            // Revoke at that boundary, after the read completed but before it can enter the shared queue.
            when(stale.getMessageId()).thenAnswer(ignored -> {
                if (revoked.compareAndSet(false, true)) {
                    fixture.update(childrenOnlyLayout(1));
                }
                return position;
            });
            parent.publish(List.of(stale));
            assertThat(revoked).isTrue();
            assertThat(receive).as("metadata revoked this reader before its new position could be offered")
                    .isNotDone();
            verify(stale).release();
            fixture.latest(1).publish(List.of(message("current-child", new MessageIdImpl(600, 0, -1))));
            assertThat(receive).isDone();
            assertThat(receive.join().value()).isEqualTo("current-child");
        }
    }

    private static Checkpoint checkpoint(MessageId boundary) throws Exception {
        return CheckpointV5.fromByteArray(new CheckpointV5(Map.of(0L, boundary)).toByteArray());
    }

    private static List<Message<String>> batchMessages(long entry) {
        List<Message<String>> batch = new ArrayList<>();
        for (int index = 0; index < 4; index++) {
            batch.add(message("batch-" + index, new BatchMessageIdImpl(509, entry, -1, index, 4, null)));
        }
        return batch;
    }

    private static List<Message<String>> transactionTail() {
        List<Message<String>> messages = new ArrayList<>();
        messages.add(message("plain-0", new MessageIdImpl(509, 28, -1)));
        messages.add(message("plain-1", new MessageIdImpl(509, 29, -1)));
        for (int index = 0; index < 4; index++) {
            messages.add(message("batch-" + index, new BatchMessageIdImpl(509, 30, -1, index, 4, null)));
        }
        return messages;
    }

    @SuppressWarnings("unchecked")
    private static Message<String> message(String value, MessageId id) {
        Message<String> message = mock(Message.class);
        when(message.getMessageId()).thenReturn(id);
        when(message.getValue()).thenReturn(value);
        when(message.getData()).thenReturn(value.getBytes(StandardCharsets.UTF_8));
        return message;
    }

    private static ClientSegmentLayout childrenOnlyLayout(long epoch) {
        ScalableTopicDAG dag = new ScalableTopicDAG().setEpoch(epoch);
        dag.addSegment().setSegmentId(1).setHashStart(0).setHashEnd(0x7fff)
                .setState(SegmentState.ACTIVE).setCreatedAtEpoch(1).addParentId(0);
        dag.addSegment().setSegmentId(2).setHashStart(0x8000).setHashEnd(0xffff)
                .setState(SegmentState.ACTIVE).setCreatedAtEpoch(1).addParentId(0);
        return ClientSegmentLayout.fromProto(dag, TOPIC);
    }

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

    private static final class Fixture implements AutoCloseable {
        private final ExecutorService executor = MoreExecutors.newDirectExecutorService();
        private final ControlledTimer timer = new ControlledTimer();
        private final Map<Long, List<ControlledReader>> readers = new HashMap<>();
        private final Map<Long, List<ReaderConfigurationData<String>>> configurations = new HashMap<>();
        private final CheckpointConsumer<String> consumer;
        private DagWatchClient.LayoutChangeListener listener;
        private ClientSegmentLayout current;

        Fixture(ClientSegmentLayout initial) {
            this(initial, (segment, reader) -> { });
        }

        Fixture(ClientSegmentLayout initial, BiConsumer<Long, ControlledReader> configure) {
            this(initial, CheckpointV5.EARLIEST, configure);
        }

        Fixture(ClientSegmentLayout initial, Checkpoint start, BiConsumer<Long, ControlledReader> configure) {
            PulsarClientImpl v4 = mock(PulsarClientImpl.class);
            PulsarClientV5 client = mock(PulsarClientV5.class);
            ExecutorProvider provider = mock(ExecutorProvider.class);
            when(client.v4Client()).thenReturn(v4);
            when(v4.externalExecutorProvider()).thenReturn(provider);
            when(provider.getExecutor()).thenReturn(executor);
            when(v4.getInternalExecutorService()).thenReturn(executor);
            when(v4.timer()).thenReturn(timer.timer);
            when(v4.<String>createSegmentReaderAsync(any(), any())).thenAnswer(invocation -> {
                ReaderConfigurationData<String> configuration = invocation.getArgument(0);
                String topic = configuration.getTopicName();
                long segment = Long.parseLong(topic.substring(topic.lastIndexOf('-') + 1));
                configurations.computeIfAbsent(segment, ignored -> new ArrayList<>()).add(configuration.clone());
                ControlledReader reader = new ControlledReader();
                configure.accept(segment, reader);
                when(reader.reader.getLastMessageIdsAsync()).thenAnswer(ignored -> CompletableFuture.completedFuture(
                        List.of(TopicMessageId.create(topic, reader.lookupPosition))));
                // ConsumerImpl can rewind an inclusive chunk start internally for complete reassembly.
                if (reader.internalStartOverride != null) {
                    configuration.setStartMessageId(reader.internalStartOverride);
                }
                readers.computeIfAbsent(segment, ignored -> new ArrayList<>()).add(reader);
                return CompletableFuture.completedFuture(reader.reader);
            });
            DagWatchClient watch = mock(DagWatchClient.class);
            when(watch.topicName()).thenReturn(TOPIC);
            doAnswer(invocation -> {
                listener = invocation.getArgument(0);
                return null;
            }).when(watch).setListener(any());
            current = initial;
            consumer = ScalableCheckpointConsumer.createUnmanagedAsync(client, Schema.string(), watch, initial,
                    start, "held-transaction-tail").join();
        }

        ControlledReader latest(long segment) {
            List<ControlledReader> attached = readers.get(segment);
            return attached.get(attached.size() - 1);
        }

        MessageId position(long segment) throws Exception {
            // Inspect the returned serialized checkpoint artifact, never the consumer's private state.
            CheckpointV5 checkpoint = (CheckpointV5) CheckpointV5.fromByteArray(consumer.checkpoint().toByteArray());
            return checkpoint.segmentPositions().get(segment);
        }

        void assertInclusiveStart(MessageId boundary) {
            List<ReaderConfigurationData<String>> configurations = this.configurations.get(0L);
            ReaderConfigurationData<String> actual = configurations.get(configurations.size() - 1);
            assertThat(actual.isResetIncludeHead()).as("the real reader must observe and ACK its exclusive boundary")
                    .isTrue();
            assertThat(actual.getStartMessageId().toByteArray()).isEqualTo(boundary.toByteArray());
        }

        void update(ClientSegmentLayout next) {
            ClientSegmentLayout previous = current;
            current = next;
            listener.onLayoutChange(next, previous);
        }

        void assertChildrenNotReading() {
            assertThat(latest(1).readCalls).as("child 1 must wait for its parent's complete tail").isZero();
            assertThat(latest(2).readCalls).as("child 2 must wait for its parent's complete tail").isZero();
        }

        void expectParentMessages(List<Message<String>> messages) {
            for (Message<String> expected : messages) {
                var received = consumer.async().receive();
                assertThat(received).as("the committed parent message must already be available").isDone();
                var actual = received.join();
                assertThat(actual.value()).isEqualTo(expected.getValue());
                MessageIdV5 id = (MessageIdV5) actual.id();
                assertThat(id.segmentId()).isZero();
                assertThat(id.v4MessageId()).isEqualTo(expected.getMessageId());
            }
        }

        String receiveValue() {
            var received = consumer.async().receive();
            assertThat(received).isDone();
            return received.join().value();
        }

        @Override
        public void close() throws Exception {
            consumer.close();
            executor.shutdown();
        }
    }

    private static final class ControlledReader {
        private final Reader<String> reader;
        private final ArrayDeque<Message<String>> buffered = new ArrayDeque<>();
        private final ArrayDeque<CompletableFuture<Boolean>> availability = new ArrayDeque<>();
        private CompletableFuture<Message<String>> pending;
        private boolean endOfTopic;
        private boolean closed;
        private boolean completePendingOnClose = true;
        private MessageId lookupPosition = MessageId.earliest;
        private MessageId internalStartOverride;
        private Consumer<Message<String>> onRead = ignored -> { };
        private int readCalls;
        private int availabilityCalls;
        private int closeCalls;

        @SuppressWarnings("unchecked")
        ControlledReader() {
            reader = mock(Reader.class);
            when(reader.hasReachedEndOfTopic()).thenAnswer(invocation -> endOfTopic);
            when(reader.hasMessageAvailableAsync()).thenAnswer(invocation -> {
                availabilityCalls++;
                if (!availability.isEmpty()) {
                    return availability.removeFirst();
                }
                return closed ? CompletableFuture.failedFuture(new AlreadyClosedException("Reader closed"))
                        : CompletableFuture.completedFuture(!buffered.isEmpty());
            });
            when(reader.readNextAsync()).thenAnswer(invocation -> {
                readCalls++;
                if (closed) {
                    return CompletableFuture.failedFuture(new AlreadyClosedException("Reader closed"));
                }
                if (!buffered.isEmpty()) {
                    Message<String> message = buffered.removeFirst();
                    onRead.accept(message);
                    return CompletableFuture.completedFuture(message);
                }
                assertThat(pending == null || pending.isDone()).as("only one pending read per reader").isTrue();
                pending = new CompletableFuture<>();
                return pending;
            });
            when(reader.closeAsync()).thenAnswer(invocation -> {
                closeCalls++;
                closed = true;
                if (completePendingOnClose && pending != null) {
                    pending.completeExceptionally(new AlreadyClosedException("Reader closed"));
                }
                return CompletableFuture.completedFuture(null);
            });
        }

        void enqueue(List<Message<String>> messages) {
            buffered.addAll(messages);
        }

        void publish(List<Message<String>> messages) {
            enqueue(messages);
            releasePending();
        }

        void releasePending() {
            if (pending != null && !pending.isDone() && !buffered.isEmpty()) {
                CompletableFuture<Message<String>> read = pending;
                pending = null;
                Message<String> message = buffered.removeFirst();
                onRead.accept(message);
                read.complete(message);
            }
        }
    }

    private static final class ControlledTimer {
        private final Timer timer = mock(Timer.class);
        private final List<ControlledTimeout> timeouts = new ArrayList<>();

        ControlledTimer() {
            when(timer.newTimeout(any(), anyLong(), any())).thenAnswer(invocation -> {
                ControlledTimeout timeout = new ControlledTimeout(timer, invocation.getArgument(0));
                timeouts.add(timeout);
                return timeout;
            });
        }

        List<ControlledTimeout> pending() {
            return timeouts.stream().filter(timeout -> !timeout.isExpired() && !timeout.isCancelled()).toList();
        }

        void fireNext() throws Exception {
            assertThat(pending()).as("a sealed reader waiting on a transaction must be probed again").isNotEmpty();
            pending().get(0).fireDespiteCancellation();
        }
    }

    private static final class ControlledTimeout implements Timeout {
        private final Timer timer;
        private final TimerTask task;
        private boolean expired;
        private boolean cancelled;

        ControlledTimeout(Timer timer, TimerTask task) {
            this.timer = timer;
            this.task = task;
        }

        void fireDespiteCancellation() throws Exception {
            expired = true;
            task.run(this);
        }

        @Override
        public Timer timer() {
            return timer;
        }

        @Override
        public TimerTask task() {
            return task;
        }

        @Override
        public boolean isExpired() {
            return expired;
        }

        @Override
        public boolean isCancelled() {
            return cancelled;
        }

        @Override
        public boolean cancel() {
            if (expired || cancelled) {
                return false;
            }
            cancelled = true;
            return true;
        }
    }
}
