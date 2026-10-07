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
package org.apache.pulsar.broker.service.persistent;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.pulsar.common.protocol.Commands.serializeMetadataAndPayload;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.EventLoopGroup;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntriesCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.AbstractReplicator.State;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.persistent.PersistentReplicator.InFlightTask;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.client.impl.ProducerImpl;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.SendCallback;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker-replication")
public class GeoShadowReplicatorUnsentEntryCleanupTest {

    @DataProvider
    public Object[][] failureKinds() {
        return new Object[][] {{false}, {true}};
    }

    @Test(dataProvider = "failureKinds")
    public void testGeoSchemaPreparationFailureReleasesUnsentSuffix(boolean error) throws Exception {
        ReplicatorFixture fixture = newReplicatorFixture();
        SchemaFailingGeoReplicator replicator = new SchemaFailingGeoReplicator(fixture, error);
        ProducerImpl producer = mock(ProducerImpl.class);
        replicator.startForTest(producer);

        List<EntryFixture> entries = entries();
        try {
            ReadRequest request = requestRead(fixture, replicator);
            request.callback.readEntriesComplete(entryList(entries), request.context);

            assertThat(replicator.getState()).isEqualTo(State.Started);
            assertUnsentSuffixReleased(entries, request.inFlightTask());
            SendCallback callback = capturedCallback(producer);
            verify(entries.get(0).entry, never()).release();

            callback.sendComplete(null, null);

            assertSubmittedEntryCompleted(entries.get(0));
        } finally {
            entries.forEach(EntryFixture::releaseBuffer);
        }
    }

    @Test(dataProvider = "failureKinds")
    public void testShadowPreSendFailureReleasesUnsentSuffix(boolean error) throws Exception {
        ReplicatorFixture fixture = newReplicatorFixture();
        TestShadowReplicator replicator = new TestShadowReplicator(fixture);
        ProducerImpl producer = mock(ProducerImpl.class);
        replicator.startForTest(producer);

        List<EntryFixture> entries = entries();
        when(entries.get(1).entry.getLedgerId()).thenThrow(error
                ? new AssertionError("pre-send metadata failure")
                : new IllegalStateException("pre-send metadata failure"));
        try {
            ReadRequest request = requestRead(fixture, replicator);
            request.callback.readEntriesComplete(entryList(entries), request.context);

            assertThat(replicator.getState()).isEqualTo(State.Started);
            assertUnsentSuffixReleased(entries, request.inFlightTask());
            SendCallback callback = capturedCallback(producer);
            verify(entries.get(0).entry, never()).release();

            callback.sendComplete(null, null);

            assertSubmittedEntryCompleted(entries.get(0));
        } finally {
            entries.forEach(EntryFixture::releaseBuffer);
        }
    }

    @Test
    public void testCompletedExceptionalSchemaSchedulesRetryWithoutImmediateRead() throws Exception {
        ReplicatorFixture fixture = newReplicatorFixture();
        ExceptionallyCompletedSchemaGeoReplicator replicator = new ExceptionallyCompletedSchemaGeoReplicator(fixture);
        ProducerImpl producer = mock(ProducerImpl.class);
        replicator.startForTest(producer);
        EntryFixture entry = entry(0);
        try {
            ReadRequest request = requestRead(fixture, replicator);
            request.callback.readEntriesComplete(List.of(entry.entry), request.context);

            assertThat(replicator.schemaRequests.get()).isOne();
            assertThat(replicator.retryDelays.get()).isOne();
            verify(fixture.cursor, times(1)).asyncReadEntriesOrWait(anyInt(), anyLong(), any(), any(), any());
            verify(fixture.executor).schedule(any(Runnable.class), anyLong(), eq(TimeUnit.MILLISECONDS));
            verify(producer, never()).sendAsync(any(), any());
            verify(entry.entry, times(1)).release();
            assertThat(request.inFlightTask().isDone()).isTrue();
        } finally {
            entry.releaseBuffer();
        }
    }

    private static ReadRequest requestRead(ReplicatorFixture fixture, PersistentReplicator replicator) {
        List<ReadRequest> requests = new ArrayList<>();
        doAnswer(invocation -> {
            requests.add(new ReadRequest(invocation.getArgument(2), invocation.getArgument(3)));
            return null;
        }).when(fixture.cursor).asyncReadEntriesOrWait(anyInt(), anyLong(), any(), any(), any());

        replicator.readMoreEntries();

        assertThat(requests).hasSize(1);
        return requests.get(0);
    }

    private static SendCallback capturedCallback(ProducerImpl producer) {
        ArgumentCaptor<SendCallback> callbackCaptor = ArgumentCaptor.forClass(SendCallback.class);
        verify(producer).sendAsync(any(), callbackCaptor.capture());
        return callbackCaptor.getValue();
    }

    private static void assertUnsentSuffixReleased(List<EntryFixture> entries, InFlightTask task) {
        verify(entries.get(1).entry, times(1)).release();
        verify(entries.get(2).entry, times(1)).release();
        assertThat(task.getCompletedEntries()).isEqualTo(2);
        assertThat(task.isSubmissionComplete()).isTrue();
        assertThat(task.isDone()).isFalse();
    }

    private static void assertSubmittedEntryCompleted(EntryFixture entry) {
        verify(entry.entry, times(1)).release();
        // The final ACK may immediately start another read and recycle the now-complete task.
    }

    private static List<EntryFixture> entries() {
        return List.of(entry(0), entry(1), entry(2));
    }

    private static List<Entry> entryList(List<EntryFixture> entries) {
        return entries.stream().map(entry -> entry.entry).toList();
    }

    private static EntryFixture entry(long entryId) {
        ByteBuf payload = Unpooled.copiedBuffer("message-" + entryId, UTF_8);
        MessageMetadata metadata = new MessageMetadata()
                .setSequenceId(entryId)
                .setProducerName("test-producer")
                .setPublishTime(1);
        ByteBuf headersAndPayload = serializeMetadataAndPayload(Commands.ChecksumType.Crc32c, metadata, payload);
        payload.release();

        Entry entry = mock(Entry.class);
        when(entry.getLedgerId()).thenReturn(1L);
        when(entry.getEntryId()).thenReturn(entryId);
        when(entry.getPosition()).thenReturn(PositionFactory.create(1, entryId));
        when(entry.getLength()).thenReturn(headersAndPayload.readableBytes());
        when(entry.getDataBuffer()).thenReturn(headersAndPayload);
        return new EntryFixture(entry, headersAndPayload);
    }

    @SuppressWarnings("unchecked")
    private static ReplicatorFixture newReplicatorFixture() throws Exception {
        ServiceConfiguration configuration = new ServiceConfiguration();
        configuration.setClusterName("local");
        configuration.setReplicationProducerQueueSize(1000);
        configuration.setDispatcherMaxReadBatchSize(1000);
        configuration.setDispatcherMaxReadSizeBytes(1024 * 1024);

        PulsarService pulsar = mock(PulsarService.class);
        when(pulsar.getConfiguration()).thenReturn(configuration);
        when(pulsar.getConfig()).thenReturn(configuration);
        when(pulsar.getClient()).thenReturn(mock(PulsarClientImpl.class));
        when(pulsar.getAdminClient()).thenReturn(mock(PulsarAdmin.class));

        BrokerService brokerService = mock(BrokerService.class);
        when(brokerService.pulsar()).thenReturn(pulsar);
        when(brokerService.getPulsar()).thenReturn(pulsar);
        EventLoopGroup executor = mock(EventLoopGroup.class);
        when(brokerService.executor()).thenReturn(executor);

        ProducerBuilder<byte[]> producerBuilder = mock(ProducerBuilder.class);
        when(producerBuilder.topic(any())).thenReturn(producerBuilder);
        when(producerBuilder.messageRoutingMode(any(MessageRoutingMode.class))).thenReturn(producerBuilder);
        when(producerBuilder.enableBatching(anyBoolean())).thenReturn(producerBuilder);
        when(producerBuilder.sendTimeout(anyInt(), any(TimeUnit.class))).thenReturn(producerBuilder);
        when(producerBuilder.maxPendingMessages(anyInt())).thenReturn(producerBuilder);
        when(producerBuilder.producerName(any())).thenReturn(producerBuilder);

        PulsarClientImpl replicationClient = mock(PulsarClientImpl.class);
        when(replicationClient.newProducer(any(Schema.class))).thenReturn(producerBuilder);

        PersistentTopic topic = mock(PersistentTopic.class);
        when(topic.getName()).thenReturn("persistent://prop/ns/replicator-cleanup");
        when(topic.getReplicatorPrefix()).thenReturn("pulsar.repl");
        when(topic.getBrokerService()).thenReturn(brokerService);
        when(topic.getMaxReadPosition()).thenReturn(PositionFactory.create(1, 10000));

        ManagedCursor cursor = mock(ManagedCursor.class);
        when(cursor.getName()).thenReturn("pulsar.repl.remote");
        when(cursor.getReadPosition()).thenReturn(PositionFactory.create(1, 1));

        return new ReplicatorFixture(topic, cursor, brokerService, replicationClient, mock(PulsarAdmin.class),
                executor);
    }

    private record ReadRequest(ReadEntriesCallback callback, Object context) {
        private InFlightTask inFlightTask() {
            return (InFlightTask) context;
        }
    }

    private record EntryFixture(Entry entry, ByteBuf data) {
        private void releaseBuffer() {
            data.release(data.refCnt());
        }
    }

    private record ReplicatorFixture(PersistentTopic topic, ManagedCursor cursor, BrokerService brokerService,
                                     PulsarClientImpl replicationClient, PulsarAdmin replicationAdmin,
                                     EventLoopGroup executor) {
    }

    private static final class SchemaFailingGeoReplicator extends GeoPersistentReplicator {
        private final AtomicInteger schemaRequests = new AtomicInteger();
        private final boolean error;

        private SchemaFailingGeoReplicator(ReplicatorFixture fixture, boolean error) throws PulsarServerException {
            super(fixture.topic, fixture.cursor, "local", "remote", fixture.brokerService, fixture.replicationClient,
                    fixture.replicationAdmin);
            this.error = error;
        }

        @Override
        protected void startProducer() {
            // The test installs a mock producer after construction.
        }

        private void startForTest(ProducerImpl producer) {
            this.producer = producer;
            this.state = State.Started;
        }

        @Override
        protected CompletableFuture<SchemaInfo> getSchemaInfo(MessageImpl msg) throws ExecutionException {
            if (schemaRequests.incrementAndGet() == 2) {
                if (error) {
                    throw new AssertionError("schema preparation failed");
                }
                throw new ExecutionException("schema preparation failed", null);
            }
            return CompletableFuture.completedFuture(null);
        }
    }

    private static final class TestShadowReplicator extends ShadowReplicator {
        private TestShadowReplicator(ReplicatorFixture fixture) throws PulsarServerException {
            super("persistent://prop/ns/shadow", fixture.topic, fixture.cursor, fixture.brokerService,
                    fixture.replicationClient, fixture.replicationAdmin);
        }

        @Override
        protected void startProducer() {
            // The test installs a mock producer after construction.
        }

        private void startForTest(ProducerImpl producer) {
            this.producer = producer;
            this.state = State.Started;
        }
    }

    private static final class ExceptionallyCompletedSchemaGeoReplicator extends GeoPersistentReplicator {
        private final AtomicInteger schemaRequests = new AtomicInteger();
        private final AtomicInteger retryDelays = new AtomicInteger();

        private ExceptionallyCompletedSchemaGeoReplicator(ReplicatorFixture fixture) throws PulsarServerException {
            super(fixture.topic, fixture.cursor, "local", "remote", fixture.brokerService, fixture.replicationClient,
                    fixture.replicationAdmin);
        }

        @Override
        protected void startProducer() {
            // The test installs a mock producer after construction.
        }

        private void startForTest(ProducerImpl producer) {
            this.producer = producer;
            this.state = State.Started;
        }

        @Override
        protected CompletableFuture<SchemaInfo> getSchemaInfo(MessageImpl msg) {
            schemaRequests.incrementAndGet();
            return CompletableFuture.failedFuture(new IllegalStateException("schema lookup failed"));
        }

        @Override
        protected long delayReadRetry() {
            retryDelays.incrementAndGet();
            return super.delayReadRetry();
        }
    }
}
