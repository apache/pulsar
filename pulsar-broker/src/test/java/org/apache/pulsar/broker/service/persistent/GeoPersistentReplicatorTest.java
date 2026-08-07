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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Answers.RETURNS_SELF;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.ExecutionError;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.EventLoopGroup;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.persistent.PersistentReplicator.InFlightTask;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.SchemaInfoProvider;
import org.apache.pulsar.client.impl.ProducerImpl;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.protocol.Commands;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker-replication")
public class GeoPersistentReplicatorTest {

    @DataProvider
    public static Object[][] synchronousSchemaLookupFailures() {
        return new Object[][] {
                { new ExecutionException(new RuntimeException("checked schema provider failure")) },
                { new UncheckedExecutionException(new RuntimeException("unchecked schema provider failure")) },
                { new ExecutionError(new AssertionError("schema provider error")) }
        };
    }

    @Test(dataProvider = "synchronousSchemaLookupFailures")
    public void testSchemaInfoSynchronousFailureDoesNotReadUntilScheduledCursorRewind(
            Throwable schemaLookupFailure) throws Exception {
        ThrowingSchemaReplicator replicator = new ThrowingSchemaReplicator(schemaLookupFailure);
        replicator.forceStarted();

        Position firstPosition = PositionFactory.create(1, 2);
        ByteBuf headersAndPayload = newMessageWithSchemaVersion();
        Entry firstEntry = newEntry(firstPosition, headersAndPayload);
        Entry secondEntry = mock(Entry.class);

        List<Entry> entries = List.of(firstEntry, secondEntry);
        InFlightTask inFlightTask = replicator.createOrRecycleInFlightTaskIntoQueue(firstPosition, entries.size());

        try {
            replicator.readEntriesComplete(entries, inFlightTask);

            assertThat(inFlightTask.getCompletedEntries())
                    .as("the failed entry and skipped remaining entries should release their in-flight permits")
                    .isEqualTo(entries.size());
            verify(firstEntry).release();
            verify(secondEntry).release();
            assertThat(headersAndPayload.refCnt())
                    .as("the entry and retained schema lookup buffer should both be released")
                    .isZero();
            verify(replicator.cursor, never()).rewind();
            verify(replicator.cursor, never()).asyncReadEntriesOrWait(
                    anyInt(), anyLong(), any(), any(), any());
            verify(replicator.context.executor, atLeastOnce()).schedule(any(Runnable.class),
                    eq((long) PersistentTopic.MESSAGE_RATE_BACKOFF_MS), eq(TimeUnit.MILLISECONDS));

            Runnable scheduledRewind = replicator.context.scheduledTasks.get(0);
            assertThat(scheduledRewind).isNotNull();

            scheduledRewind.run();
            verify(replicator.cursor).rewind();
            verify(replicator.cursor).asyncReadEntriesOrWait(
                    anyInt(), anyLong(), any(), any(), any());
        } finally {
            while (headersAndPayload.refCnt() > 0) {
                headersAndPayload.release();
            }
        }
    }

    private static Entry newEntry(Position position, ByteBuf headersAndPayload) {
        Entry entry = mock(Entry.class);
        when(entry.getLength()).thenReturn(headersAndPayload.readableBytes());
        when(entry.getDataBuffer()).thenReturn(headersAndPayload);
        when(entry.getPosition()).thenReturn(position);
        when(entry.getLedgerId()).thenReturn(position.getLedgerId());
        when(entry.getEntryId()).thenReturn(position.getEntryId());
        doAnswer(invocation -> {
            headersAndPayload.release();
            return null;
        }).when(entry).release();
        return entry;
    }

    private static ByteBuf newMessageWithSchemaVersion() {
        MessageMetadata metadata = new MessageMetadata()
                .setProducerName("producer")
                .setSequenceId(1)
                .setPublishTime(System.currentTimeMillis())
                .setSchemaVersion(new byte[] { 1 });
        ByteBuf payload = Unpooled.wrappedBuffer(new byte[] { 1 });
        try {
            return Commands.serializeMetadataAndPayload(Commands.ChecksumType.Crc32c, metadata, payload);
        } finally {
            payload.release();
        }
    }

    private static class ThrowingSchemaReplicator extends GeoPersistentReplicator {

        private final ReplicatorContext context;

        ThrowingSchemaReplicator(Throwable schemaLookupFailure)
                throws PulsarServerException, ExecutionException {
            this(new ReplicatorContext(schemaLookupFailure));
        }

        private ThrowingSchemaReplicator(ReplicatorContext context)
                throws PulsarServerException {
            super(context.topic, context.cursor, "local", "remote", context.brokerService,
                    context.replicationClient, context.replicationAdmin);
            this.context = context;
        }

        @Override
        protected void startProducer() {
            // Avoid creating a real remote producer from the superclass constructor.
        }

        void forceStarted() {
            STATE_UPDATER.set(this, State.Started);
            this.producer = mock(ProducerImpl.class);
        }

        private static PersistentTopic mockTopic(BrokerService brokerService) {
            PersistentTopic topic = mock(PersistentTopic.class);
            when(topic.getName()).thenReturn("persistent://public/default/t1");
            when(topic.getBrokerService()).thenReturn(brokerService);
            when(topic.getReplicatorPrefix()).thenReturn("pulsar.repl");
            when(topic.getReplicatorDispatchRate()).thenReturn(null);
            return topic;
        }

        private static ManagedCursor mockCursor() {
            ManagedCursor cursor = mock(ManagedCursor.class);
            when(cursor.getName()).thenReturn("pulsar.repl.remote");
            when(cursor.getReadPosition()).thenReturn(PositionFactory.create(1, 1));
            return cursor;
        }

        private static BrokerService mockBrokerService(EventLoopGroup executor,
                                                       List<Runnable> scheduledTasks)
                throws PulsarServerException {
            ServiceConfiguration config = new ServiceConfiguration();
            PulsarService pulsar = mock(PulsarService.class);
            BrokerService brokerService = mock(BrokerService.class);
            PulsarClientImpl localClient = mock(PulsarClientImpl.class);
            PulsarAdmin admin = mock(PulsarAdmin.class);

            when(pulsar.getConfiguration()).thenReturn(config);
            when(pulsar.getConfig()).thenReturn(config);
            when(pulsar.getClient()).thenReturn(localClient);
            when(pulsar.getAdminClient()).thenReturn(admin);
            when(brokerService.pulsar()).thenReturn(pulsar);
            when(brokerService.getPulsar()).thenReturn(pulsar);
            when(brokerService.executor()).thenReturn(executor);
            doAnswer(invocation -> {
                scheduledTasks.add(invocation.getArgument(0, Runnable.class));
                return null;
            }).when(executor).schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
            return brokerService;
        }

        @SuppressWarnings("unchecked")
        private static PulsarClientImpl mockReplicationClient(Throwable schemaLookupFailure)
                throws ExecutionException {
            PulsarClientImpl replicationClient = mock(PulsarClientImpl.class);
            LoadingCache<String, SchemaInfoProvider> schemaProviderLoadingCache = mock(LoadingCache.class);
            ProducerBuilder<byte[]> producerBuilder = mock(ProducerBuilder.class, RETURNS_SELF);
            when(replicationClient.getSchemaProviderLoadingCache()).thenReturn(schemaProviderLoadingCache);
            doAnswer(__ -> {
                throw schemaLookupFailure;
            }).when(schemaProviderLoadingCache).get(anyString());
            when(replicationClient.newProducer(any(Schema.class))).thenReturn(producerBuilder);
            return replicationClient;
        }
    }

    private static class ReplicatorContext {
        private final List<Runnable> scheduledTasks;
        private final EventLoopGroup executor;
        private final BrokerService brokerService;
        private final PersistentTopic topic;
        private final ManagedCursor cursor;
        private final PulsarClientImpl replicationClient;
        private final PulsarAdmin replicationAdmin;

        private ReplicatorContext(Throwable schemaLookupFailure)
                throws PulsarServerException, ExecutionException {
            this.scheduledTasks = new ArrayList<>();
            this.executor = mock(EventLoopGroup.class);
            this.brokerService = ThrowingSchemaReplicator.mockBrokerService(executor, scheduledTasks);
            this.topic = ThrowingSchemaReplicator.mockTopic(brokerService);
            this.cursor = ThrowingSchemaReplicator.mockCursor();
            this.replicationClient = ThrowingSchemaReplicator.mockReplicationClient(schemaLookupFailure);
            this.replicationAdmin = mock(PulsarAdmin.class);
        }
    }
}
