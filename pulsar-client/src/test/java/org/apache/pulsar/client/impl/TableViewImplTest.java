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
package org.apache.pulsar.client.impl;

import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pulsar.client.api.CryptoKeyReader;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.ReaderBuilder;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TableView;
import org.apache.pulsar.client.api.TopicMessageId;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.util.ScheduledExecutorProvider;
import org.apache.pulsar.common.topics.TopicCompactionStrategy;
import org.apache.pulsar.common.util.FutureUtil;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class TableViewImplTest {

    private PulsarClientImpl client;
    private TableViewConfigurationData data;

    @BeforeClass(alwaysRun = true)
    @SuppressWarnings("unchecked")
    public void setup() {
        client = mock(PulsarClientImpl.class);
        ConnectionPool connectionPool = mock(ConnectionPool.class);
        when(client.getCnxPool()).thenReturn(connectionPool);
        when(client.getConfiguration()).thenReturn(new ClientConfigurationData());
        when(client.newReader(any(Schema.class)))
            .thenReturn(new ReaderBuilderImpl(client, Schema.BYTES));

        data = new TableViewConfigurationData();
        data.setTopicName("testTopicName");
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testTableViewImpl() {
        data.setCryptoKeyReader(mock(CryptoKeyReader.class));
        TableView<?> tableView = new TableViewImpl<>(client, Schema.BYTES, data);

        assertNotNull(tableView);
    }
    @DataProvider
    public Object[][] topicNames() {
        return new Object[][]{
                {"persistent://tenant/ns/topic", true},
                {"tenant/ns/topic", true},
                {"topic", true},
                {"persistent-topic", true},
                {"non-persistent://tenant/ns/topic", false}
        };
    }

    @Test(timeOut = 10_000, dataProvider = "topicNames")
    @SuppressWarnings("unchecked")
    public void testTopicDomain(String topic, boolean persistent) throws Exception {
        PulsarClientImpl client = mock(PulsarClientImpl.class);
        ReaderBuilder<String> builder = mock(ReaderBuilder.class, RETURNS_SELF);
        Reader<String> reader = mock(Reader.class);
        when(client.getConfiguration()).thenReturn(new ClientConfigurationData());
        when(client.newReader(Schema.STRING)).thenReturn(builder);
        when(builder.createAsync()).thenReturn(CompletableFuture.completedFuture(reader));
        when(reader.closeAsync()).thenReturn(CompletableFuture.completedFuture(null));
        CompletableFuture<List<TopicMessageId>> lastMessageIds =
                new CompletableFuture<>();
        when(reader.getLastMessageIdsAsync()).thenReturn(lastMessageIds);
        when(reader.readNextAsync()).thenReturn(new CompletableFuture<>());
        TableViewConfigurationData conf = new TableViewConfigurationData();
        conf.setTopicName(topic);
        try (TableViewImpl<String> tableView = new TableViewImpl<>(client, Schema.STRING, conf)) {
            if (persistent) {
                verify(builder).readCompacted(true);
            } else {
                verify(builder, never()).readCompacted(true);
            }
            var start = tableView.start();
            assertEquals(start.isDone(), !persistent,
                    "Persistent topics must wait for the initial replay");
            lastMessageIds.complete(List.of());
            assertEquals(start.get(5, TimeUnit.SECONDS), tableView);
            if (persistent) {
                verify(reader).getLastMessageIdsAsync();
            } else {
                verify(reader, never()).getLastMessageIdsAsync();
            }
        }
    }

    @DataProvider
    public Object[][] skippedMessage() {
        return new Object[][]{{false}, {true}};
    }

    @Test(timeOut = 10_000, dataProvider = "skippedMessage")
    @SuppressWarnings("unchecked")
    public void testRefreshWaitsForMessageToBeApplied(boolean skipped) throws Exception {
        String topic = "persistent://public/default/refresh-applied";
        PulsarClientImpl client = mock(PulsarClientImpl.class);
        ReaderBuilder<String> builder = mock(ReaderBuilder.class, RETURNS_SELF);
        Reader<String> reader = mock(Reader.class);
        when(client.getConfiguration()).thenReturn(new ClientConfigurationData());
        when(client.newReader(Schema.STRING)).thenReturn(builder);
        when(builder.createAsync()).thenReturn(CompletableFuture.completedFuture(reader));
        when(reader.closeAsync()).thenReturn(CompletableFuture.completedFuture(null));
        TopicMessageIdImpl messageId = new TopicMessageIdImpl(topic, new MessageIdImpl(1, 0, -1));
        when(reader.getLastMessageIdsAsync()).thenReturn(
                CompletableFuture.completedFuture(List.of()),
                CompletableFuture.completedFuture(List.of(messageId)));
        CompletableFuture<Message<String>> nextMessage = new CompletableFuture<>();
        when(reader.readNextAsync()).thenReturn(nextMessage, new CompletableFuture<>());
        TableViewConfigurationData conf = new TableViewConfigurationData();
        conf.setTopicName(topic);
        TopicCompactionStrategy<String> strategy = mock(TopicCompactionStrategy.class);
        when(strategy.shouldKeepLeft(any(), any())).thenReturn(skipped);
        TableViewImpl<String> tableView;
        try (var strategies = mockStatic(TopicCompactionStrategy.class)) {
            strategies.when(() -> TopicCompactionStrategy.load(TopicCompactionStrategy.TABLE_VIEW_TAG, null))
                    .thenReturn(strategy);
            tableView = new TableViewImpl<>(client, Schema.STRING, conf);
        }
        tableView.start().get(5, TimeUnit.SECONDS);
        AtomicBoolean callbackRefreshCompleted = new AtomicBoolean();
        tableView.listen((key, value) -> callbackRefreshCompleted.set(tableView.refreshAsync().isDone()));
        doAnswer(invocation -> {
            callbackRefreshCompleted.set(tableView.refreshAsync().isDone());
            return null;
        }).when(strategy).handleSkippedMessage(any(), any());

        CountDownLatch decoding = new CountDownLatch(1);
        CountDownLatch applyMessage = new CountDownLatch(1);
        Message<String> message = mock(Message.class);
        when(message.getTopicName()).thenReturn(topic);
        when(message.getMessageId()).thenReturn(messageId);
        when(message.hasKey()).thenReturn(true);
        when(message.getKey()).thenReturn("key");
        when(message.size()).thenReturn(1);
        when(message.getValue()).thenAnswer(invocation -> {
            decoding.countDown();
            assertTrue(applyMessage.await(5, TimeUnit.SECONDS));
            return "value";
        });
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            var delivery = executor.submit(() -> nextMessage.complete(message));
            assertTrue(decoding.await(5, TimeUnit.SECONDS));
            assertNull(tableView.get("key"));
            var refresh = tableView.refreshAsync();
            assertFalse(refresh.isDone(), "Refresh must not finish before the message updates the table");
            applyMessage.countDown();
            delivery.get(5, TimeUnit.SECONDS);
            refresh.get(5, TimeUnit.SECONDS);
            assertEquals(tableView.get("key"), skipped ? null : "value");
            assertTrue(callbackRefreshCompleted.get(), "A callback must observe the current message as applied");
        } finally {
            applyMessage.countDown();
            executor.shutdownNow();
            tableView.close();
        }
    }

    /**
     * Fixture for the tail-read retry: a non-persistent topic, so {@code start()} issues the first tail
     * read on the calling thread, and a scheduler mock that records every delayed retry instead of
     * running it.
     */
    private static final class TailRetryFixture implements AutoCloseable {
        final Reader<String> reader = mock(Reader.class);
        final ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
        final List<Long> retryDelays = new ArrayList<>();
        final List<Runnable> retries = new ArrayList<>();
        final List<ScheduledFuture<?>> scheduledRetries = new ArrayList<>();
        final TableViewImpl<String> tableView;

        TailRetryFixture() {
            this(null);
        }

        /** @param strategy the compaction strategy the table view loads, or {@code null} for none */
        @SuppressWarnings("unchecked")
        TailRetryFixture(TopicCompactionStrategy<String> strategy) {
            PulsarClientImpl client = mock(PulsarClientImpl.class);
            ReaderBuilder<String> builder = mock(ReaderBuilder.class, RETURNS_SELF);
            when(client.newReader(Schema.STRING)).thenReturn(builder);
            when(client.getConfiguration()).thenReturn(new ClientConfigurationData());
            when(scheduler.schedule(any(Runnable.class), anyLong(), any(TimeUnit.class))).thenAnswer(inv -> {
                retries.add(inv.getArgument(0));
                retryDelays.add(inv.getArgument(1));
                ScheduledFuture<?> scheduled = mock(ScheduledFuture.class);
                scheduledRetries.add(scheduled);
                return scheduled;
            });
            ScheduledExecutorProvider provider = mock(ScheduledExecutorProvider.class);
            when(provider.getExecutor()).thenReturn(scheduler);
            when(client.getScheduledExecutorProvider()).thenReturn(provider);
            when(builder.createAsync()).thenReturn(CompletableFuture.completedFuture(reader));
            when(reader.closeAsync()).thenReturn(CompletableFuture.completedFuture(null));
            TableViewConfigurationData conf = new TableViewConfigurationData();
            conf.setTopicName(TAIL_RETRY_TOPIC);
            if (strategy == null) {
                tableView = new TableViewImpl<>(client, Schema.STRING, conf);
            } else {
                try (var strategies = mockStatic(TopicCompactionStrategy.class)) {
                    strategies.when(() -> TopicCompactionStrategy.load(TopicCompactionStrategy.TABLE_VIEW_TAG, null))
                            .thenReturn(strategy);
                    tableView = new TableViewImpl<>(client, Schema.STRING, conf);
                }
            }
        }

        /** Runs the most recently scheduled retry, as the scheduler would once its delay elapsed. */
        void runLatestRetry() {
            retries.get(retries.size() - 1).run();
        }

        /** Registers a refresh that waits for a message the reader has not delivered yet. */
        CompletableFuture<Void> pendingRefresh() {
            TopicMessageId lastMessageId = new TopicMessageIdImpl(TAIL_RETRY_TOPIC, new MessageIdImpl(1, 5, -1));
            when(reader.getLastMessageIdsAsync())
                    .thenReturn(CompletableFuture.completedFuture(List.of(lastMessageId)));
            CompletableFuture<Void> refresh = tableView.refreshAsync();
            assertFalse(refresh.isDone(), "The refresh must wait for the message at " + lastMessageId);
            return refresh;
        }

        @Override
        public void close() throws PulsarClientException {
            tableView.close();
        }
    }

    private static final String TAIL_RETRY_TOPIC = "non-persistent://tenant/ns/tail-retry";

    private static CompletableFuture<Message<String>> failedRead() {
        return FutureUtil.failedFuture(new PulsarClientException.NotConnectedException());
    }

    @Test(timeOut = 10_000)
    public void testTailReadFailureSchedulesRetryInsteadOfRecursing() throws Exception {
        try (TailRetryFixture f = new TailRetryFixture()) {
            // Every read fails immediately, so a synchronous retry would spin on the caller's stack forever.
            when(f.reader.readNextAsync()).thenAnswer(inv -> failedRead());

            f.tableView.start().get(5, TimeUnit.SECONDS);

            verify(f.reader, times(1)).readNextAsync();
            assertEquals(f.retryDelays.size(), 1, "One retry must be handed to the scheduler");
            assertTrue(f.retryDelays.get(0) > 0, "The retry must be delayed, got " + f.retryDelays);

            f.runLatestRetry();
            verify(f.reader, times(2)).readNextAsync();
        }
    }

    @Test(timeOut = 10_000)
    public void testTailReadRetryDelayGrowsOnRepeatedFailures() throws Exception {
        try (TailRetryFixture f = new TailRetryFixture()) {
            when(f.reader.readNextAsync()).thenAnswer(inv -> failedRead());

            f.tableView.start().get(5, TimeUnit.SECONDS);
            f.runLatestRetry();
            f.runLatestRetry();

            List<Long> delays = f.retryDelays;
            assertEquals(delays.size(), 3);
            assertTrue(delays.get(1) > delays.get(0), "Second delay must back off: " + delays);
            assertTrue(delays.get(2) > delays.get(1), "Third delay must back off: " + delays);
        }
    }

    @Test(timeOut = 10_000)
    @SuppressWarnings("unchecked")
    public void testTailReadRetryDelayResetsAfterSuccessfulRead() throws Exception {
        try (TailRetryFixture f = new TailRetryFixture()) {
            Message<String> message = mock(Message.class);
            when(message.getTopicName()).thenReturn(TAIL_RETRY_TOPIC);
            when(message.getMessageId()).thenReturn(new MessageIdImpl(1, 0, -1));
            when(message.hasKey()).thenReturn(false);
            // fail, fail, succeed, fail, then block: the delay after the success must start over.
            when(f.reader.readNextAsync()).thenReturn(failedRead(), failedRead(),
                    CompletableFuture.completedFuture(message), failedRead(), new CompletableFuture<>());

            f.tableView.start().get(5, TimeUnit.SECONDS);
            f.runLatestRetry();
            f.runLatestRetry();

            // start, two retries, and the read issued right after the successful one
            verify(f.reader, times(4)).readNextAsync();
            List<Long> delays = f.retryDelays;
            assertEquals(delays.size(), 3, "Two failures before the success and one after: " + delays);
            assertTrue(delays.get(2) < delays.get(1),
                    "Delay after a successful read must reset to the initial backoff: " + delays);
        }
    }

    @Test(timeOut = 10_000)
    public void testCloseCancelsPendingTailReadRetryAndFailsPendingRefreshes() throws Exception {
        try (TailRetryFixture f = new TailRetryFixture()) {
            when(f.reader.readNextAsync()).thenAnswer(inv -> failedRead());
            f.tableView.start().get(5, TimeUnit.SECONDS);
            CompletableFuture<Void> refresh = f.pendingRefresh();
            assertEquals(f.scheduledRetries.size(), 1, "The failed read must leave one retry waiting for its delay");
            AtomicReference<Throwable> seenByCallback = new AtomicReference<>();
            refresh.exceptionally(ex -> {
                seenByCallback.set(ex);
                return null;
            });

            f.tableView.closeAsync().get(5, TimeUnit.SECONDS);

            verify(f.scheduledRetries.get(0)).cancel(false);
            ExecutionException failure = expectThrows(ExecutionException.class,
                    () -> refresh.get(5, TimeUnit.SECONDS));
            assertTrue(failure.getCause() instanceof PulsarClientException.AlreadyClosedException,
                    "A refresh pending at close must fail right away, got " + failure.getCause());
            // Same shape as when the failure came from the closed reader: callbacks look at getCause().
            assertTrue(seenByCallback.get().getCause() instanceof PulsarClientException.AlreadyClosedException,
                    "A callback must find the cause where it used to be, got " + seenByCallback.get());
        }
    }

    @Test(timeOut = 10_000)
    public void testRejectedTailReadRetryFailsPendingRefreshes() throws Exception {
        try (TailRetryFixture f = new TailRetryFixture()) {
            CompletableFuture<Message<String>> read = new CompletableFuture<>();
            when(f.reader.readNextAsync()).thenReturn(read);
            f.tableView.start().get(5, TimeUnit.SECONDS);
            CompletableFuture<Void> refresh = f.pendingRefresh();
            // The client is shutting down: its scheduler no longer accepts the retry.
            doThrow(new RejectedExecutionException("shutting down"))
                    .when(f.scheduler).schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));

            read.completeExceptionally(new PulsarClientException.NotConnectedException());

            ExecutionException failure = expectThrows(ExecutionException.class,
                    () -> refresh.get(5, TimeUnit.SECONDS));
            assertTrue(failure.getCause() instanceof PulsarClientException.AlreadyClosedException,
                    "A refresh cannot complete once retrying has stopped, got " + failure.getCause());
        }
    }

    @Test(timeOut = 10_000)
    @SuppressWarnings("unchecked")
    public void testProcessingFailureBacksOffLikeAReadFailure() throws Exception {
        TopicCompactionStrategy<String> strategy = mock(TopicCompactionStrategy.class);
        when(strategy.shouldKeepLeft(any(), any())).thenThrow(new IllegalStateException("strategy failure"));
        try (TailRetryFixture f = new TailRetryFixture(strategy)) {
            Message<String> message = mock(Message.class);
            when(message.getTopicName()).thenReturn(TAIL_RETRY_TOPIC);
            when(message.getMessageId()).thenReturn(new MessageIdImpl(1, 0, -1));
            when(message.hasKey()).thenReturn(true);
            when(message.getKey()).thenReturn("key");
            when(message.size()).thenReturn(1);
            when(message.getValue()).thenReturn("value");
            // Two reads succeed but handling their message fails each time, then the reader blocks.
            when(f.reader.readNextAsync()).thenReturn(CompletableFuture.completedFuture(message),
                    CompletableFuture.completedFuture(message), new CompletableFuture<>());

            f.tableView.start().get(5, TimeUnit.SECONDS);
            f.runLatestRetry();

            List<Long> delays = f.retryDelays;
            assertEquals(delays.size(), 2, "Each failed handling must schedule a retry: " + delays);
            // Doubling with the backoff's +-5% jitter; a reset would leave both near the initial delay.
            assertTrue(delays.get(1) >= delays.get(0) * 1.5,
                    "The delay must keep doubling while handling keeps failing: " + delays);
        }
    }

    @Test(timeOut = 10_000)
    public void testRefreshInFlightWhileClosingFailsRightAway() throws Exception {
        try (TailRetryFixture f = new TailRetryFixture()) {
            when(f.reader.readNextAsync()).thenAnswer(inv -> failedRead());
            f.tableView.start().get(5, TimeUnit.SECONDS);
            // The refresh is still asking for the last message ids when the table view closes; the answer
            // arrives afterwards, when no read is left that could ever complete the refresh.
            CompletableFuture<List<TopicMessageId>> lastMessageIds = new CompletableFuture<>();
            when(f.reader.getLastMessageIdsAsync()).thenReturn(lastMessageIds);
            CompletableFuture<Void> refresh = f.tableView.refreshAsync();

            f.tableView.closeAsync().get(5, TimeUnit.SECONDS);
            assertFalse(refresh.isDone(), "The refresh is still waiting for the last message ids");
            lastMessageIds.complete(List.of(new TopicMessageIdImpl(TAIL_RETRY_TOPIC, new MessageIdImpl(1, 5, -1))));

            ExecutionException failure = expectThrows(ExecutionException.class,
                    () -> refresh.get(5, TimeUnit.SECONDS));
            assertTrue(failure.getCause() instanceof PulsarClientException.AlreadyClosedException,
                    "A refresh that registers after the close must fail right away, got " + failure.getCause());
        }
    }

}
