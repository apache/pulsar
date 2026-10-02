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
import static org.mockito.Mockito.doAnswer;
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
import org.awaitility.Awaitility;
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
     * read on the calling thread, and a table view that records every delayed retry instead of running it.
     */
    private static final class TailRetryFixture implements AutoCloseable {
        final PulsarClientImpl client = mock(PulsarClientImpl.class);
        final Reader<String> reader = mock(Reader.class);
        final List<Long> retryDelays = new ArrayList<>();
        final List<Runnable> retries = new ArrayList<>();
        /** When set, the next retry runs before the call that queued it has returned. */
        final AtomicBoolean runNextRetryRightAway = new AtomicBoolean();
        final TableViewImpl<String> tableView;

        TailRetryFixture() {
            this(null);
        }

        /** @param strategy the compaction strategy the table view loads, or {@code null} for none */
        @SuppressWarnings("unchecked")
        TailRetryFixture(TopicCompactionStrategy<String> strategy) {
            ReaderBuilder<String> builder = mock(ReaderBuilder.class, RETURNS_SELF);
            when(client.newReader(Schema.STRING)).thenReturn(builder);
            when(client.getConfiguration()).thenReturn(new ClientConfigurationData());
            when(builder.createAsync()).thenReturn(CompletableFuture.completedFuture(reader));
            when(reader.closeAsync()).thenReturn(CompletableFuture.completedFuture(null));
            TableViewConfigurationData conf = new TableViewConfigurationData();
            conf.setTopicName(TAIL_RETRY_TOPIC);
            if (strategy == null) {
                tableView = newTableView(conf);
            } else {
                try (var strategies = mockStatic(TopicCompactionStrategy.class)) {
                    strategies.when(() -> TopicCompactionStrategy.load(TopicCompactionStrategy.TABLE_VIEW_TAG, null))
                            .thenReturn(strategy);
                    tableView = newTableView(conf);
                }
            }
        }

        private TableViewImpl<String> newTableView(TableViewConfigurationData conf) {
            return new TableViewImpl<>(client, Schema.STRING, conf) {
                @Override
                void runAfterDelay(long delayMillis, Runnable retry) {
                    retries.add(retry);
                    retryDelays.add(delayMillis);
                    if (runNextRetryRightAway.compareAndSet(true, false)) {
                        retry.run();
                    }
                }
            };
        }

        /** Runs the most recently queued retry, as happens once its delay has elapsed. */
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

    /** The refresh is already settled, with the failure a closed table view gives. */
    private static void assertFailedAsClosed(CompletableFuture<Void> refresh, String message) {
        assertTrue(refresh.isCompletedExceptionally(), message);
        ExecutionException failure = expectThrows(ExecutionException.class, refresh::get);
        assertTrue(failure.getCause() instanceof PulsarClientException.AlreadyClosedException,
                message + ", got " + failure.getCause());
    }

    @Test(timeOut = 10_000)
    public void testTailReadFailureSchedulesRetryInsteadOfRecursing() throws Exception {
        try (TailRetryFixture f = new TailRetryFixture()) {
            // Every read fails immediately, so a synchronous retry would spin on the caller's stack forever.
            when(f.reader.readNextAsync()).thenAnswer(inv -> failedRead());

            f.tableView.start().get(5, TimeUnit.SECONDS);

            verify(f.reader, times(1)).readNextAsync();
            assertEquals(f.retryDelays.size(), 1, "One retry must be left waiting for its delay");
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
    public void testCloseStopsThePendingTailReadRetryAndFailsPendingRefreshes() throws Exception {
        try (TailRetryFixture f = new TailRetryFixture()) {
            when(f.reader.readNextAsync()).thenAnswer(inv -> failedRead());
            f.tableView.start().get(5, TimeUnit.SECONDS);
            CompletableFuture<Void> refresh = f.pendingRefresh();
            assertEquals(f.retries.size(), 1, "The failed read must leave one retry waiting for its delay");
            AtomicReference<Throwable> seenByCallback = new AtomicReference<>();
            refresh.exceptionally(ex -> {
                seenByCallback.set(ex);
                return null;
            });

            f.tableView.closeAsync().get(5, TimeUnit.SECONDS);

            assertFailedAsClosed(refresh, "A refresh pending at close must fail right away");
            // Same shape as when the failure came from the closed reader: callbacks look at getCause().
            assertTrue(seenByCallback.get().getCause() instanceof PulsarClientException.AlreadyClosedException,
                    "A callback must find the cause where it used to be, got " + seenByCallback.get());
            // The delay elapses after the close: the queued retry must not read any more.
            f.runLatestRetry();
            verify(f.reader, times(1)).readNextAsync();
        }
    }

    @Test(timeOut = 10_000)
    public void testReadRejectedByItsExecutorFailsPendingRefreshes() throws Exception {
        try (TailRetryFixture f = new TailRetryFixture()) {
            when(f.reader.readNextAsync()).thenAnswer(inv -> failedRead());
            f.tableView.start().get(5, TimeUnit.SECONDS);
            CompletableFuture<Void> refresh = f.pendingRefresh();
            // The executor the reader hands its work to was shut down, with the client or the resources it
            // shares: the reader cannot even take the read.
            when(f.reader.readNextAsync()).thenThrow(new RejectedExecutionException("executor terminated"));

            f.runLatestRetry();

            assertFailedAsClosed(refresh, "A refresh cannot complete once the reader takes no more reads");
            assertEquals(f.retries.size(), 1, "A read the reader rejected must not be retried");
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
            assertEquals(delays.size(), 2, "Each failed handling must be retried: " + delays);
            // Doubling with the backoff's +-5% jitter; a reset would leave both near the initial delay.
            assertTrue(delays.get(1) >= delays.get(0) * 1.5,
                    "The delay must keep doubling while handling keeps failing: " + delays);
        }
    }

    @Test(timeOut = 10_000)
    public void testRefreshFetchingLastMessageIdsFailsWhenTheTableViewCloses() throws Exception {
        try (TailRetryFixture f = new TailRetryFixture()) {
            when(f.reader.readNextAsync()).thenAnswer(inv -> failedRead());
            f.tableView.start().get(5, TimeUnit.SECONDS);
            // The refresh is still asking for the last message ids when the table view closes.
            CompletableFuture<List<TopicMessageId>> lastMessageIds = new CompletableFuture<>();
            when(f.reader.getLastMessageIdsAsync()).thenReturn(lastMessageIds);
            CompletableFuture<Void> refresh = f.tableView.refreshAsync();

            f.tableView.closeAsync().get(5, TimeUnit.SECONDS);

            assertFailedAsClosed(refresh, "A refresh still looking up the last message ids must fail at close");
            assertFalse(lastMessageIds.isDone(), "The lookup itself is left alone");
        }
    }

    @Test(timeOut = 10_000)
    public void testRetryThatRunsRightAwayDoesNotOutliveClose() throws Exception {
        try (TailRetryFixture f = new TailRetryFixture()) {
            when(f.reader.readNextAsync()).thenAnswer(inv -> failedRead());
            // The first retry runs before the failed read's callback has returned, as it can on another thread
            // with a zero delay: it fails again and queues the second retry.
            f.runNextRetryRightAway.set(true);

            f.tableView.start().get(5, TimeUnit.SECONDS);
            verify(f.reader, times(2)).readNextAsync();
            assertEquals(f.retries.size(), 2, "The nested failure must have queued a second retry");

            f.tableView.closeAsync().get(5, TimeUnit.SECONDS);
            f.runLatestRetry();

            verify(f.reader, times(2)).readNextAsync();
        }
    }

    @Test(timeOut = 10_000)
    public void testRefreshAfterARejectedReadFailsRightAway() throws Exception {
        try (TailRetryFixture f = new TailRetryFixture()) {
            when(f.reader.readNextAsync()).thenAnswer(inv -> failedRead());
            f.tableView.start().get(5, TimeUnit.SECONDS);
            when(f.reader.readNextAsync()).thenThrow(new RejectedExecutionException("executor terminated"));
            f.runLatestRetry();

            // The mocked reader would still answer, with an empty topic even.
            when(f.reader.getLastMessageIdsAsync()).thenReturn(CompletableFuture.completedFuture(List.of()));
            CompletableFuture<Void> refresh = f.tableView.refreshAsync();

            assertFailedAsClosed(refresh, "A refresh after the tail reads stopped must fail right away");
            verify(f.reader, never()).getLastMessageIdsAsync();
        }
    }

    @Test(timeOut = 10_000)
    public void testRefreshFetchingLastMessageIdsFailsWhenTheReaderIsClosed() throws Exception {
        try (TailRetryFixture f = new TailRetryFixture()) {
            CompletableFuture<Message<String>> read = new CompletableFuture<>();
            when(f.reader.readNextAsync()).thenReturn(read);
            f.tableView.start().get(5, TimeUnit.SECONDS);
            CompletableFuture<List<TopicMessageId>> lastMessageIds = new CompletableFuture<>();
            when(f.reader.getLastMessageIdsAsync()).thenReturn(lastMessageIds);
            CompletableFuture<Void> refresh = f.tableView.refreshAsync();

            // The reader is closed under the table view, by the client shutting down: the tail loop ends there.
            read.completeExceptionally(new PulsarClientException.AlreadyClosedException("Consumer was already closed"));

            assertFailedAsClosed(refresh, "A refresh still looking up the last message ids must fail with the loop");
            assertFalse(lastMessageIds.isDone(), "The lookup itself is left alone");
        }
    }

    @Test(timeOut = 10_000)
    public void testRefreshFetchingLastMessageIdsFailsWhenTheReadIsRejected() throws Exception {
        try (TailRetryFixture f = new TailRetryFixture()) {
            when(f.reader.readNextAsync()).thenAnswer(inv -> failedRead());
            f.tableView.start().get(5, TimeUnit.SECONDS);
            CompletableFuture<List<TopicMessageId>> lastMessageIds = new CompletableFuture<>();
            when(f.reader.getLastMessageIdsAsync()).thenReturn(lastMessageIds);
            CompletableFuture<Void> refresh = f.tableView.refreshAsync();
            when(f.reader.readNextAsync()).thenThrow(new RejectedExecutionException("executor terminated"));

            f.runLatestRetry();

            assertFailedAsClosed(refresh, "A refresh still looking up the last message ids must fail with the loop");
            assertFalse(lastMessageIds.isDone(), "The lookup itself is left alone");
        }
    }

    @Test(timeOut = 10_000)
    public void testRefreshAfterTheTableViewClosedDoesNotLookUpTheLastMessageIds() throws Exception {
        try (TailRetryFixture f = new TailRetryFixture()) {
            when(f.reader.readNextAsync()).thenReturn(new CompletableFuture<>());
            f.tableView.start().get(5, TimeUnit.SECONDS);
            f.tableView.closeAsync().get(5, TimeUnit.SECONDS);
            // The mocked reader would still answer, with an empty topic even: only the table view knows that
            // nothing will be read any more.
            when(f.reader.getLastMessageIdsAsync()).thenReturn(CompletableFuture.completedFuture(List.of()));

            CompletableFuture<Void> refresh = f.tableView.refreshAsync();

            assertFailedAsClosed(refresh, "A refresh on a closed table view must fail, empty topic or not");
            verify(f.reader, never()).getLastMessageIdsAsync();
        }
    }

    @Test(timeOut = 10_000)
    @SuppressWarnings("unchecked")
    public void testNoReadIsIssuedAfterTheTableViewClosed() throws Exception {
        try (TailRetryFixture f = new TailRetryFixture()) {
            CompletableFuture<Message<String>> read = new CompletableFuture<>();
            when(f.reader.readNextAsync()).thenReturn(read, new CompletableFuture<>());
            f.tableView.start().get(5, TimeUnit.SECONDS);
            f.tableView.closeAsync().get(5, TimeUnit.SECONDS);
            // The read that was in flight completes after the close; the mocked reader would take another one.
            Message<String> message = mock(Message.class);
            when(message.getTopicName()).thenReturn(TAIL_RETRY_TOPIC);
            when(message.getMessageId()).thenReturn(new MessageIdImpl(1, 0, -1));
            when(message.hasKey()).thenReturn(false);

            read.complete(message);

            verify(f.reader, times(1)).readNextAsync();
        }
    }

    @Test(timeOut = 10_000)
    public void testEmptyLookupAnswerArrivingDuringTheStopDoesNotCompleteTheRefresh() throws Exception {
        try (TailRetryFixture f = new TailRetryFixture()) {
            when(f.reader.readNextAsync()).thenReturn(new CompletableFuture<>());
            f.tableView.start().get(5, TimeUnit.SECONDS);
            CompletableFuture<List<TopicMessageId>> firstLookup = new CompletableFuture<>();
            CompletableFuture<List<TopicMessageId>> secondLookup = new CompletableFuture<>();
            when(f.reader.getLastMessageIdsAsync()).thenReturn(firstLookup, secondLookup);
            CompletableFuture<Void> first = f.tableView.refreshAsync();
            CompletableFuture<Void> second = f.tableView.refreshAsync();
            // Whichever refresh the stop fails first, the answer for the other one arrives at that very moment,
            // when the stop is under way but has not reached it yet, and says that the topic is empty.
            first.whenComplete((v, e) -> secondLookup.complete(List.of()));
            second.whenComplete((v, e) -> firstLookup.complete(List.of()));

            f.tableView.closeAsync().get(5, TimeUnit.SECONDS);

            assertFailedAsClosed(first, "An empty topic must not turn a stopped refresh into a success");
            assertFailedAsClosed(second, "An empty topic must not turn a stopped refresh into a success");
        }
    }

    @Test(timeOut = 10_000)
    public void testFirstStopCauseIsKept() throws Exception {
        try (TailRetryFixture f = new TailRetryFixture()) {
            CompletableFuture<Message<String>> read = new CompletableFuture<>();
            when(f.reader.readNextAsync()).thenReturn(read);
            f.tableView.start().get(5, TimeUnit.SECONDS);
            f.tableView.closeAsync().get(5, TimeUnit.SECONDS);
            // The reader then fails the read that was in flight: a second reason to stop.
            read.completeExceptionally(new PulsarClientException.AlreadyClosedException("Consumer already closed"));

            ExecutionException failure = expectThrows(ExecutionException.class,
                    () -> f.tableView.refreshAsync().get());
            assertTrue(failure.getCause().getMessage().contains("TableView was closed"),
                    "A later refresh must report why the tail reads stopped first, got " + failure.getCause());
        }
    }

    @Test(timeOut = 10_000)
    public void testCompletedRefreshesAreNoLongerTracked() throws Exception {
        try (TailRetryFixture f = new TailRetryFixture()) {
            when(f.reader.readNextAsync()).thenReturn(new CompletableFuture<>());
            f.tableView.start().get(5, TimeUnit.SECONDS);
            // The caller gives up on a refresh that waits for a message, and on one whose last message ids only
            // arrive afterwards.
            f.pendingRefresh().cancel(false);
            CompletableFuture<List<TopicMessageId>> lateAnswer = new CompletableFuture<>();
            when(f.reader.getLastMessageIdsAsync()).thenReturn(lateAnswer);
            f.tableView.refreshAsync().cancel(false);
            lateAnswer.complete(List.of(new TopicMessageIdImpl(TAIL_RETRY_TOPIC, new MessageIdImpl(1, 5, -1))));
            assertFalse(f.tableView.isTrackingRefreshes(), "A refresh the caller completed must not be kept around");

            // The stop fails a refresh that waits for a message and one that is still looking up the ids.
            CompletableFuture<Void> waiting = f.pendingRefresh();
            when(f.reader.getLastMessageIdsAsync()).thenReturn(new CompletableFuture<>());
            CompletableFuture<Void> lookingUp = f.tableView.refreshAsync();
            assertTrue(f.tableView.isTrackingRefreshes());
            f.tableView.closeAsync().get(5, TimeUnit.SECONDS);

            assertTrue(waiting.isDone(), "The stop must have settled the refresh waiting for a message");
            assertTrue(lookingUp.isDone(), "The stop must have settled the refresh still looking up the ids");
            assertFalse(f.tableView.isTrackingRefreshes(), "A refresh the stop failed must not be kept around");
        }
    }

    @Test(timeOut = 10_000)
    @SuppressWarnings("unchecked")
    public void testNoTailReadStartsWhenTheTableViewClosesDuringTheInitialReplay() throws Exception {
        PulsarClientImpl client = mock(PulsarClientImpl.class);
        ReaderBuilder<String> builder = mock(ReaderBuilder.class, RETURNS_SELF);
        Reader<String> reader = mock(Reader.class);
        when(client.getConfiguration()).thenReturn(new ClientConfigurationData());
        when(client.newReader(Schema.STRING)).thenReturn(builder);
        when(builder.createAsync()).thenReturn(CompletableFuture.completedFuture(reader));
        when(reader.closeAsync()).thenReturn(CompletableFuture.completedFuture(null));
        when(reader.readNextAsync()).thenReturn(new CompletableFuture<>());
        // A persistent topic: start() replays the existing messages first, and is still asking for the last
        // message ids when the table view closes.
        CompletableFuture<List<TopicMessageId>> lastMessageIds = new CompletableFuture<>();
        when(reader.getLastMessageIdsAsync()).thenReturn(lastMessageIds);
        TableViewConfigurationData conf = new TableViewConfigurationData();
        conf.setTopicName("persistent://tenant/ns/closed-during-replay");
        TableViewImpl<String> tableView = new TableViewImpl<>(client, Schema.STRING, conf);
        CompletableFuture<TableView<String>> start = tableView.start();

        tableView.closeAsync().get(5, TimeUnit.SECONDS);
        // The replay finds an empty topic and ends normally.
        lastMessageIds.complete(List.of());

        start.get(5, TimeUnit.SECONDS);
        verify(reader, never()).readNextAsync();
    }

    @Test(timeOut = 30_000)
    @SuppressWarnings("unchecked")
    public void testRetryWaitingWhenTheClientClosesStillFailsPendingRefreshes() throws Exception {
        // The client's scheduler, shut down when the client closes. The retry used to wait there and was dropped
        // with it; nothing is queued on it any more, so shutting it down below changes nothing now.
        ScheduledExecutorService clientScheduler = Executors.newSingleThreadScheduledExecutor();
        CountDownLatch clientClosed = new CountDownLatch(1);
        PulsarClientImpl client = mock(PulsarClientImpl.class);
        when(client.getConfiguration()).thenReturn(new ClientConfigurationData());
        ScheduledExecutorProvider provider = mock(ScheduledExecutorProvider.class);
        when(provider.getExecutor()).thenReturn(clientScheduler);
        when(client.getScheduledExecutorProvider()).thenReturn(provider);
        ReaderBuilder<String> builder = mock(ReaderBuilder.class, RETURNS_SELF);
        when(client.newReader(Schema.STRING)).thenReturn(builder);
        Reader<String> reader = mock(Reader.class);
        when(builder.createAsync()).thenReturn(CompletableFuture.completedFuture(reader));
        when(reader.closeAsync()).thenReturn(CompletableFuture.completedFuture(null));
        // The first read fails, so a retry waits for its delay. The read it issues finds the reader closed by the
        // client.
        when(reader.readNextAsync()).thenReturn(failedRead()).thenAnswer(inv -> {
            assertTrue(clientClosed.await(10, TimeUnit.SECONDS));
            return FutureUtil.failedFuture(new PulsarClientException.AlreadyClosedException("Consumer already closed"));
        });
        when(reader.getLastMessageIdsAsync()).thenReturn(CompletableFuture.completedFuture(
                List.of(new TopicMessageIdImpl(TAIL_RETRY_TOPIC, new MessageIdImpl(1, 5, -1)))));
        TableViewConfigurationData conf = new TableViewConfigurationData();
        conf.setTopicName(TAIL_RETRY_TOPIC);
        try (TableViewImpl<String> tableView = new TableViewImpl<>(client, Schema.STRING, conf)) {
            tableView.start().get(5, TimeUnit.SECONDS);
            CompletableFuture<Void> refresh = tableView.refreshAsync();
            assertFalse(refresh.isDone(), "The refresh must wait for a message the reader has not delivered");

            // The client closes while the table view is still open and the retry is waiting.
            clientScheduler.shutdownNow();
            clientClosed.countDown();

            Awaitility.await().atMost(10, TimeUnit.SECONDS).until(refresh::isDone);
            assertFailedAsClosed(refresh, "The retry must still run and find the closed reader");
        } finally {
            clientClosed.countDown();
            clientScheduler.shutdownNow();
        }
    }

    @Test(timeOut = 10_000)
    public void testReadThatThrowsIsRetriedLikeAFailedRead() throws Exception {
        try (TailRetryFixture f = new TailRetryFixture()) {
            when(f.reader.readNextAsync()).thenAnswer(inv -> failedRead());
            f.tableView.start().get(5, TimeUnit.SECONDS);
            assertEquals(f.retries.size(), 1);
            // A reader that throws instead of returning a failed future: thrown from the retry, it would end
            // the tail reads without anything noticing.
            when(f.reader.readNextAsync()).thenThrow(new IllegalStateException("reader failure"));

            f.runLatestRetry();

            assertEquals(f.retries.size(), 2, "The read that threw must be retried like a failed read");
            assertTrue(f.retryDelays.get(1) >= f.retryDelays.get(0) * 1.5,
                    "and keep backing off: " + f.retryDelays);
        }
    }

    @Test(timeOut = 10_000)
    @SuppressWarnings("unchecked")
    public void testHandlingFailureThatIsARejectionIsRetried() throws Exception {
        TopicCompactionStrategy<String> strategy = mock(TopicCompactionStrategy.class);
        // A strategy can be rejected by an executor of its own, a saturated one for instance: that says nothing
        // about the reader.
        when(strategy.shouldKeepLeft(any(), any())).thenThrow(new RejectedExecutionException("strategy is busy"));
        try (TailRetryFixture f = new TailRetryFixture(strategy)) {
            Message<String> message = mock(Message.class);
            when(message.getTopicName()).thenReturn(TAIL_RETRY_TOPIC);
            when(message.getMessageId()).thenReturn(new MessageIdImpl(1, 0, -1));
            when(message.hasKey()).thenReturn(true);
            when(message.getKey()).thenReturn("key");
            when(message.size()).thenReturn(1);
            when(message.getValue()).thenReturn("value");
            when(f.reader.readNextAsync()).thenReturn(CompletableFuture.completedFuture(message),
                    new CompletableFuture<>());
            f.tableView.start().get(5, TimeUnit.SECONDS);
            CompletableFuture<Void> refresh = f.pendingRefresh();

            assertEquals(f.retries.size(), 1, "Only a read the reader rejects ends the tail reads");
            f.runLatestRetry();

            verify(f.reader, times(2)).readNextAsync();
            assertFalse(refresh.isDone(), "The tail reads go on, so the refresh keeps waiting");
        }
    }

    @Test(timeOut = 10_000)
    public void testFailedReadIsNotRetriedOnceTheClientIsClosed() throws Exception {
        try (TailRetryFixture f = new TailRetryFixture()) {
            // A reader that keeps failing its reads without being closed, as one that has failed for good does:
            // the client no longer closes it.
            when(f.reader.readNextAsync()).thenAnswer(inv -> failedRead());
            f.tableView.start().get(5, TimeUnit.SECONDS);
            CompletableFuture<Void> refresh = f.pendingRefresh();
            when(f.client.isClosed()).thenReturn(true);

            f.runLatestRetry();

            assertFailedAsClosed(refresh, "A refresh cannot complete once the client is closed");
            assertEquals(f.retries.size(), 1, "Nothing is retried once the client is closed");
        }
    }

}
