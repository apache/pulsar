/**
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
package org.apache.pulsar.broker.service.streamingdispatch;

import com.google.common.base.Charsets;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.EntryImpl;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.Test;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

/**
 * Tests for {@link StreamingEntryReader}
 */
@Test(groups = "flaky")
public class StreamingEntryReaderTests extends MockedBookKeeperTestCase {

    private static final Charset Encoding = Charsets.UTF_8;
    private PersistentTopic mockTopic;
    private StreamingDispatcher mockDispatcher;
    private BrokerService mockBrokerService;
    private EventLoopGroup eventLoopGroup;
    private OrderedExecutor orderedExecutor;
    private ManagedLedgerConfig config;
    private ManagedLedgerImpl ledger;
    private ManagedCursor cursor;

    @Override
    protected void setUpTestCase() throws Exception {
        eventLoopGroup = new NioEventLoopGroup(1);
        orderedExecutor = OrderedScheduler.newSchedulerBuilder()
                .numThreads(1)
                .name("StreamingEntryReaderTests").build();
        mockTopic = mock(PersistentTopic.class);
        mockBrokerService = mock(BrokerService.class);
        mockDispatcher = mock(StreamingDispatcher.class);
        config = new ManagedLedgerConfig().setMaxEntriesPerLedger(10);
        ledger = spy((ManagedLedgerImpl) factory.open("my_test_ledger", config));
        cursor = ledger.openCursor("test");
        when(mockTopic.getBrokerService()).thenReturn(mockBrokerService);
        when(mockBrokerService.executor()).thenReturn(eventLoopGroup);
        when(mockBrokerService.getTopicOrderedExecutor()).thenReturn(orderedExecutor);
        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocationOnMock) {
                return null;
            }
        }).when(mockDispatcher).notifyConsumersEndOfTopic();
    }

    @Override
    protected void cleanUpTestCase() {
        if (eventLoopGroup != null) {
            eventLoopGroup.shutdownNow();
            eventLoopGroup = null;
        }
        if (orderedExecutor != null) {
            orderedExecutor.shutdownNow();
            orderedExecutor = null;
        }
    }

    @Test
    public void testCanReadEntryFromMLedgerHappyPath() throws Exception {
        AtomicInteger entryCount = new AtomicInteger(0);
        Stack<Position> positions = new Stack<>();

        for (int i = 0; i < 150; i++) {
            ledger.addEntry(String.format("message-%d", i).getBytes(Encoding));
        }

        StreamingEntryReader streamingEntryReader =new StreamingEntryReader((ManagedCursorImpl) cursor,
                mockDispatcher, mockTopic);

        doAnswer((InvocationOnMock invocationOnMock) -> {
                Entry entry = invocationOnMock.getArgument(0, Entry.class);
                positions.push(entry.getPosition());
                assertEquals(new String(entry.getData()), String.format("message-%d", entryCount.getAndIncrement()));
                cursor.seek(ledger.getNextValidPosition((PositionImpl) entry.getPosition()));
                return null;
            }
        ).when(mockDispatcher).readEntryComplete(any(Entry.class), any(PendingReadEntryRequest.class));

        streamingEntryReader.asyncReadEntries(50, 700, null);
        await().until(() -> entryCount.get() == 50);
        // Check cursor's read position has been properly updated
        assertEquals(cursor.getReadPosition(), ledger.getNextValidPosition((PositionImpl) positions.peek()));
        streamingEntryReader.asyncReadEntries(50, 700, null);
        await().until(() -> entryCount.get() == 100);
        assertEquals(cursor.getReadPosition(), ledger.getNextValidPosition((PositionImpl) positions.peek()));
        streamingEntryReader.asyncReadEntries(50, 700, null);
        await().until(() -> entryCount.get() == 150);
        assertEquals(cursor.getReadPosition(), ledger.getNextValidPosition((PositionImpl) positions.peek()));
    }

    @Test
    public void testCanReadEntryFromMLedgerSizeExceededLimit() throws Exception {
        AtomicBoolean readComplete = new AtomicBoolean(false);
        Stack<Position> positions = new Stack<>();
        List<String> entries = new ArrayList<>();
        int size = "mmmmmmmmmmessage-0".getBytes().length;
        for (int i = 0; i < 15; i++) {
            ledger.addEntry(String.format("mmmmmmmmmmessage-%d", i).getBytes(Encoding));
        }

        StreamingEntryReader streamingEntryReader =
                new StreamingEntryReader((ManagedCursorImpl) cursor, mockDispatcher, mockTopic);

        doAnswer((InvocationOnMock invocationOnMock) -> {
                Entry entry = invocationOnMock.getArgument(0, Entry.class);
                positions.push(entry.getPosition());
                entries.add(new String(entry.getData()));
                cursor.seek(ledger.getNextValidPosition((PositionImpl) entry.getPosition()));
                return null;
            }
        ).when(mockDispatcher).readEntryComplete(any(Entry.class), any(PendingReadEntryRequest.class));

        doAnswer((InvocationOnMock invocationOnMock) -> {
                readComplete.set(true);
                return null;
            }
        ).when(mockDispatcher).canReadMoreEntries(anyBoolean());

        PositionImpl position = ledger.getPositionAfterN(ledger.getFirstPosition(), 3, ManagedLedgerImpl.PositionBound.startExcluded);
        // Make reading from mledger return out of order.
        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
                AsyncCallbacks.ReadEntryCallback cb = invocationOnMock.getArgument(1, AsyncCallbacks.ReadEntryCallback.class);
                executor.schedule(() -> {
                    cb.readEntryComplete(EntryImpl.create(position.getLedgerId(), position.getEntryId(), "mmmmmmmmmmessage-2".getBytes()),
                            invocationOnMock.getArgument(2));
                }, 200, TimeUnit.MILLISECONDS);
                return null;
            }
        }).when(ledger).asyncReadEntry(eq(position), any(), any());

        // Only 2 entries should be read with this request.
        streamingEntryReader.asyncReadEntries(6, size * 2 + 1, null);
        await().until(() -> readComplete.get());
        assertEquals(entries.size(), 2);
        // Assert cursor's read position has been properly updated to the third entry, since we should only read
        // 2 retries with previous request
        assertEquals(cursor.getReadPosition(), ledger.getNextValidPosition((PositionImpl) positions.peek()));
        reset(ledger);
        readComplete.set(false);
        streamingEntryReader.asyncReadEntries(6, size * 2 + 1, null);
        await().until(() -> readComplete.get());
        readComplete.set(false);
        streamingEntryReader.asyncReadEntries(6, size * 2 + 1, null);
        await().until(() -> readComplete.get());
        readComplete.set(false);
        streamingEntryReader.asyncReadEntries(6, size * 2 + 1, null);
        await().until(() -> readComplete.get());
        assertEquals(cursor.getReadPosition(), ledger.getNextValidPosition((PositionImpl) positions.peek()));
        assertEquals(entries.size(), 8);
        for (int i = 0; i < entries.size(); i++) {
            assertEquals(String.format("mmmmmmmmmmessage-%d", i), entries.get(i));
        }
    }

    @Test
    public void testCanReadEntryFromMLedgerWaitingForNewEntry() throws Exception {
        AtomicInteger entryCount = new AtomicInteger(0);
        AtomicBoolean entryProcessed = new AtomicBoolean(false);
        Stack<Position> positions = new Stack<>();
        List<String> entries = new ArrayList<>();
        for (int i = 0; i < 7; i++) {
            ledger.addEntry(String.format("message-%d", i).getBytes(Encoding));
        }

        StreamingEntryReader streamingEntryReader =
                new StreamingEntryReader((ManagedCursorImpl) cursor, mockDispatcher, mockTopic);

        doAnswer((InvocationOnMock invocationOnMock) -> {
                Entry entry = invocationOnMock.getArgument(0, Entry.class);
                positions.push(entry.getPosition());
                entries.add(new String(entry.getData()));
                entryCount.getAndIncrement();
                cursor.seek(ledger.getNextValidPosition((PositionImpl) entry.getPosition()));
                entryProcessed.set(true);
                return null;
            }
        ).when(mockDispatcher).readEntryComplete(any(Entry.class), any(PendingReadEntryRequest.class));

        streamingEntryReader.asyncReadEntries(5,  100, null);
        await().until(() -> entryCount.get() == 5);
        assertEquals(cursor.getReadPosition(), ledger.getNextValidPosition((PositionImpl) positions.peek()));
        streamingEntryReader.asyncReadEntries(5, 100, null);
        // We only write 7 entries initially so only 7 entries can be read.
        await().until(() -> entryCount.get() == 7);
        // Add new entry and await for it to be send to reader.
        entryProcessed.set(false);
        ledger.addEntry("message-7".getBytes(Encoding));
        await().until(() -> entryProcessed.get());
        assertEquals(entries.size(), 8);
        entryProcessed.set(false);
        ledger.addEntry("message-8".getBytes(Encoding));
        await().until(() -> entryProcessed.get());
        assertEquals(entries.size(), 9);
        entryProcessed.set(false);
        ledger.addEntry("message-9".getBytes(Encoding));
        await().until(() -> entryProcessed.get());
        assertEquals(entries.size(), 10);
        assertEquals(cursor.getReadPosition(), ledger.getNextValidPosition((PositionImpl) positions.peek()));
        for (int i = 0; i < entries.size(); i++) {
            assertEquals(String.format("message-%d", i), entries.get(i));
        }
    }

    @Test
    public void testCanCancelReadEntryRequestAndResumeReading() throws Exception {
        Map<Position, String> messages = new HashMap<>();
        AtomicInteger count = new AtomicInteger(0);
        Stack<Position> positions = new Stack<>();
        List<String> entries = new ArrayList<>();

        for (int i = 0; i < 20; i++) {
            String msg = String.format("message-%d", i);
            messages.put(ledger.addEntry(msg.getBytes(Encoding)), msg);
        }

        StreamingEntryReader streamingEntryReader =
                new StreamingEntryReader((ManagedCursorImpl) cursor, mockDispatcher, mockTopic);

        doAnswer((InvocationOnMock invocationOnMock) -> {
                Entry entry = invocationOnMock.getArgument(0, Entry.class);
                positions.push(entry.getPosition());
                entries.add(new String(entry.getData()));
                cursor.seek(ledger.getNextValidPosition((PositionImpl) entry.getPosition()));
                return null;
            }
        ).when(mockDispatcher).readEntryComplete(any(Entry.class), any(PendingReadEntryRequest.class));

        // Only return 5 entries
        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocationOnMock) {
                AsyncCallbacks.ReadEntryCallback cb = invocationOnMock.getArgument(1, AsyncCallbacks.ReadEntryCallback.class);
                PositionImpl position = invocationOnMock.getArgument(0, PositionImpl.class);
                int c = count.getAndIncrement();
                if (c < 5) {
                    cb.readEntryComplete(EntryImpl.create(position.getLedgerId(), position.getEntryId(),
                            messages.get(position).getBytes()),
                            invocationOnMock.getArgument(2));
                }
                return null;
            }
        }).when(ledger).asyncReadEntry(any(), any(), any());

        streamingEntryReader.asyncReadEntries(20,  200, null);
        streamingEntryReader.cancelReadRequests();
        await().until(() -> streamingEntryReader.getState() == StreamingEntryReader.State.Canceled);
        // Only have 5 entry as we make ledger only return 5 entries and cancel the request.
        assertEquals(entries.size(), 5);
        assertEquals(cursor.getReadPosition(), ledger.getNextValidPosition((PositionImpl) positions.peek()));
        // Clear mock and try to read remaining entries
        reset(ledger);
        streamingEntryReader.asyncReadEntries(15,  200, null);
        streamingEntryReader.cancelReadRequests();
        await().until(() -> streamingEntryReader.getState() == StreamingEntryReader.State.Completed);
        // Only have 5 entry as we make ledger only return 5 entries and cancel the request.
        assertEquals(entries.size(), 20);
        assertEquals(cursor.getReadPosition(), ledger.getNextValidPosition((PositionImpl) positions.peek()));
        // Make sure message still returned in order
        for (int i = 0; i < entries.size(); i++) {
            assertEquals(String.format("message-%d", i), entries.get(i));
        }
    }

    @Test
    public void testCanHandleExceptionAndRetry() throws Exception {
        Map<Position, String> messages = new HashMap<>();
        AtomicBoolean entryProcessed = new AtomicBoolean(false);
        AtomicInteger count = new AtomicInteger(0);
        Stack<Position> positions = new Stack<>();
        List<String> entries = new ArrayList<>();
        for (int i = 0; i < 12; i++) {
            String msg = String.format("message-%d", i);
            messages.put(ledger.addEntry(msg.getBytes(Encoding)), msg);
        }

        StreamingEntryReader streamingEntryReader =
                new StreamingEntryReader((ManagedCursorImpl) cursor, mockDispatcher, mockTopic);

        doAnswer((InvocationOnMock invocationOnMock) -> {
                Entry entry = invocationOnMock.getArgument(0, Entry.class);
                positions.push(entry.getPosition());
                entries.add(new String(entry.getData()));
                cursor.seek(ledger.getNextValidPosition((PositionImpl) entry.getPosition()));

                if (entries.size() == 6 || entries.size() == 12) {
                    entryProcessed.set(true);
                }
                return null;
            }
        ).when(mockDispatcher).readEntryComplete(any(Entry.class), any(PendingReadEntryRequest.class));

        // Make reading from mledger throw exception randomly.
        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
                AsyncCallbacks.ReadEntryCallback cb = invocationOnMock.getArgument(1, AsyncCallbacks.ReadEntryCallback.class);
                PositionImpl position = invocationOnMock.getArgument(0, PositionImpl.class);
                int c = count.getAndIncrement();
                if (c >= 3 && c < 5 || c >= 9 && c < 11) {
                    cb.readEntryFailed(new ManagedLedgerException.TooManyRequestsException("Fake exception."),
                            invocationOnMock.getArgument(2));
                } else {
                    cb.readEntryComplete(EntryImpl.create(position.getLedgerId(), position.getEntryId(),
                            messages.get(position).getBytes()),
                            invocationOnMock.getArgument(2));
                }
                return null;
            }
        }).when(ledger).asyncReadEntry(any(), any(), any());

        streamingEntryReader.asyncReadEntries(6,  100, null);
        await().until(() -> entryProcessed.get());
        assertEquals(entries.size(), 6);
        assertEquals(cursor.getReadPosition(), ledger.getNextValidPosition((PositionImpl) positions.peek()));
        entryProcessed.set(false);
        streamingEntryReader.asyncReadEntries(6, 100, null);
        await().until(() -> entryProcessed.get());
        assertEquals(entries.size(), 12);
        assertEquals(cursor.getReadPosition(), ledger.getNextValidPosition((PositionImpl) positions.peek()));
        // Make sure message still returned in order
        for (int i = 0; i < entries.size(); i++) {
            assertEquals(String.format("message-%d", i), entries.get(i));
        }
    }

    @Test
    public void testWillCancelReadAfterExhaustingRetry() throws Exception {
        Map<Position, String> messages = new HashMap<>();
        AtomicInteger count = new AtomicInteger(0);
        Stack<Position> positions = new Stack<>();
        List<String> entries = new ArrayList<>();
        for (int i = 0; i < 12; i++) {
            String msg = String.format("message-%d", i);
            messages.put(ledger.addEntry(msg.getBytes(Encoding)), msg);
        }

        StreamingEntryReader streamingEntryReader =
                new StreamingEntryReader((ManagedCursorImpl) cursor, mockDispatcher, mockTopic);

        doAnswer((InvocationOnMock invocationOnMock) -> {
                    Entry entry = invocationOnMock.getArgument(0, Entry.class);
                    positions.push(entry.getPosition());
                    cursor.seek(ledger.getNextValidPosition((PositionImpl) entry.getPosition()));
                    entries.add(new String(entry.getData()));
                    return null;
                }
        ).when(mockDispatcher).readEntryComplete(any(Entry.class), any(PendingReadEntryRequest.class));

        // Fail after first 3 read.
        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
                AsyncCallbacks.ReadEntryCallback cb = invocationOnMock.getArgument(1, AsyncCallbacks.ReadEntryCallback.class);
                PositionImpl position = invocationOnMock.getArgument(0, PositionImpl.class);
                int c = count.getAndIncrement();
                if (c >= 3) {
                    cb.readEntryFailed(new ManagedLedgerException.TooManyRequestsException("Fake exception."),
                            invocationOnMock.getArgument(2));
                } else {
                    cb.readEntryComplete(EntryImpl.create(position.getLedgerId(), position.getEntryId(),
                            messages.get(position).getBytes()),
                            invocationOnMock.getArgument(2));
                }
                return null;
            }
        }).when(ledger).asyncReadEntry(any(), any(), any());

        streamingEntryReader.asyncReadEntries(5,  100, null);
        await().until(() -> streamingEntryReader.getState() == StreamingEntryReader.State.Completed);
        // Issued 5 read, should only have 3 entries as others were canceled after exhausting retries.
        assertEquals(entries.size(), 3);
        for (int i = 0; i < entries.size(); i++) {
            assertEquals(String.format("message-%d", i), entries.get(i));
        }
        reset(ledger);
        streamingEntryReader.asyncReadEntries(5,  100, null);
        await().until(() -> streamingEntryReader.getState() == StreamingEntryReader.State.Completed);
        assertEquals(entries.size(), 8);
        for (int i = 0; i < entries.size(); i++) {
            assertEquals(String.format("message-%d", i), entries.get(i));
        }
    }

}
