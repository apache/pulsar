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
package org.apache.pulsar.broker.delayed.bucket;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertFalse;
import io.netty.util.Timeout;
import io.netty.util.Timer;
import io.netty.util.TimerTask;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.util.Arrays;
import java.util.List;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.proto.ManagedLedgerInfo.LedgerInfo;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.pulsar.broker.delayed.AbstractDeliveryTrackerTest;
import org.apache.pulsar.broker.delayed.MockBucketSnapshotStorage;
import org.apache.pulsar.broker.delayed.MockManagedCursor;
import org.apache.pulsar.broker.service.persistent.AbstractPersistentDispatcherMultipleConsumers;
import org.awaitility.Awaitility;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class BucketDelayedDeliveryTrackerTest extends AbstractDeliveryTrackerTest {

    private BucketSnapshotStorage bucketSnapshotStorage;

    @AfterMethod
    public void clean() throws Exception {
        if (bucketSnapshotStorage != null) {
            bucketSnapshotStorage.close();
        }
    }

    @DataProvider(name = "delayedTracker")
    public Object[][] provider(Method method) throws Exception {
        dispatcher = mock(AbstractPersistentDispatcherMultipleConsumers.class);
        clock = mock(Clock.class);
        clockTime = new AtomicLong();
        when(clock.millis()).then(x -> clockTime.get());

        bucketSnapshotStorage = new MockBucketSnapshotStorage();
        bucketSnapshotStorage.start();
        ManagedCursor cursor = new MockManagedCursor("my_test_cursor");
        doReturn(cursor).when(dispatcher).getCursor();
        doReturn("persistent://public/default/testDelay" + " / " + cursor.getName()).when(dispatcher).getName();

        final String methodName = method.getName();
        return switch (methodName) {
            case "test" -> new Object[][]{{
                    new BucketDelayedDeliveryTracker(dispatcher, timer, 1, clock,
                            false, bucketSnapshotStorage, 5, TimeUnit.MILLISECONDS.toMillis(10), -1, 50)
            }};
            case "testWithTimer" -> {
                Timer timer = mock(Timer.class);

                AtomicLong clockTime = new AtomicLong();
                Clock clock = mock(Clock.class);
                when(clock.millis()).then(x -> clockTime.get());

                NavigableMap<Long, TimerTask> tasks = new TreeMap<>();

                when(timer.newTimeout(any(), anyLong(), any())).then(invocation -> {
                    TimerTask task = invocation.getArgument(0, TimerTask.class);
                    long timeout = invocation.getArgument(1, Long.class);
                    TimeUnit unit = invocation.getArgument(2, TimeUnit.class);
                    long scheduleAt = clockTime.get() + unit.toMillis(timeout);
                    tasks.put(scheduleAt, task);

                    Timeout t = mock(Timeout.class);
                    when(t.cancel()).then(i -> {
                        tasks.remove(scheduleAt, task);
                        return null;
                    });
                    return t;
                });

                yield new Object[][]{{
                        new BucketDelayedDeliveryTracker(dispatcher, timer, 1, clock,
                                false, bucketSnapshotStorage, 5, TimeUnit.MILLISECONDS.toMillis(10), -1, 50),
                        tasks
                }};
            }
            case "testAddWithinTickTime" -> new Object[][]{{
                    new BucketDelayedDeliveryTracker(dispatcher, timer, 100, clock,
                            false, bucketSnapshotStorage, 5, TimeUnit.MILLISECONDS.toMillis(10), -1, 50)
            }};
            case "testAddMessageWithStrictDelay" -> new Object[][]{{
                    new BucketDelayedDeliveryTracker(dispatcher, timer, 100, clock,
                            true, bucketSnapshotStorage, 5, TimeUnit.MILLISECONDS.toMillis(10), -1, 50)
            }};
            case "testAddMessageWithDeliverAtTimeAfterNowBeforeTickTimeFrequencyWithStrict" -> new Object[][]{{
                    new BucketDelayedDeliveryTracker(dispatcher, timer, 1000, clock,
                            true, bucketSnapshotStorage, 5, TimeUnit.MILLISECONDS.toMillis(10), -1, 50)
            }};
            case "testAddMessageWithDeliverAtTimeAfterNowAfterTickTimeFrequencyWithStrict", "testRecoverSnapshot" ->
                    new Object[][]{{
                            new BucketDelayedDeliveryTracker(dispatcher, timer, 100000, clock,
                                    true, bucketSnapshotStorage, 5, TimeUnit.MILLISECONDS.toMillis(10), -1, 50)
                    }};
            case "testAddMessageWithDeliverAtTimeAfterFullTickTimeWithStrict", "testExistDelayedMessage" ->
                    new Object[][]{{
                            new BucketDelayedDeliveryTracker(dispatcher, timer, 500, clock,
                                    true, bucketSnapshotStorage, 5, TimeUnit.MILLISECONDS.toMillis(10), -1, 50)
                    }};
            case "testMergeSnapshot", "testWithBkException", "testWithCreateFailDowngrade" -> new Object[][]{{
                    new BucketDelayedDeliveryTracker(dispatcher, timer, 100000, clock,
                            true, bucketSnapshotStorage, 5, TimeUnit.MILLISECONDS.toMillis(10), -1, 10)
            }};
            case "testMaxIndexesPerSegment" -> new Object[][]{{
                    new BucketDelayedDeliveryTracker(dispatcher, timer, 100000, clock,
                            true, bucketSnapshotStorage, 20, TimeUnit.HOURS.toMillis(1), 5, 100)
            }};
            case "testClear" -> new Object[][]{{
                    new BucketDelayedDeliveryTracker(dispatcher, timer, 100000, clock,
                            true, bucketSnapshotStorage, 1000, TimeUnit.MILLISECONDS.toMillis(100), -1, 50)
            }};
            default -> new Object[][]{{
                    new BucketDelayedDeliveryTracker(dispatcher, timer, 1, clock,
                            true, bucketSnapshotStorage, 1000, TimeUnit.MILLISECONDS.toMillis(100), -1, 50)
            }};
        };
    }

    @Test(dataProvider = "delayedTracker")
    public void testContainsMessage(BucketDelayedDeliveryTracker tracker) {
        tracker.addMessage(1, 1, 10);
        tracker.addMessage(2, 2, 20);

        assertTrue(tracker.containsMessage(1, 1));
        clockTime.set(20);

        Set<Position> scheduledMessages = tracker.getScheduledMessages(1);
        assertEquals(scheduledMessages.stream().findFirst().get().getEntryId(), 1);

        tracker.addMessage(3, 3, 30);

        tracker.addMessage(4, 4, 30);

        tracker.addMessage(5, 5, 30);

        tracker.addMessage(6, 6, 30);

        assertTrue(tracker.containsMessage(3, 3));

        tracker.close();
    }

    @Test(dataProvider = "delayedTracker", invocationCount = 10)
    public void testRecoverSnapshot(BucketDelayedDeliveryTracker tracker) throws Exception {
        for (int i = 1; i <= 100; i++) {
            tracker.addMessage(i, i, i * 10);
        }

        assertEquals(tracker.getNumberOfDelayedMessages(), 100);

        clockTime.set(1 * 10);

        Awaitility.await().untilAsserted(() -> {
            Assert.assertTrue(
                    tracker.getImmutableBuckets().asMapOfRanges().values().stream().noneMatch(x -> x.merging
                            || !x.getSnapshotCreateFuture().get().isDone()));
        });

        assertTrue(tracker.hasMessageAvailable());
        Set<Position> scheduledMessages = new TreeSet<>();
        Awaitility.await().untilAsserted(() -> {
            scheduledMessages.addAll(tracker.getScheduledMessages(100));
            assertEquals(scheduledMessages.size(), 1);
        });

        tracker.addMessage(101, 101, 101 * 10);

        tracker.close();

        clockTime.set(30 * 10);

        BucketDelayedDeliveryTracker tracker2 = new BucketDelayedDeliveryTracker(dispatcher, timer, 1000, clock,
                true, bucketSnapshotStorage, 5, TimeUnit.MILLISECONDS.toMillis(10), -1, 50);

        assertFalse(tracker2.containsMessage(101, 101));
        assertEquals(tracker2.getNumberOfDelayedMessages(), 70);

        clockTime.set(100 * 10);

        assertTrue(tracker2.hasMessageAvailable());
        Set<Position> scheduledMessages2 = new TreeSet<>();

        Awaitility.await().untilAsserted(() -> {
            scheduledMessages2.addAll(tracker2.getScheduledMessages(70));
            assertEquals(scheduledMessages2.size(), 70);
        });

        int i = 31;
        for (Position scheduledMessage : scheduledMessages2) {
            assertEquals(scheduledMessage, PositionFactory.create(i, i));
            i++;
        }

        tracker2.close();
    }

    @Test
    public void testRoaringBitmapSerialize() {
        List<Long> data = List.of(1L, 3L, 5L, 10L, 16L, 18L, 999L, 0L);
        RoaringBitmap roaringBitmap = new RoaringBitmap();
        for (Long datum : data) {
            roaringBitmap.add(datum, datum + 1);
        }

        assertEquals(roaringBitmap.getCardinality(), data.size());
        for (Long datum : data) {
            assertTrue(roaringBitmap.contains(datum, datum + 1));
        }

        byte[] array = new byte[roaringBitmap.serializedSizeInBytes()];
        roaringBitmap.serialize(ByteBuffer.wrap(array));

        RoaringBitmap roaringBitmap2 = new ImmutableRoaringBitmap(ByteBuffer.wrap(array)).toRoaringBitmap();
        assertEquals(roaringBitmap2.getCardinality(), data.size());
        for (Long datum : data) {
            assertTrue(roaringBitmap2.contains(datum, datum + 1));
        }

        byte[] array2 = new byte[roaringBitmap2.serializedSizeInBytes()];
        roaringBitmap.serialize(ByteBuffer.wrap(array2));

        assertTrue(Arrays.equals(array, array2));
        assertNotSame(array, array2);
    }

    @SuppressWarnings("deprecation")
    @Test(dataProvider = "delayedTracker")
    public void testMergeSnapshot(final BucketDelayedDeliveryTracker tracker) throws Exception {
        for (int i = 1; i <= 110; i++) {
            tracker.addMessage(i, i, i * 10);
            Awaitility.await().untilAsserted(() -> {
                Assert.assertTrue(
                        tracker.getImmutableBuckets().asMapOfRanges().values().stream().noneMatch(x -> x.merging));
            });
        }

        assertEquals(110, tracker.getNumberOfDelayedMessages());

        int size = tracker.getImmutableBuckets().asMapOfRanges().size();

        assertTrue(size <= 10);

        tracker.addMessage(111, 1011, 111 * 10);
        Awaitility.await().untilAsserted(() -> {
            Assert.assertTrue(
                    tracker.getImmutableBuckets().asMapOfRanges().values().stream().noneMatch(x -> x.merging));
        });

        MutableLong delayedMessagesInSnapshot = new MutableLong();
        tracker.getImmutableBuckets().asMapOfRanges().forEach((k, v) -> {
            delayedMessagesInSnapshot.add(v.getNumberBucketDelayedMessages());
        });

        tracker.close();

        BucketDelayedDeliveryTracker tracker2 = new BucketDelayedDeliveryTracker(dispatcher, timer, 1000, clock,
                true, bucketSnapshotStorage, 5, TimeUnit.MILLISECONDS.toMillis(10), -1, 10);

        assertEquals(tracker2.getNumberOfDelayedMessages(), delayedMessagesInSnapshot.getValue());

        for (int i = 1; i <= 110; i++) {
            tracker2.addMessage(i, i, i * 10);
        }

        clockTime.set(110 * 10);

        NavigableSet<Position> scheduledMessages = new TreeSet<>();
        Awaitility.await().untilAsserted(() -> {
            scheduledMessages.addAll(tracker2.getScheduledMessages(110));
            assertEquals(scheduledMessages.size(), 110);
        });
        for (int i = 1; i <= 110; i++) {
            Position position = scheduledMessages.pollFirst();
            assertEquals(position, PositionFactory.create(i, i));
        }

        tracker2.close();
    }

    @SuppressWarnings("deprecation")
    @Test(dataProvider = "delayedTracker")
    public void testWithBkException(final BucketDelayedDeliveryTracker tracker) throws Exception {
        MockBucketSnapshotStorage mockBucketSnapshotStorage = (MockBucketSnapshotStorage) bucketSnapshotStorage;
        mockBucketSnapshotStorage.injectCreateException(
                new BucketSnapshotPersistenceException("Bookie operation timeout, op: Create entry"));
        mockBucketSnapshotStorage.injectGetMetaDataException(
                new BucketSnapshotPersistenceException("Bookie operation timeout, op: Get entry"));
        mockBucketSnapshotStorage.injectGetSegmentException(
                new BucketSnapshotPersistenceException("Bookie operation timeout, op: Get entry"));
        mockBucketSnapshotStorage.injectDeleteException(
                new BucketSnapshotPersistenceException("Bookie operation timeout, op: Delete entry"));

        assertEquals(1, mockBucketSnapshotStorage.createExceptionQueue.size());
        assertEquals(1, mockBucketSnapshotStorage.getMetaDataExceptionQueue.size());
        assertEquals(1, mockBucketSnapshotStorage.getSegmentExceptionQueue.size());
        assertEquals(1, mockBucketSnapshotStorage.deleteExceptionQueue.size());

        for (int i = 1; i <= 110; i++) {
            tracker.addMessage(i, i, i * 10);
            Awaitility.await().untilAsserted(() -> {
                Assert.assertTrue(
                        tracker.getImmutableBuckets().asMapOfRanges().values().stream().noneMatch(x -> x.merging));
            });
        }

        assertEquals(110, tracker.getNumberOfDelayedMessages());

        int size = tracker.getImmutableBuckets().asMapOfRanges().size();

        assertTrue(size <= 10);

        tracker.addMessage(111, 1011, 111 * 10);
        Awaitility.await().untilAsserted(() -> {
            Assert.assertTrue(
                    tracker.getImmutableBuckets().asMapOfRanges().values().stream().noneMatch(x -> x.merging));
        });

        MutableLong delayedMessagesInSnapshot = new MutableLong();
        tracker.getImmutableBuckets().asMapOfRanges().forEach((k, v) -> {
            delayedMessagesInSnapshot.add(v.getNumberBucketDelayedMessages());
        });

        tracker.close();

        BucketDelayedDeliveryTracker tracker2 = new BucketDelayedDeliveryTracker(dispatcher, timer, 1000, clock,
                true, bucketSnapshotStorage, 5, TimeUnit.MILLISECONDS.toMillis(10), -1, 10);

        Long delayedMessagesInSnapshotValue = delayedMessagesInSnapshot.getValue();
        assertEquals(tracker2.getNumberOfDelayedMessages(), delayedMessagesInSnapshotValue);

        clockTime.set(110 * 10);

        mockBucketSnapshotStorage.injectGetSegmentException(
                new BucketSnapshotPersistenceException("Bookie operation timeout1, op: Get entry"));
        mockBucketSnapshotStorage.injectGetSegmentException(
                new BucketSnapshotPersistenceException("Bookie operation timeout2, op: Get entry"));
        mockBucketSnapshotStorage.injectGetSegmentException(
                new BucketSnapshotPersistenceException("Bookie operation timeout3, op: Get entry"));
        mockBucketSnapshotStorage.injectGetSegmentException(
                new BucketSnapshotPersistenceException("Bookie operation timeout4, op: Get entry"));

        assertEquals(tracker2.getScheduledMessages(100).size(), 0);

        Set<Position> scheduledMessages = new TreeSet<>();
        Awaitility.await().untilAsserted(() -> {
            scheduledMessages.addAll(tracker2.getScheduledMessages(100));
            assertEquals(scheduledMessages.size(), delayedMessagesInSnapshotValue);
        });

        assertTrue(mockBucketSnapshotStorage.createExceptionQueue.isEmpty());
        assertTrue(mockBucketSnapshotStorage.getMetaDataExceptionQueue.isEmpty());
        assertTrue(mockBucketSnapshotStorage.getSegmentExceptionQueue.isEmpty());
        assertTrue(mockBucketSnapshotStorage.deleteExceptionQueue.isEmpty());

        tracker2.close();
    }

    @Test(dataProvider = "delayedTracker")
    public void testWithCreateFailDowngrade(BucketDelayedDeliveryTracker tracker) {
        MockBucketSnapshotStorage mockBucketSnapshotStorage = (MockBucketSnapshotStorage) bucketSnapshotStorage;
        mockBucketSnapshotStorage.injectCreateException(
                new BucketSnapshotPersistenceException("Bookie operation timeout, op: Create entry"));
        mockBucketSnapshotStorage.injectCreateException(
                new BucketSnapshotPersistenceException("Bookie operation timeout, op: Create entry"));
        mockBucketSnapshotStorage.injectCreateException(
                new BucketSnapshotPersistenceException("Bookie operation timeout, op: Create entry"));
        mockBucketSnapshotStorage.injectCreateException(
                new BucketSnapshotPersistenceException("Bookie operation timeout, op: Create entry"));

        assertEquals(4, mockBucketSnapshotStorage.createExceptionQueue.size());

        for (int i = 1; i <= 6; i++) {
            tracker.addMessage(i, i, i * 10);
        }

        Awaitility.await().untilAsserted(() -> assertEquals(0, tracker.getImmutableBuckets().asMapOfRanges().size()));

        clockTime.set(5 * 10);

        assertEquals(6, tracker.getNumberOfDelayedMessages());

        NavigableSet<Position> scheduledMessages = tracker.getScheduledMessages(5);
        for (int i = 1; i <= 5; i++) {
            Position position = scheduledMessages.pollFirst();
            assertEquals(position, PositionFactory.create(i, i));
        }

        tracker.close();
    }

    @Test(dataProvider = "delayedTracker")
    public void testMaxIndexesPerSegment(BucketDelayedDeliveryTracker tracker) {
        for (int i = 1; i <= 101; i++) {
            tracker.addMessage(i, i, i * 10);
        }

        assertEquals(tracker.getImmutableBuckets().asMapOfRanges().size(), 5);

        tracker.getImmutableBuckets().asMapOfRanges().forEach((k, bucket) -> {
            assertEquals(bucket.getLastSegmentEntryId(), 4);
        });

        tracker.close();
    }

    @Test(dataProvider = "delayedTracker")
    public void testClear(BucketDelayedDeliveryTracker tracker)
            throws ExecutionException, InterruptedException, TimeoutException {
      for (int i = 1; i <= 1001; i++) {
          tracker.addMessage(i, i, i * 10);
      }

      assertEquals(tracker.getNumberOfDelayedMessages(), 1001);
      assertTrue(tracker.getImmutableBuckets().asMapOfRanges().size() > 0);
      assertEquals(tracker.getLastMutableBucket().size(), 1);

      tracker.clear().get(1, TimeUnit.MINUTES);

      assertEquals(tracker.getNumberOfDelayedMessages(), 0);
      assertEquals(tracker.getImmutableBuckets().asMapOfRanges().size(), 0);
      assertEquals(tracker.getLastMutableBucket().size(), 0);
      assertEquals(tracker.getSharedBucketPriorityQueue().size(), 0);

      tracker.close();
    }

    private static class TrackerWithStorage {
        final BucketDelayedDeliveryTracker tracker;
        final MockBucketSnapshotStorage storage;
        final AtomicLong clockTime;

        TrackerWithStorage(BucketDelayedDeliveryTracker tracker, MockBucketSnapshotStorage storage,
                           AtomicLong clockTime) {
            this.tracker = tracker;
            this.storage = storage;
            this.clockTime = clockTime;
        }

        void close() throws Exception {
            tracker.close();
            storage.close();
        }
    }

    private static class BlockingDeleteStorage extends MockBucketSnapshotStorage {
        final CompletableFuture<Void> firstDeleteFuture = new CompletableFuture<>();
        final AtomicLong deleteCalls = new AtomicLong();

        @Override
        public CompletableFuture<Void> deleteBucketSnapshot(long bucketId) {
            if (deleteCalls.incrementAndGet() <= 4) {
                return firstDeleteFuture;
            }
            return super.deleteBucketSnapshot(bucketId);
        }
    }

    private TrackerWithStorage createTrackerWithMockLedger(long firstLedgerId, int maxNumBuckets)
            throws Exception {
        return createTrackerWithMockLedger(firstLedgerId, maxNumBuckets, new MockBucketSnapshotStorage());
    }

    private TrackerWithStorage createTrackerWithMockLedger(long firstLedgerId, int maxNumBuckets,
                                                          MockBucketSnapshotStorage storage)
            throws Exception {
        storage.start();

        ManagedLedger mockLedger = mock(ManagedLedger.class);
        NavigableMap<Long, LedgerInfo> ledgerInfo = new TreeMap<>();
        ledgerInfo.put(firstLedgerId, mock(LedgerInfo.class));
        when(mockLedger.getLedgersInfo()).thenReturn(ledgerInfo);
        when(mockLedger.getName()).thenReturn("test-ledger");

        ManagedCursor mockCursor = new MockManagedCursor("test-cursor") {
            @Override
            public ManagedLedger getManagedLedger() {
                return mockLedger;
            }

            @Override
            public Position getMarkDeletedPosition() {
                return PositionFactory.create(firstLedgerId, -1);
            }
        };

        AbstractPersistentDispatcherMultipleConsumers disp =
                mock(AbstractPersistentDispatcherMultipleConsumers.class);
        Clock mockClock = mock(Clock.class);
        AtomicLong mockClockTime = new AtomicLong();
        when(mockClock.millis()).then(x -> mockClockTime.get());
        doReturn(mockCursor).when(disp).getCursor();
        doReturn("persistent://public/default/testDelay" + " / " + mockCursor.getName()).when(disp).getName();

        BucketDelayedDeliveryTracker tracker = new BucketDelayedDeliveryTracker(disp, mock(Timer.class),
                100000, mockClock, true, storage, 5, TimeUnit.MILLISECONDS.toMillis(10), -1, maxNumBuckets);
        return new TrackerWithStorage(tracker, storage, mockClockTime);
    }

    @Test
    public void testTrimRemovesOrphanedBuckets() throws Exception {
        long firstLedgerId = 31L;
        int messageCount = 36;
        TrackerWithStorage ts = createTrackerWithMockLedger(firstLedgerId, 5);

        for (int i = 1; i <= messageCount; i++) {
            ts.tracker.addMessage(i, i, i * 10);
        }
        Awaitility.await().untilAsserted(() ->
                Assert.assertTrue(ts.tracker.getImmutableBuckets().asMapOfRanges().values().stream()
                        .noneMatch(x -> x.merging)));

        int bucketCount = ts.tracker.getImmutableBuckets().asMapOfRanges().size();
        assertTrue(bucketCount <= 5,
                "Bucket count " + bucketCount + " should be <= maxNumBuckets=5 after trim+merge");

        ts.tracker.getImmutableBuckets().asMapOfRanges().forEach((range, bucket) ->
                assertTrue(range.lowerEndpoint() >= firstLedgerId,
                        "Remaining bucket range " + range + " should be >= " + firstLedgerId));

        long messagesAfterTrim = ts.tracker.getNumberOfDelayedMessages();
        ts.clockTime.set(messageCount * 10);
        NavigableSet<Position> scheduledMessages = ts.tracker.getScheduledMessages(1);
        assertTrue(scheduledMessages.stream().noneMatch(position -> position.getLedgerId() < firstLedgerId),
                "Trimmed ledgers should not be returned from the loaded shared queue");
        assertEquals(ts.tracker.getNumberOfDelayedMessages(), messagesAfterTrim - scheduledMessages.size());

        ts.close();
    }

    @Test
    public void testTrimHandlesDeleteFailure() throws Exception {
        long firstLedgerId = 50L;
        int messageCount = 31;
        TrackerWithStorage ts = createTrackerWithMockLedger(firstLedgerId, 5);

        // MaxRetryTimes=3 means the first trim delete attempt plus 3 retries = 4 exceptions consumed.
        for (int i = 0; i < 4; i++) {
            ts.storage.injectDeleteException(
                    new BucketSnapshotPersistenceException("Delete failed"));
        }

        for (int i = 1; i <= messageCount; i++) {
            ts.tracker.addMessage(i, i, i * 10);
        }
        Awaitility.await().untilAsserted(() ->
                Assert.assertTrue(ts.tracker.getImmutableBuckets().asMapOfRanges().values().stream()
                        .noneMatch(x -> x.merging)));

        Awaitility.await().untilAsserted(() ->
                assertTrue(ts.storage.deleteExceptionQueue.isEmpty(),
                        "Delete exception should have been consumed"));

        // Trim failed on the first orphaned bucket; the sequential chain stopped, so all
        // 6 orphaned buckets remain in immutableBuckets.
        assertTrue(ts.tracker.getImmutableBuckets().asMapOfRanges().size() > 0,
                "Orphaned buckets should remain when trim delete fails");
        ts.tracker.getImmutableBuckets().asMapOfRanges().forEach((range, bucket) ->
                assertTrue(range.upperEndpoint() < firstLedgerId,
                        "Remaining bucket " + range + " should be an orphaned bucket"));

        // numberDelayedMessages is unchanged because failed deletes do not decrement the count.
        assertEquals(ts.tracker.getNumberOfDelayedMessages(), messageCount);

        ts.close();
    }

    @Test
    public void testClearRunsAfterInFlightTrimFailure() throws Exception {
        long firstLedgerId = 50L;
        int messageCount = 31;
        BlockingDeleteStorage storage = new BlockingDeleteStorage();
        TrackerWithStorage ts = createTrackerWithMockLedger(firstLedgerId, 5, storage);

        for (int i = 1; i <= messageCount; i++) {
            ts.tracker.addMessage(i, i, i * 10);
        }
        Awaitility.await().untilAsserted(() ->
                assertTrue(storage.deleteCalls.get() > 0, "Trim delete should be in flight"));

        CompletableFuture<Void> clearFuture = ts.tracker.clear();
        storage.firstDeleteFuture.completeExceptionally(new BucketSnapshotPersistenceException("Delete failed"));

        clearFuture.get(1, TimeUnit.MINUTES);
        assertEquals(ts.tracker.getNumberOfDelayedMessages(), 0);
        assertEquals(ts.tracker.getImmutableBuckets().asMapOfRanges().size(), 0);
        assertEquals(ts.tracker.getLastMutableBucket().size(), 0);
        assertEquals(ts.tracker.getSharedBucketPriorityQueue().size(), 0);

        ts.close();
    }

    @Test
    public void testTrimWithNoOrphanedBuckets() throws Exception {
        TrackerWithStorage ts = createTrackerWithMockLedger(0L, 5);

        for (int i = 1; i <= 31; i++) {
            ts.tracker.addMessage(i, i, i * 10);
        }
        Awaitility.await().untilAsserted(() ->
                Assert.assertTrue(ts.tracker.getImmutableBuckets().asMapOfRanges().values().stream()
                        .noneMatch(x -> x.merging)));

        int bucketCount = ts.tracker.getImmutableBuckets().asMapOfRanges().size();
        assertTrue(bucketCount <= 5,
                "Bucket count " + bucketCount + " should be <= maxNumBuckets=5");
        assertTrue(bucketCount > 0, "Should have at least one bucket after merge");

        ts.close();
    }

    @Test
    public void testMergeEarlyReturnWhenWithinLimit() throws Exception {
        TrackerWithStorage ts = createTrackerWithMockLedger(0L, 50);

        for (int i = 1; i <= 30; i++) {
            ts.tracker.addMessage(i, i, i * 10);
        }
        Awaitility.await().untilAsserted(() ->
                Assert.assertTrue(ts.tracker.getImmutableBuckets().asMapOfRanges().values().stream()
                        .noneMatch(x -> x.merging)));

        int bucketCount = ts.tracker.getImmutableBuckets().asMapOfRanges().size();
        assertTrue(bucketCount < 50,
                "Bucket count " + bucketCount + " should be well below maxNumBuckets=50");

        long msgsBefore = ts.tracker.getNumberOfDelayedMessages();
        ts.tracker.addMessage(200, 200, 200 * 10);
        Awaitility.await().untilAsserted(() ->
                Assert.assertTrue(ts.tracker.getImmutableBuckets().asMapOfRanges().values().stream()
                        .noneMatch(x -> x.merging)));

        assertEquals(ts.tracker.getNumberOfDelayedMessages(), msgsBefore + 1);

        ts.close();
    }

    @Test
    public void testGetScheduledMessagesWhenAllOrphaned() throws Exception {
        // Reproduces IAE in nextDeliveryTime: when every delayed message lies below the
        // mark-delete position, the filter in getScheduledMessages pops the in-memory
        // messages without returning them. If the immutable bucket has additional messages
        // still in storage (later snapshot segments), numberDelayedMessages stays > 0
        // while both the mutable bucket and the shared priority queue are empty.
        // The trailing updateTimer -> nextDeliveryTime must not throw.
        long firstLedgerId = 50L;
        TrackerWithStorage ts = createTrackerWithMockLedger(firstLedgerId, 50);

        // Five delayed messages on the same orphaned ledger (ledgerId < firstLedgerId).
        // They share a mutable bucket because seal requires a strictly greater ledgerId.
        // Timestamps are 100ms apart so each lands in its own snapshot segment
        // (timeStep=10ms); only the first segment is loaded into the shared queue at seal.
        for (int i = 1; i <= 5; i++) {
            ts.tracker.addMessage(1, i, i * 100);
        }
        // A new orphaned ledgerId triggers the seal, producing immutable bucket [1..1]
        // with 5 messages across 5 segments; shared queue holds just the first segment.
        ts.tracker.addMessage(2, 1, 600);

        Awaitility.await().untilAsserted(() ->
                Assert.assertTrue(ts.tracker.getImmutableBuckets().asMapOfRanges().values().stream()
                        .noneMatch(x -> x.merging)));

        // In strict deliver-at mode getCutoffTime() is just clock.millis(), so advancing the
        // clock past the trigger message's deliverAt (600) is enough for
        // moveScheduledMessageToSharedQueue to flush the mutable bucket into the shared queue.
        ts.clockTime.set(700);

        // Both queues end up empty (filter pops the two in-memory messages), but
        // numberDelayedMessages is still 4 (segments 2..5 remain in storage).
        NavigableSet<Position> scheduledMessages = ts.tracker.getScheduledMessages(10);
        assertTrue(scheduledMessages.isEmpty(),
                "Orphaned messages should be filtered out, not returned");
        assertTrue(ts.tracker.getNumberOfDelayedMessages() > 0,
                "Remaining storage-only messages should keep the counter > 0");

        // hasMessageAvailable calls nextDeliveryTime while numberDelayedMessages > 0;
        // it must not throw IAE.
        assertFalse(ts.tracker.hasMessageAvailable());

        ts.close();
    }

    /**
     * Test that overlapping buckets are correctly cleaned up during recovery.
     * This verifies the fix for the subRangeMap clipped key issue where
     * putAndCleanOverlapRange would store clipped keys that couldn't be
     * removed by exact key matching in removeBucket().
     */
    @Test
    public void testOverlappingBucketsCleanupDuringRecovery() throws Exception {
        // Setup mocks
        AbstractPersistentDispatcherMultipleConsumers testDispatcher =
                mock(AbstractPersistentDispatcherMultipleConsumers.class);
        Clock testClock = mock(Clock.class);
        AtomicLong testClockTime = new AtomicLong();
        when(testClock.millis()).then(x -> testClockTime.get());

        MockBucketSnapshotStorage storage = new MockBucketSnapshotStorage();
        storage.start();

        ManagedCursor cursor = new MockManagedCursor("test_overlap_cursor");
        doReturn(cursor).when(testDispatcher).getCursor();
        doReturn("persistent://public/default/testOverlap / " + cursor.getName())
                .when(testDispatcher).getName();

        try {
            // Create first tracker with small minIndexCountPerBucket
            BucketDelayedDeliveryTracker tracker1 = new BucketDelayedDeliveryTracker(
                    testDispatcher, timer, 100000, testClock, true, storage,
                    3, TimeUnit.MILLISECONDS.toMillis(10), -1, 50);

            // Add messages to create multiple immutable buckets
            for (int i = 1; i <= 12; i++) {
                tracker1.addMessage(i, i, i * 10);
            }

            // Wait for all bucket operations to complete
            Awaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
                assertTrue(tracker1.getImmutableBuckets().asMapOfRanges().values().stream()
                        .noneMatch(x -> x.merging),
                        "All buckets should finish merging");
                assertTrue(tracker1.getImmutableBuckets().asMapOfRanges().size() >= 2,
                        "Should have created multiple buckets");
            });

            int bucketCountBeforeClose = tracker1.getImmutableBuckets().asMapOfRanges().size();

            tracker1.close();

            // Create second tracker - triggers recovery with putAndCleanOverlapRange
            BucketDelayedDeliveryTracker tracker2 = new BucketDelayedDeliveryTracker(
                    testDispatcher, timer, 100000, testClock, true, storage,
                    3, TimeUnit.MILLISECONDS.toMillis(10), -1, 50);

            // Verify buckets were recovered
            int bucketCountAfterRecovery = tracker2.getImmutableBuckets().asMapOfRanges().size();
            assertTrue(bucketCountAfterRecovery > 0, "Should have recovered buckets");

            // Key assertion: verify no orphaned buckets remain
            // If clipped keys weren't fixed, removeBucket() would fail and buckets would accumulate
            assertTrue(bucketCountAfterRecovery <= bucketCountBeforeClose,
                    String.format("Orphaned buckets detected: %d after recovery > %d before close",
                            bucketCountAfterRecovery, bucketCountBeforeClose));

            // Verify messages were recovered
            assertTrue(tracker2.getNumberOfDelayedMessages() > 0,
                    "Should have recovered messages");

            // Verify snapshot length tracking is correct
            long totalSnapshotLength = tracker2.getImmutableBuckets().asMapOfRanges().values().stream()
                    .mapToLong(ImmutableBucket::getSnapshotLength)
                    .sum();
            assertTrue(totalSnapshotLength >= 0,
                    "Snapshot length tracking broken - likely due to failed removeBucket()");

            tracker2.close();
        } finally {
            storage.clean();
        }
    }

    /**
     * Test that putAndCleanOverlapRange correctly uses original keys instead of truncated keys
     * when checking if a new range encloses existing buckets.
     *
     * This prevents the bug where a truncated key from subRangeMap() would incorrectly pass
     * the encloses() check, causing a bucket to be replaced when it shouldn't be.
     */
    @Test
    public void testPutAndCleanOverlapRangeWithTruncatedKeys() throws Exception {
        // Setup mocks
        AbstractPersistentDispatcherMultipleConsumers testDispatcher =
                mock(AbstractPersistentDispatcherMultipleConsumers.class);
        Clock testClock = mock(Clock.class);
        AtomicLong testClockTime = new AtomicLong();
        when(testClock.millis()).then(x -> testClockTime.get());

        MockBucketSnapshotStorage storage = new MockBucketSnapshotStorage();
        storage.start();

        ManagedCursor cursor = new MockManagedCursor("test_truncated_cursor");
        doReturn(cursor).when(testDispatcher).getCursor();
        doReturn("persistent://public/default/testTruncated / " + cursor.getName())
                .when(testDispatcher).getName();

        try {
            // Create tracker
            BucketDelayedDeliveryTracker tracker = new BucketDelayedDeliveryTracker(
                    testDispatcher, timer, 100000, testClock, true, storage,
                    3, TimeUnit.MILLISECONDS.toMillis(10), -1, 50);

            // Add messages to create a bucket [1-6]
            for (int i = 1; i <= 6; i++) {
                tracker.addMessage(i, i, i * 10);
            }

            // Wait for bucket to be created
            Awaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
                assertTrue(tracker.getImmutableBuckets().asMapOfRanges().size() >= 1,
                        "Should have created at least one bucket");
            });

            int initialBucketCount = tracker.getImmutableBuckets().asMapOfRanges().size();
            long initialMessageCount = tracker.getNumberOfDelayedMessages();

            // Now add messages that would create a bucket [7-9]
            // This should NOT replace the existing bucket [1-6]
            for (int i = 7; i <= 9; i++) {
                tracker.addMessage(i, i, i * 10);
            }

            // Wait for new bucket operations
            Awaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
                assertTrue(tracker.getImmutableBuckets().asMapOfRanges().values().stream()
                        .noneMatch(x -> x.merging),
                        "All buckets should finish processing");
            });

            // Verify bucket count increased (or stayed same if they got merged)
            int finalBucketCount = tracker.getImmutableBuckets().asMapOfRanges().size();
            assertTrue(finalBucketCount >= initialBucketCount,
                    "Bucket count should not decrease when adding non-overlapping ranges");

            // Verify all messages are tracked
            long finalMessageCount = tracker.getNumberOfDelayedMessages();
            assertTrue(finalMessageCount >= initialMessageCount,
                    String.format("Message count should not decrease: initial=%d, final=%d",
                            initialMessageCount, finalMessageCount));

            // Verify no bucket was incorrectly replaced
            // If putAndCleanOverlapRange used truncated keys, it might have incorrectly
            // removed a bucket that shouldn't have been removed
            tracker.getImmutableBuckets().asMapOfRanges().forEach((range, bucket) -> {
                assertTrue(bucket.getNumberBucketDelayedMessages() > 0,
                        "All buckets should have messages - bucket " + range + " is empty");
            });

            tracker.close();
        } finally {
            storage.clean();
        }
    }

    @Test
    public void testLateSnapshotLengthUpdateAfterClearDoesNotInflateCounter() throws Exception {
        AbstractPersistentDispatcherMultipleConsumers testDispatcher =
                mock(AbstractPersistentDispatcherMultipleConsumers.class);
        Clock testClock = mock(Clock.class);
        AtomicLong testClockTime = new AtomicLong();
        when(testClock.millis()).then(x -> testClockTime.get());

        MockBucketSnapshotStorage storage = new MockBucketSnapshotStorage();
        storage.start();
        MockBucketSnapshotStorage spyStorage = spy(storage);

        CompletableFuture<Long> blockedLength = new CompletableFuture<>();
        when(spyStorage.getBucketSnapshotLength(anyLong())).thenReturn(blockedLength);

        ManagedCursor cursor = new MockManagedCursor("test_late_update_cursor");
        doReturn(cursor).when(testDispatcher).getCursor();
        doReturn("persistent://public/default/testLateUpdate / " + cursor.getName())
                .when(testDispatcher).getName();

        try {
            BucketDelayedDeliveryTracker tracker = new BucketDelayedDeliveryTracker(
                    testDispatcher, timer, 100000, testClock, true, spyStorage,
                    3, TimeUnit.MILLISECONDS.toMillis(10), -1, 50);

            for (int i = 1; i <= 6; i++) {
                tracker.addMessage(i, i, i * 10);
            }

            Awaitility.await().untilAsserted(() ->
                    assertTrue(tracker.getBucketsCount().get() >= 1,
                            "Should have created at least one immutable bucket"));
            assertCountersConsistent(tracker);

            tracker.clear();

            assertEquals(tracker.getBucketsCount().get(), 0, "All buckets should be removed");
            assertCountersConsistent(tracker);

            blockedLength.complete(999_999L);

            Awaitility.await().untilAsserted(() -> {
                assertEquals(tracker.getTotalSnapshotLengthBytes().get(), 0,
                        "Late length update inflated totalSnapshotLengthBytes after clear");
            });

            tracker.close();
        } finally {
            storage.clean();
        }
    }

    @Test
    public void testLateSnapshotLengthUpdateAfterTrimDoesNotInflateCounter() throws Exception {
        AbstractPersistentDispatcherMultipleConsumers testDispatcher =
                mock(AbstractPersistentDispatcherMultipleConsumers.class);
        Clock testClock = mock(Clock.class);
        AtomicLong testClockTime = new AtomicLong();
        when(testClock.millis()).then(x -> testClockTime.get());

        MockBucketSnapshotStorage storage = new MockBucketSnapshotStorage();
        storage.start();
        MockBucketSnapshotStorage spyStorage = spy(storage);

        CompletableFuture<Long> blockedLength = new CompletableFuture<>();
        when(spyStorage.getBucketSnapshotLength(anyLong())).thenReturn(blockedLength);

        ManagedCursor spyCursor = spy(new MockManagedCursor("test_late_trim_cursor"));
        AtomicLong markDeletedLedger = new AtomicLong(0);
        when(spyCursor.getMarkDeletedPosition()).thenAnswer(inv ->
                PositionFactory.create(markDeletedLedger.get(), 0));
        ManagedLedger mockLedger = mock(ManagedLedger.class);
        when(mockLedger.getName()).thenReturn("test_ledger");
        when(spyCursor.getManagedLedger()).thenReturn(mockLedger);

        doReturn(spyCursor).when(testDispatcher).getCursor();
        doReturn("persistent://public/default/testLateTrim / " + spyCursor.getName())
                .when(testDispatcher).getName();

        try {
            BucketDelayedDeliveryTracker tracker = new BucketDelayedDeliveryTracker(
                    testDispatcher, timer, 100000, testClock, true, spyStorage,
                    3, TimeUnit.MILLISECONDS.toMillis(10), -1, 3);

            for (int i = 1; i <= 12; i++) {
                tracker.addMessage(i, i, i * 10);
            }

            Awaitility.await().untilAsserted(() ->
                    assertTrue(tracker.getBucketsCount().get() >= 3,
                            "Should have created at least 3 immutable buckets"));
            assertCountersConsistent(tracker);

            markDeletedLedger.set(5);

            for (int i = 13; i <= 15; i++) {
                tracker.addMessage(i, i, i * 10);
            }

            Awaitility.await().untilAsserted(() -> {
                boolean hasOldBucket = tracker.getImmutableBuckets().asMapOfRanges().keySet().stream()
                        .anyMatch(r -> r.upperEndpoint() < 5);
                Assert.assertFalse(hasOldBucket, "Buckets with endLedgerId < 5 should be trimmed");
            });
            assertCountersConsistent(tracker);

            blockedLength.complete(999_999L);

            Awaitility.await().untilAsserted(() ->
                    assertCountersConsistent(tracker));

            tracker.close();
        } finally {
            storage.clean();
        }
    }

    private static void assertCountersConsistent(BucketDelayedDeliveryTracker tracker) {
        int liveBucketCount = tracker.getImmutableBuckets().asMapOfRanges().size();
        long liveSnapshotLength = tracker.getImmutableBuckets().asMapOfRanges().values().stream()
                .mapToLong(ImmutableBucket::getSnapshotLength)
                .sum();

        assertEquals(tracker.getBucketsCount().get(), liveBucketCount,
                String.format("bucketsCount drift: cached=%d live=%d",
                        tracker.getBucketsCount().get(), liveBucketCount));
        assertEquals(tracker.getTotalSnapshotLengthBytes().get(), liveSnapshotLength,
                String.format("totalSnapshotLengthBytes drift: cached=%d live=%d",
                        tracker.getTotalSnapshotLengthBytes().get(), liveSnapshotLength));
    }
}
