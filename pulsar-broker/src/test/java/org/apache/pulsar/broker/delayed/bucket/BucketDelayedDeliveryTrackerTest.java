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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
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
                    tracker.getImmutableBuckets().asMapOfRanges().values().stream().noneMatch(x -> x.merging ||
                            !x.getSnapshotCreateFuture().get().isDone()));
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
                true, bucketSnapshotStorage, 5, TimeUnit.MILLISECONDS.toMillis(10), -1,10);

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
}
