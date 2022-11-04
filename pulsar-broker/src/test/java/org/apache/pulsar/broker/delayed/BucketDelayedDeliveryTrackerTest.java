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
package org.apache.pulsar.broker.delayed;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import io.netty.util.Timeout;
import io.netty.util.Timer;
import io.netty.util.TimerTask;
import java.lang.reflect.Method;
import java.time.Clock;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.delayed.bucket.BucketDelayedDeliveryTracker;
import org.apache.pulsar.broker.delayed.bucket.BucketSnapshotStorage;
import org.apache.pulsar.broker.service.persistent.PersistentDispatcherMultipleConsumers;
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
        dispatcher = mock(PersistentDispatcherMultipleConsumers.class);
        clock = mock(Clock.class);
        clockTime = new AtomicLong();
        when(clock.millis()).then(x -> clockTime.get());

        bucketSnapshotStorage = new MockBucketSnapshotStorage();
        bucketSnapshotStorage.start();
        ManagedCursor cursor = new MockManagedCursor("my_test_cursor");
        doReturn(cursor).when(dispatcher).getCursor();

        final String methodName = method.getName();
        return switch (methodName) {
            case "test" -> new Object[][]{{
                    new BucketDelayedDeliveryTracker(dispatcher, timer, 1, clock,
                            false, bucketSnapshotStorage, 5, TimeUnit.MILLISECONDS.toMillis(10), 50)
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
                                false, bucketSnapshotStorage, 5, TimeUnit.MILLISECONDS.toMillis(10), 50),
                        tasks
                }};
            }
            case "testAddWithinTickTime" -> new Object[][]{{
                    new BucketDelayedDeliveryTracker(dispatcher, timer, 100, clock,
                            false, bucketSnapshotStorage, 5, TimeUnit.MILLISECONDS.toMillis(10), 50)
            }};
            case "testAddMessageWithStrictDelay" -> new Object[][]{{
                    new BucketDelayedDeliveryTracker(dispatcher, timer, 100, clock,
                            true, bucketSnapshotStorage, 5, TimeUnit.MILLISECONDS.toMillis(10), 50)
            }};
            case "testAddMessageWithDeliverAtTimeAfterNowBeforeTickTimeFrequencyWithStrict" -> new Object[][]{{
                    new BucketDelayedDeliveryTracker(dispatcher, timer, 1000, clock,
                            true, bucketSnapshotStorage, 5, TimeUnit.MILLISECONDS.toMillis(10), 50)
            }};
            case "testAddMessageWithDeliverAtTimeAfterNowAfterTickTimeFrequencyWithStrict", "testRecoverSnapshot" ->
                    new Object[][]{{
                            new BucketDelayedDeliveryTracker(dispatcher, timer, 100000, clock,
                                    true, bucketSnapshotStorage, 5, TimeUnit.MILLISECONDS.toMillis(10), 50)
                    }};
            case "testAddMessageWithDeliverAtTimeAfterFullTickTimeWithStrict", "testExistDelayedMessage" ->
                    new Object[][]{{
                            new BucketDelayedDeliveryTracker(dispatcher, timer, 500, clock,
                                    true, bucketSnapshotStorage, 5, TimeUnit.MILLISECONDS.toMillis(10), 50)
                    }};
            default -> new Object[][]{{
                    new BucketDelayedDeliveryTracker(dispatcher, timer, 1, clock,
                            true, bucketSnapshotStorage, 1000, TimeUnit.MILLISECONDS.toMillis(100), 50)
            }};
        };
    }

    @Test(dataProvider = "delayedTracker")
    public void testContainsMessage(DelayedDeliveryTracker tracker) {
        tracker.addMessage(1, 1, 10);
        tracker.addMessage(2, 2, 20);

        assertTrue(tracker.containsMessage(1, 1));
        clockTime.set(20);

        Set<PositionImpl> scheduledMessages = tracker.getScheduledMessages(1);
        assertEquals(scheduledMessages.stream().findFirst().get().getEntryId(), 1);

        tracker.addMessage(3, 3, 30);

        tracker.addMessage(4, 4, 30);

        tracker.addMessage(5, 5, 30);

        tracker.addMessage(6, 6, 30);

        assertTrue(tracker.containsMessage(3, 3));

        tracker.close();
    }
}
