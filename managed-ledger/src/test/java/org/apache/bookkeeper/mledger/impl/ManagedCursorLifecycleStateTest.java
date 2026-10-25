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
package org.apache.bookkeeper.mledger.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.common.util.ThreadBoundExecutor;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.impl.cache.EntryCache;
import org.apache.bookkeeper.mledger.impl.cache.EntryCacheManager;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/** Uses real cursor lifecycle methods and the real managed-ledger active-cursor container. */
public class ManagedCursorLifecycleStateTest {
    @DataProvider(name = "cacheEvictionModes")
    public Object[][] cacheEvictionModes() {
        return new Object[][]{{false}, {true}};
    }

    @DataProvider(name = "repeatedActivityStates")
    public Object[][] repeatedActivityStates() {
        return new Object[][]{{false, false}, {false, true}, {true, false}, {true, true}};
    }

    @Test(dataProvider = "repeatedActivityStates", timeOut = 15000)
    public void testRepeatedActivityRequestDoesNotWaitForContainerMonitor(boolean expectedReadCount, boolean active)
            throws Exception {
        Fixture fixture = createFixture(expectedReadCount);
        PausingLedger ledger = fixture.ledger();
        ledger.pauseFirstActivation.set(false);
        ManagedCursorImpl cursor = new ManagedCursorImpl(fixture.bookKeeper(), ledger, "repeated-request");
        if (active) {
            cursor.setActive();
        }
        CountDownLatch monitorHeld = new CountDownLatch(1);
        CountDownLatch releaseMonitor = new CountDownLatch(1);
        ExecutorService callers = Executors.newFixedThreadPool(2,
                new DefaultThreadFactory("cursor-repeated-activity", true));
        try {
            Future<?> holder = callers.submit(() -> {
                synchronized (ledger.getActiveCursors()) {
                    monitorHeld.countDown();
                    try {
                        if (!releaseMonitor.await(5, TimeUnit.SECONDS)) {
                            throw new AssertionError("the container monitor was not released");
                        }
                    } catch (InterruptedException error) {
                        Thread.currentThread().interrupt();
                        throw new AssertionError("the monitor holder was interrupted", error);
                    }
                }
            });
            assertThat(monitorHeld.await(3, TimeUnit.SECONDS)).isTrue();
            Future<?> request = callers.submit(() -> {
                if (active) {
                    cursor.setActive();
                } else {
                    cursor.setInactive();
                }
            });
            // A request for an already established state must finish while the unrelated holder owns the monitor.
            request.get(1, TimeUnit.SECONDS);
            releaseMonitor.countDown();
            holder.get(3, TimeUnit.SECONDS);
            assertThat(cursor.isActive()).isEqualTo(active);
            assertThat(ledger.isCursorActive(cursor)).isEqualTo(active);
        } finally {
            releaseMonitor.countDown();
            callers.shutdownNow();
            assertThat(callers.awaitTermination(3, TimeUnit.SECONDS)).as("repeated-state callers stopped").isTrue();
        }
    }

    @Test(dataProvider = "cacheEvictionModes", timeOut = 15000)
    public void testActivityFlagMatchesContainerAfterConcurrentActivation(boolean expectedReadCount) throws Exception {
        Fixture fixture = createFixture(expectedReadCount);
        PausingLedger ledger = fixture.ledger();
        ManagedCursorImpl cursor = new ManagedCursorImpl(fixture.bookKeeper(), ledger, "target");
        assertThat(cursor.isActive()).isFalse();

        ExecutorService callers = Executors.newFixedThreadPool(2, new DefaultThreadFactory("cursor-lifecycle"));
        try {
            Future<?> firstActivation = callers.submit(cursor::setActive);
            assertThat(ledger.firstActivationAdded.await(3, TimeUnit.SECONDS))
                    .as("the first activation reached the real container").isTrue();
            Future<?> activateThenDeactivate = callers.submit(() -> {
                cursor.setActive();
                cursor.setInactive();
            });
            try {
                activateThenDeactivate.get(1, TimeUnit.SECONDS);
            } catch (TimeoutException expected) {
                // A cursor-level lock may correctly serialize this caller behind the paused activation.
            } finally {
                ledger.resumeFirstActivation.countDown();
            }
            firstActivation.get(3, TimeUnit.SECONDS);
            activateThenDeactivate.get(3, TimeUnit.SECONDS);

            assertThat(cursor.isActive())
                    .as("cursor activity flag must agree with real active-cursor membership after both callers finish")
                    .isEqualTo(ledger.isCursorActive(cursor));
        } finally {
            ledger.resumeFirstActivation.countDown();
            callers.shutdownNow();
            assertThat(callers.awaitTermination(3, TimeUnit.SECONDS)).as("lifecycle callers stopped").isTrue();
        }
    }

    @Test(dataProvider = "cacheEvictionModes", timeOut = 15000)
    public void testAlwaysInactiveRemainsInactiveAfterConcurrentActivation(boolean expectedReadCount) throws Exception {
        Fixture fixture = createFixture(expectedReadCount);
        PausingLedger ledger = fixture.ledger();
        ledger.pauseFirstActivation.set(false);
        PausingInactiveCursor cursor = new PausingInactiveCursor(fixture.bookKeeper(), ledger);
        cursor.setActive();
        assertThat(ledger.isCursorActive(cursor)).isTrue();

        ExecutorService callers = Executors.newFixedThreadPool(2, new DefaultThreadFactory("cursor-always-inactive"));
        try {
            Future<?> alwaysInactive = callers.submit(cursor::setAlwaysInactive);
            assertThat(cursor.inactiveCompleted.await(3, TimeUnit.SECONDS))
                    .as("the real deactivation completed before the permanent-inactive transition returns").isTrue();
            Future<?> activation = callers.submit(cursor::setActive);
            try {
                activation.get(1, TimeUnit.SECONDS);
            } catch (TimeoutException expected) {
                // The permanent-inactive transition must serialize the competing activation.
            } finally {
                cursor.resumeInactive.countDown();
            }
            alwaysInactive.get(3, TimeUnit.SECONDS);
            activation.get(3, TimeUnit.SECONDS);

            assertThat(cursor.isActive()).as("always-inactive cursor must stay inactive").isFalse();
            assertThat(ledger.isCursorActive(cursor)).isFalse();
            cursor.setActive();
            assertThat(cursor.isActive()).isFalse();
            assertThat(ledger.isCursorActive(cursor)).isFalse();
        } finally {
            cursor.resumeInactive.countDown();
            callers.shutdownNow();
            assertThat(callers.awaitTermination(3, TimeUnit.SECONDS)).as("lifecycle callers stopped").isTrue();
        }
    }

    private static Fixture createFixture(boolean expectedReadCount) {
        BookKeeper bookKeeper = mock(BookKeeper.class);
        OrderedExecutor workerPool = mock(OrderedExecutor.class);
        when(bookKeeper.getMainWorkerPool()).thenReturn(workerPool);
        when(workerPool.chooseThread(any(Object.class))).thenReturn(mock(ThreadBoundExecutor.class));
        ManagedLedgerFactoryImpl factory = mock(ManagedLedgerFactoryImpl.class);
        EntryCacheManager cacheManager = mock(EntryCacheManager.class);
        when(factory.getEntryCacheManager()).thenReturn(cacheManager);
        when(cacheManager.getEntryCache(any())).thenReturn(mock(EntryCache.class));
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setCacheEvictionByExpectedReadCount(expectedReadCount);
        return new Fixture(bookKeeper, new PausingLedger(factory, bookKeeper, config));
    }

    private record Fixture(BookKeeper bookKeeper, PausingLedger ledger) {
    }

    private static final class PausingInactiveCursor extends ManagedCursorImpl {
        private final CountDownLatch inactiveCompleted = new CountDownLatch(1);
        private final CountDownLatch resumeInactive = new CountDownLatch(1);

        private PausingInactiveCursor(BookKeeper bookKeeper, ManagedLedgerImpl ledger) {
            super(bookKeeper, ledger, "always-inactive");
        }

        @Override
        public void setInactive() {
            super.setInactive();
            inactiveCompleted.countDown();
            try {
                if (!resumeInactive.await(5, TimeUnit.SECONDS)) {
                    throw new AssertionError("the permanent-inactive transition was not resumed");
                }
            } catch (InterruptedException error) {
                Thread.currentThread().interrupt();
                throw new AssertionError("deactivation caller interrupted", error);
            }
        }
    }

    private static final class PausingLedger extends ManagedLedgerImpl {
        private final AtomicBoolean pauseFirstActivation = new AtomicBoolean(true);
        private final CountDownLatch firstActivationAdded = new CountDownLatch(1);
        private final CountDownLatch resumeFirstActivation = new CountDownLatch(1);

        private PausingLedger(ManagedLedgerFactoryImpl factory, BookKeeper bookKeeper, ManagedLedgerConfig config) {
            super(factory, bookKeeper, null, config, null, "public/default/persistent/round3-cursor-lifecycle");
        }

        @Override
        public void activateCursor(ManagedCursor cursor) {
            super.activateCursor(cursor);
            // Pause after the production activation releases its monitor, before setActive writes its flag.
            if (pauseFirstActivation.compareAndSet(true, false)) {
                firstActivationAdded.countDown();
                try {
                    if (!resumeFirstActivation.await(5, TimeUnit.SECONDS)) {
                        throw new AssertionError("the first activation was not resumed");
                    }
                } catch (InterruptedException error) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError("activation caller interrupted", error);
                }
            }
        }
    }
}
