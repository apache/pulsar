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
import static org.assertj.core.api.AssertionsForClassTypes.within;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedCursor.IndividualDeletedEntries;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.pulsar.broker.testcontext.PulsarTestContext;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tests the per-subscription backlog age quantiles against a fake cursor, so no BookKeeper is needed.
 */
@Test(groups = "broker")
public class PersistentTopicBacklogAgeQuantilesTest {
    private static final String SUBSCRIPTION = "sub-1";
    private static final long SECOND_MILLIS = 1000L;

    private PulsarTestContext pulsarTestContext;
    private PersistentTopic topic;
    private long baseTimeMillis;

    @BeforeMethod(alwaysRun = true)
    public void setup() throws Exception {
        pulsarTestContext = PulsarTestContext.builderForNonStartableContext().build();
        ManagedLedger ledger = mock(ManagedLedger.class);
        doReturn(new ManagedLedgerConfig()).when(ledger).getConfig();
        topic = new PersistentTopic("persistent://prop/ns-abc/quantiles-" + UUID.randomUUID(), ledger,
                pulsarTestContext.getBrokerService());
        baseTimeMillis = System.currentTimeMillis();
    }

    @AfterMethod(alwaysRun = true)
    public void shutdown() throws Exception {
        if (pulsarTestContext != null) {
            pulsarTestContext.close();
            pulsarTestContext = null;
        }
    }

    /**
     * Fake cursor over a list of unacknowledged entries, oldest first, where the nth entry is the nth element.
     * The backlog count follows the list size, so removing elements models acks.
     */
    private static class FakeCursor {
        final ManagedCursor cursor = mock(ManagedCursor.class);
        final List<Entry> unacked = new CopyOnWriteArrayList<>();
        final List<Integer> requestedN = new CopyOnWriteArrayList<>();
        final List<IndividualDeletedEntries> requestedModes = new CopyOnWriteArrayList<>();

        FakeCursor() {
            doReturn(true).when(cursor).isDurable();
            doAnswer(invocation -> (long) unacked.size()).when(cursor).getNumberOfEntriesInBacklog(anyBoolean());
            doAnswer(invocation -> {
                int n = invocation.getArgument(0);
                requestedModes.add(invocation.getArgument(1));
                requestedN.add(n);
                AsyncCallbacks.ReadEntryCallback callback = invocation.getArgument(2);
                // Like ManagedCursorImpl, report a null entry when fewer than n entries are in the backlog.
                callback.readEntryComplete(n <= unacked.size() ? unacked.get(n - 1) : null, null);
                return null;
            }).when(cursor).asyncGetNthEntry(anyInt(), any(), any(), any());
        }
    }

    private Entry entryAgedSeconds(long ageSeconds) {
        Entry entry = mock(Entry.class);
        doReturn(baseTimeMillis - ageSeconds * SECOND_MILLIS).when(entry).getEntryTimestamp();
        return entry;
    }

    /** Adds a backlog of {@code count} entries, oldest first, aged {@code count * 10} down to {@code 10} seconds. */
    private FakeCursor addSubscriptionWithBacklog(String name, int count) {
        FakeCursor fake = new FakeCursor();
        for (int i = 1; i <= count; i++) {
            fake.unacked.add(entryAgedSeconds((count - i + 1) * 10L));
        }
        addSubscription(name, fake);
        return fake;
    }

    private void addSubscription(String name, FakeCursor fake) {
        PersistentSubscription subscription = mock(PersistentSubscription.class);
        doReturn(fake.cursor).when(subscription).getCursor();
        topic.getSubscriptions().put(name, subscription);
    }

    private void configureQuantiles(Double... quantiles) {
        pulsarTestContext.getConfig().setSubscriptionBacklogAgeQuantiles(List.of(quantiles));
    }

    private void refresh() throws Exception {
        topic.updateSubscriptionBacklogAgeQuantiles(true).get(5, TimeUnit.SECONDS);
    }

    private Map<Double, Long> ages() {
        return topic.getBestEffortBacklogAgeQuantileSeconds(SUBSCRIPTION);
    }

    @Test
    public void quantilesMapToTheExpectedEntries() throws Exception {
        configureQuantiles(0.0, 0.5, 0.99, 1.0);
        // Entry i (1-based, oldest first) is (11 - i) * 10 seconds old: 100s, 90s, ... 10s.
        FakeCursor fake = addSubscriptionWithBacklog(SUBSCRIPTION, 10);

        refresh();

        // n = ceil(10 * (1 - q)): q=0.0 -> 10th (10s), q=0.5 -> 5th (60s), q=0.99 and q=1.0 -> the oldest (100s).
        assertThat(fake.requestedN).containsExactlyInAnyOrder(10, 5, 1, 1);
        assertThat(ages()).containsOnlyKeys(0.0, 0.5, 0.99, 1.0);
        assertThat(ages().get(0.0)).isCloseTo(10L, within(2L));
        assertThat(ages().get(0.5)).isCloseTo(60L, within(2L));
        assertThat(ages().get(0.99)).isCloseTo(100L, within(2L));
        assertThat(ages().get(1.0)).isCloseTo(100L, within(2L));
    }

    @Test
    public void quantileIndexIsNotThrownOffByFloatingPointError() throws Exception {
        // 10 * (1 - 0.7) is 3.0000000000000004 in floating point, which a plain ceil would round up to the 4th entry.
        configureQuantiles(0.7);
        FakeCursor fake = addSubscriptionWithBacklog(SUBSCRIPTION, 10);

        refresh();

        assertThat(fake.requestedN).containsExactly(3);
        assertThat(ages().get(0.7)).isCloseTo(80L, within(2L));
    }

    @Test
    public void ackedEntriesAreExcludedFromTheCountAndTheLookup() throws Exception {
        configureQuantiles(0.5);
        FakeCursor fake = addSubscriptionWithBacklog(SUBSCRIPTION, 10);
        // Ack the oldest five, leaving the entries that were 50s, 40s, 30s, 20s and 10s old.
        for (int i = 0; i < 5; i++) {
            fake.unacked.remove(0);
        }

        refresh();

        // 5 entries left, so n = ceil(5 * 0.5) = 3, the 3rd unacked entry, which was 30s old.
        assertThat(fake.requestedN).containsExactly(3);
        assertThat(fake.requestedModes).containsOnly(IndividualDeletedEntries.Exclude);
        assertThat(ages().get(0.5)).isCloseTo(30L, within(2L));
    }

    @Test
    public void singleEntryBacklogReturnsThatEntryForEveryQuantile() throws Exception {
        configureQuantiles(0.0, 0.5, 0.99);
        FakeCursor fake = addSubscriptionWithBacklog(SUBSCRIPTION, 1);

        refresh();

        assertThat(fake.requestedN).containsOnly(1);
        assertThat(ages()).containsOnlyKeys(0.0, 0.5, 0.99);
        assertThat(ages().values()).allSatisfy(age -> assertThat(age).isCloseTo(10L, within(2L)));
    }

    @Test
    public void backlogShrinkingBetweenCountAndReadLeavesQuantileUnset() throws Exception {
        configureQuantiles(0.5);
        FakeCursor fake = new FakeCursor();
        fake.unacked.add(entryAgedSeconds(10));
        addSubscription(SUBSCRIPTION, fake);
        // The count says 10 entries, but only one is still there by the time the nth entry is read.
        doReturn(10L).when(fake.cursor).getNumberOfEntriesInBacklog(anyBoolean());

        refresh();

        assertThat(fake.requestedN).containsExactly(5);
        assertThat(ages()).isEmpty();
    }

    @Test
    public void readFailureLeavesQuantileUnsetAndDoesNotFailTheRefresh() throws Exception {
        configureQuantiles(0.5, 0.99);
        FakeCursor fake = addSubscriptionWithBacklog(SUBSCRIPTION, 10);
        doAnswer(invocation -> {
            AsyncCallbacks.ReadEntryCallback callback = invocation.getArgument(2);
            callback.readEntryFailed(new ManagedLedgerException("read failed"), null);
            return null;
        }).when(fake.cursor).asyncGetNthEntry(anyInt(), any(), any(), any());

        refresh();

        assertThat(ages()).isEmpty();
    }

    @Test
    public void synchronousReadErrorLeavesQuantileUnsetAndDoesNotFailTheRefresh() throws Exception {
        configureQuantiles(0.5);
        FakeCursor fake = addSubscriptionWithBacklog(SUBSCRIPTION, 10);
        doThrow(new IllegalStateException("cursor closed")).when(fake.cursor)
                .asyncGetNthEntry(anyInt(), any(), any(), any());

        refresh();

        assertThat(ages()).isEmpty();
    }

    @Test
    public void entryIsReleasedAfterItsTimestampIsRead() throws Exception {
        configureQuantiles(0.5);
        FakeCursor fake = addSubscriptionWithBacklog(SUBSCRIPTION, 10);

        refresh();

        verify(fake.unacked.get(4)).release();
    }

    @Test
    public void subscriptionsWithoutBacklogAreNotRead() throws Exception {
        configureQuantiles(0.5);
        FakeCursor fake = addSubscriptionWithBacklog(SUBSCRIPTION, 0);

        refresh();

        verify(fake.cursor, never()).asyncGetNthEntry(anyInt(), any(), any(), any());
        assertThat(ages()).isEmpty();
    }

    @Test
    public void nonDurableCursorsAreNotRead() throws Exception {
        configureQuantiles(0.5);
        FakeCursor fake = addSubscriptionWithBacklog(SUBSCRIPTION, 10);
        doReturn(false).when(fake.cursor).isDurable();

        refresh();

        verify(fake.cursor, never()).asyncGetNthEntry(anyInt(), any(), any(), any());
        assertThat(ages()).isEmpty();
    }

    @Test
    public void disabledWhenNoQuantilesAreConfigured() throws Exception {
        configureQuantiles();
        FakeCursor fake = addSubscriptionWithBacklog(SUBSCRIPTION, 10);

        refresh();

        verify(fake.cursor, never()).asyncGetNthEntry(anyInt(), any(), any(), any());
        assertThat(ages()).isEmpty();
    }

    @Test
    public void disabledWithoutPreciseBacklogCheck() throws Exception {
        configureQuantiles(0.5);
        FakeCursor fake = addSubscriptionWithBacklog(SUBSCRIPTION, 10);

        topic.updateSubscriptionBacklogAgeQuantiles(false).get(5, TimeUnit.SECONDS);

        verify(fake.cursor, never()).asyncGetNthEntry(anyInt(), any(), any(), any());
        assertThat(ages()).isEmpty();
    }

    @Test
    public void outOfRangeQuantilesAreIgnored() throws Exception {
        configureQuantiles(-0.1, 1.5, Double.NaN, 0.5);
        FakeCursor fake = addSubscriptionWithBacklog(SUBSCRIPTION, 10);

        refresh();

        assertThat(fake.requestedN).containsExactly(5);
        assertThat(ages()).containsOnlyKeys(0.5);
    }

    @Test
    public void subscriptionsAreTrackedIndependently() throws Exception {
        configureQuantiles(0.5);
        addSubscriptionWithBacklog(SUBSCRIPTION, 10);
        addSubscriptionWithBacklog("sub-2", 4);

        refresh();

        // sub-1: 10 entries, n = 5, 60s old. sub-2: 4 entries aged 40s..10s, n = 2, 30s old.
        assertThat(ages().get(0.5)).isCloseTo(60L, within(2L));
        assertThat(topic.getBestEffortBacklogAgeQuantileSeconds("sub-2").get(0.5)).isCloseTo(30L, within(2L));
        assertThat(topic.getBestEffortBacklogAgeQuantileSeconds("missing")).isEmpty();
    }

    @Test
    public void refreshReplacesPreviousResults() throws Exception {
        configureQuantiles(0.5);
        FakeCursor fake = addSubscriptionWithBacklog(SUBSCRIPTION, 10);
        refresh();
        assertThat(ages()).containsOnlyKeys(0.5);

        // The backlog is fully acked, so the subscription must not keep reporting a stale age.
        fake.unacked.clear();
        refresh();

        assertThat(ages()).isEmpty();
        assertThat(new ArrayList<>(fake.requestedN)).hasSize(1);
    }
}
