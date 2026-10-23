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
package org.apache.pulsar.broker.service.scalable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atMostOnce;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.pulsar.broker.resources.ScalableTopicMetadata;
import org.apache.pulsar.broker.resources.ScalableTopicResources;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.TopicEventsListener;
import org.apache.pulsar.broker.service.TopicEventsListener.EventStage;
import org.apache.pulsar.broker.service.TopicEventsListener.TopicEvent;
import org.apache.pulsar.broker.service.persistent.PersistentEntryBucketDispatcherMultipleConsumers;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.scalable.HashRange;
import org.apache.pulsar.common.scalable.SegmentInfo;
import org.apache.pulsar.common.scalable.SegmentTopicName;
import org.apache.pulsar.metadata.api.MetadataCache;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.Notification;
import org.apache.pulsar.metadata.api.NotificationType;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.api.extended.SessionEvent;
import org.apache.pulsar.metadata.impl.LocalMemoryMetadataStore;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class SegmentOwnerStateTest {
    private static final TopicName PARENT = TopicName.get("topic://tenant/ns/parent");
    private static final Duration TIMEOUT = Duration.ofSeconds(10);
    private final Map<String, CompletableFuture<Optional<Topic>>> topics = new ConcurrentHashMap<>();
    private MetadataStoreExtended store;
    private ScalableTopicResources resources;
    private MetadataCache<ScalableTopicMetadata> cache;
    private BrokerService broker;
    private SegmentOwnerState state;
    private TopicEventsListener topicListener;
    private Consumer<Notification> metadataListener;

    @BeforeMethod
    public void setUp() throws Exception {
        topics.clear();
        topicListener = null;
        metadataListener = null;
        store = spy(new LocalMemoryMetadataStore("memory:local", MetadataStoreConfig.builder().build()));
        resources = spy(new ScalableTopicResources(store, 30));
        cache = spy(resources.getCache());
        doReturn(cache).when(resources).getCache();
        doAnswer(invocation -> {
            metadataListener = invocation.getArgument(0);
            return invocation.callRealMethod();
        }).when(resources).registerMetadataListener(any());
        broker = mock(BrokerService.class);
        when(broker.getTopics()).thenReturn(topics);
        when(broker.getTopicReference(anyString())).thenAnswer(invocation -> {
            CompletableFuture<Optional<Topic>> future = topics.get(invocation.getArgument(0));
            return future == null || !future.isDone() || future.isCompletedExceptionally()
                    ? Optional.empty() : future.getNow(Optional.empty());
        });
        doAnswer(invocation -> {
            TopicEventsListener[] listeners = (TopicEventsListener[]) invocation.getRawArguments()[0];
            topicListener = listeners[0];
            // Match BrokerService.addTopicEventListener: replay every cached key, including pending loads.
            topics.keySet().forEach(name -> {
                for (TopicEventsListener listener : listeners) {
                    listener.handleEvent(name, TopicEvent.LOAD, EventStage.SUCCESS, null);
                }
            });
            return null;
        }).when(broker).addTopicEventListener(any(TopicEventsListener[].class));
        state = new SegmentOwnerState(broker, resources);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
        if (state != null) {
            state.close();
        }
        if (store != null) {
            store.close();
        }
    }

    @Test
    public void callbacksBeforeStartWaitForTheSharedObserverAndOwnershipReplay() throws Exception {
        String legacyName = "persistent://tenant/ns/parent";
        resources.createScalableTopicAsync(PARENT, metadata(active(0), legacy(1, legacyName))).get();
        PersistentTopic nativeTopic = addSegment(PARENT, 0, true, 2);
        PersistentTopic legacyTopic = addPersistentTopic(legacyName, true, 3);
        clearInvocations(resources, store, cache);

        state.onTopicCacheEntryAdded(nativeTopic.getName(), topics.get(nativeTopic.getName()));
        state.onTopicTerminated(legacyTopic);
        state.onMetadataSessionEvent(SessionEvent.Reconnected);
        verify(resources, never()).getScalableTopicMetadataAsync(any());
        verify(resources, never()).registerMetadataListener(any());
        verify(store, never()).exists(anyString());
        assertThat(state.get()).isEqualTo(new SegmentOwnerMetrics.Gauges(0, 0, 0));

        // A leader can change before start. The first lookup must happen after the shared observer is registered.
        String lock = resources.controllerLockPath(PARENT);
        store.put(lock, "remote".getBytes(StandardCharsets.UTF_8), Optional.empty()).get();
        state.start();
        assertGauges(0, 1, 5);
        verify(store, atMostOnce()).exists(lock);
        verify(resources).registerMetadataListener(metadataListener);
        assertNoForcedRefresh();
    }

    @Test
    public void disconnectedBeforeStartDoesNotWarmCachesUntilReconnected() throws Exception {
        resources.createScalableTopicAsync(PARENT, metadata(active(0))).get();
        addSegment(PARENT, 0, false, 2);
        state.onMetadataSessionEvent(SessionEvent.ConnectionLost);
        state.start();
        assertThat(state.get()).isEqualTo(new SegmentOwnerMetrics.Gauges(0, 0, 2));
        verify(store, never()).exists(anyString());
        verify(resources, never()).getScalableTopicMetadataAsync(any());

        state.onMetadataSessionEvent(SessionEvent.Reconnected);
        assertGauges(1, 0, 2);
        verify(store).exists(resources.controllerLockPath(PARENT));
        assertNoForcedRefresh();
    }

    @Test
    public void nativeMetricsUseLiveCachedLayoutAndSubscriptionState() throws Exception {
        SegmentInfo active = active(0);
        SegmentInfo sealed = active(1).sealed(1, 1, List.of());
        resources.createScalableTopicAsync(PARENT, metadata(active, sealed, active(3))).get();
        PersistentTopic first = addSegment(PARENT, 0, true, 2);
        addSegment(PARENT, 1, true, 3);
        addSegment(PARENT, 2, true, 4); // Not in the layout: cannot count as unwritable.
        addSegment(PARENT, 3, false, 0);
        addPersistentTopic("persistent://tenant/ns/ordinary", false, 20);
        PersistentSubscription firstSubscription = subscription(2);
        PersistentSubscription second = subscription(5);
        when(first.getSubscriptions()).thenReturn(Map.of("first", firstSubscription, "second", second));
        state.start();
        assertGauges(1, 1, 14);

        when(first.getManagedLedger().isTerminated()).thenReturn(false);
        when(((PersistentEntryBucketDispatcherMultipleConsumers) second.getDispatcher()).getDrainingBucketsCount())
                .thenReturn(7L);
        assertThat(state.get()).isEqualTo(new SegmentOwnerMetrics.Gauges(1, 0, 16));
        when(first.getManagedLedger().isTerminated()).thenReturn(true);
        resources.updateScalableTopicAsync(PARENT,
                __ -> metadata(active.sealed(2, 2, List.of()), sealed, active(3))).get();
        assertGauges(1, 0, 16);
        resources.deleteScalableTopicAsync(PARENT).get();
        assertGauges(1, 0, 16);
        verify(resources).registerMetadataListener(metadataListener);
        verify(resources, never()).registerPathListener(any());
        assertNoForcedRefresh();
    }

    @Test
    public void controllerEventsUpdatePresenceWithoutRepeatingExists() throws Exception {
        resources.createScalableTopicAsync(PARENT, metadata(active(0))).get();
        addSegment(PARENT, 0, false, 0);
        addSegment(PARENT, 1, false, 0);
        TopicName other = TopicName.get("topic://tenant/ns/other");
        resources.createScalableTopicAsync(other, metadata(active(0))).get();
        addSegment(other, 0, false, 0);
        state.start();
        assertGauges(2, 0, 0);
        String lock = resources.controllerLockPath(PARENT);
        verify(store).exists(lock);
        clearInvocations(store);

        store.put(lock, "remote".getBytes(StandardCharsets.UTF_8), Optional.empty()).get();
        assertGauges(1, 0, 0);
        store.put(lock, "other-remote".getBytes(StandardCharsets.UTF_8), Optional.empty()).get();
        assertGauges(1, 0, 0);
        store.delete(lock, Optional.empty()).get();
        assertGauges(2, 0, 0);
        store.put(lock, "recreated".getBytes(StandardCharsets.UTF_8), Optional.empty()).get();
        assertGauges(1, 0, 0);
        for (int i = 0; i < 10; i++) {
            state.onStatsUpdate();
        }
        verify(store, never()).exists(anyString());
        verify(resources).registerMetadataListener(metadataListener);
        assertNoForcedRefresh();
    }

    @Test
    public void warmStatsAndScrapesDoNotLoadMetadataOrScanBrokerTopics() throws Exception {
        resources.createScalableTopicAsync(PARENT, metadata(active(0))).get();
        addSegment(PARENT, 0, true, 3);
        state.start();
        assertGauges(1, 1, 3);
        clearInvocations(resources, store, cache, broker);

        for (int i = 0; i < 20; i++) {
            state.onStatsUpdate();
            assertThat(state.get()).isEqualTo(new SegmentOwnerMetrics.Gauges(1, 1, 3));
        }
        verify(resources, never()).getScalableTopicMetadataAsync(any());
        verify(resources, never()).getScalableTopicMetadataAsync(any(), anyBoolean());
        verify(store, never()).get(anyString());
        verify(store, never()).exists(anyString());
        verify(store, never()).sync(anyString());
        verify(cache, never()).get(anyString());
        verify(cache, never()).invalidate(anyString());
        verify(broker, never()).forEachTopic(any());
    }

    @Test
    public void unsupportedLayoutSnapshotsDoNotPollButControllerFailuresCanRecover() {
        doReturn(false).when(cache).supportsQuietSnapshots();
        addSegment(PARENT, 0, true, 3);
        String lock = resources.controllerLockPath(PARENT);
        doReturn(CompletableFuture.failedFuture(new IllegalStateException("unavailable")))
                .when(store).exists(lock);
        state.start();
        assertThat(state.get()).isEqualTo(new SegmentOwnerMetrics.Gauges(0, 0, 3));
        doCallRealMethod().when(store).exists(lock);
        state.onStatsUpdate();
        assertGauges(1, 0, 3);
        clearInvocations(resources, store, cache);
        for (int i = 0; i < 10; i++) {
            assertThat(state.get()).isEqualTo(new SegmentOwnerMetrics.Gauges(1, 0, 3));
            state.onStatsUpdate();
        }
        verify(resources, never()).getScalableTopicMetadataAsync(any());
        verify(cache, never()).getIfCachedQuietly(anyString());
        verify(cache, never()).get(anyString());
        verifyNoInteractions(store);
        assertNoForcedRefresh();
    }

    @Test
    public void unsupportedLayoutSnapshotsDoNotPollLegacyCandidates() {
        doReturn(false).when(cache).supportsQuietSnapshots();
        addPersistentTopic("persistent://tenant/ns/parent", true, 7);
        state.start();
        for (int i = 0; i < 10; i++) {
            assertThat(state.get()).isEqualTo(new SegmentOwnerMetrics.Gauges(0, 0, 0));
            state.onStatsUpdate();
        }
        verify(resources, never()).getScalableTopicMetadataAsync(any());
        verify(cache, never()).getIfCachedQuietly(anyString());
        verify(store, never()).exists(anyString());
        assertNoForcedRefresh();
    }

    @Test
    public void coldParentReadIsSharedAndRepairRequestsAreDeduplicated() throws Exception {
        CompletableFuture<Optional<ScalableTopicMetadata>> pending = new CompletableFuture<>();
        doReturn(pending).when(resources).getScalableTopicMetadataAsync(PARENT);
        addSegment(PARENT, 0, true, 0);
        addSegment(PARENT, 1, false, 0);
        state.start();
        assertGauges(1, 0, 0);
        for (int i = 0; i < 10; i++) {
            state.get();
            state.onStatsUpdate();
        }
        verify(resources).getScalableTopicMetadataAsync(PARENT);
        verify(store).exists(resources.controllerLockPath(PARENT));

        ScalableTopicMetadata layout = metadata(active(0), active(1));
        resources.createScalableTopicAsync(PARENT, layout).get();
        cache.get(resources.topicPath(PARENT)).get();
        pending.complete(Optional.of(layout));
        assertGauges(1, 1, 0);
        clearInvocations(resources, store);
        for (int i = 0; i < 10; i++) {
            state.onStatsUpdate();
        }
        verify(resources, never()).getScalableTopicMetadataAsync(any());
        verifyNoInteractions(store);
        assertNoForcedRefresh();
    }

    @Test
    public void expiredLayoutIsQueuedByScrapeAndRepairedOnceOnStatsUpdate() throws Exception {
        resources.createScalableTopicAsync(PARENT, metadata(active(0))).get();
        addSegment(PARENT, 0, true, 0);
        state.start();
        assertGauges(1, 1, 0);
        // Simulate the existing metadata cache's TTL expiry, not a business-state failure.
        cache.invalidate(resources.topicPath(PARENT));
        clearInvocations(resources, store, cache);
        for (int i = 0; i < 10; i++) {
            assertThat(state.get()).isEqualTo(new SegmentOwnerMetrics.Gauges(1, 0, 0));
        }
        verify(resources, never()).getScalableTopicMetadataAsync(any());
        verifyNoInteractions(store);
        verify(cache, never()).get(anyString());
        state.onStatsUpdate();
        assertGauges(1, 1, 0);
        verify(resources).getScalableTopicMetadataAsync(PARENT);
        clearInvocations(resources, store);
        for (int i = 0; i < 10; i++) {
            state.onStatsUpdate();
        }
        verify(resources, never()).getScalableTopicMetadataAsync(any());
        verifyNoInteractions(store);
        assertNoForcedRefresh();
    }

    @Test
    public void failedInitialReadsAreRetriedOnlyFromTheRepairQueue() throws Exception {
        resources.createScalableTopicAsync(PARENT, metadata(active(0))).get();
        cache.invalidate(resources.topicPath(PARENT));
        clearInvocations(cache);
        addSegment(PARENT, 0, true, 0);
        String lock = resources.controllerLockPath(PARENT);
        doReturn(CompletableFuture.failedFuture(new IllegalStateException("layout unavailable")))
                .when(resources).getScalableTopicMetadataAsync(PARENT);
        doReturn(CompletableFuture.failedFuture(new IllegalStateException("controller unavailable")))
                .when(store).exists(lock);
        state.start();
        assertThat(state.get()).isEqualTo(new SegmentOwnerMetrics.Gauges(0, 0, 0));
        verify(resources).getScalableTopicMetadataAsync(PARENT);
        verify(store).exists(lock);
        clearInvocations(resources, store);
        for (int i = 0; i < 5; i++) {
            state.get();
        }
        verify(resources, never()).getScalableTopicMetadataAsync(any());
        verifyNoInteractions(store);

        doCallRealMethod().when(resources).getScalableTopicMetadataAsync(PARENT);
        doCallRealMethod().when(store).exists(lock);
        state.onStatsUpdate();
        assertGauges(1, 1, 0);
        verify(resources).getScalableTopicMetadataAsync(PARENT);
        verify(store).exists(lock);
        assertNoForcedRefresh();
    }

    @Test
    public void pendingInitialControllerReadCannotOverwriteNewerNotifications() throws Exception {
        resources.createScalableTopicAsync(PARENT, metadata(active(0))).get();
        addSegment(PARENT, 0, false, 0);
        CompletableFuture<Boolean> pending = new CompletableFuture<>();
        String lock = resources.controllerLockPath(PARENT);
        doReturn(pending).when(store).exists(lock);
        state.start();
        assertThat(state.get()).isEqualTo(new SegmentOwnerMetrics.Gauges(0, 0, 0));
        metadataListener.accept(new Notification(NotificationType.Created, lock));
        pending.complete(false);
        assertThat(state.get()).isEqualTo(new SegmentOwnerMetrics.Gauges(0, 0, 0));
        metadataListener.accept(new Notification(NotificationType.Deleted, lock));
        assertThat(state.get()).isEqualTo(new SegmentOwnerMetrics.Gauges(1, 0, 0));
        metadataListener.accept(new Notification(NotificationType.Modified, lock));
        assertThat(state.get()).isEqualTo(new SegmentOwnerMetrics.Gauges(0, 0, 0));
        state.onStatsUpdate();
        verify(store).exists(lock);
    }

    @DataProvider
    public Object[][] reconnectEvents() {
        return new Object[][]{{SessionEvent.Reconnected}, {SessionEvent.SessionReestablished}};
    }

    @Test(dataProvider = "reconnectEvents")
    public void sessionLossSuppressesUnknownPresenceAndReconnectRepairsCaches(SessionEvent reconnect) throws Exception {
        resources.createScalableTopicAsync(PARENT, metadata(active(0))).get();
        addSegment(PARENT, 0, true, 0);
        state.start();
        assertGauges(1, 1, 0);
        state.onMetadataSessionEvent(SessionEvent.SessionLost);
        // The broker/store owns session cache invalidation; the metrics observer must not perform it.
        cache.invalidate(resources.topicPath(PARENT));
        clearInvocations(resources, store, cache);
        assertThat(state.get()).isEqualTo(new SegmentOwnerMetrics.Gauges(0, 0, 0));
        state.onStatsUpdate();
        verify(resources, never()).getScalableTopicMetadataAsync(any());
        verifyNoInteractions(store);
        state.onMetadataSessionEvent(reconnect);
        assertGauges(1, 1, 0);
        verify(resources).getScalableTopicMetadataAsync(PARENT);
        verify(store).exists(resources.controllerLockPath(PARENT));
        assertNoForcedRefresh();
    }

    @Test
    public void terminatedOrdinaryNegativeMetadataIsNotPolledAndCreationClearsTheNegative() throws Exception {
        PersistentTopic ordinary = addPersistentTopic("persistent://tenant/ns/parent", true, 3);
        addPersistentTopic("persistent://tenant/ns/normal", false, 20);
        state.start();
        // Wait for the one cold lookup to settle before checking the negative-cache fast path.
        await().atMost(TIMEOUT).untilAsserted(() -> verify(resources).getScalableTopicMetadataAsync(PARENT));
        cache.get(resources.topicPath(PARENT)).get();
        clearInvocations(resources, store);
        for (int i = 0; i < 20; i++) {
            assertThat(state.get()).isEqualTo(new SegmentOwnerMetrics.Gauges(0, 0, 0));
            state.onStatsUpdate();
        }
        verify(resources, never()).getScalableTopicMetadataAsync(any());
        verifyNoInteractions(store);
        resources.createScalableTopicAsync(PARENT, metadata(legacy(0, ordinary.getName()))).get();
        assertGauges(1, 0, 3);
        assertNoForcedRefresh();
    }

    @Test
    public void reconnectClearsNegativeMetadataForPreviouslyOrdinaryCandidates() throws Exception {
        PersistentTopic ordinary = addPersistentTopic("persistent://tenant/ns/parent", true, 3);
        state.start();
        cache.get(resources.topicPath(PARENT)).get();
        assertThat(state.get()).isEqualTo(new SegmentOwnerMetrics.Gauges(0, 0, 0));
        state.onMetadataSessionEvent(SessionEvent.ConnectionLost);
        resources.createScalableTopicAsync(PARENT, metadata(legacy(0, ordinary.getName()))).get();
        cache.invalidate(resources.topicPath(PARENT));
        clearInvocations(resources, store, cache);
        state.onMetadataSessionEvent(SessionEvent.SessionReestablished);
        assertGauges(1, 0, 3);
        verify(resources).getScalableTopicMetadataAsync(PARENT);
        assertNoForcedRefresh();
    }

    @Test
    public void runningMigrationIsDiscoveredBySuccessfulTerminationRatherThanStatsScanning() throws Exception {
        PersistentTopic ordinary = addPersistentTopic("persistent://tenant/ns/parent", false, 3);
        state.start();
        for (int i = 0; i < 5; i++) {
            state.onStatsUpdate();
            state.get();
        }
        verify(resources, never()).getScalableTopicMetadataAsync(any());
        verify(store, never()).exists(anyString());
        resources.createScalableTopicAsync(PARENT, metadata(legacy(0, ordinary.getName()))).get();
        when(ordinary.getManagedLedger().isTerminated()).thenReturn(true);
        state.onStatsUpdate();
        assertThat(state.get()).isEqualTo(new SegmentOwnerMetrics.Gauges(0, 0, 0));
        state.onTopicTerminated(ordinary);
        assertGauges(1, 0, 3);
        verify(broker, never()).forEachTopic(any());
        assertNoForcedRefresh();
    }

    @DataProvider
    public Object[][] legacyPartitioning() {
        return new Object[][]{{false}, {true}};
    }

    @Test(dataProvider = "legacyPartitioning")
    public void legacyOwnersAreConfirmedByMetadataAndCleanedUpAtLastUnload(boolean partitioned) throws Exception {
        String base = "persistent://tenant/ns/parent";
        String firstName = partitioned ? base + "-partition-0" : base;
        PersistentTopic first = addPersistentTopic(firstName, true, 2);
        PersistentTopic second = partitioned ? addPersistentTopic(base + "-partition-1", true, 3) : null;
        ScalableTopicMetadata layout = second == null ? metadata(legacy(0, firstName))
                : metadata(legacy(0, firstName), legacy(1, second.getName()));
        resources.createScalableTopicAsync(PARENT, layout).get();
        state.start();
        assertGauges(1, 0, partitioned ? 5 : 2);
        verify(resources).registerMetadataListener(metadataListener);
        remove(first, TopicEvent.UNLOAD);
        if (second != null) {
            assertGauges(1, 0, 3);
            remove(second, TopicEvent.DELETE);
        }
        assertThat(state.get()).isEqualTo(new SegmentOwnerMetrics.Gauges(0, 0, 0));
        clearInvocations(resources, store);
        metadataListener.accept(new Notification(NotificationType.Modified, resources.topicPath(PARENT)));
        state.onStatsUpdate();
        verify(resources, never()).getScalableTopicMetadataAsync(any());
        verifyNoInteractions(store);
        topics.put(firstName, CompletableFuture.completedFuture(Optional.of(first)));
        topicListener.handleEvent(firstName, TopicEvent.LOAD, EventStage.SUCCESS, null);
        assertGauges(1, 0, 2);
        state.close();
        verify(resources).deregisterMetadataListener(metadataListener);
        verify(broker).removeTopicEventListener(topicListener);
        assertNoForcedRefresh();
    }

    @Test
    public void terminatedClassicTopicWithMismatchedBackingMetadataDoesNotCount() throws Exception {
        addPersistentTopic("persistent://tenant/ns/parent", true, 4);
        resources.createScalableTopicAsync(PARENT,
                metadata(legacy(0, "persistent://tenant/ns/some-other-backing-topic"))).get();
        state.start();
        assertThat(state.get()).isEqualTo(new SegmentOwnerMetrics.Gauges(0, 0, 0));
        verify(store, never()).exists(anyString());
        assertNoForcedRefresh();
    }

    @Test
    public void pendingLoadAndStaleUnloadCannotReplaceCurrentOwnership() throws Exception {
        PersistentTopic oldTopic = topic(segmentName(PARENT, 0), false, 1);
        PersistentTopic newTopic = topic(oldTopic.getName(), false, 7);
        CompletableFuture<Optional<Topic>> oldLoad = new CompletableFuture<>();
        CompletableFuture<Optional<Topic>> newLoad = new CompletableFuture<>();
        topics.put(oldTopic.getName(), oldLoad);
        state.start();
        state.onTopicCacheEntryAdded(oldTopic.getName(), oldLoad);
        assertThat(state.get()).isEqualTo(new SegmentOwnerMetrics.Gauges(0, 0, 0));
        verify(resources, never()).getScalableTopicMetadataAsync(any());
        verify(store, never()).exists(anyString());
        topics.remove(oldTopic.getName());
        topicListener.handleEvent(oldTopic.getName(), TopicEvent.UNLOAD, EventStage.SUCCESS, null);
        topics.put(oldTopic.getName(), newLoad);
        topicListener.handleEvent(oldTopic.getName(), TopicEvent.LOAD, EventStage.SUCCESS, null);
        state.onTopicCacheEntryAdded(oldTopic.getName(), newLoad);
        oldLoad.complete(Optional.of(oldTopic));
        state.onTopicCacheEntryAdded(oldTopic.getName(), oldLoad); // A late hook from the replaced cache entry.
        assertThat(state.get()).isEqualTo(new SegmentOwnerMetrics.Gauges(0, 0, 0));
        verify(resources, never()).getScalableTopicMetadataAsync(any());
        newLoad.complete(Optional.of(newTopic));
        assertGauges(1, 0, 7);
        topicListener.handleEvent(newTopic.getName(), TopicEvent.LOAD, EventStage.SUCCESS, null);
        state.onTopicCacheEntryAdded(newTopic.getName(), newLoad);
        assertThat(state.get()).isEqualTo(new SegmentOwnerMetrics.Gauges(1, 0, 7));
        verify(store).exists(resources.controllerLockPath(PARENT));
        topicListener.handleEvent(oldTopic.getName(), TopicEvent.UNLOAD, EventStage.SUCCESS, null);
        assertThat(state.get()).isEqualTo(new SegmentOwnerMetrics.Gauges(1, 0, 7));
        remove(newTopic, TopicEvent.DELETE);
        assertThat(state.get()).isEqualTo(new SegmentOwnerMetrics.Gauges(0, 0, 0));
        verify(broker, never()).forEachTopic(any());
    }

    @DataProvider
    public Object[][] unpublishedLoadDomains() {
        return new Object[][]{{true}, {false}};
    }

    @Test(dataProvider = "unpublishedLoadDomains")
    public void unpublishedLoadSurvivesOldUnloadAndTracksOnlyThePublishedInstance(boolean nativeSegment)
            throws Exception {
        String name = nativeSegment ? segmentName(PARENT, 0) : "persistent://tenant/ns/parent";
        resources.createScalableTopicAsync(PARENT,
                metadata(nativeSegment ? active(0) : legacy(0, name))).get();
        state.start();
        topicListener.handleEvent(name, TopicEvent.LOAD, EventStage.SUCCESS, null);
        topicListener.handleEvent(name, TopicEvent.UNLOAD, EventStage.SUCCESS, null);
        assertThat(state.get()).isEqualTo(new SegmentOwnerMetrics.Gauges(0, 0, 0));
        verify(store, never()).exists(anyString());
        PersistentTopic published = addPersistentTopic(name, !nativeSegment, 6);
        for (int i = 0; i < 2; i++) {
            state.onStatsUpdate();
            assertThat(state.get()).isEqualTo(new SegmentOwnerMetrics.Gauges(0, 0, 0));
        }
        verify(store, never()).exists(anyString());
        state.onTopicCacheEntryAdded(name, topics.get(name));
        assertGauges(1, 0, 6);
        remove(published, TopicEvent.UNLOAD);
        clearInvocations(resources, store);
        state.onStatsUpdate();
        assertThat(state.get()).isEqualTo(new SegmentOwnerMetrics.Gauges(0, 0, 0));
        verify(resources, never()).getScalableTopicMetadataAsync(any());
        verifyNoInteractions(store);
        assertNoForcedRefresh();
    }

    @Test
    public void lateTerminationCannotResurrectAReplacedLegacyOwner() throws Exception {
        String name = "persistent://tenant/ns/parent";
        PersistentTopic old = topic(name, true, 1);
        PersistentTopic current = addPersistentTopic(name, false, 7);
        resources.createScalableTopicAsync(PARENT, metadata(legacy(0, name))).get();
        state.start();
        state.onTopicTerminated(old);
        state.onStatsUpdate();
        assertThat(state.get()).isEqualTo(new SegmentOwnerMetrics.Gauges(0, 0, 0));
        verify(store, never()).exists(anyString());
        when(current.getManagedLedger().isTerminated()).thenReturn(true);
        state.onTopicTerminated(current);
        assertGauges(1, 0, 7);
        assertNoForcedRefresh();
    }

    @Test
    public void closeRejectsLateReadsAndCallbacksAndIsIdempotent() {
        addSegment(PARENT, 0, true, 0);
        CompletableFuture<Optional<ScalableTopicMetadata>> layout = new CompletableFuture<>();
        CompletableFuture<Boolean> controller = new CompletableFuture<>();
        doReturn(layout).when(resources).getScalableTopicMetadataAsync(PARENT);
        String lock = resources.controllerLockPath(PARENT);
        doReturn(controller).when(store).exists(lock);
        state.start();
        state.start();
        state.close();
        state.close();
        verify(resources).registerMetadataListener(metadataListener);
        verify(resources).deregisterMetadataListener(metadataListener);
        verify(resources, never()).registerPathListener(any());
        clearInvocations(resources, store);
        layout.complete(Optional.of(metadata(active(0))));
        controller.complete(false);
        metadataListener.accept(new Notification(NotificationType.Deleted, "/irrelevant/controller"));
        topicListener.handleEvent(segmentName(PARENT, 1), TopicEvent.LOAD, EventStage.SUCCESS, null);
        state.onMetadataSessionEvent(SessionEvent.Reconnected);
        state.onStatsUpdate();
        assertThat(state.get()).isEqualTo(new SegmentOwnerMetrics.Gauges(0, 0, 0));
        verifyNoInteractions(resources, store);
        verify(broker).addTopicEventListener(any(TopicEventsListener[].class));
        verify(broker).removeTopicEventListener(topicListener);
    }

    private void assertNoForcedRefresh() {
        verify(cache, never()).getIfCached(anyString());
        verify(resources, never()).getScalableTopicMetadataAsync(any(), anyBoolean());
        verify(store, never()).sync(anyString());
        verify(cache, never()).invalidate(anyString());
        verify(broker, never()).forEachTopic(any());
    }

    private void remove(PersistentTopic topic, TopicEvent event) {
        topics.remove(topic.getName());
        topicListener.handleEvent(topic.getName(), event, EventStage.SUCCESS, null);
    }

    private PersistentTopic addSegment(TopicName parent, long id, boolean terminated, long draining) {
        return addPersistentTopic(segmentName(parent, id), terminated, draining);
    }

    private static String segmentName(TopicName parent, long id) {
        return SegmentTopicName.fromParent(parent, HashRange.full(), id).toString();
    }

    private PersistentTopic addPersistentTopic(String name, boolean terminated, long draining) {
        PersistentTopic topic = topic(name, terminated, draining);
        topics.put(name, CompletableFuture.completedFuture(Optional.of(topic)));
        return topic;
    }

    private static PersistentTopic topic(String name, boolean terminated, long draining) {
        PersistentTopic topic = mock(PersistentTopic.class);
        when(topic.getName()).thenReturn(name);
        ManagedLedger ledger = mock(ManagedLedger.class);
        when(ledger.isTerminated()).thenReturn(terminated);
        when(topic.getManagedLedger()).thenReturn(ledger);
        PersistentSubscription subscription = subscription(draining);
        when(topic.getSubscriptions()).thenReturn(Map.of("sub", subscription));
        return topic;
    }

    private static PersistentSubscription subscription(long draining) {
        PersistentSubscription subscription = mock(PersistentSubscription.class);
        PersistentEntryBucketDispatcherMultipleConsumers dispatcher =
                mock(PersistentEntryBucketDispatcherMultipleConsumers.class);
        when(dispatcher.getDrainingBucketsCount()).thenReturn(draining);
        when(subscription.getDispatcher()).thenReturn(dispatcher);
        return subscription;
    }

    private void assertGauges(long leaderless, long unwritable, long draining) {
        await().atMost(TIMEOUT).untilAsserted(() -> assertThat(state.get())
                .isEqualTo(new SegmentOwnerMetrics.Gauges(leaderless, unwritable, draining)));
    }

    private static SegmentInfo legacy(long id, String backing) {
        return SegmentInfo.activeLegacy(id, HashRange.full(), backing, 0, 0).sealed(1, 1, List.of());
    }

    private static SegmentInfo active(long id) {
        return SegmentInfo.active(id, HashRange.full(), 0, 0);
    }

    private static ScalableTopicMetadata metadata(SegmentInfo... segments) {
        ScalableTopicMetadata metadata = new ScalableTopicMetadata();
        for (SegmentInfo segment : segments) {
            metadata.getSegments().put(segment.segmentId(), segment);
        }
        return metadata;
    }
}
