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
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.pulsar.broker.resources.ScalableTopicMetadata;
import org.apache.pulsar.broker.resources.ScalableTopicResources;
import org.apache.pulsar.broker.resources.ScalableTopicResources.MetadataPathListener;
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
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.Notification;
import org.apache.pulsar.metadata.api.NotificationType;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.impl.LocalMemoryMetadataStore;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class SegmentOwnerStateTest {
    private static final TopicName PARENT = TopicName.get("topic://tenant/ns/parent");
    private static final Duration TIMEOUT = Duration.ofSeconds(10);
    private final List<Topic> loaded = new CopyOnWriteArrayList<>();
    private MetadataStoreExtended store;
    private ScalableTopicResources resources;
    private BrokerService broker;
    private SegmentOwnerState state;

    @BeforeMethod
    public void setUp() throws Exception {
        loaded.clear();
        store = spy(new LocalMemoryMetadataStore("memory:local", MetadataStoreConfig.builder().build()));
        resources = spy(new ScalableTopicResources(store, 30));
        broker = mock(BrokerService.class);
        doAnswer(invocation -> {
            Consumer<Topic> visitor = invocation.getArgument(0);
            loaded.forEach(visitor);
            return null;
        }).when(broker).forEachTopic(any());
        when(broker.getTopicReference(any())).thenAnswer(invocation -> {
            String name = invocation.getArgument(0);
            return loaded.stream().filter(topic -> topic.getName().equals(name)).findFirst();
        });
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
    public void deduplicatesParentsAndObservesRemoteControllerWithoutScrapeIo() throws Exception {
        addSegment(PARENT, 0, false, 2);
        addSegment(PARENT, 1, false, 3);
        TopicName other = TopicName.get("topic://tenant/ns/other");
        addSegment(other, 0, false, 4);
        // Regular topics are not segment owners, even if they use an entry-bucket dispatcher.
        addPersistentTopic("persistent://tenant/ns/regular", false, 10);
        state.start();
        assertGauges(2, 0, 9);
        verify(resources, times(4)).registerPathListener(any());

        String lockPath = resources.controllerLockPath(PARENT);
        store.put(lockPath, "remote-broker".getBytes(StandardCharsets.UTF_8), Optional.empty()).get();
        assertGauges(1, 0, 9);
        store.delete(lockPath, Optional.empty()).get();
        assertGauges(2, 0, 9);

        clearInvocations(resources, store);
        for (int i = 0; i < 10; i++) {
            assertThat(state.get()).isEqualTo(new SegmentOwnerMetrics.Gauges(2, 0, 9));
        }
        verifyNoInteractions(resources, store);
    }

    @Test
    public void sumsSubscriptionsAndSamplesLiveStateWithoutRefresh() throws Exception {
        resources.createScalableTopicAsync(PARENT, metadata(active(0))).get();
        PersistentTopic topic = addSegment(PARENT, 0, true, 2);
        PersistentSubscription first = topic.getSubscriptions().get("sub");
        PersistentSubscription second = mock(PersistentSubscription.class);
        PersistentEntryBucketDispatcherMultipleConsumers dispatcher =
                mock(PersistentEntryBucketDispatcherMultipleConsumers.class);
        when(dispatcher.getDrainingBucketsCount()).thenReturn(3L);
        when(second.getDispatcher()).thenReturn(dispatcher);
        when(topic.getSubscriptions()).thenReturn(Map.of("first", first, "second", second));
        state.start();
        assertGauges(1, 1, 5);

        when(topic.getManagedLedger().isTerminated()).thenReturn(false);
        when(dispatcher.getDrainingBucketsCount()).thenReturn(7L);
        assertThat(state.get()).isEqualTo(new SegmentOwnerMetrics.Gauges(1, 0, 9));
        // Collection uses live ownership, even before the unload notification or reconciliation.
        loaded.clear();
        assertThat(state.get()).isEqualTo(new SegmentOwnerMetrics.Gauges(0, 0, 0));
    }

    @Test
    public void countsOnlyTerminatedSegmentsStillActiveInMetadata() throws Exception {
        SegmentInfo active = active(0);
        SegmentInfo sealed = active(1).sealed(1, 1, List.of());
        resources.createScalableTopicAsync(PARENT, metadata(active, sealed, active(3))).get();
        addSegment(PARENT, 0, true, 0);
        addSegment(PARENT, 1, true, 0);
        addSegment(PARENT, 2, true, 0); // Absent from the layout.
        addSegment(PARENT, 3, false, 0);
        state.start();
        assertGauges(1, 1, 0);

        resources.updateScalableTopicAsync(PARENT,
                __ -> metadata(active.sealed(2, 2, List.of()), sealed, active(3))).get();
        assertGauges(1, 0, 0);
        resources.deleteScalableTopicAsync(PARENT).get();
        assertGauges(1, 0, 0);
    }

    @Test
    public void includesNonPartitionedLegacyOwnerAndCleansUp() throws Exception {
        assertLegacyOwners(false);
    }

    @Test
    public void deduplicatesPartitionedLegacyOwnersAndCleansUpLastPartition() throws Exception {
        assertLegacyOwners(true);
    }

    @Test
    public void terminatedClassicTopicsRequireExactLegacyMetadataMatch() {
        String missing = "persistent://tenant/ns/missing";
        String mismatched = "persistent://tenant/ns/parent";
        String normal = "persistent://tenant/ns/normal";
        addPersistentTopic(missing, true, 3);
        addPersistentTopic(mismatched, true, 4);
        addPersistentTopic(normal, false, 5);
        doReturn(CompletableFuture.completedFuture(Optional.empty())).when(resources)
                .getScalableTopicMetadataAsync(TopicName.get(missing).toScalableTopic(), true);
        doReturn(CompletableFuture.completedFuture(Optional.of(metadata(
                legacy(0, "persistent://tenant/ns/some-other-backing-topic")))))
                .when(resources).getScalableTopicMetadataAsync(PARENT, true);
        state.start();
        assertThat(state.get()).isEqualTo(new SegmentOwnerMetrics.Gauges(0, 0, 0));
        verify(resources, times(4)).registerPathListener(any());
        verify(resources, never()).getScalableTopicMetadataAsync(TopicName.get(normal).toScalableTopic(), true);
        loaded.clear();
        state.refresh();
        verify(resources, times(4)).deregisterPathListener(any());
    }

    @Test
    public void refreshDiscoversMigrationTerminatedAfterLoadWithoutReadingNormalTopics() throws Exception {
        String name = "persistent://tenant/ns/parent";
        PersistentTopic topic = addPersistentTopic(name, false, 3);
        state.start();
        topicListener().handleEvent(name, TopicEvent.LOAD, EventStage.SUCCESS, null);
        verify(resources, never()).registerPathListener(any());
        verify(resources, never()).getScalableTopicMetadataAsync(PARENT, true);
        resources.createScalableTopicAsync(PARENT, metadata(legacy(0, name))).get();
        when(topic.getManagedLedger().isTerminated()).thenReturn(true);
        assertThat(state.get()).isEqualTo(new SegmentOwnerMetrics.Gauges(0, 0, 0));
        state.refresh();
        assertGauges(1, 0, 3);
        verify(resources, times(2)).registerPathListener(any());
        loaded.remove(topic);
        topicListener().handleEvent(name, TopicEvent.UNLOAD, EventStage.SUCCESS, null);
        verify(resources, times(2)).deregisterPathListener(any());
    }

    private void assertLegacyOwners(boolean partitioned) throws Exception {
        String base = "persistent://tenant/ns/parent";
        String firstName = partitioned ? base + "-partition-0" : base;
        PersistentTopic first = addPersistentTopic(firstName, true, 2);
        PersistentTopic second = partitioned ? addPersistentTopic(base + "-partition-1", true, 3) : null;
        ScalableTopicMetadata metadata = metadata(legacy(0, firstName));
        if (second != null) {
            metadata.getSegments().put(1L, legacy(1, second.getName()));
        }
        resources.createScalableTopicAsync(PARENT, metadata).get();
        state.start();
        assertGauges(1, 0, partitioned ? 5 : 2);
        verify(resources, times(2)).registerPathListener(any());
        TopicEventsListener listener = topicListener();
        loaded.remove(first);
        listener.handleEvent(firstName, TopicEvent.UNLOAD, EventStage.SUCCESS, null);
        if (second != null) {
            assertGauges(1, 0, 3);
            verify(resources, never()).deregisterPathListener(any());
            loaded.remove(second);
            listener.handleEvent(second.getName(), TopicEvent.UNLOAD, EventStage.SUCCESS, null);
        }
        assertGauges(0, 0, 0);
        verify(resources, times(2)).deregisterPathListener(any());
        // A terminated legacy owner loaded after startup is also discovered by the topic event.
        loaded.add(first);
        listener.handleEvent(firstName, TopicEvent.LOAD, EventStage.SUCCESS, null);
        assertGauges(1, 0, 2);
        state.close();
        verify(resources, times(4)).deregisterPathListener(any());
    }

    @Test
    public void releasesWatchesOnlyAfterLastSegmentAndOnClose() {
        PersistentTopic first = addSegment(PARENT, 0, false, 0);
        PersistentTopic second = addSegment(PARENT, 1, false, 0);
        state.start();
        assertGauges(1, 0, 0);
        TopicEventsListener listener = topicListener();

        loaded.remove(first);
        listener.handleEvent(first.getName(), TopicEvent.UNLOAD, EventStage.SUCCESS, null);
        verify(resources, never()).deregisterPathListener(any());
        assertGauges(1, 0, 0);

        loaded.remove(second);
        listener.handleEvent(second.getName(), TopicEvent.UNLOAD, EventStage.SUCCESS, null);
        verify(resources, times(2)).deregisterPathListener(any());
        assertGauges(0, 0, 0);

        loaded.add(first);
        listener.handleEvent(first.getName(), TopicEvent.LOAD, EventStage.SUCCESS, null);
        assertGauges(1, 0, 0);
        state.close();
        verify(resources, times(4)).deregisterPathListener(any());
        verify(broker).removeTopicEventListener(listener);
        assertThat(state.get()).isEqualTo(new SegmentOwnerMetrics.Gauges(0, 0, 0));
        listener.handleEvent(second.getName(), TopicEvent.LOAD, EventStage.SUCCESS, null);
        state.refresh();
        verify(resources, times(4)).registerPathListener(any());
    }

    @Test
    public void periodicRefreshReconcilesOwnershipWithoutEvents() {
        addSegment(PARENT, 0, false, 0);
        state.start();
        assertGauges(1, 0, 0);
        loaded.clear();
        state.refresh();
        verify(resources, times(2)).deregisterPathListener(any());
        addSegment(PARENT, 1, false, 0);
        state.refresh();
        assertGauges(1, 0, 0);
        verify(resources, times(4)).registerPathListener(any());
    }

    @Test
    public void failedReadsStayUnknownAndPeriodicRefreshRetries() throws Exception {
        addSegment(PARENT, 0, true, 0);
        resources.createScalableTopicAsync(PARENT, metadata(active(0))).get();
        String lockPath = resources.controllerLockPath(PARENT);
        doReturn(CompletableFuture.failedFuture(new IllegalStateException("unavailable")))
                .when(resources).getScalableTopicMetadataAsync(PARENT, true);
        doReturn(CompletableFuture.failedFuture(new IllegalStateException("unavailable")))
                .when(store).exists(lockPath);
        state.start();
        assertThat(state.get()).isEqualTo(new SegmentOwnerMetrics.Gauges(0, 0, 0));

        doCallRealMethod().when(resources).getScalableTopicMetadataAsync(PARENT, true);
        doCallRealMethod().when(store).exists(lockPath);
        state.refresh();
        assertGauges(1, 1, 0);

        doReturn(CompletableFuture.failedFuture(new IllegalStateException("unavailable")))
                .when(resources).getScalableTopicMetadataAsync(PARENT, true);
        doReturn(CompletableFuture.failedFuture(new IllegalStateException("unavailable")))
                .when(store).exists(lockPath);
        state.refresh();
        assertGauges(0, 0, 0);
    }

    @Test
    public void notificationInvalidatesInFlightReadAndCoalescesReload() {
        addSegment(PARENT, 0, true, 0);
        CompletableFuture<Optional<ScalableTopicMetadata>> staleLayout = new CompletableFuture<>();
        CompletableFuture<Optional<ScalableTopicMetadata>> currentLayout = new CompletableFuture<>();
        CompletableFuture<Boolean> staleLock = new CompletableFuture<>();
        CompletableFuture<Boolean> currentLock = new CompletableFuture<>();
        doReturn(staleLayout, currentLayout).when(resources).getScalableTopicMetadataAsync(PARENT, true);
        String lockPath = resources.controllerLockPath(PARENT);
        doReturn(staleLock, currentLock).when(store).exists(lockPath);
        state.start();
        assertThat(state.get()).isEqualTo(new SegmentOwnerMetrics.Gauges(0, 0, 0));

        ArgumentCaptor<MetadataPathListener> captor = ArgumentCaptor.forClass(MetadataPathListener.class);
        verify(resources, times(2)).registerPathListener(captor.capture());
        captor.getAllValues().forEach(listener -> {
            for (int i = 0; i < 3; i++) {
                listener.onNotification(new Notification(NotificationType.Modified, listener.getMetadataPath()));
            }
        });
        staleLayout.complete(Optional.of(metadata(active(0))));
        staleLock.complete(false);
        // Neither stale result may raise an alert while the fresh reads remain pending.
        assertThat(state.get()).isEqualTo(new SegmentOwnerMetrics.Gauges(0, 0, 0));
        verify(resources, times(2)).getScalableTopicMetadataAsync(PARENT, true);
        verify(store, times(2)).exists(lockPath);
        currentLayout.complete(Optional.of(metadata(active(0).sealed(1, 1, List.of()))));
        currentLock.complete(true);
        assertThat(state.get()).isEqualTo(new SegmentOwnerMetrics.Gauges(0, 0, 0));
    }

    @Test
    public void closeRejectsPendingReadCompletionsAndIsIdempotent() {
        addSegment(PARENT, 0, true, 0);
        CompletableFuture<Optional<ScalableTopicMetadata>> pending = new CompletableFuture<>();
        doReturn(pending).when(resources).getScalableTopicMetadataAsync(PARENT, true);
        state.start();
        state.start();
        state.close();
        state.close();
        pending.complete(Optional.of(metadata(active(0))));
        assertThat(state.get()).isEqualTo(new SegmentOwnerMetrics.Gauges(0, 0, 0));
        verify(resources, times(2)).registerPathListener(any());
        verify(resources, times(2)).deregisterPathListener(any());
        verify(broker).addTopicEventListener(any(TopicEventsListener.class));
        verify(broker).removeTopicEventListener(any(TopicEventsListener.class));
    }

    private TopicEventsListener topicListener() {
        ArgumentCaptor<TopicEventsListener> captor = ArgumentCaptor.forClass(TopicEventsListener.class);
        verify(broker).addTopicEventListener(captor.capture());
        return captor.getValue();
    }

    private PersistentTopic addSegment(TopicName parent, long id, boolean terminated, long drainingBuckets) {
        return addPersistentTopic(SegmentTopicName.fromParent(parent, HashRange.full(), id).toString(),
                terminated, drainingBuckets);
    }

    private PersistentTopic addPersistentTopic(String name, boolean terminated, long drainingBuckets) {
        PersistentTopic topic = mock(PersistentTopic.class);
        when(topic.getName()).thenReturn(name);
        ManagedLedger ledger = mock(ManagedLedger.class);
        when(ledger.isTerminated()).thenReturn(terminated);
        when(topic.getManagedLedger()).thenReturn(ledger);
        PersistentSubscription subscription = mock(PersistentSubscription.class);
        PersistentEntryBucketDispatcherMultipleConsumers dispatcher =
                mock(PersistentEntryBucketDispatcherMultipleConsumers.class);
        when(dispatcher.getDrainingBucketsCount()).thenReturn(drainingBuckets);
        when(subscription.getDispatcher()).thenReturn(dispatcher);
        when(topic.getSubscriptions()).thenReturn(Map.of("sub", subscription));
        loaded.add(topic);
        return topic;
    }

    private void assertGauges(long leaderless, long unwritable, long draining) {
        await().atMost(TIMEOUT).untilAsserted(() -> assertThat(state.get())
                .isEqualTo(new SegmentOwnerMetrics.Gauges(leaderless, unwritable, draining)));
    }

    private static SegmentInfo legacy(long id, String backingTopic) {
        return SegmentInfo.activeLegacy(id, HashRange.full(), backingTopic, 0, 0)
                .sealed(1, 1, List.of());
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
