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
package org.apache.pulsar.broker.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.mledger.AsyncCallbacks.DeleteLedgerCallback;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.BrokerServiceException.ServiceUnitNotReadyException;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.testcontext.PulsarTestContext;
import org.apache.pulsar.broker.testinterceptor.BrokerTestInterceptor;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.TopicName;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/** Deterministic ordering tests for the topic cache shared by loads, deletion and bundle unload. */
@Test(groups = "broker", timeOut = 30000)
public class BrokerServiceTopicDeletionTest {
    private PulsarTestContext context;
    private BrokerService broker;
    private AtomicInteger metadataDeletes;
    private AtomicReference<CompletableFuture<Void>> metadataDeletion;

    @BeforeMethod
    public void setup() throws Exception {
        ServiceConfiguration config = new ServiceConfiguration();
        config.setClusterName("test");
        config.setBrokerShutdownTimeoutMs(0L);
        config.setLoadBalancerOverrideBrokerNicSpeedGbps(Optional.of(1.0));
        config.setTopicLevelPoliciesEnabled(false);
        config.setSystemTopicEnabled(false);
        context = PulsarTestContext.builderForNonStartableContext().config(config).spyByDefault().build();
        broker = context.getBrokerService();
        metadataDeletes = new AtomicInteger();
        metadataDeletion = new AtomicReference<>(CompletableFuture.completedFuture(null));

        TopicPoliciesService policies = mock(TopicPoliciesService.class);
        doReturn(policies).when(context.getPulsarService()).getTopicPoliciesService();
        when(policies.deleteTopicPoliciesAsync(any())).thenReturn(CompletableFuture.completedFuture(null));
        doAnswer(invocation -> {
            invocation.<CompletableFuture<Void>>getArgument(1).complete(null);
            return null;
        }).when(broker).deleteTopicAuthenticationWithRetry(anyString(), any(), anyInt());
        doReturn(CompletableFuture.completedFuture(null)).when(broker).deleteSchema(any());
        doReturn(CompletableFuture.completedFuture(new ManagedLedgerConfig()))
                .when(broker).getManagedLedgerConfig(any());
        ManagedLedgerFactory factory = context.getDefaultManagedLedgerFactory();
        doReturn(factory).when(broker).getManagedLedgerFactoryForTopic(any(), any());
        doAnswer(invocation -> {
            metadataDeletes.incrementAndGet();
            DeleteLedgerCallback callback = invocation.getArgument(2);
            Object callbackContext = invocation.getArgument(3);
            metadataDeletion.get().whenComplete((ignored, error) -> {
                if (error == null) {
                    callback.deleteLedgerComplete(callbackContext);
                } else {
                    callback.deleteLedgerFailed(new ManagedLedgerException(error), callbackContext);
                }
            });
            return null;
        }).when(factory).asyncDelete(anyString(), any(), any(), any());
    }

    @AfterMethod(alwaysRun = true)
    public void cleanup() throws Exception {
        if (context != null) {
            // These tests publish controlled futures and mocked Topic instances, not live topics.
            broker.getTopics().clear();
            context.close();
        }
    }

    @DataProvider
    public Object[][] forceDelete() {
        return new Object[][] {{false}, {true}};
    }

    @Test(dataProvider = "forceDelete")
    public void testDeleteWaitsForPendingLoad(boolean forceDelete) throws Exception {
        String name = "persistent://public/default/delete-while-loading";
        CompletableFuture<Optional<Topic>> loading = new CompletableFuture<>();
        broker.getTopics().put(name, loading);
        Topic loaded = mock(Topic.class);
        CompletableFuture<Void> topicDeletion = new CompletableFuture<>();
        when(loaded.delete()).thenReturn(topicDeletion);
        when(loaded.deleteForcefully()).thenReturn(topicDeletion);

        CompletableFuture<Void> deletion = broker.deleteTopic(name, forceDelete);

        assertThat(metadataDeletes.get()).as("metadata must survive until its broker topic finishes loading").isZero();
        assertThat(deletion).isNotDone();
        loading.complete(Optional.of(loaded));
        if (forceDelete) {
            verify(loaded).deleteForcefully();
        } else {
            verify(loaded).delete();
        }
        assertThat(deletion).isNotDone();
        topicDeletion.complete(null);
        deletion.get(5, TimeUnit.SECONDS);
        assertThat(metadataDeletes.get()).as("loaded topics use their normal deletion lifecycle").isZero();
    }

    @Test
    public void testDeleteRechecksGenerationAfterPendingLoad() throws Exception {
        String name = "persistent://public/default/delete-replaced-load";
        CompletableFuture<Optional<Topic>> oldLoad = new CompletableFuture<>();
        CompletableFuture<Optional<Topic>> newLoad = new CompletableFuture<>();
        broker.getTopics().put(name, oldLoad);
        Topic oldTopic = mock(Topic.class);
        Topic newTopic = mock(Topic.class);
        when(newTopic.deleteForcefully()).thenReturn(CompletableFuture.completedFuture(null));

        CompletableFuture<Void> deletion = broker.deleteTopic(name, true);
        broker.getTopics().put(name, newLoad);
        oldLoad.complete(Optional.of(oldTopic));

        assertThat(deletion).isNotDone();
        assertThat(metadataDeletes.get()).isZero();
        verify(oldTopic, never()).deleteForcefully();
        newLoad.complete(Optional.of(newTopic));
        deletion.get(5, TimeUnit.SECONDS);
        verify(newTopic).deleteForcefully();
    }

    @DataProvider
    public Object[][] unusableLoad() {
        return new Object[][] {{false}, {true}};
    }

    @Test(dataProvider = "unusableLoad")
    public void testDeleteRetriesAfterUnusableLoad(boolean failed) throws Exception {
        String name = "persistent://public/default/delete-unusable-load";
        CompletableFuture<Optional<Topic>> loading = new CompletableFuture<>();
        broker.getTopics().put(name, loading);
        CompletableFuture<Void> deletion = broker.deleteTopic(name, true);

        assertThat(metadataDeletes.get()).isZero();
        if (failed) {
            loading.completeExceptionally(new IllegalStateException("Loading failed"));
            CompletableFuture<Void> failedDeletion = deletion;
            assertThatThrownBy(() -> failedDeletion.get(5, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(ServiceUnitNotReadyException.class);
            assertThat(broker.getTopics().get(name)).as("the loader still owns failed-load resource cleanup")
                    .isSameAs(loading);
            assertThat(metadataDeletes.get()).isZero();
            // The loading lifecycle, rather than deletion, removes the failed entry after its cleanup.
            broker.getTopics().remove(name, loading);
            deletion = broker.deleteTopic(name, true);
        } else {
            loading.complete(Optional.empty());
        }

        deletion.get(5, TimeUnit.SECONDS);
        assertThat(metadataDeletes.get()).isEqualTo(1);
        assertThat(broker.getTopics()).doesNotContainKey(name);
    }

    @Test(timeOut = 60000)
    @SuppressWarnings("unchecked")
    public void testDeleteDoesNotExposeTimedOutInitializationLedgerToReplacement() throws Exception {
        context.close();
        context = null;
        CompletableFuture<Void> initialization = new CompletableFuture<>();
        CompletableFuture<PersistentTopic> initializationStarted = new CompletableFuture<>();
        CompletableFuture<Void> originalClosed = new CompletableFuture<>();
        String name = "persistent://prop/ns-abc/delete-timed-out-initialization-" + UUID.randomUUID();
        AtomicInteger initializations = new AtomicInteger();
        BrokerTestInterceptor.INSTANCE.setPersistentTopicDecorator(topic -> {
            if (!topic.getName().equals(name) || initializations.getAndIncrement() != 0) {
                return topic;
            }
            PersistentTopic decorated = spy(topic);
            doAnswer(invocation -> {
                CompletableFuture<Void> initialized = (CompletableFuture<Void>) invocation.callRealMethod();
                return initialized.thenCompose(ignored -> {
                    initializationStarted.complete(decorated);
                    return initialization;
                });
            }).when(decorated).initialize(any(TopicLoadingContext.class));
            doAnswer(invocation -> {
                CompletableFuture<Void> closing = (CompletableFuture<Void>) invocation.callRealMethod();
                closing.whenComplete((ignored, error) -> {
                    if (error == null) {
                        originalClosed.complete(null);
                    } else {
                        originalClosed.completeExceptionally(error);
                    }
                });
                return closing;
            }).when(decorated).close();
            return decorated;
        });
        try (InitializingTopicFixture fixture = new InitializingTopicFixture(initialization)) {
            fixture.setup();
            BrokerService realBroker = fixture.service().getBrokerService();
            CompletableFuture<Optional<Topic>> loading = realBroker.getTopic(name, true);
            PersistentTopic original = initializationStarted.get(10, TimeUnit.SECONDS);
            ManagedLedgerImpl originalLedger = (ManagedLedgerImpl) original.getManagedLedger();
            assertThatThrownBy(() -> loading.get(10, TimeUnit.SECONDS)).hasCauseInstanceOf(TimeoutException.class);
            assertThat(realBroker.getTopics().get(name)).isSameAs(loading);
            IllegalStateException authFailure = new IllegalStateException("Injected authorization cleanup failure");
            doAnswer(invocation -> {
                invocation.<CompletableFuture<Void>>getArgument(1).completeExceptionally(authFailure);
                return null;
            }).when(realBroker).deleteTopicAuthenticationWithRetry(eq(name), any(), anyInt());
            assertThatThrownBy(() -> realBroker.deleteTopic(name, true).get(10, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class);

            CompletableFuture<Optional<Topic>> retry = realBroker.getTopic(name, true);
            PersistentTopic replacement = retry == loading ? null
                    : (PersistentTopic) retry.get(10, TimeUnit.SECONDS).orElseThrow();
            initialization.complete(null);
            originalClosed.get(10, TimeUnit.SECONDS);
            Awaitility.await().untilAsserted(() -> assertThat(realBroker.getTopics().get(name)).isNotSameAs(loading));
            if (replacement == null) {
                replacement = (PersistentTopic) realBroker.getTopic(name, true).get(10, TimeUnit.SECONDS).orElseThrow();
            }
            assertThat(replacement.getManagedLedger()).as("late cleanup must not close the replacement ledger")
                    .isNotSameAs(originalLedger);
            assertThat(replacement.getManagedLedger().addEntry(new byte[] {1})).isNotNull();
            assertThat(realBroker.getTopicReference(name)).contains(replacement);
        } finally {
            initialization.complete(null);
            BrokerTestInterceptor.INSTANCE.reset();
        }
    }

    @DataProvider
    public Object[][] replaceQueuedLoadBeforeFailure() {
        return new Object[][] {{false}, {true}};
    }

    @Test(dataProvider = "replaceQueuedLoadBeforeFailure", timeOut = 60000)
    @SuppressWarnings("unchecked")
    public void testQueuedOwnershipFailureCleansOnlyItsGenerationAndAdvancesQueue(boolean replaceBeforeFailure)
            throws Exception {
        context.close();
        context = null;
        String name = "persistent://prop/ns-abc/queued-ownership-failure-" + UUID.randomUUID();
        String followingName = name + "-following";
        String triggerName = name + "-trigger";
        try (InitializingTopicFixture fixture = new InitializingTopicFixture(new CompletableFuture<>(), 30)) {
            fixture.setup();
            fixture.acquireOwnership(name);
            fixture.acquireOwnership(followingName);
            fixture.acquireOwnership(triggerName);
            BrokerService realBroker = fixture.service().getBrokerService();
            // Retain real unloaded metadata so deletion after the failed reload has actual work to do.
            PersistentTopic seeded = (PersistentTopic) realBroker.getTopic(name, true)
                    .get(10, TimeUnit.SECONDS).orElseThrow();
            seeded.close().get(10, TimeUnit.SECONDS);
            Awaitility.await().untilAsserted(() -> assertThat(realBroker.getTopics()).doesNotContainKey(name));
            CompletableFuture<Void> ownership = new CompletableFuture<>();
            CompletableFuture<Void> dequeued = new CompletableFuture<>();
            AtomicInteger ownershipChecks = new AtomicInteger();
            doAnswer(invocation -> {
                int check = ownershipChecks.incrementAndGet();
                if (check == 1) {
                    return CompletableFuture.completedFuture(null);
                }
                if (check == 2) {
                    dequeued.complete(null);
                    return ownership;
                }
                return invocation.callRealMethod();
            }).when(realBroker).checkTopicNsOwnership(name);
            doReturn(CompletableFuture.completedFuture(null)).when(realBroker).checkTopicNsOwnership(followingName);
            CompletableFuture<Void> queued = new CompletableFuture<>();
            CompletableFuture<Void> followingQueued = new CompletableFuture<>();
            doAnswer(invocation -> {
                TopicLoadingContext loadingContext = invocation.getArgument(0);
                CompletableFuture<Optional<Topic>> result =
                        (CompletableFuture<Optional<Topic>>) invocation.callRealMethod();
                String topicName = loadingContext.getTopicName().toString();
                if (topicName.equals(name)) {
                    queued.complete(null);
                } else if (topicName.equals(followingName)) {
                    followingQueued.complete(null);
                }
                return result;
            }).when(realBroker).loadOrCreatePersistentTopic(any());
            Semaphore originalPermits = realBroker.topicLoadRequestSemaphore.get();
            Semaphore permits = new Semaphore(0);
            realBroker.topicLoadRequestSemaphore.set(permits);
            try {
                CompletableFuture<Optional<Topic>> loading = realBroker.getTopic(name, true);
                queued.get(10, TimeUnit.SECONDS);
                CompletableFuture<Optional<Topic>> following = realBroker.getTopic(followingName, true);
                followingQueued.get(10, TimeUnit.SECONDS);
                assertThat(loading).isNotDone();
                assertThat(following).isNotDone();
                permits.release();
                realBroker.getTopic(triggerName, true).get(10, TimeUnit.SECONDS);
                dequeued.get(10, TimeUnit.SECONDS);
                assertThat(permits.availablePermits()).as("dequeued ownership check precedes permit acquisition")
                        .isEqualTo(1);
                CompletableFuture<Optional<Topic>> replacement = null;
                if (replaceBeforeFailure) {
                    replacement = new CompletableFuture<>();
                    realBroker.getTopics().put(name, replacement);
                    realBroker.loadOrCreatePersistentTopic(new TopicLoadingContext(TopicName.get(name), true,
                            replacement, mock(PulsarStats.class))).get(10, TimeUnit.SECONDS);
                }
                ServiceUnitNotReadyException failure = new ServiceUnitNotReadyException("Ownership was lost in queue");
                fixture.failOwnershipAndAwaitCleanup(ownership, failure);
                assertThatThrownBy(() -> loading.get(10, TimeUnit.SECONDS)).hasRootCause(failure);
                PersistentTopic next = (PersistentTopic) following.get(10, TimeUnit.SECONDS).orElseThrow();
                assertThat(next.getManagedLedger().addEntry(new byte[] {1})).isNotNull();
                Awaitility.await().untilAsserted(() -> assertThat(permits.availablePermits()).isEqualTo(1));
                if (replacement != null) {
                    PersistentTopic replacementTopic = (PersistentTopic) replacement.join().orElseThrow();
                    assertThat(realBroker.getTopics().get(name)).isSameAs(replacement);
                    assertThat(replacementTopic.getManagedLedger().addEntry(new byte[] {2})).isNotNull();
                } else {
                    // Retry as a client would after ownership recovers. No test removes the failed cache entry.
                    Awaitility.await().untilAsserted(() ->
                            assertThat(realBroker.deleteTopic(name, true)).succeedsWithin(1, TimeUnit.SECONDS));
                    assertThat(fixture.service().getPulsarResources().getTopicResources()
                            .persistentTopicExists(TopicName.get(name)).get(10, TimeUnit.SECONDS)).isFalse();
                    PersistentTopic recreated = (PersistentTopic) realBroker.getTopic(name, true)
                            .get(10, TimeUnit.SECONDS).orElseThrow();
                    assertThat(recreated.getManagedLedger().addEntry(new byte[] {3})).isNotNull();
                }
            } finally {
                ownership.complete(null);
                realBroker.topicLoadRequestSemaphore.set(originalPermits);
            }
        }
    }

    private static class InitializingTopicFixture extends BrokerTestBase implements AutoCloseable {
        private final CompletableFuture<Void> initialization;
        private final AtomicReference<Thread> cleanupTrackingThread = new AtomicReference<>();
        private final List<CompletableFuture<Void>> trackedCleanupTasks = new CopyOnWriteArrayList<>();

        private InitializingTopicFixture(CompletableFuture<Void> initialization) {
            this(initialization, 5);
        }

        private InitializingTopicFixture(CompletableFuture<Void> initialization, int timeoutSeconds) {
            this.initialization = initialization;
            conf.setTopicFactoryClassName(BrokerTestInterceptor.TestTopicFactory.class.getName());
            conf.setTopicLoadTimeoutSeconds(timeoutSeconds);
        }

        @Override
        protected BrokerService customizeNewBrokerService(BrokerService brokerService) {
            return spy(brokerService);
        }

        @Override
        protected void beforePulsarStart(PulsarService service) throws Exception {
            super.beforePulsarStart(service);
            ScheduledExecutorService actualExecutor = service.getExecutor();
            ScheduledExecutorService observingExecutor = mock(ScheduledExecutorService.class,
                    delegatesTo(actualExecutor));
            doAnswer(invocation -> {
                Runnable action = invocation.getArgument(0);
                if (Thread.currentThread() == cleanupTrackingThread.get()) {
                    CompletableFuture<Void> completed = new CompletableFuture<>();
                    trackedCleanupTasks.add(completed);
                    actualExecutor.execute(() -> {
                        try {
                            action.run();
                            completed.complete(null);
                        } catch (Throwable error) {
                            completed.completeExceptionally(error);
                        }
                    });
                } else {
                    actualExecutor.execute(action);
                }
                return null;
            }).when(observingExecutor).execute(any(Runnable.class));
            // Install this once before the broker starts. Re-stubbing a live PulsarService spy can race an
            // unrelated background getter and attach the executor answer to that invocation instead.
            doReturn(observingExecutor).when(service).getExecutor();
        }

        private void failOwnershipAndAwaitCleanup(CompletableFuture<Void> ownership, Throwable failure)
                throws Exception {
            assertThat(cleanupTrackingThread.getAndSet(Thread.currentThread())).isNull();
            try {
                ownership.completeExceptionally(failure);
            } finally {
                cleanupTrackingThread.set(null);
            }
            assertThat(trackedCleanupTasks).as("failed queued load must dispatch its cache cleanup").hasSize(1);
            CompletableFuture.allOf(trackedCleanupTasks.toArray(CompletableFuture[]::new))
                    .get(10, TimeUnit.SECONDS);
        }

        @Override
        protected void setup() throws Exception {
            baseSetup();
        }

        @Override
        protected void cleanup() throws Exception {
            internalCleanup();
        }

        private PulsarService service() {
            return pulsar;
        }

        private void acquireOwnership(String topicName) throws Exception {
            admin.lookups().lookupTopic(topicName);
        }

        @Override
        public void close() throws Exception {
            initialization.complete(null);
            cleanup();
        }
    }

    @Test
    public void testMetadataDeletionBlocksNewLoadAndConcurrentDeleteAndBundleUnload() throws Exception {
        String name = "persistent://public/default/load-during-delete";
        CompletableFuture<Void> deleting = new CompletableFuture<>();
        metadataDeletion.set(deleting);

        CompletableFuture<Void> deletion = broker.deleteTopic(name, true);
        CompletableFuture<Optional<Topic>> reservation = broker.getTopics().get(name);

        assertThat(reservation).as("deletion must reserve the same cache key used by topic loading").isNotNull();
        assertThat(reservation).isNotDone();
        CompletableFuture<Optional<Topic>> loading = broker.getTopic(name, true);
        assertThat(loading).isSameAs(reservation).isNotDone();
        CompletableFuture<Void> concurrentDelete = broker.deleteTopic(name, true);
        assertThat(concurrentDelete).isNotDone();
        CompletableFuture<Integer> unloading = broker.unloadServiceUnit(mock(NamespaceBundle.class),
                false, false, 60, TimeUnit.SECONDS, Map.of(name, reservation));
        assertThat(unloading).as("bundle ownership must wait for the pending metadata deletion").isNotDone();

        deleting.complete(null);

        deletion.get(5, TimeUnit.SECONDS);
        concurrentDelete.get(5, TimeUnit.SECONDS);
        assertThat(unloading.get(5, TimeUnit.SECONDS)).isEqualTo(1);
        assertThatThrownBy(() -> loading.get(5, TimeUnit.SECONDS))
                .hasCauseInstanceOf(ServiceUnitNotReadyException.class);
        assertThat(metadataDeletes.get()).isEqualTo(1);
        assertThat(broker.getTopics()).doesNotContainKey(name);
    }

    @Test
    public void testMetadataDeletionFailureReleasesReservationForRetry() throws Exception {
        String name = "persistent://public/default/delete-retry";
        CompletableFuture<Void> deleting = new CompletableFuture<>();
        metadataDeletion.set(deleting);
        CompletableFuture<Void> deletion = broker.deleteTopic(name, true);
        deleting.completeExceptionally(new IllegalStateException("Metadata unavailable"));

        assertThatThrownBy(() -> deletion.get(5, TimeUnit.SECONDS)).hasRootCauseMessage("Metadata unavailable");
        assertThat(broker.getTopics()).doesNotContainKey(name);
        metadataDeletion.set(CompletableFuture.completedFuture(null));
        broker.deleteTopic(name, true).get(5, TimeUnit.SECONDS);
        assertThat(metadataDeletes.get()).isEqualTo(2);
        assertThat(broker.getTopics()).doesNotContainKey(name);
    }

    @Test
    public void testSynchronousMetadataFailureReleasesReservation() throws Exception {
        String name = "persistent://public/default/delete-synchronous-failure";
        doAnswer(invocation -> {
            throw new IllegalStateException("Authorization cleanup failed synchronously");
        }).when(broker).deleteTopicAuthenticationWithRetry(anyString(), any(), anyInt());

        CompletableFuture<Void> deletion = broker.deleteTopic(name, true);

        assertThatThrownBy(() -> deletion.get(5, TimeUnit.SECONDS))
                .hasRootCauseMessage("Authorization cleanup failed synchronously");
        assertThat(broker.getTopics()).doesNotContainKey(name);
    }

    @Test
    public void testDeleteRechecksGenerationAfterCompletedLoadIsRead() throws Exception {
        String name = "persistent://public/default/delete-completed-generation";
        Topic oldTopic = mock(Topic.class);
        Topic newTopic = mock(Topic.class);
        when(oldTopic.deleteForcefully()).thenReturn(CompletableFuture.completedFuture(null));
        when(newTopic.deleteForcefully()).thenReturn(CompletableFuture.completedFuture(null));
        CompletableFuture<Optional<Topic>> newer = CompletableFuture.completedFuture(Optional.of(newTopic));
        CompletableFuture<Optional<Topic>> old = new CompletableFuture<>() {
            @Override
            public Optional<Topic> join() {
                // Ownership replaces the cached generation after deletion has read the old future.
                broker.getTopics().put(name, newer);
                return super.join();
            }
        };
        old.complete(Optional.of(oldTopic));
        broker.getTopics().put(name, old);

        broker.deleteTopic(name, true).get(5, TimeUnit.SECONDS);

        verify(oldTopic, never()).deleteForcefully();
        verify(newTopic).deleteForcefully();
        assertThat(metadataDeletes.get()).isZero();
    }

    @Test
    public void testOldMetadataDeleteCompletionPreservesNewerGeneration() throws Exception {
        String name = "persistent://public/default/delete-newer-generation";
        CompletableFuture<Void> deleting = new CompletableFuture<>();
        metadataDeletion.set(deleting);
        CompletableFuture<Void> deletion = broker.deleteTopic(name, true);
        CompletableFuture<Optional<Topic>> newer = CompletableFuture.completedFuture(Optional.of(mock(Topic.class)));
        broker.getTopics().put(name, newer);

        deleting.complete(null);

        deletion.get(5, TimeUnit.SECONDS);
        assertThat(broker.getTopics().get(name)).isSameAs(newer);
    }
}
