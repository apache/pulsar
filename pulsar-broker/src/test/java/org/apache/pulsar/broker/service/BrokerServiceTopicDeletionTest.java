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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.mledger.AsyncCallbacks.DeleteLedgerCallback;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.BrokerServiceException.ServiceUnitNotReadyException;
import org.apache.pulsar.broker.testcontext.PulsarTestContext;
import org.apache.pulsar.common.naming.NamespaceBundle;
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
        } else {
            loading.complete(Optional.empty());
        }

        deletion.get(5, TimeUnit.SECONDS);
        assertThat(metadataDeletes.get()).isEqualTo(1);
        assertThat(broker.getTopics()).doesNotContainKey(name);
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
