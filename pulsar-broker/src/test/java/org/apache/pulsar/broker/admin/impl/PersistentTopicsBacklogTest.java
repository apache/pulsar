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
package org.apache.pulsar.broker.admin.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.resources.NamespaceResources;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.PersistentOfflineTopicStats;
import org.apache.pulsar.common.policies.data.Policies;
import org.testng.annotations.Test;

@Test(groups = "broker-admin")
public class PersistentTopicsBacklogTest {

    @Test
    public void testBacklogRequestWaitsForAsyncEstimateBeforeCaching() throws Exception {
        BacklogTopics topics = new BacklogTopics();
        CompletableFuture<Void> estimate = new CompletableFuture<>();
        AtomicReference<PersistentOfflineTopicStats> collected = new AtomicReference<>();
        doAnswer(invocation -> {
            collected.set(invocation.getArgument(0));
            return estimate;
        }).when(topics.factory).estimateUnloadedTopicBacklogAsync(any(), eq(topics.backlogTopicName), eq(false),
                isNull(), eq(topics.executor));

        CompletableFuture<PersistentOfflineTopicStats> response = topics.internalGetBacklogAsync(false);
        assertThat(response).isNotDone();
        verify(topics.brokerService, never()).cacheOfflineTopicStats(any(), any());
        verify(topics.factory, never()).estimateUnloadedTopicBacklog(any(), any(), anyBoolean(), any());

        collected.get().messageBacklog = 3;
        estimate.complete(null);
        PersistentOfflineTopicStats stats = response.get(5, TimeUnit.SECONDS);
        assertThat(stats.messageBacklog).isEqualTo(3);
        verify(topics.brokerService).cacheOfflineTopicStats(topics.backlogTopicName, stats);
    }

    @Test
    public void testFailedBacklogEstimateIsNotCached() {
        BacklogTopics topics = new BacklogTopics();
        CompletableFuture<Void> estimate = new CompletableFuture<>();
        when(topics.factory.estimateUnloadedTopicBacklogAsync(any(), eq(topics.backlogTopicName), eq(false),
                isNull(), eq(topics.executor))).thenReturn(estimate);

        CompletableFuture<PersistentOfflineTopicStats> response = topics.internalGetBacklogAsync(false);
        TimeoutException failure = new TimeoutException("metadata read timed out");
        estimate.completeExceptionally(failure);
        assertThatThrownBy(() -> response.get(5, TimeUnit.SECONDS)).hasCause(failure);
        verify(topics.brokerService, never()).cacheOfflineTopicStats(any(), any());
    }

    @Test
    public void testCachedBacklogSkipsEstimation() throws Exception {
        BacklogTopics topics = new BacklogTopics();
        PersistentOfflineTopicStats cached = new PersistentOfflineTopicStats("topic", "broker");
        when(topics.brokerService.getOfflineTopicStat(topics.backlogTopicName)).thenReturn(cached);

        assertThat(topics.internalGetBacklogAsync(false).get(5, TimeUnit.SECONDS)).isSameAs(cached);
        verifyNoInteractions(topics.factory);
        verify(topics.brokerService, never()).getManagedLedgerConfig(any());
    }

    private static class BacklogTopics extends PersistentTopicsBase {
        private final TopicName backlogTopicName = TopicName.get("persistent://tenant/namespace/topic");
        private final PulsarService pulsarService = mock(PulsarService.class);
        private final BrokerService brokerService = mock(BrokerService.class);
        private final NamespaceResources namespaceResources = mock(NamespaceResources.class);
        private final ManagedLedgerFactory factory = mock(ManagedLedgerFactory.class);
        private final ScheduledExecutorService executor = mock(ScheduledExecutorService.class);

        private BacklogTopics() {
            topicName = backlogTopicName;
            namespaceName = topicName.getNamespaceObject();
            ManagedLedgerConfig config = new ManagedLedgerConfig();
            when(pulsarService.getBrokerService()).thenReturn(brokerService);
            when(pulsarService.getExecutor()).thenReturn(executor);
            when(namespaceResources.getPoliciesAsync(namespaceName))
                    .thenReturn(CompletableFuture.completedFuture(Optional.of(new Policies())));
            when(brokerService.getManagedLedgerConfig(topicName))
                    .thenReturn(CompletableFuture.completedFuture(config));
            when(brokerService.getManagedLedgerFactoryForTopic(topicName, config.getStorageClassName()))
                    .thenReturn(factory);
        }

        @Override
        protected PulsarService pulsar() {
            return pulsarService;
        }

        @Override
        protected NamespaceResources namespaceResources() {
            return namespaceResources;
        }

        @Override
        protected CompletableFuture<Void> validateGlobalNamespaceOwnershipAsync(NamespaceName namespace) {
            return CompletableFuture.completedFuture(null);
        }
    }
}
