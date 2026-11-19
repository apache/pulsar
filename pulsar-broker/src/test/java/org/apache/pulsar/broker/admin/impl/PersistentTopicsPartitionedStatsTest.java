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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import jakarta.ws.rs.container.AsyncResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.broker.resources.NamespaceResources;
import org.apache.pulsar.broker.resources.NamespaceResources.PartitionedTopicResources;
import org.apache.pulsar.broker.service.GetStatsOptions;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.Topics;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.ErrorData;
import org.apache.pulsar.common.policies.data.TopicOperation;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.apache.pulsar.common.policies.data.stats.PartitionedTopicStatsImpl;
import org.apache.pulsar.common.policies.data.stats.TopicStatsImpl;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;

@Test(groups = "broker-admin")
public class PersistentTopicsPartitionedStatsTest {

    @Test
    public void testFailedExistenceCheckResumesRequest() throws Exception {
        PartitionedStatsTopics topics = new PartitionedStatsTopics(2);
        topics.requestStats(true);
        topics.failPartitionStats();

        verify(topics.partitionedTopicResources).partitionedTopicExistsAsync(topics.statsTopicName);
        verifyNoInteractions(topics.response);

        TimeoutException failure = new TimeoutException("partitioned topic metadata read timed out");
        topics.exists.completeExceptionally(failure);

        RestException error = resumedError(topics.response);
        assertThat(error.getResponse().getStatus()).isEqualTo(500);
        assertThat(error.getResponse().getEntity()).isInstanceOfSatisfying(ErrorData.class,
                data -> assertThat(data.reason).contains(failure.getClass().getName(), failure.getMessage()));
    }

    @Test
    public void testExistingTopicReturnsPlaceholderWhenAllPartitionStatsFail() throws Exception {
        PartitionedStatsTopics topics = new PartitionedStatsTopics(2);
        topics.requestStats(true);
        topics.failPartitionStats();
        verifyNoInteractions(topics.response);

        topics.exists.complete(true);

        PartitionedTopicStatsImpl stats = resumedStats(topics.response);
        assertThat(stats.getPartitions()).containsOnlyKeys(topics.statsTopicName.toString());
        assertThat(stats.getPartitions().get(topics.statsTopicName.toString())).isInstanceOf(TopicStatsImpl.class);
        assertThat(stats.getMetadata().partitions).isEqualTo(2);
        assertThat(stats.msgRateIn).isZero();
    }

    @Test
    public void testMissingTopicReturnsNotFoundWhenAllPartitionStatsFail() throws Exception {
        PartitionedStatsTopics topics = new PartitionedStatsTopics(2);
        topics.requestStats(true);
        topics.failPartitionStats();
        verifyNoInteractions(topics.response);

        topics.exists.complete(false);

        RestException error = resumedError(topics.response);
        assertThat(error.getResponse().getStatus()).isEqualTo(404);
        assertThat(error.getMessage()).isEqualTo("Internal topics have not been generated yet");
    }

    @Test
    public void testSuccessfulPartitionStatsAreAggregatedWithoutExistenceCheck() throws Exception {
        PartitionedStatsTopics topics = new PartitionedStatsTopics(3);
        topics.requestStats(true);

        TopicStatsImpl first = new TopicStatsImpl();
        first.msgRateIn = 3;
        TopicStatsImpl second = new TopicStatsImpl();
        second.msgRateIn = 4;
        topics.partitionStats.get(0).complete(first);
        topics.partitionStats.get(1).complete(second);
        verifyNoInteractions(topics.response);
        topics.partitionStats.get(2).completeExceptionally(new IllegalStateException("partition unavailable"));

        PartitionedTopicStatsImpl stats = resumedStats(topics.response);
        assertThat(stats.msgRateIn).isEqualTo(7);
        assertThat(stats.getPartitions()).containsOnlyKeys(topics.statsTopicName.getPartition(0).toString(),
                topics.statsTopicName.getPartition(1).toString());
        assertThat(stats.getPartitions().get(topics.statsTopicName.getPartition(0).toString())).isSameAs(first);
        assertThat(stats.getPartitions().get(topics.statsTopicName.getPartition(1).toString())).isSameAs(second);
        verifyNoInteractions(topics.partitionedTopicResources);
    }

    @Test
    public void testAllPartitionStatsFailWithoutPerPartitionReturnsEmptyAggregate() throws Exception {
        PartitionedStatsTopics topics = new PartitionedStatsTopics(2);
        topics.requestStats(false);
        topics.failPartitionStats();

        PartitionedTopicStatsImpl stats = resumedStats(topics.response);
        assertThat(stats.getMetadata().partitions).isEqualTo(2);
        assertThat(stats.getPartitions()).isEmpty();
        assertThat(stats.msgRateIn).isZero();
        verifyNoInteractions(topics.partitionedTopicResources);
    }

    @Test
    public void testZeroPartitionsReturnsNotFound() throws Exception {
        PartitionedStatsTopics topics = new PartitionedStatsTopics(0);
        topics.requestStats(true);

        assertThat(resumedError(topics.response).getResponse().getStatus()).isEqualTo(404);
        verifyNoInteractions(topics.namespaceService, topics.adminTopics, topics.partitionedTopicResources);
    }

    private static RestException resumedError(AsyncResponse response) {
        ArgumentCaptor<RestException> result = ArgumentCaptor.forClass(RestException.class);
        verify(response).resume(result.capture());
        verifyNoMoreInteractions(response);
        return result.getValue();
    }

    private static PartitionedTopicStatsImpl resumedStats(AsyncResponse response) {
        ArgumentCaptor<PartitionedTopicStatsImpl> result = ArgumentCaptor.forClass(PartitionedTopicStatsImpl.class);
        verify(response).resume(result.capture());
        verifyNoMoreInteractions(response);
        return result.getValue();
    }

    private static class PartitionedStatsTopics extends PersistentTopicsBase {
        private final TopicName statsTopicName = TopicName.get("persistent://tenant/namespace/topic");
        private final PulsarService pulsarService = mock(PulsarService.class);
        private final NamespaceService namespaceService = mock(NamespaceService.class);
        private final NamespaceResources namespaceResources = mock(NamespaceResources.class);
        private final PartitionedTopicResources partitionedTopicResources = mock(PartitionedTopicResources.class);
        private final Topics adminTopics = mock(Topics.class);
        private final AsyncResponse response = mock(AsyncResponse.class);
        private final List<CompletableFuture<TopicStats>> partitionStats = new ArrayList<>();
        private final CompletableFuture<Boolean> exists = new CompletableFuture<>();
        private final PartitionedTopicMetadata metadata;

        private PartitionedStatsTopics(int partitions) throws Exception {
            topicName = statsTopicName;
            namespaceName = topicName.getNamespaceObject();
            metadata = new PartitionedTopicMetadata(partitions);
            PulsarAdmin admin = mock(PulsarAdmin.class);
            when(pulsarService.getNamespaceService()).thenReturn(namespaceService);
            when(pulsarService.getAdminClient()).thenReturn(admin);
            when(admin.topics()).thenReturn(adminTopics);
            when(namespaceService.isServiceUnitOwnedAsync(any())).thenReturn(CompletableFuture.completedFuture(false));
            when(namespaceResources.getPartitionedTopicResources()).thenReturn(partitionedTopicResources);
            when(partitionedTopicResources.partitionedTopicExistsAsync(topicName)).thenReturn(exists);
            for (int i = 0; i < partitions; i++) {
                CompletableFuture<TopicStats> stats = new CompletableFuture<>();
                partitionStats.add(stats);
                when(adminTopics.getStatsAsync(eq(topicName.getPartition(i).toString()), any())).thenReturn(stats);
            }
        }

        private void requestStats(boolean perPartition) {
            internalGetPartitionedStats(response, false, perPartition, GetStatsOptions.builder().build());
        }

        private void failPartitionStats() {
            partitionStats.forEach(stats ->
                    stats.completeExceptionally(new IllegalStateException("partition unavailable")));
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
        public CompletableFuture<Void> validateTopicOperationAsync(TopicName topic, TopicOperation operation) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        protected CompletableFuture<Void> validateGlobalNamespaceOwnershipAsync(NamespaceName namespace) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        protected CompletableFuture<PartitionedTopicMetadata> getPartitionedTopicMetadataAsync(
                TopicName topic, boolean authoritative, boolean checkAllowAutoCreation) {
            return CompletableFuture.completedFuture(metadata);
        }
    }
}
