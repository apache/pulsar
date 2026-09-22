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
import static org.mockito.Answers.RETURNS_SELF;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.Topics;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker-replication")
public class GeoPersistentReplicatorPrepareCreateProducerTest {

    private static final String TOPIC = "persistent://prop/ns/tp";
    private static final String LEGACY_TOPIC = TOPIC + "-partition-0";

    private ExecutorService adminResponseExecutor;
    private Topics localTopics;
    private Topics remoteTopics;
    private Queue<String> localLookups;

    @BeforeMethod
    public void setup() {
        adminResponseExecutor = Executors.newSingleThreadExecutor();
        // Stub only: the lookups are recorded in "localLookups", so that Mockito does not retain every invocation
        // if the lookups are not bounded.
        localTopics = mock(Topics.class, withSettings().stubOnly());
        remoteTopics = mock(Topics.class);
        localLookups = new ConcurrentLinkedQueue<>();
    }

    @AfterMethod(alwaysRun = true)
    public void cleanup() {
        // If the lookups are not bounded, this is also what stops them.
        adminResponseExecutor.shutdownNow();
    }

    @DataProvider
    public Object[][] missingLocalTopics() {
        return new Object[][] {
                // The partitioned topic name of a regular topic is the topic itself: there is nothing to fall back to.
                {TOPIC, List.of(TOPIC)},
                // Looked up as a partition first, then only once as a legacy non-partitioned topic.
                {LEGACY_TOPIC, List.of(TOPIC, LEGACY_TOPIC)},
        };
    }

    /**
     * The local broker answers 404 for as long as the local topic is missing, e.g. because it was deleted while the
     * replicator was starting. That must fail the preparation, so that {@code startProducer()} backs off, instead of
     * looking the topic up again forever.
     */
    @Test(dataProvider = "missingLocalTopics")
    public void testLocalTopicNotFoundFailsPreparation(String topic, List<String> expectedLocalLookups)
            throws Exception {
        when(localTopics.getPartitionedTopicMetadataAsync(anyString())).thenAnswer(invocation -> {
            localLookups.add(invocation.getArgument(0));
            return respond(notFound(), null);
        });

        assertThat(newReplicator(topic).prepareCreateProducer())
                .as("preparation while the local topic is not found")
                .failsWithin(5, TimeUnit.SECONDS)
                .withThrowableOfType(ExecutionException.class)
                .withCauseInstanceOf(PulsarAdminException.NotFoundException.class);
        assertThat(localLookups).as("local partitioned metadata lookups")
                .containsExactlyElementsOf(expectedLocalLookups);
        verifyNoInteractions(remoteTopics);
    }

    @Test
    public void testLegacyNonPartitionedTopicWithPartitionSuffix() throws Exception {
        when(localTopics.getPartitionedTopicMetadataAsync(anyString())).thenAnswer(invocation -> {
            String topic = invocation.getArgument(0);
            localLookups.add(topic);
            // There is no partitioned topic "tp": "tp-partition-0" is a non-partitioned topic.
            return LEGACY_TOPIC.equals(topic)
                    ? respond(null, new PartitionedTopicMetadata(0))
                    : respond(notFound(), null);
        });
        when(remoteTopics.getPartitionedTopicMetadataAsync(anyString()))
                .thenAnswer(__ -> respond(notFound(), null));
        when(remoteTopics.createNonPartitionedTopicAsync(anyString())).thenAnswer(__ -> respond(null, null));

        assertThat(newReplicator(LEGACY_TOPIC).prepareCreateProducer())
                .as("preparation of a legacy non-partitioned topic")
                .succeedsWithin(5, TimeUnit.SECONDS);
        assertThat(localLookups).as("local partitioned metadata lookups").containsExactly(TOPIC, LEGACY_TOPIC);
        verify(remoteTopics).createNonPartitionedTopicAsync(LEGACY_TOPIC);
        verify(remoteTopics, never()).createNonPartitionedTopicAsync(TOPIC);
        verify(remoteTopics, never()).createPartitionedTopicAsync(anyString(), anyInt());
    }

    private static PulsarAdminException notFound() {
        return new PulsarAdminException.NotFoundException(null, "Topic not found", 404);
    }

    /**
     * Completes from another thread, like the admin client does with a HTTP response.
     */
    private <T> CompletableFuture<T> respond(Throwable error, T value) {
        CompletableFuture<T> future = new CompletableFuture<>();
        adminResponseExecutor.execute(() -> {
            if (error != null) {
                future.completeExceptionally(error);
            } else {
                future.complete(value);
            }
        });
        return future;
    }

    @SuppressWarnings("unchecked")
    private GeoPersistentReplicator newReplicator(String topicName) throws Exception {
        ServiceConfiguration configuration = new ServiceConfiguration();
        configuration.setClusterName("local");

        PulsarAdmin localAdmin = mock(PulsarAdmin.class);
        when(localAdmin.topics()).thenReturn(localTopics);
        PulsarAdmin remoteAdmin = mock(PulsarAdmin.class);
        when(remoteAdmin.topics()).thenReturn(remoteTopics);

        PulsarService pulsar = mock(PulsarService.class);
        when(pulsar.getConfiguration()).thenReturn(configuration);
        when(pulsar.getConfig()).thenReturn(configuration);
        when(pulsar.getClient()).thenReturn(mock(PulsarClientImpl.class));
        when(pulsar.getAdminClient()).thenReturn(localAdmin);

        BrokerService brokerService = mock(BrokerService.class);
        when(brokerService.pulsar()).thenReturn(pulsar);
        when(brokerService.getPulsar()).thenReturn(pulsar);

        PulsarClientImpl replicationClient = mock(PulsarClientImpl.class);
        when(replicationClient.newProducer(any(Schema.class))).thenReturn(mock(ProducerBuilder.class, RETURNS_SELF));

        PersistentTopic topic = mock(PersistentTopic.class);
        when(topic.getName()).thenReturn(topicName);
        when(topic.getReplicatorPrefix()).thenReturn("pulsar.repl");
        when(topic.getBrokerService()).thenReturn(brokerService);

        ManagedCursor cursor = mock(ManagedCursor.class);
        when(cursor.getName()).thenReturn("pulsar.repl.remote");

        return new GeoPersistentReplicator(topic, cursor, "local", "remote", brokerService, replicationClient,
                remoteAdmin) {
            @Override
            protected void startProducer() {
                // The tests call prepareCreateProducer() directly.
            }
        };
    }
}
