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
package org.apache.pulsar.client.impl;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import com.google.common.util.concurrent.MoreExecutors;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionMode;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.client.impl.metrics.InstrumentProvider;
import org.apache.pulsar.client.util.ExecutorProvider;

/**
 * Constructs real parent and child consumers while transport lookup remains pending.
 * Cleanup uses a direct executor; each state closes its parent, children and executor in the arbiter.
 */
final class MultiTopicsPauseStressSupport implements AutoCloseable {
    private static final String TOPIC = "non-persistent://public/default/pause-jcstress";
    final MultiTopicsConsumerImpl<byte[]> parent;
    private final int partitions;
    private final ExecutorService executor = MoreExecutors.newDirectExecutorService();

    MultiTopicsPauseStressSupport(boolean initiallyPaused, int partitions) {
        this.partitions = partitions;
        PulsarClientImpl client = mock(PulsarClientImpl.class);
        ExecutorProvider external = mock(ExecutorProvider.class);
        when(external.getExecutor()).thenReturn(executor);
        ClientConfigurationData clientConf = new ClientConfigurationData();
        clientConf.setStatsIntervalSeconds(0);
        when(client.getConfiguration()).thenReturn(clientConf);
        when(client.getCnxPool()).thenReturn(mock(ConnectionPool.class));
        when(client.getInternalExecutorService()).thenReturn(executor);
        when(client.externalExecutorProvider()).thenReturn(external);
        when(client.instrumentProvider()).thenReturn(InstrumentProvider.NOOP);
        when(client.getConnection(anyString(), anyInt())).thenReturn(new CompletableFuture<>());
        when(client.preProcessSchemaBeforeSubscribe(client, Schema.BYTES, TOPIC))
                .thenReturn(CompletableFuture.completedFuture(Schema.BYTES));

        ConsumerConfigurationData<byte[]> conf = new ConsumerConfigurationData<>();
        conf.setSubscriptionName("pause-jcstress");
        conf.setConsumerName("pause-jcstress");
        conf.setSubscriptionMode(SubscriptionMode.NonDurable);
        conf.setReceiverQueueSize(4);
        conf.setAutoUpdatePartitions(false);
        conf.setStartPaused(initiallyPaused);
        parent = new MultiTopicsConsumerImpl<>(client, conf, external,
                new CompletableFuture<>(), Schema.BYTES, null, true);
    }

    void subscribe() {
        // Known partition metadata keeps construction synchronous without completing broker subscriptions.
        // Surface an immediate construction error instead of reporting it as a pause-state outcome.
        parent.subscribeAsync(TOPIC, partitions).getNow(null);
    }

    int mismatchedChildren(boolean expectedPaused) {
        int mismatches = 0;
        for (ConsumerImpl<byte[]> child : parent.consumers.values()) {
            if (child.paused != expectedPaused) {
                mismatches++;
            }
        }
        return mismatches;
    }

    @Override
    public void close() {
        try {
            parent.closeAsync().get(10, TimeUnit.SECONDS);
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while closing consumers", error);
        } catch (ExecutionException | TimeoutException error) {
            throw new IllegalStateException("Could not close consumers", error);
        } finally {
            executor.shutdownNow();
        }
    }
}
