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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import java.time.Clock;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.qos.MonotonicClock;
import org.apache.pulsar.broker.service.BacklogQuotaManager;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.stats.OpenTelemetryReplicatedSubscriptionStats;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.impl.BacklogQuotaImpl;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

/** Measures the publish-path max-read-position callback with and without replicated-subscription snapshots. */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
public class ReplicatedSubscriptionTimestampBenchmark {

    @Param({"false", "true"})
    public boolean controllerEnabled;

    private PersistentTopic topic;
    private Position oldPosition;
    private Position newPosition;

    @Setup
    @SuppressWarnings("unchecked")
    public void setup() {
        PulsarService pulsar = mock(PulsarService.class);
        ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
        @SuppressWarnings("rawtypes")
        ScheduledFuture timer = mock(ScheduledFuture.class);
        ServiceConfiguration config = new ServiceConfiguration();
        config.setClusterName("local");
        config.setEnableReplicatedSubscriptions(true);
        MonotonicClock monotonicClock = System::nanoTime;
        BacklogQuotaManager backlogQuotaManager = mock(BacklogQuotaManager.class);
        when(backlogQuotaManager.getDefaultQuota()).thenReturn(BacklogQuotaImpl.builder()
                .limitSize(0)
                .limitTime(0)
                .retentionPolicy(BacklogQuota.RetentionPolicy.producer_request_hold)
                .build());
        BrokerService brokerService = mock(BrokerService.class);
        when(brokerService.getClock()).thenReturn(Clock.systemUTC());
        when(brokerService.pulsar()).thenReturn(pulsar);
        when(brokerService.getPulsar()).thenReturn(pulsar);
        when(brokerService.getBacklogQuotaManager()).thenReturn(backlogQuotaManager);
        when(pulsar.getExecutor()).thenReturn(executor);
        when(pulsar.getConfiguration()).thenReturn(config);
        when(pulsar.getOpenTelemetryReplicatedSubscriptionStats())
                .thenReturn(mock(OpenTelemetryReplicatedSubscriptionStats.class));
        when(pulsar.getMonotonicClock()).thenReturn(monotonicClock);
        when(executor.scheduleAtFixedRate(any(Runnable.class), anyLong(), anyLong(), any(TimeUnit.class)))
                .thenReturn(timer);

        TestPersistentTopic testTopic = new TestPersistentTopic("persistent://public/default/benchmark",
                brokerService, mock(ManagedLedger.class), mock(MessageDeduplication.class));
        if (controllerEnabled) {
            testTopic.setReplicationClusters(List.of("local", "remote"));
            PersistentSubscription subscription = mock(PersistentSubscription.class);
            when(subscription.isReplicated()).thenReturn(true);
            testTopic.getSubscriptions().put("sub", subscription);
            testTopic.checkReplicatedSubscriptionControllerState();
        }
        topic = testTopic;
        oldPosition = PositionFactory.create(1, 1);
        newPosition = PositionFactory.create(1, 2);
    }

    @Benchmark
    public long maxReadPositionMovedForward() {
        topic.getMaxReadPositionCallBack().maxReadPositionMovedForward(oldPosition, newPosition);
        return topic.getLastMaxReadPositionMovedForwardTimestamp();
    }

    @TearDown
    public void tearDown() {
        topic.getReplicatedSubscriptionController().ifPresent(ReplicatedSubscriptionsController::close);
    }

    private static final class TestPersistentTopic extends PersistentTopic {
        TestPersistentTopic(String topic, BrokerService brokerService, ManagedLedger ledger,
                            MessageDeduplication messageDeduplication) {
            super(topic, brokerService, ledger, messageDeduplication);
        }

        void setReplicationClusters(List<String> clusters) {
            topicPolicies.getReplicationClusters().updateBrokerValue(clusters);
        }
    }
}
