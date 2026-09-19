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

import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.persistent.AbstractPersistentDispatcherMultipleConsumers;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.metadata.bookkeeper.BKCluster;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.ThreadParams;

/**
 * Isolates the production Consumer -> PersistentSubscription -> dispatcher -> BrokerService accounting path.
 * No reflection, fake counters or mocked subscription/dispatcher. Network, pending-ack map and persistence
 * are outside the measured region. One operation credits and debits one message (two accounting updates).
 * The paired updates keep all balances bounded even when threads make progress at different rates.
 * Compare revisions with identical JMH/JVM options; these results are not end-to-end broker throughput.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
public class ConsumerUnackedMessagesBenchmark {
    @State(Scope.Benchmark)
    public static class AccountingState {
        @Param({"false", "true"})
        public boolean classic;
        private int consumerLimit;
        private BKCluster bookies;
        private PulsarService pulsar;
        private PulsarClient client;
        private Consumer[] consumers;
        private AbstractPersistentDispatcherMultipleConsumers[] dispatchers;

        @Setup(Level.Trial)
        public void setup(BenchmarkParams params) throws Exception {
            try {
                initialize(params.getThreads());
            } catch (Exception e) {
                try {
                    close();
                } catch (Exception cleanupFailure) {
                    e.addSuppressed(cleanupFailure);
                }
                throw e;
            }
        }

        boolean separateSubscriptions() {
            return false;
        }

        Consumer consumerAt(int index) {
            return consumers[index];
        }

        void initialize(int consumerCount) throws Exception {
            String metadataUrl = "memory:unacked-benchmark-" + UUID.randomUUID();
            bookies = BKCluster.builder().metadataServiceUri(metadataUrl).numBookies(1).build();
            ServiceConfiguration config = new ServiceConfiguration();
            config.setMetadataStoreUrl(metadataUrl);
            config.setConfigurationMetadataStoreUrl(metadataUrl);
            config.setClusterName("benchmark");
            config.setAdvertisedAddress("localhost");
            config.setBrokerServicePort(Optional.of(0));
            config.setWebServicePort(Optional.of(0));
            config.setManagedLedgerDefaultEnsembleSize(1);
            config.setManagedLedgerDefaultWriteQuorum(1);
            config.setManagedLedgerDefaultAckQuorum(1);
            config.setDefaultNumberOfNamespaceBundles(1);
            config.setLoadBalancerEnabled(false);
            config.setBrokerDeleteInactiveTopicsEnabled(false);
            config.setBrokerShutdownTimeoutMs(0L);
            config.setNumIOThreads(2);
            config.setNumExecutorThreadPoolSize(2);
            config.setNumOrderedExecutorThreads(2);
            config.setNumHttpServerThreads(4);
            config.setBookkeeperClientNumWorkerThreads(2);
            config.setBookkeeperClientNumIoThreads(2);
            config.setManagedLedgerNumSchedulerThreads(2);
            config.setManagedLedgerCacheSizeMB(8);
            config.setTopicOrderedExecutorThreadNum(2);
            config.setSubscriptionSharedUseClassicPersistentImplementation(classic);
            config.setMaxUnackedMessagesPerConsumer(consumerLimit);
            config.setMaxUnackedMessagesPerSubscription(0);
            config.setMaxUnackedMessagesPerBroker(1_000_000);
            pulsar = new PulsarService(config);
            pulsar.start();
            try (PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(pulsar.getWebServiceAddress()).build()) {
                admin.clusters().createCluster("benchmark", ClusterData.builder()
                        .serviceUrl(pulsar.getWebServiceAddress())
                        .brokerServiceUrl(pulsar.getBrokerServiceUrl()).build());
                admin.tenants().createTenant("benchmark", TenantInfo.builder()
                        .allowedClusters(Set.of("benchmark")).build());
                admin.namespaces().createNamespace("benchmark/ns", Set.of("benchmark"));
            }
            client = PulsarClient.builder().serviceUrl(pulsar.getBrokerServiceUrl()).build();
            String topicName = "persistent://benchmark/ns/accounting";
            for (int i = 0; i < consumerCount; i++) {
                // PulsarClient owns and closes these real client consumers at trial teardown.
                client.newConsumer().topic(topicName)
                        .subscriptionName(separateSubscriptions() ? "sub-" + i : "sub")
                        .subscriptionType(SubscriptionType.Shared).consumerName("consumer-" + i).subscribe();
            }
            PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService()
                    .getTopicReference(topicName).orElseThrow();
            dispatchers = new AbstractPersistentDispatcherMultipleConsumers[
                    separateSubscriptions() ? consumerCount : 1];
            consumers = new Consumer[consumerCount];
            if (separateSubscriptions()) {
                for (int i = 0; i < consumerCount; i++) {
                    dispatchers[i] = (AbstractPersistentDispatcherMultipleConsumers)
                            topic.getSubscription("sub-" + i).getDispatcher();
                    consumers[i] = dispatchers[i].getConsumers().get(0);
                }
            } else {
                dispatchers[0] = (AbstractPersistentDispatcherMultipleConsumers)
                        topic.getSubscription("sub").getDispatcher();
                consumers = dispatchers[0].getConsumers().toArray(Consumer[]::new);
            }
            if (consumers.length != consumerCount) {
                throw new IllegalStateException("Unexpected number of registered consumers");
            }
            Consumer owner = consumers[0];
            owner.addAndGetUnAckedMsgs(owner, 7);
            if (owner.getUnackedMessages() != 7 || dispatchers[0].getTotalUnackedMessages() != 7
                    || pulsar.getBrokerService().getTotalUnackedMessages() != 7) {
                throw new IllegalStateException("Fixture did not execute real accounting");
            }
            owner.addAndGetUnAckedMsgs(owner, -7);
            verifyBalance();
        }

        @TearDown(Level.Iteration)
        public void verifyBalance() {
            for (Consumer consumer : consumers) {
                if (consumer.getUnackedMessages() != 0) {
                    throw new IllegalStateException("Unbalanced consumer");
                }
            }
            for (AbstractPersistentDispatcherMultipleConsumers dispatcher : dispatchers) {
                if (dispatcher.getTotalUnackedMessages() != 0) {
                    throw new IllegalStateException("Unbalanced subscription");
                }
            }
            if (pulsar.getBrokerService().getTotalUnackedMessages() != 0) {
                throw new IllegalStateException("Unbalanced broker");
            }
        }

        @TearDown(Level.Trial)
        public void close() throws Exception {
            try {
                if (client != null) {
                    client.close();
                }
            } finally {
                try {
                    if (pulsar != null) {
                        pulsar.close();
                    }
                } finally {
                    if (bookies != null) {
                        bookies.close();
                    }
                }
            }
        }
    }

    @State(Scope.Benchmark)
    public static class SeparateSubscriptionsState extends AccountingState {
        @Override
        boolean separateSubscriptions() {
            return true;
        }
    }

    @Benchmark
    @Threads(2)
    public int separateSubscriptions(SeparateSubscriptionsState state, ThreadParams thread) {
        Consumer consumer = state.consumerAt(thread.getThreadIndex());
        return consumer.addAndGetUnAckedMsgs(consumer, 1) + consumer.addAndGetUnAckedMsgs(consumer, -1);
    }

    /**
     * Measures the real persistence-completion handler, including pending-map removal and accounting.
     * Invocation setup restores pending entries and balances; network and persistence are not timed.
     * Report ns per command, dividing by entries (not duplicate IDs) for cost per successful entry.
     * Invocation-level setup also contributes to GC profiler totals, so allocation is for the full fixture cycle.
     */
    @State(Scope.Thread)
    public static class CompletionState {
        @Param({"false", "true"})
        public boolean classic;
        @Param({"0", "50000"})
        public int consumerLimit;
        @Param({"1", "16", "64", "256"})
        public int entries;
        @Param({"same", "alternating", "duplicate"})
        public String owners;
        private AccountingState accounting;
        private Consumer.PendingAckCompletions completions;
        private Position[] positions;

        @Setup(Level.Trial)
        public void setup() throws Exception {
            accounting = new AccountingState();
            accounting.classic = classic;
            accounting.consumerLimit = consumerLimit;
            try {
                accounting.initialize(2);
                positions = new Position[entries];
                completions = new Consumer.PendingAckCompletions(entries * 2);
                for (int i = 0; i < entries; i++) {
                    Position position = PositionFactory.create(1, i);
                    positions[i] = position;
                    Consumer owner = ownerAt(i);
                    completions.add(owner, position, false, 1);
                    if (owners.equals("duplicate") && i % 2 == 0) {
                        completions.add(owner, position, false, 1);
                    }
                }
                prepare();
                accounting.consumers[0].applyPendingAckCompletions(completions);
                verify();
            } catch (Exception e) {
                try {
                    close();
                } catch (Exception cleanupFailure) {
                    e.addSuppressed(cleanupFailure);
                }
                throw e;
            }
        }

        private Consumer ownerAt(int index) {
            return accounting.consumers[owners.equals("alternating") ? index % 2 : 0];
        }

        @Setup(Level.Invocation)
        public void prepare() {
            for (int i = 0; i < entries; i++) {
                Consumer owner = ownerAt(i);
                Position position = positions[i];
                if (!owner.getPendingAcks().addPendingAckIfAllowed(
                        position.getLedgerId(), position.getEntryId(), 1, 0)) {
                    throw new IllegalStateException("Cannot restore pending entry");
                }
                owner.addAndGetUnAckedMsgs(owner, 1);
            }
        }

        @TearDown(Level.Invocation)
        public void verify() {
            accounting.verifyBalance();
            for (Consumer consumer : accounting.consumers) {
                if (consumer.getPendingAcks().size() != 0) {
                    throw new IllegalStateException("Completion left pending entries");
                }
            }
        }

        @TearDown(Level.Trial)
        public void close() throws Exception {
            accounting.close();
        }
    }

    @Benchmark
    @Threads(1)
    public void groupedCompletion(CompletionState state) {
        state.accounting.consumers[0].applyPendingAckCompletions(state.completions);
    }

    @Benchmark
    @Threads(1)
    public int singleThread(AccountingState state) {
        Consumer consumer = state.consumers[0];
        return consumer.addAndGetUnAckedMsgs(consumer, 1) + consumer.addAndGetUnAckedMsgs(consumer, -1);
    }

    @Benchmark
    @Threads(2)
    public int sameConsumer(AccountingState state) {
        Consumer consumer = state.consumers[0];
        return consumer.addAndGetUnAckedMsgs(consumer, 1) + consumer.addAndGetUnAckedMsgs(consumer, -1);
    }

    @Benchmark
    @Threads(2)
    public int separateConsumers(AccountingState state, ThreadParams thread) {
        Consumer consumer = state.consumers[thread.getThreadIndex()];
        return consumer.addAndGetUnAckedMsgs(consumer, 1) + consumer.addAndGetUnAckedMsgs(consumer, -1);
    }
}
