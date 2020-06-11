/**
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
package org.apache.pulsar.functions.worker;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerEventListener;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.ConsumerImpl;

import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class LeaderService implements AutoCloseable, ConsumerEventListener {

    private final String consumerName;
    private final FunctionAssignmentTailer functionAssignmentTailer;
    private final ErrorNotifier errorNotifier;
    private ConsumerImpl<byte[]> consumer;
    private final WorkerConfig workerConfig;
    private final PulsarClient pulsarClient;
    private final AtomicBoolean isLeader = new AtomicBoolean(false);

    static final String COORDINATION_TOPIC_SUBSCRIPTION = "participants";

    private static String WORKER_IDENTIFIER = "id";
    
    public LeaderService(WorkerService workerService,
                         PulsarClient pulsarClient,
                         FunctionAssignmentTailer functionAssignmentTailer,
                         ErrorNotifier errorNotifier) {
        this.workerConfig = workerService.getWorkerConfig();
        this.pulsarClient = pulsarClient;
        this.functionAssignmentTailer = functionAssignmentTailer;
        this.errorNotifier = errorNotifier;
        consumerName = String.format(
                "%s:%s:%d",
                workerConfig.getWorkerId(),
                workerConfig.getWorkerHostname(),
                workerConfig.getWorkerPort()
        );

    }

    public void start() throws PulsarClientException {
        // the leaders service is using a `coordination` topic for leader election.
        // we don't produce any messages into this topic, we only use the `failover` subscription
        // to elect an active consumer as the leader worker. The leader worker will be responsible
        // for scheduling snapshots for FMT and doing task assignment.
        consumer = (ConsumerImpl<byte[]>) pulsarClient.newConsumer()
                .topic(workerConfig.getClusterCoordinationTopic())
                .subscriptionName(COORDINATION_TOPIC_SUBSCRIPTION)
                .subscriptionType(SubscriptionType.Failover)
                .consumerEventListener(this)
                .property(WORKER_IDENTIFIER, consumerName)
                .subscribe();

    }

    @Override
    public void becameActive(Consumer<?> consumer, int partitionId) {
        if (isLeader.compareAndSet(false, true)) {
            log.info("Worker {} became the leader.", consumerName);
            try {
                // trigger read to the end of the topic and exit
                // Since the leader can just update its in memory assignments cache directly
                functionAssignmentTailer.triggerReadToTheEndAndExit().get();
                // close the reader
                functionAssignmentTailer.close();
            } catch (Throwable th) {
                log.error("Encountered error when initializing to become leader", th);
                errorNotifier.triggerError(th);
            }
        }
    }

    @Override
    public void becameInactive(Consumer<?> consumer, int partitionId) {
        if (isLeader.compareAndSet(true, false)) {
            log.info("Worker {} lost the leadership.", consumerName);
            // starting assignment tailer when this worker has lost leadership
            try {
                functionAssignmentTailer.start();
            } catch (Throwable th) {
                log.error("Encountered error in routine when worker lost leadership", th);
                errorNotifier.triggerError(th);
            }
        }
    }

    public boolean isLeader() {
        return isLeader.get();
    }

    @Override
    public void close() throws PulsarClientException {
        consumer.close();
    }
}
