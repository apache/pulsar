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

@Slf4j
public class LeaderService implements AutoCloseable, ConsumerEventListener {

    private final String consumerName;
    private final FunctionAssignmentTailer functionAssignmentTailer;
    private final ErrorNotifier errorNotifier;
    private final SchedulerManager schedulerManager;
    private final FunctionRuntimeManager functionRuntimeManager;
    private final FunctionMetaDataManager functionMetaDataManager;
    private ConsumerImpl<byte[]> consumer;
    private final WorkerConfig workerConfig;
    private final PulsarClient pulsarClient;
    private boolean isLeader = false;

    static final String COORDINATION_TOPIC_SUBSCRIPTION = "participants";

    private static String WORKER_IDENTIFIER = "id";

    public LeaderService(WorkerService workerService,
                         PulsarClient pulsarClient,
                         FunctionAssignmentTailer functionAssignmentTailer,
                         SchedulerManager schedulerManager,
                         FunctionRuntimeManager functionRuntimeManager,
                         FunctionMetaDataManager functionMetaDataManager,
                         ErrorNotifier errorNotifier) {
        this.workerConfig = workerService.getWorkerConfig();
        this.pulsarClient = pulsarClient;
        this.functionAssignmentTailer = functionAssignmentTailer;
        this.schedulerManager = schedulerManager;
        this.functionRuntimeManager = functionRuntimeManager;
        this.functionMetaDataManager = functionMetaDataManager;
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
                .consumerName(consumerName)
                .subscribe();
    }

    @Override
    public void becameActive(Consumer<?> consumer, int partitionId) {
        synchronized (this) {
            if (isLeader) return;
            log.info("Worker {} became the leader.", consumerName);
            try {

                // Wait for worker to be initialized.
                // We need to do this because LeaderService is started
                // before FunctionMetadataManager and FunctionRuntimeManager is done initializing
                functionMetaDataManager.getIsInitialized().get();
                functionRuntimeManager.getIsInitialized().get();

                // make sure scheduler is initialized because this worker
                // is the leader and may need to start computing and writing assignments
                // also creates exclusive producer for assignment topic
                schedulerManager.initialize();

                // trigger read to the end of the topic and exit
                // Since the leader can just update its in memory assignments cache directly
                functionAssignmentTailer.triggerReadToTheEndAndExit().get();
                functionAssignmentTailer.close();

                // need to move function meta data manager into leader mode
                functionMetaDataManager.acquireLeadership();

                isLeader = true;
            } catch (Throwable th) {
                log.error("Encountered error when initializing to become leader", th);
                errorNotifier.triggerError(th);
            }
        }

        // Once we become leader we need to schedule
        schedulerManager.schedule();
    }

    @Override
    public synchronized void becameInactive(Consumer<?> consumer, int partitionId) {
        if (isLeader) {
            log.info("Worker {} lost the leadership.", consumerName);
            isLeader = false;
            // when a worker has lost leadership it needs to start reading from the assignment topic again
            try {
                // stop scheduler manager since we are not the leader anymore
                // will block if a schedule invocation is in process
                schedulerManager.close();

                // starting reading from assignment topic from the last published message of the scheduler
                if (schedulerManager.getLastMessageProduced() == null) {
                    functionAssignmentTailer.start();
                } else {
                    functionAssignmentTailer.startFromMessage(schedulerManager.getLastMessageProduced());
                }
                functionMetaDataManager.giveupLeadership();
            } catch (Throwable th) {
                log.error("Encountered error in routine when worker lost leadership", th);
                errorNotifier.triggerError(th);
            }
        }
    }

    public synchronized boolean isLeader() {
        return isLeader;
    }

    @Override
    public void close() throws PulsarClientException {
        if (consumer != null) {
            consumer.close();
        }
    }
}
