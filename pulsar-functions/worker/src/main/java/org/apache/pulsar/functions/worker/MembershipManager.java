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

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerConfiguration;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.policies.data.ConsumerStats;
import org.apache.pulsar.common.policies.data.PersistentTopicStats;

/**
 * A simple implementation of leader election using a pulsar topic.
 */
@Slf4j
public class MembershipManager implements AutoCloseable {

    private final Producer producer;
    private final Consumer consumer;
    private WorkerConfig workerConfig;
    private PulsarAdmin pulsarAdminClient;

    // need to trigger when membership changes
    private SchedulerManager schedulerManager;

    private static final String COORDINATION_TOPIC_SUBSCRIPTION = "participants";

    MembershipManager(WorkerConfig workerConfig, SchedulerManager schedulerManager, PulsarClient client)
            throws PulsarClientException {
        producer = client.createProducer(workerConfig.getClusterCoordinationTopic());
        consumer = client.subscribe(workerConfig.getClusterCoordinationTopic(), COORDINATION_TOPIC_SUBSCRIPTION,
                new ConsumerConfiguration()
                        .setSubscriptionType(SubscriptionType.Failover)
                        .setConsumerName(workerConfig.getWorkerId()));
        this.workerConfig = workerConfig;
        this.schedulerManager = schedulerManager;
    }

    public CompletableFuture<Boolean> becomeLeader() {
        long time = System.currentTimeMillis();
        String str = String.format("%s-%d", this.workerConfig.getWorkerId(), time);
        return producer.sendAsync(str.getBytes())
                .thenCompose(messageId -> MembershipManager.this.receiveTillMessage((MessageIdImpl) messageId))
                .exceptionally(cause -> false);
    }

    private CompletableFuture<Boolean> receiveTillMessage(MessageIdImpl endMsgId) {
        CompletableFuture<Boolean> finalFuture = new CompletableFuture<>();
        receiveOne(endMsgId, finalFuture);
        return finalFuture;
    }

    private void receiveOne(MessageIdImpl endMsgId, CompletableFuture<Boolean> finalFuture) {
        consumer.receiveAsync()
                .thenAccept(message -> {
                    MessageIdImpl idReceived = (MessageIdImpl) message.getMessageId();

                    int compareResult = idReceived.compareTo(endMsgId);
                    if (compareResult < 0) {
                        // drop the message
                        consumer.acknowledgeCumulativeAsync(message);
                        // receive next message
                        receiveOne(endMsgId, finalFuture);
                        return;
                    } else if (compareResult > 0) {
                        // the end message is consumed by other participants, which it means some other
                        // consumers take over the leadership at some time. so `becomeLeader` fails
                        finalFuture.complete(false);
                        return;
                    } else {
                        // i got what I published, i become the leader
                        consumer.acknowledgeCumulativeAsync(message);
                        finalFuture.complete(true);
                        return;
                    }
                })
                .exceptionally(cause -> {
                    finalFuture.completeExceptionally(cause);
                    return null;
                });
    }

    public List<String> getCurrentMembership() {

        List<String> workerIds = new LinkedList<>();
        PersistentTopicStats persistentTopicStats = null;
        PulsarAdmin pulsarAdmin = this.getPulsarAdminClient();
        try {
            persistentTopicStats = pulsarAdmin.persistentTopics().getStats(
                    this.workerConfig.getClusterCoordinationTopic());
        } catch (PulsarAdminException e) {
            e.printStackTrace();
        }

        for (ConsumerStats consumerStats : persistentTopicStats.subscriptions
                .get(COORDINATION_TOPIC_SUBSCRIPTION).consumers) {
            workerIds.add(consumerStats.consumerName);
        }
        return workerIds;
    }

    private PulsarAdmin getPulsarAdminClient() {
        if (this.pulsarAdminClient == null) {
            this.pulsarAdminClient = Utils.getPulsarAdminClient(this.workerConfig.getPulsarWebServiceUrl());
        }
        return this.pulsarAdminClient;
    }

    @Override
    public void close() throws PulsarClientException {
        producer.close();
        consumer.close();
        if (this.pulsarAdminClient != null) {
            this.pulsarAdminClient.close();
        }
    }

}