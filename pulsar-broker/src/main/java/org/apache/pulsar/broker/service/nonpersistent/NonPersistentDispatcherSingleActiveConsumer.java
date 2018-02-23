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
package org.apache.pulsar.broker.service.nonpersistent;

import static org.apache.pulsar.broker.service.Consumer.getBatchSizeforEntry;

import java.util.List;

import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.util.Rate;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.AbstractDispatcherSingleActiveConsumer;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandSubscribe.SubType;

public final class NonPersistentDispatcherSingleActiveConsumer extends AbstractDispatcherSingleActiveConsumer implements NonPersistentDispatcher {

    private final NonPersistentTopic topic;
    private final Rate msgDrop;
    private final Subscription subscription;
    private final ServiceConfiguration serviceConfig;

    public NonPersistentDispatcherSingleActiveConsumer(SubType subscriptionType, int partitionIndex,
            NonPersistentTopic topic, Subscription subscription) {
        super(subscriptionType, partitionIndex, topic.getName());
        this.topic = topic;
        this.subscription = subscription;
        this.msgDrop = new Rate();
        this.serviceConfig = topic.getBrokerService().pulsar().getConfiguration();
    }

    @Override
    public void sendMessages(List<Entry> entries) {
        Consumer currentConsumer = ACTIVE_CONSUMER_UPDATER.get(this);
        if (currentConsumer != null && currentConsumer.getAvailablePermits() > 0 && currentConsumer.isWritable()) {
            currentConsumer.sendMessages(entries);
        } else {
            entries.forEach(entry -> {
                int totalMsgs = getBatchSizeforEntry(entry.getDataBuffer(), subscription, -1);
                if (totalMsgs > 0) {
                    msgDrop.recordEvent();
                }
                entry.release();
            });
        }
    }

    protected boolean isConsumersExceededOnTopic() {
        final int maxConsumersPerTopic = serviceConfig.getMaxConsumersPerTopic();
        if (maxConsumersPerTopic > 0 && maxConsumersPerTopic <= topic.getNumberOfConsumers()) {
            return true;
        }
        return false;
    }

    protected boolean isConsumersExceededOnSubscription() {
        final int maxConsumersPerSubscription = serviceConfig.getMaxConsumersPerSubscription();
        if (maxConsumersPerSubscription > 0 && maxConsumersPerSubscription <= consumers.size()) {
            return true;
        }
        return false;
    }

    @Override
    public Rate getMesssageDropRate() {
        return msgDrop;
    }

    @Override
    public boolean hasPermits() {
        return ACTIVE_CONSUMER_UPDATER.get(this) != null && ACTIVE_CONSUMER_UPDATER.get(this).getAvailablePermits() > 0;
    }

    @Override
    public void consumerFlow(Consumer consumer, int additionalNumberOfMessages) {
        // No-op
    }

    @Override
    protected void scheduleReadOnActiveConsumer() {
        // No-op
    }

    @Override
    protected void readMoreEntries(Consumer consumer) {
        // No-op
    }

    @Override
    protected void cancelPendingRead() {
        // No-op
    }
}
