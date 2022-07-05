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

import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import static org.apache.pulsar.broker.cache.ConfigurationCacheService.POLICIES;

import java.util.List;

import org.apache.bookkeeper.mledger.Entry;
import org.apache.pulsar.broker.admin.AdminResource;
import org.apache.pulsar.broker.service.AbstractDispatcherSingleActiveConsumer;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.EntryBatchSizes;
import org.apache.pulsar.broker.service.RedeliveryTracker;
import org.apache.pulsar.broker.service.RedeliveryTrackerDisabled;
import org.apache.pulsar.broker.service.SendMessageInfo;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.common.policies.data.TopicPolicies;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.stats.Rate;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandSubscribe.SubType;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.Policies;

@Slf4j
public final class NonPersistentDispatcherSingleActiveConsumer extends AbstractDispatcherSingleActiveConsumer implements NonPersistentDispatcher {

    private final NonPersistentTopic topic;
    private final Rate msgDrop;
    private final Subscription subscription;
    private final RedeliveryTracker redeliveryTracker;

    public NonPersistentDispatcherSingleActiveConsumer(SubType subscriptionType, int partitionIndex,
            NonPersistentTopic topic, Subscription subscription) {
        super(subscriptionType, partitionIndex, topic.getName(), subscription,
                topic.getBrokerService().pulsar().getConfiguration(), null);
        this.topic = topic;
        this.subscription = subscription;
        this.msgDrop = new Rate();
        this.redeliveryTracker = RedeliveryTrackerDisabled.REDELIVERY_TRACKER_DISABLED;
    }

    @Override
    public void sendMessages(List<Entry> entries) {
        Consumer currentConsumer = ACTIVE_CONSUMER_UPDATER.get(this);
        if (currentConsumer != null && currentConsumer.getAvailablePermits() > 0 && currentConsumer.isWritable()) {
            SendMessageInfo sendMessageInfo = SendMessageInfo.getThreadLocal();
            EntryBatchSizes batchSizes = EntryBatchSizes.get(entries.size());
            filterEntriesForConsumer(entries, batchSizes, sendMessageInfo, null, null, false);
            currentConsumer.sendMessages(entries, batchSizes, null, sendMessageInfo.getTotalMessages(),
                    sendMessageInfo.getTotalBytes(), sendMessageInfo.getTotalChunkedMessages(), getRedeliveryTracker());
        } else {
            entries.forEach(entry -> {
                int totalMsgs = Commands.getNumberOfMessagesInBatch(entry.getDataBuffer(), subscription.toString(), -1);
                if (totalMsgs > 0) {
                    msgDrop.recordEvent(totalMsgs);
                }
                entry.release();
            });
        }
    }

    protected boolean isConsumersExceededOnSubscription() {
        Policies policies = null;
        Integer maxConsumersPerSubscription = null;
        try {
            maxConsumersPerSubscription = Optional.ofNullable(topic.getBrokerService()
                    .getTopicPolicies(TopicName.get(topicName)))
                    .map(TopicPolicies::getMaxConsumersPerSubscription)
                    .orElse(null);
            if (maxConsumersPerSubscription == null) {
                // Use getDataIfPresent from zk cache to make the call non-blocking and prevent deadlocks in addConsumer
                policies = topic.getBrokerService().pulsar().getConfigurationCache().policiesCache()
                        .getDataIfPresent(AdminResource.path(POLICIES, TopicName.get(topic.getName()).getNamespace()));

                if (policies == null) {
                    policies = new Policies();
                }
            }
        } catch (Exception e) {
            policies = new Policies();
        }

        if (maxConsumersPerSubscription == null) {
            maxConsumersPerSubscription = policies.max_consumers_per_subscription > 0 ?
                    policies.max_consumers_per_subscription :
                    serviceConfig.getMaxConsumersPerSubscription();
        }

        if (maxConsumersPerSubscription > 0 && maxConsumersPerSubscription <= consumers.size()) {
            return true;
        }
        return false;
    }

    @Override
    public Rate getMessageDropRate() {
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
    public RedeliveryTracker getRedeliveryTracker() {
        return redeliveryTracker;
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
