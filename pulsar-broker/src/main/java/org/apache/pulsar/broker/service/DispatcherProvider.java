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
package org.apache.pulsar.broker.service;

import static com.google.common.base.Preconditions.checkArgument;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.nonpersistent.NonPersistentDispatcherMultipleConsumers;
import org.apache.pulsar.broker.service.nonpersistent.NonPersistentDispatcherSingleActiveConsumer;
import org.apache.pulsar.broker.service.nonpersistent.NonPersistentStickyKeyDispatcherMultipleConsumers;
import org.apache.pulsar.broker.service.nonpersistent.NonPersistentSubscription;
import org.apache.pulsar.broker.service.nonpersistent.NonPersistentTopic;
import org.apache.pulsar.broker.service.persistent.PersistentDispatcherMultipleConsumers;
import org.apache.pulsar.broker.service.persistent.PersistentDispatcherSingleActiveConsumer;
import org.apache.pulsar.broker.service.persistent.PersistentStickyKeyDispatcherMultipleConsumers;
import org.apache.pulsar.broker.service.persistent.PersistentStreamingDispatcherMultipleConsumers;
import org.apache.pulsar.broker.service.persistent.PersistentStreamingDispatcherSingleActiveConsumer;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.common.api.proto.CommandSubscribe;
import org.apache.pulsar.common.api.proto.KeySharedMeta;
import org.apache.pulsar.common.naming.TopicName;

public interface DispatcherProvider {

    Dispatcher createDispatcher(Consumer consumer, Subscription subscription);

    static DispatcherProvider createDispatcherProvider(ServiceConfiguration serviceConfiguration) {
        Class<?> providerClass;
        try {
            providerClass = Class.forName(serviceConfiguration.getDispatcherProviderClassName());
            Object obj = providerClass.getDeclaredConstructor().newInstance();
            checkArgument(obj instanceof DispatcherProvider,
                    "The factory has to be an instance of " + DispatcherProvider.class.getName());
            return (DispatcherProvider) obj;
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Slf4j
    class DefaultDispatcherProvider implements DispatcherProvider {

        @Override
        public Dispatcher createDispatcher(Consumer consumer, Subscription subscription) {
            if (subscription instanceof NonPersistentSubscription) {
                return createDispatcherForNonPersistentSubscription(consumer, subscription);
            } else if (subscription instanceof PersistentSubscription) {
                return createDispatcherForPersistentSubscription(consumer, subscription);
            }
            throw new UnsupportedOperationException("Unknown subscription type");
        }

        private Dispatcher createDispatcherForPersistentSubscription(Consumer consumer,
                                                                     Subscription subscription) {
            PersistentTopic topic = (PersistentTopic) subscription.getTopic();
            PersistentSubscription persistentSubscription = (PersistentSubscription) subscription;
            String topicName = topic.getName();
            Dispatcher previousDispatcher = subscription.getDispatcher();
            Dispatcher dispatcher = null;
            String subName = subscription.getName();
            ManagedCursor cursor = persistentSubscription.getCursor();

            boolean useStreamingDispatcher = topic.getBrokerService().getPulsar()
                    .getConfiguration().isStreamingDispatch();
            switch (consumer.subType()) {
                case Exclusive:
                    if (previousDispatcher == null
                            || previousDispatcher.getType() != CommandSubscribe.SubType.Exclusive) {
                        dispatcher = useStreamingDispatcher
                                ? new PersistentStreamingDispatcherSingleActiveConsumer(
                                cursor, CommandSubscribe.SubType.Exclusive, 0, topic, subscription)
                                : new PersistentDispatcherSingleActiveConsumer(
                                cursor, CommandSubscribe.SubType.Exclusive, 0, topic, subscription);
                    }
                    break;
                case Shared:
                    if (previousDispatcher == null || previousDispatcher.getType() != CommandSubscribe.SubType.Shared) {
                        dispatcher = useStreamingDispatcher
                                ? new PersistentStreamingDispatcherMultipleConsumers(
                                topic, cursor, subscription)
                                : new PersistentDispatcherMultipleConsumers(topic, cursor, subscription);
                    }
                    break;
                case Failover:
                    int partitionIndex = TopicName.getPartitionIndex(topicName);
                    if (partitionIndex < 0) {
                        // For non partition topics, use a negative index so
                        // dispatcher won't sort consumers before picking
                        // an active consumer for the topic.
                        partitionIndex = -1;
                    }

                    if (previousDispatcher == null
                            || previousDispatcher.getType() != CommandSubscribe.SubType.Failover) {
                        dispatcher = useStreamingDispatcher
                                ? new PersistentStreamingDispatcherSingleActiveConsumer(
                                cursor, CommandSubscribe.SubType.Failover, partitionIndex, topic, subscription) :
                                new PersistentDispatcherSingleActiveConsumer(cursor, CommandSubscribe.SubType.Failover,
                                        partitionIndex, topic, subscription);
                    }
                    break;
                case Key_Shared:
                    if (previousDispatcher == null
                            || previousDispatcher.getType() != CommandSubscribe.SubType.Key_Shared) {
                        previousDispatcher = dispatcher;
                        KeySharedMeta ksm = consumer.getKeySharedMeta();
                        dispatcher = new PersistentStickyKeyDispatcherMultipleConsumers(topic, cursor, subscription,
                                topic.getBrokerService().getPulsar().getConfiguration(), ksm);
                    }
                    break;
                default:
                    break;
            }

            if (previousDispatcher != null) {
                previousDispatcher.close().thenRun(() -> {
                    log.info("[{}][{}] Successfully closed previous dispatcher", topicName, subName);
                }).exceptionally(ex -> {
                    log.error("[{}][{}] Failed to close previous dispatcher", topicName, subName, ex);
                    return null;
                });
            }

            return dispatcher;
        }

        private Dispatcher createDispatcherForNonPersistentSubscription(Consumer consumer,
                                                                        Subscription subscription) {
            NonPersistentTopic topic = (NonPersistentTopic) subscription.getTopic();
            String topicName = topic.getName();
            Dispatcher previousDispatcher = subscription.getDispatcher();
            Dispatcher dispatcher = null;
            String subName = subscription.getName();

            switch (consumer.subType()) {
                case Exclusive:
                    if (previousDispatcher == null
                            || previousDispatcher.getType() != CommandSubscribe.SubType.Exclusive) {
                        dispatcher = new NonPersistentDispatcherSingleActiveConsumer(CommandSubscribe.SubType.Exclusive,
                                0, topic, subscription);
                    }
                    break;
                case Shared:
                    if (previousDispatcher == null || previousDispatcher.getType() != CommandSubscribe.SubType.Shared) {
                        dispatcher = new NonPersistentDispatcherMultipleConsumers(topic, subscription);
                    }
                    break;
                case Failover:
                    int partitionIndex = TopicName.getPartitionIndex(topicName);
                    if (partitionIndex < 0) {
                        // For non partition topics, assume index 0 to pick a predictable consumer
                        partitionIndex = 0;
                    }

                    if (previousDispatcher == null
                            || previousDispatcher.getType() != CommandSubscribe.SubType.Failover) {
                        dispatcher = new NonPersistentDispatcherSingleActiveConsumer(CommandSubscribe.SubType.Failover,
                                partitionIndex, topic, subscription);
                    }
                    break;
                case Key_Shared:
                    if (previousDispatcher == null
                            || previousDispatcher.getType() != CommandSubscribe.SubType.Key_Shared) {

                        switch (consumer.getKeySharedMeta().getKeySharedMode()) {
                            case STICKY:
                                dispatcher = new NonPersistentStickyKeyDispatcherMultipleConsumers(topic,
                                        subscription, new HashRangeExclusiveStickyKeyConsumerSelector());
                                break;

                            case AUTO_SPLIT:
                            default:
                                StickyKeyConsumerSelector selector;
                                ServiceConfiguration conf = topic.getBrokerService().getPulsar().getConfiguration();
                                if (conf.isSubscriptionKeySharedUseConsistentHashing()) {
                                    selector = new ConsistentHashingStickyKeyConsumerSelector(
                                            conf.getSubscriptionKeySharedConsistentHashingReplicaPoints());
                                } else {
                                    selector = new HashRangeAutoSplitStickyKeyConsumerSelector();
                                }

                                dispatcher = new NonPersistentStickyKeyDispatcherMultipleConsumers(topic, subscription,
                                        selector);
                                break;
                        }
                    }
                    break;
                default:
                    break;
            }
            if (previousDispatcher != null) {
                previousDispatcher.close().thenRun(() -> {
                    log.info("[{}][{}] Successfully closed previous dispatcher", topicName, subName);
                }).exceptionally(ex -> {
                    log.error("[{}][{}] Failed to close previous dispatcher", topicName, subName, ex);
                    return null;
                });
            }
            return dispatcher;
        }
    }
}
