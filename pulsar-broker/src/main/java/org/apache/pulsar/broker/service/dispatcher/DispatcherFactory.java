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
package org.apache.pulsar.broker.service.dispatcher;

import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.ConsistentHashingStickyKeyConsumerSelector;
import org.apache.pulsar.broker.service.HashRangeAutoSplitStickyKeyConsumerSelector;
import org.apache.pulsar.broker.service.HashRangeExclusiveStickyKeyConsumerSelector;
import org.apache.pulsar.broker.service.StickyKeyConsumerSelector;
import org.apache.pulsar.broker.service.nonpersistent.NonPersistentDispatcherMultipleConsumers;
import org.apache.pulsar.broker.service.nonpersistent.NonPersistentDispatcherSingleActiveConsumer;
import org.apache.pulsar.broker.service.nonpersistent.NonPersistentStickyKeyDispatcherMultipleConsumers;
import org.apache.pulsar.broker.service.nonpersistent.NonPersistentTopic;
import org.apache.pulsar.broker.service.persistent.PersistentDispatcherMultipleConsumers;
import org.apache.pulsar.broker.service.persistent.PersistentDispatcherSingleActiveConsumer;
import org.apache.pulsar.broker.service.persistent.PersistentStickyKeyDispatcherMultipleConsumers;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.common.api.proto.PulsarApi;

import java.io.IOException;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Factory class for creating {@link Dispatcher}.
 */
public class DispatcherFactory {

    /**
     * Get proper dispatcher based on passed in config, could be default or customized dispatcher.
     * @param dispatcherConfiguration  Holds parameters for creating default dispatchers.
     * @param serviceConfiguration     For creating customized dispatcher.
     * @return dispatcher
     * @throws BrokerServiceException
     */
    public static Dispatcher getDispatcher(DispatcherConfiguration dispatcherConfiguration,
                                           ServiceConfiguration serviceConfiguration)
            throws BrokerServiceException {
        if (dispatcherConfiguration.getSubscription() instanceof PersistentSubscription) {
            checkArgument(dispatcherConfiguration.getCursor() != null);
            switch (dispatcherConfiguration.getSubType()) {
                case Exclusive:
                    if (serviceConfiguration.getPersistentDispatcherExclusive() == null) {
                        return new PersistentDispatcherSingleActiveConsumer(dispatcherConfiguration.getCursor(),
                                PulsarApi.CommandSubscribe.SubType.Exclusive, 0,
                                (PersistentTopic) dispatcherConfiguration.getTopic(), dispatcherConfiguration.getSubscription());
                    } else {
                        return loadAndInitDispatcher(serviceConfiguration.getPersistentDispatcherExclusive(), dispatcherConfiguration);
                    }
                case Shared:
                    if (serviceConfiguration.getPersistentDispatcherShared() == null) {
                        return new PersistentDispatcherMultipleConsumers((PersistentTopic) dispatcherConfiguration.getTopic(),
                                dispatcherConfiguration.getCursor(), dispatcherConfiguration.getSubscription());
                    } else {
                        return loadAndInitDispatcher(serviceConfiguration.getPersistentDispatcherShared(),  dispatcherConfiguration);
                    }
                case Failover:
                    if (serviceConfiguration.getPersistentDispatcherFailover() == null) {
                        return new PersistentDispatcherSingleActiveConsumer(dispatcherConfiguration.getCursor(),
                                PulsarApi.CommandSubscribe.SubType.Failover, dispatcherConfiguration.getPartitionIndex(),
                                (PersistentTopic) dispatcherConfiguration.getTopic(), dispatcherConfiguration.getSubscription());
                    } else {
                        return loadAndInitDispatcher(serviceConfiguration.getPersistentDispatcherFailover(), dispatcherConfiguration);
                    }
                case Key_Shared:
                    checkArgument(dispatcherConfiguration.getKsm() != null);
                    if (serviceConfiguration.getPersistentDispatcherKeyShared() == null) {
                        return new PersistentStickyKeyDispatcherMultipleConsumers((PersistentTopic) dispatcherConfiguration.getTopic(),
                                dispatcherConfiguration.getCursor(), dispatcherConfiguration.getSubscription(),
                                serviceConfiguration, dispatcherConfiguration.getKsm());
                    } else {
                        return loadAndInitDispatcher(serviceConfiguration.getPersistentDispatcherKeyShared(), dispatcherConfiguration);
                    }
                default:
                    throw new BrokerServiceException.ServerMetadataException("Unsupported subscription type");
            }
        } else {
            switch (dispatcherConfiguration.getSubType()) {
                case Exclusive:
                    if (serviceConfiguration.getNonpersistentDispatcherExclusive() == null) {
                        return new NonPersistentDispatcherSingleActiveConsumer(PulsarApi.CommandSubscribe.SubType.Exclusive,
                                0, (NonPersistentTopic) dispatcherConfiguration.getTopic(),
                                dispatcherConfiguration.getSubscription());
                    } else {
                        return loadAndInitDispatcher(serviceConfiguration.getNonpersistentDispatcherExclusive(), dispatcherConfiguration);
                    }
                case Shared:
                    if (serviceConfiguration.getNonpersistentDispatcherShared() == null) {
                        return new NonPersistentDispatcherMultipleConsumers((NonPersistentTopic) dispatcherConfiguration.getTopic(),
                                dispatcherConfiguration.getSubscription());
                    } else {
                        return loadAndInitDispatcher(serviceConfiguration.getNonpersistentDispatcherShared(),  dispatcherConfiguration);
                    }
                case Failover:
                    if (serviceConfiguration.getNonpersistentDispatcherFailover() == null) {
                        return new NonPersistentDispatcherSingleActiveConsumer(PulsarApi.CommandSubscribe.SubType.Failover,
                                dispatcherConfiguration.getPartitionIndex(),
                                (NonPersistentTopic) dispatcherConfiguration.getTopic(), dispatcherConfiguration.getSubscription());
                    } else {
                        return loadAndInitDispatcher(serviceConfiguration.getNonpersistentDispatcherFailover(), dispatcherConfiguration);
                    }
                case Key_Shared:
                    checkArgument(dispatcherConfiguration.getKsm() != null);
                    if (serviceConfiguration.getNonpersistentDispatcherKeyShared() == null) {
                        switch (dispatcherConfiguration.getKsm().getKeySharedMode()) {
                            case STICKY:
                                return new NonPersistentStickyKeyDispatcherMultipleConsumers((NonPersistentTopic) dispatcherConfiguration.getTopic(),
                                        dispatcherConfiguration.getSubscription(), new HashRangeExclusiveStickyKeyConsumerSelector());
                            case AUTO_SPLIT:
                            default:
                                StickyKeyConsumerSelector selector;
                                if (serviceConfiguration.isSubscriptionKeySharedUseConsistentHashing()) {
                                    selector = new ConsistentHashingStickyKeyConsumerSelector(
                                            serviceConfiguration.getSubscriptionKeySharedConsistentHashingReplicaPoints());
                                } else {
                                    selector = new HashRangeAutoSplitStickyKeyConsumerSelector();
                                }

                                return new NonPersistentStickyKeyDispatcherMultipleConsumers((NonPersistentTopic) dispatcherConfiguration.getTopic(),
                                        dispatcherConfiguration.getSubscription(), selector);
                        }
                    } else {
                        return loadAndInitDispatcher(serviceConfiguration.getNonpersistentDispatcherKeyShared(), dispatcherConfiguration);
                    }
                default:
                    throw new BrokerServiceException.ServerMetadataException("Unsupported subscription type");
            }
        }
    }

    private static Dispatcher loadAndInitDispatcher(String dispatcherClassName,
                                  DispatcherConfiguration dispatcherConfiguration) throws BrokerServiceException {
        try {
            Dispatcher dispatcher = DispatcherUtils.load(dispatcherClassName);
            dispatcher.init(dispatcherConfiguration);
            return dispatcher;
        } catch (IOException e) {
            throw new BrokerServiceException(e);
        }
    }
}
