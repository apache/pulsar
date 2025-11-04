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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.ToLongFunction;
import org.apache.pulsar.common.api.proto.CommandSubscribe;
import org.apache.pulsar.common.api.proto.KeySharedMeta;
import org.apache.pulsar.common.util.FutureUtil;

public abstract class AbstractSubscription implements Subscription {
    protected final LongAdder bytesOutFromRemovedConsumers = new LongAdder();
    protected final LongAdder msgOutFromRemovedConsumer = new LongAdder();

    public long getMsgOutCounter() {
        return msgOutFromRemovedConsumer.longValue() + sumConsumers(Consumer::getMsgOutCounter);
    }

    public long getBytesOutCounter() {
        return bytesOutFromRemovedConsumers.longValue() + sumConsumers(Consumer::getBytesOutCounter);
    }

    private long sumConsumers(ToLongFunction<Consumer> toCounter) {
        return Optional.ofNullable(getDispatcher())
                .map(dispatcher -> dispatcher.getConsumers().stream().mapToLong(toCounter).sum())
                .orElse(0L);
    }

    /**
     * Checks if the given consumer is compatible with the given dispatcher. They are incompatible if
     * <ul>
     * <li>the subscription type of the consumer differs from the subscription type of the dispatcher or</li>
     * <li>if both the consumer and dispatcher are of {@link CommandSubscribe.SubType#Key_Shared} type but
     * their policies differ ({@link org.apache.pulsar.common.api.proto.KeySharedMode#AUTO_SPLIT}/
     * {@link org.apache.pulsar.common.api.proto.KeySharedMode#STICKY} or allowOutOfOrderDelivery true/false).</li>
     * </ul>
     * @param dispatcher The dispatcher of the subscription
     * @param consumer New consumer to be added to the subscription
     * @return Optional containing failed future with {@link BrokerServiceException.SubscriptionBusyException} if
     * consumer and dispatcher are incompatible or empty optional otherwise.
     */
    protected Optional<CompletableFuture<Void>> checkForConsumerCompatibilityErrorWithDispatcher(Dispatcher dispatcher,
                                                                                                 Consumer consumer) {
        if (consumer.subType() != dispatcher.getType()) {
            return Optional.of(FutureUtil.failedFuture(new BrokerServiceException.SubscriptionBusyException(
                    String.format("Subscription is of different type. Active subscription type of '%s' is different "
                                    + "than the connecting consumer's type '%s'.",
                            dispatcher.getType(), consumer.subType()))));
        } else if (dispatcher.getType() == CommandSubscribe.SubType.Key_Shared) {
            KeySharedMeta dispatcherKsm = dispatcher.getConsumers().get(0).getKeySharedMeta();
            KeySharedMeta consumerKsm = consumer.getKeySharedMeta();
            if (dispatcherKsm.getKeySharedMode() != consumerKsm.getKeySharedMode()) {
                return Optional.of(FutureUtil.failedFuture(new BrokerServiceException.SubscriptionBusyException(
                        String.format("Subscription is of different type. Active subscription key_shared mode of '%s' "
                                        + "is different than the connecting consumer's key_shared mode '%s'.",
                                dispatcherKsm.getKeySharedMode(), consumerKsm.getKeySharedMode()))));
            }
            if (dispatcherKsm.isAllowOutOfOrderDelivery() != consumerKsm.isAllowOutOfOrderDelivery()) {
                return Optional.of(FutureUtil.failedFuture(new BrokerServiceException.SubscriptionBusyException(
                        String.format("Subscription is of different type. %s",
                                dispatcherKsm.isAllowOutOfOrderDelivery()
                                        ? "Active subscription allows out of order delivery while the connecting "
                                        + "consumer does not allow it." :
                                        "Active subscription does not allow out of order delivery while the connecting "
                                                + "consumer allows it."))));
            }
        }
        return Optional.empty();
    }
}
