/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.pulsar.broker.service;

import java.util.concurrent.CompletableFuture;

import com.yahoo.pulsar.common.api.proto.PulsarApi.CommandSubscribe.SubType;
import com.yahoo.pulsar.utils.CopyOnWriteArrayList;

public interface Dispatcher {
    void addConsumer(Consumer consumer) throws BrokerServiceException;

    void removeConsumer(Consumer consumer) throws BrokerServiceException;

    /**
     * Indicates that this consumer is now ready to receive more messages
     *
     * @param consumer
     */
    void consumerFlow(Consumer consumer, int additionalNumberOfMessages);

    boolean isConsumerConnected();

    CopyOnWriteArrayList<Consumer> getConsumers();

    boolean canUnsubscribe(Consumer consumer);

    CompletableFuture<Void> disconnect();

    SubType getType();

    void redeliverUnacknowledgedMessages(Consumer consumer);
}
