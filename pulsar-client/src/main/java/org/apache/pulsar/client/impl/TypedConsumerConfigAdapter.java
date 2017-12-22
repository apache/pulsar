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
package org.apache.pulsar.client.impl;

import org.apache.pulsar.client.api.*;

class TypedConsumerConfigAdapter<T> implements ConsumerConfig<byte[]> {
    private final ConsumerConfig<T> typedConfig;
    private final Codec<T> codec;

    private TypedConsumerImpl<T> typedConsumer;

    TypedConsumerConfigAdapter(ConsumerConfig<T> typedConfig, Codec<T> codec) {
        this.typedConfig = typedConfig;
        this.codec = codec;
    }

    public void setTypedConsumer(TypedConsumerImpl<T> typedConsumer) {
        this.typedConsumer = typedConsumer;
    }

    @Override
    public long getAckTimeoutMillis() {
        return typedConfig.getAckTimeoutMillis();
    }

    @Override
    public SubscriptionType getSubscriptionType() {
        return typedConfig.getSubscriptionType();
    }

    @Override
    public MessageListener<byte[]> getMessageListener() {
        MessageListener<T> listener = typedConfig.getMessageListener();
        return new MessageListener<byte[]>() {
            @Override
            public void received(Consumer<byte[]> consumer, Message<byte[]> msg) {
                listener.received(typedConsumer, new TypedMessageImpl<>(msg, codec));
            }

            @Override
            public void reachedEndOfTopic(Consumer<byte[]> consumer) {
                listener.reachedEndOfTopic(typedConsumer);
            }
        };
    }

    @Override
    public int getReceiverQueueSize() {
        return typedConfig.getReceiverQueueSize();
    }

    @Override
    public CryptoKeyReader getCryptoKeyReader() {
        return typedConfig.getCryptoKeyReader();
    }

    @Override
    public ConsumerCryptoFailureAction getCryptoFailureAction() {
        return typedConfig.getCryptoFailureAction();
    }

    @Override
    public String getConsumerName() {
        return typedConfig.getConsumerName();
    }

    @Override
    public int getPriorityLevel() {
        return typedConfig.getPriorityLevel();
    }
}
