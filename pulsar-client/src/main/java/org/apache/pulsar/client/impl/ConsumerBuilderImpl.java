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

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.ConsumerConfiguration;
import org.apache.pulsar.client.api.ConsumerCryptoFailureAction;
import org.apache.pulsar.client.api.CryptoKeyReader;
import org.apache.pulsar.client.api.MessageListener;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.util.FutureUtil;


@SuppressWarnings("deprecation")
public class ConsumerBuilderImpl implements ConsumerBuilder {

    private static final long serialVersionUID = 1L;

    private final PulsarClientImpl client;
    private String topicName;
    private String subscriptionName;
    private final ConsumerConfiguration conf;

    ConsumerBuilderImpl(PulsarClientImpl client) {
        this.client = client;
        this.conf = new ConsumerConfiguration();
    }

    @Override
    public ConsumerBuilder clone() {
        try {
            return (ConsumerBuilder) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException("Failed to clone ConsumerBuilderImpl");
        }
    }

    @Override
    public Consumer subscribe() throws PulsarClientException {
        try {
            return subscribeAsync().get();
        } catch (ExecutionException e) {
            Throwable t = e.getCause();
            if (t instanceof PulsarClientException) {
                throw (PulsarClientException) t;
            } else {
                throw new PulsarClientException(t);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarClientException(e);
        }
    }

    @Override
    public CompletableFuture<Consumer> subscribeAsync() {
        if (topicName == null) {
            return FutureUtil
                    .failedFuture(new IllegalArgumentException("Topic name must be set on the producer builder"));
        }

        if (subscriptionName == null) {
            return FutureUtil.failedFuture(
                    new IllegalArgumentException("Subscription name must be set on the producer builder"));
        }

        return client.subscribeAsync(topicName, subscriptionName, conf);
    }

    @Override
    public ConsumerBuilder topic(String topicName) {
        this.topicName = topicName;
        return this;
    }

    @Override
    public ConsumerBuilder subscriptionName(String subscriptionName) {
        this.subscriptionName = subscriptionName;
        return this;
    }

    @Override
    public ConsumerBuilder ackTimeout(long ackTimeout, TimeUnit timeUnit) {
        conf.setAckTimeout(ackTimeout, timeUnit);
        return this;
    }

    @Override
    public ConsumerBuilder subscriptionType(SubscriptionType subscriptionType) {
        conf.setSubscriptionType(subscriptionType);
        return this;
    }

    @Override
    public ConsumerBuilder messageListener(MessageListener messageListener) {
        conf.setMessageListener(messageListener);
        return this;
    }

    @Override
    public ConsumerBuilder cryptoKeyReader(CryptoKeyReader cryptoKeyReader) {
        conf.setCryptoKeyReader(cryptoKeyReader);
        return this;
    }

    @Override
    public ConsumerBuilder cryptoFailureAction(ConsumerCryptoFailureAction action) {
        conf.setCryptoFailureAction(action);
        return this;
    }

    @Override
    public ConsumerBuilder receiverQueueSize(int receiverQueueSize) {
        conf.setReceiverQueueSize(receiverQueueSize);
        return this;
    }

    @Override
    public ConsumerBuilder consumerName(String consumerName) {
        conf.setConsumerName(consumerName);
        return this;
    }

    @Override
    public ConsumerBuilder priorityLevel(int priorityLevel) {
        conf.setPriorityLevel(priorityLevel);
        return this;
    }

    @Override
    public ConsumerBuilder property(String key, String value) {
        conf.setProperty(key, value);
        return this;
    }

    @Override
    public ConsumerBuilder properties(Map<String, String> properties) {
        conf.setProperties(properties);
        return this;
    }

    @Override
    public ConsumerBuilder maxTotalReceiverQueueSizeAcrossPartitions(int maxTotalReceiverQueueSizeAcrossPartitions) {
        conf.setMaxTotalReceiverQueueSizeAcrossPartitions(maxTotalReceiverQueueSizeAcrossPartitions);
        return this;
    }

    @Override
    public ConsumerBuilder readCompacted(boolean readCompacted) {
        conf.setReadCompacted(readCompacted);
        return this;
    }
}
