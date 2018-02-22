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

import static com.google.common.base.Preconditions.checkArgument;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.ConsumerCryptoFailureAction;
import org.apache.pulsar.client.api.CryptoKeyReader;
import org.apache.pulsar.client.api.MessageListener;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.common.util.FutureUtil;

import com.google.common.collect.Lists;

public class ConsumerBuilderImpl implements ConsumerBuilder {

    private static final long serialVersionUID = 1L;

    private final PulsarClientImpl client;
    private final ConsumerConfigurationData conf = new ConsumerConfigurationData();

    private static long MIN_ACK_TIMEOUT_MILLIS = 1000;

    ConsumerBuilderImpl(PulsarClientImpl client) {
        this.client = client;
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
        if (conf.getTopicNames().isEmpty()) {
            return FutureUtil
                    .failedFuture(new IllegalArgumentException("Topic name must be set on the consumer builder"));
        }

        if (conf.getSubscriptionName() == null) {
            return FutureUtil.failedFuture(
                    new IllegalArgumentException("Subscription name must be set on the consumer builder"));
        }

        return client.subscribeAsync(conf);
    }

    @Override
    public ConsumerBuilder topic(String... topicNames) {
        checkArgument(topicNames.length > 0, "Passed in topicNames should not be empty.");
        conf.getTopicNames().addAll(Lists.newArrayList(topicNames));
        return this;
    }

    @Override
    public ConsumerBuilder topics(List<String> topicNames) {
        checkArgument(topicNames != null && !topicNames.isEmpty(), "Passed in topicNames list should not be empty.");
        conf.getTopicNames().addAll(topicNames);

        return this;
    }

    @Override
    public ConsumerBuilder subscriptionName(String subscriptionName) {
        conf.setSubscriptionName(subscriptionName);
        return this;
    }

    @Override
    public ConsumerBuilder ackTimeout(long ackTimeout, TimeUnit timeUnit) {
        checkArgument(timeUnit.toMillis(ackTimeout) >= MIN_ACK_TIMEOUT_MILLIS,
                "Ack timeout should be should be greater than " + MIN_ACK_TIMEOUT_MILLIS + " ms");
        conf.setAckTimeoutMillis(timeUnit.toMillis(ackTimeout));
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
        conf.getProperties().put(key, value);
        return this;
    }

    @Override
    public ConsumerBuilder properties(Map<String, String> properties) {
        conf.getProperties().putAll(properties);
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
