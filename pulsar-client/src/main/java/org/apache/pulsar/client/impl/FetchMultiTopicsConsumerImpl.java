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
package org.apache.pulsar.client.impl;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.FetchConsumer;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageIdAdv;
import org.apache.pulsar.client.api.Messages;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.client.util.ExecutorProvider;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.FutureUtil;


public class FetchMultiTopicsConsumerImpl<T> extends MultiTopicsConsumerImpl<T> implements FetchConsumer<T> {

    public FetchMultiTopicsConsumerImpl(PulsarClientImpl client, ConsumerConfigurationData<T> conf,
                                        ExecutorProvider executorProvider,
                                        CompletableFuture<Consumer<T>> subscribeFuture,
                                        Schema<T> schema, ConsumerInterceptors<T> interceptors,
                                        boolean createTopicIfDoesNotExist) {
        super(client, DUMMY_TOPIC_NAME_PREFIX + RandomStringUtils.randomAlphanumeric(5), conf,
                executorProvider, subscribeFuture, schema, interceptors, createTopicIfDoesNotExist, null,
                0, false);
    }


    @Override
    public Messages<T> fetchMessages(int maxMessages, int maxBytes, MessageId messageId, int timeout, TimeUnit unit)
            throws PulsarClientException {
        try {
            return fetchMessagesAsync(maxMessages, maxBytes, messageId, timeout, unit).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new PulsarClientException(e);
        }
    }

    @Override
    public CompletableFuture<Messages<T>> fetchMessagesAsync(int maxMessages, int maxBytes, MessageId messageId,
                                                             int timeout, TimeUnit unit) {
        MessageIdAdv messageIdAdv = (MessageIdAdv) messageId;
        int partitionIndex = messageIdAdv.getPartitionIndex();
        Consumer<T> consumer = super.consumers.get(TopicName.getTopicPartitionNameString(super.topic, partitionIndex));
        if (consumer == null) {
            return FutureUtil.failedFuture(new PulsarClientException(
                    "Partition " + partitionIndex + "for topic " + super.topic + " not found"));
        }
        if (consumer instanceof FetchConsumer) {
            return ((FetchConsumer<T>) consumer).fetchMessagesAsync(maxMessages, maxBytes, messageId, timeout, unit);
        } else {
            return FutureUtil.failedFuture(new PulsarClientException(
                    "Consumer " + consumer.getTopic() + " is not a FetchConsumer"));
        }
    }
}
