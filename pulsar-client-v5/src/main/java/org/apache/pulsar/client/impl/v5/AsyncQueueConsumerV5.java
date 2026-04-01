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
package org.apache.pulsar.client.impl.v5;

import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.api.v5.Message;
import org.apache.pulsar.client.api.v5.MessageId;
import org.apache.pulsar.client.api.v5.Transaction;
import org.apache.pulsar.client.api.v5.async.AsyncQueueConsumer;

/**
 * Async view of a {@link ScalableQueueConsumer}.
 */
final class AsyncQueueConsumerV5<T> implements AsyncQueueConsumer<T> {

    private final ScalableQueueConsumer<T> consumer;

    AsyncQueueConsumerV5(ScalableQueueConsumer<T> consumer) {
        this.consumer = consumer;
    }

    @Override
    public CompletableFuture<Message<T>> receive() {
        return consumer.receiveAsync();
    }

    @Override
    public void acknowledge(MessageId messageId) {
        consumer.acknowledge(messageId);
    }

    @Override
    public void acknowledge(Message<T> message) {
        consumer.acknowledge(message.id());
    }

    @Override
    public void acknowledge(MessageId messageId, Transaction txn) {
        consumer.acknowledge(messageId, txn);
    }

    @Override
    public void negativeAcknowledge(MessageId messageId) {
        consumer.negativeAcknowledge(messageId);
    }

    @Override
    public void negativeAcknowledge(Message<T> message) {
        consumer.negativeAcknowledge(message.id());
    }

    @Override
    public CompletableFuture<Void> close() {
        return consumer.closeAsync();
    }
}
