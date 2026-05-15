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

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.api.v5.Message;
import org.apache.pulsar.client.api.v5.MessageId;
import org.apache.pulsar.client.api.v5.Transaction;
import org.apache.pulsar.client.api.v5.async.AsyncStreamConsumer;

/**
 * Async view of a {@link ScalableStreamConsumer}.
 */
final class AsyncStreamConsumerV5<T> implements AsyncStreamConsumer<T> {

    private final ScalableStreamConsumer<T> consumer;

    AsyncStreamConsumerV5(ScalableStreamConsumer<T> consumer) {
        this.consumer = consumer;
    }

    @Override
    public CompletableFuture<Message<T>> receive() {
        return consumer.receiveAsync();
    }

    @Override
    public CompletableFuture<Message<T>> receive(Duration timeout) {
        return consumer.receiveAsync(timeout);
    }

    @Override
    public CompletableFuture<List<Message<T>>> receiveMulti(int maxNumMessages, Duration timeout) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                var msgs = consumer.receiveMulti(maxNumMessages, timeout);
                List<Message<T>> result = new java.util.ArrayList<>();
                msgs.forEach(result::add);
                return result;
            } catch (Exception e) {
                throw new java.util.concurrent.CompletionException(e);
            }
        });
    }

    @Override
    public void acknowledgeCumulative(MessageId messageId) {
        consumer.acknowledgeCumulative(messageId);
    }

    @Override
    public void acknowledgeCumulative(MessageId messageId, Transaction txn) {
        consumer.acknowledgeCumulative(messageId, txn);
    }

    @Override
    public CompletableFuture<Void> close() {
        return consumer.closeAsync();
    }
}
