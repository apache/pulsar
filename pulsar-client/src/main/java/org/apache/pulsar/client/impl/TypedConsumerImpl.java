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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

class TypedConsumerImpl<T> implements Consumer<TypedMessage<T>> {

    private final Consumer<Message> untypedConsumer;
    private final Codec<T> codec;

    TypedConsumerImpl(Consumer<Message> untypedConsumer, Codec<T> codec) {
        this.untypedConsumer = untypedConsumer;
        this.codec = codec;
    }

    Codec<T> getCodec() {
        return codec;
    }

    @Override
    public String getTopic() {
        return untypedConsumer.getTopic();
    }

    @Override
    public String getSubscription() {
        return untypedConsumer.getSubscription();
    }

    @Override
    public void unsubscribe() throws PulsarClientException {
        untypedConsumer.unsubscribe();
    }

    @Override
    public CompletableFuture<Void> unsubscribeAsync() {
        return untypedConsumer.unsubscribeAsync();
    }

    @Override
    public TypedMessage<T> receive() throws PulsarClientException {
        return new TypedMessageImpl<>(untypedConsumer.receive(), codec);
    }

    @Override
    public CompletableFuture<TypedMessage<T>> receiveAsync() {
        return untypedConsumer.receiveAsync().thenApply((message) ->
            new TypedMessageImpl<>(message, codec)
        );
    }

    @Override
    public TypedMessage<T> receive(int timeout, TimeUnit unit) throws PulsarClientException {
        return new TypedMessageImpl<>(untypedConsumer.receive(), codec);
    }

    @Override
    public void acknowledge(TypedMessage<T> message) throws PulsarClientException {
        untypedConsumer.acknowledge(message);
    }

    @Override
    public void acknowledge(MessageId messageId) throws PulsarClientException {
        untypedConsumer.acknowledge(messageId);
    }

    @Override
    public void acknowledgeCumulative(TypedMessage<T> message) throws PulsarClientException {
        untypedConsumer.acknowledgeCumulative(message);
    }

    @Override
    public void acknowledgeCumulative(MessageId messageId) throws PulsarClientException {
        untypedConsumer.acknowledgeCumulative(messageId);
    }

    @Override
    public CompletableFuture<Void> acknowledgeAsync(TypedMessage<T> message) {
        return untypedConsumer.acknowledgeAsync(message);
    }

    @Override
    public CompletableFuture<Void> acknowledgeAsync(MessageId messageId) {
        return untypedConsumer.acknowledgeAsync(messageId);
    }

    @Override
    public CompletableFuture<Void> acknowledgeCumulativeAsync(TypedMessage<T> message) {
        return untypedConsumer.acknowledgeCumulativeAsync(message);
    }

    @Override
    public CompletableFuture<Void> acknowledgeCumulativeAsync(MessageId messageId) {
        return untypedConsumer.acknowledgeCumulativeAsync(messageId);
    }

    @Override
    public ConsumerStats getStats() {
        return untypedConsumer.getStats();
    }

    @Override
    public void close() throws PulsarClientException {
        untypedConsumer.close();
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        return untypedConsumer.closeAsync();
    }

    @Override
    public boolean hasReachedEndOfTopic() {
        return untypedConsumer.hasReachedEndOfTopic();
    }

    @Override
    public void redeliverUnacknowledgedMessages() {
        untypedConsumer.redeliverUnacknowledgedMessages();
    }

    @Override
    public void seek(MessageId messageId) throws PulsarClientException {
        untypedConsumer.seek(messageId);
    }

    @Override
    public CompletableFuture<Void> seekAsync(MessageId messageId) {
        return untypedConsumer.seekAsync(messageId);
    }
}
