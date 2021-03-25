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
package org.apache.pulsar.client.impl.v1;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerStats;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;

public class ConsumerV1Impl implements Consumer {
    private final org.apache.pulsar.shade.client.api.v2.Consumer<byte[]> consumer;

    public ConsumerV1Impl(org.apache.pulsar.shade.client.api.v2.Consumer<byte[]> consumer) {
        this.consumer = consumer;
    }

    @Override
    public void acknowledge(Message<?> arg0) throws PulsarClientException {
        consumer.acknowledge(arg0);
    }

    @Override
    public void acknowledge(MessageId arg0) throws PulsarClientException {
        consumer.acknowledge(arg0);
    }

    @Override
    public CompletableFuture<Void> acknowledgeAsync(Message<?> arg0) {
        return consumer.acknowledgeAsync(arg0);
    }

    @Override
    public CompletableFuture<Void> acknowledgeAsync(MessageId arg0) {
        return consumer.acknowledgeAsync(arg0);
    }

    @Override
    public void acknowledgeCumulative(Message<?> arg0) throws PulsarClientException {
        consumer.acknowledgeCumulative(arg0);
    }

    @Override
    public void acknowledgeCumulative(MessageId arg0) throws PulsarClientException {
        consumer.acknowledgeCumulative(arg0);
    }

    @Override
    public CompletableFuture<Void> acknowledgeCumulativeAsync(Message<?> arg0) {
        return consumer.acknowledgeCumulativeAsync(arg0);
    }

    @Override
    public CompletableFuture<Void> acknowledgeCumulativeAsync(MessageId arg0) {
        return consumer.acknowledgeCumulativeAsync(arg0);
    }

    @Override
    public void close() throws PulsarClientException {
        consumer.close();
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        return consumer.closeAsync();
    }

    @Override
    public String getConsumerName() {
        return consumer.getConsumerName();
    }

    @Override
    public ConsumerStats getStats() {
        return consumer.getStats();
    }

    public String getSubscription() {
        return consumer.getSubscription();
    }

    public String getTopic() {
        return consumer.getTopic();
    }

    public boolean hasReachedEndOfTopic() {
        return consumer.hasReachedEndOfTopic();
    }

    public boolean isConnected() {
        return consumer.isConnected();
    }

    public void pause() {
        consumer.pause();
    }

    public Message<byte[]> receive() throws PulsarClientException {
        return consumer.receive();
    }

    public Message<byte[]> receive(int arg0, TimeUnit arg1) throws PulsarClientException {
        return consumer.receive(arg0, arg1);
    }

    public CompletableFuture<Message<byte[]>> receiveAsync() {
        return consumer.receiveAsync();
    }

    public void redeliverUnacknowledgedMessages() {
        consumer.redeliverUnacknowledgedMessages();
    }

    public void resume() {
        consumer.resume();
    }

    public void seek(MessageId arg0) throws PulsarClientException {
        consumer.seek(arg0);
    }

    public void seek(long arg0) throws PulsarClientException {
        consumer.seek(arg0);
    }

    public void seek(Function<String, Object> function) throws PulsarClientException {
        consumer.seek(function);
    }

    public CompletableFuture<Void> seekAsync(long arg0) {
        return consumer.seekAsync(arg0);
    }

    public CompletableFuture<Void> seekAsync(MessageId arg0) {
        return consumer.seekAsync(arg0);
    }

    public CompletableFuture<Void> seekAsync(Function<String, Object> function) {
        return consumer.seekAsync(function);
    }

    public void unsubscribe() throws PulsarClientException {
        consumer.unsubscribe();
    }

    public CompletableFuture<Void> unsubscribeAsync() {
        return consumer.unsubscribeAsync();
    }

    public MessageId getLastMessageId() throws PulsarClientException {
        return consumer.getLastMessageId();
    }

    public CompletableFuture<MessageId> getLastMessageIdAsync() {
        return consumer.getLastMessageIdAsync();
    }
}
