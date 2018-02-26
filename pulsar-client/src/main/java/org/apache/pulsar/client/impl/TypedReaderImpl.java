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

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

class TypedReaderImpl<T> implements Reader<T> {

    private final Reader<byte[]> untypedReader;
    private final Schema<T> codec;

    TypedReaderImpl(Reader<byte[]> untypedReader, Schema<T> codec) {
        this.untypedReader = untypedReader;
        this.codec = codec;
    }

    @Override
    public String getTopic() {
        return untypedReader.getTopic();
    }

    @Override
    public Message<T> readNext() throws PulsarClientException {
        return new TypedMessageImpl<>(untypedReader.readNext(), codec);
    }

    @Override
    public Message<T> readNext(int timeout, TimeUnit unit) throws PulsarClientException {
        return new TypedMessageImpl<>(untypedReader.readNext(timeout, unit), codec);
    }

    @Override
    public CompletableFuture<Message<T>> readNextAsync() {
        return untypedReader.readNextAsync().thenApply((message) ->
                new TypedMessageImpl<>(message, codec)
        );
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        return untypedReader.closeAsync();
    }

    @Override
    public boolean hasReachedEndOfTopic() {
        return untypedReader.hasReachedEndOfTopic();
    }

    @Override
    public void close() throws IOException {
        untypedReader.close();
    }

    @Override
    public boolean hasMessageAvailable() throws PulsarClientException {
        return untypedReader.hasMessageAvailable();
    }

    @Override
    public CompletableFuture<Boolean> hasMessageAvailableAsync() {
        return untypedReader.hasMessageAvailableAsync();
    }
}
