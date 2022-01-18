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

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;

public class ReaderV1Impl implements Reader {

    private final org.apache.pulsar.shade.client.api.v2.Reader<byte[]> reader;

    public ReaderV1Impl(org.apache.pulsar.shade.client.api.v2.Reader<byte[]> reader) {
        this.reader = reader;
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        return reader.closeAsync();
    }

    @Override
    public String getTopic() {
        return reader.getTopic();
    }

    @Override
    public boolean hasMessageAvailable() throws PulsarClientException {
        return reader.hasMessageAvailable();
    }

    @Override
    public CompletableFuture<Boolean> hasMessageAvailableAsync() {
        return reader.hasMessageAvailableAsync();
    }

    @Override
    public boolean hasReachedEndOfTopic() {
        return reader.hasReachedEndOfTopic();
    }

    @Override
    public boolean isConnected() {
        return reader.isConnected();
    }

    @Override
    public Message<byte[]> readNext() throws PulsarClientException {
        return reader.readNext();
    }

    @Override
    public Message<byte[]> readNext(int arg0, TimeUnit arg1) throws PulsarClientException {
        return reader.readNext(arg0, arg1);
    }

    @Override
    public CompletableFuture<Message<byte[]>> readNextAsync() {
        return reader.readNextAsync();
    }
}
