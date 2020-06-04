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
package org.apache.pulsar.broker.systopic;

import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.FutureUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public abstract class SystemTopicClientBase implements SystemTopicClient {

    protected final TopicName topicName;
    protected final PulsarClient client;

    protected final List<Writer> writers;
    protected final List<Reader> readers;

    public SystemTopicClientBase(PulsarClient client, TopicName topicName) {
        this.client = client;
        this.topicName = topicName;
        this.writers = Collections.synchronizedList(new ArrayList<>());
        this.readers = Collections.synchronizedList(new ArrayList<>());
    }

    @Override
    public Reader newReader() throws PulsarClientException {
        try {
            return newReaderAsync().get();
        } catch (Exception e) {
            throw new PulsarClientException(e);
        }
    }

    @Override
    public CompletableFuture<Reader> newReaderAsync() {
        return newReaderAsyncInternal().thenCompose(reader -> {
            readers.add(reader);
            return CompletableFuture.completedFuture(reader);
        });
    }

    @Override
    public Writer newWriter() throws PulsarClientException {
        try {
            return newWriterAsync().get();
        } catch (Exception e) {
            throw new PulsarClientException(e);
        }
    }

    @Override
    public CompletableFuture<Writer> newWriterAsync() {
        return newWriterAsyncInternal().thenCompose(writer -> {
            writers.add(writer);
            return CompletableFuture.completedFuture(writer);
        });
    }

    protected abstract CompletableFuture<Writer> newWriterAsyncInternal();

    protected abstract CompletableFuture<Reader> newReaderAsyncInternal();

    @Override
    public CompletableFuture<Void> closeAsync() {
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        writers.forEach(writer -> futures.add(writer.closeAsync()));
        readers.forEach(reader -> futures.add(reader.closeAsync()));
        writers.clear();
        readers.clear();
        return FutureUtil.waitForAll(futures);
    }

    @Override
    public void close() throws Exception {
        closeAsync().get();
    }

    @Override
    public TopicName getTopicName() {
        return topicName;
    }

    @Override
    public List<Reader> getReaders() {
        return readers;
    }

    @Override
    public List<Writer> getWriters() {
        return writers;
    }

    private static final Logger log = LoggerFactory.getLogger(SystemTopicClientBase.class);
}
