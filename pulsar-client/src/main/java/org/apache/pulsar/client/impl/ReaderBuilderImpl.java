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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.apache.pulsar.client.api.ConsumerCryptoFailureAction;
import org.apache.pulsar.client.api.CryptoKeyReader;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.ReaderBuilder;
import org.apache.pulsar.client.api.ReaderConfiguration;
import org.apache.pulsar.client.api.ReaderListener;
import org.apache.pulsar.common.util.FutureUtil;

@SuppressWarnings("deprecation")
public class ReaderBuilderImpl implements ReaderBuilder {

    private static final long serialVersionUID = 1L;

    private final PulsarClientImpl client;

    private final ReaderConfiguration conf;
    private String topicName;
    private MessageId startMessageId;

    ReaderBuilderImpl(PulsarClientImpl client) {
        this.client = client;
        this.conf = new ReaderConfiguration();
    }

    @Override
    public ReaderBuilder clone() {
        try {
            return (ReaderBuilder) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException("Failed to clone ReaderBuilderImpl");
        }
    }

    @Override
    public Reader create() throws PulsarClientException {
        try {
            return createAsync().get();
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
    public CompletableFuture<Reader> createAsync() {
        if (topicName == null) {
            return FutureUtil
                    .failedFuture(new IllegalArgumentException("Topic name must be set on the reader builder"));
        }

        if (startMessageId == null) {
            return FutureUtil
                    .failedFuture(new IllegalArgumentException("Start message id must be set on the reader builder"));
        }

        return client.createReaderAsync(topicName, startMessageId, conf);
    }

    @Override
    public ReaderBuilder topic(String topicName) {
        this.topicName = topicName;
        return this;
    }

    @Override
    public ReaderBuilder startMessageId(MessageId startMessageId) {
        this.startMessageId = startMessageId;
        return this;
    }

    @Override
    public ReaderBuilder readerListener(ReaderListener readerListener) {
        conf.setReaderListener(readerListener);
        return this;
    }

    @Override
    public ReaderBuilder cryptoKeyReader(CryptoKeyReader cryptoKeyReader) {
        conf.setCryptoKeyReader(cryptoKeyReader);
        return this;
    }

    @Override
    public ReaderBuilder cryptoFailureAction(ConsumerCryptoFailureAction action) {
        conf.setCryptoFailureAction(action);
        return this;
    }

    @Override
    public ReaderBuilder receiverQueueSize(int receiverQueueSize) {
        conf.setReceiverQueueSize(receiverQueueSize);
        return this;
    }

    @Override
    public ReaderBuilder readerName(String readerName) {
        conf.setReaderName(readerName);
        return this;
    }
}
