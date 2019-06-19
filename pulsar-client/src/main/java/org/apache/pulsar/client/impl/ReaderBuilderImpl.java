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

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.apache.pulsar.client.api.ConsumerCryptoFailureAction;
import org.apache.pulsar.client.api.CryptoKeyReader;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.ReaderBuilder;
import org.apache.pulsar.client.api.ReaderListener;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.conf.ConfigurationDataUtils;
import org.apache.pulsar.client.impl.conf.ReaderConfigurationData;
import org.apache.pulsar.common.util.FutureUtil;

public class ReaderBuilderImpl<T> implements ReaderBuilder<T> {

    private final PulsarClientImpl client;

    private ReaderConfigurationData<T> conf;

    private final Schema<T> schema;

    ReaderBuilderImpl(PulsarClientImpl client, Schema<T> schema) {
        this(client, new ReaderConfigurationData<T>(), schema);
    }

    private ReaderBuilderImpl(PulsarClientImpl client, ReaderConfigurationData<T> conf, Schema<T> schema) {
        this.client = client;
        this.conf = conf;
        this.schema = schema;
    }

    @Override
    @SuppressWarnings("unchecked")
    public ReaderBuilder<T> clone() {
        try {
            return (ReaderBuilder<T>) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException("Failed to clone ReaderBuilderImpl");
        }
    }

    @Override
    public Reader<T> create() throws PulsarClientException {
        try {
            return createAsync().get();
        } catch (Exception e) {
            throw PulsarClientException.unwrap(e);
        }
    }

    @Override
    public CompletableFuture<Reader<T>> createAsync() {
        if (conf.getTopicName() == null) {
            return FutureUtil
                    .failedFuture(new IllegalArgumentException("Topic name must be set on the reader builder"));
        }

        if (conf.getStartMessageId() == null) {
            return FutureUtil
                    .failedFuture(new IllegalArgumentException("Start message id must be set on the reader builder"));
        }

        return client.createReaderAsync(conf, schema);
    }

    @Override
    public ReaderBuilder<T> loadConf(Map<String, Object> config) {
        MessageId startMessageId = conf.getStartMessageId();
        conf = ConfigurationDataUtils.loadData(config, conf, ReaderConfigurationData.class);
        conf.setStartMessageId(startMessageId);
        return this;
    }

    @Override
    public ReaderBuilder<T> topic(String topicName) {
        conf.setTopicName(topicName);
        return this;
    }

    @Override
    public ReaderBuilder<T> startMessageId(MessageId startMessageId) {
        conf.setStartMessageId(startMessageId);
        return this;
    }

    @Override
    public ReaderBuilder<T> startMessageIdInclusive() {
        conf.setResetIncludeHead(true);
        return this;
    }

    @Override
    public ReaderBuilder<T> readerListener(ReaderListener<T> readerListener) {
        conf.setReaderListener(readerListener);
        return this;
    }

    @Override
    public ReaderBuilder<T> cryptoKeyReader(CryptoKeyReader cryptoKeyReader) {
        conf.setCryptoKeyReader(cryptoKeyReader);
        return this;
    }

    @Override
    public ReaderBuilder<T> cryptoFailureAction(ConsumerCryptoFailureAction action) {
        conf.setCryptoFailureAction(action);
        return this;
    }

    @Override
    public ReaderBuilder<T> receiverQueueSize(int receiverQueueSize) {
        conf.setReceiverQueueSize(receiverQueueSize);
        return this;
    }

    @Override
    public ReaderBuilder<T> readerName(String readerName) {
        conf.setReaderName(readerName);
        return this;
    }

    @Override
    public ReaderBuilder<T> subscriptionRolePrefix(String subscriptionRolePrefix) {
        conf.setSubscriptionRolePrefix(subscriptionRolePrefix);
        return this;
    }

    @Override
    public ReaderBuilder<T> readCompacted(boolean readCompacted) {
        conf.setReadCompacted(readCompacted);
        return this;
    }
}
