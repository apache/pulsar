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

import com.google.common.annotations.VisibleForTesting;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.CryptoKeyReader;
import org.apache.pulsar.client.api.HashingScheme;
import org.apache.pulsar.client.api.MessageRouter;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.ProducerCryptoFailureAction;
import org.apache.pulsar.client.api.ProducerInterceptor;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.conf.ConfigurationDataUtils;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.apache.pulsar.common.util.FutureUtil;

import lombok.NonNull;

public class ProducerBuilderImpl<T> implements ProducerBuilder<T> {

    private final PulsarClientImpl client;
    private ProducerConfigurationData conf;
    private Schema<T> schema;
    private List<ProducerInterceptor<T>> interceptorList;

    @VisibleForTesting
    public ProducerBuilderImpl(PulsarClientImpl client, Schema<T> schema) {
        this(client, new ProducerConfigurationData(), schema);
    }

    private ProducerBuilderImpl(PulsarClientImpl client, ProducerConfigurationData conf, Schema<T> schema) {
        this.client = client;
        this.conf = conf;
        this.schema = schema;
    }


    /**
     * Allow to override schema in builder implementation
     * @return
     */
    public ProducerBuilder<T> schema(Schema<T> schema) {
        this.schema = schema;
        return this;
    }

    @Override
    public ProducerBuilder<T> clone() {
        return new ProducerBuilderImpl<>(client, conf.clone(), schema);
    }

    @Override
    public Producer<T> create() throws PulsarClientException {
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
    public CompletableFuture<Producer<T>> createAsync() {
        if (conf.getTopicName() == null) {
            return FutureUtil
                    .failedFuture(new IllegalArgumentException("Topic name must be set on the producer builder"));
        }

        return interceptorList == null || interceptorList.size() == 0 ?
                client.createProducerAsync(conf, schema, null) :
                client.createProducerAsync(conf, schema, new ProducerInterceptors<>(interceptorList));
    }

    @Override
    public ProducerBuilder<T> loadConf(Map<String, Object> config) {
        conf = ConfigurationDataUtils.loadData(
            config, conf, ProducerConfigurationData.class);
        return this;
    }

    @Override
    public ProducerBuilder<T> topic(String topicName) {
        conf.setTopicName(topicName);
        return this;
    }

    @Override
    public ProducerBuilder<T> producerName(@NonNull String producerName) {
        conf.setProducerName(producerName);
        return this;
    }

    @Override
    public ProducerBuilder<T> sendTimeout(int sendTimeout, @NonNull TimeUnit unit) {
        if (sendTimeout < 0) {
            throw new IllegalArgumentException("sendTimeout needs to be >= 0");
        }
        conf.setSendTimeoutMs(unit.toMillis(sendTimeout));
        return this;
    }

    @Override
    public ProducerBuilder<T> maxPendingMessages(int maxPendingMessages) {
        if (maxPendingMessages <= 0) {
            throw new IllegalArgumentException("maxPendingMessages needs to be > 0");
        }
        conf.setMaxPendingMessages(maxPendingMessages);
        return this;
    }

    @Override
    public ProducerBuilder<T> maxPendingMessagesAcrossPartitions(int maxPendingMessagesAcrossPartitions) {
        conf.setMaxPendingMessagesAcrossPartitions(maxPendingMessagesAcrossPartitions);
        return this;
    }

    @Override
    public ProducerBuilder<T> blockIfQueueFull(boolean blockIfQueueFull) {
        conf.setBlockIfQueueFull(blockIfQueueFull);
        return this;
    }

    @Override
    public ProducerBuilder<T> messageRoutingMode(@NonNull MessageRoutingMode messageRouteMode) {
        conf.setMessageRoutingMode(messageRouteMode);
        return this;
    }

    @Override
    public ProducerBuilder<T> compressionType(@NonNull CompressionType compressionType) {
        conf.setCompressionType(compressionType);
        return this;
    }

    @Override
    public ProducerBuilder<T> hashingScheme(@NonNull HashingScheme hashingScheme) {
        conf.setHashingScheme(hashingScheme);
        return this;
    }

    @Override
    public ProducerBuilder<T> messageRouter(@NonNull MessageRouter messageRouter) {
        conf.setCustomMessageRouter(messageRouter);
        return this;
    }

    @Override
    public ProducerBuilder<T> enableBatching(boolean batchMessagesEnabled) {
        conf.setBatchingEnabled(batchMessagesEnabled);
        return this;
    }

    @Override
    public ProducerBuilder<T> cryptoKeyReader(@NonNull CryptoKeyReader cryptoKeyReader) {
        conf.setCryptoKeyReader(cryptoKeyReader);
        return this;
    }

    @Override
    public ProducerBuilder<T> addEncryptionKey(@NonNull String key) {
        conf.getEncryptionKeys().add(key);
        return this;
    }

    @Override
    public ProducerBuilder<T> cryptoFailureAction(@NonNull ProducerCryptoFailureAction action) {
        conf.setCryptoFailureAction(action);
        return this;
    }

    @Override
    public ProducerBuilder<T> batchingMaxPublishDelay(long batchDelay, @NonNull TimeUnit timeUnit) {
        conf.setBatchingMaxPublishDelayMicros(timeUnit.toMicros(batchDelay));
        return this;
    }

    @Override
    public ProducerBuilder<T> batchingMaxMessages(int batchMessagesMaxMessagesPerBatch) {
        conf.setBatchingMaxMessages(batchMessagesMaxMessagesPerBatch);
        return this;
    }

    @Override
    public ProducerBuilder<T> initialSequenceId(long initialSequenceId) {
        conf.setInitialSequenceId(initialSequenceId);
        return this;
    }

    @Override
    public ProducerBuilder<T> property(@NonNull String key, @NonNull String value) {
        conf.getProperties().put(key, value);
        return this;
    }

    @Override
    public ProducerBuilder<T> properties(@NonNull Map<String, String> properties) {
        conf.getProperties().putAll(properties);
        return this;
    }

    @Override
    public ProducerBuilder<T> intercept(ProducerInterceptor<T>... interceptors) {
        if (interceptorList == null) {
            interceptorList = new ArrayList<>();
        }
        interceptorList.addAll(Arrays.asList(interceptors));
        return this;
    }
}
