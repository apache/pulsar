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
package org.apache.pulsar.client.impl;

import static com.google.common.base.Preconditions.checkArgument;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.MessagePayloadProcessor;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.RandomReader;
import org.apache.pulsar.client.api.RandomReaderBuilder;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.conf.ConfigurationDataUtils;
import org.apache.pulsar.client.impl.conf.RandomReaderConfigurationData;
import org.apache.pulsar.common.util.FutureUtil;

public class RandomReaderBuilderImpl<T> implements RandomReaderBuilder<T> {
    private final PulsarClientImpl client;
    private RandomReaderConfigurationData<T> conf;
    private final Schema<T> schema;

    public RandomReaderBuilderImpl(PulsarClientImpl client, Schema<T> schema) {
        this(client, new RandomReaderConfigurationData<>(), schema);
    }

    private RandomReaderBuilderImpl(PulsarClientImpl client, RandomReaderConfigurationData<T> conf, Schema<T> schema) {
        this.client = client;
        this.conf = conf;
        this.schema = schema;
    }

    @Override
    public RandomReader<T> create() throws PulsarClientException {
        try {
            return FutureUtil.getAndCleanupOnInterrupt(createAsync(), RandomReader::closeAsync);
        } catch (Exception e) {
            throw PulsarClientException.unwrap(e);
        }
    }

    @Override
    public CompletableFuture<RandomReader<T>> createAsync() {
        if (StringUtils.isBlank(conf.getTopicName())) {
            return FutureUtil.failedFuture(new IllegalArgumentException(
                    "Topic name must be set on the random reader builder"));
        }
        return client.createRandomReaderAsync(conf, schema);
    }

    @Override
    public RandomReaderBuilder<T> clone() {
        return new RandomReaderBuilderImpl<>(client, conf.clone(), schema);
    }

    @Override
    public RandomReaderBuilder<T> topic(String topicName) {
        conf.setTopicName(StringUtils.trim(topicName));
        return this;
    }

    @Override
    public RandomReaderBuilder<T> loadConf(Map<String, Object> config) {
        conf = ConfigurationDataUtils.loadData(config, conf, RandomReaderConfigurationData.class);
        return this;
    }

    @Override
    public RandomReaderBuilder<T> readerName(String readerName) {
        conf.setReaderName(readerName);
        return this;
    }

    @Override
    public RandomReaderBuilder<T> messagePayloadProcessor(MessagePayloadProcessor payloadProcessor) {
        conf.setPayloadProcessor(payloadProcessor);
        return this;
    }

    @Override
    public RandomReaderBuilder<T> poolMessages(boolean poolMessages) {
        conf.setPoolMessages(poolMessages);
        return this;
    }

    @Override
    public RandomReaderBuilder<T> readCommitted(boolean readCommitted) {
        conf.setReadCommitted(readCommitted);
        return this;
    }

    @Override
    public RandomReaderBuilder<T> property(String key, String value) {
        checkArgument(StringUtils.isNotBlank(key) && StringUtils.isNotBlank(value),
                "property key/value cannot be blank");
        conf.getProperties().put(key, value);
        return this;
    }

    @Override
    public RandomReaderBuilder<T> properties(Map<String, String> properties) {
        properties.entrySet().forEach(entry ->
                checkArgument(StringUtils.isNotBlank(entry.getKey()) && StringUtils.isNotBlank(entry.getValue()),
                        "properties' key/value cannot be blank"));
        conf.getProperties().putAll(properties);
        return this;
    }
}
