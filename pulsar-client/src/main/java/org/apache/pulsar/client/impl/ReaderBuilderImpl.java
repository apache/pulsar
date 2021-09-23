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

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.pulsar.client.api.KeySharedPolicy.DEFAULT_HASH_RANGE_SIZE;
import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.ConsumerCryptoFailureAction;
import org.apache.pulsar.client.api.CryptoKeyReader;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Range;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.ReaderBuilder;
import org.apache.pulsar.client.api.ReaderListener;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.conf.ConfigurationDataUtils;
import org.apache.pulsar.client.impl.conf.ReaderConfigurationData;
import org.apache.pulsar.common.util.FutureUtil;

@Getter(AccessLevel.PUBLIC)
public class ReaderBuilderImpl<T> implements ReaderBuilder<T> {

    private final PulsarClientImpl client;

    private ReaderConfigurationData<T> conf;

    private final Schema<T> schema;

    public ReaderBuilderImpl(PulsarClientImpl client, Schema<T> schema) {
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
        return new ReaderBuilderImpl<>(client, conf.clone(), schema);
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
        if (conf.getTopicNames().isEmpty()) {
            return FutureUtil
                    .failedFuture(new IllegalArgumentException("Topic name must be set on the reader builder"));
        }

        if (conf.getStartMessageId() != null && conf.getStartMessageFromRollbackDurationInSec() > 0 ||
                conf.getStartMessageId() == null && conf.getStartMessageFromRollbackDurationInSec() <= 0) {
            return FutureUtil
                    .failedFuture(new IllegalArgumentException(
                            "Start message id or start message from roll back must be specified but they cannot be specified at the same time"));
        }

        if (conf.getStartMessageFromRollbackDurationInSec() > 0) {
            conf.setStartMessageId(MessageId.earliest);
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
        conf.setTopicName(StringUtils.trim(topicName));
        return this;
    }

    @Override
    public ReaderBuilder<T> topics(List<String> topicNames) {
        checkArgument(topicNames != null && topicNames.size() > 0,
                "Passed in topicNames should not be null or empty.");
        topicNames.forEach(topicName ->
                checkArgument(StringUtils.isNotBlank(topicName), "topicNames cannot have blank topic"));
        conf.getTopicNames().addAll(topicNames.stream().map(StringUtils::trim)
                .collect(Collectors.toList()));
        return this;
    }

    @Override
    public ReaderBuilder<T> startMessageId(MessageId startMessageId) {
        conf.setStartMessageId(startMessageId);
        return this;
    }

    @Override
    public ReaderBuilder<T> startMessageFromRollbackDuration(long rollbackDuration, TimeUnit timeunit) {
        conf.setStartMessageFromRollbackDurationInSec(timeunit.toSeconds(rollbackDuration));
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
    public ReaderBuilder<T> defaultCryptoKeyReader(String privateKey) {
        checkArgument(StringUtils.isNotBlank(privateKey), "privateKey cannot be blank");
        return cryptoKeyReader(DefaultCryptoKeyReader.builder().defaultPrivateKey(privateKey).build());
    }

    @Override
    public ReaderBuilder<T> defaultCryptoKeyReader(@NonNull Map<String, String> privateKeys) {
        checkArgument(!privateKeys.isEmpty(), "privateKeys cannot be empty");
        return cryptoKeyReader(DefaultCryptoKeyReader.builder().privateKeys(privateKeys).build());
    }

    @Override
    public ReaderBuilder<T> cryptoFailureAction(ConsumerCryptoFailureAction action) {
        conf.setCryptoFailureAction(action);
        return this;
    }

    @Override
    public ReaderBuilder<T> receiverQueueSize(int receiverQueueSize) {
        checkArgument(receiverQueueSize >= 0, "receiverQueueSize needs to be >= 0");
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
    public ReaderBuilder<T> subscriptionName(String subscriptionName) {
        conf.setSubscriptionName(subscriptionName);
        return this;
    }

    @Override
    public ReaderBuilder<T> readCompacted(boolean readCompacted) {
        conf.setReadCompacted(readCompacted);
        return this;
    }

    @Override
    public ReaderBuilder<T> keyHashRange(Range... ranges) {
        Preconditions.checkArgument(ranges != null && ranges.length > 0,
                "Cannot specify a null ofr an empty key hash ranges for a reader");
        for (int i = 0; i < ranges.length; i++) {
            Range range1 = ranges[i];
            if (range1.getStart() < 0 || range1.getEnd() > DEFAULT_HASH_RANGE_SIZE) {
                throw new IllegalArgumentException("Ranges must be [0, 65535] but provided range is " + range1);
            }
            for (int j = 0; j < ranges.length; j++) {
                Range range2 = ranges[j];
                if (i != j && range1.intersect(range2) != null) {
                    throw new IllegalArgumentException("Key hash ranges with overlap between " + range1
                            + " and " + range2);
                }
            }
        }
        conf.setKeyHashRanges(Arrays.asList(ranges));
        return this;
    }

    @Override
    public ReaderBuilder<T> poolMessages(boolean poolMessages) {
        conf.setPoolMessages(poolMessages);
        return this;
    }
}
