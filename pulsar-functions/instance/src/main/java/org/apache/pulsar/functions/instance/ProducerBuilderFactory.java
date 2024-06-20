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
package org.apache.pulsar.functions.instance;

import static org.apache.commons.lang.StringUtils.isEmpty;
import com.google.common.annotations.VisibleForTesting;
import java.security.Security;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.BatcherBuilder;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.CryptoKeyReader;
import org.apache.pulsar.client.api.HashingScheme;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.ProducerCryptoFailureAction;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.functions.CryptoConfig;
import org.apache.pulsar.common.functions.ProducerConfig;
import org.apache.pulsar.functions.utils.CryptoUtils;
import org.bouncycastle.jce.provider.BouncyCastleProvider;

/**
 * This class is responsible for creating ProducerBuilders with the appropriate configurations to
 * match the ProducerConfig provided. Producers are created in 2 locations in Pulsar Functions and Connectors
 * and this class is used to unify the configuration of the producers without duplicating code.
 */
@Slf4j
public class ProducerBuilderFactory {

    private final PulsarClient client;
    private final ProducerConfig producerConfig;
    private final Consumer<ProducerBuilder<?>> defaultConfigurer;
    private final Crypto crypto;

    public ProducerBuilderFactory(PulsarClient client, ProducerConfig producerConfig, ClassLoader functionClassLoader,
                                  Consumer<ProducerBuilder<?>> defaultConfigurer) {
        this.client = client;
        this.producerConfig = producerConfig;
        this.defaultConfigurer = defaultConfigurer;
        try {
            this.crypto = initializeCrypto(functionClassLoader);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Unable to initialize crypto config " + producerConfig.getCryptoConfig(), e);
        }
        if (crypto == null) {
            log.info("crypto key reader is not provided, not enabling end to end encryption");
        }
    }

    public <T> ProducerBuilder<T> createProducerBuilder(String topic, Schema<T> schema, String producerName) {
        ProducerBuilder<T> builder = client.newProducer(schema);
        if (defaultConfigurer != null) {
            defaultConfigurer.accept(builder);
        }
        builder.blockIfQueueFull(true)
                .enableBatching(true)
                .batchingMaxPublishDelay(10, TimeUnit.MILLISECONDS)
                .hashingScheme(HashingScheme.Murmur3_32Hash) //
                .messageRoutingMode(MessageRoutingMode.CustomPartition)
                .messageRouter(FunctionResultRouter.of())
                // set send timeout to be infinity to prevent potential deadlock with consumer
                // that might happen when consumer is blocked due to unacked messages
                .sendTimeout(0, TimeUnit.SECONDS)
                .topic(topic);
        if (producerName != null) {
            builder.producerName(producerName);
        }
        if (producerConfig != null) {
            if (producerConfig.getCompressionType() != null) {
                builder.compressionType(producerConfig.getCompressionType());
            } else {
                // TODO: address this inconsistency.
                // PR https://github.com/apache/pulsar/pull/19470 removed the default compression type of LZ4
                // from the top level. This default is only used if producer config is provided.
                builder.compressionType(CompressionType.LZ4);
            }
            if (producerConfig.getMaxPendingMessages() != null && producerConfig.getMaxPendingMessages() != 0) {
                builder.maxPendingMessages(producerConfig.getMaxPendingMessages());
            }
            if (producerConfig.getMaxPendingMessagesAcrossPartitions() != null
                    && producerConfig.getMaxPendingMessagesAcrossPartitions() != 0) {
                builder.maxPendingMessagesAcrossPartitions(producerConfig.getMaxPendingMessagesAcrossPartitions());
            }
            if (producerConfig.getCryptoConfig() != null) {
                builder.cryptoKeyReader(crypto.keyReader);
                builder.cryptoFailureAction(crypto.failureAction);
                for (String encryptionKeyName : crypto.getEncryptionKeys()) {
                    builder.addEncryptionKey(encryptionKeyName);
                }
            }
            if (producerConfig.getBatchBuilder() != null) {
                if (producerConfig.getBatchBuilder().equals("KEY_BASED")) {
                    builder.batcherBuilder(BatcherBuilder.KEY_BASED);
                } else {
                    builder.batcherBuilder(BatcherBuilder.DEFAULT);
                }
            }
        }
        return builder;
    }


    @SuppressWarnings("unchecked")
    @VisibleForTesting
    Crypto initializeCrypto(ClassLoader functionClassLoader) throws ClassNotFoundException {
        if (producerConfig == null
                || producerConfig.getCryptoConfig() == null
                || isEmpty(producerConfig.getCryptoConfig().getCryptoKeyReaderClassName())) {
            return null;
        }

        CryptoConfig cryptoConfig = producerConfig.getCryptoConfig();

        // add provider only if it's not in the JVM
        if (Security.getProvider(BouncyCastleProvider.PROVIDER_NAME) == null) {
            Security.addProvider(new BouncyCastleProvider());
        }

        final String[] encryptionKeys = cryptoConfig.getEncryptionKeys();
        Crypto.CryptoBuilder bldr = Crypto.builder()
                .failureAction(cryptoConfig.getProducerCryptoFailureAction())
                .encryptionKeys(encryptionKeys);

        bldr.keyReader(CryptoUtils.getCryptoKeyReaderInstance(
                cryptoConfig.getCryptoKeyReaderClassName(), cryptoConfig.getCryptoKeyReaderConfig(),
                functionClassLoader));

        return bldr.build();
    }

    @Data
    @Builder
    private static class Crypto {
        private CryptoKeyReader keyReader;
        private ProducerCryptoFailureAction failureAction;
        private String[] encryptionKeys;
    }
}
