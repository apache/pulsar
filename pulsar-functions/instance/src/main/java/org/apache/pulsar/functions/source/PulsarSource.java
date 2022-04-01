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
package org.apache.pulsar.functions.source;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.DeadLetterPolicy;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.client.impl.TopicMessageImpl;
import org.apache.pulsar.client.impl.schema.AutoConsumeSchema;
import org.apache.pulsar.common.functions.ConsumerConfig;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.functions.utils.CryptoUtils;
import org.apache.pulsar.io.core.Source;
import org.bouncycastle.jce.provider.BouncyCastleProvider;

import java.security.Security;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public abstract class PulsarSource<T> implements Source<T> {
    protected final PulsarClient pulsarClient;
    protected final PulsarSourceConfig pulsarSourceConfig;
    protected final Map<String, String> properties;
    protected final ClassLoader functionClassLoader;
    protected final TopicSchema topicSchema;

    protected PulsarSource(PulsarClient pulsarClient,
                           PulsarSourceConfig pulsarSourceConfig,
                           Map<String, String> properties,
                           ClassLoader functionClassLoader) {
        this.pulsarClient = pulsarClient;
        this.pulsarSourceConfig = pulsarSourceConfig;
        this.topicSchema = new TopicSchema(pulsarClient);
        this.properties = properties;
        this.functionClassLoader = functionClassLoader;
    }

    public abstract List<Consumer<T>> getInputConsumers();

    protected ConsumerBuilder<T> createConsumeBuilder(String topic, PulsarSourceConsumerConfig conf) {

        ConsumerBuilder<T> cb = pulsarClient.newConsumer(conf.getSchema())
                .subscriptionName(pulsarSourceConfig.getSubscriptionName())
                .subscriptionInitialPosition(pulsarSourceConfig.getSubscriptionPosition())
                .subscriptionType(pulsarSourceConfig.getSubscriptionType());

        if (conf.getConsumerProperties() != null && !conf.getConsumerProperties().isEmpty()) {
            cb.loadConf(new HashMap<>(conf.getConsumerProperties()));
        }

        if (conf.isRegexPattern()) {
            cb = cb.topicsPattern(topic);
        } else {
            cb = cb.topics(Collections.singletonList(topic));
        }
        if (conf.getReceiverQueueSize() != null) {
            cb = cb.receiverQueueSize(conf.getReceiverQueueSize());
        }
        if (conf.getCryptoKeyReader() != null) {
            cb = cb.cryptoKeyReader(conf.getCryptoKeyReader());
        }
        if (conf.getConsumerCryptoFailureAction() != null) {
            cb = cb.cryptoFailureAction(conf.getConsumerCryptoFailureAction());
        }
        cb = cb.properties(properties);
        if (pulsarSourceConfig.getNegativeAckRedeliveryDelayMs() != null
                && pulsarSourceConfig.getNegativeAckRedeliveryDelayMs() > 0) {
            cb.negativeAckRedeliveryDelay(pulsarSourceConfig.getNegativeAckRedeliveryDelayMs(), TimeUnit.MILLISECONDS);
        }
        if (pulsarSourceConfig.getTimeoutMs() != null) {
            cb = cb.ackTimeout(pulsarSourceConfig.getTimeoutMs(), TimeUnit.MILLISECONDS);
        }
        if (pulsarSourceConfig.getMaxMessageRetries() != null && pulsarSourceConfig.getMaxMessageRetries() >= 0) {
            DeadLetterPolicy.DeadLetterPolicyBuilder deadLetterPolicyBuilder = DeadLetterPolicy.builder();
            deadLetterPolicyBuilder.maxRedeliverCount(pulsarSourceConfig.getMaxMessageRetries());
            if (pulsarSourceConfig.getDeadLetterTopic() != null && !pulsarSourceConfig.getDeadLetterTopic().isEmpty()) {
                deadLetterPolicyBuilder.deadLetterTopic(pulsarSourceConfig.getDeadLetterTopic());
            }
            cb = cb.deadLetterPolicy(deadLetterPolicyBuilder.build());
        }

        if (conf.isPoolMessages()) {
            cb.poolMessages(true);
        }

        return cb;
    }

    protected Record<T> buildRecord(Consumer<T> consumer, Message<T> message) {
        Schema<T> schema = null;
        if (message instanceof MessageImpl) {
            MessageImpl impl = (MessageImpl) message;
            schema = impl.getSchemaInternal();
        } else if (message instanceof TopicMessageImpl) {
            TopicMessageImpl impl = (TopicMessageImpl) message;
            schema = impl.getSchemaInternal();
        }

        // we don't want the Function/Sink to see AutoConsumeSchema
        if (schema instanceof AutoConsumeSchema) {
            AutoConsumeSchema autoConsumeSchema = (AutoConsumeSchema) schema;
            // we cannot use atSchemaVersion, because atSchemaVersion is only
            // able to decode data, here we want a Schema that
            // is able to re-encode the payload when needed.
            schema = (Schema<T>) autoConsumeSchema
                    .unwrapInternalSchema(message.getSchemaVersion());
        }
        return PulsarRecord.<T>builder()
                .message(message)
                .schema(schema)
                .topicName(message.getTopicName())
                .ackFunction(() -> {
                    try {
                        if (pulsarSourceConfig
                                .getProcessingGuarantees() == FunctionConfig.ProcessingGuarantees.EFFECTIVELY_ONCE) {
                            consumer.acknowledgeCumulativeAsync(message);
                        } else {
                            consumer.acknowledgeAsync(message);
                        }
                    } finally {
                        // don't need to check if message pooling is set
                        // client will automatically check
                        message.release();
                    }
                }).failFunction(() -> {
                    try {
                        if (pulsarSourceConfig.getProcessingGuarantees() == FunctionConfig.ProcessingGuarantees.EFFECTIVELY_ONCE) {
                            throw new RuntimeException("Failed to process message: " + message.getMessageId());
                        }
                        consumer.negativeAcknowledge(message);
                    } finally {
                        // don't need to check if message pooling is set
                        // client will automatically check
                        message.release();
                    }
                })
                .build();
    }

    protected PulsarSourceConsumerConfig<T> buildPulsarSourceConsumerConfig(String topic, ConsumerConfig conf, Class<?> typeArg) {
        PulsarSourceConsumerConfig.PulsarSourceConsumerConfigBuilder<T> consumerConfBuilder
                = PulsarSourceConsumerConfig.<T>builder().isRegexPattern(conf.isRegexPattern())
                .receiverQueueSize(conf.getReceiverQueueSize())
                .consumerProperties(conf.getConsumerProperties());

        Schema<T> schema;
        if (conf.getSerdeClassName() != null && !conf.getSerdeClassName().isEmpty()) {
            schema = (Schema<T>) topicSchema.getSchema(topic, typeArg, conf.getSerdeClassName(), true);
        } else {
            schema = (Schema<T>) topicSchema.getSchema(topic, typeArg, conf, true);
        }
        consumerConfBuilder.schema(schema);

        if (conf.getCryptoConfig() != null) {
            // add provider only if it's not in the JVM
            if (Security.getProvider(BouncyCastleProvider.PROVIDER_NAME) == null) {
                Security.addProvider(new BouncyCastleProvider());
            }

            consumerConfBuilder.consumerCryptoFailureAction(conf.getCryptoConfig().getConsumerCryptoFailureAction());
            consumerConfBuilder.cryptoKeyReader(CryptoUtils.getCryptoKeyReaderInstance(
                    conf.getCryptoConfig().getCryptoKeyReaderClassName(),
                    conf.getCryptoConfig().getCryptoKeyReaderConfig(), functionClassLoader));
        }
        consumerConfBuilder.poolMessages(conf.isPoolMessages());
        return consumerConfBuilder.build();
    }
}
