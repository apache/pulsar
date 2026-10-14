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
package org.apache.pulsar.client.cli;

import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.pulsar.client.cli.AbstractCmdConsume.LOG;
import static org.apache.pulsar.client.cli.AbstractCmdConsume.MESSAGE_BOUNDARY;
import com.google.common.util.concurrent.RateLimiter;
import java.time.Duration;
import java.util.function.Consumer;
import org.apache.pulsar.cli.converters.picocli.EnumNameConverter;
import org.apache.pulsar.client.api.v5.Message;
import org.apache.pulsar.client.api.v5.PulsarClient;
import org.apache.pulsar.client.api.v5.PulsarClientBuilder;
import org.apache.pulsar.client.api.v5.QueueConsumer;
import org.apache.pulsar.client.api.v5.QueueConsumerBuilder;
import org.apache.pulsar.client.api.v5.auth.ConsumerCryptoFailureAction;
import org.apache.pulsar.client.api.v5.config.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.v5.schema.Schema;

/**
 * Consumes the messages of a {@link CmdConsume} invocation with the V5 client.
 */
final class ConsumeV5 {

    private final CmdConsume cmd;
    private final PulsarClientBuilder clientBuilder;

    ConsumeV5(CmdConsume cmd, PulsarClientBuilder clientBuilder) {
        this.cmd = cmd;
        this.clientBuilder = clientBuilder;
    }

    /**
     * Consume the messages.
     *
     * @return 0 for success, &lt; 0 otherwise
     */
    int consume(String topic) {
        int numMessagesConsumed = 0;
        int returnCode = 0;

        final Schema<?> schema;
        if ("auto_consume".equals(cmd.schemaType)) {
            schema = Schema.autoConsume();
        } else if ("bytes".equals(cmd.schemaType)) {
            schema = Schema.bytes();
        } else {
            throw new IllegalArgumentException("schema type must be 'bytes' or 'auto_consume'");
        }
        if (cmd.subscriptionMode == CmdConsume.SubscriptionMode.NonDurable) {
            LOG.warn("--subscription-mode NonDurable is not supported by the V5 client; a durable subscription "
                    + "is used instead. For a non-durable subscription, " + AbstractCmd.USE_V4_CLIENT_HINT + ".");
        }
        if (cmd.subscriptionType == CmdConsume.SubscriptionType.Exclusive
                || cmd.subscriptionType == CmdConsume.SubscriptionType.Failover) {
            // The V5 StreamConsumer (ordered, single-reader) requires a scalable-topic subscription
            // controller, which regular topics do not have; only the QueueConsumer works against
            // both regular and scalable topics. So all subscription types use a QueueConsumer here
            // and Exclusive/Failover get work-queue (Shared-style) semantics rather than ordered.
            LOG.warn("--subscription-type {} : the V5 client consumes via a work-queue (Shared-style) "
                    + "subscription; exclusive/failover ordering is not preserved. For the v4 subscription "
                    + "types, " + AbstractCmd.USE_V4_CLIENT_HINT + ".", cmd.subscriptionType);
        }

        try (PulsarClient client = clientBuilder.build()) {
            RateLimiter limiter = (cmd.consumeRate > 0) ? RateLimiter.create(cmd.consumeRate) : null;
            QueueConsumerBuilder<?> builder = client.newQueueConsumer(schema)
                    .subscriptionName(cmd.subscriptionName)
                    .subscriptionInitialPosition(EnumNameConverter.mapByName(cmd.subscriptionInitialPosition,
                            SubscriptionInitialPosition.class));
            if (cmd.receiverQueueSize > 0) {
                builder.receiverQueueSize(cmd.receiverQueueSize);
            }
            if (isNotBlank(cmd.encKeyValue)) {
                builder.encryptionPolicy(V5MessageSupport.buildFileDecryptionPolicy(cmd.encKeyValue,
                        EnumNameConverter.mapByName(cmd.cryptoFailureAction, ConsumerCryptoFailureAction.class)));
            }
            applyTopicSelection(topic, builder::topic, builder::namespace);

            try (QueueConsumer<?> consumer = builder.subscribe()) {
                while (cmd.numMessagesToConsume == 0 || numMessagesConsumed < cmd.numMessagesToConsume) {
                    if (limiter != null) {
                        limiter.acquire();
                    }
                    Message<?> msg = consumer.receive(Duration.ofSeconds(5));
                    if (msg == null) {
                        LOG.debug("No message to consume after waiting for 5 seconds.");
                    } else {
                        if (msg.publishTime().toEpochMilli() > cmd.endTimestamp) {
                            break;
                        }
                        numMessagesConsumed += 1;
                        if (!cmd.hideContent) {
                            System.out.println(MESSAGE_BOUNDARY);
                            System.out.println(
                                    V5MessageSupport.interpretMessage(msg, cmd.displayHex, cmd.printMetadata));
                        } else if (numMessagesConsumed % 1000 == 0) {
                            System.out.println("Received " + numMessagesConsumed + " messages");
                        }
                        consumer.acknowledge(msg.id());
                    }
                }
            }
        } catch (Exception e) {
            LOG.error("Error while consuming messages");
            LOG.error(e.getMessage(), e);
            returnCode = -1;
        } finally {
            LOG.info("{} messages successfully consumed", numMessagesConsumed);
        }

        return returnCode;
    }

    /**
     * Apply the topic argument to the consumer. A plain topic uses {@code topic(...)}; a
     * {@code --regex} pattern is mapped to a namespace subscription over the pattern's
     * {@code tenant/namespace} (V5 has no topic-regex; namespace subscriptions follow the
     * namespace live). The v4 client subscribes with a real topic regex.
     */
    private void applyTopicSelection(String topic, Consumer<String> topicFn, Consumer<String> namespaceFn) {
        if (cmd.isRegex) {
            namespaceFn.accept(namespaceFromPattern(topic));
        } else {
            topicFn.accept(topic);
        }
    }

    static String namespaceFromPattern(String pattern) {
        // Strip an optional domain prefix, then take the first two path segments as tenant/namespace.
        String rest = pattern;
        int scheme = rest.indexOf("://");
        if (scheme >= 0) {
            rest = rest.substring(scheme + 3);
        }
        String[] parts = rest.split("/");
        if (parts.length < 2) {
            throw new IllegalArgumentException("Cannot derive a tenant/namespace from --regex pattern '"
                    + pattern + "'. Use a fully-qualified pattern, e.g. topic://tenant/namespace/.*");
        }
        return parts[0] + "/" + parts[1];
    }
}
