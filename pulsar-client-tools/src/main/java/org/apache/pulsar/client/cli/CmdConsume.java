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
import com.google.common.util.concurrent.RateLimiter;
import java.time.Duration;
import java.util.function.Consumer;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.v5.Message;
import org.apache.pulsar.client.api.v5.PulsarClient;
import org.apache.pulsar.client.api.v5.PulsarClientBuilder;
import org.apache.pulsar.client.api.v5.QueueConsumer;
import org.apache.pulsar.client.api.v5.QueueConsumerBuilder;
import org.apache.pulsar.client.api.v5.auth.ConsumerCryptoFailureAction;
import org.apache.pulsar.client.api.v5.config.ConsumerEncryptionPolicy;
import org.apache.pulsar.client.api.v5.config.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.v5.schema.Schema;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * pulsar-client consume command implementation, on the V5 client API.
 *
 * <p>Everything that is not V5-specific lives in {@link AbstractCmdConsumeCommand}; the v4 client
 * is driven by {@link CmdConsumeV4} under the {@code consume-v4} name.
 */
@Command(name = "consume", description = "Consume messages from a specified topic")
public class CmdConsume extends AbstractCmdConsumeCommand {

    @Option(names = { "-p", "--subscription-position" }, description = "Subscription position.")
    private SubscriptionInitialPosition subscriptionInitialPosition = SubscriptionInitialPosition.LATEST;

    @Option(names = { "-ca", "--crypto-failure-action" }, description = "Crypto Failure Action")
    private ConsumerCryptoFailureAction cryptoFailureAction = ConsumerCryptoFailureAction.FAIL;

    private PulsarClientBuilder clientBuilder;

    public CmdConsume() {
        super();
    }

    /**
     * Set client configuration.
     */
    public void updateConfig(PulsarClientBuilder clientBuilder, Authentication authentication, String serviceURL) {
        this.clientBuilder = clientBuilder;
        updateSharedConfig(authentication, serviceURL);
    }

    @Override
    protected int consume(String topic) {
        int numMessagesConsumed = 0;
        int returnCode = 0;

        final Schema<?> schema;
        if ("auto_consume".equals(schemaType)) {
            schema = Schema.autoConsume();
        } else if ("bytes".equals(schemaType)) {
            schema = Schema.bytes();
        } else {
            throw new IllegalArgumentException("schema type must be 'bytes' or 'auto_consume'");
        }
        if (!poolMessages) {
            LOG.info("--pool-messages has no effect on this version of pulsar-client.");
        }
        if (subscriptionMode == SubscriptionMode.NonDurable) {
            LOG.warn("--subscription-mode NonDurable is not supported by this version of pulsar-client; "
                    + "a durable subscription is used instead. Use consume-v4 for a non-durable subscription.");
        }
        if (subscriptionType == SubscriptionType.Exclusive || subscriptionType == SubscriptionType.Failover) {
            // The V5 StreamConsumer (ordered, single-reader) requires a scalable-topic subscription
            // controller, which regular topics do not have; only the QueueConsumer works against
            // both regular and scalable topics. So all subscription types use a QueueConsumer here
            // and Exclusive/Failover get work-queue (Shared-style) semantics rather than ordered.
            LOG.warn("--subscription-type {} : this version of pulsar-client consumes via a work-queue "
                    + "(Shared-style) subscription; exclusive/failover ordering is not preserved. "
                    + "Use consume-v4 for the v4 subscription types.",
                    subscriptionType);
        }
        if (maxPendingChunkedMessage > 0 || autoAckOldestChunkedMessageOnQueueFull) {
            LOG.warn("Chunked-message knobs (--max_chunked_msg / --auto_ack_chunk_q_full) have no effect "
                    + "on this version of pulsar-client.");
        }

        try (PulsarClient client = clientBuilder.build()) {
            RateLimiter limiter = (this.consumeRate > 0) ? RateLimiter.create(this.consumeRate) : null;
            QueueConsumerBuilder<?> builder = client.newQueueConsumer(schema)
                    .subscriptionName(this.subscriptionName)
                    .subscriptionInitialPosition(subscriptionInitialPosition)
                    .replicateSubscriptionState(replicateSubscriptionState);
            if (this.receiverQueueSize > 0) {
                builder.receiverQueueSize(this.receiverQueueSize);
            }
            if (isNotBlank(this.encKeyValue)) {
                builder.encryptionPolicy(buildConsumerEncryptionPolicy());
            }
            applyTopicSelection(builder::topic, builder::namespace);

            try (QueueConsumer<?> consumer = builder.subscribe()) {
                while (this.numMessagesToConsume == 0 || numMessagesConsumed < this.numMessagesToConsume) {
                    if (limiter != null) {
                        limiter.acquire();
                    }
                    Message<?> msg = consumer.receive(Duration.ofSeconds(5));
                    if (msg == null) {
                        LOG.debug("No message to consume after waiting for 5 seconds.");
                    } else {
                        if (msg.publishTime().toEpochMilli() > endTimestamp) {
                            break;
                        }
                        numMessagesConsumed += 1;
                        if (!hideContent) {
                            System.out.println(MESSAGE_BOUNDARY);
                            System.out.println(
                                    V5MessageSupport.interpretMessage(msg, displayHex, printMetadata));
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
     * namespace live). Use {@code consume-v4} for a real topic regex.
     */
    private void applyTopicSelection(Consumer<String> topicFn, Consumer<String> namespaceFn) {
        if (isRegex) {
            namespaceFn.accept(namespaceFromPattern(topic));
        } else {
            topicFn.accept(topic);
        }
    }

    static String namespaceFromPattern(String pattern) {
        // Strip an optional persistent:// / non-persistent:// domain prefix, then take the first
        // two path segments as tenant/namespace.
        String rest = pattern;
        int scheme = rest.indexOf("://");
        if (scheme >= 0) {
            rest = rest.substring(scheme + 3);
        }
        String[] parts = rest.split("/");
        if (parts.length < 2) {
            throw new IllegalArgumentException("Cannot derive a tenant/namespace from --regex pattern '"
                    + pattern + "'. Use a fully-qualified pattern, e.g. persistent://tenant/namespace/.*");
        }
        return parts[0] + "/" + parts[1];
    }

    private ConsumerEncryptionPolicy buildConsumerEncryptionPolicy() {
        return V5MessageSupport.buildFileDecryptionPolicy(this.encKeyValue, cryptoFailureAction);
    }
}
