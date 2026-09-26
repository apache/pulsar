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
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionType;

/**
 * Consumes the messages of a {@link CmdConsume} invocation with the v4 ({@code pulsar-client-original})
 * client, including the v4-only capabilities: real {@code Exclusive} / {@code Failover} /
 * {@code Key_Shared} subscription types, non-durable subscriptions, a real topic regex for
 * {@code --regex}, {@code --start-timestamp}, pooled messages, the chunked-message knobs, and the full
 * message metadata rendering.
 */
final class ConsumeV4 {

    private final CmdConsume cmd;
    private final CmdConsume.V4Options v4;
    private final ClientBuilder clientBuilder;

    ConsumeV4(CmdConsume cmd, ClientBuilder clientBuilder) {
        this.cmd = cmd;
        this.v4 = cmd.v4;
        this.clientBuilder = clientBuilder;
    }

    /**
     * Consume the messages.
     *
     * @return 0 for success, &lt; 0 otherwise
     */
    @SuppressWarnings("deprecation")
    int consume(String topic) {
        int numMessagesConsumed = 0;
        int returnCode = 0;

        try (PulsarClient client = clientBuilder.build()) {
            Schema<?> schema = v4.poolMessages ? Schema.BYTEBUFFER : Schema.BYTES;
            if ("auto_consume".equals(cmd.schemaType)) {
                schema = Schema.AUTO_CONSUME();
            } else if (!"bytes".equals(cmd.schemaType)) {
                throw new IllegalArgumentException("schema type must be 'bytes' or 'auto_consume'");
            }
            ConsumerBuilder<?> builder = client.newConsumer(schema)
                    .subscriptionName(cmd.subscriptionName)
                    .subscriptionType(SubscriptionType.valueOf(cmd.subscriptionType.name()))
                    .subscriptionMode(SubscriptionMode.valueOf(cmd.subscriptionMode.name()))
                    .subscriptionInitialPosition(cmd.subscriptionInitialPosition)
                    .poolMessages(v4.poolMessages)
                    .replicateSubscriptionState(v4.replicateSubscriptionState);

            if (cmd.isRegex) {
                builder.topicsPattern(Pattern.compile(topic));
            } else {
                builder.topic(topic);
            }

            if (v4.maxPendingChunkedMessage > 0) {
                builder.maxPendingChunkedMessage(v4.maxPendingChunkedMessage);
            }
            if (cmd.receiverQueueSize > 0) {
                builder.receiverQueueSize(cmd.receiverQueueSize);
            }

            builder.autoAckOldestChunkedMessageOnQueueFull(v4.autoAckOldestChunkedMessageOnQueueFull);
            builder.cryptoFailureAction(cmd.cryptoFailureAction);

            if (isNotBlank(cmd.encKeyValue)) {
                builder.defaultCryptoKeyReader(cmd.encKeyValue);
            }

            try (Consumer<?> consumer = builder.subscribe()) {
                if (v4.startTimestamp > 0L) {
                    consumer.seek(v4.startTimestamp);
                }
                RateLimiter limiter = (cmd.consumeRate > 0) ? RateLimiter.create(cmd.consumeRate) : null;
                while (cmd.numMessagesToConsume == 0 || numMessagesConsumed < cmd.numMessagesToConsume) {
                    if (limiter != null) {
                        limiter.acquire();
                    }
                    Message<?> msg = consumer.receive(5, TimeUnit.SECONDS);
                    if (msg == null) {
                        LOG.debug("No message to consume after waiting for 5 seconds.");
                    } else {
                        try {
                            if (msg.getPublishTime() > cmd.endTimestamp) {
                                break;
                            }
                            numMessagesConsumed += 1;
                            if (!cmd.hideContent) {
                                System.out.println(MESSAGE_BOUNDARY);
                                System.out.println(
                                        V4MessageSupport.interpretMessage(msg, cmd.displayHex, cmd.printMetadata));
                            } else if (numMessagesConsumed % 1000 == 0) {
                                System.out.println("Received " + numMessagesConsumed + " messages");
                            }
                            consumer.acknowledge(msg);
                        } finally {
                            msg.release();
                        }
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
}
