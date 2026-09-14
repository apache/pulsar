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
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.ConsumerCryptoFailureAction;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * The {@code consume} command driven by the v4 ({@code pulsar-client-original}) client.
 *
 * <p>This is the counterpart of {@link CmdConsume}: the CLI options and the WebSocket path come
 * from {@link AbstractCmdConsumeCommand}, and only the client bindings differ. It restores the
 * v4-only consumer capabilities: real {@code Exclusive} / {@code Failover} / {@code Key_Shared}
 * subscription types, non-durable subscriptions, a real topic regex for {@code --regex},
 * {@code --start-timestamp}, pooled messages, the chunked-message knobs, and the full message
 * metadata rendering.
 */
@Command(name = "consume-v4", description = "Consume messages from a specified topic using the v4 client")
public class CmdConsumeV4 extends AbstractCmdConsumeCommand {

    @Option(names = { "-p", "--subscription-position" }, description = "Subscription position.")
    private SubscriptionInitialPosition subscriptionInitialPosition = SubscriptionInitialPosition.Latest;

    @Option(names = { "-ca", "--crypto-failure-action" }, description = "Crypto Failure Action")
    private ConsumerCryptoFailureAction cryptoFailureAction = ConsumerCryptoFailureAction.FAIL;

    @Option(names = { "-stp", "--start-timestamp" }, description = "Start timestamp for consuming messages")
    private long startTimestamp = 0L;

    private Supplier<ClientBuilder> clientBuilder;

    public CmdConsumeV4() {
        super();
    }

    /**
     * Set client configuration. The builder is supplied lazily so that constructing it — which
     * validates the service URL and parses the whole {@code client.conf} — only happens when this
     * command actually runs, not on every {@code pulsar-client} invocation.
     */
    public void updateConfig(Supplier<ClientBuilder> clientBuilder, Authentication authentication,
                             String serviceURL) {
        this.clientBuilder = clientBuilder;
        updateSharedConfig(authentication, serviceURL);
    }

    @Override
    protected void validateArguments() {
        if (this.startTimestamp < 0) {
            throw new CommandLine.ParameterException(commandSpec.commandLine(),
                    "start timestamp should be positive.");
        }
        if (this.endTimestamp < startTimestamp) {
            throw new CommandLine.ParameterException(commandSpec.commandLine(),
                    "end timestamp should be greater than start timestamp.");
        }
    }

    @Override
    @SuppressWarnings("deprecation")
    protected int consume(String topic) {
        int numMessagesConsumed = 0;
        int returnCode = 0;

        try (PulsarClient client = clientBuilder.get().build()) {
            Schema<?> schema = poolMessages ? Schema.BYTEBUFFER : Schema.BYTES;
            if ("auto_consume".equals(schemaType)) {
                schema = Schema.AUTO_CONSUME();
            } else if (!"bytes".equals(schemaType)) {
                throw new IllegalArgumentException("schema type must be 'bytes' or 'auto_consume'");
            }
            ConsumerBuilder<?> builder = client.newConsumer(schema)
                    .subscriptionName(this.subscriptionName)
                    .subscriptionType(
                            org.apache.pulsar.client.api.SubscriptionType.valueOf(subscriptionType.name()))
                    .subscriptionMode(
                            org.apache.pulsar.client.api.SubscriptionMode.valueOf(subscriptionMode.name()))
                    .subscriptionInitialPosition(subscriptionInitialPosition)
                    .poolMessages(poolMessages)
                    .replicateSubscriptionState(replicateSubscriptionState);

            if (isRegex) {
                builder.topicsPattern(Pattern.compile(topic));
            } else {
                builder.topic(topic);
            }

            if (this.maxPendingChunkedMessage > 0) {
                builder.maxPendingChunkedMessage(this.maxPendingChunkedMessage);
            }
            if (this.receiverQueueSize > 0) {
                builder.receiverQueueSize(this.receiverQueueSize);
            }

            builder.autoAckOldestChunkedMessageOnQueueFull(this.autoAckOldestChunkedMessageOnQueueFull);
            builder.cryptoFailureAction(cryptoFailureAction);

            if (isNotBlank(this.encKeyValue)) {
                builder.defaultCryptoKeyReader(this.encKeyValue);
            }

            try (Consumer<?> consumer = builder.subscribe()) {
                if (startTimestamp > 0L) {
                    consumer.seek(startTimestamp);
                }
                RateLimiter limiter = (this.consumeRate > 0) ? RateLimiter.create(this.consumeRate) : null;
                while (this.numMessagesToConsume == 0 || numMessagesConsumed < this.numMessagesToConsume) {
                    if (limiter != null) {
                        limiter.acquire();
                    }
                    Message<?> msg = consumer.receive(5, TimeUnit.SECONDS);
                    if (msg == null) {
                        LOG.debug("No message to consume after waiting for 5 seconds.");
                    } else {
                        try {
                            if (msg.getPublishTime() > endTimestamp) {
                                break;
                            }
                            numMessagesConsumed += 1;
                            if (!hideContent) {
                                System.out.println(MESSAGE_BOUNDARY);
                                System.out.println(
                                        V4MessageSupport.interpretMessage(msg, displayHex, printMetadata));
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
