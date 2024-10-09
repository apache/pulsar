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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.RateLimiter;
import java.io.IOException;
import java.net.URI;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.ConsumerCryptoFailureAction;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.naming.TopicName;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.websocket.client.ClientUpgradeRequest;
import org.eclipse.jetty.websocket.client.WebSocketClient;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.Spec;

/**
 * pulsar-client consume command implementation.
 */
@Command(description = "Consume messages from a specified topic")
public class CmdConsume extends AbstractCmdConsume {

    @Parameters(description = "TopicName", arity = "1")
    private String topic;

    @Option(names = { "-t", "--subscription-type" }, description = "Subscription type.")
    private SubscriptionType subscriptionType = SubscriptionType.Exclusive;

    @Option(names = { "-m", "--subscription-mode" }, description = "Subscription mode.")
    private SubscriptionMode subscriptionMode = SubscriptionMode.Durable;

    @Option(names = { "-p", "--subscription-position" }, description = "Subscription position.")
    private SubscriptionInitialPosition subscriptionInitialPosition = SubscriptionInitialPosition.Latest;

    @Option(names = { "-s", "--subscription-name" }, required = true, description = "Subscription name.")
    private String subscriptionName;

    @Option(names = { "-n",
            "--num-messages" }, description = "Number of messages to consume, 0 means to consume forever.")
    private int numMessagesToConsume = 1;

    @Option(names = { "--hex" }, description = "Display binary messages in hex.")
    private boolean displayHex = false;

    @Option(names = { "--hide-content" }, description = "Do not write the message to console.")
    private boolean hideContent = false;

    @Option(names = { "-r", "--rate" }, description = "Rate (in msg/sec) at which to consume, "
            + "value 0 means to consume messages as fast as possible.")
    private double consumeRate = 0;

    @Option(names = { "--regex" }, description = "Indicate the topic name is a regex pattern")
    private boolean isRegex = false;

    @Option(names = {"-q", "--queue-size"}, description = "Consumer receiver queue size.")
    private int receiverQueueSize = 0;

    @Option(names = { "-mc", "--max_chunked_msg" }, description = "Max pending chunk messages")
    private int maxPendingChunkedMessage = 0;

    @Option(names = { "-ac",
            "--auto_ack_chunk_q_full" }, description = "Auto ack for oldest message on queue is full")
    private boolean autoAckOldestChunkedMessageOnQueueFull = false;

    @Option(names = { "-ekv",
            "--encryption-key-value" }, description = "The URI of private key to decrypt payload, for example "
                    + "file:///path/to/private.key or data:application/x-pem-file;base64,*****")
    private String encKeyValue;

    @Option(names = { "-st", "--schema-type"},
            description = "Set a schema type on the consumer, it can be 'bytes' or 'auto_consume'")
    private String schemaType = "bytes";

    @Option(names = { "-pm", "--pool-messages" }, description = "Use the pooled message", arity = "1")
    private boolean poolMessages = true;

    @Option(names = {"-rs", "--replicated" }, description = "Whether the subscription status should be replicated")
    private boolean replicateSubscriptionState = false;

    @Option(names = { "-ca", "--crypto-failure-action" }, description = "Crypto Failure Action")
    private ConsumerCryptoFailureAction cryptoFailureAction = ConsumerCryptoFailureAction.FAIL;

    @Option(names = { "-mp", "--print-metadata" }, description = "Message metadata")
    private boolean printMetadata = false;

    public CmdConsume() {
        // Do nothing
        super();
    }

    @Spec
    private CommandSpec commandSpec;

    /**
     * Run the consume command.
     *
     * @return 0 for success, < 0 otherwise
     */
    public int run() throws IOException {
        if (this.subscriptionName == null || this.subscriptionName.isEmpty()) {
            throw new CommandLine.ParameterException(commandSpec.commandLine(), "Subscription name is not provided.");
        }
        if (this.numMessagesToConsume < 0) {
            throw new CommandLine.ParameterException(commandSpec.commandLine(),
                    "Number of messages should be zero or positive.");
        }

        if (this.serviceURL.startsWith("ws")) {
            return consumeFromWebSocket(topic);
        } else {
            return consume(topic);
        }
    }

    private int consume(String topic) {
        int numMessagesConsumed = 0;
        int returnCode = 0;

        try (PulsarClient client = clientBuilder.build()) {
            ConsumerBuilder<?> builder;
            Schema<?> schema = poolMessages ? Schema.BYTEBUFFER : Schema.BYTES;
            if ("auto_consume".equals(schemaType)) {
                schema = Schema.AUTO_CONSUME();
            } else if (!"bytes".equals(schemaType)) {
                throw new IllegalArgumentException("schema type must be 'bytes' or 'auto_consume'");
            }
            builder = client.newConsumer(schema)
                    .subscriptionName(this.subscriptionName)
                    .subscriptionType(subscriptionType)
                    .subscriptionMode(subscriptionMode)
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

            try (Consumer<?> consumer = builder.subscribe();) {
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
                            numMessagesConsumed += 1;
                            if (!hideContent) {
                                System.out.println(MESSAGE_BOUNDARY);
                                String output = this.interpretMessage(msg, displayHex, printMetadata);
                                System.out.println(output);
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

    @SuppressWarnings("deprecation")
    @VisibleForTesting
    public String getWebSocketConsumeUri(String topic) {
        String serviceURLWithoutTrailingSlash = serviceURL.substring(0,
                serviceURL.endsWith("/") ? serviceURL.length() - 1 : serviceURL.length());

        TopicName topicName = TopicName.get(topic);
        String wsTopic;
        if (topicName.isV2()) {
            wsTopic = String.format("%s/%s/%s/%s", topicName.getDomain(), topicName.getTenant(),
                    topicName.getNamespacePortion(), topicName.getLocalName());
        } else {
            wsTopic = String.format("%s/%s/%s/%s/%s", topicName.getDomain(), topicName.getTenant(),
                    topicName.getCluster(), topicName.getNamespacePortion(), topicName.getLocalName());
        }

        String uriFormat = "%s/ws" + (topicName.isV2() ? "/v2/" : "/")
                + "consumer/%s/%s?subscriptionType=%s&subscriptionMode=%s";
        return String.format(uriFormat, serviceURLWithoutTrailingSlash, wsTopic, subscriptionName,
                subscriptionType.toString(), subscriptionMode.toString());
    }

    @SuppressWarnings("deprecation")
    private int consumeFromWebSocket(String topic) {
        int numMessagesConsumed = 0;
        int returnCode = 0;

        URI consumerUri = URI.create(getWebSocketConsumeUri(topic));

        WebSocketClient consumeClient = new WebSocketClient(new SslContextFactory(true));
        ClientUpgradeRequest consumeRequest = new ClientUpgradeRequest();
        try {
            if (authentication != null) {
                authentication.start();
                AuthenticationDataProvider authData = authentication.getAuthData();
                if (authData.hasDataForHttp()) {
                    for (Map.Entry<String, String> kv : authData.getHttpHeaders()) {
                        consumeRequest.setHeader(kv.getKey(), kv.getValue());
                    }
                }
            }
        } catch (Exception e) {
            LOG.error("Authentication plugin error: " + e.getMessage());
            return -1;
        }
        CompletableFuture<Void> connected = new CompletableFuture<>();
        ConsumerSocket consumerSocket = new ConsumerSocket(connected);
        try {
            consumeClient.start();
        } catch (Exception e) {
            LOG.error("Failed to start websocket-client", e);
            return -1;
        }

        try {
            LOG.info("Trying to create websocket session..{}", consumerUri);
            consumeClient.connect(consumerSocket, consumerUri, consumeRequest);
            connected.get();
        } catch (Exception e) {
            LOG.error("Failed to create web-socket session", e);
            return -1;
        }

        try {
            RateLimiter limiter = (this.consumeRate > 0) ? RateLimiter.create(this.consumeRate) : null;
            while (this.numMessagesToConsume == 0 || numMessagesConsumed < this.numMessagesToConsume) {
                if (limiter != null) {
                    limiter.acquire();
                }
                String msg = consumerSocket.receive(5, TimeUnit.SECONDS);
                if (msg == null) {
                    LOG.debug("No message to consume after waiting for 5 seconds.");
                } else {
                    try {
                        String output = interpretByteArray(displayHex, Base64.getDecoder().decode(msg));
                        System.out.println(output); // print decode
                    } catch (Exception e) {
                        System.out.println(msg);
                    }
                    numMessagesConsumed += 1;
                }
            }
            consumerSocket.awaitClose(2, TimeUnit.SECONDS);
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
