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
import java.time.Duration;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.api.v5.Message;
import org.apache.pulsar.client.api.v5.PulsarClient;
import org.apache.pulsar.client.api.v5.QueueConsumer;
import org.apache.pulsar.client.api.v5.QueueConsumerBuilder;
import org.apache.pulsar.client.api.v5.auth.ConsumerCryptoFailureAction;
import org.apache.pulsar.client.api.v5.config.ConsumerEncryptionPolicy;
import org.apache.pulsar.client.api.v5.config.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.apache.pulsar.common.naming.TopicName;
import org.eclipse.jetty.client.HttpClient;
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

    /**
     * v4-compatible subscription-type flag. This version of pulsar-client consumes through a V5
     * {@link QueueConsumer} for all types, since it is the only consumer that works against both
     * regular and scalable topics (the ordered StreamConsumer requires a scalable-topic
     * controller). Exclusive / Failover therefore get work-queue (Shared-style) semantics and log
     * a warning rather than preserving single-reader ordering.
     */
    public enum SubscriptionType {
        Exclusive,
        Shared,
        Failover,
        Key_Shared
    }

    /** v4-compatible subscription-mode flag. Only honored by the WebSocket path; the V5 binary
     *  consumer is always durable, so NonDurable logs a warning. */
    public enum SubscriptionMode {
        Durable,
        NonDurable
    }

    @Parameters(description = "TopicName", arity = "1")
    private String topic;

    @Option(names = { "-t", "--subscription-type" }, description = "Subscription type.")
    private SubscriptionType subscriptionType = SubscriptionType.Exclusive;

    @Option(names = { "-m", "--subscription-mode" }, description = "Subscription mode.")
    private SubscriptionMode subscriptionMode = SubscriptionMode.Durable;

    @Option(names = { "-p", "--subscription-position" }, description = "Subscription position.")
    private SubscriptionInitialPosition subscriptionInitialPosition = SubscriptionInitialPosition.LATEST;

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

    @Option(names = { "-etp", "--end-timestamp" }, description = "End timestamp for consuming messages")
    private long endTimestamp = Long.MAX_VALUE;

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
        if (this.endTimestamp < 0) {
            throw new CommandLine.ParameterException(commandSpec.commandLine(),
                    "end timestamp should be positive.");
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
                    + "a durable subscription is used instead.");
        }
        if (subscriptionType == SubscriptionType.Exclusive || subscriptionType == SubscriptionType.Failover) {
            // The V5 StreamConsumer (ordered, single-reader) requires a scalable-topic subscription
            // controller, which regular topics do not have; only the QueueConsumer works against
            // both regular and scalable topics. So all subscription types use a QueueConsumer here
            // and Exclusive/Failover get work-queue (Shared-style) semantics rather than ordered.
            LOG.warn("--subscription-type {} : this version of pulsar-client consumes via a work-queue "
                    + "(Shared-style) subscription; exclusive/failover ordering is not preserved.",
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
                            System.out.println(this.interpretMessage(msg, displayHex, printMetadata));
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
     * namespace live).
     */
    private void applyTopicSelection(java.util.function.Consumer<String> topicFn,
                                     java.util.function.Consumer<String> namespaceFn) {
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
        return buildFileDecryptionPolicy(this.encKeyValue, cryptoFailureAction);
    }

    @VisibleForTesting
    public String getWebSocketConsumeUri(String topic) {
        String serviceURLWithoutTrailingSlash = serviceURL.substring(0,
                serviceURL.endsWith("/") ? serviceURL.length() - 1 : serviceURL.length());

        TopicName topicName = TopicName.get(topic);
        String wsTopic = String.format("%s/%s/%s/%s", topicName.getDomain(), topicName.getTenant(),
                topicName.getNamespacePortion(), topicName.getLocalName());

        return String.format("%s/ws/v2/consumer/%s/%s?subscriptionType=%s&subscriptionMode=%s",
                serviceURLWithoutTrailingSlash, wsTopic, subscriptionName,
                subscriptionType.toString(), subscriptionMode.toString());
    }

    @SuppressWarnings("deprecation")
    private int consumeFromWebSocket(String topic) {
        int numMessagesConsumed = 0;
        int returnCode = 0;

        URI consumerUri = URI.create(getWebSocketConsumeUri(topic));

        HttpClient httpClient = new HttpClient();
        httpClient.setSslContextFactory(new SslContextFactory.Client(true));
        WebSocketClient consumeClient = new WebSocketClient(httpClient);
        consumeClient.setMaxTextMessageSize(64 * 1024);
        ClientUpgradeRequest consumeRequest = new ClientUpgradeRequest(consumerUri);
        try {
            if (authentication != null) {
                authentication.start();
                AuthenticationDataProvider authData = authentication.getAuthData(consumerUri.getHost());
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
            consumeClient.connect(consumerSocket, consumeRequest);
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


        try {
            consumeClient.stop();
        } catch (Exception e) {
            LOG.error("Failed to stop websocket-client", e);
        }
        try {
            httpClient.stop();
        } catch (Exception e) {
            LOG.error("Failed to stop http-client", e);
        }
        return returnCode;
    }

}
