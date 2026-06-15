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
import org.apache.pulsar.client.api.v5.Checkpoint;
import org.apache.pulsar.client.api.v5.CheckpointConsumer;
import org.apache.pulsar.client.api.v5.CheckpointConsumerBuilder;
import org.apache.pulsar.client.api.v5.Message;
import org.apache.pulsar.client.api.v5.PulsarClient;
import org.apache.pulsar.client.api.v5.PulsarClientException;
import org.apache.pulsar.client.api.v5.auth.ConsumerCryptoFailureAction;
import org.apache.pulsar.client.api.v5.config.ConsumerEncryptionPolicy;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.apache.pulsar.common.naming.TopicName;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.websocket.client.ClientUpgradeRequest;
import org.eclipse.jetty.websocket.client.WebSocketClient;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

/**
 * pulsar-client read command implementation.
 */
@Command(description = "Read messages from a specified topic")
public class CmdRead extends AbstractCmdConsume {

    @Parameters(description = "TopicName", arity = "1")
    private String topic;

    @Option(names = { "-m", "--start-message-id" },
            description = "Initial reader position, it can be 'latest' or 'earliest'")
    private String startMessageId = "latest";

    @Option(names = { "-i", "--start-message-id-inclusive" },
            description = "Whether to include the position specified by -m option.")
    private boolean startMessageIdInclusive = false;

    @Option(names = { "-n",
            "--num-messages" }, description = "Number of messages to read, 0 means to read forever.")
    private int numMessagesToRead = 1;

    @Option(names = { "--hex" }, description = "Display binary messages in hex.")
    private boolean displayHex = false;

    @Option(names = { "--hide-content" }, description = "Do not write the message to console.")
    private boolean hideContent = false;

    @Option(names = { "-r", "--rate" }, description = "Rate (in msg/sec) at which to read, "
            + "value 0 means to read messages as fast as possible.")
    private double readRate = 0;

    @Option(names = { "-q", "--queue-size" }, description = "Reader receiver queue size.")
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

    @Option(names = { "-st", "--schema-type" },
            description = "Set a schema type on the reader, it can be 'bytes' or 'auto_consume'")
    private String schemaType = "bytes";

    @Option(names = { "-pm", "--pool-messages" }, description = "Use the pooled message", arity = "1")
    private boolean poolMessages = true;

    @Option(names = { "-ca", "--crypto-failure-action" }, description = "Crypto Failure Action")
    private ConsumerCryptoFailureAction cryptoFailureAction = ConsumerCryptoFailureAction.FAIL;

    private static final String START_EARLIEST = "earliest";
    private static final String START_LATEST = "latest";

    @Option(names = { "-mp", "--print-metadata" }, description = "Message metadata")
    private boolean printMetadata = false;

    public CmdRead() {
        // Do nothing
        super();
    }

    /**
     * Run the read command.
     *
     * @return 0 for success, < 0 otherwise
     */
    public int run() throws PulsarClientException, IOException {
        if (this.numMessagesToRead < 0) {
            throw (new IllegalArgumentException("Number of messages should be zero or positive."));
        }
        if (!START_LATEST.equals(startMessageId) && !START_EARLIEST.equals(startMessageId)) {
            throw new IllegalArgumentException("--start-message-id must be 'latest' or 'earliest'; the "
                    + "'<ledgerId>:<entryId>' form is not supported by this version of pulsar-client.");
        }

        if (this.serviceURL.startsWith("ws")) {
            return readFromWebSocket(topic);
        } else {
            return read(topic);
        }
    }

    private int read(String topic) {
        int numMessagesRead = 0;
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
        if (this.startMessageIdInclusive) {
            LOG.warn("--start-message-id-inclusive has no effect on this version of pulsar-client.");
        }
        if (maxPendingChunkedMessage > 0 || autoAckOldestChunkedMessageOnQueueFull) {
            LOG.warn("Chunked-message knobs (--max_chunked_msg / --auto_ack_chunk_q_full) have no effect "
                    + "on this version of pulsar-client.");
        }

        Checkpoint startPosition = START_EARLIEST.equals(startMessageId)
                ? Checkpoint.earliest() : Checkpoint.latest();

        try (PulsarClient client = clientBuilder.build()) {
            CheckpointConsumerBuilder<?> builder = client.newCheckpointConsumer(schema)
                    .topic(topic)
                    .startPosition(startPosition);
            if (isNotBlank(this.encKeyValue)) {
                builder.encryptionPolicy(buildConsumerEncryptionPolicy());
            }

            try (CheckpointConsumer<?> reader = builder.create()) {
                RateLimiter limiter = (this.readRate > 0) ? RateLimiter.create(this.readRate) : null;
                while (this.numMessagesToRead == 0 || numMessagesRead < this.numMessagesToRead) {
                    if (limiter != null) {
                        limiter.acquire();
                    }

                    Message<?> msg = reader.receive(Duration.ofSeconds(5));
                    if (msg == null) {
                        LOG.debug("No message to read after waiting for 5 seconds.");
                    } else {
                        numMessagesRead += 1;
                        if (!hideContent) {
                            System.out.println(MESSAGE_BOUNDARY);
                            System.out.println(this.interpretMessage(msg, displayHex, printMetadata));
                        } else if (numMessagesRead % 1000 == 0) {
                            System.out.println("Received " + numMessagesRead + " messages");
                        }
                    }
                }
            }
        } catch (Exception e) {
            LOG.error("Error while reading messages");
            LOG.error(e.getMessage(), e);
            returnCode = -1;
        } finally {
            LOG.info("{} messages successfully read", numMessagesRead);
        }

        return returnCode;
    }

    private ConsumerEncryptionPolicy buildConsumerEncryptionPolicy() {
        return buildFileDecryptionPolicy(this.encKeyValue, cryptoFailureAction);
    }

    @VisibleForTesting
    public String getWebSocketReadUri(String topic) {
        String serviceURLWithoutTrailingSlash = serviceURL.substring(0,
                serviceURL.endsWith("/") ? serviceURL.length() - 1 : serviceURL.length());

        TopicName topicName = TopicName.get(topic);
        String wsTopic = String.format("%s/%s/%s/%s", topicName.getDomain(), topicName.getTenant(),
                topicName.getNamespacePortion(), topicName.getLocalName());

        // Only 'latest' / 'earliest' are accepted (validated in run()).
        return String.format("%s/ws/v2/reader/%s?messageId=%s", serviceURLWithoutTrailingSlash, wsTopic,
                startMessageId);
    }

    @SuppressWarnings("deprecation")
    private int readFromWebSocket(String topic) {
        int numMessagesRead = 0;
        int returnCode = 0;

        URI readerUri = URI.create(getWebSocketReadUri(topic));

        HttpClient httpClient = new HttpClient();
        httpClient.setSslContextFactory(new SslContextFactory.Client(true));
        WebSocketClient readClient = new WebSocketClient(httpClient);
        ClientUpgradeRequest readRequest = new ClientUpgradeRequest(readerUri);
        try {
            if (authentication != null) {
                authentication.start();
                AuthenticationDataProvider authData = authentication.getAuthData(readerUri.getHost());
                if (authData.hasDataForHttp()) {
                    for (Map.Entry<String, String> kv : authData.getHttpHeaders()) {
                        readRequest.setHeader(kv.getKey(), kv.getValue());
                    }
                }
            }
        } catch (Exception e) {
            LOG.error("Authentication plugin error: " + e.getMessage());
            return -1;
        }
        CompletableFuture<Void> connected = new CompletableFuture<>();
        ConsumerSocket readerSocket = new ConsumerSocket(connected);
        try {
            readClient.start();
        } catch (Exception e) {
            LOG.error("Failed to start websocket-client", e);
            return -1;
        }

        try {
            LOG.info("Trying to create websocket session..{}", readerUri);
            readClient.connect(readerSocket, readRequest);
            connected.get();
        } catch (Exception e) {
            LOG.error("Failed to create web-socket session", e);
            return -1;
        }

        try {
            RateLimiter limiter = (this.readRate > 0) ? RateLimiter.create(this.readRate) : null;
            while (this.numMessagesToRead == 0 || numMessagesRead < this.numMessagesToRead) {
                if (limiter != null) {
                    limiter.acquire();
                }
                String msg = readerSocket.receive(5, TimeUnit.SECONDS);
                if (msg == null) {
                    LOG.debug("No message to read after waiting for 5 seconds.");
                } else {
                    try {
                        String output = interpretByteArray(displayHex, Base64.getDecoder().decode(msg));
                        System.out.println(output); // print decode
                    } catch (Exception e) {
                        System.out.println(msg);
                    }
                    numMessagesRead += 1;
                }
            }
            readerSocket.awaitClose(2, TimeUnit.SECONDS);
        } catch (Exception e) {
            LOG.error("Error while reading messages");
            LOG.error(e.getMessage(), e);
            returnCode = -1;
        } finally {
            LOG.info("{} messages successfully read", numMessagesRead);
        }

        try {
            readClient.stop();
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
