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
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.RateLimiter;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.ReaderBuilder;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.naming.TopicName;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.websocket.client.ClientUpgradeRequest;
import org.eclipse.jetty.websocket.client.WebSocketClient;

/**
 * pulsar-client read command implementation.
 *
 */
@Parameters(commandDescription = "Read messages from a specified topic")
public class CmdRead extends AbstractCmdConsume {

    private static final Pattern MSG_ID_PATTERN = Pattern.compile("^(-?[1-9][0-9]*|0):(-?[1-9][0-9]*|0)$");

    @Parameter(description = "TopicName", required = true)
    private List<String> mainOptions = new ArrayList<String>();

    @Parameter(names = { "-m", "--start-message-id" },
            description = "Initial reader position, it can be 'latest', 'earliest' or '<ledgerId>:<entryId>'")
    private String startMessageId = "latest";

    @Parameter(names = { "-i", "--start-message-id-inclusive" },
            description = "Whether to include the position specified by -m option.")
    private boolean startMessageIdInclusive = false;

    @Parameter(names = { "-n",
            "--num-messages" }, description = "Number of messages to read, 0 means to read forever.")
    private int numMessagesToRead = 1;

    @Parameter(names = { "--hex" }, description = "Display binary messages in hex.")
    private boolean displayHex = false;

    @Parameter(names = { "--hide-content" }, description = "Do not write the message to console.")
    private boolean hideContent = false;

    @Parameter(names = { "-r", "--rate" }, description = "Rate (in msg/sec) at which to read, "
            + "value 0 means to read messages as fast as possible.")
    private double readRate = 0;

    @Parameter(names = {"-q", "--queue-size"}, description = "Reader receiver queue size.")
    private int receiverQueueSize = 0;

    @Parameter(names = { "-mc", "--max_chunked_msg" }, description = "Max pending chunk messages")
    private int maxPendingChunkedMessage = 0;

    @Parameter(names = { "-ac",
            "--auto_ack_chunk_q_full" }, description = "Auto ack for oldest message on queue is full")
    private boolean autoAckOldestChunkedMessageOnQueueFull = false;

    @Parameter(names = { "-ekv",
            "--encryption-key-value" }, description = "The URI of private key to decrypt payload, for example "
                    + "file:///path/to/private.key or data:application/x-pem-file;base64,*****")
    private String encKeyValue;

    @Parameter(names = { "-st", "--schema-type"},
            description = "Set a schema type on the reader, it can be 'bytes' or 'auto_consume'")
    private String schemaType = "bytes";

    @Parameter(names = { "-pm", "--pool-messages" }, description = "Use the pooled message", arity = 1)
    private boolean poolMessages = true;

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
        if (mainOptions.size() != 1) {
            throw (new ParameterException("Please provide one and only one topic name."));
        }
        if (this.numMessagesToRead < 0) {
            throw (new ParameterException("Number of messages should be zero or positive."));
        }

        String topic = this.mainOptions.get(0);

        if (this.serviceURL.startsWith("ws")) {
            return readFromWebSocket(topic);
        } else {
            return read(topic);
        }
    }

    private int read(String topic) {
        int numMessagesRead = 0;
        int returnCode = 0;

        try (PulsarClient client = clientBuilder.build()){
            ReaderBuilder<?> builder;

            Schema<?> schema = poolMessages ? Schema.BYTEBUFFER : Schema.BYTES;
            if ("auto_consume".equals(schemaType)) {
                schema = Schema.AUTO_CONSUME();
            } else if (!"bytes".equals(schemaType)) {
                throw new IllegalArgumentException("schema type must be 'bytes' or 'auto_consume'");
            }
            builder = client.newReader(schema)
                    .topic(topic)
                    .startMessageId(parseMessageId(startMessageId))
                    .poolMessages(poolMessages);

            if (this.startMessageIdInclusive) {
                builder.startMessageIdInclusive();
            }
            if (this.maxPendingChunkedMessage > 0) {
                builder.maxPendingChunkedMessage(this.maxPendingChunkedMessage);
            }
            if (this.receiverQueueSize > 0) {
                builder.receiverQueueSize(this.receiverQueueSize);
            }

            builder.autoAckOldestChunkedMessageOnQueueFull(this.autoAckOldestChunkedMessageOnQueueFull);

            if (isNotBlank(this.encKeyValue)) {
                builder.defaultCryptoKeyReader(this.encKeyValue);
            }

            try (Reader<?> reader = builder.create()) {
                RateLimiter limiter = (this.readRate > 0) ? RateLimiter.create(this.readRate) : null;
                while (this.numMessagesToRead == 0 || numMessagesRead < this.numMessagesToRead) {
                    if (limiter != null) {
                        limiter.acquire();
                    }

                    Message<?> msg = reader.readNext(5, TimeUnit.SECONDS);
                    if (msg == null) {
                        LOG.debug("No message to read after waiting for 5 seconds.");
                    } else {
                        try {
                            numMessagesRead += 1;
                            if (!hideContent) {
                                System.out.println(MESSAGE_BOUNDARY);
                                String output = this.interpretMessage(msg, displayHex);
                                System.out.println(output);
                            } else if (numMessagesRead % 1000 == 0) {
                                System.out.println("Received " + numMessagesRead + " messages");
                            }
                        } finally {
                            msg.release();
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

    @SuppressWarnings("deprecation")
    @VisibleForTesting
    public String getWebSocketReadUri(String topic) {
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

        String msgIdQueryParam;
        if ("latest".equals(startMessageId) || "earliest".equals(startMessageId)) {
            msgIdQueryParam = startMessageId;
        } else {
            MessageId msgId = parseMessageId(startMessageId);
            msgIdQueryParam = Base64.getEncoder().encodeToString(msgId.toByteArray());
        }

        String uriFormat = "%s/ws" + (topicName.isV2() ? "/v2/" : "/") + "reader/%s?messageId=%s";
        return String.format(uriFormat, serviceURLWithoutTrailingSlash, wsTopic, msgIdQueryParam);
    }

    @SuppressWarnings("deprecation")
    private int readFromWebSocket(String topic) {
        int numMessagesRead = 0;
        int returnCode = 0;

        URI readerUri = URI.create(getWebSocketReadUri(topic));

        WebSocketClient readClient = new WebSocketClient(new SslContextFactory(true));
        ClientUpgradeRequest readRequest = new ClientUpgradeRequest();
        try {
            if (authentication != null) {
                authentication.start();
                AuthenticationDataProvider authData = authentication.getAuthData();
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
            readClient.connect(readerSocket, readerUri, readRequest);
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

        return returnCode;
    }

    @VisibleForTesting
    static MessageId parseMessageId(String msgIdStr) {
        MessageId msgId;
        if ("latest".equals(msgIdStr)) {
            msgId = MessageId.latest;
        } else if ("earliest".equals(msgIdStr)) {
            msgId = MessageId.earliest;
        } else {
            Matcher matcher = MSG_ID_PATTERN.matcher(msgIdStr);
            if (matcher.find()) {
                msgId = new MessageIdImpl(Long.parseLong(matcher.group(1)), Long.parseLong(matcher.group(2)), -1);
            } else {
                throw new IllegalArgumentException("Message ID must be 'latest', 'earliest' or '<ledgerId>:<entryId>'");
            }
        }
        return msgId;
    }

}
