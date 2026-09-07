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
import java.util.Base64;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.ConsumerCryptoFailureAction;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.ReaderBuilder;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.MessageIdImpl;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * The {@code read} command driven by the v4 ({@code pulsar-client-original}) client.
 *
 * <p>This is the counterpart of {@link CmdRead}, which drives a V5 {@code CheckpointConsumer} — a
 * different broker-side entity — so without this command nothing in {@code pulsar-client} exercises
 * the v4 {@code Reader}. It also restores the v4-only reader capabilities: a
 * {@code <ledgerId>:<entryId>} start message id (also over WebSocket),
 * {@code --start-message-id-inclusive}, {@code --queue-size}, pooled messages, the chunked-message
 * knobs, and the full message metadata rendering.
 */
@Command(name = "read-v4", description = "Read messages from a specified topic using the v4 client")
public class CmdReadV4 extends AbstractCmdReadCommand {

    private static final Pattern MSG_ID_PATTERN = Pattern.compile("^(-?[1-9][0-9]*|0):(-?[1-9][0-9]*|0)$");

    @Option(names = { "-m", "--start-message-id" },
            description = "Initial reader position, it can be 'latest', 'earliest' or '<ledgerId>:<entryId>'")
    private String startMessageId = "latest";

    @Option(names = { "-i", "--start-message-id-inclusive" },
            description = "Whether to include the position specified by -m option.")
    private boolean startMessageIdInclusive = false;

    @Option(names = { "-ca", "--crypto-failure-action" }, description = "Crypto Failure Action")
    private ConsumerCryptoFailureAction cryptoFailureAction = ConsumerCryptoFailureAction.FAIL;

    private Supplier<ClientBuilder> clientBuilder;

    public CmdReadV4() {
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
    protected String startMessageId() {
        return startMessageId;
    }

    @Override
    protected void validateArguments() {
        // Fail fast on a malformed id rather than at reader creation.
        parseMessageId(startMessageId);
    }

    @Override
    protected String webSocketStartMessageId() {
        if (START_LATEST.equals(startMessageId) || START_EARLIEST.equals(startMessageId)) {
            return startMessageId;
        }
        return Base64.getEncoder().encodeToString(parseMessageId(startMessageId).toByteArray());
    }

    @Override
    @SuppressWarnings("deprecation")
    protected int read(String topic) {
        int numMessagesRead = 0;
        int returnCode = 0;

        try (PulsarClient client = clientBuilder.get().build()) {
            Schema<?> schema = poolMessages ? Schema.BYTEBUFFER : Schema.BYTES;
            if ("auto_consume".equals(schemaType)) {
                schema = Schema.AUTO_CONSUME();
            } else if (!"bytes".equals(schemaType)) {
                throw new IllegalArgumentException("schema type must be 'bytes' or 'auto_consume'");
            }
            ReaderBuilder<?> builder = client.newReader(schema)
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
            builder.cryptoFailureAction(cryptoFailureAction);

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
                                System.out.println(
                                        V4MessageSupport.interpretMessage(msg, displayHex, printMetadata));
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

    @VisibleForTesting
    static MessageId parseMessageId(String msgIdStr) {
        MessageId msgId;
        if (START_LATEST.equals(msgIdStr)) {
            msgId = MessageId.latest;
        } else if (START_EARLIEST.equals(msgIdStr)) {
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
