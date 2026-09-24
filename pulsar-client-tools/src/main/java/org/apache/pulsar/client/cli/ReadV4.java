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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.RateLimiter;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.ReaderBuilder;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.MessageIdImpl;

/**
 * Reads the messages of a {@link CmdRead} invocation with the v4 ({@code pulsar-client-original})
 * {@code Reader}, including the v4-only capabilities: a {@code <ledgerId>:<entryId>} start message id,
 * {@code --start-message-id-inclusive}, {@code --queue-size}, pooled messages, the chunked-message
 * knobs, and the full message metadata rendering.
 */
final class ReadV4 {

    private static final Pattern MSG_ID_PATTERN = Pattern.compile("^(-?[1-9][0-9]*|0):(-?[1-9][0-9]*|0)$");

    private final CmdRead cmd;
    private final CmdRead.V4Options v4;
    private final ClientBuilder clientBuilder;

    ReadV4(CmdRead cmd, ClientBuilder clientBuilder) {
        this.cmd = cmd;
        this.v4 = cmd.v4;
        this.clientBuilder = clientBuilder;
    }

    /**
     * Read the messages.
     *
     * @return 0 for success, &lt; 0 otherwise
     */
    @SuppressWarnings("deprecation")
    int read(String topic) {
        int numMessagesRead = 0;
        int returnCode = 0;

        try (PulsarClient client = clientBuilder.build()) {
            Schema<?> schema = v4.poolMessages ? Schema.BYTEBUFFER : Schema.BYTES;
            if ("auto_consume".equals(cmd.schemaType)) {
                schema = Schema.AUTO_CONSUME();
            } else if (!"bytes".equals(cmd.schemaType)) {
                throw new IllegalArgumentException("schema type must be 'bytes' or 'auto_consume'");
            }
            ReaderBuilder<?> builder = client.newReader(schema)
                    .topic(topic)
                    .startMessageId(parseMessageId(cmd.startMessageId))
                    .poolMessages(v4.poolMessages);

            if (v4.startMessageIdInclusive) {
                builder.startMessageIdInclusive();
            }
            if (v4.maxPendingChunkedMessage > 0) {
                builder.maxPendingChunkedMessage(v4.maxPendingChunkedMessage);
            }
            if (v4.receiverQueueSize > 0) {
                builder.receiverQueueSize(v4.receiverQueueSize);
            }

            builder.autoAckOldestChunkedMessageOnQueueFull(v4.autoAckOldestChunkedMessageOnQueueFull);
            builder.cryptoFailureAction(cmd.cryptoFailureAction);

            if (isNotBlank(cmd.encKeyValue)) {
                builder.defaultCryptoKeyReader(cmd.encKeyValue);
            }

            try (Reader<?> reader = builder.create()) {
                RateLimiter limiter = (cmd.readRate > 0) ? RateLimiter.create(cmd.readRate) : null;
                while (cmd.numMessagesToRead == 0 || numMessagesRead < cmd.numMessagesToRead) {
                    if (limiter != null) {
                        limiter.acquire();
                    }

                    Message<?> msg = reader.readNext(5, TimeUnit.SECONDS);
                    if (msg == null) {
                        LOG.debug("No message to read after waiting for 5 seconds.");
                    } else {
                        try {
                            numMessagesRead += 1;
                            if (!cmd.hideContent) {
                                System.out.println(MESSAGE_BOUNDARY);
                                System.out.println(
                                        V4MessageSupport.interpretMessage(msg, cmd.displayHex, cmd.printMetadata));
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
        if (CmdRead.START_LATEST.equals(msgIdStr)) {
            msgId = MessageId.latest;
        } else if (CmdRead.START_EARLIEST.equals(msgIdStr)) {
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
