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
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.v5.Checkpoint;
import org.apache.pulsar.client.api.v5.CheckpointConsumer;
import org.apache.pulsar.client.api.v5.CheckpointConsumerBuilder;
import org.apache.pulsar.client.api.v5.Message;
import org.apache.pulsar.client.api.v5.PulsarClient;
import org.apache.pulsar.client.api.v5.PulsarClientBuilder;
import org.apache.pulsar.client.api.v5.auth.ConsumerCryptoFailureAction;
import org.apache.pulsar.client.api.v5.config.ConsumerEncryptionPolicy;
import org.apache.pulsar.client.api.v5.schema.Schema;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * pulsar-client read command implementation, on the V5 client API.
 *
 * <p>V5 has no {@code Reader}; the closest equivalent is the {@code CheckpointConsumer}, which is
 * what this command drives. Everything that is not V5-specific lives in
 * {@link AbstractCmdReadCommand}; the v4 {@code Reader} is driven by {@link CmdReadV4} under the
 * {@code read-v4} name.
 */
@Command(name = "read", description = "Read messages from a specified topic")
public class CmdRead extends AbstractCmdReadCommand {

    @Option(names = { "-m", "--start-message-id" },
            description = "Initial reader position, it can be 'latest' or 'earliest'")
    private String startMessageId = "latest";

    @Option(names = { "-i", "--start-message-id-inclusive" },
            description = "Whether to include the position specified by -m option.")
    private boolean startMessageIdInclusive = false;

    @Option(names = { "-ca", "--crypto-failure-action" }, description = "Crypto Failure Action")
    private ConsumerCryptoFailureAction cryptoFailureAction = ConsumerCryptoFailureAction.FAIL;

    private PulsarClientBuilder clientBuilder;

    public CmdRead() {
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
    protected String startMessageId() {
        return startMessageId;
    }

    @Override
    protected String webSocketStartMessageId() {
        // Only 'latest' / 'earliest' are accepted (validated in validateArguments()).
        return startMessageId;
    }

    @Override
    protected void validateArguments() {
        if (!START_LATEST.equals(startMessageId) && !START_EARLIEST.equals(startMessageId)) {
            throw new IllegalArgumentException("--start-message-id must be 'latest' or 'earliest'; the "
                    + "'<ledgerId>:<entryId>' form is not supported by this version of pulsar-client. "
                    + "Use read-v4, which does support it.");
        }
    }

    @Override
    protected int read(String topic) {
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
        if (this.receiverQueueSize > 0) {
            LOG.info("--queue-size has no effect on this version of pulsar-client's read command.");
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
                            System.out.println(
                                    V5MessageSupport.interpretMessage(msg, displayHex, printMetadata));
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
        return V5MessageSupport.buildFileDecryptionPolicy(this.encKeyValue, cryptoFailureAction);
    }
}
