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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.RateLimiter;
import java.io.IOException;
import java.net.URI;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.pulsar.cli.ClientApi;
import org.apache.pulsar.cli.ClientApiOptionGroups;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.ConsumerCryptoFailureAction;
import org.apache.pulsar.client.api.v5.PulsarClientBuilder;
import org.apache.pulsar.common.naming.TopicName;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.websocket.client.ClientUpgradeRequest;
import org.eclipse.jetty.websocket.client.WebSocketClient;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.Spec;

/**
 * The {@code pulsar-client read} command: the CLI options, the argument validation and the
 * WebSocket reading path (which speaks HTTP and has no client generation of its own). Reading over
 * the binary protocol is delegated to {@link ReadV5}, which drives a V5 {@code CheckpointConsumer},
 * or {@link ReadV4}, which drives a v4 {@code Reader}, picked from the topic domain or
 * {@code --client-api}.
 */
@Command(name = "read", description = {"Read messages from a specified topic", "",
        AbstractCmd.CLIENT_API_DESCRIPTION},
        sortOptions = false, optionListHeading = "%nCommon options:%n")
public class CmdRead extends AbstractCmdConsume {

    protected static final String START_EARLIEST = "earliest";
    protected static final String START_LATEST = "latest";

    @Parameters(description = "TopicName", arity = "1")
    protected String topic;

    @Option(names = ClientApi.OPTION_NAME, description = ClientApi.OPTION_DESCRIPTION)
    protected ClientApi clientApi;

    @Option(names = { "-m", "--start-message-id" },
            description = "Initial reader position, it can be 'latest', 'earliest' or '<ledgerId>:<entryId>' "
                    + "(the last form requires the v4 client)")
    protected String startMessageId = START_LATEST;

    @Option(names = { "-n",
            "--num-messages" }, description = "Number of messages to read, 0 means to read forever.")
    protected int numMessagesToRead = 1;

    @Option(names = { "--hex" }, description = "Display binary messages in hex.")
    protected boolean displayHex = false;

    @Option(names = { "--hide-content" }, description = "Do not write the message to console.")
    protected boolean hideContent = false;

    @Option(names = { "-r", "--rate" }, description = "Rate (in msg/sec) at which to read, "
            + "value 0 means to read messages as fast as possible.")
    protected double readRate = 0;

    @Option(names = { "-ekv",
            "--encryption-key-value" }, description = "The URI of private key to decrypt payload, for example "
            + "file:///path/to/private.key or data:application/x-pem-file;base64,***** (data: URIs require the "
            + "v4 client)")
    protected String encKeyValue;

    @Option(names = { "-ca", "--crypto-failure-action" }, description = "Crypto Failure Action")
    protected ConsumerCryptoFailureAction cryptoFailureAction = ConsumerCryptoFailureAction.FAIL;

    @Option(names = { "-st", "--schema-type" },
            description = "Set a schema type on the reader, it can be 'bytes' or 'auto_consume'")
    protected String schemaType = "bytes";

    @Option(names = { "-mp", "--print-metadata" }, description = "Message metadata")
    protected boolean printMetadata = false;

    @ArgGroup(exclusive = false, validate = false, order = 1, heading = ClientApiOptionGroups.V4_HEADING)
    protected V4Options v4 = new V4Options();

    /** Options that only the v4 client supports. */
    public static class V4Options implements ClientApiOptionGroups.V4ClientOptions {
        @Option(names = { "-i", "--start-message-id-inclusive" },
                description = "Whether to include the position specified by -m option.")
        protected boolean startMessageIdInclusive = false;

        @Option(names = { "-q", "--queue-size" }, description = "Reader receiver queue size.")
        protected int receiverQueueSize = 0;

        @Option(names = { "-mc", "--max_chunked_msg" }, description = "Max pending chunk messages")
        protected int maxPendingChunkedMessage = 0;

        @Option(names = { "-ac",
                "--auto_ack_chunk_q_full" }, description = "Auto ack for oldest message on queue is full")
        protected boolean autoAckOldestChunkedMessageOnQueueFull = false;

        @Option(names = { "-pm", "--pool-messages" }, description = "Use the pooled message", arity = "1")
        protected boolean poolMessages = true;
    }

    private PulsarClientBuilder clientBuilder;
    private Supplier<ClientBuilder> v4ClientBuilder;

    @Spec
    protected CommandSpec commandSpec;

    public CmdRead() {
        super();
    }

    /**
     * Set the V5 client configuration, and the settings shared by both clients.
     */
    public void updateConfig(PulsarClientBuilder clientBuilder, Authentication authentication, String serviceURL) {
        this.clientBuilder = clientBuilder;
        updateSharedConfig(authentication, serviceURL);
    }

    /**
     * Set the v4 client configuration. The builder is supplied lazily so that constructing it —
     * which validates the service URL and parses the whole {@code client.conf} — only happens when
     * the v4 client is actually used, not on every {@code pulsar-client} invocation.
     */
    public void updateV4Config(Supplier<ClientBuilder> clientBuilder) {
        this.v4ClientBuilder = clientBuilder;
    }

    /**
     * Run the read command.
     *
     * @return 0 for success, &lt; 0 otherwise
     */
    public int run() throws IOException {
        if (this.numMessagesToRead < 0) {
            throw (new IllegalArgumentException("Number of messages should be zero or positive."));
        }
        ClientApi resolvedClientApi = resolveClientApi(commandSpec, clientApi, topic, serviceURL);
        validateStartMessageId(resolvedClientApi);
        if (resolvedClientApi == ClientApi.V5) {
            validateV5EncryptionKeyUri(commandSpec, encKeyValue);
        }

        if (isWebSocketUrl(this.serviceURL)) {
            return readFromWebSocket(topic);
        }
        LOG.info("Using the {} for topic {}", resolvedClientApi.displayName(), topic);
        if (resolvedClientApi == ClientApi.V5) {
            return new ReadV5(this, clientBuilder).read(topic);
        } else {
            return new ReadV4(this, v4ClientBuilder.get()).read(topic);
        }
    }

    /**
     * The V5 client can only start at {@code latest} or {@code earliest}; the v4 client also takes
     * a {@code <ledgerId>:<entryId>}, which is checked here to fail fast on a malformed id.
     */
    @VisibleForTesting
    void validateStartMessageId(ClientApi resolvedClientApi) {
        if (START_LATEST.equals(startMessageId) || START_EARLIEST.equals(startMessageId)) {
            return;
        }
        if (resolvedClientApi == ClientApi.V5) {
            throw new CommandLine.ParameterException(commandSpec.commandLine(), "--start-message-id must be "
                    + "'latest' or 'earliest' with the V5 client; for a '<ledgerId>:<entryId>' start position, "
                    + USE_V4_CLIENT_HINT + ".");
        }
        try {
            ReadV4.parseMessageId(startMessageId);
        } catch (IllegalArgumentException e) {
            throw new CommandLine.ParameterException(commandSpec.commandLine(), e.getMessage(), e);
        }
    }

    /** The {@code messageId} query parameter of the WebSocket reader URI. */
    @VisibleForTesting
    String webSocketStartMessageId() {
        if (START_LATEST.equals(startMessageId) || START_EARLIEST.equals(startMessageId)) {
            return startMessageId;
        }
        return Base64.getEncoder().encodeToString(ReadV4.parseMessageId(startMessageId).toByteArray());
    }

    @VisibleForTesting
    public String getWebSocketReadUri(String topic) {
        String serviceURLWithoutTrailingSlash = serviceURL.substring(0,
                serviceURL.endsWith("/") ? serviceURL.length() - 1 : serviceURL.length());

        TopicName topicName = TopicName.get(topic);
        String wsTopic = String.format("%s/%s/%s/%s", topicName.getDomain(), topicName.getTenant(),
                topicName.getNamespacePortion(), topicName.getLocalName());

        return String.format("%s/ws/v2/reader/%s?messageId=%s", serviceURLWithoutTrailingSlash, wsTopic,
                webSocketStartMessageId());
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
