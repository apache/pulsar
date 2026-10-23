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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.RateLimiter;
import com.google.gson.JsonParseException;
import io.github.merlimat.slog.Logger;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Stream;
import lombok.CustomLog;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.apache.pulsar.cli.ClientApi;
import org.apache.pulsar.cli.ClientApiOptionGroups;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.v5.PulsarClientBuilder;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.websocket.data.ProducerMessage;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.websocket.api.Callback;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketClose;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketOpen;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;
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
 * The {@code pulsar-client produce} command: the CLI options, the message bodies, the WebSocket
 * publishing path (which speaks HTTP and has no client generation of its own) and the argument
 * validation. Publishing over the binary protocol is delegated to {@link ProduceV5} or
 * {@link ProduceV4}, picked from the topic domain or {@code --client-api}.
 */
@Command(name = "produce", description = {"Produce messages to a specified topic", "",
        AbstractCmd.CLIENT_API_DESCRIPTION},
        sortOptions = false, optionListHeading = "%nCommon options:%n")
public class CmdProduce extends AbstractCmd {
    protected static final int MAX_MESSAGES = 1000;
    static final String KEY_VALUE_ENCODING_TYPE_NOT_SET = "";

    final Logger log = Logger.get(CmdProduce.class);

    /** Counterpart of {@link #log} for the static message-body helper. */
    private static final Logger STATIC_LOG = Logger.get(CmdProduce.class);

    @Parameters(description = "TopicName", arity = "1")
    protected String topic;

    @Option(names = ClientApi.OPTION_NAME, description = ClientApi.OPTION_DESCRIPTION)
    protected ClientApi clientApi;

    @Option(names = { "-m", "--messages" },
            description = "Messages to send, either -m or -f must be specified. Specify -m for each message.")
    protected List<String> messages = new ArrayList<>();

    @Option(names = { "-f", "--files" },
            description = "Comma separated file paths to send, either -m or -f must be specified.")
    protected List<String> messageFileNames = new ArrayList<>();

    @Option(names = { "-n", "--num-produce" },
            description = "Number of times to send message(s), the count of messages/files * num-produce "
                    + "should below than " + MAX_MESSAGES + ".")
    protected int numTimesProduce = 1;

    @Option(names = { "-r", "--rate" },
            description = "Rate (in msg/sec) at which to produce,"
                    + " value 0 means to produce messages as fast as possible.")
    protected double publishRate = 0;

    @Option(names = { "-db", "--disable-batching" }, description = "Disable batch sending of messages")
    protected boolean disableBatching = false;

    @Option(names = { "-c",
            "--chunking" }, description = "Should split the message and publish in chunks if message size is "
            + "larger than allowed max size")
    protected boolean chunkingAllowed = false;

    @Option(names = { "-s", "--separator" },
            description = "Character to split messages string on default is comma")
    protected String separator = ",";

    @Option(names = { "-p", "--properties"}, description = "Properties to add, Comma separated "
            + "key=value string, like k1=v1,k2=v2.")
    protected List<String> properties = new ArrayList<>();

    @Option(names = { "-k", "--key"}, description = "Partitioning key to add to each message")
    protected String key;

    @Option(names = { "-vs", "--value-schema"}, description = "Schema type (can be bytes,avro,json,string...)")
    protected String valueSchema = "bytes";

    @Option(names = { "-ekn", "--encryption-key-name" }, description = "The public key name to encrypt payload")
    protected String encKeyName = null;

    @Option(names = { "-ekv",
            "--encryption-key-value" }, description = "The URI of public key to encrypt payload, for example "
            + "file:///path/to/public.key or data:application/x-pem-file;base64,***** (data: URIs require the "
            + "v4 client)")
    protected String encKeyValue = null;

    @ArgGroup(exclusive = false, validate = false, order = 1, heading = ClientApiOptionGroups.V4_HEADING)
    protected V4Options v4 = new V4Options();

    /** Options that only the v4 client supports. */
    public static class V4Options implements ClientApiOptionGroups.V4ClientOptions {
        @Option(names = { "-kvk", "--key-value-key"},
                description = "Value to add as message key in KeyValue schema")
        protected String keyValueKey;

        @Option(names = {"-kvkf", "--key-value-key-file"},
                description = "Path to file containing the value to add as message key in KeyValue schema. "
                        + "JSON and AVRO files are supported.")
        protected String keyValueKeyFile;

        @Option(names = { "-ks", "--key-schema"}, description = "Schema type (can be bytes,avro,json,string...)")
        protected String keySchema = "string";

        @Option(names = { "-kvet", "--key-value-encoding-type"},
                description = "Key Value Encoding Type (it can be separated or inline)")
        protected String keyValueEncodingType = null;

        @Option(names = { "-dr",
                "--disable-replication" }, description = "Disable geo-replication for messages.")
        protected boolean disableReplication = false;
    }

    protected Authentication authentication;
    protected String serviceURL;
    private PulsarClientBuilder clientBuilder;
    private Supplier<ClientBuilder> v4ClientBuilder;

    @Spec
    protected CommandSpec commandSpec;

    public CmdProduce() {
        // Do nothing
    }

    /**
     * Set the V5 client configuration, and the settings shared by both clients.
     */
    public void updateConfig(PulsarClientBuilder newBuilder, Authentication authentication, String serviceURL) {
        this.clientBuilder = newBuilder;
        this.authentication = authentication;
        this.serviceURL = serviceURL;
    }

    /**
     * Set the v4 client configuration. The builder is supplied lazily so that constructing it —
     * which validates the service URL and parses the whole {@code client.conf} — only happens when
     * the v4 client is actually used, not on every {@code pulsar-client} invocation.
     */
    public void updateV4Config(Supplier<ClientBuilder> newBuilder) {
        this.v4ClientBuilder = newBuilder;
    }

    /**
     * Run the producer.
     *
     * @return 0 for success, &lt; 0 otherwise
     */
    public int run() {
        if (this.numTimesProduce <= 0) {
            throw new CommandLine.ParameterException(commandSpec.commandLine(),
                    "Number of times need to be positive number.");
        }

        if (messages.size() > 0) {
            messages = messages.stream().map(str -> str.split(separator)).flatMap(Stream::of).toList();
        }

        if (messages.size() == 0 && messageFileNames.size() == 0) {
            throw new CommandLine.ParameterException(commandSpec.commandLine(),
                    "Please supply message content with either --messages or --files");
        }

        ClientApi resolvedClientApi = resolveClientApi(commandSpec, clientApi, topic, serviceURL);
        // Validate before normalising: "flag absent" (null) and "flag present but empty" are
        // different, and the latter is rejected.
        validateKeyValueEncodingType();
        if (resolvedClientApi == ClientApi.V5) {
            validateV5EncryptionKeyUri(commandSpec, encKeyValue);
        }
        if (v4.keyValueEncodingType == null) {
            v4.keyValueEncodingType = KEY_VALUE_ENCODING_TYPE_NOT_SET;
        }

        int totalMessages = (messages.size() + messageFileNames.size()) * numTimesProduce;
        if (totalMessages > MAX_MESSAGES) {
            String msg = "Attempting to send " + totalMessages + " messages. Please do not send more than "
                    + MAX_MESSAGES + " messages";
            throw new IllegalArgumentException(msg);
        }

        if (isWebSocketUrl(this.serviceURL)) {
            return publishToWebSocket(topic);
        }
        log.info().attr("topic", topic).log("Using the " + resolvedClientApi.displayName());
        if (resolvedClientApi == ClientApi.V5) {
            return new ProduceV5(this, clientBuilder).publish(topic);
        } else {
            return new ProduceV4(this, v4ClientBuilder.get()).publish(topic);
        }
    }

    /**
     * Validate {@code --key-value-encoding-type} on the raw value, which is {@code null} when the
     * flag was not given. An explicitly-supplied value must name a real encoding type, including
     * the empty string: producing plain messages instead would silently give a KeyValue-schema
     * consumer the wrong message shape.
     */
    @VisibleForTesting
    void validateKeyValueEncodingType() {
        String keyValueEncodingType = v4.keyValueEncodingType;
        if (keyValueEncodingType == null) {
            return;
        }
        switch (keyValueEncodingType) {
            case ProduceV4.KEY_VALUE_ENCODING_TYPE_SEPARATED:
            case ProduceV4.KEY_VALUE_ENCODING_TYPE_INLINE:
                break;
            default:
                throw new CommandLine.ParameterException(commandSpec.commandLine(), "--key-value-encoding-type "
                        + keyValueEncodingType + " is not valid, only 'separated' or 'inline'");
        }
    }

    /** The {@code -p/--properties} arguments as a map. */
    protected Map<String, String> propertiesMap() {
        Map<String, String> kvMap = new HashMap<>();
        for (String property : properties) {
            String[] kv = property.split("=");
            kvMap.put(kv[0], kv[1]);
        }
        return kvMap;
    }

    /*
     * Generate a list of message bodies which can be used to build messages
     *
     * @param stringMessages List of strings to send
     *
     * @param messageFileNames List of file names to read and send
     *
     * @return list of message bodies
     */
    static List<byte[]> generateMessageBodies(List<String> stringMessages, List<String> messageFileNames,
                                              org.apache.avro.Schema avroSchema) {
        List<byte[]> messageBodies = new ArrayList<>();

        for (String m : stringMessages) {
            if (avroSchema != null) {
                // JSON TO AVRO — the V5 Schema does not expose the native Avro schema, so the
                // caller passes the parsed Avro definition directly.
                messageBodies.add(jsonToAvro(m, avroSchema));
            } else {
                messageBodies.add(m.getBytes());
            }
        }

        try {
            for (String filename : messageFileNames) {
                byte[] fileBytes = Files.readAllBytes(Paths.get(filename));
                messageBodies.add(fileBytes);
            }
        } catch (Exception e) {
            STATIC_LOG.error().exception(e).log(e.getMessage());
        }

        return messageBodies;
    }

    private static byte[] jsonToAvro(String m, org.apache.avro.Schema avroSchema) {
        try {
            GenericDatumReader<Object> reader = new GenericDatumReader<>(avroSchema);
            JsonDecoder jsonDecoder = DecoderFactory.get().jsonDecoder(avroSchema, m);
            GenericDatumWriter<Object> writer = new GenericDatumWriter<>(avroSchema);
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            Encoder e = EncoderFactory.get().binaryEncoder(out, null);
            Object datum = null;
            while (true) {
                try {
                    datum = reader.read(datum, jsonDecoder);
                } catch (EOFException eofException) {
                    break;
                }
                writer.write(datum, e);
                e.flush();
            }
            return out.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Cannot convert " + m + " to AVRO " + e.getMessage(), e);
        }
    }

    @VisibleForTesting
    public String getWebSocketProduceUri(String topic) {
        String serviceURLWithoutTrailingSlash = serviceURL.substring(0,
                serviceURL.endsWith("/") ? serviceURL.length() - 1 : serviceURL.length());

        TopicName topicName = TopicName.get(topic);
        String wsTopic = String.format("%s/%s/%s/%s", topicName.getDomain(), topicName.getTenant(),
                topicName.getNamespacePortion(), topicName.getLocalName());

        return String.format("%s/ws/v2/producer/%s", serviceURLWithoutTrailingSlash, wsTopic);
    }

    @SuppressWarnings("deprecation")
    private int publishToWebSocket(String topic) {
        int numMessagesSent = 0;
        int returnCode = 0;

        URI produceUri = URI.create(getWebSocketProduceUri(topic));

        HttpClient httpClient = new HttpClient();
        httpClient.setSslContextFactory(new SslContextFactory.Client(true));
        WebSocketClient produceClient = new WebSocketClient(httpClient);
        produceClient.setMaxTextMessageSize(64 * 1024);

        ClientUpgradeRequest produceRequest = new ClientUpgradeRequest(produceUri);
        try {
            if (authentication != null) {
                authentication.start();
                AuthenticationDataProvider authData = authentication.getAuthData(produceUri.getHost());
                if (authData.hasDataForHttp()) {
                    for (Map.Entry<String, String> kv : authData.getHttpHeaders()) {
                        produceRequest.setHeader(kv.getKey(), kv.getValue());
                    }
                }
            }
        } catch (Exception e) {
            log.error().exceptionMessage(e).log("Authentication plugin error");
            return -1;
        }

        CompletableFuture<Void> connected = new CompletableFuture<>();
        ProducerSocket produceSocket = new ProducerSocket(connected);
        try {
            produceClient.start();
        } catch (Exception e) {
            log.error().exception(e).log("Failed to start websocket-client");
            return -1;
        }

        try {
            log.info().attr("uri", produceUri).attr("request", produceRequest)
                    .log("Trying to create websocket session");
            produceClient.connect(produceSocket, produceRequest);
            connected.get();
        } catch (Exception e) {
            log.error().exception(e).log("Failed to create web-socket session");
            return -1;
        }

        try {
            List<byte[]> messageBodies = generateMessageBodies(this.messages, this.messageFileNames, null);
            RateLimiter limiter = (this.publishRate > 0) ? RateLimiter.create(this.publishRate) : null;
            for (int i = 0; i < this.numTimesProduce; i++) {
                int index = i * 10;
                for (byte[] content : messageBodies) {
                    if (limiter != null) {
                        limiter.acquire();
                    }
                    produceSocket.send(index++, content).get(30, TimeUnit.SECONDS);
                    numMessagesSent++;
                }
            }
            produceSocket.close();
        } catch (Exception e) {
            log.error().exception(e).log("Error while producing messages");
            returnCode = -1;
        } finally {
            log.infof("%d messages successfully produced", numMessagesSent);
        }

        try {
            produceClient.stop();
        } catch (Exception e) {
            log.error().exception(e).log("Failed to stop websocket-client");
        }
        try {
            httpClient.stop();
        } catch (Exception e) {
            log.error().exception(e).log("Failed to stop http-client");
        }

        return returnCode;
    }

    /** WebSocket client socket used by the {@code ws://} publishing path. */
    @WebSocket
    @CustomLog
    public static class ProducerSocket {

        private final CountDownLatch closeLatch;
        private Session session;
        private CompletableFuture<Void> connected;
        private volatile CompletableFuture<Void> result;

        public ProducerSocket(CompletableFuture<Void> connected) {
            this.closeLatch = new CountDownLatch(1);
            this.connected = connected;
        }

        public CompletableFuture<Void> send(int index, byte[] content) throws Exception {
            // Publish the future before the frame goes out: onMessage() runs on Jetty's read
            // thread and can see the ack before sendText() returns. Assigning afterwards would
            // overwrite the future that ack completed and leave the caller waiting the full
            // timeout.
            this.result = new CompletableFuture<>();
            this.session.sendText(getTestJsonPayload(index, content), Callback.NOOP);
            return result;
        }

        private static String getTestJsonPayload(int index, byte[] content) throws JsonProcessingException {
            ProducerMessage msg = new ProducerMessage();
            msg.payload = Base64.getEncoder().encodeToString(content);
            msg.key = Integer.toString(index);
            return ObjectMapperFactory.getMapper().writer().writeValueAsString(msg);
        }

        public boolean awaitClose(int duration, TimeUnit unit) throws InterruptedException {
            return this.closeLatch.await(duration, unit);
        }

        @OnWebSocketClose
        public void onClose(int statusCode, String reason) {
            log.info().attr("statusCode", statusCode).attr("reason", reason)
                    .log("Connection closed");
            this.session = null;
            this.closeLatch.countDown();
        }

        @OnWebSocketOpen
        public void onConnect(Session session) {
            log.info().attr("session", session).log("Got connect");
            this.session = session;
            this.connected.complete(null);
        }

        @OnWebSocketMessage
        public synchronized void onMessage(String msg) throws JsonParseException {
            log.info().attr("ack", msg).log("Received ack");
            // A text frame can arrive outside a pending send — before the first send() or after
            // close() — in which case there is no future to complete and the frame is ignored.
            if (this.result != null) {
                this.result.complete(null);
            }
        }

        public Session getSession() {
            return session;
        }

        public void close() {
            // onClose() nulls the session, so a close after the proxy already closed the
            // connection would otherwise NPE.
            if (session != null) {
                session.close();
            }
        }
    }
}
