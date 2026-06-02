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
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.RateLimiter;
import com.google.gson.JsonParseException;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
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
import java.util.stream.Stream;
import lombok.CustomLog;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.api.v5.MessageBuilder;
import org.apache.pulsar.client.api.v5.Producer;
import org.apache.pulsar.client.api.v5.ProducerBuilder;
import org.apache.pulsar.client.api.v5.PulsarClient;
import org.apache.pulsar.client.api.v5.PulsarClientBuilder;
import org.apache.pulsar.client.api.v5.PulsarClientException;
import org.apache.pulsar.client.api.v5.config.BatchingPolicy;
import org.apache.pulsar.client.api.v5.config.ChunkingPolicy;
import org.apache.pulsar.client.api.v5.config.ProducerEncryptionPolicy;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.apache.pulsar.client.api.v5.schema.SchemaInfo;
import org.apache.pulsar.client.api.v5.schema.SchemaType;
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
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.Spec;

/**
 * pulsar-client produce command implementation.
 */
@Command(description = "Produce messages to a specified topic")
@CustomLog
public class CmdProduce extends AbstractCmd {
    private static final int MAX_MESSAGES = 1000;
    static final String KEY_VALUE_ENCODING_TYPE_NOT_SET = "";

    @Parameters(description = "TopicName", arity = "1")
    private String topic;

    @Option(names = { "-m", "--messages" },
            description = "Messages to send, either -m or -f must be specified. Specify -m for each message.")
    private List<String> messages = new ArrayList<>();

    @Option(names = { "-f", "--files" },
               description = "Comma separated file paths to send, either -m or -f must be specified.")
    private List<String> messageFileNames = new ArrayList<>();

    @Option(names = { "-n", "--num-produce" },
               description = "Number of times to send message(s), the count of messages/files * num-produce "
                       + "should below than " + MAX_MESSAGES + ".")
    private int numTimesProduce = 1;

    @Option(names = { "-r", "--rate" },
               description = "Rate (in msg/sec) at which to produce,"
                       + " value 0 means to produce messages as fast as possible.")
    private double publishRate = 0;

    @Option(names = { "-db", "--disable-batching" }, description = "Disable batch sending of messages")
    private boolean disableBatching = false;

    @Option(names = { "-c",
            "--chunking" }, description = "Should split the message and publish in chunks if message size is "
            + "larger than allowed max size")
    private boolean chunkingAllowed = false;

    @Option(names = { "-s", "--separator" },
               description = "Character to split messages string on default is comma")
    private String separator = ",";

    @Option(names = { "-p", "--properties"}, description = "Properties to add, Comma separated "
            + "key=value string, like k1=v1,k2=v2.")
    private List<String> properties = new ArrayList<>();

    @Option(names = { "-k", "--key"}, description = "Partitioning key to add to each message")
    private String key;
    @Option(names = { "-kvk", "--key-value-key"}, description = "Value to add as message key in KeyValue schema")
    private String keyValueKey;
    @Option(names = {"-kvkf", "--key-value-key-file"},
            description = "Path to file containing the value to add as message key in KeyValue schema. "
            + "JSON and AVRO files are supported.")
    private String keyValueKeyFile;

    @Option(names = { "-vs", "--value-schema"}, description = "Schema type (can be bytes,avro,json,string...)")
    private String valueSchema = "bytes";

    @Option(names = { "-ks", "--key-schema"}, description = "Schema type (can be bytes,avro,json,string...)")
    private String keySchema = "string";

    @Option(names = { "-kvet", "--key-value-encoding-type"},
            description = "Key Value Encoding Type (it can be separated or inline)")
    private String keyValueEncodingType = null;

    @Option(names = { "-ekn", "--encryption-key-name" }, description = "The public key name to encrypt payload")
    private String encKeyName = null;

    @Option(names = { "-ekv",
            "--encryption-key-value" }, description = "The URI of public key to encrypt payload, for example "
                    + "file:///path/to/public.key or data:application/x-pem-file;base64,*****")
    private String encKeyValue = null;

    @Option(names = { "-dr",
            "--disable-replication" }, description = "Disable geo-replication for messages.")
    private boolean disableReplication = false;

    private PulsarClientBuilder clientBuilder;
    private Authentication authentication;
    private String serviceURL;

    public CmdProduce() {
        // Do nothing
    }

    /**
     * Set Pulsar client configuration.
     */
    public void updateConfig(PulsarClientBuilder newBuilder, Authentication authentication, String serviceURL) {
        this.clientBuilder = newBuilder;
        this.authentication = authentication;
        this.serviceURL = serviceURL;
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
            log.error().exception(e).log(e.getMessage());
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

    @Spec
    private CommandSpec commandSpec;

    /**
     * Run the producer.
     *
     * @return 0 for success, < 0 otherwise
     * @throws Exception
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    public int run() throws PulsarClientException {
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

        if (keyValueEncodingType == null) {
            keyValueEncodingType = KEY_VALUE_ENCODING_TYPE_NOT_SET;
        } else if (!KEY_VALUE_ENCODING_TYPE_NOT_SET.equals(keyValueEncodingType)) {
            // KeyValue schemas are not yet supported by the V5-based pulsar-client.
            throw new IllegalArgumentException("KeyValue schemas (--key-value-encoding-type) are not "
                    + "supported by this version of pulsar-client; produce with a plain value schema "
                    + "(-vs bytes|string|avro:<def>|json:<def>) instead.");
        }

        int totalMessages = (messages.size() + messageFileNames.size()) * numTimesProduce;
        if (totalMessages > MAX_MESSAGES) {
            String msg = "Attempting to send " + totalMessages + " messages. Please do not send more than "
                    + MAX_MESSAGES + " messages";
            throw new IllegalArgumentException(msg);
        }

        if (this.serviceURL.startsWith("ws")) {
            return publishToWebSocket(topic);
        } else {
            return publish(topic);
        }
    }

    private int publish(String topic) {
        int numMessagesSent = 0;
        int returnCode = 0;

        if (this.disableReplication) {
            log.warn("--disable-replication has no effect on this version of pulsar-client and is ignored.");
        }

        try (PulsarClient client = clientBuilder.build()) {
            ValueSchema vs = buildValueSchema(this.valueSchema);
            ProducerBuilder<byte[]> producerBuilder = client.newProducer(vs.schema).topic(topic);
            if (this.chunkingAllowed) {
                producerBuilder.chunkingPolicy(ChunkingPolicy.builder().enabled(true).build());
                producerBuilder.batchingPolicy(BatchingPolicy.ofDisabled());
            } else if (this.disableBatching) {
                producerBuilder.batchingPolicy(BatchingPolicy.ofDisabled());
            }
            if (isNotBlank(this.encKeyName) && isNotBlank(this.encKeyValue)) {
                producerBuilder.encryptionPolicy(buildEncryptionPolicy(this.encKeyName, this.encKeyValue));
            }
            try (Producer<byte[]> producer = producerBuilder.create()) {
                List<byte[]> messageBodies = generateMessageBodies(this.messages, this.messageFileNames,
                        vs.avroNative);
                RateLimiter limiter = (this.publishRate > 0) ? RateLimiter.create(this.publishRate) : null;

                Map<String, String> kvMap = new HashMap<>();
                for (String property : properties) {
                    String[] kv = property.split("=");
                    kvMap.put(kv[0], kv[1]);
                }

                for (int i = 0; i < this.numTimesProduce; i++) {
                    for (byte[] content : messageBodies) {
                        if (limiter != null) {
                            limiter.acquire();
                        }

                        MessageBuilder<byte[]> message = producer.newMessage();
                        if (!kvMap.isEmpty()) {
                            message.properties(kvMap);
                        }
                        if (key != null && !key.isEmpty()) {
                            message.key(key);
                        }
                        message.value(content);
                        message.send();
                        numMessagesSent++;
                    }
                }
            }
        } catch (Exception e) {
            log.error().exception(e).log("Error while producing messages");
            returnCode = -1;
        } finally {
            log.infof("%d messages successfully produced", numMessagesSent);
        }

        return returnCode;
    }

    /** A V5 producer schema (always {@code byte[]}) plus, for {@code avro:}, the parsed Avro
     *  definition used to convert JSON input into Avro bytes. */
    record ValueSchema(Schema<byte[]> schema, org.apache.avro.Schema avroNative) {
    }

    static ValueSchema buildValueSchema(String valueSchema) {
        switch (valueSchema) {
            case "bytes":
                return new ValueSchema(Schema.bytes(), null);
            case "string":
                return new ValueSchema(Schema.autoProduceBytesOf(Schema.string()), null);
            default:
                if (valueSchema.startsWith("avro:")) {
                    String def = valueSchema.substring(5);
                    org.apache.avro.Schema avroNative = new org.apache.avro.Schema.Parser().parse(def);
                    Schema<?> generic = Schema.generic(
                            SchemaInfo.of("client", SchemaType.AVRO,
                                    def.getBytes(StandardCharsets.UTF_8), null));
                    return new ValueSchema(Schema.autoProduceBytesOf(generic), avroNative);
                } else if (valueSchema.startsWith("json:")) {
                    String def = valueSchema.substring(5);
                    Schema<?> generic = Schema.generic(
                            SchemaInfo.of("client", SchemaType.JSON,
                                    def.getBytes(StandardCharsets.UTF_8), null));
                    return new ValueSchema(Schema.autoProduceBytesOf(generic), null);
                }
                throw new IllegalArgumentException("Invalid schema type: " + valueSchema);
        }
    }

    private static ProducerEncryptionPolicy buildEncryptionPolicy(String keyName, String keyUri) {
        return ProducerEncryptionPolicy.builder()
                .publicKeyProvider(org.apache.pulsar.client.api.v5.auth.PemFileKeyProvider.builder()
                        .publicKey(keyName, fileUriToPath(keyUri))
                        .build())
                .keyName(keyName)
                .build();
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
            this.session.sendText(getTestJsonPayload(index, content), Callback.NOOP);
            this.result = new CompletableFuture<>();
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
            if (this.result != null) {
                this.result.complete(null);
            }
        }

        public Session getSession() {
            return this.session;
        }

        public void close() {
            this.session.close();
        }

    }
}
