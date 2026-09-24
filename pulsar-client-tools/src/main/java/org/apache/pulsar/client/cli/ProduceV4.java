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
import static org.apache.pulsar.client.cli.CmdProduce.KEY_VALUE_ENCODING_TYPE_NOT_SET;
import com.google.common.util.concurrent.RateLimiter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.api.schema.KeyValueSchema;
import org.apache.pulsar.client.impl.schema.SchemaInfoImpl;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.SchemaType;

/**
 * Publishes the messages of a {@link CmdProduce} invocation with the v4 ({@code pulsar-client-original})
 * client, including the v4-only capabilities: KeyValue schemas ({@code --key-value-encoding-type} with
 * {@code --key-value-key} / {@code --key-value-key-file} and {@code --key-schema}),
 * {@code --disable-replication}, and {@code data:} URI encryption keys.
 */
final class ProduceV4 {

    static final String KEY_VALUE_ENCODING_TYPE_SEPARATED = "separated";
    static final String KEY_VALUE_ENCODING_TYPE_INLINE = "inline";

    private final CmdProduce cmd;
    private final CmdProduce.V4Options v4;
    private final ClientBuilder clientBuilder;

    ProduceV4(CmdProduce cmd, ClientBuilder clientBuilder) {
        this.cmd = cmd;
        this.v4 = cmd.v4;
        this.clientBuilder = clientBuilder;
    }

    /**
     * Publish the messages.
     *
     * @return 0 for success, &lt; 0 otherwise
     */
    @SuppressWarnings({"unchecked", "rawtypes", "deprecation"})
    int publish(String topic) {
        int numMessagesSent = 0;
        int returnCode = 0;

        try (PulsarClient client = clientBuilder.build()) {
            Schema<?> schema = buildSchema(v4.keySchema, cmd.valueSchema, v4.keyValueEncodingType);
            ProducerBuilder<?> producerBuilder = client.newProducer(schema).topic(topic);
            if (cmd.chunkingAllowed) {
                producerBuilder.enableChunking(true);
                producerBuilder.enableBatching(false);
            } else if (cmd.disableBatching) {
                producerBuilder.enableBatching(false);
            }
            if (isNotBlank(cmd.encKeyName) && isNotBlank(cmd.encKeyValue)) {
                producerBuilder.addEncryptionKey(cmd.encKeyName);
                producerBuilder.defaultCryptoKeyReader(cmd.encKeyValue);
            }
            try (Producer<?> producer = producerBuilder.create()) {
                Schema<?> schemaForPayload = schema.getSchemaInfo().getType() == SchemaType.KEY_VALUE
                        ? ((KeyValueSchema) schema).getValueSchema() : schema;
                List<byte[]> messageBodies = CmdProduce.generateMessageBodies(cmd.messages, cmd.messageFileNames,
                        nativeAvroSchemaOrNull(schemaForPayload));
                RateLimiter limiter = (cmd.publishRate > 0) ? RateLimiter.create(cmd.publishRate) : null;

                Map<String, String> kvMap = cmd.propertiesMap();
                final byte[] keyValueKeyBytes = resolveKeyValueKeyBytes();

                for (int i = 0; i < cmd.numTimesProduce; i++) {
                    for (byte[] content : messageBodies) {
                        if (limiter != null) {
                            limiter.acquire();
                        }

                        TypedMessageBuilder<Object> message =
                                (TypedMessageBuilder<Object>) producer.newMessage();

                        if (!kvMap.isEmpty()) {
                            message.properties(kvMap);
                        }

                        if (KEY_VALUE_ENCODING_TYPE_NOT_SET.equals(v4.keyValueEncodingType)) {
                            if (cmd.key != null && !cmd.key.isEmpty()) {
                                message.key(cmd.key);
                            }
                            message.value(content);
                        } else {
                            message.value(new KeyValue<>(keyValueKeyBytes, content));
                        }

                        if (v4.disableReplication) {
                            message.disableReplication();
                        }

                        message.send();
                        numMessagesSent++;
                    }
                }
            }
        } catch (Exception e) {
            cmd.log.error().exception(e).log("Error while producing messages");
            returnCode = -1;
        } finally {
            cmd.log.infof("%d messages successfully produced", numMessagesSent);
        }

        return returnCode;
    }

    /**
     * The key bytes of a KeyValue message, from {@code --key-value-key},
     * {@code --key-value-key-file} or, failing both, {@code --key}.
     */
    private byte[] resolveKeyValueKeyBytes() throws Exception {
        if (v4.keyValueKey != null) {
            requireKeyValueEncodingType("--key-value-key");
            return v4.keyValueKey.getBytes(StandardCharsets.UTF_8);
        }
        if (v4.keyValueKeyFile != null) {
            requireKeyValueEncodingType("--key-value-key-file");
            return Files.readAllBytes(Paths.get(v4.keyValueKeyFile));
        }
        if (cmd.key != null) {
            return cmd.key.getBytes(StandardCharsets.UTF_8);
        }
        return null;
    }

    private void requireKeyValueEncodingType(String flag) {
        if (KEY_VALUE_ENCODING_TYPE_NOT_SET.equals(v4.keyValueEncodingType)) {
            throw new IllegalArgumentException(
                    "Key value encoding type must be set when using " + flag);
        }
    }

    /**
     * The parsed Avro definition behind an {@code avro:} schema, or null for any other type. An
     * Avro schema that exposes no native definition is an error rather than a fallback: without it
     * the JSON text would be shipped verbatim as the payload under an Avro schema.
     */
    private static org.apache.avro.Schema nativeAvroSchemaOrNull(Schema<?> schema) {
        if (schema.getSchemaInfo().getType() != SchemaType.AVRO) {
            return null;
        }
        return (org.apache.avro.Schema) schema.getNativeSchema()
                .orElseThrow(() -> new IllegalStateException(
                        "No native Avro definition available for schema '"
                                + schema.getSchemaInfo().getName()
                                + "', so the message cannot be encoded from JSON"));
    }

    static Schema<?> buildSchema(String keySchema, String schema, String keyValueEncodingType) {
        if (KEY_VALUE_ENCODING_TYPE_NOT_SET.equals(keyValueEncodingType)) {
            return buildComponentSchema(schema);
        }
        switch (keyValueEncodingType) {
            case KEY_VALUE_ENCODING_TYPE_SEPARATED:
                return Schema.KeyValue(buildComponentSchema(keySchema), buildComponentSchema(schema),
                        KeyValueEncodingType.SEPARATED);
            case KEY_VALUE_ENCODING_TYPE_INLINE:
                return Schema.KeyValue(buildComponentSchema(keySchema), buildComponentSchema(schema),
                        KeyValueEncodingType.INLINE);
            default:
                throw new IllegalArgumentException("Invalid KeyValueEncodingType "
                        + keyValueEncodingType + ", only: 'none','separated' and 'inline");
        }
    }

    private static Schema<?> buildComponentSchema(String schema) {
        Schema<?> base;
        switch (schema) {
            case "string":
                base = Schema.STRING;
                break;
            case "bytes":
                // no need for wrappers
                return Schema.BYTES;
            default:
                if (schema.startsWith("avro:")) {
                    base = buildGenericSchema(SchemaType.AVRO, schema.substring(5));
                } else if (schema.startsWith("json:")) {
                    base = buildGenericSchema(SchemaType.JSON, schema.substring(5));
                } else {
                    throw new IllegalArgumentException("Invalid schema type: " + schema);
                }
        }
        return Schema.AUTO_PRODUCE_BYTES(base);
    }

    private static Schema<?> buildGenericSchema(SchemaType type, String definition) {
        return Schema.generic(SchemaInfoImpl
                .builder()
                .schema(definition.getBytes(StandardCharsets.UTF_8))
                .name("client")
                .properties(new HashMap<>())
                .type(type)
                .build());
    }
}
