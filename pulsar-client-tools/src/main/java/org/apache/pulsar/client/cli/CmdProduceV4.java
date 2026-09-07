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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.pulsar.client.api.Authentication;
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
import picocli.CommandLine.Command;

/**
 * The {@code produce} command driven by the v4 ({@code pulsar-client-original}) client.
 *
 * <p>This is the counterpart of {@link CmdProduce}: the CLI options, the message bodies and the
 * WebSocket path come from {@link AbstractCmdProduce}, and only the client bindings differ. It
 * restores the v4-only producer capabilities: KeyValue schemas ({@code --key-value-encoding-type}
 * with {@code --key-value-key} / {@code --key-value-key-file} and {@code --key-schema}),
 * {@code --disable-replication}, and {@code data:} URI encryption keys.
 */
@Command(name = "produce-v4", description = "Produce messages to a specified topic using the v4 client")
public class CmdProduceV4 extends AbstractCmdProduce {

    private static final String KEY_VALUE_ENCODING_TYPE_SEPARATED = "separated";
    private static final String KEY_VALUE_ENCODING_TYPE_INLINE = "inline";

    private Supplier<ClientBuilder> clientBuilder;

    public CmdProduceV4() {
        // Do nothing
    }

    /**
     * Set Pulsar client configuration. The builder is supplied lazily so that constructing it —
     * which validates the service URL and parses the whole {@code client.conf} — only happens when
     * this command actually runs, not on every {@code pulsar-client} invocation.
     */
    public void updateConfig(Supplier<ClientBuilder> newBuilder, Authentication authentication, String serviceURL) {
        this.clientBuilder = newBuilder;
        updateSharedConfig(authentication, serviceURL);
    }

    @Override
    protected void validateSchemaOptions() {
        // An absent flag is fine; an explicitly-supplied value must name a real encoding type —
        // including the empty string, which the pre-migration v4 command also rejected.
        if (keyValueEncodingType == null) {
            return;
        }
        switch (keyValueEncodingType) {
            case KEY_VALUE_ENCODING_TYPE_SEPARATED:
            case KEY_VALUE_ENCODING_TYPE_INLINE:
                break;
            default:
                throw new IllegalArgumentException("--key-value-encoding-type "
                        + keyValueEncodingType + " is not valid, only 'separated' or 'inline'");
        }
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes", "deprecation"})
    protected int publish(String topic) {
        int numMessagesSent = 0;
        int returnCode = 0;

        try (PulsarClient client = clientBuilder.get().build()) {
            Schema<?> schema = buildSchema(this.keySchema, this.valueSchema, this.keyValueEncodingType);
            ProducerBuilder<?> producerBuilder = client.newProducer(schema).topic(topic);
            if (this.chunkingAllowed) {
                producerBuilder.enableChunking(true);
                producerBuilder.enableBatching(false);
            } else if (this.disableBatching) {
                producerBuilder.enableBatching(false);
            }
            if (isNotBlank(this.encKeyName) && isNotBlank(this.encKeyValue)) {
                producerBuilder.addEncryptionKey(this.encKeyName);
                producerBuilder.defaultCryptoKeyReader(this.encKeyValue);
            }
            try (Producer<?> producer = producerBuilder.create()) {
                Schema<?> schemaForPayload = schema.getSchemaInfo().getType() == SchemaType.KEY_VALUE
                        ? ((KeyValueSchema) schema).getValueSchema() : schema;
                List<byte[]> messageBodies = generateMessageBodies(this.messages, this.messageFileNames,
                        nativeAvroSchemaOrNull(schemaForPayload));
                RateLimiter limiter = (this.publishRate > 0) ? RateLimiter.create(this.publishRate) : null;

                Map<String, String> kvMap = propertiesMap();
                final byte[] keyValueKeyBytes = resolveKeyValueKeyBytes();

                for (int i = 0; i < this.numTimesProduce; i++) {
                    for (byte[] content : messageBodies) {
                        if (limiter != null) {
                            limiter.acquire();
                        }

                        TypedMessageBuilder<Object> message =
                                (TypedMessageBuilder<Object>) producer.newMessage();

                        if (!kvMap.isEmpty()) {
                            message.properties(kvMap);
                        }

                        if (KEY_VALUE_ENCODING_TYPE_NOT_SET.equals(keyValueEncodingType)) {
                            if (key != null && !key.isEmpty()) {
                                message.key(key);
                            }
                            message.value(content);
                        } else {
                            message.value(new KeyValue<>(keyValueKeyBytes, content));
                        }

                        if (disableReplication) {
                            message.disableReplication();
                        }

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

    /**
     * The key bytes of a KeyValue message, from {@code --key-value-key},
     * {@code --key-value-key-file} or, failing both, {@code --key}.
     */
    private byte[] resolveKeyValueKeyBytes() throws Exception {
        if (this.keyValueKey != null) {
            requireKeyValueEncodingType("--key-value-key");
            return this.keyValueKey.getBytes(StandardCharsets.UTF_8);
        }
        if (this.keyValueKeyFile != null) {
            requireKeyValueEncodingType("--key-value-key-file");
            return Files.readAllBytes(Paths.get(this.keyValueKeyFile));
        }
        if (this.key != null) {
            return this.key.getBytes(StandardCharsets.UTF_8);
        }
        return null;
    }

    private void requireKeyValueEncodingType(String flag) {
        if (KEY_VALUE_ENCODING_TYPE_NOT_SET.equals(keyValueEncodingType)) {
            throw new IllegalArgumentException(
                    "Key value encoding type must be set when using " + flag);
        }
    }

    /** The parsed Avro definition behind an {@code avro:} schema, or null for any other type. */
    private static org.apache.avro.Schema nativeAvroSchemaOrNull(Schema<?> schema) {
        if (schema.getSchemaInfo().getType() != SchemaType.AVRO) {
            return null;
        }
        return (org.apache.avro.Schema) schema.getNativeSchema().orElse(null);
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
