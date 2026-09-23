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
import java.util.List;
import java.util.Map;
import org.apache.pulsar.client.api.v5.MessageBuilder;
import org.apache.pulsar.client.api.v5.Producer;
import org.apache.pulsar.client.api.v5.ProducerBuilder;
import org.apache.pulsar.client.api.v5.PulsarClient;
import org.apache.pulsar.client.api.v5.PulsarClientBuilder;
import org.apache.pulsar.client.api.v5.auth.PemFileKeyProvider;
import org.apache.pulsar.client.api.v5.config.BatchingPolicy;
import org.apache.pulsar.client.api.v5.config.ChunkingPolicy;
import org.apache.pulsar.client.api.v5.config.ProducerEncryptionPolicy;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.apache.pulsar.client.api.v5.schema.SchemaInfo;
import org.apache.pulsar.client.api.v5.schema.SchemaType;

/**
 * Publishes the messages of a {@link CmdProduce} invocation with the V5 client.
 */
final class ProduceV5 {

    private final CmdProduce cmd;
    private final PulsarClientBuilder clientBuilder;

    ProduceV5(CmdProduce cmd, PulsarClientBuilder clientBuilder) {
        this.cmd = cmd;
        this.clientBuilder = clientBuilder;
    }

    /**
     * Publish the messages.
     *
     * @return 0 for success, &lt; 0 otherwise
     */
    int publish(String topic) {
        int numMessagesSent = 0;
        int returnCode = 0;

        try (PulsarClient client = clientBuilder.build()) {
            ValueSchema vs = buildValueSchema(cmd.valueSchema);
            ProducerBuilder<byte[]> producerBuilder = client.newProducer(vs.schema).topic(topic);
            if (cmd.chunkingAllowed) {
                producerBuilder.chunkingPolicy(ChunkingPolicy.builder().enabled(true).build());
                producerBuilder.batchingPolicy(BatchingPolicy.ofDisabled());
            } else if (cmd.disableBatching) {
                producerBuilder.batchingPolicy(BatchingPolicy.ofDisabled());
            }
            if (isNotBlank(cmd.encKeyName) && isNotBlank(cmd.encKeyValue)) {
                producerBuilder.encryptionPolicy(buildEncryptionPolicy(cmd.encKeyName, cmd.encKeyValue));
            }
            try (Producer<byte[]> producer = producerBuilder.create()) {
                List<byte[]> messageBodies = CmdProduce.generateMessageBodies(cmd.messages, cmd.messageFileNames,
                        vs.avroNative);
                RateLimiter limiter = (cmd.publishRate > 0) ? RateLimiter.create(cmd.publishRate) : null;

                Map<String, String> kvMap = cmd.propertiesMap();

                for (int i = 0; i < cmd.numTimesProduce; i++) {
                    for (byte[] content : messageBodies) {
                        if (limiter != null) {
                            limiter.acquire();
                        }

                        MessageBuilder<byte[]> message = producer.newMessage();
                        if (!kvMap.isEmpty()) {
                            message.properties(kvMap);
                        }
                        if (cmd.key != null && !cmd.key.isEmpty()) {
                            message.key(cmd.key);
                        }
                        message.value(content);
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
                .publicKeyProvider(PemFileKeyProvider.builder()
                        .publicKey(keyName, AbstractCmd.fileUriToPath(keyUri))
                        .build())
                .keyName(keyName)
                .build();
    }
}
