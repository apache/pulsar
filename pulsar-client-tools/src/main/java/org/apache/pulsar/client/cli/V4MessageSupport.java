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

import static org.apache.pulsar.client.cli.AbstractCmdConsume.interpretByteArray;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.schema.Field;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.common.api.EncryptionContext;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.util.DateFormatter;

/**
 * Message rendering for the v4-client consume and read commands. Keeps the metadata the v4 tool
 * printed and the V5 {@code Message} interface does not expose: the encryption context, the
 * formatted broker publish and event times, the ordering key, the schema version and the index.
 */
final class V4MessageSupport {

    private V4MessageSupport() {
    }

    /**
     * Interprets the message to create a string representation.
     *
     * @param message the message to interpret
     * @param displayHex whether to display BytesMessages in hexdump style, ignored for simple text messages
     * @param printMetadata whether to append the message metadata
     * @return String representation of the message
     */
    static String interpretMessage(Message<?> message, boolean displayHex, boolean printMetadata)
            throws IOException {
        StringBuilder sb = new StringBuilder();

        String properties = Arrays.toString(message.getProperties().entrySet().toArray());

        String data;
        Object value = message.getValue();
        if (value == null) {
            data = "null";
        } else if (value instanceof byte[]) {
            data = interpretByteArray(displayHex, (byte[]) value);
        } else if (value instanceof GenericObject) {
            data = genericObjectToMap((GenericObject) value, displayHex).toString();
        } else if (value instanceof ByteBuffer) {
            data = new String(getBytes((ByteBuffer) value));
        } else {
            data = value.toString();
        }

        sb.append("publishTime:[").append(message.getPublishTime()).append("], ");
        sb.append("eventTime:[").append(message.getEventTime()).append("], ");

        String key = null;
        if (message.hasKey()) {
            key = message.getKey();
        }

        sb.append("key:[").append(key).append("], ");
        if (!properties.isEmpty()) {
            sb.append("properties:").append(properties).append(", ");
        }
        sb.append("content:").append(data);

        if (printMetadata) {
            appendEncryptionContext(sb, message);
            if (message.hasBrokerPublishTime()) {
                sb.append(", ").append("publish-time:").append(DateFormatter.format(message.getPublishTime()));
            }
            sb.append(", ").append("event-time:").append(DateFormatter.format(message.getEventTime()));
            sb.append(", ").append("message-id:").append(message.getMessageId());
            sb.append(", ").append("producer-name:").append(message.getProducerName());
            sb.append(", ").append("sequence-id:").append(message.getSequenceId());
            sb.append(", ").append("replicated-from:").append(message.getReplicatedFrom());
            sb.append(", ").append("redelivery-count:").append(message.getRedeliveryCount());
            sb.append(", ").append("ordering-key:")
                    .append(message.getOrderingKey() != null ? new String(message.getOrderingKey()) : "");
            sb.append(", ").append("schema-version:")
                    .append(message.getSchemaVersion() != null ? new String(message.getSchemaVersion()) : "");
            if (message.hasIndex()) {
                sb.append(", ").append("index:").append(message.getIndex());
            }
        }

        return sb.toString();
    }

    private static void appendEncryptionContext(StringBuilder sb, Message<?> message) {
        if (message.getEncryptionCtx().isEmpty()) {
            return;
        }
        EncryptionContext encContext = message.getEncryptionCtx().get();
        if (encContext.getKeys() == null || encContext.getKeys().isEmpty()) {
            return;
        }
        sb.append(", ");
        sb.append("encryption-keys:").append(", ");
        encContext.getKeys().forEach((keyName, keyInfo) -> {
            String metadata = Arrays.toString(keyInfo.getMetadata().entrySet().toArray());
            sb.append("name:").append(keyName).append(", ").append("key-value:")
                    .append(Base64.getEncoder().encodeToString(keyInfo.getKeyValue())).append(", ")
                    .append("metadata:").append(metadata).append(", ");
        });
        sb.append(", ").append("param:").append(Base64.getEncoder().encodeToString(encContext.getParam()))
                .append(", ").append("algorithm:").append(encContext.getAlgorithm()).append(", ")
                .append("compression-type:").append(encContext.getCompressionType()).append(", ")
                .append("uncompressed-size").append(encContext.getUncompressedMessageSize()).append(", ")
                .append("batch-size")
                .append(encContext.getBatchSize().isPresent() ? encContext.getBatchSize().get() : 1);
    }

    static byte[] getBytes(ByteBuffer buffer) {
        buffer = buffer.duplicate();
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return bytes;
    }

    static Map<String, Object> genericObjectToMap(GenericObject value, boolean displayHex)
            throws IOException {
        switch (value.getSchemaType()) {
            case AVRO:
            case JSON:
            case PROTOBUF_NATIVE:
                return genericRecordToMap((GenericRecord) value, displayHex);
            case KEY_VALUE:
                return keyValueToMap((KeyValue<?, ?>) value.getNativeObject(), displayHex);
            default:
                return primitiveValueToMap(value.getNativeObject(), displayHex);
        }
    }

    static Map<String, Object> keyValueToMap(KeyValue<?, ?> value, boolean displayHex) throws IOException {
        if (value == null) {
            return Map.of("value", "NULL");
        }
        return Map.of("key", primitiveValueToMap(value.getKey(), displayHex),
                "value", primitiveValueToMap(value.getValue(), displayHex));
    }

    static Map<String, Object> primitiveValueToMap(Object value, boolean displayHex) throws IOException {
        if (value == null) {
            return Map.of("value", "NULL");
        }
        if (value instanceof GenericObject) {
            return genericObjectToMap((GenericObject) value, displayHex);
        }
        if (value instanceof byte[]) {
            value = interpretByteArray(displayHex, (byte[]) value);
        }
        return Map.of("value", value.toString(), "type", value.getClass());
    }

    static Map<String, Object> genericRecordToMap(GenericRecord value, boolean displayHex)
            throws IOException {
        Map<String, Object> res = new HashMap<>();
        for (Field f : value.getFields()) {
            Object fieldValue = value.getField(f);
            if (fieldValue instanceof GenericRecord) {
                fieldValue = genericRecordToMap((GenericRecord) fieldValue, displayHex);
            } else if (fieldValue == null) {
                fieldValue = "NULL";
            } else if (fieldValue instanceof byte[]) {
                fieldValue = interpretByteArray(displayHex, (byte[]) fieldValue);
            }
            res.put(f.getName(), fieldValue);
        }
        return res;
    }
}
