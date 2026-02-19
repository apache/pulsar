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
package org.apache.pulsar.broker.admin;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.netty.buffer.Unpooled;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import lombok.Getter;
import org.apache.pulsar.common.api.proto.MessageIdData;

/**
 * Server-side request body for skipping messages by message IDs with support for multiple formats.
 */
@Getter
@JsonDeserialize(using = SkipMessageIdsRequest.Deserializer.class)
public class SkipMessageIdsRequest {
    private final List<MessageIdItem> items = new ArrayList<>();

    public SkipMessageIdsRequest() { }

    private void addItem(long ledgerId, long entryId, Integer batchIndex) {
        items.add(new MessageIdItem(ledgerId, entryId, batchIndex));
    }

    public record MessageIdItem(long ledgerId, long entryId, Integer batchIndex) {
        public long getLedgerId() {
            return ledgerId;
        }

        public long getEntryId() {
            return entryId;
        }

        public Integer getBatchIndex() {
            return batchIndex;
        }
    }

    public static class Deserializer extends JsonDeserializer<SkipMessageIdsRequest> {
        @Override
        public SkipMessageIdsRequest deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
            ObjectCodec codec = p.getCodec();
            JsonNode node = codec.readTree(p);
            SkipMessageIdsRequest r = new SkipMessageIdsRequest();

            if (node == null || node.isNull()) {
                throw new IOException("Invalid skipByMessageIds payload: empty body");
            }

            if (node.isArray()) {
                // Treat as default byteArray list
                ArrayNode arr = (ArrayNode) node;
                for (JsonNode idNode : arr) {
                    if (idNode != null && !idNode.isNull()) {
                        appendFromBase64(idNode.asText(), r);
                    }
                }
                return r;
            }

            if (node.isObject()) {
                ObjectNode obj = (ObjectNode) node;
                JsonNode typeNode = obj.get("type");
                String type = typeNode != null && !typeNode.isNull() ? typeNode.asText() : null;
                JsonNode messageIdsNode = obj.get("messageIds");

                if (messageIdsNode != null) {
                    if (messageIdsNode.isArray()) {
                        ArrayNode arr = (ArrayNode) messageIdsNode;
                        if (type == null || type.isEmpty() || "byteArray".equalsIgnoreCase(type)) {
                            for (JsonNode idNode : arr) {
                                if (idNode != null && !idNode.isNull()) {
                                    appendFromBase64(idNode.asText(), r);
                                }
                            }
                        } else if ("messageId".equalsIgnoreCase(type)) {
                            for (JsonNode idObj : arr) {
                                if (idObj == null || idObj.isNull()) {
                                    continue;
                                }
                                if (!idObj.isObject()) {
                                    throw new IOException("Invalid skipByMessageIds payload:"
                                            + " messageIds elements must be objects");
                                }
                                long ledgerId = requiredLong(idObj.get("ledgerId"), "ledgerId");
                                long entryId = requiredLong(idObj.get("entryId"), "entryId");
                                Integer batchIndex = optionalNonNegativeInt(idObj.get("batchIndex"), "batchIndex");
                                r.addItem(ledgerId, entryId, batchIndex);
                            }
                        } else {
                            // Unknown type with array payload => reject
                            throw new IOException("Invalid skipByMessageIds payload: unsupported type for array");
                        }
                        return r;
                    } else if (messageIdsNode.isObject()) {
                        throw new IOException("Invalid skipByMessageIds payload: messageIds must be an array");
                    } else {
                        throw new IOException("Invalid skipByMessageIds payload: unsupported messageIds type");
                    }
                }

                // No messageIds field => reject
                throw new IOException("Invalid skipByMessageIds payload: missing messageIds");
            }

            throw new IOException("Invalid skipByMessageIds payload: unsupported top-level JSON");
        }

        private static long requiredLong(JsonNode node, String fieldName) throws IOException {
            if (node == null || node.isNull()) {
                throw new IOException("Invalid skipByMessageIds payload: missing " + fieldName);
            }
            try {
                if (node.isNumber()) {
                    return node.longValue();
                }
                if (node.isTextual()) {
                    return Long.parseLong(node.asText());
                }
            } catch (Exception e) {
                throw new IOException("Invalid skipByMessageIds payload: invalid " + fieldName, e);
            }
            throw new IOException("Invalid skipByMessageIds payload: invalid " + fieldName);
        }

        private static Integer optionalNonNegativeInt(JsonNode node, String fieldName) throws IOException {
            if (node == null || node.isNull()) {
                return null;
            }
            try {
                int v;
                if (node.isNumber()) {
                    v = node.intValue();
                } else if (node.isTextual()) {
                    v = Integer.parseInt(node.asText());
                } else {
                    throw new IOException("Invalid skipByMessageIds payload: invalid " + fieldName);
                }
                return v >= 0 ? v : null;
            } catch (NumberFormatException e) {
                throw new IOException("Invalid skipByMessageIds payload: invalid " + fieldName, e);
            }
        }

        private static void appendFromBase64(String base64, SkipMessageIdsRequest r)
                throws IOException {
            if (base64 == null) {
                return;
            }
            byte[] data;
            try {
                data = Base64.getDecoder().decode(base64);
            } catch (IllegalArgumentException e) {
                throw new IOException("Invalid skipByMessageIds payload: invalid base64 messageId", e);
            }
            if (data.length == 0) {
                throw new IOException("Invalid skipByMessageIds payload: invalid base64 messageId (empty)");
            }
            MessageIdData idData = new MessageIdData();
            try {
                idData.parseFrom(Unpooled.wrappedBuffer(data, 0, data.length), data.length);
            } catch (Exception e) {
                throw new IOException(e);
            }
            long ledgerId = idData.getLedgerId();
            long entryId = idData.getEntryId();
            int batchIndex = idData.hasBatchIndex() ? idData.getBatchIndex() : -1;
            if (batchIndex >= 0) {
                r.addItem(ledgerId, entryId, batchIndex);
            } else {
                r.addItem(ledgerId, entryId, null);
            }
        }
    }
}
