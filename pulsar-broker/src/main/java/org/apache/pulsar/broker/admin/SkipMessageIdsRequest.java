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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import org.apache.pulsar.common.api.proto.MessageIdData;

/**
 * Server-side request body for skipping messages by message IDs with support for multiple formats.
 * Normalizes to the legacy map used by Subscription#skipMessages(Map<String,String>).
 */
@Getter
@JsonDeserialize(using = SkipMessageIdsRequest.Deserializer.class)
public class SkipMessageIdsRequest {
    private Map<String, String> legacyMap;
    private final List<MessageIdItem> items = new ArrayList<>();

    public SkipMessageIdsRequest() {
        this.legacyMap = new LinkedHashMap<>();
    }

    private void addItem(long ledgerId, long entryId, Integer batchIndex) {
        items.add(new MessageIdItem(ledgerId, entryId, batchIndex));
    }

    @Getter
    public static class MessageIdItem {
        private final long ledgerId;
        private final long entryId;
        // nullable
        private final Integer batchIndex;

        public MessageIdItem(long ledgerId, long entryId, Integer batchIndex) {
            this.ledgerId = ledgerId;
            this.entryId = entryId;
            this.batchIndex = batchIndex;
        }
    }

    void setLegacyMap(Map<String, String> legacyMap) {
        this.legacyMap = legacyMap;
    }

    public static class Deserializer extends JsonDeserializer<SkipMessageIdsRequest> {
        @Override
        public SkipMessageIdsRequest deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
            ObjectCodec codec = p.getCodec();
            JsonNode node = codec.readTree(p);

            Map<String, String> result = new LinkedHashMap<>();
            SkipMessageIdsRequest r = new SkipMessageIdsRequest();

            if (node == null || node.isNull()) {
                throw new IOException("Invalid skipByMessageIds payload: empty body");
            }

            if (node.isArray()) {
                // Treat as default byteArray list
                ArrayNode arr = (ArrayNode) node;
                for (JsonNode idNode : arr) {
                    if (idNode != null && !idNode.isNull()) {
                        appendFromBase64(idNode.asText(), result, r);
                    }
                }
                r.setLegacyMap(result);
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
                                    appendFromBase64(idNode.asText(), result, r);
                                }
                            }
                        } else if ("messageId".equalsIgnoreCase(type)) {
                            for (JsonNode idObj : arr) {
                                if (idObj == null || idObj.isNull()) {
                                    continue;
                                }
                                long ledgerId = optLong(idObj.get("ledgerId"));
                                long entryId = optLong(idObj.get("entryId"));
                                int batchIndex = optInt(idObj.get("batchIndex"), -1);
                                if (batchIndex >= 0) {
                                    result.put(Long.toString(ledgerId), entryId + ":" + batchIndex);
                                    r.addItem(ledgerId, entryId, batchIndex);
                                } else {
                                    result.put(Long.toString(ledgerId), Long.toString(entryId));
                                    r.addItem(ledgerId, entryId, null);
                                }
                            }
                        } else {
                            // Unknown type with array payload => reject
                            throw new IOException("Invalid skipByMessageIds payload: unsupported type for array");
                        }
                        r.setLegacyMap(result);
                        return r;
                    } else if (messageIdsNode.isObject()) {
                        if ("map_of_ledgerId_entryId".equalsIgnoreCase(type)) {
                            ObjectNode midMap = (ObjectNode) messageIdsNode;
                            Iterator<Map.Entry<String, JsonNode>> fields = midMap.fields();
                            while (fields.hasNext()) {
                                Map.Entry<String, JsonNode> e = fields.next();
                                String key = e.getKey();
                                String valueStr = asScalarText(e.getValue());
                                result.put(key, valueStr);
                                long ledgerId = Long.parseLong(key);
                                long entryId = Long.parseLong(valueStr);
                                r.addItem(ledgerId, entryId, null);
                            }
                            r.setLegacyMap(result);
                            return r;
                        }
                        throw new IOException("Invalid skipByMessageIds payload:"
                                + " object messageIds requires type=map_of_ledgerId_entryId");
                    } else {
                        throw new IOException("Invalid skipByMessageIds payload: unsupported messageIds type");
                    }
                }

                // No messageIds field => reject legacy map form
                throw new IOException("Invalid skipByMessageIds payload: missing messageIds");
            }

            throw new IOException("Invalid skipByMessageIds payload: unsupported top-level JSON");
        }

        private static String asScalarText(JsonNode node) {
            if (node == null || node.isNull()) {
                return null;
            }
            if (node.isTextual()) {
                return node.asText();
            }
            if (node.isNumber()) {
                return node.asText();
            }
            return node.toString();
        }

        private static long optLong(JsonNode node) {
            if (node == null || node.isNull()) {
                return 0L;
            }
            try {
                return node.asLong();
            } catch (Exception e) {
                return 0L;
            }
        }

        private static int optInt(JsonNode node, int def) {
            if (node == null || node.isNull()) {
                return def;
            }
            try {
                return node.asInt();
            } catch (Exception e) {
                return def;
            }
        }

        private static void appendFromBase64(String base64, Map<String, String> result, SkipMessageIdsRequest r)
                throws IOException {
            if (base64 == null) {
                return;
            }
            byte[] data = Base64.getDecoder().decode(base64);
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
                result.put(Long.toString(ledgerId), entryId + ":" + batchIndex);
                r.addItem(ledgerId, entryId, batchIndex);
            } else {
                result.put(Long.toString(ledgerId), Long.toString(entryId));
                r.addItem(ledgerId, entryId, null);
            }
        }
    }
}
