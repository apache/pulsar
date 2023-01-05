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
package org.apache.pulsar.io.elasticsearch;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.KeyValueSchema;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.core.annotations.Connector;
import org.apache.pulsar.io.core.annotations.IOType;

@Connector(
        name = "elastic_search",
        type = IOType.SINK,
        help = "A sink connector that sends pulsar messages to elastic search",
        configClass = ElasticSearchConfig.class
)
@Slf4j
public class ElasticSearchSink implements Sink<GenericObject> {

    private ElasticSearchConfig elasticSearchConfig;
    private ElasticSearchClient elasticsearchClient;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private ObjectMapper sortedObjectMapper;
    private List<String> primaryFields = null;
    private final Pattern nonPrintableCharactersPattern = Pattern.compile("[\\p{C}]");
    private final Base64.Encoder base64Encoder = Base64.getEncoder().withoutPadding();

    @Override
    public void open(Map<String, Object> config, SinkContext sinkContext) throws Exception {
        elasticSearchConfig = ElasticSearchConfig.load(config, sinkContext);
        elasticSearchConfig.validate();
        elasticsearchClient = new ElasticSearchClient(elasticSearchConfig);
        if (!Strings.isNullOrEmpty(elasticSearchConfig.getPrimaryFields())) {
            primaryFields = Arrays.asList(elasticSearchConfig.getPrimaryFields().split(","));
        }
        if (elasticSearchConfig.isCanonicalKeyFields()) {
            sortedObjectMapper = JsonMapper
                    .builder()
                            .configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true)
                    .nodeFactory(new JsonNodeFactory() {
                        @Override
                        public ObjectNode objectNode() {
                            return new ObjectNode(this, new TreeMap<String, JsonNode>());
                        }

                    })
                    .build();
        }
    }

    @Override
    public void close() {
        if (elasticsearchClient != null) {
            elasticsearchClient.close();
            elasticsearchClient = null;
        }
    }

    @VisibleForTesting
    void setElasticsearchClient(ElasticSearchClient elasticsearchClient) {
        this.elasticsearchClient = elasticsearchClient;
    }

    @Override
    public void write(Record<GenericObject> record) throws Exception {
        if (!elasticsearchClient.isFailed()) {
            Pair<String, String> idAndDoc = extractIdAndDocument(record);
            try {
                if (log.isDebugEnabled()) {
                    log.debug("index doc {} {}", idAndDoc.getLeft(), idAndDoc.getRight());
                }
                if (idAndDoc.getRight() == null) {
                    switch (elasticSearchConfig.getNullValueAction()) {
                        case DELETE:
                            if (idAndDoc.getLeft() != null) {
                                if (elasticSearchConfig.isBulkEnabled()) {
                                    elasticsearchClient.bulkDelete(record, idAndDoc.getLeft());
                                } else {
                                    elasticsearchClient.deleteDocument(record, idAndDoc.getLeft());
                                }
                            }
                            break;
                        case IGNORE:
                            break;
                        case FAIL:
                            elasticsearchClient.failed(
                                    new PulsarClientException.InvalidMessageException("Unexpected null message value"));
                            throw elasticsearchClient.irrecoverableError.get();
                    }
                } else {
                    if (elasticSearchConfig.isBulkEnabled()) {
                        elasticsearchClient.bulkIndex(record, idAndDoc);
                    } else {
                        elasticsearchClient.indexDocument(record, idAndDoc);
                    }
                }
            } catch (JsonProcessingException jsonProcessingException) {
                switch (elasticSearchConfig.getMalformedDocAction()) {
                    case IGNORE:
                        break;
                    case WARN:
                        log.warn("Ignoring malformed document messageId={}",
                                record.getMessage().map(Message::getMessageId).orElse(null),
                                jsonProcessingException);
                        elasticsearchClient.failed(jsonProcessingException);
                        throw jsonProcessingException;
                    case FAIL:
                        log.error("Malformed document messageId={}",
                                record.getMessage().map(Message::getMessageId).orElse(null),
                                jsonProcessingException);
                        elasticsearchClient.failed(jsonProcessingException);
                        throw jsonProcessingException;
                }
            } catch (Exception e) {
                log.error("write error for {} {}:", idAndDoc.getLeft(), idAndDoc.getRight(), e);
                throw e;
            }
        } else {
            throw new IllegalStateException("Elasticsearch client is in FAILED status");
        }
    }

    @VisibleForTesting
    ElasticSearchClient getElasticsearchClient() {
        return this.elasticsearchClient;
    }

    /**
     * Extract ES _id and _source using the Schema if available.
     *
     * @param record
     * @return A pair for _id and _source
     */
    public Pair<String, String> extractIdAndDocument(Record<GenericObject> record) throws JsonProcessingException {
        if (elasticSearchConfig.isSchemaEnable()) {
            Object key = null;
            GenericObject value = null;
            Schema<?> keySchema = null;
            Schema<?> valueSchema = null;
            if (record.getSchema() != null && record.getSchema() instanceof KeyValueSchema) {
                KeyValueSchema<GenericObject, GenericObject> keyValueSchema = (KeyValueSchema) record.getSchema();
                keySchema = keyValueSchema.getKeySchema();
                valueSchema = keyValueSchema.getValueSchema();
                KeyValue<GenericObject, GenericObject> keyValue =
                        (KeyValue<GenericObject, GenericObject>) record.getValue().getNativeObject();
                key = keyValue.getKey();
                value = keyValue.getValue();
            } else {
                key = record.getKey().orElse(null);
                valueSchema = record.getSchema();
                value = record.getValue();
            }

            String id = null;
            if (!elasticSearchConfig.isKeyIgnore() && key != null && keySchema != null) {
                id = stringifyKey(keySchema, key);
            }

            String doc = null;
            if (value != null) {
                if (valueSchema != null) {
                    if (elasticSearchConfig.isCopyKeyFields()
                            && (keySchema.getSchemaInfo().getType().equals(SchemaType.AVRO)
                            || keySchema.getSchemaInfo().getType().equals(SchemaType.JSON))) {
                        JsonNode keyNode = extractJsonNode(keySchema, key);
                        JsonNode valueNode = extractJsonNode(valueSchema, value);
                        doc = stringify(JsonConverter.topLevelMerge(keyNode, valueNode));
                    } else {
                        doc = stringifyValue(valueSchema, value);
                    }
                } else {
                    if (value.getNativeObject() instanceof byte[]) {
                        // for BWC with the ES-Sink
                        doc = new String((byte[]) value.getNativeObject(), StandardCharsets.UTF_8);
                    } else {
                        doc = value.getNativeObject().toString();
                    }
                }
            }

            if (doc != null && primaryFields != null) {
                try {
                    // extract the PK from the JSON document
                    JsonNode jsonNode = objectMapper.readTree(doc);
                    id = stringifyKey(jsonNode, primaryFields);
                } catch (JsonProcessingException e) {
                    log.error("Failed to read JSON", e);
                    throw e;
                }
            }

            final ElasticSearchConfig.IdHashingAlgorithm idHashingAlgorithm =
                    elasticSearchConfig.getIdHashingAlgorithm();
            if (id != null
                    && idHashingAlgorithm != null
                    && idHashingAlgorithm != ElasticSearchConfig.IdHashingAlgorithm.NONE) {
                final byte[] idBytes = id.getBytes(StandardCharsets.UTF_8);

                boolean performHashing = true;
                if (elasticSearchConfig.isConditionalIdHashing() && idBytes.length <= 512) {
                    performHashing = false;
                }
                if (performHashing) {
                    Hasher hasher;
                    switch (idHashingAlgorithm) {
                        case SHA256:
                            hasher = Hashing.sha256().newHasher();
                            break;
                        case SHA512:
                            hasher = Hashing.sha512().newHasher();
                            break;
                        default:
                            throw new UnsupportedOperationException("Unsupported IdHashingAlgorithm: "
                                    + idHashingAlgorithm);
                    }
                    hasher.putBytes(idBytes);
                    id = base64Encoder.encodeToString(hasher.hash().asBytes());
                }
            }

            if (log.isDebugEnabled()) {
                SchemaType schemaType = null;
                if (record.getSchema() != null && record.getSchema().getSchemaInfo() != null) {
                    schemaType = record.getSchema().getSchemaInfo().getType();
                }
                log.debug("recordType={} schemaType={} id={} doc={}",
                        record.getClass().getName(),
                        schemaType,
                        id,
                        doc);
            }
            doc = sanitizeValue(doc);
            return Pair.of(id, doc);
    } else {
            final byte[] data = record
                    .getMessage()
                    .orElseThrow(() -> new IllegalArgumentException("Record does not carry message information"))
                    .getData();
            String doc = new String(data, StandardCharsets.UTF_8);
            doc = sanitizeValue(doc);
            return Pair.of(null, doc);
        }
    }

    private String sanitizeValue(String value) {
        if (value == null || !elasticSearchConfig.isStripNonPrintableCharacters()) {
            return value;
        }
        return nonPrintableCharactersPattern.matcher(value).replaceAll("");

    }

    public String stringifyKey(Schema<?> schema, Object val) throws JsonProcessingException {
        switch (schema.getSchemaInfo().getType()) {
            case INT8:
                return Byte.toString((Byte) val);
            case INT16:
                return Short.toString((Short) val);
            case INT32:
                return Integer.toString((Integer) val);
            case INT64:
                return Long.toString((Long) val);
            case STRING:
                return (String) val;
            case JSON:
            case AVRO:
                return stringifyKey(extractJsonNode(schema, val));
            default:
                throw new UnsupportedOperationException("Unsupported key schemaType="
                        + schema.getSchemaInfo().getType());
        }
    }


    /**
     * Convert a JsonNode to an Elasticsearch id.
     */

    public String stringifyKey(JsonNode jsonNode) throws JsonProcessingException {
        List<String> fields = new ArrayList<>();
        jsonNode.fieldNames().forEachRemaining(fields::add);
        return stringifyKey(jsonNode, fields);
    }

    public String stringifyKey(JsonNode jsonNode, List<String> fields) throws JsonProcessingException {
        JsonNode toConvert;
        if (fields.size() == 1) {
            toConvert = jsonNode.get(fields.get(0));
        } else {
            toConvert = JsonConverter.toJsonArray(jsonNode, fields);
        }

        String serializedId;
        if (elasticSearchConfig.isCanonicalKeyFields()) {
            final Object obj = sortedObjectMapper.treeToValue(toConvert, Object.class);
            serializedId = sortedObjectMapper.writeValueAsString(obj);
        } else {
            serializedId = objectMapper.writeValueAsString(toConvert);
        }

        return (serializedId.startsWith("\"") && serializedId.endsWith("\""))
                ? serializedId.substring(1, serializedId.length() - 1)  // remove double quotes
                : serializedId;
    }

    public String stringifyValue(Schema<?> schema, Object val) throws JsonProcessingException {
        JsonNode jsonNode = extractJsonNode(schema, val);
        return stringify(jsonNode);
    }

    public String stringify(JsonNode jsonNode) throws JsonProcessingException {
        return elasticSearchConfig.isStripNulls()
                ? objectMapper.writeValueAsString(stripNullNodes(jsonNode))
                : objectMapper.writeValueAsString(jsonNode);
    }

    public static JsonNode stripNullNodes(JsonNode node) {
        Iterator<JsonNode> it = node.iterator();
        while (it.hasNext()) {
            JsonNode child = it.next();
            if (child.isNull()) {
                it.remove();
            } else {
                stripNullNodes(child);
            }
        }
        return node;
    }

    public static JsonNode extractJsonNode(Schema<?> schema, Object val) {
        if (val == null) {
            return null;
        }
        switch (schema.getSchemaInfo().getType()) {
            case JSON:
                return (JsonNode) ((GenericRecord) val).getNativeObject();
            case AVRO:
                org.apache.avro.generic.GenericRecord node = (org.apache.avro.generic.GenericRecord)
                        ((GenericRecord) val).getNativeObject();
                return JsonConverter.toJson(node);
            default:
                throw new UnsupportedOperationException("Unsupported value schemaType="
                        + schema.getSchemaInfo().getType());
        }
    }

}
