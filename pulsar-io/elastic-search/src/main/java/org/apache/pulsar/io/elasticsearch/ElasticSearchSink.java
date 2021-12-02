/**
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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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
    private ObjectMapper objectMapper = new ObjectMapper();
    private List<String> primaryFields = null;

    @Override
    public void open(Map<String, Object> config, SinkContext sinkContext) throws Exception {
        elasticSearchConfig = ElasticSearchConfig.load(config);
        elasticSearchConfig.validate();
        elasticsearchClient = new ElasticSearchClient(elasticSearchConfig);
        if (!Strings.isNullOrEmpty(elasticSearchConfig.getPrimaryFields())) {
            primaryFields = Arrays.asList(elasticSearchConfig.getPrimaryFields().split(","));
        }
    }

    @Override
    public void close() {
        if (elasticsearchClient != null) {
            elasticsearchClient.close();
            elasticsearchClient = null;
        }
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
                            elasticsearchClient.failed(new PulsarClientException.InvalidMessageException("Unexpected null message value"));
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
                KeyValue<GenericObject, GenericObject> keyValue = (KeyValue<GenericObject, GenericObject>) record.getValue().getNativeObject();
                key = keyValue.getKey();
                value = keyValue.getValue();
            } else {
                key = record.getKey().orElse(null);
                valueSchema = record.getSchema();
                value = record.getValue();
            }

            String id = null;
            if (elasticSearchConfig.isKeyIgnore() == false && key != null && keySchema != null) {
                id = stringifyKey(keySchema, key);
            }

            String doc = null;
            if (value != null) {
                if (valueSchema != null) {
                    doc = stringifyValue(valueSchema, value);
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
            return Pair.of(id, doc);
    } else {
        return Pair.of(null, new String(
                record.getMessage()
                        .orElseThrow(() -> new IllegalArgumentException("Record does not carry message information"))
                        .getData(), StandardCharsets.UTF_8));
        }
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
                throw new UnsupportedOperationException("Unsupported key schemaType=" + schema.getSchemaInfo().getType());
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
        if (fields.size() == 1) {
            JsonNode singleNode = jsonNode.get(fields.get(0));
            String id = objectMapper.writeValueAsString(singleNode);
            return (id.startsWith("\"") && id.endsWith("\""))
                    ? id.substring(1, id.length() - 1)  // remove double quotes
                    : id;
        } else {
            return JsonConverter.toJsonArray(jsonNode, fields).toString();
        }
    }

    public String stringifyValue(Schema<?> schema, Object val) throws JsonProcessingException {
        JsonNode jsonNode = extractJsonNode(schema, val);
        return elasticSearchConfig.isStripNulls()
                ? objectMapper.writeValueAsString(stripNullNodes(jsonNode))
                : objectMapper.writeValueAsString(jsonNode);
    }

    public static JsonNode stripNullNodes(JsonNode node) {
        Iterator<JsonNode> it = node.iterator();
        while (it.hasNext()) {
            JsonNode child = it.next();
            if (child.isNull())
                it.remove();
            else
                stripNullNodes(child);
        }
        return node;
    }

    public static JsonNode extractJsonNode(Schema<?> schema, Object val) {
        switch (schema.getSchemaInfo().getType()) {
            case JSON:
                return (JsonNode) ((GenericRecord) val).getNativeObject();
            case AVRO:
                org.apache.avro.generic.GenericRecord node = (org.apache.avro.generic.GenericRecord) ((GenericRecord) val).getNativeObject();
                return JsonConverter.toJson(node);
            default:
                throw new UnsupportedOperationException("Unsupported value schemaType=" + schema.getSchemaInfo().getType());
        }
    }

}