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
import com.google.gson.JsonArray;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.client.impl.schema.KeyValueSchema;
import org.apache.pulsar.client.impl.schema.generic.GenericAvroRecord;
import org.apache.pulsar.client.impl.schema.generic.GenericJsonRecord;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.core.annotations.Connector;
import org.apache.pulsar.io.core.annotations.IOType;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.codec.binary.Hex;
/**
 * The base abstract class for ElasticSearch sinks.
 * Users need to implement extractKeyValue function to use this sink.
 * This class assumes that the input will be JSON documents
 */
@Connector(
        name = "elastic_search",
        type = IOType.SINK,
        help = "A sink connector that sends pulsar messages to elastic search",
        configClass = ElasticsearchConfig.class
)
@Slf4j
public class ElasticSearchSink implements Sink<GenericObject> {

    private ElasticsearchConfig elasticSearchConfig;
    private ElasticsearchClient elasticsearchClient;
    private ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void open(Map<String, Object> config, SinkContext sinkContext) throws Exception {
        elasticSearchConfig = ElasticsearchConfig.load(config);
        elasticSearchConfig.validate();
        elasticsearchClient = new ElasticsearchClient(elasticSearchConfig);
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
            try {
                Pair<String, String> idAndDoc = extractIdAndDocument(record);
                if (idAndDoc.getRight() == null) {
                    switch (elasticSearchConfig.getNullValueAction()) {
                        case DELETE:
                            if (elasticSearchConfig.isBulkEnabled()) {
                                elasticsearchClient.bulkDelete(record, idAndDoc.getLeft());
                            } else {
                                elasticsearchClient.deleteDocument(record, idAndDoc.getLeft());
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
            } catch (Exception e) {
                log.error("write error:", e);
                throw e;
            }
        }
    }

    @VisibleForTesting
    ElasticsearchClient getElasticsearchClient() {
        return this.elasticsearchClient;
    }

    /**
     * Extract ES _id and _source using the Schema if available.
     *
     * @param record
     * @return A pair for _id and _source
     */
    public Pair<String, String> extractIdAndDocument(Record<GenericObject> record) {
        Object key = null;
        Object value = null;
        Schema<?> keySchema = null;
        Schema<?> valueSchema = null;

        if (record.getSchema() instanceof KeyValueSchema) {
            KeyValueSchema<GenericObject,GenericObject> keyValueSchema = (KeyValueSchema) record.getSchema();
            keySchema = keyValueSchema.getKeySchema();
            valueSchema = keyValueSchema.getValueSchema();
            if (KeyValueEncodingType.SEPARATED.equals(keyValueSchema.getKeyValueEncodingType()) && record.getKey().isPresent()) {
                key = keySchema.decode(record.getKey().get().getBytes(StandardCharsets.UTF_8));
            }
            if (record.getValue() != null) {
                value = ((KeyValue) record.getValue().getNativeObject()).getValue();
                if (KeyValueEncodingType.INLINE.equals(keyValueSchema.getKeyValueEncodingType())) {
                    key = ((KeyValue) record.getValue().getNativeObject()).getKey();
                }
            }
        } else {
            key = record.getKey().orElse(null);
            valueSchema = record.getSchema();
            value = record.getValue() == null ? null : record.getValue().getNativeObject();
        }

        String id = key.toString();
        if (keySchema != null) {
            id = stringifyKey(keySchema, key);
        }

        String doc = null;
        if (value != null) {
            if (valueSchema != null) {
                doc = stringifyValue(valueSchema, value);
            } else {
                doc = value.toString();
            }
        }

        if (elasticSearchConfig.isKeyIgnore()) {
            if (doc == null || Strings.isNullOrEmpty(elasticSearchConfig.getPrimaryFields())) {
                id = Hex.encodeHexString(record.getMessage().get().getMessageId().toByteArray());
            } else {
                try {
                    // extract the PK from the JSON document
                    JsonNode jsonNode = objectMapper.readTree(doc);
                    List<String> pkFields = Arrays.asList(elasticSearchConfig.getPrimaryFields().split(","));
                    id = stringifyKey(jsonNode, pkFields);
                } catch (JsonProcessingException e) {
                    log.error("Failed to read JSON", e);
                }
            }
        }

        SchemaType schemaType = null;
        if (record.getSchema() != null && record.getSchema().getSchemaInfo() != null) {
            schemaType = record.getSchema().getSchemaInfo().getType();
        }
        log.debug("recordType={} schemaType={} id={} doc={}",
                record.getClass().getName(),
                schemaType,
                id,
                doc);
        return Pair.of(id, doc);
    }

    public String stringifyKey(Schema<?> schema, Object val) {
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
                try {
                    GenericJsonRecord genericJsonRecord = (GenericJsonRecord) val;
                    return stringifyKey(genericJsonRecord.getJsonNode());
                } catch (JsonProcessingException e) {
                    log.error("Failed to convert JSON to a JSON string", e);
                    throw new RuntimeException(e);
                }
            case AVRO:
                try {
                    GenericAvroRecord genericAvroRecord = (GenericAvroRecord) val;
                    JsonNode jsonNode = JsonConverter.toJson(genericAvroRecord.getAvroRecord());
                    return stringifyKey(jsonNode);
                } catch (Exception e) {
                    log.error("Failed to convert AVRO to a JSON string", e);
                    throw new RuntimeException(e);
                }
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

    public String stringifyValue(Schema<?> schema, Object val) {
        switch (schema.getSchemaInfo().getType()) {
            case JSON:
                try {
                    GenericJsonRecord genericJsonRecord = (GenericJsonRecord) val;
                    return objectMapper.writeValueAsString(genericJsonRecord.getJsonNode());
                } catch (JsonProcessingException e) {
                    log.error("Failed to convert JSON to a JSON string", e);
                    throw new RuntimeException(e);
                }
            case AVRO:
                try {
                    GenericAvroRecord genericAvroRecord = (GenericAvroRecord) val;
                    JsonNode jsonNode = JsonConverter.toJson(genericAvroRecord.getAvroRecord());
                    return objectMapper.writeValueAsString(jsonNode);
                } catch (Exception e) {
                    log.error("Failed to convert AVRO to a JSON string", e);
                    throw new RuntimeException(e);
                }
            default:
                throw new UnsupportedOperationException("Unsupported value schemaType=" + schema.getSchemaInfo().getType());
        }
    }
}