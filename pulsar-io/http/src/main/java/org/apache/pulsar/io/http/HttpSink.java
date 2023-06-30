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

package org.apache.pulsar.io.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.client.api.schema.KeyValueSchema;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;

/**
 * A Sink that makes a POST request to a configured HTTP endpoint for each record (webhook).
 * The body of the HTTP request is the JSON representation of the record value.
 * Some headers are added to the HTTP request:
 * <ul>
 *   <li>PulsarTopic: the topic of the record</li>
 *   <li>PulsarKey: the key of the record</li>
 *   <li>PulsarEventTime: the event time of the record</li>
 *   <li>PulsarPublishTime: the publish time of the record</li>
 *   <li>PulsarMessageId: the ID of the message contained in the record</li>
 *   <li>PulsarProperties-*: each record property is passed with the property name prefixed by PulsarProperties-</li>
 * </ul>
 */
public class HttpSink implements Sink<GenericObject> {

    HttpSinkConfig httpSinkConfig;
    private HttpClient httpClient;
    private ObjectMapper mapper;
    private URI uri;

    @Override
    public void open(Map<String, Object> config, SinkContext sinkContext) throws Exception {
        httpSinkConfig = HttpSinkConfig.load(config);
        uri = new URI(httpSinkConfig.getUrl());
        httpClient = HttpClient.newHttpClient();
        mapper = new ObjectMapper().registerModule(new JavaTimeModule());
    }

    @Override
    public void write(Record<GenericObject> record) throws Exception {
        Object json = toJsonSerializable(record.getSchema(), record.getValue().getNativeObject());
        byte[] bytes = mapper.writeValueAsBytes(json);
        HttpRequest.Builder builder = HttpRequest.newBuilder()
            .uri(uri)
            .POST(HttpRequest.BodyPublishers.ofByteArray(bytes));
        httpSinkConfig.getHeaders().forEach(builder::header);
        record.getProperties().forEach((k, v) -> builder.header("PulsarProperties-" + k, v));
        record.getTopicName().ifPresent(topic -> builder.header("PulsarTopic", topic));
        record.getEventTime().ifPresent(eventTime -> builder.header("PulsarEventTime", eventTime.toString()));
        record.getKey().ifPresent(key -> builder.header("PulsarKey", key));
        record.getMessage().ifPresent(
            message -> {
              if (message.getMessageId() != null) {
                String messageId = Base64.getEncoder().encodeToString(message.getMessageId().toByteArray());
                builder.header("PulsarMessageId", messageId);
              }
              if (message.getPublishTime() != 0) {
                builder.header("PulsarPublishTime", String.valueOf(message.getPublishTime()));
              }
            }
        );
        builder.header("Content-Type", "application/json");

        HttpResponse<String> response = httpClient.send(builder.build(), HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            throw new IOException(
                String.format("HTTP call to %s failed with status code %s", uri, response.statusCode()));
        }

    }

    private static Object toJsonSerializable(Schema<?> schema, Object val) {
        if (schema == null || schema.getSchemaInfo().getType().isPrimitive()) {
            return val;
        }
        switch (schema.getSchemaInfo().getType()) {
            case KEY_VALUE:
                KeyValueSchema<?, ?> keyValueSchema = (KeyValueSchema<?, ?>) schema;
                org.apache.pulsar.common.schema.KeyValue<?, ?> keyValue =
                    (org.apache.pulsar.common.schema.KeyValue<?, ?>) val;
                Map<String, Object> jsonKeyValue = new HashMap<>();
                Object key = keyValue.getKey();
                Object value = keyValue.getValue();
                jsonKeyValue.put("key", toJsonSerializable(keyValueSchema.getKeySchema(),
                    key instanceof GenericObject ? ((GenericObject) key).getNativeObject() : key));
                jsonKeyValue.put("value", toJsonSerializable(keyValueSchema.getValueSchema(),
                    value instanceof GenericObject ? ((GenericObject) value).getNativeObject() : value));
                return jsonKeyValue;
            case AVRO:
                return JsonConverter.toJson((org.apache.avro.generic.GenericRecord) val);
            case JSON:
                return val;
            default:
                throw new UnsupportedOperationException("Unsupported schema type ="
                    + schema.getSchemaInfo().getType());
        }
    }

    @Override
    public void close() {}
}
