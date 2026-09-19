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
package org.apache.pulsar.tests.integration.io;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.protobuf.DynamicMessage;
import java.util.Map;
import lombok.CustomLog;
import org.apache.avro.generic.GenericRecord;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.client.api.schema.KeyValueSchema;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;

@CustomLog
public class TestGenericObjectSink implements Sink<GenericObject> {

    @Override
    public void open(Map<String, Object> config, SinkContext sourceContext) throws Exception {
    }

    public void write(Record<GenericObject> record) {
        log.info().attr("topic", record.getTopicName().orElse(null)).log("topic");
        log.info().attr("properties", record.getProperties()).log("properties");
        log.info().attr("record", record).attr("class", record.getClass()).log("received record");
        log.info().attr("schema", record.getSchema()).log("schema");
        log.info().attr("nativeSchema", record.getSchema().getNativeSchema().orElse(null))
                .log("native schema");
        log.info().attr("schemaInfo", record.getSchema().getSchemaInfo()).log("schemaInfo");
        log.info().attr("type", record.getSchema().getSchemaInfo().getType())
                .log("schemaInfo.type");

        String expectedRecordType = record.getProperties().getOrDefault("expectedType", "MISSING");
        log.info().attr("expectedRecordType", expectedRecordType).log("expectedRecordType");
        if (!expectedRecordType.equals(record.getSchema().getSchemaInfo().getType().name())) {
            final String message = String.format(
                    "Unexpected record type %s is not %s",
                    record.getSchema().getSchemaInfo().getType().name(),
                    expectedRecordType);
            throw new RuntimeException(message);
        }

        log.info().attr("value", record.getValue()).log("value");
        log.info().attr("schemaType", record.getValue().getSchemaType()).log("value schema type");
        log.info().attr("nativeObject", record.getValue().getNativeObject())
                .log("value native object");

        if (record.getSchema().getSchemaInfo().getType() == SchemaType.KEY_VALUE) {
            // assert that we are able to access the schema (leads to ClassCastException if there is a problem)
            KeyValueSchema kvSchema = (KeyValueSchema) record.getSchema();
            log.info().attr("keySchema", kvSchema.getKeySchema()).log("key schema type");
            log.info().attr("valueSchema", kvSchema.getValueSchema()).log("value schema type");
            log.info().attr("keyEncoding", kvSchema.getKeyValueEncodingType()).log("key encoding");

            KeyValue keyValue = (KeyValue) record.getValue().getNativeObject();
            log.info().attr("key", keyValue.getKey()).log("kvkey");
            log.info().attr("value", keyValue.getValue()).log("kvvalue");
        }

        final GenericObject value = record.getValue();
        log.info().attr("value", value).log("value");
        log.info().attr("schemaType", value.getSchemaType()).log("value schema type");
        log.info().attr("nativeObject", value.getNativeObject())
                .attr("class", value.getNativeObject().getClass())
                .log("value native object");

        String expectedSchemaDefinition = record.getProperties().getOrDefault("expectedSchemaDefinition", "");
        log.info().attr("schemaDefinition", record.getSchema().getSchemaInfo().getSchemaDefinition())
                .log("schemaDefinition");
        log.info().attr("expectedSchemaDefinition", expectedSchemaDefinition)
                .log("expectedSchemaDefinition");
        if (!expectedSchemaDefinition.isEmpty()) {
            String schemaDefinition = record.getSchema().getSchemaInfo().getSchemaDefinition();
            if (!expectedSchemaDefinition.equals(schemaDefinition)) {
                final String message = String.format(
                        "Unexpected schema definition %s is not %s", schemaDefinition, expectedSchemaDefinition);
                throw new RuntimeException(message);
            }
        }

        // testing that actually the Sink is able to use Native AVRO
        if (record.getSchema().getSchemaInfo().getType() == SchemaType.AVRO) {
            GenericRecord nativeGenericRecord = (GenericRecord) record.getValue().getNativeObject();
            log.info().attr("schema", nativeGenericRecord.getSchema())
                    .log("Schema from AVRO generic object");
        }

        // testing that actually the Sink is able to use Native JSON
        if (record.getSchema().getSchemaInfo().getType() == SchemaType.JSON) {
            JsonNode nativeGenericRecord = (JsonNode) record.getValue().getNativeObject();
            log.info().attr("nodeType", nativeGenericRecord.getNodeType())
                    .log("NodeType from JsonNode generic object");
        }

        // testing that actually the Sink is able to use Native JSON
        if (record.getSchema().getSchemaInfo().getType() == SchemaType.PROTOBUF_NATIVE) {
            DynamicMessage dynamicMessage = (DynamicMessage) record.getValue().getNativeObject();
            log.info().attr("fields", dynamicMessage.getAllFields())
                    .log("Schema from PROTOBUF_NATIVE generic object");
        }

        record.ack();
    }

    @Override
    public void close() throws Exception {

    }
}
