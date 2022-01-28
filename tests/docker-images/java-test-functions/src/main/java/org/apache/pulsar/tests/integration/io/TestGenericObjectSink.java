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
package org.apache.pulsar.tests.integration.io;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.client.api.schema.KeyValueSchema;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;
import java.util.Map;

@Slf4j
public class TestGenericObjectSink implements Sink<GenericObject> {

    @Override
    public void open(Map<String, Object> config, SinkContext sourceContext) throws Exception {
    }

    public void write(Record<GenericObject> record) {
        log.info("topic {}", record.getTopicName().orElse(null));
        log.info("properties {}", record.getProperties());
        log.info("received record {} {}", record, record.getClass());
        log.info("schema {}", record.getSchema());
        log.info("native schema {}", record.getSchema().getNativeSchema().orElse(null));
        log.info("schemaInfo {}", record.getSchema().getSchemaInfo());
        log.info("schemaInfo.type {}", record.getSchema().getSchemaInfo().getType());

        String expectedRecordType = record.getProperties().getOrDefault("expectedType", "MISSING");
        log.info("expectedRecordType {}", expectedRecordType);
        if (!expectedRecordType.equals(record.getSchema().getSchemaInfo().getType().name())) {
            throw new RuntimeException("Unexpected record type " + record.getSchema().getSchemaInfo().getType().name() + " is not " + expectedRecordType);
        }

        log.info("value {}", record.getValue());
        log.info("value schema type {}", record.getValue().getSchemaType());
        log.info("value native object {}", record.getValue().getNativeObject());

        if (record.getSchema().getSchemaInfo().getType() == SchemaType.KEY_VALUE) {
            // assert that we are able to access the schema (leads to ClassCastException if there is a problem)
            KeyValueSchema kvSchema = (KeyValueSchema) record.getSchema();
            log.info("key schema type {}", kvSchema.getKeySchema());
            log.info("value schema type {}", kvSchema.getValueSchema());
            log.info("key encoding {}", kvSchema.getKeyValueEncodingType());

            KeyValue keyValue = (KeyValue) record.getValue().getNativeObject();
            log.info("kvkey {}", keyValue.getKey());
            log.info("kvvalue {}", keyValue.getValue());
        }
        log.info("value {}", record.getValue());
        log.info("value schema type {}", record.getValue().getSchemaType());
        log.info("value native object {} class {}", record.getValue().getNativeObject(), record.getValue().getNativeObject().getClass());

        String expectedSchemaDefinition = record.getProperties().getOrDefault("expectedSchemaDefinition", "");
        log.info("schemaDefinition {}", record.getSchema().getSchemaInfo().getSchemaDefinition());
        log.info("expectedSchemaDefinition {}", expectedSchemaDefinition);
        if (!expectedSchemaDefinition.isEmpty()) {
            String schemaDefinition = record.getSchema().getSchemaInfo().getSchemaDefinition();
            if (!expectedSchemaDefinition.equals(schemaDefinition)) {
                throw new RuntimeException("Unexpected schema definition " + schemaDefinition + " is not " + expectedSchemaDefinition);
            }
        }

        // testing that actually the Sink is able to use Native AVRO
        if (record.getSchema().getSchemaInfo().getType() == SchemaType.AVRO) {
            GenericRecord nativeGenericRecord = (GenericRecord) record.getValue().getNativeObject();
            log.info("Schema from AVRO generic object {}", nativeGenericRecord.getSchema());
        }

        // testing that actually the Sink is able to use Native JSON
        if (record.getSchema().getSchemaInfo().getType() == SchemaType.JSON) {
            JsonNode nativeGenericRecord = (JsonNode) record.getValue().getNativeObject();
            log.info("NodeType from JsonNode generic object {}", nativeGenericRecord.getNodeType());
        }

        record.ack();
    }

    @Override
    public void close() throws Exception {

    }
}
