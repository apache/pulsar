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
package org.apache.pulsar.client.impl.schema.generic;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.schema.Field;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

/**
 * Generic json record.
 */
@Slf4j
public class GenericJsonRecord extends VersionedGenericRecord {

    private final JsonNode jn;
    private final SchemaInfo schemaInfo;

    GenericJsonRecord(byte[] schemaVersion,
                      List<Field> fields,
                      JsonNode jn) {
        this(schemaVersion, fields, jn, null);
    }

    public GenericJsonRecord(byte[] schemaVersion,
                      List<Field> fields,
                      JsonNode jn, SchemaInfo schemaInfo) {
        super(schemaVersion, fields);
        this.jn = jn;
        this.schemaInfo = schemaInfo;
    }

    public JsonNode getJsonNode() {
        return jn;
    }

    @Override
    public Object getField(String fieldName) {
        JsonNode fn = jn.get(fieldName);
        if (fn == null) {
            return null;
        }
        if (fn.isContainerNode()) {
            AtomicInteger idx = new AtomicInteger(0);
            List<Field> fields = Lists.newArrayList(fn.fieldNames())
                .stream()
                .map(f -> new Field(f, idx.getAndIncrement()))
                .collect(Collectors.toList());
            return new GenericJsonRecord(schemaVersion, fields, fn, schemaInfo);
        } else if (fn.isBoolean()) {
            return fn.asBoolean();
        } else if (fn.isFloatingPointNumber()) {
            return fn.asDouble();
        } else if (fn.isBigInteger()) {
            if (fn.canConvertToLong()) {
                return fn.asLong();
            } else {
                return fn.asText();
            }
        } else if (fn.isNumber()) {
            return fn.numberValue();
        } else if (fn.isBinary()) {
            try {
                return fn.binaryValue();
            } catch (IOException e) {
                return fn.asText();
            }
        } else if (isBinaryValue(fieldName)) {
            try {
                return fn.binaryValue();
            } catch (IOException e) {
                return fn.asText();
            }
        } else if (fn.isNull()) {
            return null;
        } else {
            return fn.asText();
        }
    }

    private boolean isBinaryValue(String fieldName) {
        if (schemaInfo == null) {
            return false;
        }

        boolean isBinary = false;
        try {
            org.apache.avro.Schema schema = parseAvroSchema(schemaInfo.getSchemaDefinition());
            org.apache.avro.Schema.Field field = schema.getField(fieldName);
            if (field == null) {
                return false;
            }
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode jsonNode = objectMapper.readTree(field.schema().toString());
            for (JsonNode node : jsonNode) {
                JsonNode jn = node.get("type");
                if (jn != null && ("bytes".equals(jn.asText()) || "byte".equals(jn.asText()))) {
                    isBinary = true;
                    break;
                }
            }
        } catch (Exception e) {
            log.error("parse schemaInfo failed. ", e);
        }
        return isBinary;
    }

    private static org.apache.avro.Schema parseAvroSchema(String schemaJson) {
        final org.apache.avro.Schema.Parser parser = new org.apache.avro.Schema.Parser();
        parser.setValidateDefaults(false);
        return parser.parse(schemaJson);
    }

    @Override
    public Object getNativeObject() {
        return jn;
    }

    @Override
    public SchemaType getSchemaType() {
        return SchemaType.JSON;
    }
}
