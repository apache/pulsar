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

package org.apache.pulsar.io.jdbc;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import java.sql.PreparedStatement;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.KeyValueSchema;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.jdbc.JdbcUtils.ColumnId;

/**
 * An abstract Jdbc sink, which interprets input Record in generic record.
 */
@Slf4j
public abstract class BaseJdbcAutoSchemaSink extends JdbcAbstractSink<GenericObject> {

    @Override
    public void bindValue(PreparedStatement statement,
                          Record<GenericObject> message, String action) throws Exception {
        final GenericObject record = message.getValue();
        Function<String, Object> recordValueGetter;
        if (message.getSchema() != null && message.getSchema() instanceof KeyValueSchema) {
            KeyValueSchema<GenericObject, GenericObject> keyValueSchema = (KeyValueSchema) message.getSchema();

            final org.apache.pulsar.client.api.Schema<GenericObject> keySchema = keyValueSchema.getKeySchema();
            final org.apache.pulsar.client.api.Schema<GenericObject> valueSchema = keyValueSchema.getValueSchema();
            KeyValue<GenericObject, GenericObject> keyValue =
                    (KeyValue<GenericObject, GenericObject>) record.getNativeObject();

            final GenericObject key = keyValue.getKey();
            final GenericObject value = keyValue.getValue();

            Map<String, Object> data = new HashMap<>();
            fillKeyValueSchemaData(keySchema, key, data);
            fillKeyValueSchemaData(valueSchema, value, data);
            recordValueGetter = (k) -> data.get(k);
        } else {
            recordValueGetter = (key) -> ((GenericRecord) record).getField(key);
        }

        List<ColumnId> columns = Lists.newArrayList();
        if (action == null || action.equals(INSERT)) {
            columns = tableDefinition.getColumns();
        } else if (action.equals(DELETE)){
            columns.addAll(tableDefinition.getKeyColumns());
        } else if (action.equals(UPDATE)){
            columns.addAll(tableDefinition.getNonKeyColumns());
            columns.addAll(tableDefinition.getKeyColumns());
        }

        int index = 1;
        for (ColumnId columnId : columns) {
            String colName = columnId.getName();
            int colType = columnId.getType();
            if (log.isDebugEnabled()) {
                log.debug("colName: {} colType: {}", colName, colType);
            }
            try {
                Object obj = recordValueGetter.apply(colName);
                if (obj != null) {
                    setColumnValue(statement, index++, obj);
                } else {
                    if (log.isDebugEnabled()) {
                        log.debug("Column {} is null", colName);
                    }
                    setColumnNull(statement, index++, colType);
                }
            } catch (NullPointerException e) {
                // With JSON schema field is omitted, so get NPE
                // In this case we want to set column to Null
                if (log.isDebugEnabled()) {
                    log.debug("Column {} is null", colName);
                }
                setColumnNull(statement, index++, colType);
            }
        }
    }

    private static void setColumnNull(PreparedStatement statement, int index, int type) throws Exception {
        if (log.isDebugEnabled()) {
            log.debug("Setting column value to null, statement: {}, index: {}", statement.toString(), index);
        }
        statement.setNull(index, type);

    }

    private static void setColumnValue(PreparedStatement statement, int index, Object value) throws Exception {

        log.debug("Setting column value, statement: {}, index: {}, value: {}", statement, index, value);

        if (value instanceof Integer) {
            statement.setInt(index, (Integer) value);
        } else if (value instanceof Long) {
            statement.setLong(index, (Long) value);
        } else if (value instanceof Double) {
            statement.setDouble(index, (Double) value);
        } else if (value instanceof Float) {
            statement.setFloat(index, (Float) value);
        } else if (value instanceof Boolean) {
            statement.setBoolean(index, (Boolean) value);
        } else if (value instanceof String) {
            statement.setString(index, (String) value);
        } else if (value instanceof Short) {
            statement.setShort(index, (Short) value);
        } else {
            throw new Exception("Not support value type, need to add it. " + value.getClass());
        }
    }

    private static Object getValueFromJsonNode(final JsonNode fn) {
        if (fn == null || fn.isNull()) {
            return null;
        }
        if (fn.isContainerNode()) {
            throw new IllegalArgumentException("Container nodes are not supported, the JSON must contains only "
                    + "first level fields.");
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
        } else {
            return fn.asText();
        }
    }

    private static void fillKeyValueSchemaData(org.apache.pulsar.client.api.Schema<GenericObject> schema,
                                        GenericObject record,
                                        Map<String, Object> data) {
        switch (schema.getSchemaInfo().getType()) {
            case JSON:
                final JsonNode jsonNode = (JsonNode) record.getNativeObject();
                final Iterator<String> fieldNames = jsonNode.fieldNames();
                while (fieldNames.hasNext()) {
                    String fieldName = fieldNames.next();
                    final JsonNode nodeValue = jsonNode.get(fieldName);
                    data.put(fieldName, getValueFromJsonNode(nodeValue));
                }
                break;
            case AVRO:
                org.apache.avro.generic.GenericRecord avroNode =
                        (org.apache.avro.generic.GenericRecord) record.getNativeObject();
                for (Schema.Field field : avroNode.getSchema().getFields()) {
                    final String fieldName = field.name();
                    data.put(fieldName, convertAvroField(avroNode.get(fieldName), field.schema()));
                }
                break;
            default:
                throw new IllegalArgumentException("unexpected schema type: "
                        + schema.getSchemaInfo().getType()
                        + " with KeyValueSchema");
        }
    }

    @VisibleForTesting
    static Object convertAvroField(Object avroValue, Schema schema) {
        switch (schema.getType()) {
            case NULL:
            case INT:
            case LONG:
            case DOUBLE:
            case FLOAT:
            case BOOLEAN:
                return avroValue;
            case ENUM:
            case STRING:
                return avroValue.toString(); // can be a String or org.apache.avro.util.Utf8
            case UNION:
                for (Schema s : schema.getTypes()) {
                    if (s.getType() == Schema.Type.NULL) {
                        continue;
                    }
                    return convertAvroField(avroValue, s);
                }
                throw new IllegalArgumentException("Found UNION schema but it doesn't contain any type");
            case ARRAY:
            case BYTES:
            case FIXED:
            case RECORD:
            case MAP:
            default:
                throw new UnsupportedOperationException("Unsupported avro schema type=" + schema.getType()
                        + " for value field schema " + schema.getName());
        }
    }
}

