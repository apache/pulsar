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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;

/**
 * Convert an AVRO GenericRecord to a JsonNode.
 */
public class JsonConverter {

    private static final Map<String, LogicalTypeConverter> logicalTypeConverters = new HashMap<>();
    private static final JsonNodeFactory jsonNodeFactory = JsonNodeFactory.withExactBigDecimals(true);

    public static JsonNode toJson(GenericRecord genericRecord) {
        if (genericRecord == null) {
            return null;
        }
        ObjectNode objectNode = jsonNodeFactory.objectNode();
        for (Schema.Field field : genericRecord.getSchema().getFields()) {
            objectNode.set(field.name(), toJson(field.schema(), genericRecord.get(field.name())));
        }
        return objectNode;
    }

    public static JsonNode toJson(Schema schema, Object value) {
        if (schema.getLogicalType() != null && logicalTypeConverters.containsKey(schema.getLogicalType().getName())) {
            return logicalTypeConverters.get(schema.getLogicalType().getName()).toJson(schema, value);
        }
        if (value == null) {
            return jsonNodeFactory.nullNode();
        }
        try {
            switch (schema.getType()) {
                case NULL: // this should not happen
                    return jsonNodeFactory.nullNode();
                case INT:
                    return jsonNodeFactory.numberNode((Integer) value);
                case LONG:
                    return jsonNodeFactory.numberNode((Long) value);
                case DOUBLE:
                    return jsonNodeFactory.numberNode((Double) value);
                case FLOAT:
                    return jsonNodeFactory.numberNode((Float) value);
                case BOOLEAN:
                    return jsonNodeFactory.booleanNode((Boolean) value);
                case BYTES:
                    return jsonNodeFactory.binaryNode((byte[]) value);
                case FIXED:
                    return jsonNodeFactory.binaryNode(((GenericFixed) value).bytes());
                case ENUM: // GenericEnumSymbol
                case STRING:
                    return jsonNodeFactory.textNode(value.toString()); // can be a String or org.apache.avro.util.Utf8
                case ARRAY: {
                    Schema elementSchema = schema.getElementType();
                    ArrayNode arrayNode = jsonNodeFactory.arrayNode();
                    for (Object elem : (Object[]) value) {
                        JsonNode fieldValue = toJson(elementSchema, elem);
                        arrayNode.add(fieldValue);
                    }
                    return arrayNode;
                }
                case MAP: {
                    Map<String, Object> map = (Map<String, Object>) value;
                    ObjectNode objectNode = jsonNodeFactory.objectNode();
                    for (Map.Entry<String, Object> entry : map.entrySet()) {
                        JsonNode jsonNode = toJson(schema.getValueType(), entry.getValue());
                        objectNode.set(entry.getKey(), jsonNode);
                    }
                    return objectNode;
                }
                case RECORD:
                    return toJson((GenericRecord) value);
                case UNION:
                    for (Schema s : schema.getTypes()) {
                        if (s.getType() == Schema.Type.NULL) {
                            continue;
                        }
                        return toJson(s, value);
                    }
                    // this case should not happen
                    return jsonNodeFactory.textNode(value.toString());
                default:
                    throw new UnsupportedOperationException("Unknown AVRO schema type=" + schema.getType());
            }
        } catch (ClassCastException error) {
            throw new IllegalArgumentException("Error while converting a value of type "
                    + value.getClass() + " to a " + schema.getType() + ": " + error, error);
        }
    }

    abstract static class LogicalTypeConverter {
        abstract JsonNode toJson(Schema schema, Object value);
    }

    private static void checkTypeAndNotNull(Object value, String name, Class expected) {
        if (value == null) {
            throw new IllegalArgumentException("Invalid type for " + name
                    + ", expected " + expected.getName() + " but was NULL");
        }
        if (!expected.isInstance(value)) {
            throw new IllegalArgumentException("Invalid type for " + name
                    + ", expected " + expected.getName() + " but was " + value.getClass());
        }
    }

    static {
        logicalTypeConverters.put("decimal", new JsonConverter.LogicalTypeConverter() {
            @Override
            JsonNode toJson(Schema schema, Object value) {
                if (!(value instanceof BigDecimal)) {
                    throw new IllegalArgumentException("Invalid type for Decimal, expected BigDecimal but was "
                            + value.getClass());
                }
                BigDecimal decimal = (BigDecimal) value;
                return jsonNodeFactory.numberNode(decimal);
            }
        });
        logicalTypeConverters.put("date", new JsonConverter.LogicalTypeConverter() {
            @Override
            JsonNode toJson(Schema schema, Object value) {
                if (!(value instanceof Integer)) {
                    throw new IllegalArgumentException("Invalid type for date, expected Integer but was "
                            + value.getClass());
                }
                Integer daysFromEpoch = (Integer) value;
                return jsonNodeFactory.numberNode(daysFromEpoch);
            }
        });
        logicalTypeConverters.put("time-millis", new JsonConverter.LogicalTypeConverter() {
            @Override
            JsonNode toJson(Schema schema, Object value) {
                if (!(value instanceof Integer)) {
                    throw new IllegalArgumentException("Invalid type for time-millis, expected Integer but was "
                            + value.getClass());
                }
                Integer timeMillis = (Integer) value;
                return jsonNodeFactory.numberNode(timeMillis);
            }
        });
        logicalTypeConverters.put("time-micros", new JsonConverter.LogicalTypeConverter() {
            @Override
            JsonNode toJson(Schema schema, Object value) {
                if (!(value instanceof Long)) {
                    throw new IllegalArgumentException("Invalid type for time-micros, expected Long but was "
                            + value.getClass());
                }
                Long timeMicro = (Long) value;
                return jsonNodeFactory.numberNode(timeMicro);
            }
        });
        logicalTypeConverters.put("timestamp-millis", new JsonConverter.LogicalTypeConverter() {
            @Override
            JsonNode toJson(Schema schema, Object value) {
                if (!(value instanceof Long)) {
                    throw new IllegalArgumentException("Invalid type for timestamp-millis, expected Long but was "
                            + value.getClass());
                }
                Long epochMillis = (Long) value;
                return jsonNodeFactory.numberNode(epochMillis);
            }
        });
        logicalTypeConverters.put("timestamp-micros", new JsonConverter.LogicalTypeConverter() {
            @Override
            JsonNode toJson(Schema schema, Object value) {
                if (!(value instanceof Long)) {
                    throw new IllegalArgumentException("Invalid type for timestamp-micros, expected Long but was "
                            + value.getClass());
                }
                Long epochMillis = (Long) value;
                return jsonNodeFactory.numberNode(epochMillis);
            }
        });
        logicalTypeConverters.put("uuid", new JsonConverter.LogicalTypeConverter() {
            @Override
            JsonNode toJson(Schema schema, Object value) {
                return jsonNodeFactory.textNode(value == null ? null : value.toString());
            }
        });
        // Cassandra CQL types
        logicalTypeConverters.put("cql_varint", new JsonConverter.LogicalTypeConverter() {
            @Override
            JsonNode toJson(Schema schema, Object value) {
                if (!(value instanceof byte[])) {
                    throw new IllegalArgumentException("Invalid type for cql_varint, expected byte[] but was "
                            + value.getClass());
                }
                return jsonNodeFactory.numberNode(new BigInteger((byte[]) value));
            }
        });

        logicalTypeConverters.put("cql_decimal", new JsonConverter.LogicalTypeConverter() {
            @Override
            JsonNode toJson(Schema schema, Object value) {
                if (value == null) {
                    return jsonNodeFactory.nullNode();
                }
                checkTypeAndNotNull(value, "cql_decimal", GenericRecord.class);
                GenericRecord record = (GenericRecord) value;
                Object bigint = record.get("bigint");
                checkTypeAndNotNull(bigint, "cql_decimal - bigint", byte[].class);
                Object scale = record.get("scale");
                checkTypeAndNotNull(scale, "cql_decimal - scale", Integer.class);
                BigInteger asBigint =  new BigInteger((byte[]) record.get("bigint"));
                return jsonNodeFactory.numberNode(new BigDecimal(asBigint, (Integer) scale));
            }
        });
    }

    public static ArrayNode toJsonArray(JsonNode jsonNode, List<String> fields) {
        ArrayNode arrayNode = jsonNodeFactory.arrayNode();
        Iterator<String>  it = jsonNode.fieldNames();
        while (it.hasNext()) {
            String fieldName = it.next();
            if (fields.contains(fieldName)) {
                arrayNode.add(jsonNode.get(fieldName));
            }
        }
        return arrayNode;
    }

}
