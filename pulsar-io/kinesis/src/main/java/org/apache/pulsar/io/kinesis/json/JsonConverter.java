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
package org.apache.pulsar.io.kinesis.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.avro.Conversion;
import org.apache.avro.Conversions;
import org.apache.avro.Schema;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;

/**
 * Convert an AVRO GenericRecord to a JsonNode.
 */
public class JsonConverter {

    private static final Map<String, LogicalTypeConverter<?>> logicalTypeConverters = new HashMap<>();
    private static final JsonNodeFactory jsonNodeFactory = JsonNodeFactory.withExactBigDecimals(true);

    public static JsonNode toJson(GenericRecord genericRecord, boolean convertBytesToString) {
        if (genericRecord == null) {
            return null;
        }
        ObjectNode objectNode = jsonNodeFactory.objectNode();
        for (Schema.Field field : genericRecord.getSchema().getFields()) {
            objectNode.set(field.name(), toJson(field.schema(), genericRecord.get(field.name()), convertBytesToString));
        }
        return objectNode;
    }

    public static JsonNode toJson(Schema schema, Object value, boolean convertBytesToString) {
        if (schema.getLogicalType() != null && logicalTypeConverters.containsKey(schema.getLogicalType().getName())) {
            return logicalTypeConverters.get(schema.getLogicalType().getName()).toJson(schema, value);
        }
        if (value == null) {
            return jsonNodeFactory.nullNode();
        }
        switch(schema.getType()) {
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
                byte[] bytes = new byte[((ByteBuffer) value).remaining()];
                ((ByteBuffer) value).get(bytes);
                // Workaround for https://github.com/wnameless/json-flattener/issues/91
                if (convertBytesToString) {
                    return jsonNodeFactory.textNode(Base64.getEncoder().encodeToString(bytes));
                }
                return jsonNodeFactory.binaryNode(bytes);
            case FIXED:
                // Workaround for https://github.com/wnameless/json-flattener/issues/91
                if (convertBytesToString) {
                    return jsonNodeFactory.textNode(Base64.getEncoder().encodeToString(((GenericFixed) value).bytes()));
                }
                return jsonNodeFactory.binaryNode(((GenericFixed) value).bytes());
            case ENUM: // GenericEnumSymbol
            case STRING:
                return jsonNodeFactory.textNode(value.toString()); // can be a String or org.apache.avro.util.Utf8
            case ARRAY: {
                Schema elementSchema = schema.getElementType();
                ArrayNode arrayNode = jsonNodeFactory.arrayNode();
                Object[] iterable;
                if (value instanceof GenericData.AbstractArray) {
                    iterable = ((GenericData.AbstractArray) value).toArray();
                } else {
                    iterable = (Object[]) value;
                }
                for (Object elem : iterable) {
                    JsonNode fieldValue = toJson(elementSchema, elem, convertBytesToString);
                    arrayNode.add(fieldValue);
                }
                return arrayNode;
            }
            case MAP: {
                Map<Object, Object> map = (Map<Object, Object>) value;
                ObjectNode objectNode = jsonNodeFactory.objectNode();
                for (Map.Entry<Object, Object> entry : map.entrySet()) {
                    JsonNode jsonNode = toJson(schema.getValueType(), entry.getValue(), convertBytesToString);
                    // can be a String or org.apache.avro.util.Utf8
                    final String entryKey = entry.getKey() == null ? null : entry.getKey().toString();
                    objectNode.set(entryKey, jsonNode);
                }
                return objectNode;
            }
            case RECORD:
                return toJson((GenericRecord) value, convertBytesToString);
            case UNION:
                for (Schema s : schema.getTypes()) {
                    if (s.getType() == Schema.Type.NULL) {
                        continue;
                    }
                    return toJson(s, value, convertBytesToString);
                }
                // this case should not happen
                return jsonNodeFactory.textNode(value.toString());
            default:
                throw new UnsupportedOperationException("Unknown AVRO schema type=" + schema.getType());
        }
    }

    abstract static class LogicalTypeConverter<T> {
        final Conversion<T> conversion;

        public LogicalTypeConverter(Conversion<T> conversion) {
            this.conversion = conversion;
        }

        abstract JsonNode toJson(Schema schema, Object value);
    }

    static {
        logicalTypeConverters.put("decimal", new LogicalTypeConverter<BigDecimal>(
                new Conversions.DecimalConversion()) {
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
        logicalTypeConverters.put("date", new LogicalTypeConverter<LocalDate>(
                new TimeConversions.DateConversion()) {
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
        logicalTypeConverters.put("time-millis", new LogicalTypeConverter<LocalTime>(
                new TimeConversions.TimeMillisConversion()) {
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
        logicalTypeConverters.put("time-micros", new LogicalTypeConverter<LocalTime>(
                new TimeConversions.TimeMicrosConversion()) {
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
        logicalTypeConverters.put("timestamp-millis", new LogicalTypeConverter<Instant>(
                new TimeConversions.TimestampMillisConversion()) {
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
        logicalTypeConverters.put("timestamp-micros", new LogicalTypeConverter<Instant>(
                new TimeConversions.TimestampMicrosConversion()) {
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
        logicalTypeConverters.put("uuid", new LogicalTypeConverter<UUID>(
                new Conversions.UUIDConversion()) {
            @Override
            JsonNode toJson(Schema schema, Object value) {
                return jsonNodeFactory.textNode(value == null ? null : value.toString());
            }
        });
    }

}
