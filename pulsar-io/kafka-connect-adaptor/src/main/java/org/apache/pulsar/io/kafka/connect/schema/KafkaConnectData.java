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

package org.apache.pulsar.io.kafka.connect.schema;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericData;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.pulsar.client.api.schema.GenericRecord;

@Slf4j
public class KafkaConnectData {

    private static List<Object> arrayToList(Object nativeObject, Schema kafkaValueSchema) {
        Preconditions.checkArgument(nativeObject.getClass().isArray());
        int length = Array.getLength(nativeObject);
        List<Object> out = new ArrayList<>(length);
        for (int i = 0; i < length; i++) {
            // this handles primitive values too
            Object elem = Array.get(nativeObject, i);
            out.add(getKafkaConnectData(elem, kafkaValueSchema));
        }
        return out;
    }

    @SuppressWarnings("unchecked")
    public static Object getKafkaConnectData(Object nativeObject, Schema kafkaSchema) {
        if (kafkaSchema == null) {
            return nativeObject;
        }

        if (nativeObject == null) {
            return defaultOrThrow(kafkaSchema);
        }

        if (nativeObject instanceof JsonNode) {
            JsonNode node = (JsonNode) nativeObject;
            return jsonAsConnectData(node, kafkaSchema);
        }

        switch (kafkaSchema.type()) {
            case ARRAY:
                 if (nativeObject instanceof List) {
                    List arr = (List) nativeObject;
                    return arr.stream()
                            .map(x -> getKafkaConnectData(x, kafkaSchema.valueSchema()))
                            .collect(Collectors.toList());
                } else if (nativeObject.getClass().isArray()) {
                    return arrayToList(nativeObject, kafkaSchema.valueSchema());
                }
                throw new IllegalStateException("Don't know how to convert " + nativeObject.getClass()
                        + " into kafka ARRAY");
            case MAP:
                if (nativeObject instanceof Map) {
                    Map<Object, Object> map = (Map<Object, Object>) nativeObject;
                    Map<Object, Object> responseMap = new HashMap<>(map.size());
                    for (Map.Entry<Object, Object> kv : map.entrySet()) {
                        Object key = getKafkaConnectData(kv.getKey(), kafkaSchema.keySchema());
                        Object val = getKafkaConnectData(kv.getValue(), kafkaSchema.valueSchema());
                        responseMap.put(key, val);
                    }
                    return responseMap;
                } else if (nativeObject instanceof org.apache.pulsar.common.schema.KeyValue) {
                    org.apache.pulsar.common.schema.KeyValue kv =
                            (org.apache.pulsar.common.schema.KeyValue) nativeObject;
                    Map<Object, Object> responseMap = new HashMap<>();
                    Object key = getKafkaConnectData(kv.getKey(), kafkaSchema.keySchema());
                    Object val = getKafkaConnectData(kv.getValue(), kafkaSchema.valueSchema());
                    responseMap.put(key, val);
                    return responseMap;
                }
                throw new IllegalStateException("Don't know how to convert " + nativeObject.getClass()
                        + " into kafka MAP");
            case STRUCT:
                if (nativeObject instanceof GenericData.Record) {
                    GenericData.Record avroRecord = (GenericData.Record) nativeObject;
                    return avroAsConnectData(avroRecord, kafkaSchema);
                } else if (nativeObject instanceof GenericRecord) {
                    GenericRecord pulsarGenericRecord = (GenericRecord) nativeObject;
                    // Pulsar's GenericRecord
                    if (pulsarGenericRecord.getNativeObject() instanceof JsonNode
                            || pulsarGenericRecord.getNativeObject() instanceof GenericData.Record) {
                        return getKafkaConnectData(pulsarGenericRecord.getNativeObject(), kafkaSchema);
                    }
                    return pulsarGenericRecordAsConnectData(pulsarGenericRecord, kafkaSchema);
                }
                throw new IllegalStateException("Don't know how to convert " + nativeObject.getClass()
                        + "into kafka STRUCT");
            default:
                Preconditions.checkArgument(kafkaSchema.type().isPrimitive(),
                        "Expected primitive schema but got " + kafkaSchema.type());
                return castToKafkaSchema(nativeObject, kafkaSchema);
        }
    }

    public static Object castToKafkaSchema(Object nativeObject, Schema kafkaSchema) {
        if (nativeObject instanceof Number) {
            // This is needed in case
            // jackson decided to fit value into some other type internally
            // (e.g. Double instead of Float).
            // Kafka's ConnectSchema expects exact type
            // https://github.com/apache/kafka/blob/trunk/connect/api/src/main/java/org/apache/kafka/connect/data/ConnectSchema.java#L47-L71
            Number num = (Number) nativeObject;
            switch (kafkaSchema.type()) {
                case INT8:
                    if (!(nativeObject instanceof Byte)) {
                        if (log.isDebugEnabled()) {
                            log.debug("nativeObject of type {} converted to Byte", nativeObject.getClass());
                        }
                        return num.byteValue();
                    }
                    break;
                case INT16:
                    if (!(nativeObject instanceof Short)) {
                        if (log.isDebugEnabled()) {
                            log.debug("nativeObject of type {} converted to Short", nativeObject.getClass());
                        }
                        return num.shortValue();
                    }
                    break;
                case INT32:
                    if (!(nativeObject instanceof Integer)) {
                        if (log.isDebugEnabled()) {
                            log.debug("nativeObject of type {} converted to Integer", nativeObject.getClass());
                        }
                        return num.intValue();
                    }
                    break;
                case INT64:
                    if (!(nativeObject instanceof Long)) {
                        if (log.isDebugEnabled()) {
                            log.debug("nativeObject of type {} converted to Long", nativeObject.getClass());
                        }
                        return num.longValue();
                    }
                    break;
                case FLOAT32:
                    if (!(nativeObject instanceof Float)) {
                        if (log.isDebugEnabled()) {
                            log.debug("nativeObject of type {} converted to Float", nativeObject.getClass());
                        }
                        return num.floatValue();
                    }
                    break;
                case FLOAT64:
                    if (!(nativeObject instanceof Double)) {
                        if (log.isDebugEnabled()) {
                            log.debug("nativeObject of type {} converted to Double", nativeObject.getClass());
                        }
                        return num.doubleValue();
                    }
                    break;
            }
        }

        if (nativeObject instanceof Character) {
            Character ch = (Character) nativeObject;
            if (kafkaSchema.type() == Schema.Type.STRING) {
                return ch.toString();
            }
            return castToKafkaSchema(Character.getNumericValue(ch), kafkaSchema);
        }

        if (kafkaSchema.type() == Schema.Type.STRING && nativeObject instanceof CharSequence) {
            // e.g. org.apache.avro.util.Utf8
            return nativeObject.toString();
        }

        return nativeObject;
    }

    static Object avroAsConnectData(GenericData.Record avroRecord, Schema kafkaSchema) {
        if (kafkaSchema == null) {
            if (avroRecord == null) {
                return null;
            }
            throw new DataException("Don't know how to convert " + avroRecord + " to Connect data (schema is null).");
        }

        Struct struct = new Struct(kafkaSchema);
        for (Field field : kafkaSchema.fields()) {
            struct.put(field, getKafkaConnectData(avroRecord.get(field.name()), field.schema()));
        }
        return struct;
    }

    static Object pulsarGenericRecordAsConnectData(GenericRecord genericRecord, Schema kafkaSchema) {
        if (kafkaSchema == null) {
            if (genericRecord == null) {
                return null;
            }
            throw new DataException("Don't know how to convert " + genericRecord + " to Connect data (schema is null).");
        }

        Struct struct = new Struct(kafkaSchema);
        for (Field field : kafkaSchema.fields()) {
            struct.put(field, getKafkaConnectData(genericRecord.getField(field.name()), field.schema()));
        }
        return struct;
    }

    // with some help of
    // https://github.com/apache/kafka/blob/trunk/connect/json/src/main/java/org/apache/kafka/connect/json/JsonConverter.java
    static Object jsonAsConnectData(JsonNode jsonNode, Schema kafkaSchema) {
        if (kafkaSchema == null) {
            if (jsonNode == null || jsonNode.isNull()) {
                return null;
            }
            throw new DataException("Don't know how to convert " + jsonNode
                + " to Connect data (schema is null).");
        }

        if (jsonNode == null || jsonNode.isNull()) {
            return defaultOrThrow(kafkaSchema);
        }

        switch (kafkaSchema.type()) {
            case INT8:
                Preconditions.checkArgument(jsonNode.isNumber());
                return (byte) jsonNode.shortValue();
            case INT16:
                Preconditions.checkArgument(jsonNode.isNumber());
                return jsonNode.shortValue();
            case INT32:
                if (jsonNode.isTextual() && jsonNode.textValue().length() == 1) {
                    // char encoded as String instead of Integer
                    return Character.getNumericValue(jsonNode.textValue().charAt(0));
                }
                Preconditions.checkArgument(jsonNode.isNumber());
                return jsonNode.intValue();
            case INT64:
                Preconditions.checkArgument(jsonNode.isNumber());
                return  jsonNode.longValue();
            case FLOAT32:
                Preconditions.checkArgument(jsonNode.isNumber());
                return jsonNode.floatValue();
            case FLOAT64:
                Preconditions.checkArgument(jsonNode.isNumber());
                return jsonNode.doubleValue();
            case BOOLEAN:
                Preconditions.checkArgument(jsonNode.isBoolean());
                return jsonNode.booleanValue();
            case STRING:
                Preconditions.checkArgument(jsonNode.isTextual());
                return jsonNode.textValue();
            case BYTES:
                Preconditions.checkArgument(jsonNode.isBinary());
                try {
                    return jsonNode.binaryValue();
                } catch (IOException e) {
                    throw new DataException("Cannot get binary value for " + jsonNode + " with schema " + kafkaSchema);
                }
            case ARRAY:
                if (jsonNode.isTextual() && kafkaSchema.valueSchema().type() == Schema.Type.INT32) {
                    // char[] encoded as String in json
                    List<Object> list = new ArrayList<>();
                    for (char ch: jsonNode.textValue().toCharArray()) {
                        list.add(Character.getNumericValue(ch));
                    }
                    return list;
                }

                Preconditions.checkArgument(jsonNode.isArray(), "jsonNode has to be an array");
                List<Object> list = new ArrayList<>();
                for (Iterator<JsonNode> it = jsonNode.elements(); it.hasNext();) {
                    list.add(jsonAsConnectData(it.next(), kafkaSchema.valueSchema()));
                }
                return list;
            case MAP:
                Preconditions.checkArgument(jsonNode.isObject(), "jsonNode has to be an Object node");
                Preconditions.checkArgument(kafkaSchema.keySchema().type() == Schema.Type.STRING,
                        "kafka schema for json map is expected to be STRING");
                Map<String, Object> map = new HashMap<>();
                for (Iterator<Map.Entry<String, JsonNode>> it = jsonNode.fields(); it.hasNext(); ) {
                    Map.Entry<String, JsonNode> elem = it.next();
                    map.put(elem.getKey(),
                            jsonAsConnectData(elem.getValue(), kafkaSchema.valueSchema()));
                }
                return map;
            case STRUCT:
                Struct struct = new Struct(kafkaSchema);
                for (Field field: kafkaSchema.fields()) {
                    struct.put(field, jsonAsConnectData(jsonNode.get(field.name()), field.schema()));
                }
                return struct;
            default:
                throw new DataException("Unknown schema type " + kafkaSchema.type());
        }
    }

    private static Object defaultOrThrow(Schema kafkaSchema) {
        if (kafkaSchema.defaultValue() != null) {
            return kafkaSchema.defaultValue(); // any logical type conversions should already have been applied
        }
        if (kafkaSchema.isOptional()) {
            return null;
        }
        throw new DataException("Invalid null value for required " + kafkaSchema.type() + " field");
    }
}
