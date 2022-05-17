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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericData;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.pulsar.client.api.schema.GenericRecord;

@Slf4j
public class KafkaConnectData {
    public static Object getKafkaConnectData(Object nativeObject, Schema kafkaSchema) {
        if (kafkaSchema == null) {
            return nativeObject;
        }

        if (nativeObject instanceof JsonNode) {
            JsonNode node = (JsonNode) nativeObject;
            return jsonAsConnectData(node, kafkaSchema);
        } else if (nativeObject instanceof GenericData.Record) {
            GenericData.Record avroRecord = (GenericData.Record) nativeObject;
            return avroAsConnectData(avroRecord, kafkaSchema);
        } else if (nativeObject instanceof GenericRecord) {
            // Pulsar's GenericRecord
            GenericRecord pulsarGenericRecord = (GenericRecord) nativeObject;
            return pulsarGenericRecordAsConnectData(pulsarGenericRecord, kafkaSchema);
        }

        return castToKafkaSchema(nativeObject, kafkaSchema);
    }

    public static Object castToKafkaSchema(Object nativeObject, Schema kafkaSchema) {
        if (nativeObject == null) {
            return defaultOrThrow(kafkaSchema);
        }

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
            switch (jsonNode.getNodeType()) {
                case BINARY:
                    try {
                        return jsonNode.binaryValue();
                    } catch (IOException e) {
                        throw new DataException("Cannot get binary value for " + jsonNode);
                    }
                case BOOLEAN:
                    return jsonNode.booleanValue();
                case NUMBER:
                    return jsonNode.doubleValue();
                case STRING:
                    return jsonNode.textValue();
                default:
                    throw new DataException("Don't know how to convert " + jsonNode
                            + " to Connect data (schema is null).");
            }
        }

        if (jsonNode == null || jsonNode.isNull()) {
            return defaultOrThrow(kafkaSchema);
        }

        switch (kafkaSchema.type()) {
            case INT8:
                return (byte) jsonNode.shortValue();
            case INT16:
                return jsonNode.shortValue();
            case INT32:
                return jsonNode.intValue();
            case INT64:
                return  jsonNode.longValue();
            case FLOAT32:
                return jsonNode.floatValue();
            case FLOAT64:
                return jsonNode.doubleValue();
            case BOOLEAN:
                return jsonNode.booleanValue();
            case STRING:
                return jsonNode.textValue();
            case BYTES:
                try {
                    return jsonNode.binaryValue();
                } catch (IOException e) {
                    throw new DataException("Cannot get binary value for " + jsonNode + " with schema " + kafkaSchema);
                }
            case ARRAY:
                List<Object> list = new ArrayList<>();
                Preconditions.checkArgument(jsonNode.isArray(), "jsonNode has to be an array");
                for (Iterator<JsonNode> it = jsonNode.elements(); it.hasNext(); ) {
                    list.add(jsonAsConnectData(it.next(), kafkaSchema.valueSchema()));
                }
                return list;
            case MAP:
                Map<String, Object> map = new HashMap<>();
                for (Iterator<Map.Entry<String, JsonNode>> it = jsonNode.fields(); it.hasNext(); ) {
                    Map.Entry<String, JsonNode> elem = it.next();
                    map.put(elem.getKey(), jsonAsConnectData(elem.getValue(), kafkaSchema.valueSchema()));
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
