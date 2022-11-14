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
import org.apache.avro.generic.GenericData;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class KafkaConnectData {
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
        } else if (nativeObject instanceof GenericData.Record) {
            GenericData.Record avroRecord = (GenericData.Record) nativeObject;
            return avroAsConnectData(avroRecord, kafkaSchema);
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
                    jsonNode.doubleValue();
                case STRING:
                    jsonNode.textValue();
                default:
                    throw new DataException("Don't know how to convert " + jsonNode +
                            " to Connect data (schema is null).");
            }
        }

        if (jsonNode == null || jsonNode.isNull()) {
            return defaultOrThrow(kafkaSchema);
        }

        switch (kafkaSchema.type()) {
            case INT8:
                return (byte)jsonNode.shortValue();
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
