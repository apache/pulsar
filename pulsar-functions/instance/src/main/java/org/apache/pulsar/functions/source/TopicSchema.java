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
package org.apache.pulsar.functions.source;

import io.netty.buffer.ByteBuf;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.schema.AvroSchema;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.apache.pulsar.client.impl.schema.ProtobufSchema;
import org.apache.pulsar.common.functions.ConsumerConfig;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.api.SerDe;
import org.apache.pulsar.functions.instance.InstanceUtils;

@Slf4j
public class TopicSchema {

    public static final String JSR_310_CONVERSION_ENABLED = "jsr310ConversionEnabled";
    public static final String ALWAYS_ALLOW_NULL = "alwaysAllowNull";
    private final Map<String, Schema<?>> cachedSchemas = new HashMap<>();
    private final PulsarClient client;

    public TopicSchema(PulsarClient client) {
        this.client = client;
    }

    /**
     * If there is no other information available, use JSON as default schema type.
     */
    private static final SchemaType DEFAULT_SCHEMA_TYPE = SchemaType.JSON;

    public static final String DEFAULT_SERDE = "org.apache.pulsar.functions.api.utils.DefaultSerDe";

    public Schema<?> getSchema(String topic, Object object, String schemaTypeOrClassName, boolean input) {
        return getSchema(topic, object.getClass(), schemaTypeOrClassName, input);
    }

    public Schema<?> getSchema(String topic, Class<?> clazz, String schemaTypeOrClassName, boolean input) {
        return cachedSchemas.computeIfAbsent(topic, t -> newSchemaInstance(topic, clazz, schemaTypeOrClassName, input));
    }

    public Schema<?> getSchema(String topic, Class<?> clazz, ConsumerConfig conf, boolean input) {
        return cachedSchemas.computeIfAbsent(topic, t -> newSchemaInstance(topic, clazz, conf, input));
    }

    public Schema<?> getSchema(String topic, Class<?> clazz, Optional<SchemaType> schemaType) {
        return cachedSchemas.computeIfAbsent(topic, key -> {
            // If schema type was not provided, try to get it from schema registry, or fallback to default types
            SchemaType type = schemaType.orElse(getSchemaTypeOrDefault(topic, clazz));
            return newSchemaInstance(clazz, type);
        });
    }

    public Schema<?> getSchema(String topic, Class<?> clazz, SchemaType schemaType) {
        return cachedSchemas.computeIfAbsent(topic, t -> newSchemaInstance(clazz, schemaType));
    }

    public Schema<?> getSchema(String topic, Class<?> clazz, String schemaTypeOrClassName, boolean input, ClassLoader classLoader) {
        return cachedSchemas.computeIfAbsent(topic, t -> newSchemaInstance(topic, clazz, schemaTypeOrClassName, input, classLoader));
    }

    public Schema<?> getSchema(String topic, Class<?> clazz, ConsumerConfig conf, boolean input, ClassLoader classLoader) {
        return cachedSchemas.computeIfAbsent(topic, t -> newSchemaInstance(topic, clazz, conf, input, classLoader));
    }

    public Schema<?> getSchema(String topic, Class<?> clazz, String schemaTypeOrClassName, boolean input, ClassLoader classLoader, Type kvSchemaGenericType) {
        return cachedSchemas.computeIfAbsent(topic, t -> newSchemaInstance(topic, clazz, new ConsumerConfig(schemaTypeOrClassName), input, classLoader, kvSchemaGenericType));
    }


    /**
     * If the topic is already created, we should be able to fetch the schema type (avro, json, ...)
     */
    private SchemaType getSchemaTypeOrDefault(String topic, Class<?> clazz) {
        if (GenericRecord.class.isAssignableFrom(clazz)) {
            return SchemaType.AUTO_CONSUME;
        } else if (byte[].class.equals(clazz)
                || ByteBuf.class.equals(clazz)
                || ByteBuffer.class.equals(clazz)) {
            // if function uses bytes, we should ignore
            return SchemaType.NONE;
        } else {
            Optional<SchemaInfo> schema = ((PulsarClientImpl) client).getSchema(topic).join();
            if (schema.isPresent()) {
                if (schema.get().getType() == SchemaType.NONE) {
                    return getDefaultSchemaType(clazz);
                } else {
                    return schema.get().getType();
                }
            } else {
                return getDefaultSchemaType(clazz);
            }
        }
    }

    private static SchemaType getDefaultSchemaType(Class<?> clazz) {
        if (byte[].class.equals(clazz)
            || ByteBuf.class.equals(clazz)
            || ByteBuffer.class.equals(clazz)) {
            return SchemaType.NONE;
        } else if (GenericRecord.class.isAssignableFrom(clazz)) {
            // the function is taking generic record, so we do auto schema detection
            return SchemaType.AUTO_CONSUME;
        } else if (String.class.equals(clazz)) {
            // If type is String, then we use schema type string, otherwise we fallback on default schema
            return SchemaType.STRING;
        } else if (isProtobufClass(clazz)) {
            return SchemaType.PROTOBUF;
        } else if (KeyValue.class.equals(clazz)) {
            return SchemaType.KEY_VALUE;
        } else {
            return DEFAULT_SCHEMA_TYPE;
        }
    }

    public static Schema getDefaultSchema(Class<?> clazz) {
        if (Byte[].class.equals(clazz)) {
            return Schema.BYTES;
        } else if (ByteBuffer.class.equals(clazz)) {
            return Schema.BYTEBUFFER;
        } else if (String.class.equals(clazz)) {
            return Schema.STRING;
        } else if (Byte.class.equals(clazz)) {
            return Schema.INT8;
        } else if (Short.class.equals(clazz)) {
            return Schema.INT16;
        } else if (Integer.class.equals(clazz)) {
            return Schema.INT32;
        } else if (Long.class.equals(clazz)) {
            return Schema.INT64;
        } else if (Boolean.class.equals(clazz)) {
            return Schema.BOOL;
        } else if (Float.class.equals(clazz)) {
            return Schema.FLOAT;
        } else if (Double.class.equals(clazz)) {
            return Schema.DOUBLE;
        } else if (Date.class.equals(clazz)) {
            return Schema.DATE;
        } else if (Time.class.equals(clazz)) {
            return Schema.TIME;
        } else if (Timestamp.class.equals(clazz)) {
            return Schema.TIMESTAMP;
        }
        throw new IllegalArgumentException("Schema class type is incorrect");
    }

    @SuppressWarnings("unchecked")
    private static <T> Schema<T> newSchemaInstance(Class<T> clazz, SchemaType type) {
        return newSchemaInstance(clazz, type, new ConsumerConfig());
    }

    private static <T> Schema<T> newSchemaInstance(Class<T> clazz, SchemaType type, ConsumerConfig conf) {
        return newSchemaInstance(clazz, type, null, null, conf);
    }

    @SuppressWarnings("unchecked")
    private static <T> Schema<T> newSchemaInstance(Class<T> clazz, SchemaType type, Type kvSchemaGenericType, ClassLoader classLoader, ConsumerConfig conf) {
        switch (type) {
        case NONE:
            return (Schema<T>) Schema.BYTES;

        case AUTO_CONSUME:
        case AUTO:
            return (Schema<T>) Schema.AUTO_CONSUME();

        case STRING:
            return (Schema<T>) Schema.STRING;

        case AVRO:
            return AvroSchema.of(SchemaDefinition.<T>builder()
                    .withProperties(new HashMap<>(conf.getSchemaProperties()))
                    .withPojo(clazz).build());

        case JSON:
            return JSONSchema.of(SchemaDefinition.<T>builder().withPojo(clazz).build());

        case KEY_VALUE:
            return buildKeyValueSchema(kvSchemaGenericType);

        case PROTOBUF:
            return ProtobufSchema.ofGenericClass(clazz, new HashMap<>());

        default:
            throw new RuntimeException("Unsupported schema type" + type);
        }
    }

    /**
     * According to the generic parameters of function, get the corresponding schema.
     * @param kvSchemaGenericType
     * @return
     */
    private static Schema buildKeyValueSchema(Type kvSchemaGenericType) {
        if(!(kvSchemaGenericType instanceof ParameterizedType)){
            return Schema.KV_BYTES();
        }
        return doBuildKvSchema(((ParameterizedType) kvSchemaGenericType).getActualTypeArguments());
    }

    /**
     * Traverse keyvalue recursively and generate schema
     * @param kvType
     * @return
     */
    private static Schema doBuildKvSchema(Type[] kvType) {
        if (kvType == null || kvType.length < 2) {
            return Schema.KV_BYTES();
        }
        if (isInstanceOfKeyValue(kvType[0]) && isInstanceOfKeyValue(kvType[1])) {
            return Schema.KeyValue(doBuildKvSchema(((ParameterizedType) kvType[0]).getActualTypeArguments()), doBuildKvSchema(((ParameterizedType) kvType[1]).getActualTypeArguments()));
        } else if (isInstanceOfKeyValue(kvType[0])) {
            return Schema.KeyValue(doBuildKvSchema(((ParameterizedType) kvType[0]).getActualTypeArguments()), getDefaultSchema((Class<?>) kvType[1]));
        } else if (isInstanceOfKeyValue(kvType[1])) {
            return Schema.KeyValue(getDefaultSchema((Class<?>) kvType[0]), doBuildKvSchema(((ParameterizedType) kvType[1]).getActualTypeArguments()));
        } else {
            return Schema.KeyValue(convertTypeToClass(kvType[0]), convertTypeToClass(kvType[1]));
        }
    }

    private static Class<?> convertTypeToClass(Type type) {
        if (type instanceof ParameterizedType) {
            return (Class<?>) ((ParameterizedType) type).getRawType();
        }
        return (Class<?>) type;
    }

    private static boolean isInstanceOfKeyValue(Type type) {
        if (!(type instanceof ParameterizedType)) {
            return false;
        }
        return ((ParameterizedType) type).getRawType() == KeyValue.class;
    }

    private static boolean isProtobufClass(Class<?> pojoClazz) {
        try {
            Class<?> protobufBaseClass = Class.forName("com.google.protobuf.GeneratedMessageV3");
            return protobufBaseClass.isAssignableFrom(pojoClazz);
        } catch (ClassNotFoundException | NoClassDefFoundError e) {
            // If function does not have protobuf in classpath then it cannot be protobuf
            return false;
        }
    }
    @SuppressWarnings("unchecked")
    private <T> Schema<T> newSchemaInstance(String topic, Class<T> clazz, String schemaTypeOrClassName, boolean input, ClassLoader classLoader){
        return newSchemaInstance(topic, clazz, new ConsumerConfig(schemaTypeOrClassName), input, classLoader);
    }

    @SuppressWarnings("unchecked")
    private <T> Schema<T> newSchemaInstance(String topic, Class<T> clazz, ConsumerConfig conf, boolean input, ClassLoader classLoader) {
        return newSchemaInstance(topic, clazz, conf, input, classLoader, null);
    }

    @SuppressWarnings("unchecked")
    private <T> Schema<T> newSchemaInstance(String topic, Class<T> clazz, ConsumerConfig conf, boolean input,
                                            ClassLoader classLoader, Type kvSchemaGenericType) {
        // The schemaTypeOrClassName can represent multiple thing, either a schema type, a schema class name or a ser-de
        // class name.
        String schemaTypeOrClassName = conf.getSchemaType();
        if (StringUtils.isEmpty(schemaTypeOrClassName) || DEFAULT_SERDE.equals(schemaTypeOrClassName)) {
            // No preferred schema was provided, auto-discover schema or fallback to defaults
            return newSchemaInstance(clazz, getSchemaTypeOrDefault(topic, clazz), kvSchemaGenericType, classLoader, null);
        }

        SchemaType schemaType = null;
        try {
            schemaType = SchemaType.valueOf(schemaTypeOrClassName.toUpperCase());
        } catch (IllegalArgumentException e) {
            // schemaType is not referring to builtin type
        }

        if (schemaType != null) {
            // The parameter passed was indeed a valid builtin schema type
            return newSchemaInstance(clazz, schemaType, conf);
        }

        // At this point, the string can represent either a schema or serde class name. Create an instance and
        // check if it complies with either interface

        // First try with Schema
        try {
            return (Schema<T>) InstanceUtils.initializeCustomSchema(schemaTypeOrClassName,
                    classLoader, clazz, input);
        } catch (Throwable t) {
            // Now try with Serde or just fail
            SerDe<T> serDe = (SerDe<T>) InstanceUtils.initializeSerDe(schemaTypeOrClassName,
                    classLoader, clazz, input);
            return new SerDeSchema<>(serDe);
        }
    }

    @SuppressWarnings("unchecked")
    private <T> Schema<T> newSchemaInstance(String topic, Class<T> clazz, String schemaTypeOrClassName, boolean input) {
        return newSchemaInstance(topic, clazz, new ConsumerConfig(schemaTypeOrClassName), input, Thread.currentThread().getContextClassLoader());
    }

    @SuppressWarnings("unchecked")
    private <T> Schema<T> newSchemaInstance(String topic, Class<T> clazz, ConsumerConfig conf, boolean input) {
        return newSchemaInstance(topic, clazz, conf, input, Thread.currentThread().getContextClassLoader());
    }
}
