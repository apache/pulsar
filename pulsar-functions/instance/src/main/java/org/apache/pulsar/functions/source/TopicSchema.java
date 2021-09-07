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
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.schema.AvroSchema;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.apache.pulsar.client.impl.schema.ProtobufNativeSchema;
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
            SchemaType type = schemaType.orElseGet(() -> getSchemaTypeOrDefault(topic, clazz));
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


    /**
     * If the topic is already created, we should be able to fetch the schema type (avro, json, ...)
     */
    private SchemaType getSchemaTypeOrDefault(String topic, Class<?> clazz) {
        if (GenericObject.class.isAssignableFrom(clazz)) {
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
        } else if (GenericObject.class.isAssignableFrom(clazz)) {
            // the function is taking generic record/object, so we do auto schema detection
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

    @SuppressWarnings("unchecked")
    private static <T> Schema<T> newSchemaInstance(Class<T> clazz, SchemaType type) {
        return newSchemaInstance(clazz, type, new ConsumerConfig());
    }

    private static <T> Schema<T> newSchemaInstance(Class<T> clazz, SchemaType type, ConsumerConfig conf) {
        switch (type) {
        case NONE:
            if (ByteBuffer.class.isAssignableFrom(clazz)) {
                return (Schema<T>) Schema.BYTEBUFFER;
            } else {
                return (Schema<T>) Schema.BYTES;
            }

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
            return (Schema<T>)Schema.KV_BYTES();

        case PROTOBUF:
            return ProtobufSchema.ofGenericClass(clazz, new HashMap<>());

        case PROTOBUF_NATIVE:
            return ProtobufNativeSchema.ofGenericClass(clazz, new HashMap<>());

        case AUTO_PUBLISH:
            return (Schema<T>) Schema.AUTO_PRODUCE_BYTES();

        default:
            throw new RuntimeException("Unsupported schema type" + type);
        }
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
        // The schemaTypeOrClassName can represent multiple thing, either a schema type, a schema class name or a ser-de
        // class name.
        String schemaTypeOrClassName = conf.getSchemaType();
        if (StringUtils.isEmpty(schemaTypeOrClassName) || DEFAULT_SERDE.equals(schemaTypeOrClassName)) {
            // No preferred schema was provided, auto-discover schema or fallback to defaults
            return newSchemaInstance(clazz, getSchemaTypeOrDefault(topic, clazz));
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
