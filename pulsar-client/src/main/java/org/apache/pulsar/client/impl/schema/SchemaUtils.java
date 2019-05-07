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
package org.apache.pulsar.client.impl.schema;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pulsar.common.schema.SchemaType;

/**
 * Utils for schemas.
 */
final class SchemaUtils {

    private SchemaUtils() {}

    /**
     * Keeps a map between {@link SchemaType} to a list of java classes that can be used to represent them.
     */
    private static final Map<SchemaType, List<Class>> SCHEMA_TYPE_CLASSES = new HashMap<>();

    /**
     * Maps the java classes to the corresponding {@link SchemaType}.
     */
    private static final Map<Class<?>, SchemaType> JAVA_CLASS_SCHEMA_TYPES = new HashMap<>();

    static {
        // int8
        SCHEMA_TYPE_CLASSES.put(
                SchemaType.INT8,
                Arrays.asList(Byte.class));
        // int16
        SCHEMA_TYPE_CLASSES.put(
                SchemaType.INT16,
                Arrays.asList(Short.class));
        // int32
        SCHEMA_TYPE_CLASSES.put(
                SchemaType.INT32,
                Arrays.asList(Integer.class));
        // int64
        SCHEMA_TYPE_CLASSES.put(
                SchemaType.INT64,
                Arrays.asList(Long.class));
        // float
        SCHEMA_TYPE_CLASSES.put(
                SchemaType.FLOAT,
                Arrays.asList(Float.class));
        // double
        SCHEMA_TYPE_CLASSES.put(
                SchemaType.DOUBLE,
                Arrays.asList(Double.class));
        // boolean
        SCHEMA_TYPE_CLASSES.put(
                SchemaType.BOOLEAN,
                Arrays.asList(Boolean.class));
        // string
        SCHEMA_TYPE_CLASSES.put(
                SchemaType.STRING,
                Arrays.asList(String.class));
        // bytes
        SCHEMA_TYPE_CLASSES.put(
                SchemaType.BYTES,
                Arrays.asList(byte[].class, ByteBuffer.class, ByteBuf.class));
        // build the reverse mapping
        SCHEMA_TYPE_CLASSES.forEach(
                (type, classes) -> classes.forEach(clz -> JAVA_CLASS_SCHEMA_TYPES.put(clz, type)));
    }

    public static void validateFieldSchema(String name,
                                           SchemaType type,
                                           Object val) {
        if (null == val) {
            return;
        }

        List<Class> expectedClasses = SCHEMA_TYPE_CLASSES.get(type);

        if (null == expectedClasses) {
            throw new RuntimeException("Invalid Java object for schema type " + type
                    + " : " + val.getClass()
                    + " for field : \"" + name + "\"");
        }

        boolean foundMatch = false;
        for (Class<?> expectedCls : expectedClasses) {
            if (expectedCls.isInstance(val)) {
                foundMatch = true;
                break;
            }
        }

        if (!foundMatch) {
            throw new RuntimeException("Invalid Java object for schema type " + type
                    + " : " + val.getClass()
                    + " for field : \"" + name + "\"");
        }

        switch (type) {
            case INT8:
            case INT16:
            case PROTOBUF:
            case AVRO:
            case AUTO_CONSUME:
            case AUTO_PUBLISH:
            case AUTO:
            case KEY_VALUE:
            case JSON:
            case NONE:
                throw new RuntimeException("Currently " + type.name() + " is not supported");
            default:
                break;
        }
    }

    public static Object toAvroObject(Object value) {
        if (value != null) {
            if (value instanceof ByteBuffer) {
                ByteBuffer bb = (ByteBuffer) value;
                byte[] bytes = new byte[bb.remaining()];
                bb.duplicate().get(bytes);
                return bytes;
            } else if (value instanceof ByteBuf) {
                return ByteBufUtil.getBytes((ByteBuf) value);
            } else {
                return value;
            }
        } else {
            return null;
        }
    }

}
