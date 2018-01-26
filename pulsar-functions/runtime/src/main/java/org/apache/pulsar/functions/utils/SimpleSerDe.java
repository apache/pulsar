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

package org.apache.pulsar.functions.utils;

import com.google.common.collect.Sets;
import org.apache.pulsar.functions.api.SerDe;

import java.io.*;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Simplest form of SerDe.
 */
public class SimpleSerDe implements SerDe {

    private static final Set<Type> supportedInputTypes = Sets.newHashSet(
            Integer.class,
            Double.class,
            Long.class,
            String.class,
            Short.class,
            Byte.class,
            Float.class,
            Map.class,
            List.class,
            Object.class
    );
    private Type type;
    private boolean ser;

    public SimpleSerDe(Type type, boolean ser) {
        this.type = type;
        this.ser = ser;
        verifySupportedType(type, ser);
    }

    @Override
    public Object deserialize(byte[] input) {
        if (ser) {
            throw new RuntimeException("Serializer function cannot deserialize");
        }
        String data = new String(input, StandardCharsets.UTF_8);
        if (type.equals(Integer.class)) {
            return Integer.valueOf(data);
        } else if (type.equals(Double.class)) {
            return Double.valueOf(data);
        } else if (type.equals(Long.class)) {
            return Long.valueOf(data);
        } else if (type.equals(String.class)) {
            return data;
        } else if (type.equals(Short.class)) {
            return Short.valueOf(data);
        } else if (type.equals(Byte.class)) {
            return Byte.decode(data);
        } else if (type.equals(Float.class)) {
            return Float.valueOf(data);
        } else if (type.equals(Map.class)) {
            try {
                ByteArrayInputStream byteIn = new ByteArrayInputStream(input);
                ObjectInputStream in = new ObjectInputStream(byteIn);
                return (Map<Object, Object>) in.readObject();
            } catch (Exception ex) {
                return null;
            }
        } else if (type.equals(List.class)) {
            try {
                ByteArrayInputStream byteIn = new ByteArrayInputStream(input);
                ObjectInputStream in = new ObjectInputStream(byteIn);
                return (List<Object>) in.readObject();
            } catch (Exception ex) {
                return null;
            }
        } else {
            throw new RuntimeException("Unknown type " + type);
        }
    }

    @Override
    public byte[] serialize(Object input) {
        if (!ser) {
            throw new RuntimeException("DeSerializer function cannot serialize");
        }
        if (type.equals(Integer.class)) {
            return ((Integer) input).toString().getBytes(StandardCharsets.UTF_8);
        } else if (type.equals(Double.class)) {
            return ((Double) input).toString().getBytes(StandardCharsets.UTF_8);
        } else if (type.equals(Long.class)) {
            return ((Long) input).toString().getBytes(StandardCharsets.UTF_8);
        } else if (type.equals(String.class)) {
            return ((String) input).getBytes(StandardCharsets.UTF_8);
        } else if (type.equals(Short.class)) {
            return ((Short) input).toString().getBytes(StandardCharsets.UTF_8);
        } else if (type.equals(Byte.class)) {
            return ((Byte) input).toString().getBytes(StandardCharsets.UTF_8);
        } else if (type.equals(Float.class)) {
            return ((Float) input).toString().getBytes(StandardCharsets.UTF_8);
        } else if (type.equals(Map.class) || type.equals(List.class)) {
            try {
                ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
                ObjectOutputStream out = new ObjectOutputStream(byteOut);
                out.writeObject(input);
                return byteOut.toByteArray();
            } catch (Exception ex) {
                return null;
            }
        } else {
            throw new RuntimeException("Unknown type " + type);
        }
    }

    public void verifySupportedType(Type type, boolean allowVoid) {
        if (!allowVoid && !supportedInputTypes.contains(type)) {
            throw new RuntimeException("Non Basic types not yet supported: " + type);
        } else if (!(supportedInputTypes.contains(type) || type.equals(Void.class))) {
            throw new RuntimeException("Non Basic types not yet supported: " + type);
        }
    }
}
