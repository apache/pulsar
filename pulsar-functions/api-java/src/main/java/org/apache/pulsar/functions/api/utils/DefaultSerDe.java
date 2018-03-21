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

package org.apache.pulsar.functions.api.utils;

import org.apache.pulsar.functions.api.SerDe;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Simplest form of SerDe.
 */
public class DefaultSerDe implements SerDe<Object> {

    private static final Set<Class> supportedInputTypes = new HashSet<>(Arrays.asList(
            byte[].class,
            Integer.class,
            Double.class,
            Long.class,
            String.class,
            Short.class,
            Byte.class,
            Float.class
    ));
    private final Class type;

    public DefaultSerDe(Class type) {
        this.type = type;
    }

    @Override
    public Object deserialize(byte[] input) {
        String data = new String(input, StandardCharsets.UTF_8);

        if (type.equals(byte[].class)) {
            return input;
        } else if (type.equals(Integer.class)) {
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
        } else {
            throw new RuntimeException("Unknown type " + type);
        }
    }

    @Override
    public byte[] serialize(Object input) {
        if (type.equals(byte[].class)) {
            return (byte[]) input;
        } else if (type.equals(Integer.class)) {
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
        } else {
            throw new RuntimeException("Unknown type " + type);
        }
    }

    public static boolean IsSupportedType(Class typ) {
        return supportedInputTypes.contains(typ);
    }
}
