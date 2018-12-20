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

import net.jodah.typetools.TypeResolver;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.api.SerDe;

import static org.apache.commons.lang3.StringUtils.isEmpty;

public class ValidatorUtils {
    private static final String DEFAULT_SERDE = "org.apache.pulsar.functions.api.utils.DefaultSerDe";

    public static void validateSchema(String schemaType, Class<?> typeArg, ClassLoader clsLoader,
                                      boolean input) {
        if (isEmpty(schemaType) || getBuiltinSchemaType(schemaType) != null) {
            // If it's empty, we use the default schema and no need to validate
            // If it's built-in, no need to validate
        } else {
            Utils.implementsClass(schemaType, Schema.class, clsLoader);
            validateSchemaType(schemaType, typeArg, clsLoader, input);
        }
    }

    private static SchemaType getBuiltinSchemaType(String schemaTypeOrClassName) {
        try {
            return SchemaType.valueOf(schemaTypeOrClassName.toUpperCase());
        } catch (IllegalArgumentException e) {
            // schemaType is not referring to builtin type
            return null;
        }
    }

    public static void validateSerde(String inputSerializer, Class<?> typeArg, ClassLoader clsLoader,
                                     boolean deser) {
        if (isEmpty(inputSerializer)) return;
        if (inputSerializer.equals(DEFAULT_SERDE)) return;
        try {
            Class<?> serdeClass = Utils.loadClass(inputSerializer, clsLoader);
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException(
                    String.format("The input serialization/deserialization class %s does not exist",
                            inputSerializer));
        }
        Utils.implementsClass(inputSerializer, SerDe.class, clsLoader);

        SerDe serDe = (SerDe) Reflections.createInstance(inputSerializer, clsLoader);
        if (serDe == null) {
            throw new IllegalArgumentException(String.format("The SerDe class %s does not exist",
                    inputSerializer));
        }
        Class<?>[] serDeTypes = TypeResolver.resolveRawArguments(SerDe.class, serDe.getClass());

        // type inheritance information seems to be lost in generic type
        // load the actual type class for verification
        Class<?> fnInputClass;
        Class<?> serdeInputClass;
        try {
            fnInputClass = Class.forName(typeArg.getName(), true, clsLoader);
            serdeInputClass = Class.forName(serDeTypes[0].getName(), true, clsLoader);
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("Failed to load type class", e);
        }

        if (deser) {
            if (!fnInputClass.isAssignableFrom(serdeInputClass)) {
                throw new IllegalArgumentException("Serializer type mismatch " + typeArg + " vs " + serDeTypes[0]);
            }
        } else {
            if (!serdeInputClass.isAssignableFrom(fnInputClass)) {
                throw new IllegalArgumentException("Serializer type mismatch " + typeArg + " vs " + serDeTypes[0]);
            }
        }
    }

    private static void validateSchemaType(String schemaClassName, Class<?> typeArg, ClassLoader clsLoader,
                                           boolean input) {
        Schema<?> schema = (Schema<?>) Reflections.createInstance(schemaClassName, clsLoader);
        if (schema == null) {
            throw new IllegalArgumentException(String.format("The Schema class %s does not exist",
                    schemaClassName));
        }
        Class<?>[] schemaTypes = TypeResolver.resolveRawArguments(Schema.class, schema.getClass());

        // type inheritance information seems to be lost in generic type
        // load the actual type class for verification
        Class<?> fnInputClass;
        Class<?> schemaInputClass;
        try {
            fnInputClass = Class.forName(typeArg.getName(), true, clsLoader);
            schemaInputClass = Class.forName(schemaTypes[0].getName(), true, clsLoader);
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("Failed to load type class", e);
        }

        if (input) {
            if (!fnInputClass.isAssignableFrom(schemaInputClass)) {
                throw new IllegalArgumentException(
                        "Schema type mismatch " + typeArg + " vs " + schemaTypes[0]);
            }
        } else {
            if (!schemaInputClass.isAssignableFrom(fnInputClass)) {
                throw new IllegalArgumentException(
                        "Schema type mismatch " + typeArg + " vs " + schemaTypes[0]);
            }
        }
    }
}
