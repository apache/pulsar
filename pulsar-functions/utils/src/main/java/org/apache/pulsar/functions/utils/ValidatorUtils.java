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

import static org.apache.commons.lang3.StringUtils.isEmpty;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import net.bytebuddy.description.type.TypeDefinition;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.pool.TypePool;
import org.apache.pulsar.client.api.CryptoKeyReader;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.functions.CryptoConfig;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.api.SerDe;

@Slf4j
public class ValidatorUtils {
    private static final String DEFAULT_SERDE = "org.apache.pulsar.functions.api.utils.DefaultSerDe";

    public static void validateSchema(String schemaType, TypeDefinition typeArg, TypePool typePool,
                                      boolean input) {
        if (isEmpty(schemaType) || getBuiltinSchemaType(schemaType) != null) {
            // If it's empty, we use the default schema and no need to validate
            // If it's built-in, no need to validate
        } else {
            TypeDescription schemaClass = null;
            try {
                schemaClass = typePool.describe(schemaType).resolve();
            } catch (TypePool.Resolution.NoSuchTypeException e) {
                throw new IllegalArgumentException(
                        String.format("The schema class %s does not exist", schemaType));
            }
            if (!schemaClass.asErasure().isAssignableTo(Schema.class)) {
                throw new IllegalArgumentException(
                        String.format("%s does not implement %s", schemaType, Schema.class.getName()));
            }
            validateSchemaType(schemaClass, typeArg, typePool, input);
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


    public static void validateCryptoKeyReader(CryptoConfig conf, TypePool typePool, boolean isProducer) {
        if (isEmpty(conf.getCryptoKeyReaderClassName())) {
            return;
        }

        String cryptoClassName = conf.getCryptoKeyReaderClassName();
        TypeDescription cryptoClass = null;
        try {
            cryptoClass = typePool.describe(cryptoClassName).resolve();
        } catch (TypePool.Resolution.NoSuchTypeException e) {
            throw new IllegalArgumentException(
                    String.format("The crypto key reader class %s does not exist", cryptoClassName));
        }
        if (!cryptoClass.asErasure().isAssignableTo(CryptoKeyReader.class)) {
            throw new IllegalArgumentException(
                    String.format("%s does not implement %s", cryptoClassName, CryptoKeyReader.class.getName()));
        }

        boolean hasConstructor = cryptoClass.getDeclaredMethods().stream()
                .anyMatch(method -> method.isConstructor() && method.getParameters().size() == 1
                        && method.getParameters().get(0).getType().asErasure().represents(Map.class));

        if (!hasConstructor) {
            throw new IllegalArgumentException(
                    String.format("The crypto key reader class %s does not implement the desired constructor.",
                            conf.getCryptoKeyReaderClassName()));
        }

        if (isProducer && (conf.getEncryptionKeys() == null || conf.getEncryptionKeys().length == 0)) {
            throw new IllegalArgumentException("Missing encryption key name for producer crypto key reader");
        }
    }

    public static void validateSerde(String inputSerializer, TypeDefinition typeArg, TypePool typePool,
                                     boolean deser) {
        if (isEmpty(inputSerializer)) {
            return;
        }
        if (inputSerializer.equals(DEFAULT_SERDE)) {
            return;
        }
        TypeDescription serdeClass;
        try {
            serdeClass = typePool.describe(inputSerializer).resolve();
        } catch (TypePool.Resolution.NoSuchTypeException e) {
            throw new IllegalArgumentException(
                    String.format("The input serialization/deserialization class %s does not exist",
                            inputSerializer));
        }
        TypeDescription.Generic serDeTypeArg = serdeClass.getInterfaces().stream()
                .filter(i -> i.asErasure().isAssignableTo(SerDe.class))
                .findFirst()
                .map(i -> i.getTypeArguments().get(0))
                .orElseThrow(() -> new IllegalArgumentException(
                        String.format("%s does not implement %s", inputSerializer, SerDe.class.getName())));

        if (deser) {
            if (!serDeTypeArg.asErasure().isAssignableTo(typeArg.asErasure())) {
                throw new IllegalArgumentException("Serializer type mismatch " + typeArg.getActualName() + " vs "
                        + serDeTypeArg.getActualName());
            }
        } else {
            if (!serDeTypeArg.asErasure().isAssignableFrom(typeArg.asErasure())) {
                throw new IllegalArgumentException("Serializer type mismatch " + typeArg.getActualName() + " vs "
                        + serDeTypeArg.getActualName());
            }
        }
    }

    private static void validateSchemaType(TypeDefinition schema, TypeDefinition typeArg, TypePool typePool,
                                           boolean input) {

        TypeDescription.Generic schemaTypeArg = schema.getInterfaces().stream()
                .filter(i -> i.asErasure().isAssignableTo(Schema.class))
                .findFirst()
                .map(i -> i.getTypeArguments().get(0))
                .orElse(null);

        if (input) {
            if (!schemaTypeArg.asErasure().isAssignableTo(typeArg.asErasure())) {
                throw new IllegalArgumentException(
                        "Schema type mismatch " + typeArg.getActualName() + " vs " + schemaTypeArg.getActualName());
            }
        } else {
            if (!schemaTypeArg.asErasure().isAssignableFrom(typeArg.asErasure())) {
                throw new IllegalArgumentException(
                        "Schema type mismatch " + typeArg.getActualName() + " vs " + schemaTypeArg.getActualName());
            }
        }
    }
}
