/*
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
package org.apache.pulsar.broker.service.schema;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.Edition;
import com.google.protobuf.DescriptorProtos.EnumDescriptorProto;
import com.google.protobuf.DescriptorProtos.FeatureSet;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.JavaFeaturesProto;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.HexFormat;
import org.apache.pulsar.broker.service.schema.exceptions.IncompatibleSchemaException;
import org.apache.pulsar.client.api.SchemaSerializationException;
import org.apache.pulsar.client.impl.schema.ProtobufNativeSchemaUtils;
import org.apache.pulsar.common.policies.data.SchemaCompatibilityStrategy;
import org.apache.pulsar.common.protocol.schema.ProtobufNativeSchemaData;
import org.apache.pulsar.common.protocol.schema.SchemaData;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.common.util.ObjectMapperFactory;

/** Opt-in field-aware compatibility checker for Protobuf native schemas. */
public class ProtobufNativeSchemaAdvancedCompatibilityCheck implements SchemaCompatibilityCheck {
    private static final int MAX_DIAGNOSTIC_LENGTH = 2048;

    @Override
    public SchemaType getSchemaType() {
        return SchemaType.PROTOBUF_NATIVE;
    }

    @Override
    public void checkCompatible(SchemaData from, SchemaData to, SchemaCompatibilityStrategy strategy)
            throws IncompatibleSchemaException {
        checkCompatible(Collections.singletonList(from), to, strategy);
    }

    @Override
    public void checkCompatible(Iterable<SchemaData> from, SchemaData to, SchemaCompatibilityStrategy strategy)
            throws IncompatibleSchemaException {
        if (strategy == SchemaCompatibilityStrategy.ALWAYS_COMPATIBLE) {
            return;
        }
        if (strategy == SchemaCompatibilityStrategy.ALWAYS_INCOMPATIBLE) {
            throw new IncompatibleSchemaException("ALWAYS_INCOMPATIBLE: strategy=" + strategy);
        }
        if (strategy == null || !isDirectional(strategy)) {
            throw new IncompatibleSchemaException("UNKNOWN_STRATEGY: strategy=" + strategy);
        }
        if (from == null || to == null) {
            throw new IncompatibleSchemaException("SCHEMA_RECONSTRUCTION_FAILED: missing schema");
        }
        Descriptor proposed = null;
        for (SchemaData existingData : from) {
            if (proposed == null) {
                proposed = deserialize(to);
            }
            Descriptor existing = deserialize(existingData);
            if (isBackward(strategy)) {
                compare(existing, proposed, existingData, strategy, "BACKWARD");
            }
            if (isForward(strategy)) {
                compare(proposed, existing, existingData, strategy, "FORWARD");
            }
        }
    }

    private static void compare(Descriptor writer, Descriptor reader, SchemaData existing,
                                SchemaCompatibilityStrategy strategy, String direction)
            throws IncompatibleSchemaException {
        try {
            ProtobufNativeSchemaCompatibility.canRead(writer, reader);
        } catch (IncompatibleSchemaException e) {
            String prefix = "strategy=" + strategy + ", direction=" + direction
                    + ", existingSchemaSha256=" + fingerprint(existing.getData()) + ", ";
            String message = e.getMessage();
            throw new IncompatibleSchemaException(limit(message.substring(0, message.indexOf(':') + 1)
                    + " " + prefix + message.substring(message.indexOf(':') + 1)), e);
        }
    }

    private static Descriptor deserialize(SchemaData data) throws IncompatibleSchemaException {
        try {
            return ProtobufNativeSchemaUtils.deserialize(data.getData());
        } catch (SchemaSerializationException e) {
            if (hasUnsupportedMetadata(data.getData())) {
                throw new IncompatibleSchemaException(
                        "UNSUPPORTED_FEATURE: unsupported Protobuf language or feature", e);
            }
            throw new IncompatibleSchemaException("SCHEMA_RECONSTRUCTION_FAILED: invalid native descriptor", e);
        }
    }

    private static boolean hasUnsupportedMetadata(byte[] bytes) {
        try {
            ProtobufNativeSchemaData stored = ObjectMapperFactory.getMapper()
                    .reader().forType(ProtobufNativeSchemaData.class).readValue(bytes);
            DescriptorProtos.getDescriptor();
            ExtensionRegistry extensions = ExtensionRegistry.newInstance();
            JavaFeaturesProto.registerAllExtensions(extensions);
            for (FileDescriptorProto file : FileDescriptorSet.parseFrom(stored.getFileDescriptorSet(), extensions)
                    .getFileList()) {
                String syntax = file.getSyntax();
                if (syntax.equals("editions")) {
                    if (!file.hasEdition() || file.getEdition() != Edition.EDITION_2023
                            && file.getEdition() != Edition.EDITION_2024) {
                        return true;
                    }
                } else if (!syntax.isEmpty() && !syntax.equals("proto2") && !syntax.equals("proto3")
                        || file.hasEdition()) {
                    return true;
                }
                if (hasUnknownFeatures(file.getOptions().getFeatures())) {
                    return true;
                }
                for (var enumeration : file.getEnumTypeList()) {
                    if (hasUnknownEnumFeatures(enumeration)) {
                        return true;
                    }
                }
                ArrayDeque<DescriptorProto> messages = new ArrayDeque<>(file.getMessageTypeList());
                while (!messages.isEmpty()) {
                    DescriptorProto message = messages.removeFirst();
                    if (hasUnknownMessageFeatures(message)) {
                        return true;
                    }
                    messages.addAll(message.getNestedTypeList());
                }
            }
        } catch (IOException | RuntimeException ignored) {
            // A malformed envelope or descriptor set is a reconstruction error.
        }
        return false;
    }

    private static boolean hasUnknownMessageFeatures(DescriptorProto message) {
        if (hasUnknownFeatures(message.getOptions().getFeatures())) {
            return true;
        }
        for (var field : message.getFieldList()) {
            if (hasUnknownFeatures(field.getOptions().getFeatures())) {
                return true;
            }
        }
        for (var oneof : message.getOneofDeclList()) {
            if (hasUnknownFeatures(oneof.getOptions().getFeatures())) {
                return true;
            }
        }
        for (var enumeration : message.getEnumTypeList()) {
            if (hasUnknownEnumFeatures(enumeration)) {
                return true;
            }
        }
        return false;
    }

    private static boolean hasUnknownEnumFeatures(EnumDescriptorProto enumeration) {
        if (hasUnknownFeatures(enumeration.getOptions().getFeatures())) {
            return true;
        }
        for (var value : enumeration.getValueList()) {
            if (hasUnknownFeatures(value.getOptions().getFeatures())) {
                return true;
            }
        }
        return false;
    }

    private static boolean hasUnknownFeatures(FeatureSet features) {
        return ProtobufNativeSchemaCompatibility.findUnsupportedFeature(features) != null;
    }

    private static String fingerprint(byte[] bytes) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    }

    private static String limit(String value) {
        return value.length() <= MAX_DIAGNOSTIC_LENGTH ? value : value.substring(0, MAX_DIAGNOSTIC_LENGTH - 3)
                + "...";
    }

    private static boolean isDirectional(SchemaCompatibilityStrategy strategy) {
        return isBackward(strategy) || isForward(strategy);
    }

    private static boolean isBackward(SchemaCompatibilityStrategy strategy) {
        return switch (strategy) {
            case BACKWARD, BACKWARD_TRANSITIVE, FULL, FULL_TRANSITIVE -> true;
            default -> false;
        };
    }

    private static boolean isForward(SchemaCompatibilityStrategy strategy) {
        return switch (strategy) {
            case FORWARD, FORWARD_TRANSITIVE, FULL, FULL_TRANSITIVE -> true;
            default -> false;
        };
    }
}
