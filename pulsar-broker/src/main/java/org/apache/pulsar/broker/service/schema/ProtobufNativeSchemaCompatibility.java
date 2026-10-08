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

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.Edition;
import com.google.protobuf.DescriptorProtos.FeatureSet;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumDescriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Descriptors.OneofDescriptor;
import com.google.protobuf.JavaFeaturesProto;
import com.google.protobuf.JavaFeaturesProto.JavaFeatures;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pulsar.broker.service.schema.exceptions.IncompatibleSchemaException;

/** Compares the binary reading behavior of two resolved Protobuf descriptor graphs. */
final class ProtobufNativeSchemaCompatibility {
    private static final int MAX_PATH_LENGTH = 768;
    private static final int MAX_DETAIL_LENGTH = 256;
    private static final long MAX_COMPARISON_WORK = 100_000;
    private static final long GRAPH_WORK_MULTIPLIER = 16;

    private ProtobufNativeSchemaCompatibility() {
    }

    static void canRead(Descriptor writer, Descriptor reader) throws IncompatibleSchemaException {
        compareGraphs(writer, reader);
    }

    @VisibleForTesting
    static WorkSummary comparisonWork(Descriptor writer, Descriptor reader) throws IncompatibleSchemaException {
        Comparison comparison = compareGraphs(writer, reader);
        return new WorkSummary(comparison.graphWork, comparison.work, comparison.limit);
    }

    private static Comparison compareGraphs(Descriptor writer, Descriptor reader) throws IncompatibleSchemaException {
        if (!writer.getFullName().equals(reader.getFullName())) {
            throw incompatible("ROOT_MESSAGE_CHANGED", writer.getFullName(), 0,
                    writer.getFullName(), reader.getFullName());
        }
        Comparison comparison = new Comparison();
        checkSupportedGraph(writer, "writer", comparison);
        checkSupportedGraph(reader, "reader", comparison);
        comparison.graphWork = comparison.work;
        comparison.limit = Math.min(MAX_COMPARISON_WORK, comparison.graphWork * GRAPH_WORK_MULTIPLIER);
        comparison.enqueue(writer, reader, writer.getFullName());
        while (!comparison.queue.isEmpty()) {
            compareMessagePair(comparison.queue.removeFirst(), comparison);
        }
        return comparison;
    }

    private static void checkSupportedGraph(Descriptor root, String side, Comparison comparison)
            throws IncompatibleSchemaException {
        ArrayDeque<Descriptor> queue = new ArrayDeque<>();
        Set<Descriptor> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        Set<EnumDescriptor> visitedEnums = Collections.newSetFromMap(new IdentityHashMap<>());
        queue.add(root);
        visited.add(root);
        while (!queue.isEmpty()) {
            Descriptor message = queue.removeFirst();
            String path = message.getFullName();
            // Reachable messages are visited once per side; sharing does not skip their referring fields below.
            comparison.charge(1, path);
            comparison.checkFile(message.getFile(), side, path);
            if (message.toProto().getExtensionRangeCount() != 0
                    || message.getOptions().getMessageSetWireFormat()) {
                throw incompatible("UNSUPPORTED_FEATURE", path, 0, side, "extension range or MessageSet");
            }
            comparison.checkContainingFeatures(message);
            for (FieldDescriptor field : comparison.sortedFields(message)) {
                // Every reference field is charged, including references to an already visited message or enum.
                comparison.charge(1, path);
                String fieldPath = append(path, field.getName());
                checkFeatures(field.toProto().getOptions().getFeatures(), fieldPath);
                if (field.getJavaType() == FieldDescriptor.JavaType.ENUM) {
                    EnumDescriptor enumType = field.getEnumType();
                    if (visitedEnums.add(enumType)) {
                        comparison.charge(1L + enumType.getValues().size(), fieldPath);
                        comparison.checkFile(enumType.getFile(), side, fieldPath);
                        comparison.checkContainingFeatures(enumType.getContainingType());
                        checkFeatures(enumType.toProto().getOptions().getFeatures(), fieldPath);
                        for (EnumValueDescriptor value : enumType.getValues()) {
                            checkFeatures(value.toProto().getOptions().getFeatures(), fieldPath);
                        }
                    }
                    if (enumType.isClosed() != legacyEnumFieldTreatedAsClosed(field)) {
                        throw incompatible("UNSUPPORTED_FEATURE", fieldPath, field.getNumber(), side,
                                "Java legacy closed-enum behavior differs from enum openness");
                    }
                } else if (field.getJavaType() == FieldDescriptor.JavaType.MESSAGE) {
                    Descriptor child = field.getMessageType();
                    if (visited.add(child)) {
                        queue.addLast(child);
                    }
                }
            }
            for (OneofDescriptor oneof : comparison.sortedOneofs(message)) {
                comparison.charge(1, path);
                checkFeatures(oneof.toProto().getOptions().getFeatures(), append(path, oneof.getName()));
            }
        }
    }

    private static void checkLanguage(FileDescriptor file, String side, String path)
            throws IncompatibleSchemaException {
        FileDescriptorProto proto = file.toProto();
        String syntax = proto.getSyntax();
        boolean supported = (syntax.isEmpty() || syntax.equals("proto2") || syntax.equals("proto3"))
                && !proto.hasEdition();
        if (syntax.equals("editions")) {
            supported = proto.hasEdition()
                    && (proto.getEdition() == Edition.EDITION_2023
                    || proto.getEdition() == Edition.EDITION_2024);
        }
        if (!supported) {
            throw incompatible("UNSUPPORTED_FEATURE", path, 0, side, "syntax=" + syntax
                    + ", edition=" + proto.getEdition());
        }
        checkFeatures(proto.getOptions().getFeatures(), path);
    }

    private static void checkFeatures(FeatureSet features, String path) throws IncompatibleSchemaException {
        String unsupported = findUnsupportedFeature(features);
        if (unsupported != null) {
            throw incompatible("UNSUPPORTED_FEATURE", path, 0, "feature", unsupported);
        }
    }

    static String findUnsupportedFeature(FeatureSet features) {
        // Unknown feature values and extensions cannot be interpreted as resolved wire behavior.
        if (!features.getUnknownFields().asMap().isEmpty()) {
            int number = Collections.min(features.getUnknownFields().asMap().keySet());
            return Integer.toString(number);
        }
        // Only inspect explicitly set wire features; absent options inherit their enclosing or edition defaults.
        if (features.hasFieldPresence()
                && features.getFieldPresence() == FeatureSet.FieldPresence.FIELD_PRESENCE_UNKNOWN) {
            return "field_presence=FIELD_PRESENCE_UNKNOWN";
        }
        if (features.hasEnumType() && features.getEnumType() == FeatureSet.EnumType.ENUM_TYPE_UNKNOWN) {
            return "enum_type=ENUM_TYPE_UNKNOWN";
        }
        if (features.hasRepeatedFieldEncoding() && features.getRepeatedFieldEncoding()
                == FeatureSet.RepeatedFieldEncoding.REPEATED_FIELD_ENCODING_UNKNOWN) {
            return "repeated_field_encoding=REPEATED_FIELD_ENCODING_UNKNOWN";
        }
        if (features.hasUtf8Validation()
                && features.getUtf8Validation() == FeatureSet.Utf8Validation.UTF8_VALIDATION_UNKNOWN) {
            return "utf8_validation=UTF8_VALIDATION_UNKNOWN";
        }
        if (features.hasMessageEncoding()
                && features.getMessageEncoding() == FeatureSet.MessageEncoding.MESSAGE_ENCODING_UNKNOWN) {
            return "message_encoding=MESSAGE_ENCODING_UNKNOWN";
        }
        if (features.hasExtension(JavaFeaturesProto.java_)) {
            JavaFeatures javaFeatures = features.getExtension(JavaFeaturesProto.java_);
            var unknownJavaFeatures = javaFeatures.getUnknownFields().asMap();
            if (!unknownJavaFeatures.isEmpty()) {
                int number = Collections.min(unknownJavaFeatures.keySet());
                return "Java feature " + number;
            }
            if (javaFeatures.hasUtf8Validation()
                    && javaFeatures.getUtf8Validation() == JavaFeatures.Utf8Validation.UTF8_VALIDATION_UNKNOWN) {
                return "java.utf8_validation=UTF8_VALIDATION_UNKNOWN";
            }
        }
        return null;
    }

    private static void compareMessagePair(MessagePair pair, Comparison comparison)
            throws IncompatibleSchemaException {
        Descriptor writer = pair.writer;
        Descriptor reader = pair.reader;
        List<FieldDescriptor> readerFields = comparison.sortedFields(reader);
        // Reserve three complete reader-field scans, independently of pair or preparation cache hits.
        comparison.charge(3L * readerFields.size(), pair.path);
        for (FieldDescriptor readerField : readerFields) {
            FieldDescriptor sameName = writer.findFieldByName(readerField.getName());
            if (sameName != null && sameName.getNumber() != readerField.getNumber()) {
                throw incompatible("FIELD_NUMBER_CHANGED", append(pair.path, readerField.getName()),
                        readerField.getNumber(), Integer.toString(sameName.getNumber()),
                        Integer.toString(readerField.getNumber()));
            }
        }
        for (FieldDescriptor readerField : readerFields) {
            if (readerField.isRequired()) {
                FieldDescriptor writerField = writer.findFieldByNumber(readerField.getNumber());
                if (writerField == null || !writerField.isRequired()) {
                    throw incompatible("REQUIRED_FIELD_NOT_GUARANTEED", append(pair.path, readerField.getName()),
                            readerField.getNumber(), writerField == null ? "absent" : "optional", "required");
                }
            }
        }
        checkOneofRelationships(pair, comparison);
        for (FieldDescriptor readerField : readerFields) {
            FieldDescriptor writerField = writer.findFieldByNumber(readerField.getNumber());
            if (writerField != null) {
                compareField(writerField, readerField, append(pair.path, readerField.getName()), comparison);
            }
        }
    }

    private static void checkOneofRelationships(MessagePair pair, Comparison comparison)
            throws IncompatibleSchemaException {
        for (OneofDescriptor oneof : comparison.sortedOneofs(pair.reader)) {
            comparison.charge(1, pair.path);
            if (oneof.getFieldCount() == 1 && oneof.getField(0).getRealContainingOneof() == null) {
                continue;
            }
            List<FieldDescriptor> matchingWriterFields = new ArrayList<>();
            List<FieldDescriptor> oneofFields = comparison.oneofFields.get(oneof);
            // Two logical passes per real-oneof member; synthetic oneofs only pay the body charge above.
            comparison.charge(2L * oneofFields.size(), pair.path);
            for (FieldDescriptor readerField : oneofFields) {
                FieldDescriptor writerField = pair.writer.findFieldByNumber(readerField.getNumber());
                if (writerField != null) {
                    matchingWriterFields.add(writerField);
                }
            }
            if (matchingWriterFields.size() < 2) {
                continue;
            }
            OneofDescriptor writerGroup = matchingWriterFields.get(0).getRealContainingOneof();
            for (FieldDescriptor writerField : matchingWriterFields) {
                if (writerGroup == null || writerField.getRealContainingOneof() != writerGroup) {
                    throw incompatible("ONEOF_CONFLICT", append(pair.path, oneof.getName()),
                            writerField.getNumber(), "independent groups", "oneof");
                }
            }
        }
    }

    private static void compareField(FieldDescriptor writer, FieldDescriptor reader, String path,
                                     Comparison comparison)
            throws IncompatibleSchemaException {
        if (writer.isMapField() != reader.isMapField()) {
            throw incompatible("MAP_CHANGED", path, reader.getNumber(),
                    Boolean.toString(writer.isMapField()), Boolean.toString(reader.isMapField()));
        }
        if (writer.isRepeated() != reader.isRepeated()) {
            throw incompatible("CARDINALITY_CHANGED", path, reader.getNumber(),
                    writer.isRepeated() ? "repeated" : "singular", reader.isRepeated() ? "repeated" : "singular");
        }
        if (writer.getType() != reader.getType()) {
            throw incompatible("TYPE_CHANGED", path, reader.getNumber(),
                    writer.getType().name(), reader.getType().name());
        }
        if (!reader.isRepeated() && reader.getJavaType() != FieldDescriptor.JavaType.MESSAGE
                && !sameEffectiveDefault(writer, reader)) {
            throw incompatible("DEFAULT_CHANGED", path, reader.getNumber(),
                    describeDefault(writer), describeDefault(reader));
        }
        if (reader.getType() == FieldDescriptor.Type.STRING
                && needsUtf8Check(reader) && !needsUtf8Check(writer)) {
            throw incompatible("UTF8_VALIDATION_ADDED", path, reader.getNumber(), "unchecked", "checked");
        }
        if (reader.getJavaType() == FieldDescriptor.JavaType.ENUM) {
            compareEnum(writer.getEnumType(), reader.getEnumType(), path, reader.getNumber(), comparison);
        } else if (reader.getJavaType() == FieldDescriptor.JavaType.MESSAGE) {
            comparison.enqueue(writer.getMessageType(), reader.getMessageType(), path);
        }
    }

    private static void compareEnum(EnumDescriptor writer, EnumDescriptor reader, String path, int number,
                                    Comparison comparison)
            throws IncompatibleSchemaException {
        Set<EnumDescriptor> readers = comparison.enumPairs.computeIfAbsent(writer,
                ignored -> Collections.newSetFromMap(new IdentityHashMap<>()));
        if (!readers.add(reader)) {
            return;
        }
        // Only the first directed enum pair reserves both name and numeric-value passes.
        comparison.charge(1L + 2L * writer.getValues().size() + 2L * reader.getValues().size(), path);
        Map<String, Integer> writerNames = new HashMap<>();
        for (EnumValueDescriptor value : writer.getValues()) {
            writerNames.put(value.getName(), value.getNumber());
        }
        for (EnumValueDescriptor value : reader.getValues()) {
            Integer oldNumber = writerNames.get(value.getName());
            if (oldNumber != null && oldNumber != value.getNumber()) {
                throw incompatible("ENUM_VALUE_NOT_READABLE", path, number,
                        value.getName() + "=" + oldNumber, value.getName() + "=" + value.getNumber());
            }
        }
        if (!writer.isClosed() && reader.isClosed()) {
            throw incompatible("ENUM_VALUE_NOT_READABLE", path, number, "open", "closed");
        }
        if (writer.isClosed() && reader.isClosed()) {
            Set<Integer> readable = new HashSet<>();
            for (EnumValueDescriptor value : reader.getValues()) {
                readable.add(value.getNumber());
            }
            Integer missing = null;
            for (EnumValueDescriptor value : writer.getValues()) {
                if (!readable.contains(value.getNumber()) && (missing == null || value.getNumber() < missing)) {
                    missing = value.getNumber();
                }
            }
            if (missing != null) {
                throw incompatible("ENUM_VALUE_NOT_READABLE", path, number,
                        Integer.toString(missing), "absent");
            }
        }
    }

    private static boolean sameEffectiveDefault(FieldDescriptor writer, FieldDescriptor reader) {
        Object left = writer.getDefaultValue();
        Object right = reader.getDefaultValue();
        return switch (writer.getType()) {
            case ENUM -> ((EnumValueDescriptor) left).getNumber() == ((EnumValueDescriptor) right).getNumber();
            case FLOAT -> Float.floatToIntBits((Float) left) == Float.floatToIntBits((Float) right);
            case DOUBLE -> Double.doubleToLongBits((Double) left) == Double.doubleToLongBits((Double) right);
            default -> left.equals(right);
        };
    }

    private static String describeDefault(FieldDescriptor field) {
        Object value = field.getDefaultValue();
        if (field.getType() == FieldDescriptor.Type.ENUM) {
            return Integer.toString(((EnumValueDescriptor) value).getNumber());
        }
        if (field.getType() == FieldDescriptor.Type.BYTES) {
            return "bytes[" + ((ByteString) value).size() + "]";
        }
        return bounded(String.valueOf(value), 64);
    }

    private static boolean needsUtf8Check(FieldDescriptor field) {
        return field.needsUtf8Check();
    }

    private static boolean legacyEnumFieldTreatedAsClosed(FieldDescriptor field) {
        return field.legacyEnumFieldTreatedAsClosed();
    }

    /**
     * Logical work accounting for one direction of one historical schema pair. No cache crosses that boundary.
     * Support traversal deduplicates reachable messages and enums on each side, but charges every examined field
     * and oneof. Each message, field and oneof visit costs one unit; a first enum visit costs 1 + valueCount.
     * File checks and containing-feature scopes each cost one unit on their first identity visit within this
     * comparison. Field/oneof sorting preparation is also cached by identity and costs one unit per field, oneof
     * and member, including synthetic-oneof members.
     * The accumulated support work G remains part of the total; the final limit is min(100000, 16 * G).
     * A first directed message pair costs one unit, followed by the reader-field/oneof scans. A memoization hit
     * skips the pair and its subgraph, while the parent field scans still cost their usual units. A first enum pair
     * costs 1 + 2 * writerValues + 2 * readerValues; later references skip that enum-pair cost only.
     * Every message pair reserves 3 * readerFieldCount, one per reader oneof, and two per non-synthetic member.
     * These are logical charges, not elapsed CPU time or counts of collection implementation operations.
     */
    private static final class Comparison {
        private final ArrayDeque<MessagePair> queue = new ArrayDeque<>();
        private final IdentityHashMap<Descriptor, Set<Descriptor>> messagePairs = new IdentityHashMap<>();
        private final IdentityHashMap<EnumDescriptor, Set<EnumDescriptor>> enumPairs = new IdentityHashMap<>();
        private final IdentityHashMap<Descriptor, List<FieldDescriptor>> fields = new IdentityHashMap<>();
        private final IdentityHashMap<Descriptor, List<OneofDescriptor>> oneofs = new IdentityHashMap<>();
        private final IdentityHashMap<OneofDescriptor, List<FieldDescriptor>> oneofFields = new IdentityHashMap<>();
        private final Set<FileDescriptor> checkedFiles = Collections.newSetFromMap(new IdentityHashMap<>());
        private final Set<Descriptor> checkedScopes = Collections.newSetFromMap(new IdentityHashMap<>());
        private long work;
        private long graphWork;
        private long limit = MAX_COMPARISON_WORK;

        private void charge(long amount, String path) throws IncompatibleSchemaException {
            // Equality is allowed. Support scanning uses the absolute cap until G establishes the final limit.
            if (amount > limit - work) {
                throw incompatible("COMPARISON_LIMIT_EXCEEDED", path, 0,
                        "work=" + (work + amount), "limit=" + limit);
            }
            work += amount;
        }

        private void enqueue(Descriptor writer, Descriptor reader, String path) throws IncompatibleSchemaException {
            Set<Descriptor> readers = messagePairs.computeIfAbsent(writer,
                    ignored -> Collections.newSetFromMap(new IdentityHashMap<>()));
            if (readers.add(reader)) {
                charge(1, path);
                queue.addLast(new MessagePair(writer, reader, bounded(path, MAX_PATH_LENGTH)));
            }
        }

        private List<FieldDescriptor> sortedFields(Descriptor descriptor) throws IncompatibleSchemaException {
            List<FieldDescriptor> sorted = fields.get(descriptor);
            if (sorted == null) {
                charge(descriptor.getFields().size(), descriptor.getFullName());
                sorted = new ArrayList<>(descriptor.getFields());
                sorted.sort(Comparator.comparingInt(FieldDescriptor::getNumber));
                fields.put(descriptor, sorted);
            }
            return sorted;
        }

        private List<OneofDescriptor> sortedOneofs(Descriptor descriptor) throws IncompatibleSchemaException {
            List<OneofDescriptor> sorted = oneofs.get(descriptor);
            if (sorted == null) {
                charge(descriptor.getOneofs().size(), descriptor.getFullName());
                sorted = new ArrayList<>(descriptor.getOneofs());
                for (OneofDescriptor oneof : sorted) {
                    charge(oneof.getFieldCount(), descriptor.getFullName());
                    List<FieldDescriptor> members = new ArrayList<>(oneof.getFields());
                    members.sort(Comparator.comparingInt(FieldDescriptor::getNumber));
                    oneofFields.put(oneof, members);
                }
                sorted.sort(Comparator.comparingInt(oneof -> oneofFields.get(oneof).isEmpty()
                        ? Integer.MAX_VALUE : oneofFields.get(oneof).get(0).getNumber()));
                oneofs.put(descriptor, sorted);
            }
            return sorted;
        }

        private void checkFile(FileDescriptor file, String side, String path) throws IncompatibleSchemaException {
            if (checkedFiles.add(file)) {
                charge(1, path);
                checkLanguage(file, side, path);
            }
        }

        private void checkContainingFeatures(Descriptor descriptor) throws IncompatibleSchemaException {
            for (Descriptor scope = descriptor; scope != null && checkedScopes.add(scope);
                    scope = scope.getContainingType()) {
                charge(1, scope.getFullName());
                checkFeatures(scope.toProto().getOptions().getFeatures(), scope.getFullName());
            }
        }
    }

    private static String append(String path, String name) {
        return bounded(path + "." + name, MAX_PATH_LENGTH);
    }

    private static String bounded(String value, int max) {
        return value.length() <= max ? value : value.substring(0, max - 3) + "...";
    }

    private static IncompatibleSchemaException incompatible(String rule, String path, int field,
                                                             String writer, String reader) {
        return new IncompatibleSchemaException(rule + ": path=" + bounded(path, MAX_PATH_LENGTH)
                + (field == 0 ? "" : ", field=" + field)
                + ", writer=" + bounded(writer, MAX_DETAIL_LENGTH)
                + ", reader=" + bounded(reader, MAX_DETAIL_LENGTH));
    }

    private record MessagePair(Descriptor writer, Descriptor reader, String path) {
    }

    record WorkSummary(long graphWork, long chargedWork, long limit) {
    }
}
