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

import static com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL;
import static com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED;
import static com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Label.LABEL_REQUIRED;
import static com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type.TYPE_BYTES;
import static com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type.TYPE_ENUM;
import static com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type.TYPE_FLOAT;
import static com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type.TYPE_GROUP;
import static com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32;
import static com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64;
import static com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE;
import static com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.Edition;
import com.google.protobuf.DescriptorProtos.EnumDescriptorProto;
import com.google.protobuf.DescriptorProtos.EnumOptions;
import com.google.protobuf.DescriptorProtos.EnumValueDescriptorProto;
import com.google.protobuf.DescriptorProtos.FeatureSet;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Label;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type;
import com.google.protobuf.DescriptorProtos.FieldOptions;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.DescriptorProtos.FileOptions;
import com.google.protobuf.DescriptorProtos.MessageOptions;
import com.google.protobuf.DescriptorProtos.OneofDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.JavaFeaturesProto;
import com.google.protobuf.JavaFeaturesProto.JavaFeatures;
import com.google.protobuf.UnknownFieldSet;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.pulsar.broker.service.schema.exceptions.IncompatibleSchemaException;
import org.apache.pulsar.client.api.SchemaSerializationException;
import org.apache.pulsar.client.impl.schema.ProtobufNativeSchemaUtils;
import org.apache.pulsar.common.policies.data.SchemaCompatibilityStrategy;
import org.apache.pulsar.common.protocol.schema.ProtobufNativeSchemaData;
import org.apache.pulsar.common.protocol.schema.SchemaData;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class ProtobufNativeSchemaCompatibilityTest {
    private final ProtobufNativeSchemaAdvancedCompatibilityCheck checker =
            new ProtobufNativeSchemaAdvancedCompatibilityCheck();

    @Test
    public void testDirectionalRequiredAndOptionalAddition() throws Exception {
        Descriptor optional = root(message("Order", field("id", 1, TYPE_INT32, LABEL_OPTIONAL)));
        Descriptor required = root(message("Order", field("id", 1, TYPE_INT32, LABEL_REQUIRED)));
        Descriptor added = root(message("Order", field("id", 1, TYPE_INT32, LABEL_OPTIONAL),
                field("note", 2, TYPE_STRING, LABEL_OPTIONAL)));

        fails(optional, required, "REQUIRED_FIELD_NOT_GUARANTEED");
        accepts(required, optional);
        accepts(optional, added);
        accepts(added, optional);
        assertFalse(DynamicMessage.newBuilder(required).isInitialized());
        assertTrue(DynamicMessage.newBuilder(required).setField(required.findFieldByName("id"), 7)
                .build().isInitialized());
    }

    @Test
    public void testTypesCardinalityAndFieldIdentity() throws Exception {
        Descriptor original = root(message("Order", field("id", 1, TYPE_INT32, LABEL_OPTIONAL)));
        Descriptor wider = root(message("Order", field("id", 1, TYPE_INT64, LABEL_OPTIONAL)));
        Descriptor renamed = root(message("Order", field("identifier", 1, TYPE_INT32, LABEL_OPTIONAL)));
        Descriptor renumbered = root(message("Order", field("id", 2, TYPE_INT32, LABEL_OPTIONAL)));
        Descriptor repeated = root(message("Order", field("id", 1, TYPE_INT32, LABEL_REPEATED)));
        fails(original, wider, "TYPE_CHANGED");
        fails(wider, original, "TYPE_CHANGED");
        accepts(original, renamed);
        fails(original, renumbered, "FIELD_NUMBER_CHANGED");
        fails(original, repeated, "CARDINALITY_CHANGED");
        fails(repeated, original, "CARDINALITY_CHANGED");
        DynamicMessage bytes = DynamicMessage.newBuilder(original).setField(original.findFieldByName("id"), 42)
                .build();
        DynamicMessage read = DynamicMessage.parseFrom(renamed, bytes.toByteArray());
        assertEquals(read.getField(renamed.findFieldByNumber(1)), 42);
    }

    @Test
    public void testDefaultsAndEnumReordering() throws Exception {
        Descriptor implicit = root(message("Order", field("id", 1, TYPE_INT32, LABEL_OPTIONAL)));
        Descriptor explicitZero = root(message("Order", field("id", 1, TYPE_INT32, LABEL_OPTIONAL)
                .toBuilder().setDefaultValue("0").build()));
        Descriptor explicitOne = root(message("Order", field("id", 1, TYPE_INT32, LABEL_OPTIONAL)
                .toBuilder().setDefaultValue("1").build()));
        accepts(implicit, explicitZero);
        fails(implicit, explicitOne, "DEFAULT_CHANGED");

        Descriptor first = enumRoot(false, false);
        Descriptor reordered = enumRoot(true, false);
        fails(first, reordered, "DEFAULT_CHANGED");
        assertEquals(((com.google.protobuf.Descriptors.EnumValueDescriptor) DynamicMessage.getDefaultInstance(first)
                .getField(first.findFieldByName("state"))).getNumber(), 1);
        assertEquals(((com.google.protobuf.Descriptors.EnumValueDescriptor) DynamicMessage.getDefaultInstance(reordered)
                .getField(reordered.findFieldByName("state"))).getNumber(), 2);
        accepts(enumRoot(false, true), enumRoot(true, true));

        Descriptor positiveZero = root(message("Order", field("amount", 1, TYPE_FLOAT, LABEL_OPTIONAL)
                .toBuilder().setDefaultValue("0").build()));
        Descriptor negativeZero = root(message("Order", field("amount", 1, TYPE_FLOAT, LABEL_OPTIONAL)
                .toBuilder().setDefaultValue("-0").build()));
        fails(positiveZero, negativeZero, "DEFAULT_CHANGED");
        Descriptor bytesA = root(message("Order", field("code", 1, TYPE_BYTES, LABEL_OPTIONAL)
                .toBuilder().setDefaultValue("abc").build()));
        Descriptor bytesB = root(message("Order", field("code", 1, TYPE_BYTES, LABEL_OPTIONAL)
                .toBuilder().setDefaultValue("abd").build()));
        fails(bytesA, bytesB, "DEFAULT_CHANGED");
    }

    @Test
    public void testOneofAndRecursion() throws Exception {
        DescriptorProto.Builder independent = DescriptorProto.newBuilder().setName("Order")
                .addField(field("a", 1, TYPE_INT32, LABEL_OPTIONAL))
                .addField(field("b", 2, TYPE_INT32, LABEL_OPTIONAL));
        DescriptorProto.Builder grouped = independent.clone()
                .addOneofDecl(OneofDescriptorProto.newBuilder().setName("choice"))
                .setField(0, independent.getField(0).toBuilder().setOneofIndex(0))
                .setField(1, independent.getField(1).toBuilder().setOneofIndex(0));
        Descriptor writer = root(independent.build());
        Descriptor reader = root(grouped.build());
        fails(writer, reader, "ONEOF_CONFLICT");
        accepts(reader, writer);
        DynamicMessage bytes = DynamicMessage.newBuilder(writer).setField(writer.findFieldByName("a"), 1)
                .setField(writer.findFieldByName("b"), 2).build();
        DynamicMessage parsed = DynamicMessage.parseFrom(reader, bytes.toByteArray());
        assertFalse(parsed.hasField(reader.findFieldByName("a")));
        assertTrue(parsed.hasField(reader.findFieldByName("b")));

        FieldDescriptorProto child = field("child", 1, TYPE_MESSAGE, LABEL_OPTIONAL).toBuilder()
                .setTypeName(".example.Order").build();
        Descriptor recursive = root(message("Order", child));
        accepts(recursive, recursive);
    }

    @Test
    public void testSparseFieldsAndUnsupportedReachableGraph() throws Exception {
        Descriptor sparse = root(message("Order", field("first", 1, TYPE_INT32, LABEL_OPTIONAL),
                field("last", 536870911, TYPE_INT32, LABEL_OPTIONAL)));
        accepts(sparse, sparse);
        DescriptorProto withRange = message("Order", field("id", 1, TYPE_INT32, LABEL_OPTIONAL))
                .toBuilder().addExtensionRange(DescriptorProto.ExtensionRange.newBuilder()
                        .setStart(100).setEnd(200)).build();
        fails(root(withRange), sparse, "UNSUPPORTED_FEATURE");

        Descriptor base = root(message("Order", field("id", 1, TYPE_INT32, LABEL_OPTIONAL)));
        DescriptorProto unsupported = DescriptorProto.newBuilder().setName("Unsupported")
                .addExtensionRange(DescriptorProto.ExtensionRange.newBuilder().setStart(100).setEnd(200))
                .build();
        FileDescriptorProto.Builder file = FileDescriptorProto.newBuilder().setName("reachable.proto")
                .setPackage("example").setSyntax("proto2")
                .addMessageType(message("Order", field("id", 1, TYPE_INT32, LABEL_OPTIONAL)))
                .addMessageType(unsupported);
        Descriptor unreachable = FileDescriptor.buildFrom(file.build(), new FileDescriptor[0])
                .findMessageTypeByName("Order");
        accepts(base, unreachable);
        file.setMessageType(0, DescriptorProto.newBuilder().setName("Order")
                .addField(field("id", 1, TYPE_INT32, LABEL_OPTIONAL))
                .addField(field("extra", 2, TYPE_MESSAGE, LABEL_OPTIONAL).toBuilder()
                        .setTypeName(".example.Unsupported")));
        Descriptor reachable = FileDescriptor.buildFrom(file.build(), new FileDescriptor[0])
                .findMessageTypeByName("Order");
        fails(base, reachable, "UNSUPPORTED_FEATURE");

        Descriptor messageSet = root(DescriptorProto.newBuilder().setName("Order")
                .setOptions(MessageOptions.newBuilder().setMessageSetWireFormat(true)).build());
        fails(messageSet, messageSet, "UNSUPPORTED_FEATURE");
    }

    @Test
    public void testPackedAndExpandedReading() throws Exception {
        FieldDescriptorProto expandedField = field("ids", 1, TYPE_INT32, LABEL_REPEATED).toBuilder()
                .setOptions(FieldOptions.newBuilder().setPacked(false)).build();
        FieldDescriptorProto packedField = expandedField.toBuilder()
                .setOptions(FieldOptions.newBuilder().setPacked(true)).build();
        Descriptor expanded = root(message("Order", expandedField));
        Descriptor packed = root(message("Order", packedField));
        accepts(expanded, packed);
        accepts(packed, expanded);
        for (Descriptor writer : List.of(expanded, packed)) {
            Descriptor reader = writer == expanded ? packed : expanded;
            DynamicMessage bytes = DynamicMessage.newBuilder(writer)
                    .addRepeatedField(writer.findFieldByName("ids"), 3)
                    .addRepeatedField(writer.findFieldByName("ids"), 4).build();
            assertEquals(DynamicMessage.parseFrom(reader, bytes.toByteArray())
                    .getField(reader.findFieldByName("ids")), List.of(3, 4));
        }
    }

    @Test
    public void testMapKeyAndValueRules() throws Exception {
        Descriptor original = mapRoot(TYPE_STRING, TYPE_INT32);
        Descriptor keyChanged = mapRoot(TYPE_INT32, TYPE_INT32);
        Descriptor valueChanged = mapRoot(TYPE_STRING, TYPE_STRING);
        accepts(original, original);
        fails(original, keyChanged, "TYPE_CHANGED");
        fails(original, valueChanged, "TYPE_CHANGED");
        DescriptorProto plainEntry = DescriptorProto.newBuilder().setName("Entry")
                .addField(field("key", 1, TYPE_STRING, LABEL_OPTIONAL))
                .addField(field("value", 2, TYPE_INT32, LABEL_OPTIONAL)).build();
        Descriptor repeatedEntry = proto3Root(DescriptorProto.newBuilder().setName("Order")
                .addNestedType(plainEntry)
                .addField(field("items", 1, TYPE_MESSAGE, LABEL_REPEATED).toBuilder()
                        .setTypeName(".example.Order.Entry")).build());
        fails(original, repeatedEntry, "MAP_CHANGED");
    }

    @Test
    public void testEnumOpenClosedAndValueNumbers() throws Exception {
        Descriptor closed = enumSchema("proto2", true);
        Descriptor open = enumSchema("proto3", true);
        accepts(closed, open);
        fails(open, closed, "ENUM_VALUE_NOT_READABLE");
        fails(closed, enumSchema("proto2", false), "ENUM_VALUE_NOT_READABLE");

        Descriptor aliased = enumAliasSchema(true, 1);
        Descriptor noAlias = enumAliasSchema(false, 1);
        accepts(aliased, noAlias);
        accepts(noAlias, aliased);
        fails(noAlias, enumAliasSchema(false, 2), "ENUM_VALUE_NOT_READABLE");
    }

    @DataProvider
    public Object[][] enumFieldLabels() {
        return new Object[][]{{LABEL_OPTIONAL}, {LABEL_REPEATED}};
    }

    @Test(dataProvider = "enumFieldLabels")
    public void testClosedEnumMaximumValueRemoval(Label label) throws Exception {
        EnumDescriptorProto.Builder enumeration = EnumDescriptorProto.newBuilder().setName("State")
                .addValue(EnumValueDescriptorProto.newBuilder().setName("ZERO").setNumber(0))
                .addValue(EnumValueDescriptorProto.newBuilder().setName("MAX").setNumber(Integer.MAX_VALUE));
        DescriptorProto message = message("Order", field("state", 1, TYPE_ENUM, label).toBuilder()
                .setTypeName(".example.State").build());
        Descriptor writer = root(message, enumeration.build());
        Descriptor reader = root(message, enumeration.clone().removeValue(1).build());
        FieldDescriptor writerField = writer.findFieldByNumber(1);
        FieldDescriptor readerField = reader.findFieldByNumber(1);
        EnumValueDescriptor value = writerField.getEnumType().findValueByNumber(Integer.MAX_VALUE);
        DynamicMessage.Builder written = DynamicMessage.newBuilder(writer);
        if (label == LABEL_REPEATED) {
            written.addRepeatedField(writerField, value);
        } else {
            written.setField(writerField, value);
        }
        DynamicMessage read = DynamicMessage.parseFrom(reader, written.build().toByteArray());
        if (label == LABEL_REPEATED) {
            assertThat(read.getRepeatedFieldCount(readerField)).isZero();
        } else {
            assertThat(read.hasField(readerField)).isFalse();
            assertThat(((EnumValueDescriptor) read.getField(readerField)).getNumber()).isZero();
        }
        assertThat(read.getUnknownFields().getField(1).getVarintList()).containsExactly((long) Integer.MAX_VALUE);

        assertThatThrownBy(() -> checker.checkCompatible(schema(writer), schema(reader),
                SchemaCompatibilityStrategy.BACKWARD))
                .isInstanceOf(IncompatibleSchemaException.class)
                .hasMessageContaining("ENUM_VALUE_NOT_READABLE").hasMessageContaining("writer=2147483647");
        checker.checkCompatible(schema(writer), schema(reader), SchemaCompatibilityStrategy.FORWARD);
        Descriptor renamed = root(message, enumeration.setValue(1,
                enumeration.getValue(1).toBuilder().setName("MAX_ALIAS")).build());
        checker.checkCompatible(schema(writer), schema(renamed), SchemaCompatibilityStrategy.FULL);
    }

    @Test
    public void testUtf8ValidationDirection() throws Exception {
        Descriptor unchecked = utf8Root(false);
        Descriptor checked = utf8Root(true);
        assertFalse(unchecked.findFieldByName("name").needsUtf8Check());
        assertTrue(checked.findFieldByName("name").needsUtf8Check());
        fails(unchecked, checked, "UTF8_VALIDATION_ADDED");
        accepts(checked, unchecked);
    }

    @Test
    public void testEditionJavaUtf8OverrideAfterRoundTrip() throws Exception {
        Descriptor writer = editionUtf8Root(JavaFeatures.Utf8Validation.DEFAULT);
        Descriptor reader = editionUtf8Root(JavaFeatures.Utf8Validation.VERIFY);
        assertFalse(roundTrip(writer).findFieldByName("name").needsUtf8Check());
        assertTrue(roundTrip(reader).findFieldByName("name").needsUtf8Check());
        fails(writer, reader, "UTF8_VALIDATION_ADDED");
        accepts(reader, writer);
    }

    @Test
    public void testMessageTypeRenameAndNestedChange() throws Exception {
        Descriptor left = messageGraph("OldPayload", TYPE_INT32);
        Descriptor renamed = messageGraph("NewPayload", TYPE_INT32);
        Descriptor changed = messageGraph("NewPayload", TYPE_STRING);
        accepts(left, renamed);
        fails(left, changed, "TYPE_CHANGED");

        Descriptor codegenA = codegenRoot("example.generated.a");
        Descriptor codegenB = codegenRoot("example.generated.b");
        accepts(codegenA, codegenB);
    }

    @Test
    public void testEditionRequirednessAndUnsupportedEdition() throws Exception {
        for (Edition edition : List.of(Edition.EDITION_2023, Edition.EDITION_2024)) {
            Descriptor optional = editionRoot(edition, FeatureSet.FieldPresence.EXPLICIT);
            Descriptor required = editionRoot(edition, FeatureSet.FieldPresence.LEGACY_REQUIRED);
            assertTrue(required.findFieldByName("id").isRequired());
            fails(optional, required, "REQUIRED_FIELD_NOT_GUARANTEED");
            accepts(required, optional);
        }
        SchemaData unsupported = unsupportedEditionSchema();
        try {
            checker.checkCompatible(unsupported, unsupported, SchemaCompatibilityStrategy.BACKWARD);
            fail("Edition 2026 must be unsupported");
        } catch (IncompatibleSchemaException e) {
            assertTrue(e.getMessage().contains("UNSUPPORTED_FEATURE"), e.getMessage());
        }
    }

    @Test
    public void testUnknownWireFeatureIsUnsupported() throws Exception {
        FeatureSet features = FeatureSet.newBuilder().setUnknownFields(UnknownFieldSet.newBuilder()
                .addField(FeatureSet.FIELD_PRESENCE_FIELD_NUMBER,
                        UnknownFieldSet.Field.newBuilder().addVarint(99).build()).build()).build();
        FileDescriptorProto file = FileDescriptorProto.newBuilder().setName("unknown-feature.proto")
                .setPackage("example").setSyntax("editions").setEdition(Edition.EDITION_2023)
                .setOptions(FileOptions.newBuilder().setFeatures(features))
                .addMessageType(message("Order", field("id", 1, TYPE_INT32, LABEL_OPTIONAL))).build();
        SchemaData schema = rawSchema(file);
        try {
            checker.checkCompatible(schema, schema, SchemaCompatibilityStrategy.BACKWARD);
            fail("Unknown wire feature must not resolve to a default");
        } catch (IncompatibleSchemaException e) {
            assertTrue(e.getMessage().contains("UNSUPPORTED_FEATURE"), e.getMessage());
        }
    }

    @DataProvider
    public Object[][] explicitUnknownWireFeatures() {
        DescriptorProtos.getDescriptor();
        return new Object[][]{
                {FeatureSet.newBuilder().setFieldPresence(FeatureSet.FieldPresence.FIELD_PRESENCE_UNKNOWN).build()},
                {FeatureSet.newBuilder().setEnumType(FeatureSet.EnumType.ENUM_TYPE_UNKNOWN).build()},
                {FeatureSet.newBuilder().setRepeatedFieldEncoding(
                        FeatureSet.RepeatedFieldEncoding.REPEATED_FIELD_ENCODING_UNKNOWN).build()},
                {FeatureSet.newBuilder().setUtf8Validation(FeatureSet.Utf8Validation.UTF8_VALIDATION_UNKNOWN).build()},
                {FeatureSet.newBuilder().setMessageEncoding(
                        FeatureSet.MessageEncoding.MESSAGE_ENCODING_UNKNOWN).build()},
                {FeatureSet.newBuilder().setExtension(JavaFeaturesProto.java_, JavaFeatures.newBuilder()
                        .setUtf8Validation(JavaFeatures.Utf8Validation.UTF8_VALIDATION_UNKNOWN).build()).build()}
        };
    }

    @Test(dataProvider = "explicitUnknownWireFeatures")
    public void testExplicitUnknownWireFeatureIsUnsupported(FeatureSet features) throws Exception {
        Descriptor descriptor = FileDescriptor.buildFrom(editionFeatureFile(features), new FileDescriptor[0])
                .findMessageTypeByName("Order");
        FeatureSet restored = roundTrip(descriptor).getFile().toProto().getOptions().getFeatures();
        assertThat(restored).isEqualTo(features);
        assertThat(restored.getUnknownFields().asMap()).isEmpty();
        assertThatThrownBy(() -> checker.checkCompatible(schema(descriptor), schema(descriptor),
                SchemaCompatibilityStrategy.BACKWARD))
                .isInstanceOf(IncompatibleSchemaException.class).hasMessageContaining("UNSUPPORTED_FEATURE");
    }

    @Test(dataProvider = "explicitUnknownWireFeatures")
    public void testExplicitUnknownWireFeatureOnReconstructionFailure(FeatureSet features) throws Exception {
        SchemaData schema = rawSchema(editionFeatureFile(features).toBuilder()
                .addDependency("missing.proto").build());
        assertThatThrownBy(() -> ProtobufNativeSchemaUtils.deserialize(schema.getData()))
                .isInstanceOf(SchemaSerializationException.class);
        assertThatThrownBy(() -> checker.checkCompatible(schema, schema, SchemaCompatibilityStrategy.BACKWARD))
                .isInstanceOf(IncompatibleSchemaException.class).hasMessageContaining("UNSUPPORTED_FEATURE");
    }

    @Test
    public void testUnsetWireFeaturesInheritDefaults() throws Exception {
        DescriptorProtos.getDescriptor();
        for (FeatureSet features : List.of(FeatureSet.getDefaultInstance(), FeatureSet.newBuilder()
                .setExtension(JavaFeaturesProto.java_, JavaFeatures.getDefaultInstance()).build())) {
            for (Edition edition : List.of(Edition.EDITION_2023, Edition.EDITION_2024)) {
                Descriptor descriptor = FileDescriptor.buildFrom(editionFeatureFile(features).toBuilder()
                        .setEdition(edition).build(), new FileDescriptor[0]).findMessageTypeByName("Order");
                Descriptor restored = roundTrip(descriptor);
                assertThat(restored.findFieldByName("name").needsUtf8Check()).isTrue();
                assertThat(restored.findFieldByName("name").hasPresence()).isTrue();
                assertThat(restored.findFieldByName("name").isRequired()).isFalse();
                checker.checkCompatible(schema(descriptor), schema(restored), SchemaCompatibilityStrategy.FULL);
            }
        }
    }

    @Test
    public void testUnknownJavaUtf8FeatureIsUnsupported() throws Exception {
        DescriptorProtos.getDescriptor();
        FeatureSet features = FeatureSet.newBuilder()
                .setUtf8Validation(FeatureSet.Utf8Validation.NONE)
                .setExtension(JavaFeaturesProto.java_, JavaFeatures.newBuilder()
                        .setUnknownFields(UnknownFieldSet.newBuilder()
                                .addField(JavaFeatures.UTF8_VALIDATION_FIELD_NUMBER,
                                        UnknownFieldSet.Field.newBuilder().addVarint(999).build()).build())
                        .build()).build();
        FileDescriptorProto file = FileDescriptorProto.newBuilder().setName("unknown-java-utf8.proto")
                .setPackage("example").setSyntax("editions").setEdition(Edition.EDITION_2023)
                .addDependency(JavaFeaturesProto.getDescriptor().getName())
                .setOptions(FileOptions.newBuilder().setFeatures(features))
                .addMessageType(message("Order", field("name", 1, TYPE_STRING, LABEL_OPTIONAL))).build();
        Descriptor root = FileDescriptor.buildFrom(file, new FileDescriptor[]{JavaFeaturesProto.getDescriptor()})
                .findMessageTypeByName("Order");
        assertTrue(roundTrip(root).getFile().toProto().getOptions().getFeatures()
                .getExtension(JavaFeaturesProto.java_).getUnknownFields().asMap()
                .containsKey(JavaFeatures.UTF8_VALIDATION_FIELD_NUMBER));
        fails(root, root, "UNSUPPORTED_FEATURE");
    }

    @Test
    public void testUnknownReferencedEnumParentFeatureIsUnsupported() throws Exception {
        FeatureSet features = FeatureSet.newBuilder().setUnknownFields(UnknownFieldSet.newBuilder()
                .addField(FeatureSet.ENUM_TYPE_FIELD_NUMBER,
                        UnknownFieldSet.Field.newBuilder().addVarint(99).build()).build()).build();
        DescriptorProto outer = DescriptorProto.newBuilder().setName("Outer")
                .setOptions(MessageOptions.newBuilder().setFeatures(features))
                .addEnumType(EnumDescriptorProto.newBuilder().setName("State")
                        .addValue(EnumValueDescriptorProto.newBuilder().setName("A").setNumber(0)))
                .build();
        FileDescriptorProto file = FileDescriptorProto.newBuilder().setName("unknown-enum-parent.proto")
                .setPackage("example").setSyntax("editions").setEdition(Edition.EDITION_2023)
                .addMessageType(message("Order", field("state", 1, TYPE_ENUM, LABEL_OPTIONAL).toBuilder()
                        .setTypeName(".example.Outer.State").build()))
                .addMessageType(outer).build();
        Descriptor root = FileDescriptor.buildFrom(file, new FileDescriptor[0]).findMessageTypeByName("Order");
        Descriptor parent = roundTrip(root).findFieldByName("state").getEnumType().getContainingType();
        assertTrue(parent.toProto().getOptions().getFeatures().getUnknownFields().asMap()
                .containsKey(FeatureSet.ENUM_TYPE_FIELD_NUMBER));
        fails(root, root, "UNSUPPORTED_FEATURE");
    }

    @Test
    public void testDelimitedMessageEncodingAndContents() throws Exception {
        Descriptor group = groupRoot();
        Descriptor delimited = editionMessageRoot(FeatureSet.MessageEncoding.DELIMITED, TYPE_INT32);
        Descriptor lengthPrefixed = editionMessageRoot(FeatureSet.MessageEncoding.LENGTH_PREFIXED, TYPE_INT32);
        Descriptor changedContent = editionMessageRoot(FeatureSet.MessageEncoding.DELIMITED, TYPE_STRING);
        assertEquals(delimited.findFieldByName("group").getType(),
                group.findFieldByName("group").getType());
        accepts(group, delimited);
        accepts(delimited, group);
        fails(group, lengthPrefixed, "TYPE_CHANGED");
        fails(group, changedContent, "TYPE_CHANGED");
    }

    @Test
    public void testSyntheticOptionalOneofIsNotExclusive() throws Exception {
        Descriptor writer = proto3Root(message("Order", field("id", 1, TYPE_INT32, LABEL_OPTIONAL),
                field("other", 2, TYPE_INT32, LABEL_OPTIONAL)));
        DescriptorProto readerMessage = DescriptorProto.newBuilder().setName("Order")
                .addOneofDecl(OneofDescriptorProto.newBuilder().setName("_id"))
                .addField(field("id", 1, TYPE_INT32, LABEL_OPTIONAL).toBuilder()
                        .setOneofIndex(0).setProto3Optional(true))
                .addField(field("other", 2, TYPE_INT32, LABEL_OPTIONAL)).build();
        Descriptor reader = proto3Root(readerMessage);
        accepts(writer, reader);
        accepts(reader, writer);
    }

    @Test
    public void testSameWriterMessageCanPairWithDifferentReaders() throws Exception {
        Descriptor writer = pairedGraph(false);
        Descriptor reader = pairedGraph(true);
        fails(writer, reader, "TYPE_CHANGED");
    }

    @Test
    public void testNestedRootMutualRecursionAndImportedShortNames() throws Exception {
        FileDescriptorProto nestedFile = FileDescriptorProto.newBuilder().setName("nested-root.proto")
                .setPackage("example").setSyntax("proto2")
                .addMessageType(DescriptorProto.newBuilder().setName("Outer")
                        .addNestedType(message("Inner", field("id", 1, TYPE_INT32, LABEL_OPTIONAL))))
                .build();
        Descriptor nested = FileDescriptor.buildFrom(nestedFile, new FileDescriptor[0])
                .findMessageTypeByName("Outer").findNestedTypeByName("Inner");
        accepts(nested, nested);

        FileDescriptorProto recursiveFile = FileDescriptorProto.newBuilder().setName("mutual.proto")
                .setPackage("example").setSyntax("proto2")
                .addMessageType(message("Order", field("next", 1, TYPE_MESSAGE, LABEL_OPTIONAL)
                        .toBuilder().setTypeName(".example.Other").build()))
                .addMessageType(message("Other", field("previous", 1, TYPE_MESSAGE, LABEL_OPTIONAL)
                        .toBuilder().setTypeName(".example.Order").build())).build();
        Descriptor recursive = FileDescriptor.buildFrom(recursiveFile, new FileDescriptor[0])
                .findMessageTypeByName("Order");
        accepts(recursive, recursive);

        Descriptor imported = importedSameShortNames(TYPE_STRING);
        accepts(imported, imported);
        fails(imported, importedSameShortNames(TYPE_INT32), "TYPE_CHANGED");
    }

    @Test
    public void testCheckerDoesNotShareTraversalState() throws Exception {
        Descriptor writer = root(message("Order", field("id", 1, TYPE_INT32, LABEL_OPTIONAL)));
        Descriptor incompatible = root(message("Order", field("id", 1, TYPE_STRING, LABEL_OPTIONAL)));
        SchemaData oldSchema = schema(writer);
        SchemaData changed = schema(incompatible);
        ExecutorService executor = Executors.newFixedThreadPool(4);
        try {
            List<Callable<Boolean>> jobs = List.of(
                    () -> checker.isCompatible(oldSchema, oldSchema, SchemaCompatibilityStrategy.FULL),
                    () -> checker.isCompatible(oldSchema, changed, SchemaCompatibilityStrategy.BACKWARD),
                    () -> checker.isCompatible(oldSchema, oldSchema, SchemaCompatibilityStrategy.FULL),
                    () -> checker.isCompatible(oldSchema, changed, SchemaCompatibilityStrategy.FORWARD));
            List<Future<Boolean>> results = executor.invokeAll(jobs);
            assertTrue(results.get(0).get());
            assertFalse(results.get(1).get());
            assertTrue(results.get(2).get());
            assertFalse(results.get(3).get());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testDeclarationOrderDoesNotChangeFirstFailure() throws Exception {
        Descriptor writer = root(message("Order", field("a", 1, TYPE_INT32, LABEL_OPTIONAL),
                field("b", 2, TYPE_INT32, LABEL_OPTIONAL)));
        Descriptor reader = root(message("Order", field("b", 2, TYPE_STRING, LABEL_OPTIONAL),
                field("a", 1, TYPE_STRING, LABEL_OPTIONAL)));
        try {
            accepts(writer, reader);
            fail("Two changed types should fail");
        } catch (IncompatibleSchemaException e) {
            assertTrue(e.getMessage().contains("field=1"), e.getMessage());
        }
    }

    @Test
    public void testStrategyHistoryAndDiagnostic() throws Exception {
        SchemaData v1 = schema(root(message("Order", field("id", 1, TYPE_INT32, LABEL_OPTIONAL))));
        SchemaData v2 = schema(root(message("Order")));
        SchemaData v3 = schema(root(message("Order", field("id", 1, TYPE_STRING, LABEL_OPTIONAL))));
        checker.checkCompatible(List.of(v2), v3, SchemaCompatibilityStrategy.BACKWARD);
        checker.checkCompatible(List.of(v2), v3, SchemaCompatibilityStrategy.FORWARD);
        checker.checkCompatible(List.of(v2), v3, SchemaCompatibilityStrategy.FULL);
        for (SchemaCompatibilityStrategy strategy : List.of(SchemaCompatibilityStrategy.BACKWARD_TRANSITIVE,
                SchemaCompatibilityStrategy.FORWARD_TRANSITIVE, SchemaCompatibilityStrategy.FULL_TRANSITIVE)) {
            try {
                checker.checkCompatible(List.of(v1, v2), v3, strategy);
                fail("Every historical schema must be compared for " + strategy);
            } catch (IncompatibleSchemaException e) {
                assertTrue(e.getMessage().contains("TYPE_CHANGED"), e.getMessage());
            }
        }
        try {
            checker.checkCompatible(List.of(v1, v2), v3, SchemaCompatibilityStrategy.BACKWARD_TRANSITIVE);
            fail("The original field type must be checked");
        } catch (IncompatibleSchemaException e) {
            assertTrue(e.getMessage().contains("TYPE_CHANGED"));
            assertTrue(e.getMessage().contains("direction=BACKWARD"));
            assertTrue(e.getMessage().contains("existingSchemaSha256="));
            assertTrue(e.getMessage().length() <= 2048);
        }
        checker.checkCompatible(List.of(v1, v2), v3, SchemaCompatibilityStrategy.ALWAYS_COMPATIBLE);
        try {
            checker.checkCompatible(List.of(), v3, SchemaCompatibilityStrategy.ALWAYS_INCOMPATIBLE);
            fail("ALWAYS_INCOMPATIBLE must reject when invoked");
        } catch (IncompatibleSchemaException expected) {
            assertTrue(expected.getMessage().contains("ALWAYS_INCOMPATIBLE"));
        }
    }

    private static FileDescriptorProto editionFeatureFile(FeatureSet features) {
        return FileDescriptorProto.newBuilder().setName("edition-features.proto")
                .setPackage("example").setSyntax("editions").setEdition(Edition.EDITION_2023)
                .setOptions(FileOptions.newBuilder().setFeatures(features))
                .addMessageType(message("Order", field("name", 1, TYPE_STRING, LABEL_OPTIONAL))).build();
    }

    private static Descriptor enumRoot(boolean reversed, boolean explicitDefault) throws Exception {
        EnumDescriptorProto.Builder enumeration = EnumDescriptorProto.newBuilder().setName("State");
        EnumValueDescriptorProto a = EnumValueDescriptorProto.newBuilder().setName("A").setNumber(1).build();
        EnumValueDescriptorProto b = EnumValueDescriptorProto.newBuilder().setName("B").setNumber(2).build();
        enumeration.addValue(reversed ? b : a).addValue(reversed ? a : b);
        FieldDescriptorProto.Builder field = field("state", 1, TYPE_ENUM, LABEL_OPTIONAL).toBuilder()
                .setTypeName(".example.State");
        if (explicitDefault) {
            field.setDefaultValue("A");
        }
        return root(message("Order", field.build()), enumeration.build());
    }

    private static Descriptor mapRoot(Type keyType, Type valueType) throws Exception {
        DescriptorProto entry = DescriptorProto.newBuilder().setName("Entry")
                .setOptions(MessageOptions.newBuilder().setMapEntry(true))
                .addField(field("key", 1, keyType, LABEL_OPTIONAL))
                .addField(field("value", 2, valueType, LABEL_OPTIONAL)).build();
        return proto3Root(DescriptorProto.newBuilder().setName("Order")
                .addNestedType(entry)
                .addField(field("items", 1, TYPE_MESSAGE, LABEL_REPEATED).toBuilder()
                        .setTypeName(".example.Order.Entry")).build());
    }

    private static Descriptor enumSchema(String syntax, boolean includeB) throws Exception {
        EnumDescriptorProto.Builder enumeration = EnumDescriptorProto.newBuilder().setName("State")
                .addValue(EnumValueDescriptorProto.newBuilder().setName("A").setNumber(0));
        if (includeB) {
            enumeration.addValue(EnumValueDescriptorProto.newBuilder().setName("B").setNumber(1));
        }
        FileDescriptorProto file = FileDescriptorProto.newBuilder().setName("enum.proto")
                .setPackage("example").setSyntax(syntax)
                .addEnumType(enumeration)
                .addMessageType(DescriptorProto.newBuilder().setName("Order")
                        .addField(field("state", 1, TYPE_ENUM, LABEL_OPTIONAL).toBuilder()
                                .setTypeName(".example.State"))).build();
        return FileDescriptor.buildFrom(file, new FileDescriptor[0]).findMessageTypeByName("Order");
    }

    private static Descriptor enumAliasSchema(boolean alias, int bNumber) throws Exception {
        EnumDescriptorProto.Builder enumeration = EnumDescriptorProto.newBuilder().setName("State")
                .addValue(EnumValueDescriptorProto.newBuilder().setName("A").setNumber(0))
                .addValue(EnumValueDescriptorProto.newBuilder().setName("B").setNumber(bNumber));
        if (alias) {
            enumeration.setOptions(EnumOptions.newBuilder().setAllowAlias(true))
                    .addValue(EnumValueDescriptorProto.newBuilder().setName("B_ALIAS").setNumber(bNumber));
        }
        FileDescriptorProto file = FileDescriptorProto.newBuilder().setName("alias.proto")
                .setPackage("example").setSyntax("proto2").addEnumType(enumeration)
                .addMessageType(DescriptorProto.newBuilder().setName("Order")
                        .addField(field("state", 1, TYPE_ENUM, LABEL_OPTIONAL).toBuilder()
                                .setTypeName(".example.State"))).build();
        return FileDescriptor.buildFrom(file, new FileDescriptor[0]).findMessageTypeByName("Order");
    }

    private static Descriptor utf8Root(boolean validate) throws Exception {
        FileDescriptorProto file = FileDescriptorProto.newBuilder().setName("utf8.proto")
                .setPackage("example").setSyntax("proto2")
                .setOptions(FileOptions.newBuilder().setJavaStringCheckUtf8(validate))
                .addMessageType(message("Order", field("name", 1, TYPE_STRING, LABEL_OPTIONAL))).build();
        return FileDescriptor.buildFrom(file, new FileDescriptor[0]).findMessageTypeByName("Order");
    }

    private static Descriptor editionUtf8Root(JavaFeatures.Utf8Validation javaValidation) throws Exception {
        DescriptorProtos.getDescriptor();
        FeatureSet features = FeatureSet.newBuilder()
                .setUtf8Validation(FeatureSet.Utf8Validation.NONE)
                .setExtension(JavaFeaturesProto.java_, JavaFeatures.newBuilder()
                        .setUtf8Validation(javaValidation).build()).build();
        FileDescriptorProto file = FileDescriptorProto.newBuilder().setName("edition-utf8.proto")
                .setPackage("example").setSyntax("editions").setEdition(Edition.EDITION_2023)
                .addDependency(JavaFeaturesProto.getDescriptor().getName())
                .setOptions(FileOptions.newBuilder().setFeatures(features))
                .addMessageType(message("Order", field("name", 1, TYPE_STRING, LABEL_OPTIONAL))).build();
        return FileDescriptor.buildFrom(file, new FileDescriptor[]{JavaFeaturesProto.getDescriptor()})
                .findMessageTypeByName("Order");
    }

    private static Descriptor messageGraph(String payloadName, Type valueType) throws Exception {
        FileDescriptorProto file = FileDescriptorProto.newBuilder().setName("payload.proto")
                .setPackage("example").setSyntax("proto2")
                .addMessageType(DescriptorProto.newBuilder().setName("Order")
                        .addField(field("payload", 1, TYPE_MESSAGE, LABEL_OPTIONAL).toBuilder()
                                .setTypeName(".example." + payloadName)))
                .addMessageType(message(payloadName, field("id", 1, valueType, LABEL_OPTIONAL))).build();
        return FileDescriptor.buildFrom(file, new FileDescriptor[0]).findMessageTypeByName("Order");
    }

    private static Descriptor codegenRoot(String javaPackage) throws Exception {
        FileDescriptorProto file = FileDescriptorProto.newBuilder().setName("codegen.proto")
                .setPackage("example").setSyntax("proto2")
                .setOptions(FileOptions.newBuilder().setJavaPackage(javaPackage))
                .addMessageType(message("Order", field("id", 1, TYPE_INT32, LABEL_OPTIONAL))).build();
        return FileDescriptor.buildFrom(file, new FileDescriptor[0]).findMessageTypeByName("Order");
    }

    private static Descriptor proto3Root(DescriptorProto message) throws Exception {
        FileDescriptorProto file = FileDescriptorProto.newBuilder().setName("order.proto")
                .setPackage("example").setSyntax("proto3").addMessageType(message).build();
        return FileDescriptor.buildFrom(file, new FileDescriptor[0]).findMessageTypeByName("Order");
    }

    private static Descriptor editionRoot(Edition edition, FeatureSet.FieldPresence presence) throws Exception {
        FieldDescriptorProto field = FieldDescriptorProto.newBuilder().setName("id").setNumber(1)
                .setType(TYPE_INT32).setOptions(FieldOptions.newBuilder()
                        .setFeatures(FeatureSet.newBuilder().setFieldPresence(presence))).build();
        FileDescriptorProto file = FileDescriptorProto.newBuilder().setName("edition.proto")
                .setPackage("example").setSyntax("editions").setEdition(edition)
                .addMessageType(message("Order", field)).build();
        return FileDescriptor.buildFrom(file, new FileDescriptor[0]).findMessageTypeByName("Order");
    }

    private static SchemaData unsupportedEditionSchema() throws Exception {
        FileDescriptorProto file = FileDescriptorProto.newBuilder().setName("future.proto")
                .setPackage("example").setSyntax("editions").setEdition(Edition.EDITION_2026)
                .addMessageType(message("Order", field("id", 1, TYPE_INT32, LABEL_OPTIONAL))).build();
        return rawSchema(file);
    }

    private static SchemaData rawSchema(FileDescriptorProto file) throws Exception {
        ProtobufNativeSchemaData data = ProtobufNativeSchemaData.builder()
                .fileDescriptorSet(FileDescriptorSet.newBuilder().addFile(file).build().toByteArray())
                .rootFileDescriptorName(file.getName()).rootMessageTypeName("example.Order").build();
        return SchemaData.builder().type(SchemaType.PROTOBUF_NATIVE)
                .data(ObjectMapperFactory.getMapperWithIncludeAlways().writer().writeValueAsBytes(data)).build();
    }

    private static Descriptor groupRoot() throws Exception {
        DescriptorProto nested = message("Group", field("id", 1, TYPE_INT32, LABEL_OPTIONAL));
        DescriptorProto outer = DescriptorProto.newBuilder().setName("Order").addNestedType(nested)
                .addField(field("group", 1, TYPE_GROUP, LABEL_OPTIONAL).toBuilder()
                        .setTypeName(".example.Order.Group")).build();
        return root(outer);
    }

    private static Descriptor pairedGraph(boolean reader) throws Exception {
        String firstType = reader ? "PayloadA" : "Payload";
        String secondType = reader ? "PayloadB" : "Payload";
        FileDescriptorProto.Builder file = FileDescriptorProto.newBuilder().setName("pairs.proto")
                .setPackage("example").setSyntax("proto2")
                .addMessageType(DescriptorProto.newBuilder().setName("Order")
                        .addField(field("first", 1, TYPE_MESSAGE, LABEL_OPTIONAL).toBuilder()
                                .setTypeName(".example." + firstType))
                        .addField(field("second", 2, TYPE_MESSAGE, LABEL_OPTIONAL).toBuilder()
                                .setTypeName(".example." + secondType)));
        if (reader) {
            file.addMessageType(message("PayloadA", field("id", 1, TYPE_INT32, LABEL_OPTIONAL)))
                    .addMessageType(message("PayloadB", field("id", 1, TYPE_STRING, LABEL_OPTIONAL)));
        } else {
            file.addMessageType(message("Payload", field("id", 1, TYPE_INT32, LABEL_OPTIONAL)));
        }
        return FileDescriptor.buildFrom(file.build(), new FileDescriptor[0]).findMessageTypeByName("Order");
    }

    private static Descriptor importedSameShortNames(Type betaType) throws Exception {
        FileDescriptor alpha = FileDescriptor.buildFrom(FileDescriptorProto.newBuilder().setName("alpha.proto")
                .setPackage("alpha").setSyntax("proto2")
                .addMessageType(message("Payload", field("id", 1, TYPE_INT32, LABEL_OPTIONAL)))
                .build(), new FileDescriptor[0]);
        FileDescriptor beta = FileDescriptor.buildFrom(FileDescriptorProto.newBuilder().setName("beta.proto")
                .setPackage("beta").setSyntax("proto2")
                .addMessageType(message("Payload", field("id", 1, betaType, LABEL_OPTIONAL)))
                .build(), new FileDescriptor[0]);
        FileDescriptorProto rootFile = FileDescriptorProto.newBuilder().setName("imports.proto")
                .setPackage("example").setSyntax("proto2")
                .addDependency("alpha.proto").addDependency("beta.proto")
                .addMessageType(message("Order",
                        field("alpha", 1, TYPE_MESSAGE, LABEL_OPTIONAL).toBuilder()
                                .setTypeName(".alpha.Payload").build(),
                        field("beta", 2, TYPE_MESSAGE, LABEL_OPTIONAL).toBuilder()
                                .setTypeName(".beta.Payload").build())).build();
        return FileDescriptor.buildFrom(rootFile, new FileDescriptor[]{alpha, beta})
                .findMessageTypeByName("Order");
    }

    private static Descriptor editionMessageRoot(FeatureSet.MessageEncoding encoding, Type nestedType)
            throws Exception {
        DescriptorProto nested = message("Group", field("id", 1, nestedType, LABEL_OPTIONAL));
        FieldDescriptorProto groupField = FieldDescriptorProto.newBuilder().setName("group").setNumber(1)
                .setType(TYPE_MESSAGE).setTypeName(".example.Order.Group")
                .setOptions(FieldOptions.newBuilder().setFeatures(
                        FeatureSet.newBuilder().setMessageEncoding(encoding))).build();
        FileDescriptorProto file = FileDescriptorProto.newBuilder().setName("group-edition.proto")
                .setPackage("example").setSyntax("editions").setEdition(Edition.EDITION_2023)
                .addMessageType(DescriptorProto.newBuilder().setName("Order").addNestedType(nested)
                        .addField(groupField)).build();
        return FileDescriptor.buildFrom(file, new FileDescriptor[0]).findMessageTypeByName("Order");
    }

    private static Descriptor root(DescriptorProto message) throws Exception {
        return root(message, null);
    }

    private static Descriptor root(DescriptorProto message, EnumDescriptorProto enumeration) throws Exception {
        FileDescriptorProto.Builder file = FileDescriptorProto.newBuilder().setName("order.proto")
                .setPackage("example").setSyntax("proto2").addMessageType(message);
        if (enumeration != null) {
            file.addEnumType(enumeration);
        }
        return FileDescriptor.buildFrom(file.build(), new FileDescriptor[0]).findMessageTypeByName("Order");
    }

    private static DescriptorProto message(String name, FieldDescriptorProto... fields) {
        return DescriptorProto.newBuilder().setName(name).addAllField(List.of(fields)).build();
    }

    private static FieldDescriptorProto field(String name, int number, Type type, Label label) {
        return FieldDescriptorProto.newBuilder().setName(name).setNumber(number).setType(type).setLabel(label).build();
    }

    private static SchemaData schema(Descriptor descriptor) {
        return SchemaData.builder().type(SchemaType.PROTOBUF_NATIVE)
                .data(ProtobufNativeSchemaUtils.serialize(descriptor)).build();
    }

    private static void accepts(Descriptor writer, Descriptor reader) throws Exception {
        ProtobufNativeSchemaCompatibility.canRead(roundTrip(writer), roundTrip(reader));
    }

    private static void fails(Descriptor writer, Descriptor reader, String rule) throws Exception {
        try {
            accepts(writer, reader);
            fail("Expected " + rule);
        } catch (IncompatibleSchemaException e) {
            assertTrue(e.getMessage().contains(rule), e.getMessage());
        }
    }

    private static Descriptor roundTrip(Descriptor descriptor) {
        return ProtobufNativeSchemaUtils.deserialize(ProtobufNativeSchemaUtils.serialize(descriptor));
    }
}
