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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.Edition;
import com.google.protobuf.DescriptorProtos.FeatureSet;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileOptions;
import com.google.protobuf.DescriptorProtos.MessageOptions;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.JavaFeaturesProto;
import com.google.protobuf.JavaFeaturesProto.JavaFeatures;
import java.util.List;
import org.apache.pulsar.broker.service.schema.exceptions.IncompatibleSchemaException;
import org.apache.pulsar.broker.service.schema.generated.checked.NativeUtf8Checked;
import org.apache.pulsar.broker.service.schema.generated.edition.NativeEditionJavaUtf8;
import org.apache.pulsar.broker.service.schema.generated.enums.NativeLegacyEnum;
import org.apache.pulsar.broker.service.schema.generated.enums.NativeOpenEnum;
import org.apache.pulsar.broker.service.schema.generated.unchecked.NativeUtf8Unchecked;
import org.apache.pulsar.client.impl.schema.ProtobufNativeSchemaUtils;
import org.apache.pulsar.common.policies.data.SchemaCompatibilityStrategy;
import org.apache.pulsar.common.protocol.schema.SchemaData;
import org.apache.pulsar.common.schema.SchemaType;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class ProtobufNativeSchemaCompatibilityReadTest {
    @Test
    public void testEditionAndJavaFeatureRoundTrip() throws Exception {
        DescriptorProtos.getDescriptor();
        for (Edition edition : new Edition[]{Edition.EDITION_2023, Edition.EDITION_2024}) {
            FeatureSet features = FeatureSet.newBuilder()
                    .setUtf8Validation(FeatureSet.Utf8Validation.NONE)
                    .setExtension(JavaFeaturesProto.java_, JavaFeatures.newBuilder()
                            .setUtf8Validation(JavaFeatures.Utf8Validation.VERIFY).build())
                    .build();
            FileDescriptorProto proto = FileDescriptorProto.newBuilder()
                    .setName("edition-" + edition.getNumber() + ".proto")
                    .setPackage("example")
                    .setSyntax("editions")
                    .setEdition(edition)
                    .addDependency(JavaFeaturesProto.getDescriptor().getName())
                    .setOptions(FileOptions.newBuilder().setFeatures(features))
                    .addMessageType(DescriptorProto.newBuilder().setName("Order")
                            .addField(FieldDescriptorProto.newBuilder().setName("name").setNumber(1)
                                    .setType(FieldDescriptorProto.Type.TYPE_STRING)))
                    .build();
            FileDescriptor file = FileDescriptor.buildFrom(proto,
                    new FileDescriptor[]{JavaFeaturesProto.getDescriptor()});
            Descriptor restored = ProtobufNativeSchemaUtils.deserialize(
                    ProtobufNativeSchemaUtils.serialize(file.findMessageTypeByName("Order")));
            assertEquals(restored.getFullName(), "example.Order");
            assertEquals(restored.getFile().toProto().getEdition(), edition);
            assertEquals(restored.getFile().toProto().getOptions().getFeatures().getUtf8Validation(),
                    FeatureSet.Utf8Validation.NONE);
            assertTrue(restored.getFile().toProto().getOptions().getFeatures()
                    .hasExtension(JavaFeaturesProto.java_));
            assertTrue(restored.findFieldByName("name").needsUtf8Check());
        }
    }

    @Test
    public void testGeneratedEditionJavaFeatureOverrideRoundTrip() throws Exception {
        Descriptor generated = NativeEditionJavaUtf8.Order.getDescriptor();
        assertEquals(generated.getFile().toProto().getEdition(), Edition.EDITION_2023);
        assertEquals(generated.getFile().toProto().getOptions().getFeatures().getUtf8Validation(),
                FeatureSet.Utf8Validation.NONE);
        assertTrue(generated.getFile().toProto().getOptions().getFeatures()
                .hasExtension(JavaFeaturesProto.java_));
        assertTrue(generated.findFieldByName("name").needsUtf8Check());

        Descriptor restored = ProtobufNativeSchemaUtils.deserialize(ProtobufNativeSchemaUtils.serialize(generated));
        assertEquals(restored.getFullName(), generated.getFullName());
        assertTrue(restored.findFieldByName("name").needsUtf8Check());

        byte[] valid = NativeEditionJavaUtf8.Order.newBuilder().setName("valid").build().toByteArray();
        assertEquals(DynamicMessage.parseFrom(restored, valid).getField(restored.findFieldByName("name")), "valid");
        byte[] invalid = NativeUtf8Unchecked.Order.newBuilder()
                .setNameBytes(ByteString.copyFrom(new byte[]{(byte) 0xff})).build().toByteArray();
        org.testng.Assert.expectThrows(InvalidProtocolBufferException.class,
                () -> NativeEditionJavaUtf8.Order.parseFrom(invalid));
    }

    @Test
    public void testGeneratedUtf8ValidationWitness() throws Exception {
        Descriptor unchecked = NativeUtf8Unchecked.Order.getDescriptor();
        Descriptor checked = NativeUtf8Checked.Order.getDescriptor();
        assertFalse(unchecked.findFieldByName("name").needsUtf8Check());
        assertTrue(checked.findFieldByName("name").needsUtf8Check());

        byte[] invalid = NativeUtf8Unchecked.Order.newBuilder()
                .setNameBytes(ByteString.copyFrom(new byte[]{(byte) 0xff})).build().toByteArray();
        try {
            NativeUtf8Checked.Order.parseFrom(invalid);
            fail("Generated validating reader must reject non-UTF-8 bytes");
        } catch (InvalidProtocolBufferException expected) {
            assertTrue(expected.getMessage().contains("UTF-8"));
        }

        byte[] valid = NativeUtf8Checked.Order.newBuilder().setName("valid").build().toByteArray();
        assertEquals(NativeUtf8Unchecked.Order.parseFrom(valid).getName(), "valid");
    }

    @Test
    public void testGeneratedLegacyClosedEnumBoundary() throws Exception {
        Descriptor original = NativeLegacyEnum.Order.getDescriptor();
        assertFalse(original.findFieldByName("state").getEnumType().isClosed());
        assertTrue(original.findFieldByName("state").legacyEnumFieldTreatedAsClosed());
        NativeLegacyEnum.Order parsed = NativeLegacyEnum.Order.parseFrom(new byte[]{0x08, 0x7f});
        assertFalse(parsed.hasState());
        assertEquals(parsed.getUnknownFields().getField(1).getVarintList(), List.of(127L));

        FileDescriptorProto proto = original.getFile().toProto().toBuilder()
                .setMessageType(0, original.toProto().toBuilder()
                        .addField(FieldDescriptorProto.newBuilder().setName("new_field").setNumber(3)
                                .setType(FieldDescriptorProto.Type.TYPE_INT32)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)))
                .build();
        FileDescriptor newFile = FileDescriptor.buildFrom(proto,
                new FileDescriptor[]{NativeOpenEnum.getDescriptor()});
        SchemaData oldSchema = schema(original);
        SchemaData newSchema = schema(newFile.findMessageTypeByName("Order"));
        try {
            new ProtobufNativeSchemaAdvancedCompatibilityCheck().checkCompatible(oldSchema, newSchema,
                    SchemaCompatibilityStrategy.BACKWARD);
            fail("Reachable legacy closed enum mismatch must be unsupported");
        } catch (IncompatibleSchemaException e) {
            assertTrue(e.getMessage().contains("UNSUPPORTED_FEATURE"), e.getMessage());
        }
    }

    @Test
    public void testDynamicMapAndNestedReading() throws Exception {
        DescriptorProto entry = DescriptorProto.newBuilder().setName("ItemEntry")
                .setOptions(MessageOptions.newBuilder().setMapEntry(true))
                .addField(FieldDescriptorProto.newBuilder().setName("key").setNumber(1)
                        .setType(FieldDescriptorProto.Type.TYPE_STRING))
                .addField(FieldDescriptorProto.newBuilder().setName("value").setNumber(2)
                        .setType(FieldDescriptorProto.Type.TYPE_INT32)).build();
        DescriptorProto root = DescriptorProto.newBuilder().setName("Order")
                .addNestedType(entry)
                .addField(FieldDescriptorProto.newBuilder().setName("items").setNumber(1)
                        .setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                        .setTypeName(".example.Order.ItemEntry")
                        .setLabel(FieldDescriptorProto.Label.LABEL_REPEATED)).build();
        Descriptor writer = dynamicRoot(root);
        Descriptor reader = ProtobufNativeSchemaUtils.deserialize(ProtobufNativeSchemaUtils.serialize(writer));
        Descriptor writerEntry = writer.findFieldByName("items").getMessageType();
        DynamicMessage item = DynamicMessage.newBuilder(writerEntry)
                .setField(writerEntry.findFieldByName("key"), "book")
                .setField(writerEntry.findFieldByName("value"), 3).build();
        DynamicMessage encoded = DynamicMessage.newBuilder(writer)
                .addRepeatedField(writer.findFieldByName("items"), item).build();
        DynamicMessage decoded = DynamicMessage.parseFrom(reader, encoded.toByteArray());
        assertEquals(decoded.getRepeatedFieldCount(reader.findFieldByName("items")), 1);
        DynamicMessage readEntry = (DynamicMessage) decoded.getRepeatedField(reader.findFieldByName("items"), 0);
        assertEquals(readEntry.getField(readEntry.getDescriptorForType().findFieldByName("key")), "book");
        assertEquals(readEntry.getField(readEntry.getDescriptorForType().findFieldByName("value")), 3);
    }

    private static Descriptor dynamicRoot(DescriptorProto root) throws Exception {
        FileDescriptorProto file = FileDescriptorProto.newBuilder().setName("dynamic.proto")
                .setPackage("example").setSyntax("proto3").addMessageType(root).build();
        return FileDescriptor.buildFrom(file, new FileDescriptor[0]).findMessageTypeByName("Order");
    }

    private static SchemaData schema(Descriptor descriptor) {
        return SchemaData.builder().type(SchemaType.PROTOBUF_NATIVE)
                .data(ProtobufNativeSchemaUtils.serialize(descriptor)).build();
    }
}
