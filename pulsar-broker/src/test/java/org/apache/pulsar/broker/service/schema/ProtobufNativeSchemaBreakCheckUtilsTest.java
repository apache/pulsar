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

import static com.google.protobuf.Descriptors.Descriptor;
import lombok.extern.slf4j.Slf4j;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.fail;
import org.apache.pulsar.broker.service.schema.exceptions.ProtoBufCanReadCheckException;
import org.apache.pulsar.client.schema.proto.reader.Reader;
import org.apache.pulsar.client.schema.proto.writer.Writer;
import org.apache.pulsar.client.schema.proto.writerWithAddHasDefaultValue.WriterWithAddHasDefaultValue;
import org.apache.pulsar.client.schema.proto.writerWithAddNoDefaultValue.WriterWithAddNoDefaultValue;
import org.apache.pulsar.client.schema.proto.writerWithFieldNameChange.WriterWithFieldNameChange;
import org.apache.pulsar.client.schema.proto.writerWithRemoveDefaultValueField.WriterWithRemoveDefaultValueField;
import org.apache.pulsar.client.schema.proto.writerWithRemoveNoDefaultValueField.WriterWithRemoveNoDefaultValueField;
import org.apache.pulsar.client.schema.proto.writerAddRequiredField.WriterAddRequiredField;
import org.apache.pulsar.client.schema.proto.writerDeleteRequiredField.WriterDeleteRequiredField;
import org.apache.pulsar.client.schema.proto.writerWithEnumAdd.WriterWithEnumAdd;
import org.apache.pulsar.client.schema.proto.writerWithEnumDelete.WriterWithEnumDelete;
import org.apache.pulsar.client.schema.proto.writerWithFieldNumberChange.WriterWithFieldNumberChange;
import org.apache.pulsar.client.schema.proto.writerWithFieldTypeChange.WriterWithFieldTypeChange;
import org.apache.pulsar.client.schema.proto.writerWithTypeNameChange.WriterWithTypeNameChange;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

@Slf4j
public class ProtobufNativeSchemaBreakCheckUtilsTest {
    private Descriptor readDescriptor;

    @BeforeTest
    private void initReadSchema() {
        this.readDescriptor = Reader.ProtobufSchema.getDescriptor();
        assertNotNull(readDescriptor);
    }

    @Test
    public void testCheckSchemaCompatibilityWithSameVersion() throws ProtoBufCanReadCheckException {
        Descriptor writtenDescriptor = Writer.ProtobufSchema.getDescriptor();
        assertNotNull(writtenDescriptor);
        ProtobufNativeSchemaBreakCheckUtils.checkSchemaCompatibility(writtenDescriptor, readDescriptor);
    }

    @Test
    public void testCheckSchemaCompatibilityWithAddNoDefaultValueField() throws ProtoBufCanReadCheckException {
        Descriptor writerWithAddNoDefaultValue = WriterWithAddNoDefaultValue.ProtobufSchema.getDescriptor();
        assertNotNull(writerWithAddNoDefaultValue);
        ProtobufNativeSchemaBreakCheckUtils.checkSchemaCompatibility(writerWithAddNoDefaultValue,
                readDescriptor);
    }

    @Test
    public void testCheckSchemaCompatibilityWithAddDefaultValueField() throws ProtoBufCanReadCheckException {
        Descriptor writerWithAddHasDefaultValue = WriterWithAddHasDefaultValue.ProtobufSchema.getDescriptor();
        assertNotNull(writerWithAddHasDefaultValue);
        ProtobufNativeSchemaBreakCheckUtils.checkSchemaCompatibility(writerWithAddHasDefaultValue,
                readDescriptor);
    }

    @Test
    public void testCheckSchemaCompatibilityWithRemoveNoDefaultValueField() {
        Descriptor writerWithRemoveNoDefaultValueField = WriterWithRemoveNoDefaultValueField.ProtobufSchema.getDescriptor();
        assertNotNull(writerWithRemoveNoDefaultValueField);
        try {
            ProtobufNativeSchemaBreakCheckUtils.checkSchemaCompatibility(writerWithRemoveNoDefaultValueField,
                    readDescriptor);
            fail("Schema should be incompatible");
        } catch (ProtoBufCanReadCheckException e) {
            log.warn(e.getMessage());
            assertEquals(e.getMessage(), "No default value fields have been added or removed.");
        }
    }

    @Test
    public void testCheckSchemaCompatibilityWithRemoveDefaultValueField() throws ProtoBufCanReadCheckException {
        Descriptor writerWithRemoveDefaultValueField = WriterWithRemoveDefaultValueField.ProtobufSchema.getDescriptor();
        assertNotNull(writerWithRemoveDefaultValueField);
        ProtobufNativeSchemaBreakCheckUtils.checkSchemaCompatibility(writerWithRemoveDefaultValueField,
                readDescriptor);
    }

    @Test
    public void testCheckSchemaCompatibilityWithRequiredFieldChange() {
        // add required field
        Descriptor writtenAddRequiredDescriptor = WriterAddRequiredField.ProtobufSchema.getDescriptor();
        assertNotNull(writtenAddRequiredDescriptor);
        try {
            ProtobufNativeSchemaBreakCheckUtils.checkSchemaCompatibility(writtenAddRequiredDescriptor,
                    readDescriptor);
            fail("Schema should be incompatible");
        } catch (ProtoBufCanReadCheckException e) {
            log.warn(e.getMessage());
            assertEquals(e.getMessage(), "Required Fields have been modified.");
        }

        // delete required field
        Descriptor writtenDeleteRequiredDescriptor = WriterDeleteRequiredField.ProtobufSchema.getDescriptor();
        assertNotNull(writtenDeleteRequiredDescriptor);
        try {
            ProtobufNativeSchemaBreakCheckUtils.checkSchemaCompatibility(writtenDeleteRequiredDescriptor,
                    readDescriptor);
            fail("Schema should be incompatible");
        } catch (ProtoBufCanReadCheckException e) {
            log.warn(e.getMessage());
            assertEquals(e.getMessage(), "Required Fields have been modified.");
        }
    }

    @Test
    public void testCheckSchemaCompatibilityWithFieldTypeChange() throws ProtoBufCanReadCheckException {
        Descriptor writerWithFieldTypeChange = WriterWithFieldTypeChange.ProtobufSchema.getDescriptor();
        assertNotNull(writerWithFieldTypeChange);
        ProtobufNativeSchemaBreakCheckUtils.checkSchemaCompatibility(writerWithFieldTypeChange,
                readDescriptor);
    }

    @Test
    public void testCheckSchemaCompatibilityWithFieldTypeNameChange() {
        Descriptor writerWithTypeNameChange = WriterWithTypeNameChange.ProtobufSchema.getDescriptor();
        assertNotNull(writerWithTypeNameChange);
        try {
            ProtobufNativeSchemaBreakCheckUtils.checkSchemaCompatibility(writerWithTypeNameChange,
                    readDescriptor);
            fail("Schema should be incompatible");
        } catch (ProtoBufCanReadCheckException e) {
            log.warn(e.getMessage());
            assertEquals(e.getMessage(), "The field type name have been changed.");
        }
    }

    @Test
    public void testCheckSchemaCompatibilityWithFieldNumberChange() {
        Descriptor writerWithFieldNumberChange = WriterWithFieldNumberChange.ProtobufSchema.getDescriptor();
        assertNotNull(writerWithFieldNumberChange);
        try {
            ProtobufNativeSchemaBreakCheckUtils.checkSchemaCompatibility(writerWithFieldNumberChange,
                    readDescriptor);
            fail("Schema should be incompatible");
        } catch (ProtoBufCanReadCheckException e) {
            log.warn(e.getMessage());
            assertEquals(e.getMessage(), "The field number of the field have been changed.");
        }
    }

    @Test
    public void testCheckSchemaCompatibilityWithFieldNameChange() {
        Descriptor writerWithFieldNameChange = WriterWithFieldNameChange.ProtobufSchema.getDescriptor();
        assertNotNull(writerWithFieldNameChange);
        try {
            ProtobufNativeSchemaBreakCheckUtils.checkSchemaCompatibility(writerWithFieldNameChange,
                    readDescriptor);
            fail("Schema should be incompatible");
        } catch (ProtoBufCanReadCheckException e) {
            log.warn(e.getMessage());
            assertEquals(e.getMessage(), "The field name of the field have been changed.");
        }
    }

    @Test
    public void testCheckSchemaCompatibilityWithEnumChange() throws ProtoBufCanReadCheckException {
        // add enum field
        Descriptor writerWithEnumAdd = WriterWithEnumAdd.ProtobufSchema.getDescriptor();
        assertNotNull(writerWithEnumAdd);
        ProtobufNativeSchemaBreakCheckUtils.checkSchemaCompatibility(writerWithEnumAdd,
                readDescriptor);

        // delete enum field
        Descriptor writerWithEnumDelete = WriterWithEnumDelete.ProtobufSchema.getDescriptor();
        assertNotNull(writerWithEnumDelete);
        ProtobufNativeSchemaBreakCheckUtils.checkSchemaCompatibility(writerWithEnumDelete,
                readDescriptor);
    }

}
