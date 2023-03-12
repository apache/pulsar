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
import static org.testng.Assert.assertNotNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.impl.schema.ProtobufNativeSchemaUtils;
import org.apache.pulsar.client.schema.proto.reader.Reader;
import org.apache.pulsar.client.schema.proto.writerWithAddHasDefaultValue.WriterWithAddHasDefaultValue;
import org.apache.pulsar.client.schema.proto.writerWithAddNoDefaultValue.WriterWithAddNoDefaultValue;
import org.apache.pulsar.client.schema.proto.writerWithRemoveDefaultValueField.WriterWithRemoveDefaultValueField;
import org.apache.pulsar.client.schema.proto.writerWithRemoveNoDefaultValueField.WriterWithRemoveNoDefaultValueField;
import org.apache.pulsar.common.policies.data.SchemaCompatibilityStrategy;
import org.apache.pulsar.common.protocol.schema.SchemaData;
import org.apache.pulsar.common.schema.SchemaType;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import java.util.Arrays;
import java.util.Collections;

@Slf4j
@Test(groups = "broker")
public class ProtobufNativeSchemaAdvanceCompatibilityCheckTest {
    private SchemaCompatibilityCheck schemaCompatibilityCheck;

    private static final SchemaData schemaData1 = getSchemaData(org.apache.pulsar.client.api.schema.proto.Test.TestMessage.getDescriptor());

    private static final SchemaData schemaData2 = getSchemaData(org.apache.pulsar.client.api.schema.proto.Test.SubMessage.getDescriptor());

    private static final SchemaData reader = getSchemaData(Reader.ProtobufSchema.getDescriptor());
    private static final SchemaData reader2 = getSchemaData(Reader.ProtobufSchema.getDescriptor());
    private static final SchemaData writerWithAddHasDefaultValue = getSchemaData(WriterWithAddHasDefaultValue.ProtobufSchema.getDescriptor());
    private static final SchemaData writerWithAddNoDefaultValue = getSchemaData(WriterWithAddNoDefaultValue.ProtobufSchema.getDescriptor());
    private static final SchemaData writerWithRemoveNoDefaultValueField = getSchemaData(WriterWithRemoveNoDefaultValueField.ProtobufSchema.getDescriptor());
    private static final SchemaData writerWithRemoveDefaultValueField = getSchemaData(WriterWithRemoveDefaultValueField.ProtobufSchema.getDescriptor());

    @BeforeTest
    private void initReadSchema() {
        this.schemaCompatibilityCheck = new ProtobufNativeSchemaAdvanceCompatibilityCheck();
        assertNotNull(schemaCompatibilityCheck);
    }

    /**
     * make sure protobuf root message isn't allow change
     */
    @Test
    public void testRootMessageChange() {
        Assert.assertFalse(schemaCompatibilityCheck.isCompatible(schemaData2, schemaData1,
                SchemaCompatibilityStrategy.FULL),
                "Protobuf root message isn't allow change");
    }

    @Test
    public void testBackwardCompatibility() {
        // adding a field with default is backwards compatible
        log.info("adding a field with default is backwards compatible");
        Assert.assertTrue(schemaCompatibilityCheck.isCompatible(reader, writerWithAddHasDefaultValue,
                        SchemaCompatibilityStrategy.BACKWARD),
                "adding a field with default is backwards compatible");
        // adding a field without default is NOT backwards compatible
        log.info("adding a field without default is NOT backwards compatible");
        Assert.assertFalse(schemaCompatibilityCheck.isCompatible(reader, writerWithAddNoDefaultValue,
                        SchemaCompatibilityStrategy.BACKWARD),
                "adding a field without default is NOT backwards compatible");
        // removing a field with no default is backwards compatible
        log.info("removing a field with no default is backwards compatible");
        Assert.assertTrue(schemaCompatibilityCheck.isCompatible(reader, writerWithRemoveNoDefaultValueField,
                        SchemaCompatibilityStrategy.BACKWARD),
                "removing a field with no default is backwards compatible");
        // removing a field with default value is backwards compatible
        log.info("removing a field with default value is backwards compatible");
        Assert.assertTrue(schemaCompatibilityCheck.isCompatible(reader, writerWithRemoveDefaultValueField,
                        SchemaCompatibilityStrategy.BACKWARD),
                "removing a field with default value is backwards compatible");
    }

    @Test
    public void testForwardCompatibility() {
        // adding a field with default is forward compatible
        log.info("adding a field with default is forward compatible");
        Assert.assertTrue(schemaCompatibilityCheck.isCompatible(reader, writerWithAddHasDefaultValue,
                        SchemaCompatibilityStrategy.FORWARD),
                "adding a field with default is forward compatible");
        // adding a field without default is forward compatible
        log.info("adding a field without default is forward compatible");
        Assert.assertTrue(schemaCompatibilityCheck.isCompatible(reader, writerWithAddNoDefaultValue,
                        SchemaCompatibilityStrategy.FORWARD),
                "adding a field without default is forward compatible");
        // removing a field with no default is not forward compatible
        log.info("removing a field with no default is not forward compatible");
        Assert.assertFalse(schemaCompatibilityCheck.isCompatible(reader, writerWithRemoveNoDefaultValueField,
                        SchemaCompatibilityStrategy.FORWARD),
                "removing a field with no default is not forward compatible");
        // removing a field with default value is forward compatible
        log.info("removing a field with default value is forward compatible");
        Assert.assertTrue(schemaCompatibilityCheck.isCompatible(reader, writerWithRemoveDefaultValueField,
                        SchemaCompatibilityStrategy.FORWARD),
                "removing a field with default value is forward compatible");
    }

    @Test
    public void testFullCompatibility() {
        Assert.assertTrue(schemaCompatibilityCheck.isCompatible(reader, writerWithAddHasDefaultValue,
                        SchemaCompatibilityStrategy.FULL),
                "adding a field with default fully compatible");
        Assert.assertFalse(schemaCompatibilityCheck.isCompatible(reader, writerWithAddNoDefaultValue,
                        SchemaCompatibilityStrategy.FULL),
                "adding a field without default is not fully compatible");
        Assert.assertTrue(schemaCompatibilityCheck.isCompatible(reader, writerWithRemoveDefaultValueField,
                        SchemaCompatibilityStrategy.FULL),
                "removing a field with default is fully compatible");
        Assert.assertFalse(schemaCompatibilityCheck.isCompatible(reader, writerWithRemoveNoDefaultValueField,
                        SchemaCompatibilityStrategy.FULL),
                "removing a field with no default is not fully compatible");
    }

    @Test
    public void testBackwardTransitive() {
        Assert.assertTrue(schemaCompatibilityCheck.isCompatible(Arrays.asList(reader, reader2), writerWithAddHasDefaultValue,
                SchemaCompatibilityStrategy.BACKWARD_TRANSITIVE));
        Assert.assertTrue(schemaCompatibilityCheck.isCompatible(Arrays.asList(reader, reader2, writerWithAddHasDefaultValue),
                writerWithRemoveNoDefaultValueField, SchemaCompatibilityStrategy.BACKWARD_TRANSITIVE));

        Assert.assertTrue(schemaCompatibilityCheck.isCompatible(Collections.singletonList(reader), writerWithRemoveDefaultValueField,
                SchemaCompatibilityStrategy.BACKWARD_TRANSITIVE));

        Assert.assertTrue(schemaCompatibilityCheck.isCompatible(Arrays.asList(reader, writerWithAddNoDefaultValue),
                writerWithAddNoDefaultValue, SchemaCompatibilityStrategy.BACKWARD));
        Assert.assertFalse(schemaCompatibilityCheck.isCompatible(Arrays.asList(reader, writerWithAddNoDefaultValue),
                writerWithAddNoDefaultValue, SchemaCompatibilityStrategy.BACKWARD_TRANSITIVE));
    }

    @Test
    public void testForwardTransitive() {
        Assert.assertTrue(schemaCompatibilityCheck.isCompatible(Arrays.asList(reader, reader2), writerWithRemoveDefaultValueField,
                SchemaCompatibilityStrategy.FORWARD_TRANSITIVE));
        Assert.assertTrue(schemaCompatibilityCheck.isCompatible(Arrays.asList(reader, reader2, writerWithRemoveDefaultValueField),
                writerWithAddHasDefaultValue, SchemaCompatibilityStrategy.FORWARD_TRANSITIVE));
        Assert.assertTrue(schemaCompatibilityCheck.isCompatible(Arrays.asList(reader, writerWithRemoveNoDefaultValueField),
                writerWithRemoveNoDefaultValueField, SchemaCompatibilityStrategy.FORWARD));
        Assert.assertFalse(schemaCompatibilityCheck.isCompatible(Arrays.asList(reader, writerWithRemoveNoDefaultValueField),
                writerWithRemoveNoDefaultValueField, SchemaCompatibilityStrategy.FORWARD_TRANSITIVE));
    }

    @Test
    public void testFullTransitive() {
        Assert.assertTrue(schemaCompatibilityCheck.isCompatible(Arrays.asList(reader, writerWithRemoveDefaultValueField),
                writerWithAddHasDefaultValue, SchemaCompatibilityStrategy.FULL));
        Assert.assertFalse(schemaCompatibilityCheck.isCompatible(Arrays.asList(reader, reader2),
                writerWithAddNoDefaultValue, SchemaCompatibilityStrategy.FULL_TRANSITIVE));
    }

    private static SchemaData getSchemaData(Descriptor descriptor) {
        return SchemaData.builder().data(ProtobufNativeSchemaUtils.serialize(descriptor)).type(SchemaType.PROTOBUF_NATIVE).build();
    }

}
