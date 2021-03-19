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
package org.apache.pulsar.broker.service.schema;

import org.apache.pulsar.client.impl.schema.ProtobufNativeSchemaUtils;
import org.apache.pulsar.common.policies.data.SchemaCompatibilityStrategy;
import org.apache.pulsar.common.protocol.schema.SchemaData;
import org.apache.pulsar.common.schema.SchemaType;
import org.testng.Assert;
import org.testng.annotations.Test;

import static com.google.protobuf.Descriptors.Descriptor;

@Test(groups = "broker")
public class ProtobufNativeSchemaCompatibilityCheckTest {

    private static final SchemaData schemaData1 = getSchemaData(org.apache.pulsar.client.api.schema.proto.Test.TestMessage.getDescriptor());

    private static final SchemaData schemaData2 = getSchemaData(org.apache.pulsar.client.api.schema.proto.Test.SubMessage.getDescriptor());

    /**
     * make sure protobuf root message isn't allow change
     */
    @Test
    public void testRootMessageChange() {
        ProtobufNativeSchemaCompatibilityCheck compatibilityCheck = new ProtobufNativeSchemaCompatibilityCheck();
        Assert.assertFalse(compatibilityCheck.isCompatible(schemaData2, schemaData1,
                SchemaCompatibilityStrategy.FULL),
                "Protobuf root message isn't allow change");
    }

    private static SchemaData getSchemaData(Descriptor descriptor) {
        return SchemaData.builder().data(ProtobufNativeSchemaUtils.serialize(descriptor)).type(SchemaType.PROTOBUF_NATIVE).build();
    }
}
