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

import static org.testng.Assert.fail;
import org.apache.pulsar.broker.service.schema.exceptions.IncompatibleSchemaException;
import org.apache.pulsar.common.policies.data.SchemaCompatibilityStrategy;
import org.apache.pulsar.common.protocol.schema.SchemaData;
import org.apache.pulsar.common.schema.SchemaType;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class ExternalSchemaCompatibilityCheckTest {

    private final ExternalSchemaCompatibilityCheck compatibilityCheck = new ExternalSchemaCompatibilityCheck();

    private final SchemaData externalSchemaData = SchemaData.builder()
            .type(SchemaType.EXTERNAL)
            .data(new byte[0])
            .build();

    @DataProvider(name = "otherSchemasProvider")
    public Object[] otherSchemasProvider() {
        return new Object[] {
                SchemaData.builder()
                        .type(SchemaType.JSON)
                        .build(),
                SchemaData.builder()
                        .type(SchemaType.AVRO)
                        .build(),
                SchemaData.builder()
                        .type(SchemaType.PROTOBUF)
                        .build(),
                SchemaData.builder()
                        .type(SchemaType.PROTOBUF_NATIVE)
                        .build()
        };
    }

    @Test(dataProvider = "otherSchemasProvider")
    public void testExternalSchemaCompatibilityCheck(SchemaData schemaData) {
        try {
            compatibilityCheck.checkCompatible(
                    schemaData, externalSchemaData, SchemaCompatibilityStrategy.FULL);
            fail("Expected IncompatibleSchemaException not thrown");
        } catch (IncompatibleSchemaException e) {
            // Expected exception, as external schema is not compatible with the other schemas
        }

        try {
            compatibilityCheck.checkCompatible(
                    externalSchemaData, schemaData, SchemaCompatibilityStrategy.FULL);
            fail("Expected IncompatibleSchemaException not thrown");
        } catch (IncompatibleSchemaException e) {
            // Expected exception, as external schema is not compatible with the other schemas
        }
    }

    @Test
    public void testExternalSchemaData() {
        try {
            SchemaData exSchemaData = SchemaData.builder()
                    .type(SchemaType.EXTERNAL)
                    .data(new byte[0])
                    .build();
            compatibilityCheck.checkCompatible(
                    exSchemaData, externalSchemaData, SchemaCompatibilityStrategy.FULL);
        } catch (IncompatibleSchemaException e) {
            fail("Did not expect IncompatibleSchemaException to be thrown");
        }
    }

}
