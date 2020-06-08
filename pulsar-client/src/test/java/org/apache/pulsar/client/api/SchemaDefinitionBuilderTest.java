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
package org.apache.pulsar.client.api;

import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.api.schema.SchemaReader;
import org.apache.pulsar.client.api.schema.SchemaWriter;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

public class SchemaDefinitionBuilderTest {

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testVerificationWithoutParams() {
        SchemaDefinition.builder().build();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testVerificationWithPojoAndJsonDef() {
        SchemaDefinition.builder().withJsonDef("{}").withPojo(Object.class).build();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testVerificationWithReaderOnly() {
        SchemaReader<Object> reader = Mockito.mock(SchemaReader.class);
        SchemaDefinition.builder().withSchemaReader(reader).build();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testVerificationWithWriterOnly() {
        SchemaWriter<Object> writer = Mockito.mock(SchemaWriter.class);
        SchemaDefinition.builder().withSchemaWriter(writer).build();
    }

    @Test
    public void testVerificationWithJsonDef() {
        SchemaDefinition.builder().withJsonDef("{}").build();
    }

    @Test
    public void testVerificationWithPojo() {
        SchemaDefinition.builder().withPojo(Object.class).build();
    }

    @Test
    public void testVerificationWithPojoAndReaderAndWriter() {
        SchemaReader<Object> reader = Mockito.mock(SchemaReader.class);
        SchemaWriter<Object> writer = Mockito.mock(SchemaWriter.class);
        SchemaDefinition<Object> definition = SchemaDefinition.builder()
                .withPojo(Object.class)
                .withSchemaReader(reader)
                .withSchemaWriter(writer)
                .build();
        Assert.assertNotNull(definition);
    }

    @Test
    public void testVerificationWithJsonDefAndReaderAndWriter() {
        SchemaReader<Object> reader = Mockito.mock(SchemaReader.class);
        SchemaWriter<Object> writer = Mockito.mock(SchemaWriter.class);
        SchemaDefinition<Object> definition = SchemaDefinition.builder()
                .withJsonDef("{}")
                .withSchemaReader(reader)
                .withSchemaWriter(writer)
                .build();
        Assert.assertNotNull(definition);
    }

    @Test
    public void testReaderWriterRegistration() {
        SchemaReader<Object> reader = Mockito.mock(SchemaReader.class);
        SchemaWriter<Object> writer = Mockito.mock(SchemaWriter.class);
        SchemaDefinition<Object> definition = SchemaDefinition.builder()
                .withPojo(Object.class)
                .withSchemaReader(reader)
                .withSchemaWriter(writer)
                .build();
        Assert.assertTrue(definition.getSchemaReaderOpt().isPresent());
        Assert.assertTrue(definition.getSchemaWriterOpt().isPresent());
        Assert.assertSame(reader, definition.getSchemaReaderOpt().get());
        Assert.assertSame(writer, definition.getSchemaWriterOpt().get());
    }
}
