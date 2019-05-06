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
package org.apache.pulsar.client.impl.schema.generic;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Unit test {@link MultiVersionGenericSchema}.
 */
public class MultiVersionGenericSchemaTest {

    private SchemaProvider<GenericRecord> schemaProvider;
    private MultiVersionGenericSchema schema;

    @BeforeMethod
    public void setup() {
        this.schemaProvider = mock(SchemaProvider.class);
        this.schema = new MultiVersionGenericSchema(schemaProvider);
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testEncode() {
        this.schema.encode(mock(GenericRecord.class));
    }

    @Test
    public void testSupportSchemaVersioning() {
        assertTrue(schema.supportSchemaVersioning());
    }

    @Test
    public void testGetSchemaInfo() {
        assertEquals(new byte[0], schema.getSchemaInfo().getSchema());
    }

    @Test
    public void testDecode() {
        Schema<GenericRecord> mockSchema = mock(Schema.class);
        when(schemaProvider.getSchema(any(byte[].class)))
            .thenReturn(mockSchema);
        when(schemaProvider.getSchema(eq(null)))
            .thenReturn(mockSchema);

        GenericRecord mockRecord = mock(GenericRecord.class);
        when(mockSchema.decode(any(byte[].class), any(byte[].class)))
            .thenReturn(mockRecord);
        when(mockSchema.decode(any(byte[].class)))
            .thenReturn(mockRecord);

        assertSame(
            mockRecord, schema.decode(new byte[0]));
        verify(mockSchema, times(1))
            .decode(any(byte[].class));

        assertSame(
            mockRecord, schema.decode(new byte[0], new byte[0]));
        verify(mockSchema, times(1))
            .decode(any(byte[].class), any(byte[].class));
    }

}
