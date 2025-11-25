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
package org.apache.pulsar.client.impl.schema;

import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertTrue;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.SchemaType;
import org.testng.annotations.Test;

public class AutoProduceBytesSchemaTest {
    @Test
    public void testClone() {
        // test user provided schema
        Schema<String> stringSchema = Schema.STRING;
        AutoProduceBytesSchema<String> schema1 = new AutoProduceBytesSchema<>(stringSchema);
        Schema<byte[]> clone1 = schema1.clone();

        assertNotSame(schema1, clone1);
        assertTrue(clone1 instanceof AutoProduceBytesSchema);
        AutoProduceBytesSchema<String> castedClone1 = (AutoProduceBytesSchema<String>) clone1;
        assertTrue(castedClone1.hasUserProvidedSchema());
        assertTrue(castedClone1.schemaInitialized());
        assertEquals(castedClone1.getSchemaInfo().getType(), SchemaType.STRING);

        // test no user provided schema
        AutoProduceBytesSchema<String> schema2 = new AutoProduceBytesSchema<>();
        Schema<byte[]> clone2 = schema2.clone();

        assertNotSame(schema2, clone2);
        assertTrue(clone2 instanceof AutoProduceBytesSchema);
        AutoProduceBytesSchema<String> castedClone2 = (AutoProduceBytesSchema<String>) clone2;
        assertFalse(castedClone2.hasUserProvidedSchema());
        assertFalse(castedClone2.schemaInitialized());

        // test no user provided schema after setSchema
        AutoProduceBytesSchema<String> schema3 = new AutoProduceBytesSchema<>();
        schema3.setSchema(stringSchema);
        Schema<byte[]> clone3 = schema3.clone();

        assertNotSame(schema3, clone3);
        assertTrue(clone3 instanceof AutoProduceBytesSchema);
        AutoProduceBytesSchema<String> castedClone3 = (AutoProduceBytesSchema<String>) clone3;
        assertFalse(castedClone3.hasUserProvidedSchema());
        assertTrue(castedClone3.schemaInitialized());
        assertEquals(castedClone3.getSchemaInfo().getType(), SchemaType.STRING);
    }

    @Test
    public void testInnerSchemaGetsCloned() {
        Schema<String> stringSchema = spy(Schema.STRING);
        AutoProduceBytesSchema<String> schema = new AutoProduceBytesSchema<>(stringSchema);
        schema.clone();
        verify(stringSchema, times(1)).clone();
    }

}