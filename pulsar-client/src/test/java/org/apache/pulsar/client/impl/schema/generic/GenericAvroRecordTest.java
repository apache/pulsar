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

import static org.testng.Assert.assertEquals;

import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.RecordSchemaBuilder;
import org.apache.pulsar.client.api.schema.SchemaBuilder;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.testng.annotations.Test;

public class GenericAvroRecordTest {

    @Test
    public void testAvroGetNativeRecord() {
        SchemaType schemaType = SchemaType.AVRO;
        RecordSchemaBuilder test = SchemaBuilder
                .record("test");
        test.field("test").type(SchemaType.STRING);
        SchemaInfo build = test.build(schemaType);
        GenericSchemaImpl schema = GenericAvroSchema.of(build);

        GenericRecord record = schema.newRecordBuilder().set("test", "foo").build();
        assertEquals(GenericAvroRecord.class, record.getClass());
        org.apache.avro.generic.GenericRecord nativeRecord = (org.apache.avro.generic.GenericRecord) record.getNativeObject();
        assertEquals("foo", nativeRecord.get("test").toString());
        assertEquals(1, nativeRecord.getSchema().getFields().size());
        assertEquals(schemaType, record.getSchemaType());
    }

}