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
package org.apache.flink.batch.connectors.pulsar.serialization;

import org.apache.avro.generic.GenericRecord;
import org.apache.flink.avro.generated.NasaMission;
import org.apache.flink.formats.avro.AvroDeserializationSchema;
import org.testng.annotations.Test;

import java.io.IOException;

import static org.testng.Assert.assertEquals;

/**
 * Tests for Avro Serialization Schema
 */
public class AvroSerializationSchemaTest {

    @Test
    public void testAvroSerializationSchemaWithSuccessfulCase() throws IOException {
        NasaMission nasaMission = NasaMission.newBuilder().setId(1).setName("Mercury program").setStartYear(1959).setEndYear(1963).build();
        AvroSerializationSchema schema = new AvroSerializationSchema();
        byte[] rowBytes = schema.serialize(nasaMission);

        AvroDeserializationSchema<GenericRecord> deserializationSchema = AvroDeserializationSchema.forGeneric(nasaMission.getSchema());
        GenericRecord genericRecord = deserializationSchema.deserialize(rowBytes);

        assertEquals(nasaMission.getId(), genericRecord.get("id"));
        assertEquals(nasaMission.getName(), genericRecord.get("name").toString());
        assertEquals(nasaMission.getStartYear(), genericRecord.get("start_year"));
        assertEquals(nasaMission.getEndYear(), genericRecord.get("end_year"));
    }

    @Test
    public void testAvroSerializationSchemaWithEmptyRecord() throws IOException {
        NasaMission nasaMission = NasaMission.newBuilder().setId(1).setName("Mercury program").setStartYear(null).setEndYear(null).build();
        AvroSerializationSchema schema = new AvroSerializationSchema();
        byte[] rowBytes = schema.serialize(nasaMission);

        AvroDeserializationSchema<GenericRecord> deserializationSchema = AvroDeserializationSchema.forGeneric(nasaMission.getSchema());
        GenericRecord genericRecord = deserializationSchema.deserialize(rowBytes);

        assertEquals(nasaMission.getId(), genericRecord.get("id"));
        assertEquals(nasaMission.getName(), genericRecord.get("name").toString());
        assertEquals(null, genericRecord.get("start_year"));
        assertEquals(null, genericRecord.get("end_year"));
    }

}
