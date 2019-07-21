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

import org.apache.commons.io.IOUtils;
import org.apache.flink.api.java.tuple.Tuple3;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.testng.Assert.assertEquals;

/**
 * Tests for Csv Serialization Schema
 */
public class CsvSerializationSchemaTest {

    @Test
    public void testCsvSerializationSchemaWithSuccessfulCase() throws IOException {
        Tuple3<Integer, String, String> employee = new Tuple3(1, "Wolfgang Amadeus", "Mozart");
        CsvSerializationSchema schema = new CsvSerializationSchema();
        byte[] rowBytes = schema.serialize(employee);
        String csvContent = IOUtils.toString(rowBytes, StandardCharsets.UTF_8.toString());
        assertEquals(csvContent, "1,Wolfgang Amadeus,Mozart");
    }

    @Test
    public void testCsvSerializationSchemaWithEmptyRecord() throws IOException {
        Tuple3<Integer, String, String> employee = new Tuple3();
        CsvSerializationSchema schema = new CsvSerializationSchema();
        byte[] employeeBytes = schema.serialize(employee);
        String str = IOUtils.toString(employeeBytes, StandardCharsets.UTF_8.toString());
        assertEquals(str, ",,");
    }

}
