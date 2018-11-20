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
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Tests for CsvSerializationSchema
 */
public class CsvSerializationSchemaTest {

    @Test
    public void testPulsarCsvOutputFormatWithSuccessfulCase() throws IOException {
        List<String> fieldNames = Arrays.asList("id", "name");
        Employee employee = new Employee(1, "Mike");
        CsvSerializationSchema<Employee> schema = new CsvSerializationSchema<>(fieldNames);
        byte[] employeeBytes = schema.serialize(employee);
        String str = IOUtils.toString(employeeBytes, StandardCharsets.UTF_8.toString());
        assertEquals(str, "1,Mike");
    }

    @Test
    public void testPulsarCsvOutputFormatWithFieldNamesPropertyHasInvalidProperty() throws IOException {
        List<String> fieldNames = Arrays.asList("id", "invalidPropertyName2");
        Employee employee = new Employee(1, "Mike");
        CsvSerializationSchema<Employee> schema = new CsvSerializationSchema<>(fieldNames);
        byte[] employeeBytes = schema.serialize(employee);
        String str = IOUtils.toString(employeeBytes, StandardCharsets.UTF_8.toString());
        assertEquals(str, "1,");
    }

    @Test(expected = RuntimeException.class)
    public void testPulsarCsvOutputFormatWhenFieldNamesPropertySizeIsBiggerThanPropertySize() throws IOException {
        List<String> fieldNames = Arrays.asList("test-field1", "test-field2", "test-field3");
        CsvSerializationSchema<Employee> schema = new CsvSerializationSchema<>(fieldNames);
        schema.serialize(new Employee(1, "Mike"));
    }

    /**
     * Data type for Employee Model.
     */
    private class Employee {

        public long id;
        public String name;

        public Employee(long id, String name) {
            this.id = id;
            this.name = name;
        }

    }
}
