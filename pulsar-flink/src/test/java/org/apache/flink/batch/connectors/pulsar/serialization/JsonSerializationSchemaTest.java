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
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.testng.Assert.assertEquals;

/**
 * Tests for Json Serialization Schema
 */
public class JsonSerializationSchemaTest {

    @Test
    public void testJsonSerializationSchemaWithSuccessfulCase() throws IOException {
        Employee employee = new Employee(1, "Test Name");
        JsonSerializationSchema schema = new JsonSerializationSchema();
        byte[] rowBytes = schema.serialize(employee);
        String jsonContent = IOUtils.toString(rowBytes, StandardCharsets.UTF_8.toString());
        assertEquals(jsonContent, "{\"id\":1,\"name\":\"Test Name\"}");
    }

    @Test
    public void testJsonSerializationSchemaWithEmptyRecord() throws IOException {
        Employee employee = new Employee();
        JsonSerializationSchema schema = new JsonSerializationSchema();
        byte[] employeeBytes = schema.serialize(employee);
        String jsonContent = IOUtils.toString(employeeBytes, StandardCharsets.UTF_8.toString());
        assertEquals(jsonContent, "{\"id\":0,\"name\":null}");
    }

    @Test(expectedExceptions = RuntimeException.class)
    public void testJsonSerializationSchemaWithNotSerializableObject() {
        NotSerializableObject notSerializableObject = new NotSerializableObject();
        JsonSerializationSchema schema = new JsonSerializationSchema();
        schema.serialize(notSerializableObject);
    }

    /**
     * Employee data model
     */
    private static class Employee {

        private long id;
        private String name;

        public Employee() {
        }

        public Employee(long id, String name) {
            this.id = id;
            this.name = name;
        }

        public long getId() {
            return id;
        }

        public String getName() {
            return name;
        }

    }

    /**
     * Not Serializable Object due to not having any public property
     */
    private static class NotSerializableObject {

        private long id;
        private String name;

    }
}
