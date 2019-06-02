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
package org.apache.flink.batch.connectors.pulsar;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.pulsar.client.impl.auth.AuthenticationDisabled;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

/**
 * Tests for Pulsar Output Format
 */
public class PulsarOutputFormatTest {

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testPulsarOutputFormatConstructorWhenServiceUrlIsNull() {
        new PulsarOutputFormat(null, "testTopic", new AuthenticationDisabled(), text -> text.toString().getBytes());
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testPulsarOutputFormatConstructorWhenTopicNameIsNull() {
        new PulsarOutputFormat("testServiceUrl", null, new AuthenticationDisabled(), text -> text.toString().getBytes());
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testPulsarOutputFormatConstructorWhenTopicNameIsBlank() {
        new PulsarOutputFormat("testServiceUrl", " ", new AuthenticationDisabled(), text -> text.toString().getBytes());
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testPulsarOutputFormatConstructorWhenServiceUrlIsBlank() {
        new PulsarOutputFormat(" ", "testTopic", new AuthenticationDisabled(), text -> text.toString().getBytes());
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testPulsarOutputFormatConstructorWhenSerializationSchemaIsNull() {
        new PulsarOutputFormat("testServiceUrl", "testTopic", new AuthenticationDisabled(), null);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testPulsarOutputFormatConstructorV2WhenServiceUrlIsNull() {
        ClientConfigurationData clientConf = new ClientConfigurationData();
        clientConf.setServiceUrl(null);

        ProducerConfigurationData producerConf = new ProducerConfigurationData();
        producerConf.setTopicName("testTopic");

        new PulsarOutputFormat(clientConf, producerConf, text -> text.toString().getBytes());
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testPulsarOutputFormatConstructorV2WhenTopicNameIsNull() {
        ClientConfigurationData clientConf = new ClientConfigurationData();
        clientConf.setServiceUrl("testServiceUrl");

        ProducerConfigurationData producerConf = new ProducerConfigurationData();
        producerConf.setTopicName(null);

        new PulsarOutputFormat(clientConf, producerConf, text -> text.toString().getBytes());
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testPulsarOutputFormatConstructorV2WhenTopicNameIsBlank() {
        ClientConfigurationData clientConf = new ClientConfigurationData();
        clientConf.setServiceUrl("testServiceUrl");

        ProducerConfigurationData producerConf = new ProducerConfigurationData();
        producerConf.setTopicName(StringUtils.EMPTY);

        new PulsarOutputFormat(clientConf, producerConf, text -> text.toString().getBytes());
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testPulsarOutputFormatConstructorV2WhenServiceUrlIsBlank() {
        ClientConfigurationData clientConf = new ClientConfigurationData();
        clientConf.setServiceUrl(StringUtils.EMPTY);

        ProducerConfigurationData producerConf = new ProducerConfigurationData();
        producerConf.setTopicName("testTopic");

        new PulsarOutputFormat(clientConf, producerConf, text -> text.toString().getBytes());
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testPulsarOutputFormatConstructorV2WhenSerializationSchemaIsNull() {
        ClientConfigurationData clientConf = new ClientConfigurationData();
        clientConf.setServiceUrl("testServiceUrl");

        ProducerConfigurationData producerConf = new ProducerConfigurationData();
        producerConf.setTopicName("testTopic");
        new PulsarOutputFormat(clientConf, producerConf, null);
    }

    @Test
    public void testPulsarOutputFormatConstructorV2() {
        ClientConfigurationData clientConf = new ClientConfigurationData();
        clientConf.setServiceUrl("testServiceUrl");

        ProducerConfigurationData producerConf = new ProducerConfigurationData();
        producerConf.setTopicName("testTopic");

        PulsarCsvOutputFormat pulsarCsvOutputFormat = new PulsarCsvOutputFormat(clientConf, producerConf);
        assertNotNull(pulsarCsvOutputFormat);
    }

    @Test
    public void testPulsarOutputFormatWithStringSerializationSchema() throws IOException {
        String input = "Wolfgang Amadeus Mozart";
        PulsarOutputFormat pulsarOutputFormat =
                new PulsarOutputFormat("testServiceUrl", "testTopic", new AuthenticationDisabled(),
                        text -> text.toString().getBytes());
        assertNotNull(pulsarOutputFormat);
        byte[] bytes = pulsarOutputFormat.serializationSchema.serialize(input);
        String resultString = IOUtils.toString(bytes, StandardCharsets.UTF_8.toString());
        assertEquals(input, resultString);
    }

    @Test
    public void testPulsarOutputFormatWithCustomSerializationSchema() throws IOException {
        Employee employee = new Employee(1, "Test Employee", "Test Department");
        PulsarOutputFormat pulsarOutputFormat =
                new PulsarOutputFormat("testServiceUrl", "testTopic", new AuthenticationDisabled(),
                        new EmployeeSerializationSchema());
        assertNotNull(pulsarOutputFormat);

        byte[] bytes = pulsarOutputFormat.serializationSchema.serialize(employee);
        String resultString = IOUtils.toString(bytes, StandardCharsets.UTF_8.toString());
        assertEquals(employee.toString(), resultString);
    }

    /**
     * Employee Serialization Schema.
     */
    private class EmployeeSerializationSchema implements SerializationSchema<Employee> {

        @Override
        public byte[] serialize(Employee employee) {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append(employee.id);
            stringBuilder.append(" - ");
            stringBuilder.append(employee.name);
            stringBuilder.append(" - ");
            stringBuilder.append(employee.department);

            return stringBuilder.toString().getBytes();
        }
    }

    /**
     * Data type for Employee Model.
     */
    private class Employee {

        public long id;
        public String name;
        public String department;

        public Employee(long id, String name, String department) {
            this.id = id;
            this.name = name;
            this.department = department;
        }

        @Override
        public String toString() {
            return id + " - " + name + " - " + department;
        }
    }

}
