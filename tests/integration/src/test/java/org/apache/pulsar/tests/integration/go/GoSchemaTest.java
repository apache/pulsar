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
package org.apache.pulsar.tests.integration.go;

import lombok.Cleanup;
import lombok.Data;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.apache.pulsar.tests.integration.docker.ContainerExecResult;
import org.apache.pulsar.tests.integration.suites.PulsarTestSuite;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class GoSchemaTest extends PulsarTestSuite {

    @Data
    static class People {
        String name;
        int id;
    }

    /**
     * Publish from Java and consume from Golang
     */
    @Test(dataProvider = "ServiceUrls")
    public void testJavaPublishGolangConsume(String serviceUrl) throws Exception {
        String nsName = generateNamespaceName();
        pulsarCluster.createNamespace(nsName);

        String topicName = generateTopicName(nsName, "testJavaPublishGolangConsume", true);

        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(serviceUrl)
                .build();

        String string = "{\"type\":\"record\",\"name\":\"Example\",\"namespace\":\"com.test.json\"," +
                "\"fields\":[{\"name\":\"ID\",\"type\":\"int\"},{\"name\":\"Name\",\"type\":\"string\"}]}";
        JSONSchema<People> jsonSchema = JSONSchema.of(SchemaDefinition.<People>builder().
                withPojo(People.class).withAlwaysAllowNull(false).withJsonDef(string).build());

        @Cleanup
        Producer<GoSchemaTest.People> producer = client.newProducer(jsonSchema)
                .topic(topicName)
                .create();

        GoSchemaTest.People p1 = new GoSchemaTest.People();
        p1.setId(100);
        p1.setName("pulsar");
        producer.send(p1);

        ContainerExecResult res = pulsarCluster.getAnyBroker()
                .execCmd("tests/docker-images/latest-version-image/go-test-schema/consumer_schema", "pulsar://localhost:6650", topicName);
        assertEquals(res.getExitCode(), 0);
    }

    /**
     * Publish from Golang and consume from Java
     */
    @Test(dataProvider = "ServiceUrls")
    public void testGolangPublishJavaConsume(String serviceUrl) throws Exception {
        String nsName = generateNamespaceName();
        pulsarCluster.createNamespace(nsName);

        String topicName = generateTopicName(nsName, "testGoPublishJavaConsume", true);

        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(serviceUrl)
                .build();

        String string = "{\"type\":\"record\",\"name\":\"Example\",\"namespace\":\"com.test.json\"," +
                "\"fields\":[{\"name\":\"ID\",\"type\":\"int\"},{\"name\":\"Name\",\"type\":\"string\"}]}";
        JSONSchema<People> jsonSchema = JSONSchema.of(SchemaDefinition.<People>builder().
                withPojo(People.class).withAlwaysAllowNull(false).withJsonDef(string).build());

        @Cleanup
        Consumer<People> consumer = client.newConsumer(jsonSchema)
                .topic(topicName)
                .subscriptionName("sub-1")
                .subscribe();

        ContainerExecResult res = pulsarCluster.getAnyBroker()
                .execCmd("tests/docker-images/latest-version-image/go-test-schema/producer_schema", "pulsar://localhost:6650", topicName);
        assertEquals(res.getExitCode(), 0);


        // Verify Golang can receive the typed message
        Message<People> peopleMessage = consumer.receive();
        assertEquals(peopleMessage.getValue().id, 100);
        assertEquals(peopleMessage.getValue().name, "pulsar");
    }
}
