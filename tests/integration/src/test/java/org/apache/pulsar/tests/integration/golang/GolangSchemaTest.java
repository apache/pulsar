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
package org.apache.pulsar.tests.integration.golang;

import lombok.Cleanup;
import lombok.Data;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.apache.pulsar.tests.integration.docker.ContainerExecResult;
import org.apache.pulsar.tests.integration.suites.PulsarTestSuite;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class GolangSchemaTest extends PulsarTestSuite {

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
        Producer<GolangSchemaTest.People> producer = client.newProducer(jsonSchema)
                .topic(topicName)
                .create();

        GolangSchemaTest.People p1 = new GolangSchemaTest.People();
        p1.setId(100);
        p1.setName("pulsar");
        producer.send(p1);

        // Verify Golang can receive the typed message

        ContainerExecResult res = pulsarCluster.getAnyBroker()
                .execCmd("go run pulsar-client-go/examples/consumer/consumer_schema.go", "pulsar://localhost:6650", topicName);
        assertEquals(res.getExitCode(), 0);
    }

    /**
     * Publish from Java and consume from Python
     */
    @Test(dataProvider = "ServiceUrls")
    public void testGolangPublishJavaConsume(String serviceUrl) throws Exception {
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

        Consumer<People> consumer = client.newConsumer(jsonSchema)
                .topic(topicName)
                .subscriptionName("sub-1")
                .subscribe();

        // Verify Golang can receive the typed message

        ContainerExecResult res = pulsarCluster.getAnyBroker()
                .execCmd("go run pulsar-client-go/examples/producer/producer_schema.go", "pulsar://localhost:6650", topicName);
        assertEquals(res.getExitCode(), 0);


        Message<People> peopleMessage = consumer.receive();
        assertEquals(peopleMessage.getValue().id, 100);
        assertEquals(peopleMessage.getValue().name, "pulsar");
    }
}
