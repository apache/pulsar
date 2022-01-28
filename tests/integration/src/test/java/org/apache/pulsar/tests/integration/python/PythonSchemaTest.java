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
package org.apache.pulsar.tests.integration.python;

import static org.testng.Assert.assertEquals;

import java.util.function.Supplier;
import lombok.Cleanup;
import lombok.Data;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.tests.integration.docker.ContainerExecResult;
import org.apache.pulsar.tests.integration.suites.PulsarTestSuite;
import org.testng.annotations.Test;

/**
 * Test pulsar Python/Java schema interoperability
 */
public class PythonSchemaTest extends PulsarTestSuite {

    @Data
    static class Example1 {
        private Integer x;
        private Long y;
    }

    @Data
    static class Example2 {
        private String a;
        private int b;
    }

    /**
     * Publish from Java and consume from Python
     */
    @Test(dataProvider = "ServiceUrls")
    public void testJavaPublishPythonConsume(Supplier<String> serviceUrl) throws Exception {
        String nsName = generateNamespaceName();
        pulsarCluster.createNamespace(nsName);

        String topicName = generateTopicName(nsName, "testJavaPublishPythonConsume", true);

        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(serviceUrl.get())
                .build();

        // Create subscription to retain data
        client.newConsumer(Schema.JSON(Example1.class))
                .topic(topicName)
                .subscriptionName("my-subscription")
                .subscribe()
                .close();

        @Cleanup
        Producer<Example1> producer = client.newProducer(Schema.JSON(Example1.class))
                .topic(topicName)
                .create();

        Example1 e1 = new Example1();
        e1.setX(1);
        e1.setY(2L);
        producer.send(e1);

        // Verify Python can receive the typed message

        ContainerExecResult res = pulsarCluster.getAnyBroker()
                .execCmd("/pulsar/examples/python-examples/consumer_schema.py", "pulsar://localhost:6650", topicName);
        assertEquals(res.getExitCode(), 0);
    }

    /**
     * Publish from Java and consume from Python
     */
    @Test(dataProvider = "ServiceUrls")
    public void testPythonPublishJavaConsume(Supplier<String> serviceUrl) throws Exception {
        String nsName = generateNamespaceName();
        pulsarCluster.createNamespace(nsName);

        String topicName = generateTopicName(nsName, "testPythonPublishJavaConsume", true);

        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(serviceUrl.get())
                .build();

        @Cleanup
        Consumer<Example2> consumer = client.newConsumer(Schema.AVRO(Example2.class))
                .topic(topicName)
                .subscriptionName("test-sub")
                .subscribe();

        // Verify Python can receive the typed message

        ContainerExecResult res = pulsarCluster.getAnyBroker()
                .execCmd("/pulsar/examples/python-examples/producer_schema.py", "pulsar://localhost:6650", topicName);
        assertEquals(res.getExitCode(), 0);

        Message<Example2> msg = consumer.receive();
        Example2 e2 = msg.getValue();
        assertEquals(e2.a, "Hello");
        assertEquals(e2.b, 1);
    }
}