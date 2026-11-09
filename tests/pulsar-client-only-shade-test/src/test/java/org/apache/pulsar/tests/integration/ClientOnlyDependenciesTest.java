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
package org.apache.pulsar.tests.integration;

import static org.testng.Assert.assertThrows;
import java.net.ServerSocket;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.testng.annotations.Test;

/**
 * Runs the shaded client with only its published dependencies. No broker is started, so creating a
 * producer or consumer is expected to fail to connect; a missing dependency fails with an Error instead.
 */
public class ClientOnlyDependenciesTest {

    @Test
    public void createProducerAndConsumerWithOnlyPublishedDependencies() throws Exception {
        try (PulsarClient client = PulsarClient.builder()
                .serviceUrl("pulsar://localhost:" + unusedPort())
                .operationTimeout(1, TimeUnit.SECONDS)
                .build()) {
            String topic = "persistent://public/default/client-only-dependencies";
            assertThrows(PulsarClientException.class, () -> client.newProducer().topic(topic).create());
            assertThrows(PulsarClientException.class,
                    () -> client.newConsumer().topic(topic).subscriptionName("sub").subscribe());
        }
    }

    private static int unusedPort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
