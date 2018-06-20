/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.tests.integration;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.tests.PulsarClusterUtils;
import org.apache.pulsar.tests.integration.cluster.Cluster3Bookie2Broker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

@Ignore
public class TestSmoke {

    private static final Logger LOG = LoggerFactory.getLogger(TestSmoke.class);
    private static byte[] PASSWD = "foobar".getBytes();
    private static String clusterName = "test";
    private Cluster3Bookie2Broker cluster;

    @BeforeClass
    public void setup() throws ExecutionException, InterruptedException, IOException {
        cluster = new Cluster3Bookie2Broker(TestSmoke.class.getSimpleName());
        cluster.start();
        cluster.startAllBrokers();
        cluster.startAllProxies();
    }

    @Test
    public void testPublishAndConsume() throws Exception {
        // create property and namespace
        cluster.execInBroker(PulsarClusterUtils.PULSAR_ADMIN, "tenants",
            "create", "smoke-test", "--allowed-clusters", clusterName,
            "--admin-roles", "smoke-admin");

        cluster.execInBroker(PulsarClusterUtils.PULSAR_ADMIN, "namespaces",
            "create", "smoke-test/test/ns1");

        String serviceUrl = "pulsar://" + cluster.getPulsarProxyIP() + ":" + cluster.getPulsarProxyPort();
        String topic = "persistent://smoke-test/test/ns1/topic1";

        try (PulsarClient client = PulsarClient.create(serviceUrl);
             Consumer consumer = client.subscribe(topic, "my-sub");
             Producer producer = client.createProducer(topic)) {
            for (int i = 0; i < 10; i++) {
                producer.send(("smoke-message" + i).getBytes());
            }
            for (int i = 0; i < 10; i++) {
                Message m = consumer.receive();
                Assert.assertEquals("smoke-message" + i, new String(m.getData()));
            }
        }

    }

    @AfterClass
    public void cleanup() throws Exception {
        cluster.stop();
    }
}
