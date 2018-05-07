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
/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package org.apache.pulsar.tests.integration;

import com.github.dockerjava.api.DockerClient;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.tests.DockerUtils;
import org.apache.pulsar.tests.PulsarClusterUtils;

import org.jboss.arquillian.testng.Arquillian;
import org.jboss.arquillian.test.api.ArquillianResource;

import org.testng.Assert;
import org.testng.annotations.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestSmoke extends Arquillian {
    private static final Logger LOG = LoggerFactory.getLogger(TestSmoke.class);
    private static byte[] PASSWD = "foobar".getBytes();
    private static String clusterName = "test";

    @ArquillianResource
    DockerClient docker;

    @Test
    public void testPublishAndConsume() throws Exception {
        Assert.assertTrue(PulsarClusterUtils.startAllBrokers(docker, clusterName));
        Assert.assertTrue(PulsarClusterUtils.startAllProxies(docker, clusterName));

        // create property and namespace
        PulsarClusterUtils.runOnAnyBroker(docker, clusterName,
                "/pulsar/bin/pulsar-admin", "tenants",
                "create", "smoke-test", "--allowed-clusters", clusterName,
                "--admin-roles", "smoke-admin");
        PulsarClusterUtils.runOnAnyBroker(docker, clusterName,
                "/pulsar/bin/pulsar-admin", "namespaces",
                "create", "smoke-test/test/ns1");

        String brokerIp = DockerUtils.getContainerIP(
                docker, PulsarClusterUtils.proxySet(docker, clusterName).stream().findAny().get());
        String serviceUrl = "pulsar://" + brokerIp + ":6650";
        String topic = "persistent://smoke-test/test/ns1/topic1";

        try(PulsarClient client = PulsarClient.create(serviceUrl);
            Consumer consumer = client.subscribe(topic, "my-sub");
            Producer producer = client.createProducer(topic)) {
            for (int i = 0; i < 10; i++) {
                producer.send(("smoke-message"+i).getBytes());
            }
            for (int i = 0; i < 10; i++) {
                Message m = consumer.receive();
                Assert.assertEquals("smoke-message"+i, new String(m.getData()));
            }
        }

        PulsarClusterUtils.stopAllProxies(docker, clusterName);
        Assert.assertTrue(PulsarClusterUtils.stopAllBrokers(docker, clusterName));
    }
}
