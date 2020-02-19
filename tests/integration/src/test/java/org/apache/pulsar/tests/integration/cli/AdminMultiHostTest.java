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
package org.apache.pulsar.tests.integration.cli;

import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.tests.integration.containers.BrokerContainer;
import org.apache.pulsar.tests.integration.topologies.PulsarCluster;
import org.apache.pulsar.tests.integration.topologies.PulsarClusterSpec;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Test for admin service url is multi host.
 */
public class AdminMultiHostTest {

    private final String clusterName = "MultiHostTest-" + UUID.randomUUID();
    private final PulsarClusterSpec spec = PulsarClusterSpec.builder().clusterName(clusterName).numBrokers(3).build();
    private PulsarCluster pulsarCluster = null;

    @BeforeMethod
    public void setupCluster() throws Exception {
        pulsarCluster = PulsarCluster.forSpec(spec);
        pulsarCluster.start();
    }

    @AfterMethod
    public void tearDownCluster() {
        if (pulsarCluster != null) {
            pulsarCluster.stop();
            pulsarCluster = null;
        }
    }

    @Test
    public void testAdminMultiHost() throws Exception {
        String hosts = pulsarCluster.getAllBrokersHttpServiceUrl();
        PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(hosts).build();
        try {
            admin.namespaces().getNamespaces("public");
            admin.topics().getList("public/default");
        } catch (PulsarAdminException pae) {
            Assert.fail("Should not throw any exceptions", pae);
        }

        // kill one broker admin should be usable
        BrokerContainer one = pulsarCluster.getBroker(0);
        one.stop();
        waitBrokerDown(admin, 2, 60);
        try {
            admin.namespaces().getNamespaces("public");
            admin.topics().getList("public/default");
        } catch (PulsarAdminException pae) {
            Assert.fail("Should not throw any exceptions", pae);
        }

        // kill another broker
        BrokerContainer two = pulsarCluster.getBroker(1);
        two.stop();
        waitBrokerDown(admin, 1, 60);
        try {
            admin.namespaces().getNamespaces("public");
            admin.topics().getList("public/default");
        } catch (PulsarAdminException pae) {
            Assert.fail("Should not throw any exceptions", pae);
        }
    }

    // Because zookeeper session timeout is 30ms and ticktime is 2ms, so we need wait more than 32ms
    private void waitBrokerDown(PulsarAdmin admin, int expectBrokers, int timeout)
        throws InterruptedException, ExecutionException, TimeoutException {
        FutureTask<Boolean> futureTask = new FutureTask<>(() -> {
            while (admin.brokers().getActiveBrokers(clusterName).size() != expectBrokers) {
                admin.brokers().healthcheck();
                TimeUnit.MILLISECONDS.sleep(1000);
            }
            return true;
        });
        new Thread(futureTask).start();
        futureTask.get(timeout, TimeUnit.SECONDS);
    }
}
