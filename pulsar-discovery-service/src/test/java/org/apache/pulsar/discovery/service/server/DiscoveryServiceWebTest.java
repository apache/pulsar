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
package org.apache.pulsar.discovery.service.server;

import static org.testng.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;

import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
import org.apache.pulsar.discovery.service.DiscoveryService;
import org.mockito.Mockito;
import org.testng.annotations.Test;

/**
 * 1. starts discovery service a. loads broker list from zk 2. http-client calls multiple http request: GET, PUT and
 * POST. 3. discovery service redirects to appropriate brokers in round-robin 4. client receives unknown host exception
 * with redirected broker
 *
 */
public class DiscoveryServiceWebTest {


    @Test
    public void testWebDiscoveryServiceStarter() throws Exception {
        File testConfigFile = new File("tmp." + System.currentTimeMillis() + ".properties");
        if (testConfigFile.exists()) {
            testConfigFile.delete();
        }
        PrintWriter printWriter = new PrintWriter(new OutputStreamWriter(new FileOutputStream(testConfigFile)));
        printWriter.println("zookeeperServers=z1.pulsar.com,z2.pulsar.com,z3.pulsar.com");
        printWriter.println("configurationStoreServers=z1.pulsar.com,z2.pulsar.com,z3.pulsar.com");
        printWriter.println("webServicePort=8080");
        printWriter.close();
        testConfigFile.deleteOnExit();
        final ServiceConfig config = PulsarConfigurationLoader.create(testConfigFile.getAbsolutePath(), ServiceConfig.class);
        final ServerManager server = new ServerManager(config);
        DiscoveryServiceStarter.startWebService(server, config);
        assertTrue(server.isStarted());
        server.stop();
        testConfigFile.delete();
    }

    /**
     * Test Configuration BackwardCompat for the change from globalzookeeper to configurationStore
     */
    @Test
    public void testConfigurationBackwardCompat() throws Exception {
        DiscoveryService service = Mockito.mock(DiscoveryService.class);

        File testConfigFile = new File("tmp." + System.currentTimeMillis() + ".properties");
        if (testConfigFile.exists()) {
            testConfigFile.delete();
        }
        PrintWriter printWriter = new PrintWriter(new OutputStreamWriter(new FileOutputStream(testConfigFile)));
        printWriter.println("zookeeperServers=z1.pulsar.com,z2.pulsar.com,z3.pulsar.com");
        printWriter.println("globalZookeeperServers=z1.pulsar.com,z2.pulsar.com,z3.pulsar.com");
        printWriter.println("webServicePort=8080");
        printWriter.close();
        testConfigFile.deleteOnExit();

        ServiceConfig config = PulsarConfigurationLoader.create(testConfigFile.getAbsolutePath(), ServiceConfig.class);
        // have zookeeperServers and globalZookeeperServers, config is valid
        // should not throw IllegalArgumentException.
        DiscoveryServiceStarter.checkConfig(config);


        if (testConfigFile.exists()) {
            testConfigFile.delete();
        }
        PrintWriter printWriter2 = new PrintWriter(new OutputStreamWriter(new FileOutputStream(testConfigFile)));
        printWriter2.println("zookeeperServers=z1.pulsar.com,z2.pulsar.com,z3.pulsar.com");
        printWriter2.println("configurationStoreServers=z1.pulsar.com,z2.pulsar.com,z3.pulsar.com");
        printWriter2.println("webServicePort=8080");
        printWriter2.close();
        config = PulsarConfigurationLoader.create(testConfigFile.getAbsolutePath(), ServiceConfig.class);
        // have zookeeperServers and configurationStoreServers, config is valid
        // should not throw IllegalArgumentException.
        DiscoveryServiceStarter.checkConfig(config);


        if (testConfigFile.exists()) {
            testConfigFile.delete();
        }
        PrintWriter printWriter3 = new PrintWriter(new OutputStreamWriter(new FileOutputStream(testConfigFile)));
        printWriter3.println("zookeeperServers=z1.pulsar.com,z2.pulsar.com,z3.pulsar.com");
        printWriter3.println("webServicePort=8080");
        printWriter3.close();
        config = PulsarConfigurationLoader.create(testConfigFile.getAbsolutePath(), ServiceConfig.class);
        // only have zookeeperServers
        // should throw IllegalArgumentException.
        try {
            DiscoveryServiceStarter.checkConfig(config);
        } catch (IllegalArgumentException e) {
            // expected: configure error
        }

        testConfigFile.delete();
    }

}
