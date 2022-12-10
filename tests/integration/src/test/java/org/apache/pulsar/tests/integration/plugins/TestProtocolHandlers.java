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
package org.apache.pulsar.tests.integration.plugins;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import org.apache.pulsar.tests.integration.containers.BrokerContainer;
import org.apache.pulsar.tests.integration.suites.PulsarTestSuite;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestProtocolHandlers extends PulsarTestSuite {

    private static final Logger LOG = LoggerFactory.getLogger(TestProtocolHandlers.class);

    private static final int PORT = 55000;
    private static final String PREFIX = "PULSAR_PREFIX_";
    public static final int DATA_LENGTH = 127;

    @Override
    public void setupCluster() throws Exception {
        brokerEnvs.put(PREFIX + "messagingProtocols", "echo");
        brokerEnvs.put(PREFIX + "protocolHandlerDirectory", "/pulsar/examples");
        brokerEnvs.put(PREFIX + "echoServerPort", "" + PORT);
        brokerAdditionalPorts.add(PORT);
        super.setupCluster();
    }

    @Test
    public void test() throws Exception {
        BrokerContainer broker = pulsarCluster.getAnyBroker();
        String host = broker.getHost();
        String data = randomName(DATA_LENGTH);
        int mappedPort = broker.getMappedPort(PORT);
        LOG.debug("Sending data to {}:{}", host, mappedPort);
        try (Socket client = new Socket(host, mappedPort);
             OutputStream out = client.getOutputStream();
             InputStream in = client.getInputStream()) {
            LOG.debug("Connection established");
            out.write(data.getBytes());
            LOG.debug("Data sent");
            byte[] response = in.readNBytes(DATA_LENGTH);
            Assert.assertEquals(new String(response), data);
        }
    }



}
