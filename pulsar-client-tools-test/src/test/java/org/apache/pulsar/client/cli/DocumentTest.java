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
package org.apache.pulsar.client.cli;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.beust.jcommander.JCommander;
import org.apache.pulsar.broker.service.BrokerTestBase;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Properties;


public class DocumentTest extends BrokerTestBase {

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.resetConfig();
        super.internalCleanup();
    }

    @Test
    public void testSpecifyModuleName() {
        Properties properties = new Properties();
        properties.setProperty("serviceUrl", brokerUrl.toString());
        properties.setProperty("useTls", "false");
        PulsarClientTool tool = new PulsarClientTool(properties);
        String[] args = new String[]{"generate_documentation", "-n", "produce", "-n", "consume"};
        assertEquals(tool.run(args), 0);
        assertEquals(tool.generateDocumentation.getCommandNames().size(), 2);
    }

    @Test
    public void testGenerator() {
        PulsarClientTool pulsarClientTool = new PulsarClientTool(new Properties());
        JCommander commander = pulsarClientTool.commandParser;
        CmdGenerateDocumentation document = new CmdGenerateDocumentation();
        for (Map.Entry<String, JCommander> cmd : commander.getCommands().entrySet()) {
            String res = document.generateDocument(cmd.getKey(), commander);
            assertTrue(res.contains("pulsar-client " + cmd.getKey() + " [options]"));
        }
    }
}
