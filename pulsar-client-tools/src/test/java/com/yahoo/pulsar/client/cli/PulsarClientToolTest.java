/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.pulsar.client.cli;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.MalformedURLException;
import java.util.Properties;
import java.lang.InterruptedException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.yahoo.pulsar.broker.service.BrokerTestBase;
import com.yahoo.pulsar.client.cli.PulsarClientTool;

@Test
public class PulsarClientToolTest extends BrokerTestBase {

    @BeforeClass
    @Override
    public void setup() throws Exception {
        super.internalSetup();
    }

    @AfterClass
    @Override
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test(timeOut = 10000)
    public void testInitialzation() throws MalformedURLException, InterruptedException, ExecutionException {
        Properties properties = new Properties();
        properties.setProperty("serviceUrl", brokerUrl.toString());
        properties.setProperty("useTls", "false");

        String topicName = "persistent://property/ns/topic-scale-ns-0/topic";

        int numberOfMessages = 100;

        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

        Future<Boolean> future = executor.submit(() -> {
            PulsarClientTool pulsarClientToolConsumer;
            try {
                pulsarClientToolConsumer = new PulsarClientTool(properties);
                String[] args = { "consume", "-t", "Exclusive", "-s", "sub-name", "-n",
                        Integer.toString(numberOfMessages), "--hex", "-r", "100", topicName };
                Assert.assertEquals(pulsarClientToolConsumer.run(args), 0);
            } catch (MalformedURLException e) {
                Assert.fail("Exception : " + e.getMessage());
                return false;
            }
            return true;
        });

        PulsarClientTool pulsarClientToolProducer = new PulsarClientTool(properties);

        String[] args = { "produce", "--messages", "Have a nice day", "-n", Integer.toString(numberOfMessages), "-r",
                "200", topicName };
        Assert.assertEquals(pulsarClientToolProducer.run(args), 0);

        Assert.assertTrue(future.get(), "Exception occured while running consume task.");
    }
}
