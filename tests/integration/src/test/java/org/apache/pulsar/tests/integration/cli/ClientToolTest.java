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
package org.apache.pulsar.tests.integration.cli;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;
import org.apache.pulsar.tests.integration.containers.BrokerContainer;
import org.apache.pulsar.tests.integration.containers.ChaosContainer;
import org.apache.pulsar.tests.integration.containers.ProxyContainer;
import org.apache.pulsar.tests.integration.containers.PulsarContainer;
import org.apache.pulsar.tests.integration.containers.ZKContainer;
import org.apache.pulsar.tests.integration.docker.ContainerExecResult;
import org.apache.pulsar.tests.integration.messaging.TopicMessagingBase;
import org.testng.annotations.Test;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ClientToolTest extends TopicMessagingBase {

    private static final int MESSAGE_COUNT = 10;

    @Test
    public void testProduceConsumeThroughBrokers() throws Exception {
        BrokerContainer broker = pulsarCluster.getAnyBroker();

        final String serviceUrl = "pulsar://" + broker.getContainerName() + ":" + PulsarContainer.BROKER_PORT;
        final String topicName = getNonPartitionedTopic("testProduceConsumeBroker", true);
        testProduceConsume(serviceUrl, topicName);
    }

    @Test
    public void testProduceConsumeThroughProxy() throws Exception {
        ProxyContainer proxy = pulsarCluster.getProxy();
        String serviceUrl = "pulsar://" + proxy.getContainerName() + ":" + PulsarContainer.BROKER_PORT;
        final String topicName = getNonPartitionedTopic("testProduceConsumeProxy", true);
        testProduceConsume(serviceUrl, topicName);
    }

    private void testProduceConsume(String serviceUrl, String topicName) throws Exception {
        List<String> data = randomStrings();
        // Using the ZK container as it is separate from brokers, so its environment resembles real world usage more
        ZKContainer<?> clientToolContainer = pulsarCluster.getZooKeeper();
        produce(clientToolContainer, serviceUrl, topicName, data);
        List<String> consumed = consume(clientToolContainer, serviceUrl, topicName);
        assertEquals(consumed, data);
    }

    private static List<String> randomStrings() {
        return IntStream.range(0, MESSAGE_COUNT).mapToObj(i -> randomName(10)).collect(Collectors.toList());
    }

    private void produce(ChaosContainer<?> container, String url, String topic, List<String> messages) throws Exception {
        ContainerExecResult result = container.execCmd("bin/pulsar-client", "--url", url, "produce", topic,
                "-m", String.join(",", messages));
        if (result.getExitCode() != 0) {
            fail("Producing failed. Command output:\n" + result.getStdout()
                    + "\nError output:\n" + result.getStderr());
        }
    }

    private List<String> consume(ChaosContainer<?> container, String url, String topic) throws Exception {
        ContainerExecResult result = container.execCmd("bin/pulsar-client", "--url", url, "consume",
                "-s", randomName(8), "-n", String.valueOf(MESSAGE_COUNT), "-p", "Earliest", topic);
        if (result.getExitCode() != 0) {
            fail("Consuming failed. Command output:\n" + result.getStdout()
                    + "\nError output:\n" + result.getStderr());
        }
        String output = result.getStdout();
        Pattern message = Pattern.compile("----- got message -----\nkey:\\[null\\], properties:\\[\\], content:(.*)");
        Matcher matcher = message.matcher(output);
        List<String> received = new ArrayList<>(MESSAGE_COUNT);
        while (matcher.find()) {
            received.add(matcher.group(1));
        }
        return received;
    }

}
