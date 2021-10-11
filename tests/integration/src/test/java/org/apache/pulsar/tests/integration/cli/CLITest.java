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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import java.util.concurrent.TimeUnit;
import java.util.UUID;
import lombok.Cleanup;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.functions.api.examples.pojo.Tick;
import org.apache.pulsar.tests.integration.containers.BrokerContainer;
import org.apache.pulsar.tests.integration.docker.ContainerExecException;
import org.apache.pulsar.tests.integration.docker.ContainerExecResult;
import org.apache.pulsar.tests.integration.suites.PulsarTestSuite;
import org.apache.pulsar.tests.integration.topologies.PulsarCluster;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Test Pulsar CLI.
 */
public class CLITest extends PulsarTestSuite {

    @Test
    public void testDeprecatedCommands() throws Exception {
        String tenantName = "test-deprecated-commands";

        ContainerExecResult result = pulsarCluster.runAdminCommandOnAnyBroker("--help");
        assertFalse(result.getStdout().isEmpty());
        assertFalse(result.getStdout().contains("Usage: properties "));
        result = pulsarCluster.runAdminCommandOnAnyBroker(
            "properties", "create", tenantName,
            "--allowed-clusters", pulsarCluster.getClusterName(),
            "--admin-roles", "admin"
        );
        assertTrue(result.getStderr().contains("deprecated"));

        result = pulsarCluster.runAdminCommandOnAnyBroker(
            "properties", "list");
        assertTrue(result.getStdout().contains(tenantName));
        result = pulsarCluster.runAdminCommandOnAnyBroker(
            "tenants", "list");
        assertTrue(result.getStdout().contains(tenantName));
    }

    @Test
    public void testGetTopicListCommand() throws Exception {
        ContainerExecResult result;

        final String namespaceLocalName = "list-topics-" + randomName(8);
        result = pulsarCluster.createNamespace(namespaceLocalName);
        final String namespace = "public/" + namespaceLocalName;
        assertEquals(0, result.getExitCode());

        PulsarClient client = PulsarClient.builder().serviceUrl(pulsarCluster.getPlainTextServiceUrl()).build();

        final String persistentTopicName = TopicName.get(
                "persistent",
                NamespaceName.get(namespace),
                "get_topics_mode_" + UUID.randomUUID().toString()).toString();

        final String nonPersistentTopicName = TopicName.get(
                "non-persistent",
                NamespaceName.get(namespace),
                "get_topics_mode_" + UUID.randomUUID().toString()).toString();

        Producer<byte[]> producer1 = client.newProducer()
                .topic(persistentTopicName)
                .create();

        Producer<byte[]> producer2 = client.newProducer()
                .topic(nonPersistentTopicName)
                .create();

        BrokerContainer container = pulsarCluster.getAnyBroker();

        result = container.execCmd(
                PulsarCluster.ADMIN_SCRIPT,
                "topics",
                "list",
                namespace);

        assertTrue(result.getStdout().contains(persistentTopicName));
        assertTrue(result.getStdout().contains(nonPersistentTopicName));

        result = container.execCmd(
                PulsarCluster.ADMIN_SCRIPT,
                "topics",
                "list",
                "--topic-domain",
                "persistent",
                namespace);

        assertTrue(result.getStdout().contains(persistentTopicName));
        assertFalse(result.getStdout().contains(nonPersistentTopicName));

        result = container.execCmd(
                PulsarCluster.ADMIN_SCRIPT,
                "topics",
                "list",
                "--topic-domain",
                "non_persistent",
                namespace);

        assertFalse(result.getStdout().contains(persistentTopicName));
        assertTrue(result.getStdout().contains(nonPersistentTopicName));

        try {
            container.execCmd(
                PulsarCluster.ADMIN_SCRIPT,
                "topics",
                "list",
                "--topic-domain",
                "none",
                namespace);
            fail();
        } catch (ContainerExecException ignore) {
        }

        producer1.close();
        producer2.close();
    }

    @Test
    public void testCreateSubscriptionCommand() throws Exception {
        String topic = "testCreateSubscriptionCommmand";

        String subscriptionPrefix = "subscription-";

        int i = 0;
        for (BrokerContainer container : pulsarCluster.getBrokers()) {
            ContainerExecResult result = container.execCmd(
                PulsarCluster.ADMIN_SCRIPT,
                "topics",
                "create-subscription",
                "persistent://public/default/" + topic,
                "--subscription",
                "" + subscriptionPrefix + i
            );
            result.assertNoOutput();
            i++;
        }
    }

    @Test
    public void testTopicTerminationOnTopicsWithoutConnectedConsumers() throws Exception {
        String topicName = "persistent://public/default/test-topic-termination";
        BrokerContainer container = pulsarCluster.getAnyBroker();
        container.execCmd(
                PulsarCluster.ADMIN_SCRIPT,
                "topics",
                "create",
                topicName);

        ContainerExecResult result = container.execCmd(
            PulsarCluster.CLIENT_SCRIPT,
            "produce",
            "-m",
            "\"test topic termination\"",
            "-n",
            "1",
            topicName);

        assertTrue(result.getStdout().contains("1 messages successfully produced"));

        // terminate the topic
        result = container.execCmd(
            PulsarCluster.ADMIN_SCRIPT,
            "topics",
            "terminate",
            topicName);
        assertTrue(result.getStdout().contains("Topic successfully terminated at"));

        // try to produce should fail
        try {
            pulsarCluster.getAnyBroker().execCmd(PulsarCluster.CLIENT_SCRIPT,
                                                 "produce",
                                                 "-m",
                                                 "\"test topic termination\"",
                                                 "-n",
                                                 "1",
                                                 topicName);
            fail("Command should have exited with non-zero");
        } catch (ContainerExecException e) {
            assertTrue(e.getResult().getStdout().contains("Topic was already terminated"));
        }
    }

    @Test
    public void testPropertiesCLI() throws Exception {
        final BrokerContainer container = pulsarCluster.getAnyBroker();
        final String namespace = "public/default";

        ContainerExecResult result = container.execCmd(
                PulsarCluster.ADMIN_SCRIPT,
                "namespaces",
                "set-property",
                "-k",
                "a",
                "-v",
                "a",
                namespace);
        assertTrue(result.getStdout().isEmpty());

        result = container.execCmd(
                PulsarCluster.ADMIN_SCRIPT,
                "namespaces",
                "get-property",
                "-k",
                "a",
                namespace);
        assertTrue(result.getStdout().contains("a"));

        result = container.execCmd(
                PulsarCluster.ADMIN_SCRIPT,
                "namespaces",
                "remove-property",
                "-k",
                "a",
                namespace);
        assertTrue(result.getStdout().contains("a"));

        result = container.execCmd(
                PulsarCluster.ADMIN_SCRIPT,
                "namespaces",
                "remove-property",
                "-k",
                "a",
                namespace);
        assertTrue(result.getStdout().contains("null"));

        result = container.execCmd(
                PulsarCluster.ADMIN_SCRIPT,
                "namespaces",
                "set-properties",
                "-p",
                "a=a,b=b,c=c",
                namespace);
        assertTrue(result.getStdout().isEmpty());

        result = container.execCmd(
                PulsarCluster.ADMIN_SCRIPT,
                "namespaces",
                "get-properties",
                namespace);
        assertFalse(result.getStdout().isEmpty());

        result = container.execCmd(
                PulsarCluster.ADMIN_SCRIPT,
                "namespaces",
                "clear-properties",
                namespace);
        assertTrue(result.getStdout().isEmpty());
    }

    @Test
    public void testSchemaCLI() throws Exception {
        BrokerContainer container = pulsarCluster.getAnyBroker();
        String topicName = "persistent://public/default/test-schema-cli";

        ContainerExecResult result = container.execCmd(
            PulsarCluster.CLIENT_SCRIPT,
            "produce",
            "-m",
            "\"test topic schema\"",
            "-n",
            "1",
            topicName);
        assertTrue(result.getStdout().contains("1 messages successfully produced"));

        result = container.execCmd(
            PulsarCluster.ADMIN_SCRIPT,
            "schemas",
            "upload",
            topicName,
            "-f",
            "/pulsar/conf/schema_example.conf"
        );
        result.assertNoOutput();

        // get schema
        result = container.execCmd(
            PulsarCluster.ADMIN_SCRIPT,
            "schemas",
            "get",
            topicName);
        assertTrue(result.getStdout().contains("\"type\": \"STRING\""));

        // delete the schema
        result = container.execCmd(
            PulsarCluster.ADMIN_SCRIPT,
            "schemas",
            "delete",
            topicName);
        result.assertNoOutput();

        // get schema again
        try {
            container.execCmd(PulsarCluster.ADMIN_SCRIPT,
                              "schemas",
                              "get",
                              "persistent://public/default/test-schema-cli"
                              );
            fail("Command should have exited with non-zero");
        } catch (ContainerExecException e) {
            assertTrue(e.getResult().getStderr().contains("Reason: HTTP 404 Not Found"));
        }
    }

    @Test
    public void testSetInfiniteRetention() throws Exception {
        ContainerExecResult result;

        String namespace = "get-and-set-retention" + randomName(8);
        pulsarCluster.createNamespace(namespace);

        String[] setCommand = {
            "namespaces", "set-retention", "public/" + namespace,
            "--size", "-1",
            "--time", "-1"
        };

        result = pulsarCluster.runAdminCommandOnAnyBroker(setCommand);
        result.assertNoOutput();

        String[] getCommand = {
            "namespaces", "get-retention", "public/" + namespace
        };

        result = pulsarCluster.runAdminCommandOnAnyBroker(getCommand);
        assertTrue(
            result.getStdout().contains("\"retentionTimeInMinutes\" : -1"),
            result.getStdout());
        assertTrue(
            result.getStdout().contains("\"retentionSizeInMB\" : -1"),
            result.getStdout());
    }

    // authorization related tests

    @Test
    public void testGrantPermissionsAuthorizationDisabled() throws Exception {
        ContainerExecResult result;

        String namespace = "grant-permissions-" + randomName(8);
        result = pulsarCluster.createNamespace(namespace);
        assertEquals(0, result.getExitCode());

        String[] grantCommand = {
            "namespaces", "grant-permission", "public/" + namespace,
            "--actions", "produce",
            "--role", "test-role"
        };
        try {
            pulsarCluster.runAdminCommandOnAnyBroker(grantCommand);
        } catch (ContainerExecException cee) {
            result = cee.getResult();
            assertTrue(result.getStderr().contains("HTTP 501 Not Implemented"), result.getStderr());
        }
    }

    @Test
    public void testJarPojoSchemaUploadAvro() throws Exception {

        ContainerExecResult containerExecResult = pulsarCluster.runAdminCommandOnAnyBroker(
                "schemas",
                "extract", "--jar", "/pulsar/examples/api-examples.jar", "--type", "avro",
                "--classname", "org.apache.pulsar.functions.api.examples.pojo.Tick",
                "persistent://public/default/pojo-avro");

        Assert.assertEquals(containerExecResult.getExitCode(), 0);
        testPublishAndConsume("persistent://public/default/pojo-avro", "avro", Schema.AVRO(Tick.class));
    }

    @Test
    public void testJarPojoSchemaUploadJson() throws Exception {

        ContainerExecResult containerExecResult = pulsarCluster.runAdminCommandOnAnyBroker(
                "schemas",
                "extract", "--jar", "/pulsar/examples/api-examples.jar", "--type", "json",
                "--classname", "org.apache.pulsar.functions.api.examples.pojo.Tick",
                "persistent://public/default/pojo-json");

        Assert.assertEquals(containerExecResult.getExitCode(), 0);
        testPublishAndConsume("persistent://public/default/pojo-json", "json", Schema.JSON(Tick.class));
    }

    private void testPublishAndConsume(String topic, String sub, Schema type) throws PulsarClientException {

        @Cleanup
        PulsarClient client = PulsarClient.builder().serviceUrl(pulsarCluster.getPlainTextServiceUrl()).build();

        @Cleanup
        Producer<Tick> producer = client.newProducer(type)
                .topic(topic + "-message")
                .create();

        @Cleanup
        Consumer<Tick> consumer = client.newConsumer(type)
                .topic(topic + "-message")
                .subscriptionName(sub)
                .subscribe();

        final int numOfMessages = 10;

        for (int i = 1; i < numOfMessages; ++i) {
            producer.send(new Tick(i, "Stock_" + i, 100 + i, 110 + i));
        }

        for (int i = 1; i < numOfMessages; ++i) {
            Tick expected = new Tick(i, "Stock_" + i, 100 + i, 110 + i);
            Message<Tick> receive = consumer.receive(5, TimeUnit.SECONDS);
            Assert.assertEquals(receive.getValue(), expected);
        }
    }

    @Test
    public void testListNonPersistentTopicsCmd() throws Exception {
        String persistentTopic = "test-list-non-persistent-topic";
        ContainerExecResult result = pulsarCluster.runAdminCommandOnAnyBroker("topics", "create", persistentTopic);
        assertEquals(result.getExitCode(), 0);
        HttpGet get = new HttpGet(pulsarCluster.getHttpServiceUrl() + "/admin/v2/non-persistent/public/default");
        try (CloseableHttpClient client = HttpClients.createDefault();
             CloseableHttpResponse response = client.execute(get)) {
            assertFalse(EntityUtils.toString(response.getEntity()).contains(persistentTopic));
        }
    }

    @Test
    public void testGenerateDocForModule() throws Exception {
        String[] moduleNames = {
                "clusters",
                "tenants",
                "brokers",
                "broker-stats",
                "namespaces",
                "topics",
                "schemas",
                "bookies",
                "functions",
                "ns-isolation-policy",
                "resource-quotas",
                "functions",
                "sources",
                "sinks"
        };
        BrokerContainer container = pulsarCluster.getAnyBroker();
        for (int i = 0; i < moduleNames.length; i++) {
            ContainerExecResult result = container.execCmd(
                    PulsarCluster.ADMIN_SCRIPT,
                    "documents", "generate", moduleNames[i]);
            Assert.assertTrue(result.getStdout().contains("------------\n\n# " + moduleNames[i]));
        }
    }

}
