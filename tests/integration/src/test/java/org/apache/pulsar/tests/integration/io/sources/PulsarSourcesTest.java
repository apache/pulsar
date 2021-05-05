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
package org.apache.pulsar.tests.integration.io.sources;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.SourceStatus;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.tests.integration.containers.DebeziumMongoDbContainer;
import org.apache.pulsar.tests.integration.containers.DebeziumMySQLContainer;
import org.apache.pulsar.tests.integration.containers.DebeziumPostgreSqlContainer;
import org.apache.pulsar.tests.integration.docker.ContainerExecException;
import org.apache.pulsar.tests.integration.docker.ContainerExecResult;
import org.apache.pulsar.tests.integration.io.PulsarIOTestBase;
import org.apache.pulsar.tests.integration.topologies.PulsarCluster;
import org.testcontainers.containers.GenericContainer;
import org.testng.annotations.Test;

import com.google.gson.Gson;

import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import net.jodah.failsafe.Failsafe;

@Slf4j
public class PulsarSourcesTest extends PulsarIOTestBase {

    @Test(groups = "source")
    public void testDebeziumMySqlSourceJson() throws Exception {
        testDebeziumMySqlConnect("org.apache.kafka.connect.json.JsonConverter", true);
    }

    @Test(groups = "source")
    public void testDebeziumMySqlSourceAvro() throws Exception {
        testDebeziumMySqlConnect(
                "org.apache.pulsar.kafka.shade.io.confluent.connect.avro.AvroConverter", false);
    }

    @Test(groups = "source")
    public void testDebeziumPostgreSqlSource() throws Exception {
        testDebeziumPostgreSqlConnect("org.apache.kafka.connect.json.JsonConverter", true);
    }

    @Test(groups = "source")
    public void testDebeziumMongoDbSource() throws Exception{
        testDebeziumMongoDbConnect("org.apache.kafka.connect.json.JsonConverter", true);
    }

    private void testDebeziumMySqlConnect(String converterClassName, boolean jsonWithEnvelope) throws Exception {

        final String tenant = TopicName.PUBLIC_TENANT;
        final String namespace = TopicName.DEFAULT_NAMESPACE;
        final String outputTopicName = "debe-output-topic-name-" + testId.getAndIncrement();
        boolean isJsonConverter = converterClassName.endsWith("JsonConverter");
        final String consumeTopicName = "debezium/mysql-"
                + (isJsonConverter ? "json" : "avro")
                + "/dbserver1.inventory.products";
        final String sourceName = "test-source-debezium-mysql" + (isJsonConverter ? "json" : "avro")
                + "-" + functionRuntimeType + "-" + randomName(8);

        // This is the binlog count that contained in mysql container.
        final int numMessages = 47;

        if (pulsarCluster == null) {
            super.setupCluster();
            super.setupFunctionWorkers();
        }

        @Cleanup
        PulsarClient client = PulsarClient.builder()
            .serviceUrl(pulsarCluster.getPlainTextServiceUrl())
            .build();

        @Cleanup
        PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(pulsarCluster.getHttpServiceUrl()).build();
        initNamespace(admin);

        try {
            SchemaInfo lastSchemaInfo = admin.schemas().getSchemaInfo(consumeTopicName);
            log.info("lastSchemaInfo: {}", lastSchemaInfo == null ? "null" : lastSchemaInfo.toString());
        } catch (Exception e) {
            log.warn("failed to get schemaInfo for topic: {}, exceptions message: {}",
                    consumeTopicName, e.getMessage());
        }

        admin.topics().createNonPartitionedTopic(outputTopicName);

        @Cleanup
        DebeziumMySqlSourceTester sourceTester = new DebeziumMySqlSourceTester(pulsarCluster, converterClassName);
        sourceTester.getSourceConfig().put("json-with-envelope", jsonWithEnvelope);

        // setup debezium mysql server
        DebeziumMySQLContainer mySQLContainer = new DebeziumMySQLContainer(pulsarCluster.getClusterName());
        sourceTester.setServiceContainer(mySQLContainer);

        // prepare the testing environment for source
        prepareSource(sourceTester);

        // submit the source connector
        submitSourceConnector(sourceTester, tenant, namespace, sourceName, outputTopicName);

        // get source info
        getSourceInfoSuccess(sourceTester, tenant, namespace, sourceName);

        // get source status
        Failsafe.with(statusRetryPolicy).run(() -> getSourceStatus(tenant, namespace, sourceName));

        // wait for source to process messages
        Failsafe.with(statusRetryPolicy).run(() ->
                waitForProcessingSourceMessages(tenant, namespace, sourceName, numMessages));

        @Cleanup
        Consumer consumer = client.newConsumer(getSchema(jsonWithEnvelope))
                .topic(consumeTopicName)
                .subscriptionName("debezium-source-tester")
                .subscriptionType(SubscriptionType.Exclusive)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();
        log.info("[debezium mysql test] create consumer finish. converterName: {}", converterClassName);

        // validate the source result
        sourceTester.validateSourceResult(consumer, 9, null, converterClassName);

        // prepare insert event
        sourceTester.prepareInsertEvent();

        // validate the source insert event
        sourceTester.validateSourceResult(consumer, 1, SourceTester.INSERT, converterClassName);

        // prepare update event
        sourceTester.prepareUpdateEvent();

        // validate the source update event
        sourceTester.validateSourceResult(consumer, 1, SourceTester.UPDATE, converterClassName);

        // prepare delete event
        sourceTester.prepareDeleteEvent();

        // validate the source delete event
        sourceTester.validateSourceResult(consumer, 1, SourceTester.DELETE, converterClassName);

        // delete the source
        deleteSource(tenant, namespace, sourceName);

        // get source info (source should be deleted)
        getSourceInfoNotFound(tenant, namespace, sourceName);
    }

    private void testDebeziumPostgreSqlConnect(String converterClassName, boolean jsonWithEnvelope) throws Exception {

        final String tenant = TopicName.PUBLIC_TENANT;
        final String namespace = TopicName.DEFAULT_NAMESPACE;
        final String outputTopicName = "debe-output-topic-name-" + testId.getAndIncrement();
        final String consumeTopicName = "debezium/postgresql/dbserver1.inventory.products";
        final String sourceName = "test-source-debezium-postgersql-" + functionRuntimeType + "-" + randomName(8);


        // This is the binlog count that contained in postgresql container.
        final int numMessages = 26;

        if (pulsarCluster == null) {
            super.setupCluster();
            super.setupFunctionWorkers();
        }

        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(pulsarCluster.getPlainTextServiceUrl())
                .build();

        @Cleanup
        PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(pulsarCluster.getHttpServiceUrl()).build();
        initNamespace(admin);

        admin.topics().createNonPartitionedTopic(consumeTopicName);
        admin.topics().createNonPartitionedTopic(outputTopicName);

        @Cleanup
        Consumer consumer = client.newConsumer(getSchema(jsonWithEnvelope))
                .topic(consumeTopicName)
                .subscriptionName("debezium-source-tester")
                .subscriptionType(SubscriptionType.Exclusive)
                .subscribe();

        @Cleanup
        DebeziumPostgreSqlSourceTester sourceTester = new DebeziumPostgreSqlSourceTester(pulsarCluster);
        sourceTester.getSourceConfig().put("json-with-envelope", jsonWithEnvelope);

        // setup debezium postgresql server
        DebeziumPostgreSqlContainer postgreSqlContainer = new DebeziumPostgreSqlContainer(pulsarCluster.getClusterName());
        sourceTester.setServiceContainer(postgreSqlContainer);

        // prepare the testing environment for source
        prepareSource(sourceTester);

        // submit the source connector
        submitSourceConnector(sourceTester, tenant, namespace, sourceName, outputTopicName);

        // get source info
        getSourceInfoSuccess(sourceTester, tenant, namespace, sourceName);

        // get source status
        Failsafe.with(statusRetryPolicy).run(() -> getSourceStatus(tenant, namespace, sourceName));

        // wait for source to process messages
        Failsafe.with(statusRetryPolicy).run(() ->
                waitForProcessingSourceMessages(tenant, namespace, sourceName, numMessages));

        // validate the source result
        sourceTester.validateSourceResult(consumer, 9, null, converterClassName);

        // prepare insert event
        sourceTester.prepareInsertEvent();

        // validate the source insert event
        sourceTester.validateSourceResult(consumer, 1, SourceTester.INSERT, converterClassName);

        // prepare update event
        sourceTester.prepareUpdateEvent();

        // validate the source update event
        sourceTester.validateSourceResult(consumer, 1, SourceTester.UPDATE, converterClassName);

        // prepare delete event
        sourceTester.prepareDeleteEvent();

        // validate the source delete event
        sourceTester.validateSourceResult(consumer, 1, SourceTester.DELETE, converterClassName);

        // delete the source
        deleteSource(tenant, namespace, sourceName);

        // get source info (source should be deleted)
        getSourceInfoNotFound(tenant, namespace, sourceName);
    }

    private  void testDebeziumMongoDbConnect(String converterClassName, boolean jsonWithEnvelope) throws Exception {

        final String tenant = TopicName.PUBLIC_TENANT;
        final String namespace = TopicName.DEFAULT_NAMESPACE;
        final String outputTopicName = "debe-output-topic-name";
        final String consumeTopicName = "debezium/mongodb/dbserver1.inventory.products";
        final String sourceName = "test-source-connector-"
                + functionRuntimeType + "-name-" + randomName(8);

        // This is the binlog count that contained in mongodb container.
        final int numMessages = 17;

        if (pulsarCluster == null) {
            super.setupCluster();
            super.setupFunctionWorkers();
        }

        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(pulsarCluster.getPlainTextServiceUrl())
                .build();

        @Cleanup
        PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(pulsarCluster.getHttpServiceUrl()).build();
        initNamespace(admin);

        admin.topics().createNonPartitionedTopic(consumeTopicName);
        admin.topics().createNonPartitionedTopic(outputTopicName);

        @Cleanup
        Consumer consumer = client.newConsumer(getSchema(jsonWithEnvelope))
                .topic(consumeTopicName)
                .subscriptionName("debezium-source-tester")
                .subscriptionType(SubscriptionType.Exclusive)
                .subscribe();

        @Cleanup
        DebeziumMongoDbSourceTester sourceTester = new DebeziumMongoDbSourceTester(pulsarCluster);
        sourceTester.getSourceConfig().put("json-with-envelope", jsonWithEnvelope);

        // setup debezium mongodb server
        DebeziumMongoDbContainer mongoDbContainer = new DebeziumMongoDbContainer(pulsarCluster.getClusterName());
        sourceTester.setServiceContainer(mongoDbContainer);
        // prepare the testing environment for source
        prepareSource(sourceTester);

        // submit the source connector
        submitSourceConnector(sourceTester, tenant, namespace, sourceName, outputTopicName);

        // get source info
        getSourceInfoSuccess(sourceTester, tenant, namespace, sourceName);

        // get source status
        Failsafe.with(statusRetryPolicy).run(() -> getSourceStatus(tenant, namespace, sourceName));

        // wait for source to process messages
        Failsafe.with(statusRetryPolicy).run(() ->
                waitForProcessingSourceMessages(tenant, namespace, sourceName, numMessages));

        // validate the source result
        sourceTester.validateSourceResult(consumer, 9, null, converterClassName);

        // prepare insert event
        sourceTester.prepareInsertEvent();

        // validate the source insert event
        sourceTester.validateSourceResult(consumer, 1, SourceTester.INSERT, converterClassName);

        // prepare update event
        sourceTester.prepareUpdateEvent();

        // validate the source update event
        sourceTester.validateSourceResult(consumer, 1, SourceTester.UPDATE, converterClassName);

        // prepare delete event
        sourceTester.prepareDeleteEvent();

        // validate the source delete event
        sourceTester.validateSourceResult(consumer, 1, SourceTester.DELETE, converterClassName);

        // delete the source
        deleteSource(tenant, namespace, sourceName);

        // get source info (source should be deleted)
        getSourceInfoNotFound(tenant, namespace, sourceName);
    }

    private <T extends GenericContainer> void testSource(SourceTester<T> tester)  throws Exception {
        final String tenant = TopicName.PUBLIC_TENANT;
        final String namespace = TopicName.DEFAULT_NAMESPACE;
        final String outputTopicName = "test-source-connector-"
            + functionRuntimeType + "-output-topic-" + randomName(8);
        final String sourceName = "test-source-connector-"
            + functionRuntimeType + "-name-" + randomName(8);
        final int numMessages = 20;

        @Cleanup
        PulsarClient client = PulsarClient.builder()
            .serviceUrl(pulsarCluster.getPlainTextServiceUrl())
            .build();

        @Cleanup
        PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(pulsarCluster.getHttpServiceUrl()).build();
        admin.topics().createNonPartitionedTopic(outputTopicName);

        @Cleanup
        Consumer<String> consumer = client.newConsumer(Schema.STRING)
            .topic(outputTopicName)
            .subscriptionName("source-tester")
            .subscriptionType(SubscriptionType.Exclusive)
            .subscribe();

        // prepare the testing environment for source
        prepareSource(tester);

        // submit the source connector
        submitSourceConnector(tester, tenant, namespace, sourceName, outputTopicName);

        // get source info
        getSourceInfoSuccess(tester, tenant, namespace, sourceName);

        // get source status
        Failsafe.with(statusRetryPolicy).run(() -> getSourceStatus(tenant, namespace, sourceName));

        // produce messages
        Map<String, String> kvs = tester.produceSourceMessages(numMessages);

        // wait for source to process messages
        Failsafe.with(statusRetryPolicy).run(() ->
                waitForProcessingSourceMessages(tenant, namespace, sourceName, numMessages));

        // validate the source result
        validateSourceResult(consumer, kvs);

        // update the source connector
        updateSourceConnector(tester, tenant, namespace, sourceName, outputTopicName);

        // delete the source
        deleteSource(tenant, namespace, sourceName);

        // get source info (source should be deleted)
        getSourceInfoNotFound(tenant, namespace, sourceName);
    }

    protected void prepareSource(SourceTester tester) throws Exception {
        tester.prepareSource();
    }

    protected void submitSourceConnector(SourceTester tester,
                                         String tenant,
                                         String namespace,
                                         String sourceName,
                                         String outputTopicName) throws Exception {
        final String[] commands = {
            PulsarCluster.ADMIN_SCRIPT,
            "source", "create",
            "--tenant", tenant,
            "--namespace", namespace,
            "--name", sourceName,
            "--source-type", tester.sourceType(),
            "--sourceConfig", new Gson().toJson(tester.sourceConfig()),
            "--destinationTopicName", outputTopicName
        };

        log.info("Run command : {}", StringUtils.join(commands, ' '));
        ContainerExecResult result = pulsarCluster.getAnyWorker().execCmd(commands);
        assertTrue(
            result.getStdout().contains("\"Created successfully\""),
            result.getStdout());
    }

    protected void updateSourceConnector(SourceTester tester,
                                         String tenant,
                                         String namespace,
                                         String sourceName,
                                         String outputTopicName) throws Exception {
        final String[] commands = {
                PulsarCluster.ADMIN_SCRIPT,
                "source", "update",
                "--tenant", tenant,
                "--namespace", namespace,
                "--name", sourceName,
                "--source-type", tester.sourceType(),
                "--sourceConfig", new Gson().toJson(tester.sourceConfig()),
                "--destinationTopicName", outputTopicName,
                "--parallelism", "2"
        };

        log.info("Run command : {}", StringUtils.join(commands, ' '));
        ContainerExecResult result = pulsarCluster.getAnyWorker().execCmd(commands);
        assertTrue(
                result.getStdout().contains("\"Updated successfully\""),
                result.getStdout());
    }

    protected void getSourceInfoSuccess(SourceTester tester,
                                        String tenant,
                                        String namespace,
                                        String sourceName) throws Exception {
        final String[] commands = {
            PulsarCluster.ADMIN_SCRIPT,
            "source",
            "get",
            "--tenant", tenant,
            "--namespace", namespace,
            "--name", sourceName
        };

        ContainerExecResult result = pulsarCluster.getAnyWorker().execCmd(commands);
        log.info("Get source info : {}", result.getStdout());
        assertTrue(
            result.getStdout().contains("\"archive\": \"builtin://" + tester.getSourceType() + "\""),
            result.getStdout()
        );
    }

    protected void getSourceStatus(String tenant, String namespace, String sourceName) throws Exception {

        final String[] commands = {
            PulsarCluster.ADMIN_SCRIPT,
            "source",
            "status",
            "--tenant", tenant,
            "--namespace", namespace,
            "--name", sourceName
        };

        final ContainerExecResult result = pulsarCluster.getAnyWorker().execCmd(commands);
        log.info("Get source status : {}", result.getStdout());

        assertEquals(result.getExitCode(), 0);

        final SourceStatus sourceStatus = SourceStatus.decode(result.getStdout());

        assertEquals(sourceStatus.getNumInstances(), 1);
        assertEquals(sourceStatus.getNumRunning(), 1);
        assertEquals(sourceStatus.getInstances().size(), 1);
        assertEquals(sourceStatus.getInstances().get(0).getStatus().isRunning(), true);
        assertEquals(sourceStatus.getInstances().get(0).getStatus().getNumRestarts(), 0);
        assertEquals(sourceStatus.getInstances().get(0).getStatus().getLatestSystemExceptions().size(), 0);

        assertTrue(result.getStdout().contains("\"running\" : true"));

    }

    protected void validateSourceResult(Consumer<String> consumer,
                                        Map<String, String> kvs) throws Exception {
        for (Map.Entry<String, String> kv : kvs.entrySet()) {
            Message<String> msg = consumer.receive();
            assertEquals(kv.getKey(), msg.getKey());
            assertEquals(kv.getValue(), msg.getValue());
        }
    }

    protected void waitForProcessingSourceMessages(String tenant,
                                                   String namespace,
                                                   String sourceName,
                                                   int numMessages) throws Exception {
        final String[] commands = {
            PulsarCluster.ADMIN_SCRIPT,
            "source",
            "status",
            "--tenant", tenant,
            "--namespace", namespace,
            "--name", sourceName
        };

        final ContainerExecResult result = pulsarCluster.getAnyWorker().execCmd(commands);
        log.info("Get source status : {}", result.getStdout());

        assertEquals(result.getExitCode(), 0);

        SourceStatus sourceStatus = SourceStatus.decode(result.getStdout());
        assertEquals(sourceStatus.getNumInstances(), 1);
        assertEquals(sourceStatus.getNumRunning(), 1);
        assertEquals(sourceStatus.getInstances().size(), 1);
        assertEquals(sourceStatus.getInstances().get(0).getInstanceId(), 0);
        assertEquals(sourceStatus.getInstances().get(0).getStatus().isRunning(), true);
        assertTrue(sourceStatus.getInstances().get(0).getStatus().getLastReceivedTime() > 0);
        assertEquals(sourceStatus.getInstances().get(0).getStatus().getNumReceivedFromSource(), numMessages);
        assertEquals(sourceStatus.getInstances().get(0).getStatus().getNumWritten(), numMessages);
        assertEquals(sourceStatus.getInstances().get(0).getStatus().getNumRestarts(), 0);
        assertEquals(sourceStatus.getInstances().get(0).getStatus().getLatestSystemExceptions().size(), 0);
    }

    protected void deleteSource(String tenant, String namespace, String sourceName) throws Exception {

        final String[] commands = {
            PulsarCluster.ADMIN_SCRIPT,
            "source",
            "delete",
            "--tenant", tenant,
            "--namespace", namespace,
            "--name", sourceName
        };

        ContainerExecResult result = pulsarCluster.getAnyWorker().execCmd(commands);
        assertTrue(
            result.getStdout().contains("Delete source successfully"),
            result.getStdout()
        );
        result.assertNoStderr();
    }

    protected void getSourceInfoNotFound(String tenant, String namespace, String sourceName) throws Exception {

        final String[] commands = {
            PulsarCluster.ADMIN_SCRIPT,
            "source",
            "get",
            "--tenant", tenant,
            "--namespace", namespace,
            "--name", sourceName
        };

        try {
            pulsarCluster.getAnyWorker().execCmd(commands);
            fail("Command should have exited with non-zero");
        } catch (ContainerExecException e) {
            assertTrue(e.getResult().getStderr().contains("Reason: Source " + sourceName + " doesn't exist"));
        }
    }
}
