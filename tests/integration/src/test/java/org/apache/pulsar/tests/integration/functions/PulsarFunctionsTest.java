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
package org.apache.pulsar.tests.integration.functions;

import com.google.gson.Gson;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.schema.AvroSchema;
import org.apache.pulsar.client.impl.schema.KeyValueSchema;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.FunctionStats;
import org.apache.pulsar.common.policies.data.FunctionStatus;
import org.apache.pulsar.common.policies.data.SinkStatus;
import org.apache.pulsar.common.policies.data.SourceStatus;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.functions.api.examples.AutoSchemaFunction;
import org.apache.pulsar.functions.api.examples.AvroSchemaTestFunction;
import org.apache.pulsar.functions.api.examples.pojo.AvroTestObject;
import org.apache.pulsar.functions.api.examples.serde.CustomObject;
import org.apache.pulsar.tests.integration.containers.DebeziumMongoDbContainer;
import org.apache.pulsar.tests.integration.containers.DebeziumMySQLContainer;
import org.apache.pulsar.tests.integration.containers.DebeziumPostgreSqlContainer;
import org.apache.pulsar.tests.integration.docker.ContainerExecException;
import org.apache.pulsar.tests.integration.docker.ContainerExecResult;
import org.apache.pulsar.tests.integration.functions.utils.CommandGenerator;
import org.apache.pulsar.tests.integration.functions.utils.CommandGenerator.Runtime;
import org.apache.pulsar.tests.integration.io.CassandraSinkTester;
import org.apache.pulsar.tests.integration.io.DebeziumMongoDbSourceTester;
import org.apache.pulsar.tests.integration.io.DebeziumMySqlSourceTester;
import org.apache.pulsar.tests.integration.io.DebeziumPostgreSqlSourceTester;
import org.apache.pulsar.tests.integration.io.ElasticSearchSinkTester;
import org.apache.pulsar.tests.integration.io.HdfsSinkTester;
import org.apache.pulsar.tests.integration.io.JdbcPostgresSinkTester;
import org.apache.pulsar.tests.integration.io.JdbcPostgresSinkTester.Foo;
import org.apache.pulsar.tests.integration.io.KafkaSinkTester;
import org.apache.pulsar.tests.integration.io.KafkaSourceTester;
import org.apache.pulsar.tests.integration.io.RabbitMQSinkTester;
import org.apache.pulsar.tests.integration.io.RabbitMQSourceTester;
import org.apache.pulsar.tests.integration.io.SinkTester;
import org.apache.pulsar.tests.integration.io.SourceTester;
import org.apache.pulsar.tests.integration.topologies.FunctionRuntimeType;
import org.apache.pulsar.tests.integration.topologies.PulsarCluster;
import org.assertj.core.api.Assertions;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.shaded.com.google.common.collect.Sets;
import org.testng.annotations.Test;
import org.testng.collections.Maps;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

/**
 * A test base for testing sink.
 */
@Slf4j
public abstract class PulsarFunctionsTest extends PulsarFunctionsTestBase {

    final Duration ONE_MINUTE = Duration.ofMinutes(1);
    final Duration TEN_SECONDS = Duration.ofSeconds(10);

    final RetryPolicy statusRetryPolicy = new RetryPolicy()
            .withMaxDuration(ONE_MINUTE)
            .withDelay(TEN_SECONDS)
            .onRetry(e -> log.error("Retry ... "));

    PulsarFunctionsTest(FunctionRuntimeType functionRuntimeType) {
        super(functionRuntimeType);
    }

    @Test(groups = "sink")
    public void testKafkaSink() throws Exception {
        final String kafkaContainerName = "kafka-" + randomName(8);
        testSink(new KafkaSinkTester(kafkaContainerName), true, new KafkaSourceTester(kafkaContainerName));
    }

    @Test(enabled = false, groups = "sink")
    public void testCassandraSink() throws Exception {
        testSink(CassandraSinkTester.createTester(true), true);
    }

    @Test(enabled = false, groups = "sink")
    public void testCassandraArchiveSink() throws Exception {
        testSink(CassandraSinkTester.createTester(false), false);
    }

    @Test(enabled = false, groups = "sink")
    public void testHdfsSink() throws Exception {
        testSink(new HdfsSinkTester(), false);
    }

    @Test(groups = "sink")
    public void testJdbcSink() throws Exception {
        testSink(new JdbcPostgresSinkTester(), true);
    }

    @Test(enabled = false, groups = "sink")
    public void testElasticSearchSink() throws Exception {
        testSink(new ElasticSearchSinkTester(), true);
    }

    @Test(groups = "sink")
    public void testRabbitMQSink() throws Exception {
        final String containerName = "rabbitmq-" + randomName(8);
        testSink(new RabbitMQSinkTester(containerName), true, new RabbitMQSourceTester(containerName));
    }

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

    private void testSink(SinkTester tester, boolean builtin) throws Exception {
        if (pulsarCluster == null) {
            super.setupCluster();
            super.setupFunctionWorkers();
        }
        tester.startServiceContainer(pulsarCluster);
        try {
            runSinkTester(tester, builtin);
        } finally {
            tester.stopServiceContainer(pulsarCluster);
        }
    }


    private <ServiceContainerT extends GenericContainer>  void testSink(SinkTester<ServiceContainerT> sinkTester,
                                                                        boolean builtinSink,
                                                                        SourceTester<ServiceContainerT> sourceTester)
            throws Exception {
        if (pulsarCluster == null) {
            super.setupCluster();
            super.setupFunctionWorkers();
        }
        ServiceContainerT serviceContainer = sinkTester.startServiceContainer(pulsarCluster);
        try {
            runSinkTester(sinkTester, builtinSink);
            if (null != sourceTester) {
                sourceTester.setServiceContainer(serviceContainer);
                testSource(sourceTester);
            }
        } finally {
            sinkTester.stopServiceContainer(pulsarCluster);
        }
    }
    private <T extends GenericContainer> void runSinkTester(SinkTester<T> tester, boolean builtin) throws Exception {
        final String tenant = TopicName.PUBLIC_TENANT;
        final String namespace = TopicName.DEFAULT_NAMESPACE;
        final String inputTopicName = "test-sink-connector-"
            + tester.getSinkType() + "-" + functionRuntimeType + "-input-topic-" + randomName(8);
        final String sinkName = "test-sink-connector-"
            + tester.getSinkType().name().toLowerCase() + "-" + functionRuntimeType + "-name-" + randomName(8);
        final int numMessages = 20;

        // prepare the testing environment for sink
        prepareSink(tester);

        ensureSubscriptionCreated(
            inputTopicName,
            String.format("public/default/%s", sinkName),
            tester.getInputTopicSchema());

        // submit the sink connector
        submitSinkConnector(tester, tenant, namespace, sinkName, inputTopicName);

        // get sink info
        getSinkInfoSuccess(tester, tenant, namespace, sinkName, builtin);

        // get sink status
        Failsafe.with(statusRetryPolicy).run(() -> getSinkStatus(tenant, namespace, sinkName));

        // produce messages
        Map<String, String> kvs;
        if (tester instanceof JdbcPostgresSinkTester) {
            kvs = produceSchemaInsertMessagesToInputTopic(inputTopicName, numMessages, AvroSchema.of(JdbcPostgresSinkTester.Foo.class));
            // wait for sink to process messages
            Failsafe.with(statusRetryPolicy).run(() ->
                    waitForProcessingSinkMessages(tenant, namespace, sinkName, numMessages));

            // validate the sink result
            tester.validateSinkResult(kvs);

            kvs = produceSchemaUpdateMessagesToInputTopic(inputTopicName, numMessages, AvroSchema.of(JdbcPostgresSinkTester.Foo.class));

            // wait for sink to process messages
            Failsafe.with(statusRetryPolicy).run(() ->
                    waitForProcessingSinkMessages(tenant, namespace, sinkName, numMessages + 20));

            // validate the sink result
            tester.validateSinkResult(kvs);

            kvs = produceSchemaDeleteMessagesToInputTopic(inputTopicName, numMessages, AvroSchema.of(JdbcPostgresSinkTester.Foo.class));

            // wait for sink to process messages
            Failsafe.with(statusRetryPolicy).run(() ->
                    waitForProcessingSinkMessages(tenant, namespace, sinkName, numMessages + 20 + 20));

            // validate the sink result
            tester.validateSinkResult(kvs);

        } else {
            kvs = produceMessagesToInputTopic(inputTopicName, numMessages);
            // wait for sink to process messages
            Failsafe.with(statusRetryPolicy).run(() ->
                    waitForProcessingSinkMessages(tenant, namespace, sinkName, numMessages));
            // validate the sink result
            tester.validateSinkResult(kvs);
        }

        // update the sink
        updateSinkConnector(tester, tenant, namespace, sinkName, inputTopicName);

        // delete the sink
        deleteSink(tenant, namespace, sinkName);

        // get sink info (sink should be deleted)
        getSinkInfoNotFound(tenant, namespace, sinkName);
    }

    protected void prepareSink(SinkTester tester) throws Exception {
        tester.prepareSink();
    }

    protected void submitSinkConnector(SinkTester tester,
                                       String tenant,
                                       String namespace,
                                       String sinkName,
                                       String inputTopicName) throws Exception {
        String[] commands;
        if (tester.getSinkType() != SinkTester.SinkType.UNDEFINED) {
            commands = new String[] {
                    PulsarCluster.ADMIN_SCRIPT,
                    "sink", "create",
                    "--tenant", tenant,
                    "--namespace", namespace,
                    "--name", sinkName,
                    "--sink-type", tester.sinkType().getValue().toLowerCase(),
                    "--sinkConfig", new Gson().toJson(tester.sinkConfig()),
                    "--inputs", inputTopicName
            };
        } else {
            commands = new String[] {
                    PulsarCluster.ADMIN_SCRIPT,
                    "sink", "create",
                    "--tenant", tenant,
                    "--namespace", namespace,
                    "--name", sinkName,
                    "--archive", tester.getSinkArchive(),
                    "--classname", tester.getSinkClassName(),
                    "--sinkConfig", new Gson().toJson(tester.sinkConfig()),
                    "--inputs", inputTopicName
            };
        }
        log.info("Run command : {}", StringUtils.join(commands, ' '));
        ContainerExecResult result = pulsarCluster.getAnyWorker().execCmd(commands);
        assertTrue(
            result.getStdout().contains("\"Created successfully\""),
            result.getStdout());
    }

    protected void updateSinkConnector(SinkTester tester,
                                       String tenant,
                                       String namespace,
                                       String sinkName,
                                       String inputTopicName) throws Exception {
        String[] commands;
        if (tester.getSinkType() != SinkTester.SinkType.UNDEFINED) {
            commands = new String[] {
                    PulsarCluster.ADMIN_SCRIPT,
                    "sink", "update",
                    "--tenant", tenant,
                    "--namespace", namespace,
                    "--name", sinkName,
                    "--sink-type", tester.sinkType().getValue().toLowerCase(),
                    "--sinkConfig", new Gson().toJson(tester.sinkConfig()),
                    "--inputs", inputTopicName,
                    "--parallelism", "2"
            };
        } else {
            commands = new String[] {
                    PulsarCluster.ADMIN_SCRIPT,
                    "sink", "create",
                    "--tenant", tenant,
                    "--namespace", namespace,
                    "--name", sinkName,
                    "--archive", tester.getSinkArchive(),
                    "--classname", tester.getSinkClassName(),
                    "--sinkConfig", new Gson().toJson(tester.sinkConfig()),
                    "--inputs", inputTopicName,
                    "--parallelism", "2"
            };
        }
        log.info("Run command : {}", StringUtils.join(commands, ' '));
        ContainerExecResult result = pulsarCluster.getAnyWorker().execCmd(commands);
        assertTrue(
                result.getStdout().contains("\"Updated successfully\""),
                result.getStdout());
    }

    protected void getSinkInfoSuccess(SinkTester tester,
                                      String tenant,
                                      String namespace,
                                      String sinkName,
                                      boolean builtin) throws Exception {
        String[] commands = {
            PulsarCluster.ADMIN_SCRIPT,
            "sink",
            "get",
            "--tenant", tenant,
            "--namespace", namespace,
            "--name", sinkName
        };
        ContainerExecResult result = pulsarCluster.getAnyWorker().execCmd(commands);
        log.info("Get sink info : {}", result.getStdout());
        if (builtin) {
            assertTrue(
                    result.getStdout().contains("\"archive\": \"builtin://" + tester.getSinkType().getValue().toLowerCase() + "\""),
                    result.getStdout()
            );
        } else {
            assertTrue(
                    result.getStdout().contains("\"className\": \"" + tester.getSinkClassName() + "\""),
                    result.getStdout()
            );
        }
    }

    protected void getSinkStatus(String tenant, String namespace, String sinkName) throws Exception {
        final String[] commands = {
            PulsarCluster.ADMIN_SCRIPT,
            "sink",
            "status",
            "--tenant", tenant,
            "--namespace", namespace,
            "--name", sinkName
        };

        final ContainerExecResult result = pulsarCluster.getAnyWorker().execCmd(commands);
        log.info("Get sink status : {}", result.getStdout());

        assertEquals(result.getExitCode(), 0);

        SinkStatus sinkStatus = SinkStatus.decode(result.getStdout());

        assertEquals(sinkStatus.getNumInstances(), 1);
        assertEquals(sinkStatus.getNumRunning(), 1);
        assertEquals(sinkStatus.getInstances().size(), 1);
        assertEquals(sinkStatus.getInstances().get(0).getInstanceId(), 0);
        assertEquals(sinkStatus.getInstances().get(0).getStatus().isRunning(), true);
        assertEquals(sinkStatus.getInstances().get(0).getStatus().getNumRestarts(), 0);
        assertEquals(sinkStatus.getInstances().get(0).getStatus().getLatestSystemExceptions().size(), 0);
    }

    protected Map<String, String> produceMessagesToInputTopic(String inputTopicName,
                                                              int numMessages) throws Exception {
        @Cleanup
        PulsarClient client = PulsarClient.builder()
            .serviceUrl(pulsarCluster.getPlainTextServiceUrl())
            .build();

        @Cleanup
        Producer<String> producer = client.newProducer(Schema.STRING)
            .topic(inputTopicName)
            .create();

        LinkedHashMap<String, String> kvs = new LinkedHashMap<>();
        for (int i = 0; i < numMessages; i++) {
            String key = "key-" + i;
            String value = "value-" + i;
            kvs.put(key, value);
            producer.newMessage()
                .key(key)
                .value(value)
                .send();
        }
        return kvs;
    }

    // This for JdbcPostgresSinkTester
    protected Map<String, String> produceSchemaInsertMessagesToInputTopic(String inputTopicName,
                                                                          int numMessages,
                                                                          Schema<Foo> schema) throws Exception {
        @Cleanup
        PulsarClient client = PulsarClient.builder()
            .serviceUrl(pulsarCluster.getPlainTextServiceUrl())
            .build();

        @Cleanup
        Producer<Foo> producer = client.newProducer(schema)
            .topic(inputTopicName)
            .create();

        LinkedHashMap<String, String> kvs = new LinkedHashMap<>();
        for (int i = 0; i < numMessages; i++) {
            String key = "key-" + i;

            JdbcPostgresSinkTester.Foo obj = new JdbcPostgresSinkTester.Foo();
            obj.setField1("field1_insert_" + i);
            obj.setField2("field2_insert_" + i);
            obj.setField3(i);
            String value = new String(schema.encode(obj));
            Map<String, String> properties = Maps.newHashMap();
            properties.put("ACTION", "INSERT");

            kvs.put(key, value);
            kvs.put("ACTION", "INSERT");
            producer.newMessage()
                    .properties(properties)
                    .key(key)
                    .value(obj)
                    .send();
        }
        return kvs;
    }

    // This for JdbcPostgresSinkTester
    protected Map<String, String> produceSchemaUpdateMessagesToInputTopic(String inputTopicName,
                                                                          int numMessages,
                                                                          Schema<Foo> schema) throws Exception {
        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(pulsarCluster.getPlainTextServiceUrl())
                .build();

        @Cleanup
        Producer<Foo> producer = client.newProducer(schema)
                .topic(inputTopicName)
                .create();

        LinkedHashMap<String, String> kvs = new LinkedHashMap<>();
        log.info("update start");
        for (int i = 0; i < numMessages; i++) {
            String key = "key-" + i;

            JdbcPostgresSinkTester.Foo obj = new JdbcPostgresSinkTester.Foo();
            obj.setField1("field1_insert_" + i);
            obj.setField2("field2_update_" + i);
            obj.setField3(i);
            String value = new String(schema.encode(obj));
            Map<String, String> properties = Maps.newHashMap();
            properties.put("ACTION", "UPDATE");

            kvs.put(key, value);
            kvs.put("ACTION", "UPDATE");
            producer.newMessage()
                    .properties(properties)
                    .key(key)
                    .value(obj)
                    .send();
        }
        log.info("update end");
        return kvs;
    }

    // This for JdbcPostgresSinkTester
    protected Map<String, String> produceSchemaDeleteMessagesToInputTopic(String inputTopicName,
                                                                          int numMessages,
                                                                          Schema<Foo> schema) throws Exception {
        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(pulsarCluster.getPlainTextServiceUrl())
                .build();

        @Cleanup
        Producer<Foo> producer = client.newProducer(schema)
                .topic(inputTopicName)
                .create();

        LinkedHashMap<String, String> kvs = new LinkedHashMap<>();
        for (int i = 0; i < numMessages; i++) {
            String key = "key-" + i;

            JdbcPostgresSinkTester.Foo obj = new JdbcPostgresSinkTester.Foo();
            obj.setField1("field1_insert_" + i);
            obj.setField2("field2_update_" + i);
            obj.setField3(i);
            String value = new String(schema.encode(obj));
            Map<String, String> properties = Maps.newHashMap();
            properties.put("ACTION", "DELETE");

            kvs.put(key, value);
            kvs.put("ACTION", "DELETE");
            producer.newMessage()
                    .properties(properties)
                    .key(key)
                    .value(obj)
                    .send();
        }
        return kvs;
    }

    protected void deleteSink(String tenant, String namespace, String sinkName) throws Exception {

        final String[] commands = {
            PulsarCluster.ADMIN_SCRIPT,
            "sink",
            "delete",
            "--tenant", tenant,
            "--namespace", namespace,
            "--name", sinkName
        };

        ContainerExecResult result = pulsarCluster.getAnyWorker().execCmd(commands);
        assertTrue(
            result.getStdout().contains("Deleted successfully"),
            result.getStdout()
        );
        assertTrue(
            result.getStderr().isEmpty(),
            result.getStderr()
        );
    }

    protected void getSinkInfoNotFound(String tenant, String namespace, String sinkName) throws Exception {
        final String[] commands = {
            PulsarCluster.ADMIN_SCRIPT,
            "sink",
            "get",
            "--tenant", tenant,
            "--namespace", namespace,
            "--name", sinkName
        };
        try {
            pulsarCluster.getAnyWorker().execCmd(commands);
            fail("Command should have exited with non-zero");
        } catch (ContainerExecException e) {
            assertTrue(e.getResult().getStderr().contains("Reason: Sink " + sinkName + " doesn't exist"));
        }
    }

    //
    // Source Test
    //

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

    protected void waitForProcessingSinkMessages(String tenant,
                                                 String namespace,
                                                 String sinkName,
                                                 int numMessages) throws Exception {
        final String[] commands = {
                PulsarCluster.ADMIN_SCRIPT,
                "sink",
                "status",
                "--tenant", tenant,
                "--namespace", namespace,
                "--name", sinkName
        };

        final ContainerExecResult result = pulsarCluster.getAnyWorker().execCmd(commands);
        log.info("Get sink status : {}", result.getStdout());

        assertEquals(result.getExitCode(), 0);

        final SinkStatus sinkStatus = SinkStatus.decode(result.getStdout());

        assertEquals(sinkStatus.getNumInstances(), 1);
        assertEquals(sinkStatus.getNumRunning(), 1);
        assertEquals(sinkStatus.getInstances().size(), 1);
        assertEquals(sinkStatus.getInstances().get(0).getInstanceId(), 0);
        assertEquals(sinkStatus.getInstances().get(0).getStatus().isRunning(), true);
        assertTrue(sinkStatus.getInstances().get(0).getStatus().getLastReceivedTime() > 0);
        assertEquals(sinkStatus.getInstances().get(0).getStatus().getNumReadFromPulsar(), numMessages);
        assertEquals(sinkStatus.getInstances().get(0).getStatus().getNumWrittenToSink(), numMessages);
        assertEquals(sinkStatus.getInstances().get(0).getStatus().getNumRestarts(), 0);
        assertEquals(sinkStatus.getInstances().get(0).getStatus().getLatestSystemExceptions().size(), 0);
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
        assertTrue(
            result.getStderr().isEmpty(),
            result.getStderr()
        );
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

    @Test(groups = {"python_function", "function"})
    public void testPythonFunctionLocalRun() throws Exception {
        testFunctionLocalRun(Runtime.PYTHON);
    }

    @Test(enabled = false, groups = {"go_function", "function"})
    public void testGoFunctionLocalRun() throws Exception {
        testFunctionLocalRun(Runtime.GO);
    }

    @Test(groups = {"java_function", "function"})
    public void testJavaFunctionLocalRun() throws Exception {
        testFunctionLocalRun(Runtime.JAVA);
    }

    public void testFunctionLocalRun(Runtime runtime) throws  Exception {
        if (functionRuntimeType == FunctionRuntimeType.THREAD) {
            return;
        }

        if (pulsarCluster == null) {
            super.setupCluster();
            super.setupFunctionWorkers();
        }

        String inputTopicName = "persistent://public/default/test-function-local-run-" + runtime + "-input-" + randomName(8);
        String outputTopicName = "test-function-local-run-" + runtime + "-output-" + randomName(8);

        final int numMessages = 10;
        String cmd = "";
        CommandGenerator commandGenerator = new CommandGenerator();
        commandGenerator.setAdminUrl("pulsar://pulsar-broker-0:6650");
        commandGenerator.setSourceTopic(inputTopicName);
        commandGenerator.setSinkTopic(outputTopicName);
        commandGenerator.setFunctionName("localRunTest");
        commandGenerator.setRuntime(runtime);
        switch (runtime) {
            case JAVA:
                commandGenerator.setFunctionClassName(EXCLAMATION_JAVA_CLASS);
                cmd = commandGenerator.generateLocalRunCommand(null);
                break;
            case PYTHON:
                commandGenerator.setFunctionClassName(EXCLAMATION_PYTHON_CLASS);
                cmd = commandGenerator.generateLocalRunCommand(EXCLAMATION_PYTHON_FILE);
                break;
            case GO:
                commandGenerator.setFunctionClassName(null);
                cmd = commandGenerator.generateLocalRunCommand(EXCLAMATION_GO_FILE);
                break;
        }

        log.info("cmd: {}", cmd);
        pulsarCluster.getAnyWorker().execCmdAsync(cmd.split(" "));

        try (PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(pulsarCluster.getHttpServiceUrl()).build()) {

            admin.topics().createNonPartitionedTopic(inputTopicName);
            admin.topics().createNonPartitionedTopic(outputTopicName);
            retryStrategically((test) -> {
                try {
                    return admin.topics().getStats(inputTopicName).subscriptions.size() == 1;
                } catch (PulsarAdminException e) {
                    return false;
                }
            }, 30, 200);

            assertEquals(admin.topics().getStats(inputTopicName).subscriptions.size(), 1);

            // publish and consume result
            if (Runtime.JAVA == runtime) {
                // java supports schema
                @Cleanup PulsarClient client = PulsarClient.builder()
                        .serviceUrl(pulsarCluster.getPlainTextServiceUrl())
                        .build();

                @Cleanup Consumer<String> consumer = client.newConsumer(Schema.STRING)
                        .topic(outputTopicName)
                        .subscriptionType(SubscriptionType.Exclusive)
                        .subscriptionName("test-sub")
                        .subscribe();

                @Cleanup Producer<String> producer = client.newProducer(Schema.STRING)
                        .topic(inputTopicName)
                        .create();

                for (int i = 0; i < numMessages; i++) {
                    producer.send("message-" + i);
                }

                Set<String> expectedMessages = new HashSet<>();
                for (int i = 0; i < numMessages; i++) {
                    expectedMessages.add("message-" + i + "!");
                }

                for (int i = 0; i < numMessages; i++) {
                    Message<String> msg = consumer.receive(60 * 2, TimeUnit.SECONDS);
                    log.info("Received: {}", msg.getValue());
                    assertTrue(expectedMessages.contains(msg.getValue()));
                    expectedMessages.remove(msg.getValue());
                }
                assertEquals(expectedMessages.size(), 0);

            } else {
                // python doesn't support schema

                @Cleanup PulsarClient client = PulsarClient.builder()
                        .serviceUrl(pulsarCluster.getPlainTextServiceUrl())
                        .build();
                @Cleanup Consumer<byte[]> consumer = client.newConsumer(Schema.BYTES)
                        .topic(outputTopicName)
                        .subscriptionType(SubscriptionType.Exclusive)
                        .subscriptionName("test-sub")
                        .subscribe();

                @Cleanup Producer<byte[]> producer = client.newProducer(Schema.BYTES)
                        .topic(inputTopicName)
                        .create();

                for (int i = 0; i < numMessages; i++) {
                    producer.newMessage().value(("message-" + i).getBytes(UTF_8)).send();
                }

                Set<String> expectedMessages = new HashSet<>();
                for (int i = 0; i < numMessages; i++) {
                    expectedMessages.add("message-" + i + "!");
                }

                for (int i = 0; i < numMessages; i++) {
                    Message<byte[]> msg = consumer.receive(60 * 2, TimeUnit.SECONDS);
                    String msgValue = new String(msg.getValue(), UTF_8);
                    log.info("Received: {}", msgValue);
                    assertTrue(expectedMessages.contains(msgValue));
                    expectedMessages.remove(msgValue);
                }
                assertEquals(expectedMessages.size(), 0);
            }
        }

    }

    public void testWindowFunction(String type, String[] expectedResults) throws Exception {
        int NUM_OF_MESSAGES = 100;
        int windowLengthCount = 10;
        int slidingIntervalCount = 5;
        String functionName = "test-" + type + "-window-fn-" + randomName(8);

        String inputTopicName = "test-" + type + "-count-window-" + functionRuntimeType + "-input-" + randomName(8);
        String outputTopicName = "test-" + type + "-count-window-" + functionRuntimeType + "-output-" + randomName(8);
        if (pulsarCluster == null) {
            super.setupCluster();
            super.setupFunctionWorkers();
        }
        try (PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(pulsarCluster.getHttpServiceUrl()).build()) {
            admin.topics().createNonPartitionedTopic(inputTopicName);
            admin.topics().createNonPartitionedTopic(outputTopicName);
        }

        CommandGenerator generator = CommandGenerator.createDefaultGenerator(
                inputTopicName,
                "org.apache.pulsar.functions.api.examples.WindowDurationFunction");
        generator.setFunctionName(functionName);
        generator.setSinkTopic(outputTopicName);
        generator.setWindowLengthCount(windowLengthCount);
        if (type.equals("sliding")) {
            generator.setSlidingIntervalCount(slidingIntervalCount);
        }


        String[] commands = {
                "sh", "-c", generator.generateCreateFunctionCommand()
        };

        ContainerExecResult containerExecResult = pulsarCluster.getAnyWorker().execCmd(commands);
        assertTrue(containerExecResult.getStdout().contains("\"Created successfully\""));

        // get function info
        getFunctionInfoSuccess(functionName);

        containerExecResult = pulsarCluster.getAnyWorker().execCmd(
                PulsarCluster.ADMIN_SCRIPT,
                "functions",
                "status",
                "--tenant", "public",
                "--namespace", "default",
                "--name", functionName
        );

        FunctionStatus functionStatus = FunctionStatus.decode(containerExecResult.getStdout());
        assertEquals(functionStatus.getNumInstances(), 1);
        assertEquals(functionStatus.getNumRunning(), 1);
        assertEquals(functionStatus.getInstances().size(), 1);
        assertEquals(functionStatus.getInstances().get(0).getInstanceId(), 0);
        assertEquals(functionStatus.getInstances().get(0).getStatus().isRunning(), true);
        assertEquals(functionStatus.getInstances().get(0).getStatus().getNumReceived(), 0);
        assertEquals(functionStatus.getInstances().get(0).getStatus().getNumSuccessfullyProcessed(), 0);
        assertEquals(functionStatus.getInstances().get(0).getStatus().getLatestUserExceptions().size(), 0);
        assertEquals(functionStatus.getInstances().get(0).getStatus().getLatestSystemExceptions().size(), 0);

        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(pulsarCluster.getPlainTextServiceUrl())
                .build();

        @Cleanup
        Reader<byte[]> reader = client.newReader().startMessageId(MessageId.earliest)
                .topic(outputTopicName)
                .create();

        @Cleanup
        Producer<byte[]> producer = client.newProducer(Schema.BYTES)
                .topic(inputTopicName)
                .enableBatching(false)
                .create();

        for (int i = 0; i < NUM_OF_MESSAGES; i++) {
            producer.send(String.format("%d", i).getBytes());
        }

        int i = 0;
        while (true) {
            if (i > expectedResults.length) {
                Assertions.fail("More results than expected");
            }

            Message<byte[]> msg = reader.readNext(30, TimeUnit.SECONDS);
            if (msg == null) {
                break;
            }
            String msgStr = new String(msg.getData());
            log.info("[testWindowFunction] i: {} RECV: {}", i, msgStr);
            String result = msgStr.split(":")[0];
            assertThat(result).contains(expectedResults[i]);
            i++;
        }

        getFunctionStatus(functionName, NUM_OF_MESSAGES, true);

        // in case last commit is not updated
        assertThat(i).isGreaterThanOrEqualTo(expectedResults.length - 1);

        deleteFunction(functionName);

        getFunctionInfoNotFound(functionName);
    }

    @Test(groups = {"java_function", "function"})
    public void testSlidingCountWindowTest() throws Exception {
        String[] EXPECTED_RESULTS = {
                "0,1,2,3,4",
                "0,1,2,3,4,5,6,7,8,9",
                "5,6,7,8,9,10,11,12,13,14",
                "10,11,12,13,14,15,16,17,18,19",
                "15,16,17,18,19,20,21,22,23,24",
                "20,21,22,23,24,25,26,27,28,29",
                "25,26,27,28,29,30,31,32,33,34",
                "30,31,32,33,34,35,36,37,38,39",
                "35,36,37,38,39,40,41,42,43,44",
                "40,41,42,43,44,45,46,47,48,49",
                "45,46,47,48,49,50,51,52,53,54",
                "50,51,52,53,54,55,56,57,58,59",
                "55,56,57,58,59,60,61,62,63,64",
                "60,61,62,63,64,65,66,67,68,69",
                "65,66,67,68,69,70,71,72,73,74",
                "70,71,72,73,74,75,76,77,78,79",
                "75,76,77,78,79,80,81,82,83,84",
                "80,81,82,83,84,85,86,87,88,89",
                "85,86,87,88,89,90,91,92,93,94",
                "90,91,92,93,94,95,96,97,98,99",
        };

        testWindowFunction("sliding", EXPECTED_RESULTS);
    }

    @Test(groups = {"java_function", "function"})
    public void testTumblingCountWindowTest() throws Exception {
        String[] EXPECTED_RESULTS = {
                "0,1,2,3,4,5,6,7,8,9",
                "10,11,12,13,14,15,16,17,18,19",
                "20,21,22,23,24,25,26,27,28,29",
                "30,31,32,33,34,35,36,37,38,39",
                "40,41,42,43,44,45,46,47,48,49",
                "50,51,52,53,54,55,56,57,58,59",
                "60,61,62,63,64,65,66,67,68,69",
                "70,71,72,73,74,75,76,77,78,79",
                "80,81,82,83,84,85,86,87,88,89",
                "90,91,92,93,94,95,96,97,98,99",
        };

        testWindowFunction("tumbling", EXPECTED_RESULTS);
    }

    //
    // Test CRUD functions on different runtimes.
    //

    @Test(groups = {"python_function", "function"})
    public void testPythonFunctionNegAck() throws Exception {
        testFunctionNegAck(Runtime.PYTHON);
    }

    @Test(groups = {"java_function", "function"})
    public void testJavaFunctionNegAck() throws Exception {
        testFunctionNegAck(Runtime.JAVA);
    }

    private void testFunctionNegAck(Runtime runtime) throws Exception {
        if (functionRuntimeType == FunctionRuntimeType.THREAD) {
            return;
        }

        if (pulsarCluster == null) {
            super.setupCluster();
            super.setupFunctionWorkers();
        }

        Schema<?> schema;
        if (Runtime.JAVA == runtime) {
            schema = Schema.STRING;
        } else {
            schema = Schema.BYTES;
        }

        String inputTopicName = "persistent://public/default/test-neg-ack-" + runtime + "-input-" + randomName(8);
        String outputTopicName = "test-neg-ack-" + runtime + "-output-" + randomName(8);
        try (PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(pulsarCluster.getHttpServiceUrl()).build()) {
            admin.topics().createNonPartitionedTopic(inputTopicName);
            admin.topics().createNonPartitionedTopic(outputTopicName);
        }

        String functionName = "test-neg-ack-fn-" + randomName(8);
        final int numMessages = 20;

        // submit the exclamation function

        if (runtime == Runtime.PYTHON) {
            submitFunction(
                    runtime, inputTopicName, outputTopicName, functionName, EXCEPTION_FUNCTION_PYTHON_FILE, EXCEPTION_PYTHON_CLASS, schema);
        } else {
            submitFunction(
                    runtime, inputTopicName, outputTopicName, functionName, null, EXCEPTION_JAVA_CLASS, schema);
        }

        // get function info
        getFunctionInfoSuccess(functionName);

        // get function stats
        getFunctionStatsEmpty(functionName);

        // publish and consume result
        if (Runtime.JAVA == runtime) {
            // java supports schema
            @Cleanup PulsarClient client = PulsarClient.builder()
                    .serviceUrl(pulsarCluster.getPlainTextServiceUrl())
                    .build();
            @Cleanup Consumer<String> consumer = client.newConsumer(Schema.STRING)
                    .topic(outputTopicName)
                    .subscriptionType(SubscriptionType.Exclusive)
                    .subscriptionName("test-sub")
                    .subscribe();
            @Cleanup Producer<String> producer = client.newProducer(Schema.STRING)
                    .topic(inputTopicName)
                    .create();

            for (int i = 0; i < numMessages; i++) {
                producer.send("message-" + i);
            }

            Set<String> expectedMessages = new HashSet<>();
            for (int i = 0; i < numMessages; i++) {
                expectedMessages.add("message-" + i + "!");
            }

            for (int i = 0; i < numMessages; i++) {
                Message<String> msg = consumer.receive(60 * 2, TimeUnit.SECONDS);
                log.info("Received: {}", msg.getValue());
                assertTrue(expectedMessages.contains(msg.getValue()));
                expectedMessages.remove(msg.getValue());
            }
            assertEquals(expectedMessages.size(), 0);

        } else {
            // python doesn't support schema

            @Cleanup PulsarClient client = PulsarClient.builder()
                    .serviceUrl(pulsarCluster.getPlainTextServiceUrl())
                    .build();

            @Cleanup Consumer<byte[]> consumer = client.newConsumer(Schema.BYTES)
                    .topic(outputTopicName)
                    .subscriptionType(SubscriptionType.Exclusive)
                    .subscriptionName("test-sub")
                    .subscribe();

            @Cleanup Producer<byte[]> producer = client.newProducer(Schema.BYTES)
                    .topic(inputTopicName)
                    .create();

            for (int i = 0; i < numMessages; i++) {
                producer.newMessage().value(("message-" + i).getBytes(UTF_8)).send();
            }

            Set<String> expectedMessages = new HashSet<>();
            for (int i = 0; i < numMessages; i++) {
                expectedMessages.add("message-" + i + "!");
            }

            for (int i = 0; i < numMessages; i++) {
                Message<byte[]> msg = consumer.receive(60 * 2, TimeUnit.SECONDS);
                String msgValue = new String(msg.getValue(), UTF_8);
                log.info("Received: {}", msgValue);
                assertTrue(expectedMessages.contains(msgValue));
                expectedMessages.remove(msgValue);
            }
            assertEquals(expectedMessages.size(), 0);
        }

        // get function status
        ContainerExecResult result = pulsarCluster.getAnyWorker().execCmd(
                PulsarCluster.ADMIN_SCRIPT,
                "functions",
                "status",
                "--tenant", "public",
                "--namespace", "default",
                "--name", functionName
        );

        FunctionStatus functionStatus = FunctionStatus.decode(result.getStdout());

        assertEquals(functionStatus.getNumInstances(), 1);
        assertEquals(functionStatus.getNumRunning(), 1);
        assertEquals(functionStatus.getInstances().size(), 1);
        assertEquals(functionStatus.getInstances().get(0).getInstanceId(), 0);
        assertTrue(functionStatus.getInstances().get(0).getStatus().getAverageLatency() > 0.0);
        assertEquals(functionStatus.getInstances().get(0).getStatus().isRunning(), true);
        assertTrue(functionStatus.getInstances().get(0).getStatus().getLastInvocationTime() > 0);
        // going to receive two more tuples because of delivery
        assertEquals(functionStatus.getInstances().get(0).getStatus().getNumReceived(), numMessages + 2);
        // only going to successfully process 20
        assertEquals(functionStatus.getInstances().get(0).getStatus().getNumSuccessfullyProcessed(), numMessages);
        assertEquals(functionStatus.getInstances().get(0).getStatus().getNumRestarts(), 0);
        assertEquals(functionStatus.getInstances().get(0).getStatus().getLatestUserExceptions().size(), 2);
        assertEquals(functionStatus.getInstances().get(0).getStatus().getLatestSystemExceptions().size(), 0);

        // get function stats
        result = pulsarCluster.getAnyWorker().execCmd(
                PulsarCluster.ADMIN_SCRIPT,
                "functions",
                "stats",
                "--tenant", "public",
                "--namespace", "default",
                "--name", functionName
        );

        log.info("FUNCTION STATS: {}", result.getStdout());

        FunctionStats functionStats = FunctionStats.decode(result.getStdout());
        assertEquals(functionStats.getReceivedTotal(), numMessages + 2);
        assertEquals(functionStats.getProcessedSuccessfullyTotal(), numMessages);
        assertEquals(functionStats.getSystemExceptionsTotal(), 0);
        assertEquals(functionStats.getUserExceptionsTotal(), 2);
        assertTrue(functionStats.avgProcessLatency > 0);
        assertTrue(functionStats.getLastInvocation() > 0);

        assertEquals(functionStats.instances.size(), 1);
        assertEquals(functionStats.instances.get(0).getInstanceId(), 0);
        assertEquals(functionStats.instances.get(0).getMetrics().getReceivedTotal(), numMessages + 2);
        assertEquals(functionStats.instances.get(0).getMetrics().getProcessedSuccessfullyTotal(), numMessages);
        assertEquals(functionStats.instances.get(0).getMetrics().getSystemExceptionsTotal(), 0);
        assertEquals(functionStats.instances.get(0).getMetrics().getUserExceptionsTotal(), 2);
        assertTrue(functionStats.instances.get(0).getMetrics().avgProcessLatency > 0);

        // delete function
        deleteFunction(functionName);

        // get function info
        getFunctionInfoNotFound(functionName);

        // make sure subscriptions are cleanup
        checkSubscriptionsCleanup(inputTopicName);
    }

    @Test(groups = {"python_function", "function"})
    public void testPythonPublishFunction() throws Exception {
        testPublishFunction(Runtime.PYTHON);
    }

    @Test(groups = {"java_function", "function"})
    public void testJavaPublishFunction() throws Exception {
        testPublishFunction(Runtime.JAVA);
    }

    public void testGoPublishFunction() throws Exception {
        testPublishFunction(Runtime.GO);
    }

    private void testPublishFunction(Runtime runtime) throws Exception {
        if (functionRuntimeType == FunctionRuntimeType.THREAD) {
            return;
        }

        Schema<?> schema;
        if (Runtime.JAVA == runtime) {
            schema = Schema.STRING;
        } else {
            schema = Schema.BYTES;
        }

        if (pulsarCluster == null) {
            super.setupCluster();
            super.setupFunctionWorkers();
        }

        String inputTopicName = "persistent://public/default/test-publish-" + runtime + "-input-" + randomName(8);
        String outputTopicName = "test-publish-" + runtime + "-output-" + randomName(8);
        try (PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(pulsarCluster.getHttpServiceUrl()).build()) {
            admin.topics().createNonPartitionedTopic(inputTopicName);
            admin.topics().createNonPartitionedTopic(outputTopicName);
        }

        String functionName = "test-publish-fn-" + randomName(8);
        final int numMessages = 10;

        // submit the exclamation function
        switch (runtime){
            case JAVA:
                submitFunction(
                        runtime,
                        inputTopicName,
                        outputTopicName,
                        functionName,
                        null,
                        PUBLISH_JAVA_CLASS,
                        schema,
                        Collections.singletonMap("publish-topic", outputTopicName));
                break;
            case PYTHON:
                submitFunction(
                        runtime,
                        inputTopicName,
                        outputTopicName,
                        functionName,
                        PUBLISH_FUNCTION_PYTHON_FILE,
                        PUBLISH_PYTHON_CLASS,
                        schema,
                        Collections.singletonMap("publish-topic", outputTopicName));
                break;
            case GO:
                submitFunction(
                        runtime,
                        inputTopicName,
                        outputTopicName,
                        functionName,
                        PUBLISH_FUNCTION_GO_FILE,
                        null,
                        schema,
                        Collections.singletonMap("publish-topic", outputTopicName));
        }

        // get function info
        getFunctionInfoSuccess(functionName);

        // get function stats
        getFunctionStatsEmpty(functionName);

        // publish and consume result

        if (Runtime.JAVA == runtime) {
            // java supports schema
            publishAndConsumeMessages(inputTopicName, outputTopicName, numMessages);
        } else {
            // python doesn't support schema. Does Go? Maybe we need a switch instead for the Go case.

            @Cleanup PulsarClient client = PulsarClient.builder()
                    .serviceUrl(pulsarCluster.getPlainTextServiceUrl())
                    .build();

            @Cleanup Consumer<byte[]> consumer = client.newConsumer(Schema.BYTES)
                    .topic(outputTopicName)
                    .subscriptionType(SubscriptionType.Exclusive)
                    .subscriptionName("test-sub")
                    .subscribe();

            @Cleanup Producer<byte[]> producer = client.newProducer(Schema.BYTES)
                    .topic(inputTopicName)
                    .create();

            for (int i = 0; i < numMessages; i++) {
                producer.newMessage().key(String.valueOf(i)).property("count", String.valueOf(i)).value(("message-" + i).getBytes(UTF_8)).send();
            }

            Set<String> expectedMessages = new HashSet<>();
            for (int i = 0; i < numMessages; i++) {
                expectedMessages.add("message-" + i + "!");
            }

            for (int i = 0; i < numMessages; i++) {
                Message<byte[]> msg = consumer.receive(30, TimeUnit.SECONDS);
                String msgValue = new String(msg.getValue(), UTF_8);
                log.info("Received: {}", msgValue);
                assertEquals(msg.getKey(), String.valueOf(i));
                assertEquals(msg.getProperties().get("count"), String.valueOf(i));
                assertEquals(msg.getProperties().get("input_topic"), inputTopicName);
                assertTrue(msg.getEventTime() > 0);
                assertTrue(expectedMessages.contains(msgValue));
                expectedMessages.remove(msgValue);
            }
        }

        // get function status
        getFunctionStatus(functionName, numMessages, true);

        // get function stats
        getFunctionStats(functionName, numMessages);

        // delete function
        deleteFunction(functionName);

        // get function info
        getFunctionInfoNotFound(functionName);

        // make sure subscriptions are cleanup
        checkSubscriptionsCleanup(inputTopicName);
    }

    @Test(groups = {"java_function", "function"})
    public void testSerdeFunction() throws Exception {
        testCustomSerdeFunction();
    }

    private void testCustomSerdeFunction() throws Exception {
        if (functionRuntimeType == FunctionRuntimeType.THREAD) {
            return;
        }

        if (pulsarCluster == null) {
            super.setupCluster();
            super.setupFunctionWorkers();
        }

        String inputTopicName = "persistent://public/default/test-serde-java-input-" + randomName(8);
        String outputTopicName = "test-publish-serde-output-" + randomName(8);
        try (PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(pulsarCluster.getHttpServiceUrl()).build()) {
            admin.topics().createNonPartitionedTopic(inputTopicName);
            admin.topics().createNonPartitionedTopic(outputTopicName);
        }

        String functionName = "test-serde-fn-" + randomName(8);
        submitFunction(
                Runtime.JAVA, inputTopicName, outputTopicName, functionName, null, SERDE_JAVA_CLASS,
                SERDE_OUTPUT_CLASS, Collections.singletonMap("serde-topic", outputTopicName)
        );

        // get function info
        getFunctionInfoSuccess(functionName);
        // get function stats
        getFunctionStatsEmpty(functionName);

        ContainerExecResult result = pulsarCluster.getAnyWorker().execCmd(
                PulsarCluster.ADMIN_SCRIPT,
                "functions",
                "status",
                "--tenant", "public",
                "--namespace", "default",
                "--name", functionName
        );

        FunctionStatus functionStatus = FunctionStatus.decode(result.getStdout());
        assertEquals(functionStatus.getNumInstances(), 1);
        assertEquals(functionStatus.getInstances().get(0).getStatus().isRunning(), true);
    }

    @Test(groups = {"python_function", "function"})
    public void testPythonExclamationFunction() throws Exception {
        testExclamationFunction(Runtime.PYTHON, false, false, false);
    }

    @Test(groups = {"python_function", "function"})
    public void testPythonExclamationFunctionWithExtraDeps() throws Exception {
        testExclamationFunction(Runtime.PYTHON, false, false, true);
    }

    @Test(groups = {"python_function", "function"})
    public void testPythonExclamationZipFunction() throws Exception {
        testExclamationFunction(Runtime.PYTHON, false, true, false);
    }

    @Test(groups = {"python_function", "function"})
    public void testPythonExclamationTopicPatternFunction() throws Exception {
        testExclamationFunction(Runtime.PYTHON, true, false, false);
    }

    @Test(groups = {"java_function", "function"})
    public void testJavaExclamationFunction() throws Exception {
        testExclamationFunction(Runtime.JAVA, false, false, false);
    }

    @Test(groups = {"java_function", "function"})
    public void testJavaExclamationTopicPatternFunction() throws Exception {
        testExclamationFunction(Runtime.JAVA, true, false, false);
    }

    private void testExclamationFunction(Runtime runtime,
                                         boolean isTopicPattern,
                                         boolean pyZip,
                                         boolean withExtraDeps) throws Exception {
        if (functionRuntimeType == FunctionRuntimeType.THREAD && runtime == Runtime.PYTHON) {
            // python can only run on process mode
            return;
        }

        if (pulsarCluster == null) {
            super.setupCluster();
            super.setupFunctionWorkers();
        }

        Schema<?> schema;
        if (Runtime.JAVA == runtime) {
            schema = Schema.STRING;
        } else {
            schema = Schema.BYTES;
        }

        String inputTopicName = "persistent://public/default/test-exclamation-" + runtime + "-input-" + randomName(8);
        String outputTopicName = "test-exclamation-" + runtime + "-output-" + randomName(8);
        try (PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(pulsarCluster.getHttpServiceUrl()).build()) {
            admin.topics().createNonPartitionedTopic(inputTopicName);
            admin.topics().createNonPartitionedTopic(outputTopicName);
        }
        if (isTopicPattern) {
            @Cleanup PulsarClient client = PulsarClient.builder()
                    .serviceUrl(pulsarCluster.getPlainTextServiceUrl())
                    .build();

            @Cleanup Consumer<?> consumer1 = client.newConsumer(schema)
                    .topic(inputTopicName + "1")
                    .subscriptionType(SubscriptionType.Exclusive)
                    .subscriptionName("test-sub")
                    .subscribe();

            @Cleanup Consumer<?> consumer2 = client.newConsumer(schema)
                    .topic(inputTopicName + "2")
                    .subscriptionType(SubscriptionType.Exclusive)
                    .subscriptionName("test-sub")
                    .subscribe();
            inputTopicName = inputTopicName + ".*";
        }
        String functionName = "test-exclamation-fn-" + randomName(8);
        final int numMessages = 10;

        // submit the exclamation function
        submitExclamationFunction(
            runtime, inputTopicName, outputTopicName, functionName, pyZip, withExtraDeps, schema);

        // get function info
        getFunctionInfoSuccess(functionName);

        // get function stats
        getFunctionStatsEmpty(functionName);

        // publish and consume result
        if (Runtime.JAVA == runtime) {
            // java supports schema
            publishAndConsumeMessages(inputTopicName, outputTopicName, numMessages);
        } else {
            // python doesn't support schema
            publishAndConsumeMessagesBytes(inputTopicName, outputTopicName, numMessages);
        }

        // get function status
        getFunctionStatus(functionName, numMessages, true);

        // get function stats
        getFunctionStats(functionName, numMessages);

        // update parallelism
        updateFunctionParallelism(functionName, 2);

        //get function status
        getFunctionStatus(functionName, 0, true, 2);

        // delete function
        deleteFunction(functionName);

        // get function info
        getFunctionInfoNotFound(functionName);

        // make sure subscriptions are cleanup
        checkSubscriptionsCleanup(inputTopicName);

    }

    private static void submitExclamationFunction(Runtime runtime,
                                                  String inputTopicName,
                                                  String outputTopicName,
                                                  String functionName,
                                                  boolean pyZip,
                                                  boolean withExtraDeps,
                                                  Schema<?> schema) throws Exception {
        submitFunction(
            runtime,
            inputTopicName,
            outputTopicName,
            functionName,
            pyZip,
            withExtraDeps,
            false,
            getExclamationClass(runtime, pyZip, withExtraDeps),
            schema);
    }

    private static <T> void submitFunction(Runtime runtime,
                                           String inputTopicName,
                                           String outputTopicName,
                                           String functionName,
                                           boolean pyZip,
                                           boolean withExtraDeps,
                                           boolean isPublishFunction,
                                           String functionClass,
                                           Schema<T> inputTopicSchema) throws Exception {

        String file = null;
        if (Runtime.JAVA == runtime) {
            file = null;
        } else if (Runtime.PYTHON == runtime) {
            if (isPublishFunction) {
                file = PUBLISH_FUNCTION_PYTHON_FILE;
            } else if (pyZip) {
                file = EXCLAMATION_PYTHON_ZIP_FILE;
            } else if (withExtraDeps) {
                file = EXCLAMATION_WITH_DEPS_PYTHON_FILE;
            } else {
                file = EXCLAMATION_PYTHON_FILE;
            }
        }

        submitFunction(runtime, inputTopicName, outputTopicName, functionName, file, functionClass, inputTopicSchema);
    }

    private static <T> void submitFunction(Runtime runtime,
                                           String inputTopicName,
                                           String outputTopicName,
                                           String functionName,
                                           String functionFile,
                                           String functionClass,
                                           Schema<T> inputTopicSchema) throws Exception {
        submitFunction(runtime, inputTopicName, outputTopicName, functionName, functionFile, functionClass, inputTopicSchema, null);
    }

    private static <T> void submitFunction(Runtime runtime,
                                           String inputTopicName,
                                           String outputTopicName,
                                           String functionName,
                                           String functionFile,
                                           String functionClass,
                                           Schema<T> inputTopicSchema,
                                           Map<String, String> userConfigs) throws Exception {

        CommandGenerator generator;
        log.info("------- INPUT TOPIC: '{}'", inputTopicName);
        if (inputTopicName.endsWith(".*")) {
            log.info("----- CREATING TOPIC PATTERN FUNCTION --- ");
            generator = CommandGenerator.createTopicPatternGenerator(inputTopicName, functionClass);
        } else {
            log.info("----- CREATING REGULAR FUNCTION --- ");
            generator = CommandGenerator.createDefaultGenerator(inputTopicName, functionClass);
        }
        generator.setSinkTopic(outputTopicName);
        generator.setFunctionName(functionName);
        if (userConfigs != null) {
            generator.setUserConfig(userConfigs);
        }
        String command = "";

        switch (runtime){
            case JAVA:
                command = generator.generateCreateFunctionCommand();
                break;
            case PYTHON:
            case GO:
                generator.setRuntime(runtime);
                command = generator.generateCreateFunctionCommand(functionFile);
                break;
            default:
                throw new IllegalArgumentException("Unsupported runtime : " + runtime);
        }

        log.info("---------- Function command: {}", command);
        String[] commands = {
                "sh", "-c", command
        };
        ContainerExecResult result = pulsarCluster.getAnyWorker().execCmd(
                commands);
        assertTrue(result.getStdout().contains("\"Created successfully\""));

        ensureSubscriptionCreated(inputTopicName, String.format("public/default/%s", functionName), inputTopicSchema);
    }

    private static void updateFunctionParallelism(String functionName, int parallelism) throws Exception {

        CommandGenerator generator = new CommandGenerator();
        generator.setFunctionName(functionName);
        generator.setParallelism(parallelism);
        String command = generator.generateUpdateFunctionCommand();

        log.info("---------- Function command: {}", command);
        String[] commands = {
                "sh", "-c", command
        };
        ContainerExecResult result = pulsarCluster.getAnyWorker().execCmd(
                commands);
        assertTrue(result.getStdout().contains("\"Updated successfully\""));
    }

    private static <T> void submitFunction(Runtime runtime,
                                           String inputTopicName,
                                           String outputTopicName,
                                           String functionName,
                                           String functionFile,
                                           String functionClass,
                                           String outputSerdeClassName,
                                           Map<String, String> userConfigs) throws Exception {

        CommandGenerator generator;
        log.info("------- INPUT TOPIC: '{}'", inputTopicName);
        if (inputTopicName.endsWith(".*")) {
            log.info("----- CREATING TOPIC PATTERN FUNCTION --- ");
            generator = CommandGenerator.createTopicPatternGenerator(inputTopicName, functionClass);
        } else {
            log.info("----- CREATING REGULAR FUNCTION --- ");
            generator = CommandGenerator.createDefaultGenerator(inputTopicName, functionClass);
        }
        generator.setSinkTopic(outputTopicName);
        generator.setFunctionName(functionName);
        generator.setOutputSerDe(outputSerdeClassName);
        if (userConfigs != null) {
            generator.setUserConfig(userConfigs);
        }
        String command;
        if (Runtime.JAVA == runtime) {
            command = generator.generateCreateFunctionCommand();
        } else if (Runtime.PYTHON == runtime) {
            generator.setRuntime(runtime);
            command = generator.generateCreateFunctionCommand(functionFile);
        } else {
            throw new IllegalArgumentException("Unsupported runtime : " + runtime);
        }

        log.info("---------- Function command: {}", command);
        String[] commands = {
                "sh", "-c", command
        };
        ContainerExecResult result = pulsarCluster.getAnyWorker().execCmd(
                commands);
        assertTrue(result.getStdout().contains("\"Created successfully\""));
    }

    private static <T> void ensureSubscriptionCreated(String inputTopicName,
                                                      String subscriptionName,
                                                      Schema<T> inputTopicSchema)
            throws Exception {
        // ensure the function subscription exists before we start producing messages
        try (PulsarClient client = PulsarClient.builder()
            .serviceUrl(pulsarCluster.getPlainTextServiceUrl())
            .build()) {
            try (Consumer<T> ignored = client.newConsumer(inputTopicSchema)
                .topic(inputTopicName)
                .subscriptionType(SubscriptionType.Shared)
                .subscriptionName(subscriptionName)
                .subscribe()) {
            }
        }
    }

    private static void getFunctionInfoSuccess(String functionName) throws Exception {
        ContainerExecResult result = pulsarCluster.getAnyWorker().execCmd(
            PulsarCluster.ADMIN_SCRIPT,
            "functions",
            "get",
            "--tenant", "public",
            "--namespace", "default",
            "--name", functionName
        );

        log.info("FUNCTION STATE: {}", result.getStdout());
        assertTrue(result.getStdout().contains("\"name\": \"" + functionName + "\""));
    }

    private static void getFunctionStatsEmpty(String functionName) throws Exception {
        ContainerExecResult result = pulsarCluster.getAnyWorker().execCmd(
                PulsarCluster.ADMIN_SCRIPT,
                "functions",
                "stats",
                "--tenant", "public",
                "--namespace", "default",
                "--name", functionName
        );

        log.info("FUNCTION STATS: {}", result.getStdout());
        FunctionStats functionStats = FunctionStats.decode(result.getStdout());

        assertEquals(functionStats.getReceivedTotal(), 0);
        assertEquals(functionStats.getProcessedSuccessfullyTotal(), 0);
        assertEquals(functionStats.getSystemExceptionsTotal(), 0);
        assertEquals(functionStats.getUserExceptionsTotal(), 0);
        assertEquals(functionStats.avgProcessLatency, null);
        assertEquals(functionStats.oneMin.getReceivedTotal(), 0);
        assertEquals(functionStats.oneMin.getProcessedSuccessfullyTotal(), 0);
        assertEquals(functionStats.oneMin.getSystemExceptionsTotal(), 0);
        assertEquals(functionStats.oneMin.getUserExceptionsTotal(), 0);
        assertEquals(functionStats.oneMin.getAvgProcessLatency(), null);
        assertEquals(functionStats.getAvgProcessLatency(), functionStats.oneMin.getAvgProcessLatency());
        assertEquals(functionStats.getLastInvocation(), null);

        assertEquals(functionStats.instances.size(), 1);
        assertEquals(functionStats.instances.get(0).getInstanceId(), 0);
        assertEquals(functionStats.instances.get(0).getMetrics().getReceivedTotal(), 0);
        assertEquals(functionStats.instances.get(0).getMetrics().getProcessedSuccessfullyTotal(), 0);
        assertEquals(functionStats.instances.get(0).getMetrics().getSystemExceptionsTotal(), 0);
        assertEquals(functionStats.instances.get(0).getMetrics().getUserExceptionsTotal(), 0);
        assertEquals(functionStats.instances.get(0).getMetrics().avgProcessLatency, null);
        assertEquals(functionStats.instances.get(0).getMetrics().oneMin.getReceivedTotal(), 0);
        assertEquals(functionStats.instances.get(0).getMetrics().oneMin.getProcessedSuccessfullyTotal(), 0);
        assertEquals(functionStats.instances.get(0).getMetrics().oneMin.getSystemExceptionsTotal(), 0);
        assertEquals(functionStats.instances.get(0).getMetrics().oneMin.getUserExceptionsTotal(), 0);
        assertEquals(functionStats.instances.get(0).getMetrics().oneMin.getAvgProcessLatency(), null);
    }

    private static void getFunctionStats(String functionName, int numMessages) throws Exception {
        ContainerExecResult result = pulsarCluster.getAnyWorker().execCmd(
                PulsarCluster.ADMIN_SCRIPT,
                "functions",
                "stats",
                "--tenant", "public",
                "--namespace", "default",
                "--name", functionName
        );

        log.info("FUNCTION STATS: {}", result.getStdout());

        FunctionStats functionStats = FunctionStats.decode(result.getStdout());
        assertEquals(functionStats.getReceivedTotal(), numMessages);
        assertEquals(functionStats.getProcessedSuccessfullyTotal(), numMessages);
        assertEquals(functionStats.getSystemExceptionsTotal(), 0);
        assertEquals(functionStats.getUserExceptionsTotal(), 0);
        assertTrue(functionStats.avgProcessLatency > 0);
        assertEquals(functionStats.oneMin.getReceivedTotal(), numMessages);
        assertEquals(functionStats.oneMin.getProcessedSuccessfullyTotal(), numMessages);
        assertEquals(functionStats.oneMin.getSystemExceptionsTotal(), 0);
        assertEquals(functionStats.oneMin.getUserExceptionsTotal(), 0);
        assertTrue(functionStats.oneMin.getAvgProcessLatency() > 0);
        assertEquals(functionStats.getAvgProcessLatency(), functionStats.oneMin.getAvgProcessLatency());
        assertTrue(functionStats.getLastInvocation() > 0);

        assertEquals(functionStats.instances.size(), 1);
        assertEquals(functionStats.instances.get(0).getInstanceId(), 0);
        assertEquals(functionStats.instances.get(0).getMetrics().getReceivedTotal(), numMessages);
        assertEquals(functionStats.instances.get(0).getMetrics().getProcessedSuccessfullyTotal(), numMessages);
        assertEquals(functionStats.instances.get(0).getMetrics().getSystemExceptionsTotal(), 0);
        assertEquals(functionStats.instances.get(0).getMetrics().getUserExceptionsTotal(), 0);
        assertTrue(functionStats.instances.get(0).getMetrics().avgProcessLatency > 0);
        assertEquals(functionStats.instances.get(0).getMetrics().oneMin.getReceivedTotal(), numMessages);
        assertEquals(functionStats.instances.get(0).getMetrics().oneMin.getProcessedSuccessfullyTotal(), numMessages);
        assertEquals(functionStats.instances.get(0).getMetrics().oneMin.getSystemExceptionsTotal(), 0);
        assertEquals(functionStats.instances.get(0).getMetrics().oneMin.getUserExceptionsTotal(), 0);
        assertTrue(functionStats.instances.get(0).getMetrics().oneMin.getAvgProcessLatency() > 0);
    }

    private static void getFunctionInfoNotFound(String functionName) throws Exception {
        retryStrategically(aVoid -> {
            try {
                pulsarCluster.getAnyWorker().execCmd(
                        PulsarCluster.ADMIN_SCRIPT,
                        "functions",
                        "get",
                        "--tenant", "public",
                        "--namespace", "default",
                        "--name", functionName);
            } catch (ContainerExecException e) {
                if (e.getResult().getStderr().contains("Reason: Function " + functionName + " doesn't exist")) {
                    return true;
                }

            } catch (Exception e) {

            }
            return false;
        }, 5, 100, true);
    }

    private static void checkSubscriptionsCleanup(String topic) throws Exception {
        try {
            ContainerExecResult result = pulsarCluster.getAnyBroker().execCmd(
                    PulsarCluster.ADMIN_SCRIPT,
                    "topics",
                    "stats",
                    topic);
            TopicStats topicStats = new Gson().fromJson(result.getStdout(), TopicStats.class);
            assertEquals(topicStats.subscriptions.size(), 0);

        } catch (ContainerExecException e) {
            fail("Command should have exited with non-zero");
        }
    }

    private static void getFunctionStatus(String functionName, int numMessages, boolean checkRestarts) throws Exception {
        getFunctionStatus(functionName, numMessages, checkRestarts, 1);
    }

    private static void getFunctionStatus(String functionName, int numMessages, boolean checkRestarts, int parallelism)
        throws Exception {
        ContainerExecResult result = pulsarCluster.getAnyWorker().execCmd(
            PulsarCluster.ADMIN_SCRIPT,
            "functions",
            "status",
            "--tenant", "public",
            "--namespace", "default",
            "--name", functionName
        );

        FunctionStatus functionStatus = FunctionStatus.decode(result.getStdout());

        assertEquals(functionStatus.getNumInstances(), parallelism);
        assertEquals(functionStatus.getNumRunning(), parallelism);
        assertEquals(functionStatus.getInstances().size(), parallelism);
        boolean avgLatencyGreaterThanZero = false;
        int totalMessagesProcessed = 0;
        int totalMessagesSuccessfullyProcessed = 0;
        boolean lastInvocationTimeGreaterThanZero = false;
        for (int i = 0; i < parallelism; ++i) {
            assertEquals(functionStatus.getInstances().get(i).getStatus().isRunning(), true);
            assertTrue(functionStatus.getInstances().get(i).getInstanceId() >= 0);
            assertTrue(functionStatus.getInstances().get(i).getInstanceId() < parallelism);
            avgLatencyGreaterThanZero = avgLatencyGreaterThanZero
                    || functionStatus.getInstances().get(i).getStatus().getAverageLatency() > 0.0;
            lastInvocationTimeGreaterThanZero = lastInvocationTimeGreaterThanZero
                    || functionStatus.getInstances().get(i).getStatus().getLastInvocationTime() > 0;
            totalMessagesProcessed += functionStatus.getInstances().get(i).getStatus().getNumReceived();
            totalMessagesSuccessfullyProcessed += functionStatus.getInstances().get(i).getStatus().getNumSuccessfullyProcessed();
            if (checkRestarts) {
                assertEquals(functionStatus.getInstances().get(i).getStatus().getNumRestarts(), 0);
            }
            assertEquals(functionStatus.getInstances().get(i).getStatus().getLatestUserExceptions().size(), 0);
            assertEquals(functionStatus.getInstances().get(i).getStatus().getLatestSystemExceptions().size(), 0);
        }
        if (numMessages > 0) {
            assertTrue(avgLatencyGreaterThanZero);
            assertTrue(lastInvocationTimeGreaterThanZero);
        }
        assertEquals(totalMessagesProcessed, numMessages);
        assertEquals(totalMessagesSuccessfullyProcessed, numMessages);
    }

    private static void publishAndConsumeMessages(String inputTopic,
                                                  String outputTopic,
                                                  int numMessages) throws Exception {
        @Cleanup PulsarClient client = PulsarClient.builder()
            .serviceUrl(pulsarCluster.getPlainTextServiceUrl())
            .build();

        @Cleanup Consumer<String> consumer = client.newConsumer(Schema.STRING)
            .topic(outputTopic)
            .subscriptionType(SubscriptionType.Exclusive)
            .subscriptionName("test-sub")
            .subscribe();

        if (inputTopic.endsWith(".*")) {
            @Cleanup Producer<String> producer1 = client.newProducer(Schema.STRING)
                    .topic(inputTopic.substring(0, inputTopic.length() - 2) + "1")
                    .create();

            @Cleanup Producer<String> producer2 = client.newProducer(Schema.STRING)
                    .topic(inputTopic.substring(0, inputTopic.length() - 2) + "2")
                    .create();

            for (int i = 0; i < numMessages / 2; i++) {
                producer1.send("message-" + i);
            }

            for (int i = numMessages / 2; i < numMessages; i++) {
                producer2.send("message-" + i);
            }
        } else {
            @Cleanup Producer<String> producer = client.newProducer(Schema.STRING)
                    .topic(inputTopic)
                    .create();

            for (int i = 0; i < numMessages; i++) {
                producer.send("message-" + i);
            }
        }

        Set<String> expectedMessages = new HashSet<>();
        for (int i = 0; i < numMessages; i++) {
            expectedMessages.add("message-" + i + "!");
        }

        for (int i = 0; i < numMessages; i++) {
            Message<String> msg = consumer.receive(30, TimeUnit.SECONDS);
            log.info("Received: {}", msg.getValue());
            assertTrue(expectedMessages.contains(msg.getValue()));
            expectedMessages.remove(msg.getValue());
        }
    }

    private static void publishAndConsumeMessagesBytes(String inputTopic,
                                                       String outputTopic,
                                                       int numMessages) throws Exception {
        @Cleanup PulsarClient client = PulsarClient.builder()
            .serviceUrl(pulsarCluster.getPlainTextServiceUrl())
            .build();

        @Cleanup Consumer<byte[]> consumer = client.newConsumer(Schema.BYTES)
            .topic(outputTopic)
            .subscriptionType(SubscriptionType.Exclusive)
            .subscriptionName("test-sub")
            .subscribe();

        if (inputTopic.endsWith(".*")) {
            @Cleanup Producer<byte[]> producer1 = client.newProducer(Schema.BYTES)
                    .topic(inputTopic.substring(0, inputTopic.length() - 2) + "1")
                    .create();

            @Cleanup Producer<byte[]> producer2 = client.newProducer(Schema.BYTES)
                    .topic(inputTopic.substring(0, inputTopic.length() - 2) + "2")
                    .create();

            for (int i = 0; i < numMessages / 2; i++) {
                producer1.send(("message-" + i).getBytes(UTF_8));
            }

            for (int i = numMessages / 2; i < numMessages; i++) {
                producer2.send(("message-" + i).getBytes(UTF_8));
            }
        } else {
            @Cleanup Producer<byte[]> producer = client.newProducer(Schema.BYTES)
                    .topic(inputTopic)
                    .create();

            for (int i = 0; i < numMessages; i++) {
                producer.send(("message-" + i).getBytes(UTF_8));
            }
        }

        Set<String> expectedMessages = new HashSet<>();
        for (int i = 0; i < numMessages; i++) {
            expectedMessages.add("message-" + i + "!");
        }

        for (int i = 0; i < numMessages; i++) {
            Message<byte[]> msg = consumer.receive(30, TimeUnit.SECONDS);
            String msgValue = new String(msg.getValue(), UTF_8);
            log.info("Received: {}", msgValue);
            assertTrue(expectedMessages.contains(msgValue));
            expectedMessages.remove(msgValue);
        }
    }

    private static void deleteFunction(String functionName) throws Exception {
        ContainerExecResult result = pulsarCluster.getAnyWorker().execCmd(
            PulsarCluster.ADMIN_SCRIPT,
            "functions",
            "delete",
            "--tenant", "public",
            "--namespace", "default",
            "--name", functionName
        );
        assertTrue(result.getStdout().contains("Deleted successfully"));
        assertTrue(result.getStderr().isEmpty());
    }

    @Test(groups = "function")
    public void testAutoSchemaFunction() throws Exception {
        String inputTopicName = "test-autoschema-input-" + randomName(8);
        String outputTopicName = "test-autoshcema-output-" + randomName(8);
        String functionName = "test-autoschema-fn-" + randomName(8);
        final int numMessages = 10;

        if (pulsarCluster == null) {
            super.setupCluster();
            super.setupFunctionWorkers();
        }

        // submit the exclamation function
        submitFunction(
            Runtime.JAVA,
            inputTopicName,
            outputTopicName,
            functionName,
            false,
            false,
            false,
            AutoSchemaFunction.class.getName(),
            Schema.AVRO(CustomObject.class));

        // get function info
        getFunctionInfoSuccess(functionName);

        // publish and consume result
        publishAndConsumeAvroMessages(inputTopicName, outputTopicName, numMessages);

        // get function status. Note that this function might restart a few times until
        // the producer above writes the messages.
        getFunctionStatus(functionName, numMessages, false);

        // delete function
        deleteFunction(functionName);

        // get function info
        getFunctionInfoNotFound(functionName);
    }

    private static void publishAndConsumeAvroMessages(String inputTopic,
                                                      String outputTopic,
                                                      int numMessages) throws Exception {

        @Cleanup PulsarClient client = PulsarClient.builder()
            .serviceUrl(pulsarCluster.getPlainTextServiceUrl())
            .build();

        @Cleanup Consumer<String> consumer = client.newConsumer(Schema.STRING)
            .topic(outputTopic)
            .subscriptionType(SubscriptionType.Exclusive)
            .subscriptionName("test-sub")
            .subscribe();

        @Cleanup Producer<CustomObject> producer = client.newProducer(Schema.AVRO(CustomObject.class))
            .topic(inputTopic)
            .create();

        for (int i = 0; i < numMessages; i++) {
            CustomObject co = new CustomObject(i);
            producer.send(co);
        }

        for (int i = 0; i < numMessages; i++) {
            Message<String> msg = consumer.receive();
            assertEquals("value-" + i, msg.getValue());
        }
    }

    @Test(groups = "function")
    public void testAvroSchemaFunction() throws Exception {
        log.info("testAvroSchemaFunction start ...");
        final String inputTopic = "test-avroschema-input-" + randomName(8);
        final String outputTopic = "test-avroschema-output-" + randomName(8);
        final String functionName = "test-avroschema-fn-202003241756";
        final int numMessages = 10;

        if (pulsarCluster == null) {
            log.info("pulsarClient is null");
            this.setupCluster();
            this.setupFunctionWorkers();
        }

        @Cleanup PulsarClient pulsarClient = PulsarClient.builder()
                .serviceUrl(pulsarCluster.getPlainTextServiceUrl()).build();
        log.info("pulsar client init - input: {}, output: {}", inputTopic, outputTopic);

        @Cleanup Producer<AvroTestObject> producer = pulsarClient
                .newProducer(Schema.AVRO(AvroTestObject.class))
                .topic(inputTopic).create();
        log.info("pulsar producer init - {}", inputTopic);

        @Cleanup Consumer<AvroTestObject> consumer = pulsarClient
                .newConsumer(Schema.AVRO(AvroTestObject.class))
                .subscriptionType(SubscriptionType.Exclusive)
                .subscriptionName("test-avro-schema")
                .topic(outputTopic)
                .subscribe();
        log.info("pulsar consumer init - {}", outputTopic);

        CompletableFuture<Optional<SchemaInfo>> inputSchemaFuture =
                ((PulsarClientImpl) pulsarClient).getSchema(inputTopic);
        inputSchemaFuture.whenComplete((schemaInfo, throwable) -> {
            if (schemaInfo.isPresent()) {
                log.info("inputSchemaInfo: {}", schemaInfo.get().toString());
            } else {
                log.error("input schema is not present!");
            }
        });

        CompletableFuture<Optional<SchemaInfo>> outputSchemaFuture =
                ((PulsarClientImpl) pulsarClient).getSchema(outputTopic);
        outputSchemaFuture.whenComplete((schemaInfo, throwable) -> {
            if (throwable != null) {
                log.error("get output schemaInfo error", throwable);
                throwable.printStackTrace();
                return;
            }
            if (schemaInfo.isPresent()) {
                log.info("outputSchemaInfo: {}", schemaInfo.get().toString());
            } else {
                log.error("output schema is not present!");
            }
        });

        submitFunction(
                Runtime.JAVA,
                inputTopic,
                outputTopic,
                functionName,
                null,
                AvroSchemaTestFunction.class.getName(),
                Schema.AVRO(AvroTestObject.class));
        log.info("pulsar submitFunction");

        getFunctionInfoSuccess(functionName);

        AvroSchemaTestFunction function = new AvroSchemaTestFunction();
        Set<Object> expectedSet = new HashSet<>();

        log.info("test-avro-schema producer connected: " + producer.isConnected());
        for (int i = 0 ; i < numMessages ; i++) {
            AvroTestObject inputObject = new AvroTestObject();
            inputObject.setBaseValue(i);
            MessageId messageId = producer.send(inputObject);
            log.info("test-avro-schema messageId: {}", messageId.toString());
            expectedSet.add(function.process(inputObject, null));
            log.info("test-avro-schema expectedSet size: {}", expectedSet.size());
        }
        getFunctionStatus(functionName, numMessages, false);
        log.info("test-avro-schema producer send message finish");

        CompletableFuture<Optional<SchemaInfo>> outputSchemaFuture2 =
                ((PulsarClientImpl) pulsarClient).getSchema(outputTopic);
        outputSchemaFuture2.whenComplete((schemaInfo, throwable) -> {
            if (throwable != null) {
                log.error("get output schemaInfo error", throwable);
                throwable.printStackTrace();
                return;
            }
            if (schemaInfo.isPresent()) {
                log.info("outputSchemaInfo: {}", schemaInfo.get().toString());
            } else {
                log.error("output schema is not present!");
            }
        });

        log.info("test-avro-schema consumer connected: " + consumer.isConnected());
        for (int i = 0 ; i < numMessages ; i++) {
            log.info("test-avro-schema consumer receive [{}] start", i);
            Message<AvroTestObject> message = consumer.receive();
            log.info("test-avro-schema consumer receive [{}] over", i);
            AvroTestObject outputObject = message.getValue();
            assertTrue(expectedSet.contains(outputObject));
            expectedSet.remove(outputObject);
            consumer.acknowledge(message);
        }
        log.info("test-avro-schema consumer receive message finish");

        assertEquals(expectedSet.size(), 0);

        deleteFunction(functionName);

        getFunctionInfoNotFound(functionName);
    }

    private void testDebeziumMySqlConnect(String converterClassName, boolean jsonWithEnvelope) throws Exception {

        final String tenant = TopicName.PUBLIC_TENANT;
        final String namespace = TopicName.DEFAULT_NAMESPACE;
        final String outputTopicName = "debe-output-topic-name";
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

    private  void testDebeziumPostgreSqlConnect(String converterClassName, boolean jsonWithEnvelope) throws Exception {

        final String tenant = TopicName.PUBLIC_TENANT;
        final String namespace = TopicName.DEFAULT_NAMESPACE;
        final String outputTopicName = "debe-output-topic-name";
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

    private void initNamespace(PulsarAdmin admin) {
        log.info("[initNamespace] start.");
        try {
            admin.tenants().createTenant("debezium", new TenantInfo(Sets.newHashSet(),
                    Sets.newHashSet(pulsarCluster.getClusterName())));
            admin.namespaces().createNamespace("debezium/mysql-json");
            admin.namespaces().createNamespace("debezium/mysql-avro");
            admin.namespaces().createNamespace("debezium/mongodb");
            admin.namespaces().createNamespace("debezium/postgresql");
        } catch (Exception e) {
            log.info("[initNamespace] msg: {}", e.getMessage());
        }
        log.info("[initNamespace] finish.");
    }

    private Schema getSchema(boolean jsonWithEnvelope) {
        if (jsonWithEnvelope) {
            return KeyValueSchema.kvBytes();
        } else {
            return KeyValueSchema.of(Schema.AUTO_CONSUME(), Schema.AUTO_CONSUME(), KeyValueEncodingType.SEPARATED);
        }
    }

}
