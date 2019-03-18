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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.google.common.base.Stopwatch;
import com.google.gson.Gson;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.schema.AvroSchema;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.FunctionStats;
import org.apache.pulsar.common.policies.data.FunctionStatus;
import org.apache.pulsar.common.policies.data.SinkStatus;
import org.apache.pulsar.common.policies.data.SourceStatus;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.apache.pulsar.functions.api.examples.AutoSchemaFunction;
import org.apache.pulsar.functions.api.examples.serde.CustomObject;
import org.apache.pulsar.tests.integration.containers.DebeziumMySQLContainer;
import org.apache.pulsar.tests.integration.docker.ContainerExecException;
import org.apache.pulsar.tests.integration.docker.ContainerExecResult;
import org.apache.pulsar.tests.integration.functions.utils.CommandGenerator;
import org.apache.pulsar.tests.integration.functions.utils.CommandGenerator.Runtime;
import org.apache.pulsar.tests.integration.io.*;
import org.apache.pulsar.tests.integration.io.JdbcSinkTester.Foo;
import org.apache.pulsar.tests.integration.topologies.FunctionRuntimeType;
import org.apache.pulsar.tests.integration.topologies.PulsarCluster;
import org.testcontainers.containers.GenericContainer;
import org.testng.annotations.Test;

/**
 * A test base for testing sink.
 */
@Slf4j
public abstract class PulsarFunctionsTest extends PulsarFunctionsTestBase {

    PulsarFunctionsTest(FunctionRuntimeType functionRuntimeType) {
        super(functionRuntimeType);
    }

    @Test
    public void testKafkaSink() throws Exception {
        String kafkaContainerName = "kafka-" + randomName(8);
        testSink(new KafkaSinkTester(kafkaContainerName), true, new KafkaSourceTester(kafkaContainerName));
    }

    @Test(enabled = false)
    public void testCassandraSink() throws Exception {
        testSink(CassandraSinkTester.createTester(true), true);
    }

    @Test(enabled = false)
    public void testCassandraArchiveSink() throws Exception {
        testSink(CassandraSinkTester.createTester(false), false);
    }

    @Test(enabled = false)
    public void testHdfsSink() throws Exception {
        testSink(new HdfsSinkTester(), false);
    }

    @Test
    public void testJdbcSink() throws Exception {
        testSink(new JdbcSinkTester(), true);
    }

    @Test(enabled = false)
    public void testElasticSearchSink() throws Exception {
        testSink(new ElasticSearchSinkTester(), true);
    }

    @Test
    public void testDebeziumMySqlSource() throws Exception {
        testDebeziumMySqlConnect();
    }

    private void testSink(SinkTester tester, boolean builtin) throws Exception {
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
    private void runSinkTester(SinkTester tester, boolean builtin) throws Exception {
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
        getSinkStatus(tenant, namespace, sinkName);

        // produce messages
        Map<String, String> kvs;
        if (tester instanceof JdbcSinkTester) {
            kvs = produceSchemaMessagesToInputTopic(inputTopicName, numMessages, AvroSchema.of(JdbcSinkTester.Foo.class));
        } else {
            kvs = produceMessagesToInputTopic(inputTopicName, numMessages);
        }

        // wait for sink to process messages
        waitForProcessingSinkMessages(tenant, namespace, sinkName, numMessages);

        // validate the sink result
        tester.validateSinkResult(kvs);

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
                    "--sink-type", tester.sinkType().name().toLowerCase(),
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
                    result.getStdout().contains("\"archive\": \"builtin://" + tester.getSinkType().name().toLowerCase() + "\""),
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
        String[] commands = {
            PulsarCluster.ADMIN_SCRIPT,
            "sink",
            "status",
            "--tenant", tenant,
            "--namespace", namespace,
            "--name", sinkName
        };
        while (true) {
            try {
                ContainerExecResult result = pulsarCluster.getAnyWorker().execCmd(commands);
                log.info("Get sink status : {}", result.getStdout());

                assertEquals(result.getExitCode(), 0);

                SinkStatus sinkStatus = SinkStatus.decode(result.getStdout());
                try {
                    assertEquals(sinkStatus.getNumInstances(), 1);
                    assertEquals(sinkStatus.getNumRunning(), 1);
                    assertEquals(sinkStatus.getInstances().size(), 1);
                    assertEquals(sinkStatus.getInstances().get(0).getInstanceId(), 0);
                    assertEquals(sinkStatus.getInstances().get(0).getStatus().isRunning(), true);
                    assertEquals(sinkStatus.getInstances().get(0).getStatus().getNumRestarts(), 0);
                    assertEquals(sinkStatus.getInstances().get(0).getStatus().getLatestSystemExceptions().size(), 0);
                    return;
                } catch (Exception e) {
                    // noop
                }
            } catch (ContainerExecException e) {
                // expected in early iterations
            }
            log.info("Backoff 1 second until the sink is running");
            TimeUnit.SECONDS.sleep(1);
        }
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

    // This for JdbcSinkTester
    protected Map<String, String> produceSchemaMessagesToInputTopic(String inputTopicName,
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

            JdbcSinkTester.Foo obj = new JdbcSinkTester.Foo();
            obj.setField1("field1_" + i);
            obj.setField2("field2_" + i);
            obj.setField3(i);
            String value = new String(schema.encode(obj));

            kvs.put(key, value);
            producer.newMessage()
                .key(key)
                .value(obj)
                .send();
        }
        return kvs;
    }

    protected void deleteSink(String tenant, String namespace, String sinkName) throws Exception {
        String[] commands = {
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
        String[] commands = {
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

    private void testSource(SourceTester tester)  throws Exception {
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
        getSourceStatus(tenant, namespace, sourceName);

        // produce messages
        Map<String, String> kvs = tester.produceSourceMessages(numMessages);

        // wait for source to process messages
        waitForProcessingSourceMessages(tenant, namespace, sourceName, numMessages);

        // validate the source result
        validateSourceResult(consumer, kvs);

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
        String[] commands = {
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

    protected void getSourceInfoSuccess(SourceTester tester,
                                        String tenant,
                                        String namespace,
                                        String sourceName) throws Exception {
        String[] commands = {
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
        String[] commands = {
            PulsarCluster.ADMIN_SCRIPT,
            "source",
            "status",
            "--tenant", tenant,
            "--namespace", namespace,
            "--name", sourceName
        };
        while (true) {
            try {
                ContainerExecResult result = pulsarCluster.getAnyWorker().execCmd(commands);
                log.info("Get source status : {}", result.getStdout());

                assertEquals(result.getExitCode(), 0);

                SourceStatus sourceStatus = SourceStatus.decode(result.getStdout());
                try {
                    assertEquals(sourceStatus.getNumInstances(), 1);
                    assertEquals(sourceStatus.getNumRunning(), 1);
                    assertEquals(sourceStatus.getInstances().size(), 1);
                    assertEquals(sourceStatus.getInstances().get(0).getStatus().isRunning(), true);
                    assertEquals(sourceStatus.getInstances().get(0).getStatus().getNumRestarts(), 0);
                    assertEquals(sourceStatus.getInstances().get(0).getStatus().getLatestSystemExceptions().size(), 0);
                    return;
                } catch (Exception e) {
                    // noop
                }

                if (result.getStdout().contains("\"running\": true")) {
                    return;
                }
            } catch (ContainerExecException e) {
                // expected for early iterations
            }
            log.info("Backoff 1 second until the function is running");
            TimeUnit.SECONDS.sleep(1);
        }
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
        String[] commands = {
            PulsarCluster.ADMIN_SCRIPT,
            "source",
            "status",
            "--tenant", tenant,
            "--namespace", namespace,
            "--name", sourceName
        };
        Stopwatch stopwatch = Stopwatch.createStarted();
        while (true) {
            try {
                ContainerExecResult result = pulsarCluster.getAnyWorker().execCmd(commands);
                log.info("Get source status : {}", result.getStdout());

                assertEquals(result.getExitCode(), 0);

                SourceStatus sourceStatus = SourceStatus.decode(result.getStdout());
                try {
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
                    return;
                } catch (Exception e) {
                    // noop
                }
            } catch (ContainerExecException e) {
                // expected for early iterations
            }
            log.info("{} ms has elapsed but the source hasn't process {} messages, backoff to wait for another 1 second",
                stopwatch.elapsed(TimeUnit.MILLISECONDS), numMessages);
            TimeUnit.SECONDS.sleep(1);
        }
    }

    protected void waitForProcessingSinkMessages(String tenant,
                                                 String namespace,
                                                 String sinkName,
                                                 int numMessages) throws Exception {
        String[] commands = {
                PulsarCluster.ADMIN_SCRIPT,
                "sink",
                "status",
                "--tenant", tenant,
                "--namespace", namespace,
                "--name", sinkName
        };
        Stopwatch stopwatch = Stopwatch.createStarted();
        while (true) {
            try {
                ContainerExecResult result = pulsarCluster.getAnyWorker().execCmd(commands);
                log.info("Get sink status : {}", result.getStdout());

                assertEquals(result.getExitCode(), 0);

                SinkStatus sinkStatus = SinkStatus.decode(result.getStdout());
                try {
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
                    return;
                } catch (Exception e) {
                    // noop
                }

            } catch (ContainerExecException e) {
                // expected for early iterations
            }
            log.info("{} ms has elapsed but the sink hasn't process {} messages, backoff to wait for another 1 second",
                    stopwatch.elapsed(TimeUnit.MILLISECONDS), numMessages);
            TimeUnit.SECONDS.sleep(1);
        }
    }

    protected void deleteSource(String tenant, String namespace, String sourceName) throws Exception {
        String[] commands = {
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
        String[] commands = {
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

    //
    // Test CRUD functions on different runtimes.
    //

    @Test
    public void testPythonExclamationFunction() throws Exception {
        testExclamationFunction(Runtime.PYTHON, false, false, false);
    }

    @Test
    public void testPythonExclamationFunctionWithExtraDeps() throws Exception {
        testExclamationFunction(Runtime.PYTHON, false, false, true);
    }

    @Test
    public void testPythonExclamationZipFunction() throws Exception {
        testExclamationFunction(Runtime.PYTHON, false, true, false);
    }

    @Test
    public void testPythonExclamationTopicPatternFunction() throws Exception {
        testExclamationFunction(Runtime.PYTHON, true, false, false);
    }

    @Test
    public void testJavaExclamationFunction() throws Exception {
        testExclamationFunction(Runtime.JAVA, false, false, false);
    }

    @Test
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

        Schema<?> schema;
        if (Runtime.JAVA == runtime) {
            schema = Schema.STRING;
        } else {
            schema = Schema.BYTES;
        }

        String inputTopicName = "persistent://public/default/test-exclamation-" + runtime + "-input-" + randomName(8);
        String outputTopicName = "test-exclamation-" + runtime + "-output-" + randomName(8);
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
        getFunctionStatus(functionName, numMessages);

        // get function stats
        getFunctionStats(functionName, numMessages);

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
            getExclamationClass(runtime, pyZip, withExtraDeps),
            schema);
    }

    private static <T> void submitFunction(Runtime runtime,
                                           String inputTopicName,
                                           String outputTopicName,
                                           String functionName,
                                           boolean pyZip,
                                           boolean withExtraDeps,
                                           String functionClass,
                                           Schema<T> inputTopicSchema) throws Exception {
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
        String command;
        if (Runtime.JAVA == runtime) {
            command = generator.generateCreateFunctionCommand();
        } else if (Runtime.PYTHON == runtime) {
            generator.setRuntime(runtime);
            if (pyZip) {
                command = generator.generateCreateFunctionCommand(EXCLAMATION_PYTHONZIP_FILE);
            } else if (withExtraDeps) {
                command = generator.generateCreateFunctionCommand(EXCLAMATION_WITH_DEPS_PYTHON_FILE);
            } else {
                command = generator.generateCreateFunctionCommand(EXCLAMATION_PYTHON_FILE);
            }
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

        ensureSubscriptionCreated(inputTopicName, String.format("public/default/%s", functionName), inputTopicSchema);
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
        try {
            pulsarCluster.getAnyWorker().execCmd(
                    PulsarCluster.ADMIN_SCRIPT,
                    "functions",
                    "get",
                    "--tenant", "public",
                    "--namespace", "default",
                    "--name", functionName);
            fail("Command should have exited with non-zero");
        } catch (ContainerExecException e) {
            assertTrue(e.getResult().getStderr().contains("Reason: Function " + functionName + " doesn't exist"));
        }
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

    private static void getFunctionStatus(String functionName, int numMessages) throws Exception {
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
        assertEquals(functionStatus.getInstances().get(0).getStatus().getNumReceived(), numMessages);
        assertEquals(functionStatus.getInstances().get(0).getStatus().getNumSuccessfullyProcessed(), numMessages);
        assertEquals(functionStatus.getInstances().get(0).getStatus().getNumRestarts(), 0);
        assertEquals(functionStatus.getInstances().get(0).getStatus().getLatestUserExceptions().size(), 0);
        assertEquals(functionStatus.getInstances().get(0).getStatus().getLatestSystemExceptions().size(), 0);
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

    @Test
    public void testAutoSchemaFunction() throws Exception {
        String inputTopicName = "test-autoschema-input-" + randomName(8);
        String outputTopicName = "test-autoshcema-output-" + randomName(8);
        String functionName = "test-autoschema-fn-" + randomName(8);
        final int numMessages = 10;

        // submit the exclamation function
        submitFunction(
            Runtime.JAVA, inputTopicName, outputTopicName, functionName, false, false,
            AutoSchemaFunction.class.getName(),
            Schema.AVRO(CustomObject.class));

        // get function info
        getFunctionInfoSuccess(functionName);

        // publish and consume result
        publishAndConsumeAvroMessages(inputTopicName, outputTopicName, numMessages);

        // get function status
        getFunctionStatus(functionName, numMessages);

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

    private  void testDebeziumMySqlConnect()
        throws Exception {

        final String tenant = TopicName.PUBLIC_TENANT;
        final String namespace = TopicName.DEFAULT_NAMESPACE;
        final String outputTopicName = "debe-output-topic-name";
        final String consumeTopicName = "public/default/dbserver1.inventory.products";
        final String sourceName = "test-source-connector-"
            + functionRuntimeType + "-name-" + randomName(8);

        // This is the binlog count that contained in mysql container.
        final int numMessages = 47;

        @Cleanup
        PulsarClient client = PulsarClient.builder()
            .serviceUrl(pulsarCluster.getPlainTextServiceUrl())
            .build();

        @Cleanup
        Consumer<String> consumer = client.newConsumer(Schema.STRING)
            .topic(consumeTopicName)
            .subscriptionName("debezium-source-tester")
            .subscriptionType(SubscriptionType.Exclusive)
            .subscribe();

        @Cleanup
        DebeziumMySqlSourceTester sourceTester = new DebeziumMySqlSourceTester(pulsarCluster);

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
        getSourceStatus(tenant, namespace, sourceName);

        // wait for source to process messages
        waitForProcessingSourceMessages(tenant, namespace, sourceName, numMessages);

        // validate the source result
        sourceTester.validateSourceResult(consumer, 9);

        // delete the source
        deleteSource(tenant, namespace, sourceName);

        // get source info (source should be deleted)
        getSourceInfoNotFound(tenant, namespace, sourceName);
    }

}
