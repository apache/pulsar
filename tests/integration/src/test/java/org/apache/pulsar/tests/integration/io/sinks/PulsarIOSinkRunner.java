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
package org.apache.pulsar.tests.integration.io.sinks;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.schema.AvroSchema;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.SinkStatus;
import org.apache.pulsar.common.policies.data.SinkStatusUtil;
import org.apache.pulsar.tests.integration.docker.ContainerExecException;
import org.apache.pulsar.tests.integration.docker.ContainerExecResult;
import org.apache.pulsar.tests.integration.io.PulsarIOTestRunner;
import org.apache.pulsar.tests.integration.io.sinks.JdbcPostgresSinkTester.Foo;
import org.apache.pulsar.tests.integration.topologies.PulsarCluster;
import org.apache.pulsar.tests.integration.topologies.PulsarTestBase;
import org.testcontainers.containers.GenericContainer;
import org.testng.collections.Maps;

import com.google.gson.Gson;

import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import net.jodah.failsafe.Failsafe;

@Slf4j
public class PulsarIOSinkRunner extends PulsarIOTestRunner {

	public PulsarIOSinkRunner(PulsarCluster cluster, String functionRuntimeType) {
		super(cluster, functionRuntimeType);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public <T extends GenericContainer> void runSinkTester(SinkTester<T> tester, boolean builtin) throws Exception {
        final String tenant = TopicName.PUBLIC_TENANT;
        final String namespace = TopicName.DEFAULT_NAMESPACE;
        final String inputTopicName = "test-sink-connector-"
            + tester.getSinkType() + "-" + functionRuntimeType + "-input-topic-" + PulsarTestBase.randomName(8);
        final String sinkName = "test-sink-connector-"
            + tester.getSinkType().name().toLowerCase() + "-" + functionRuntimeType + "-name-" + PulsarTestBase.randomName(8);
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
        try {

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
                kvs = produceMessagesToInputTopic(inputTopicName, numMessages, tester);
                // wait for sink to process messages
                Failsafe.with(statusRetryPolicy).run(() ->
                        waitForProcessingSinkMessages(tenant, namespace, sinkName, numMessages));
                // validate the sink result
                tester.validateSinkResult(kvs);
            }
        } finally {
            // always print the content of the logs, in order to ease debugging
            pulsarCluster.dumpFunctionLogs(sinkName);
        }

        // update the sink
        updateSinkConnector(tester, tenant, namespace, sinkName, inputTopicName);

        // delete the sink
        deleteSink(tenant, namespace, sinkName);

        // get sink info (sink should be deleted)
        getSinkInfoNotFound(tenant, namespace, sinkName);
    }

    @SuppressWarnings("rawtypes")
	protected void prepareSink(SinkTester tester) throws Exception {
        tester.prepareSink();
    }

    protected void submitSinkConnector(@SuppressWarnings("rawtypes") SinkTester tester,
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
                    "--inputs", inputTopicName,
                    "--ram", String.valueOf(RUNTIME_INSTANCE_RAM_BYTES)
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
                    "--ram", String.valueOf(RUNTIME_INSTANCE_RAM_BYTES)
            };
        }
        log.info("Run command : {}", StringUtils.join(commands, ' '));
        ContainerExecResult result = pulsarCluster.getAnyWorker().execCmd(commands);
        assertTrue(
            result.getStdout().contains("Created successfully"),
            result.getStdout());
    }

    protected void updateSinkConnector(@SuppressWarnings("rawtypes") SinkTester tester,
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
                    "--parallelism", "2",
                    "--ram", String.valueOf(RUNTIME_INSTANCE_RAM_BYTES)
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
                    "--parallelism", "2",
                    "--ram", String.valueOf(RUNTIME_INSTANCE_RAM_BYTES)
            };
        }
        log.info("Run command : {}", StringUtils.join(commands, ' '));
        ContainerExecResult result = pulsarCluster.getAnyWorker().execCmd(commands);
        assertTrue(
                result.getStdout().contains("Updated successfully"),
                result.getStdout());
    }

    protected void getSinkInfoSuccess(@SuppressWarnings("rawtypes") SinkTester tester,
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

        SinkStatus sinkStatus = SinkStatusUtil.decode(result.getStdout());

        assertEquals(sinkStatus.getNumInstances(), 1);
        assertEquals(sinkStatus.getNumRunning(), 1);
        assertEquals(sinkStatus.getInstances().size(), 1);
        assertEquals(sinkStatus.getInstances().get(0).getInstanceId(), 0);
        assertEquals(sinkStatus.getInstances().get(0).getStatus().isRunning(), true);
        assertEquals(sinkStatus.getInstances().get(0).getStatus().getNumRestarts(), 0);
        assertEquals(sinkStatus.getInstances().get(0).getStatus().getLatestSystemExceptions().size(), 0);
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
        result.assertNoStderr();
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

        final SinkStatus sinkStatus = SinkStatusUtil.decode(result.getStdout());

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
}
