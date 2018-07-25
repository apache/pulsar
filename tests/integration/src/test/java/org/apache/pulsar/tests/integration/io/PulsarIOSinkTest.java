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
package org.apache.pulsar.tests.integration.io;

import static org.testng.Assert.assertTrue;

import com.google.common.base.Stopwatch;
import com.google.gson.Gson;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.tests.integration.functions.PulsarFunctionsTestBase;
import org.apache.pulsar.tests.integration.topologies.FunctionRuntimeType;
import org.apache.pulsar.tests.integration.topologies.PulsarCluster;
import org.apache.pulsar.tests.integration.topologies.PulsarClusterSpec.PulsarClusterSpecBuilder;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.containers.GenericContainer;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;
import org.testng.collections.Maps;

/**
 * A test base for testing sink.
 */
@Slf4j
public class PulsarIOSinkTest extends PulsarFunctionsTestBase {

    @DataProvider(name = "Sinks")
    public static Object[][] getData() {
        return new Object[][] {
            { FunctionRuntimeType.PROCESS,  new CassandraSinkTester() },
            { FunctionRuntimeType.THREAD, new CassandraSinkTester() },
            { FunctionRuntimeType.PROCESS, new KafkaSinkTester() },
            { FunctionRuntimeType.THREAD, new KafkaSinkTester() }
        };
    }

    protected final SinkTester tester;

    @Factory(dataProvider = "Sinks")
    PulsarIOSinkTest(FunctionRuntimeType functionRuntimeType, SinkTester tester) {
        super(functionRuntimeType);
        this.tester = tester;
    }

    @Override
    protected PulsarClusterSpecBuilder beforeSetupCluster(String clusterName,
                                                          PulsarClusterSpecBuilder specBuilder) {
        Map<String, GenericContainer<?>> externalServices = Maps.newHashMap();
        externalServices.putAll(tester.newSinkService(clusterName));
        return super.beforeSetupCluster(clusterName, specBuilder)
            .externalServices(externalServices);
    }

    @Test
    public void testSink() throws Exception {
        final String tenant = TopicName.PUBLIC_TENANT;
        final String namespace = TopicName.DEFAULT_NAMESPACE;
        final String inputTopicName = "test-sink-connector-input-topic-" + randomName(8);
        final String sinkName = "test-sink-connector-name-" + randomName(8);
        final int numMessages = 20;

        // prepare the testing environment for sink
        prepareSink();

        // submit the sink connector
        submitSinkConnector(tenant, namespace, sinkName, inputTopicName);

        // get sink info
        getSinkInfoSuccess(tenant, namespace, sinkName);

        // get sink status
        getSinkStatus(tenant, namespace, sinkName);

        // produce messages
        Map<String, String> kvs = produceMessagesToInputTopic(inputTopicName, numMessages);

        // wait for sink to process messages
        waitForProcessingMessages(tenant, namespace, sinkName, numMessages);

        // validate the sink result
        tester.validateSinkResult(kvs);

        // delete the sink
        deleteSink(tenant, namespace, sinkName);

        // get sink info (sink should be deleted)
        getSinkInfoNotFound(tenant, namespace, sinkName);
    }

    protected void prepareSink() throws Exception {
        tester.prepareSink();
    }

    protected void submitSinkConnector(String tenant,
                                       String namespace,
                                       String sinkName,
                                       String inputTopicName) throws Exception {
        String[] commands = {
            PulsarCluster.ADMIN_SCRIPT,
            "sink", "create",
            "--tenant", tenant,
            "--namespace", namespace,
            "--name", sinkName,
            "--sink-type", tester.sinkType(),
            "--sinkConfig", new Gson().toJson(tester.sinkConfig()),
            "--inputs", inputTopicName
        };
        log.info("Run command : {}", StringUtils.join(commands, ' '));
        ExecResult result = pulsarCluster.getAnyWorker().execCmd(commands);
        assertTrue(
            result.getStdout().contains("\"Created successfully\""),
            result.getStdout());
    }

    protected void getSinkInfoSuccess(String tenant, String namespace, String sinkName) throws Exception {
        String[] commands = {
            PulsarCluster.ADMIN_SCRIPT,
            "functions",
            "get",
            "--tenant", tenant,
            "--namespace", namespace,
            "--name", sinkName
        };
        ExecResult result = pulsarCluster.getAnyWorker().execCmd(commands);
        log.info("Get sink info : {}", result.getStdout());
        assertTrue(
            result.getStdout().contains("\"builtin\": \"" + tester.sinkType + "\""),
            result.getStdout()
        );
    }

    protected void getSinkStatus(String tenant, String namespace, String sinkName) throws Exception {
        String[] commands = {
            PulsarCluster.ADMIN_SCRIPT,
            "functions",
            "getstatus",
            "--tenant", tenant,
            "--namespace", namespace,
            "--name", sinkName
        };
        while (true) {
            ExecResult result = pulsarCluster.getAnyWorker().execCmd(commands);
            log.info("Get sink status : {}", result.getStdout());
            if (result.getStdout().contains("\"running\": true")) {
                return;
            }
            log.info("Backoff 1 second until the function is running");
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

    protected void waitForProcessingMessages(String tenant,
                                             String namespace,
                                             String sinkName,
                                             int numMessages) throws Exception {
        String[] commands = {
            PulsarCluster.ADMIN_SCRIPT,
            "functions",
            "getstatus",
            "--tenant", tenant,
            "--namespace", namespace,
            "--name", sinkName
        };
        Stopwatch stopwatch = Stopwatch.createStarted();
        while (true) {
            ExecResult result = pulsarCluster.getAnyWorker().execCmd(commands);
            log.info("Get sink status : {}", result.getStdout());
            if (result.getStdout().contains("\"numProcessed\": \"" + numMessages + "\"")) {
                return;
            }
            log.info("{} ms has elapsed but the sink hasn't process {} messages, backoff to wait for another 1 second",
                stopwatch.elapsed(TimeUnit.MILLISECONDS), numMessages);
            TimeUnit.SECONDS.sleep(1);
        }
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
        ExecResult result = pulsarCluster.getAnyWorker().execCmd(commands);
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
            "functions",
            "get",
            "--tenant", tenant,
            "--namespace", namespace,
            "--name", sinkName
        };
        ExecResult result = pulsarCluster.getAnyWorker().execCmd(commands);
        assertTrue(result.getStderr().contains("Reason: Function " + sinkName + " doesn't exist"));
    }
}
