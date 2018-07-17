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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.google.common.base.Stopwatch;
import com.google.gson.Gson;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.tests.integration.functions.PulsarFunctionsTestBase;
import org.apache.pulsar.tests.topologies.FunctionRuntimeType;
import org.apache.pulsar.tests.topologies.PulsarCluster;
import org.apache.pulsar.tests.topologies.PulsarClusterSpec.PulsarClusterSpecBuilder;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.containers.GenericContainer;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;
import org.testng.collections.Maps;

/**
 * A test base for testing source.
 */
@Slf4j
public class PulsarIOSourceTest extends PulsarFunctionsTestBase {

    @DataProvider(name = "Sources")
    public static Object[][] getData() {
        return new Object[][] {
            { FunctionRuntimeType.PROCESS, new KafkaSourceTester() },
            { FunctionRuntimeType.THREAD, new KafkaSourceTester() }
        };
    }

    protected final SourceTester tester;

    @Factory(dataProvider = "Sources")
    PulsarIOSourceTest(FunctionRuntimeType functionRuntimeType, SourceTester tester) {
        super(functionRuntimeType);
        this.tester = tester;
    }

    @Override
    protected PulsarClusterSpecBuilder beforeSetupCluster(String clusterName,
                                                          PulsarClusterSpecBuilder specBuilder) {
        Map<String, GenericContainer<?>> externalServices = Maps.newHashMap();
        externalServices.putAll(tester.newSourceService(clusterName));
        return super.beforeSetupCluster(clusterName, specBuilder)
            .externalServices(externalServices);
    }

    @Test
    public void testSource() throws Exception {
        final String tenant = TopicName.PUBLIC_TENANT;
        final String namespace = TopicName.DEFAULT_NAMESPACE;
        final String outputTopicName = "test-source-connector-output-topic-" + randomName(8);
        final String sourceName = "test-source-connector-name-" + randomName(8);
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
        prepareSource();

        // submit the source connector
        submitSourceConnector(tenant, namespace, sourceName, outputTopicName);

        // get source info
        getSourceInfoSuccess(tenant, namespace, sourceName);

        // get source status
        getSourceStatus(tenant, namespace, sourceName);

        // produce messages
        Map<String, String> kvs = tester.produceSourceMessages(numMessages);

        // wait for source to process messages
        waitForProcessingMessages(tenant, namespace, sourceName, numMessages);

        // validate the source result
        validateSourceResult(consumer, kvs);

        // delete the source
        deleteSource(tenant, namespace, sourceName);

        // get source info (source should be deleted)
        getSourceInfoNotFound(tenant, namespace, sourceName);
    }

    protected void prepareSource() throws Exception {
        tester.prepareSource();
    }

    protected void submitSourceConnector(String tenant,
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
        ExecResult result = pulsarCluster.getAnyWorker().execCmd(commands);
        assertTrue(
            result.getStdout().contains("\"Created successfully\""),
            result.getStdout());
    }

    protected void getSourceInfoSuccess(String tenant, String namespace, String sourceName) throws Exception {
        String[] commands = {
            PulsarCluster.ADMIN_SCRIPT,
            "functions",
            "get",
            "--tenant", tenant,
            "--namespace", namespace,
            "--name", sourceName
        };
        ExecResult result = pulsarCluster.getAnyWorker().execCmd(commands);
        log.info("Get source info : {}", result.getStdout());
        assertTrue(
            result.getStdout().contains("\"builtin\": \"" + tester.sourceType + "\""),
            result.getStdout()
        );
    }

    protected void getSourceStatus(String tenant, String namespace, String sourceName) throws Exception {
        String[] commands = {
            PulsarCluster.ADMIN_SCRIPT,
            "functions",
            "getstatus",
            "--tenant", tenant,
            "--namespace", namespace,
            "--name", sourceName
        };
        while (true) {
            ExecResult result = pulsarCluster.getAnyWorker().execCmd(commands);
            log.info("Get source status : {}", result.getStdout());
            if (result.getStdout().contains("\"running\": true")) {
                return;
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

    protected void waitForProcessingMessages(String tenant,
                                             String namespace,
                                             String sourceName,
                                             int numMessages) throws Exception {
        String[] commands = {
            PulsarCluster.ADMIN_SCRIPT,
            "functions",
            "getstatus",
            "--tenant", tenant,
            "--namespace", namespace,
            "--name", sourceName
        };
        Stopwatch stopwatch = Stopwatch.createStarted();
        while (true) {
            ExecResult result = pulsarCluster.getAnyWorker().execCmd(commands);
            log.info("Get source status : {}", result.getStdout());
            if (result.getStdout().contains("\"numProcessed\": \"" + numMessages + "\"")) {
                return;
            }
            log.info("{} ms has elapsed but the source hasn't process {} messages, backoff to wait for another 1 second",
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
        ExecResult result = pulsarCluster.getAnyWorker().execCmd(commands);
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
            "functions",
            "get",
            "--tenant", tenant,
            "--namespace", namespace,
            "--name", sourceName
        };
        ExecResult result = pulsarCluster.getAnyWorker().execCmd(commands);
        assertTrue(result.getStderr().contains("Reason: Function " + sourceName + " doesn't exist"));
    }
}
