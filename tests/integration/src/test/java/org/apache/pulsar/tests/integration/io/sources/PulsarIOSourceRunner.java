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
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.SourceStatus;
import org.apache.pulsar.common.policies.data.SourceStatusUtil;
import org.apache.pulsar.tests.integration.docker.ContainerExecException;
import org.apache.pulsar.tests.integration.docker.ContainerExecResult;
import org.apache.pulsar.tests.integration.io.PulsarIOTestRunner;
import org.apache.pulsar.tests.integration.topologies.PulsarCluster;
import org.apache.pulsar.tests.integration.topologies.PulsarTestBase;
import org.testcontainers.containers.GenericContainer;

import com.google.gson.Gson;

import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import net.jodah.failsafe.Failsafe;

@Slf4j
public class PulsarIOSourceRunner extends PulsarIOTestRunner {

    public PulsarIOSourceRunner(PulsarCluster cluster, String functionRuntimeType) {
		super(cluster, functionRuntimeType);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public <T extends GenericContainer> void testSource(SourceTester<T> tester)  throws Exception {
        final String tenant = TopicName.PUBLIC_TENANT;
        final String namespace = TopicName.DEFAULT_NAMESPACE;
        final String outputTopicName = "test-source-connector-"
            + functionRuntimeType + "-output-topic-" + PulsarTestBase.randomName(8);
        final String sourceName = "test-source-connector-"
            + functionRuntimeType + "-name-" + PulsarTestBase.randomName(8);
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

    @SuppressWarnings("rawtypes")
	protected void prepareSource(SourceTester tester) throws Exception {
        tester.prepareSource();
    }

    @SuppressWarnings("rawtypes")
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
            "--destinationTopicName", outputTopicName,
            "--ram", String.valueOf(RUNTIME_INSTANCE_RAM_BYTES)
        };

        log.info("Run command : {}", StringUtils.join(commands, ' '));
        ContainerExecResult result = pulsarCluster.getAnyWorker().execCmd(commands);
        assertTrue(
            result.getStdout().contains("Created successfully"),
            result.getStdout());
    }

    @SuppressWarnings("rawtypes")
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
                "--parallelism", "2",
                "--ram", String.valueOf(RUNTIME_INSTANCE_RAM_BYTES)
        };

        log.info("Run command : {}", StringUtils.join(commands, ' '));
        ContainerExecResult result = pulsarCluster.getAnyWorker().execCmd(commands);
        assertTrue(
                result.getStdout().contains("Updated successfully"),
                result.getStdout());
    }

    @SuppressWarnings("rawtypes")
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

        final SourceStatus sourceStatus = SourceStatusUtil.decode(result.getStdout());

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

        SourceStatus sourceStatus = SourceStatusUtil.decode(result.getStdout());
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
