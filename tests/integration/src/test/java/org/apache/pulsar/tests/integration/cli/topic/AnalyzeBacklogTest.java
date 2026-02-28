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
package org.apache.pulsar.tests.integration.cli.topic;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.Cleanup;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.common.stats.AnalyzeSubscriptionBacklogResult;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.tests.integration.docker.ContainerExecResult;
import org.apache.pulsar.tests.integration.suites.PulsarTestSuite;
import org.testng.annotations.Test;

public class AnalyzeBacklogTest extends PulsarTestSuite {

    private static final String PREFIX = "PULSAR_PREFIX_";
    private static final String ANALYZE_BACKLOG_TOPIC_NAME = "public/default/analyze-backlog-topic";
    private static final String ANALYZE_BACKLOG_SUBSCRIPTION_NAME = "sub1";
    private static final int SUBSCRIPTION_BACKLOG_SCAN_MAX_ENTRIES = 10;
    private static final String LINE_SEPARATOR_REGEX = "\\r?\\n";
    private static final String TOPICS_CMD = "topics";

    @Override
    public void setupCluster() throws Exception {
        brokerEnvs.put(PREFIX + "subscriptionBacklogScanMaxEntries",
                String.valueOf(SUBSCRIPTION_BACKLOG_SCAN_MAX_ENTRIES));
        super.setupCluster();
    }

    @Test
    public void testAnalyzeBacklogUsingDefaultConfig() throws Exception {
        prepareSubscriptionBacklog(SUBSCRIPTION_BACKLOG_SCAN_MAX_ENTRIES + 1);

        ContainerExecResult result =
                pulsarCluster.runAdminCommandOnAnyBroker(TOPICS_CMD, "analyze-backlog", ANALYZE_BACKLOG_TOPIC_NAME,
                        "-s", ANALYZE_BACKLOG_SUBSCRIPTION_NAME);

        String stdout = result.getStdout();
        AnalyzeSubscriptionBacklogResult backlogResult =
                jsonMapper().readValue(stdout, AnalyzeSubscriptionBacklogResult.class);
        assertEquals(SUBSCRIPTION_BACKLOG_SCAN_MAX_ENTRIES, backlogResult.getEntries());

        String[] lines = stdout.split(LINE_SEPARATOR_REGEX);
        assertTrue(lines.length > 1);
    }

    @Test
    public void testAnalyzeBacklogUsingPlainPrint() throws Exception {
        prepareSubscriptionBacklog(SUBSCRIPTION_BACKLOG_SCAN_MAX_ENTRIES + 1);

        ContainerExecResult result =
                pulsarCluster.runAdminCommandOnAnyBroker(TOPICS_CMD, "analyze-backlog", ANALYZE_BACKLOG_TOPIC_NAME,
                        "-s", ANALYZE_BACKLOG_SUBSCRIPTION_NAME, "-pp", "false");

        String stdout = result.getStdout();
        AnalyzeSubscriptionBacklogResult backlogResult =
                jsonMapper().readValue(stdout, AnalyzeSubscriptionBacklogResult.class);
        assertEquals(SUBSCRIPTION_BACKLOG_SCAN_MAX_ENTRIES, backlogResult.getEntries());

        String[] lines = stdout.split(LINE_SEPARATOR_REGEX);
        assertEquals(1, lines.length);
    }

    @Test
    public void testAnalyzeBacklogClientSideLoopUsingPlainPrint() throws Exception {
        int backlogNum = 35;
        prepareSubscriptionBacklog(backlogNum);

        ContainerExecResult result =
                pulsarCluster.runAdminCommandOnAnyBroker(TOPICS_CMD, "analyze-backlog", ANALYZE_BACKLOG_TOPIC_NAME,
                        "-s", ANALYZE_BACKLOG_SUBSCRIPTION_NAME, "-pp", "false");

        int expectedResultLines = 4;
        String stdout = result.getStdout();
        String[] lines = stdout.split(LINE_SEPARATOR_REGEX);
        assertEquals(expectedResultLines, lines.length);

        for (int i = 1; i <= expectedResultLines; i++) {
            AnalyzeSubscriptionBacklogResult backlogResult =
                    jsonMapper().readValue(lines[i], AnalyzeSubscriptionBacklogResult.class);
            assertEquals((long) SUBSCRIPTION_BACKLOG_SCAN_MAX_ENTRIES * i, backlogResult.getEntries());
        }
    }

    @Test
    public void testAnalyzeBacklogClientSideLoopUsingQuietPlainPrint() throws Exception {
        int backlogNum = 30;
        prepareSubscriptionBacklog(backlogNum);

        ContainerExecResult result =
                pulsarCluster.runAdminCommandOnAnyBroker(TOPICS_CMD, "analyze-backlog", ANALYZE_BACKLOG_TOPIC_NAME,
                        "-s", ANALYZE_BACKLOG_SUBSCRIPTION_NAME, "-q", "true", "-pp", "false");

        String stdout = result.getStdout();
        String[] lines = stdout.split(LINE_SEPARATOR_REGEX);
        assertEquals(1, lines.length);

        AnalyzeSubscriptionBacklogResult backlogResult =
                jsonMapper().readValue(stdout, AnalyzeSubscriptionBacklogResult.class);
        assertEquals(backlogNum, backlogResult.getEntries());
    }

    private void prepareSubscriptionBacklog(int backlogNum) throws Exception {
        @Cleanup
        PulsarClient client = PulsarClient.builder().serviceUrl(pulsarCluster.getPlainTextServiceUrl()).build();
        @Cleanup
        Producer<byte[]> producer =
                client.newProducer().topic(ANALYZE_BACKLOG_TOPIC_NAME).enableBatching(false).create();
        @Cleanup
        Consumer<byte[]> consumer = client.newConsumer().topic(ANALYZE_BACKLOG_TOPIC_NAME)
                .subscriptionName(ANALYZE_BACKLOG_SUBSCRIPTION_NAME).subscribe();

        List<CompletableFuture<MessageId>> futures = new ArrayList<>(backlogNum);
        for (int i = 0; i < backlogNum; i++) {
            byte[] msgBytes = ("test" + i).getBytes(StandardCharsets.UTF_8);
            CompletableFuture<MessageId> future = producer.sendAsync(msgBytes);
            futures.add(future);
        }
        FutureUtil.waitForAll(futures).get();
    }

}
