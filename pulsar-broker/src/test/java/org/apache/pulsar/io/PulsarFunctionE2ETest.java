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
package org.apache.pulsar.io;

import static org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest.retryStrategically;
import static org.apache.pulsar.functions.worker.PulsarFunctionLocalRunTest.getPulsarApiExamplesJar;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import lombok.Cleanup;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.functions.ConsumerConfig;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.policies.data.FunctionInstanceStatsDataImpl;
import org.apache.pulsar.common.policies.data.FunctionStatsImpl;
import org.apache.pulsar.common.policies.data.FunctionStatus;
import org.apache.pulsar.common.policies.data.SubscriptionStats;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.apache.pulsar.compaction.TwoPhaseCompactor;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.instance.InstanceUtils;
import org.apache.pulsar.functions.utils.FunctionCommon;
import org.apache.pulsar.functions.worker.FunctionRuntimeManager;
import org.apache.pulsar.functions.worker.PulsarFunctionTestUtils;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Test Pulsar sink on function
 */
@Test(groups = "broker-io")
public class PulsarFunctionE2ETest extends AbstractPulsarE2ETest {

    protected static FunctionConfig createFunctionConfig(String tenant, String namespace, String functionName, String sourceTopic, String sinkTopic, String subscriptionName) {
        FunctionConfig functionConfig = new FunctionConfig();
        functionConfig.setTenant(tenant);
        functionConfig.setNamespace(namespace);
        functionConfig.setName(functionName);
        functionConfig.setParallelism(1);
        functionConfig.setProcessingGuarantees(FunctionConfig.ProcessingGuarantees.EFFECTIVELY_ONCE);
        functionConfig.setSubName(subscriptionName);
        if (sourceTopic != null) {
            String sourceTopicPattern = String.format("persistent://%s/%s/%s", tenant, namespace, sourceTopic);
            functionConfig.setTopicsPattern(sourceTopicPattern);
        }
        functionConfig.setAutoAck(true);
        functionConfig.setClassName("org.apache.pulsar.functions.api.examples.ExclamationFunction");
        functionConfig.setRuntime(FunctionConfig.Runtime.JAVA);
        functionConfig.setOutput(sinkTopic);
        functionConfig.setCleanupSubscription(true);
        return functionConfig;
    }

    /**
     * Validates pulsar sink e2e functionality on functions.
     *
     * @throws Exception
     */
    private void testE2EPulsarFunction(String jarFilePathUrl) throws Exception {

        final String namespacePortion = "io";
        final String replNamespace = tenant + "/" + namespacePortion;
        final String sourceTopic = "persistent://" + replNamespace + "/my-topic1";
        final String sinkTopic = "persistent://" + replNamespace + "/output";
        final String sinkTopic2 = "persistent://" + replNamespace + "/output2";
        final String propertyKey = "key";
        final String propertyValue = "value";
        final String functionName = "PulsarFunction-test";
        final String subscriptionName = "test-sub";
        admin.namespaces().createNamespace(replNamespace);
        Set<String> clusters = Sets.newHashSet(Lists.newArrayList("use"));
        admin.namespaces().setNamespaceReplicationClusters(replNamespace, clusters);

        // create a producer that creates a topic at broker
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(sourceTopic).create();
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING).topic(sinkTopic2).subscriptionName("sub").subscribe();

        FunctionConfig functionConfig = createFunctionConfig(tenant, namespacePortion, functionName,
                "my.*", sinkTopic, subscriptionName);
        functionConfig.setProcessingGuarantees(FunctionConfig.ProcessingGuarantees.ATLEAST_ONCE);
        admin.functions().createFunctionWithUrl(functionConfig, jarFilePathUrl);

        // try to update function to test: update-function functionality
        functionConfig.setParallelism(2);
        functionConfig.setOutput(sinkTopic2);
        admin.functions().updateFunctionWithUrl(functionConfig, jarFilePathUrl);

        Awaitility.await().ignoreExceptions().untilAsserted(() -> {
            TopicStats topicStats = admin.topics().getStats(sinkTopic2);
            assertEquals(topicStats.getPublishers().size(), 2);
            assertNotNull(topicStats.getPublishers().get(0).getMetadata());
            assertTrue(topicStats.getPublishers().get(0).getMetadata().containsKey("id"));
            assertEquals(topicStats.getPublishers().get(0).getMetadata().get("id"),
                    String.format("%s/%s/%s", tenant, namespacePortion, functionName));
        });

        Awaitility.await().ignoreExceptions().untilAsserted(() -> {
            // validate pulsar sink consumer has started on the topic
            assertEquals(admin.topics().getStats(sourceTopic).getSubscriptions().size(), 1);
        });

        int totalMsgs = 5;
        for (int i = 0; i < totalMsgs; i++) {
            String data = "my-message-" + i;
            producer.newMessage().property(propertyKey, propertyValue).value(data).send();
        }

        Awaitility.await().ignoreExceptions().untilAsserted(() -> {
            SubscriptionStats subStats = admin.topics().getStats(sourceTopic).getSubscriptions().get(subscriptionName);
            assertEquals(subStats.getUnackedMessages(), 0);
        });

        Message<String> msg = consumer.receive(5, TimeUnit.SECONDS);
        String receivedPropertyValue = msg.getProperty(propertyKey);
        assertEquals(propertyValue, receivedPropertyValue);

        // validate pulsar-sink consumer has consumed all messages and delivered to Pulsar sink but unacked messages
        // due to publish failure
        assertNotEquals(admin.topics().getStats(sourceTopic).getSubscriptions().values().iterator().next().getUnackedMessages(),
                totalMsgs);

        // delete functions
        admin.functions().deleteFunction(tenant, namespacePortion, functionName);

        Awaitility.await().ignoreExceptions().untilAsserted(() -> {
            // make sure subscriptions are cleanup
            assertEquals(admin.topics().getStats(sourceTopic).getSubscriptions().size(), 0);
        });

        tempDirectory.assertThatFunctionDownloadTempFilesHaveBeenDeleted();
    }

    @Test(timeOut = 20000)
    public void testE2EPulsarFunctionWithFile() throws Exception {
        String jarFilePathUrl = getPulsarApiExamplesJar().toURI().toString();
        testE2EPulsarFunction(jarFilePathUrl);
    }

    @Test(timeOut = 40000)
    public void testE2EPulsarFunctionWithUrl() throws Exception {
        testE2EPulsarFunction(fileServer.getUrl("/pulsar-functions-api-examples.jar"));
    }

    @Test(timeOut = 30000)
    public void testReadCompactedFunction() throws Exception {
        final String namespacePortion = "io";
        final String replNamespace = tenant + "/" + namespacePortion;
        final String sourceTopic = "persistent://" + replNamespace + "/my-topic1";
        final String sinkTopic = "persistent://" + replNamespace + "/output";
        final String functionName = "PulsarFunction-test";
        final String subscriptionName = "test-sub";
        admin.namespaces().createNamespace(replNamespace);
        Set<String> clusters = Sets.newHashSet(Lists.newArrayList("use"));
        admin.namespaces().setNamespaceReplicationClusters(replNamespace, clusters);
        final int messageNum = 20;
        final int maxKeys = 10;
        // 1 Setup producer
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(sourceTopic)
                .enableBatching(false)
                .messageRoutingMode(MessageRoutingMode.SinglePartition)
                .create();
        pulsarClient.newConsumer().topic(sourceTopic).subscriptionName(subscriptionName).readCompacted(true).subscribe().close();
        // 2 Send messages and record the expected values after compaction
        Map<String, String> expected = new HashMap<>();
        for (int j = 0; j < messageNum; j++) {
            String key = "key" + j % maxKeys;
            String value = "my-message-" + key + j;
            producer.newMessage().key(key).value(value).send();
            //Duplicate keys will exist, the value of the new key will be retained
            expected.put(key, value);
        }
        // 3 Trigger compaction
        @Cleanup("shutdownNow")
        ScheduledExecutorService compactionScheduler = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder().setNameFormat("compactor").setDaemon(true).build());
        TwoPhaseCompactor twoPhaseCompactor = new TwoPhaseCompactor(config,
                pulsarClient, pulsar.getBookKeeperClient(), compactionScheduler);
        twoPhaseCompactor.compact(sourceTopic).get();

        // 4 Setup function
        // set source topic to null because we are setting the topic information separately
        FunctionConfig functionConfig = createFunctionConfig(tenant, namespacePortion, functionName,
                null, sinkTopic, subscriptionName);
        Map<String, ConsumerConfig> inputSpecs = new HashMap<>();
        ConsumerConfig consumerConfig = new ConsumerConfig();
        Map<String,String> consumerProperties = new HashMap<>();
        consumerProperties.put("readCompacted","true");
        consumerConfig.setConsumerProperties(consumerProperties);
        inputSpecs.put(sourceTopic, consumerConfig);
        functionConfig.setInputSpecs(inputSpecs);
        String jarFilePathUrl = getPulsarApiExamplesJar().toURI().toString();
        admin.functions().createFunctionWithUrl(functionConfig, jarFilePathUrl);

        // 5 Function should only read compacted value，so we will only receive compacted messages
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING).topic(sinkTopic).subscriptionName("sink-sub").subscribe();
        int count = 0;
        while (true) {
            Message<String> message = consumer.receive(10, TimeUnit.SECONDS);
            if (message == null) {
                break;
            }
            consumer.acknowledge(message);
            count++;
            Assert.assertEquals(expected.remove(message.getKey()) + "!", message.getValue());
        }
        Assert.assertEquals(count, maxKeys);
        Assert.assertTrue(expected.isEmpty());

        consumer.close();
        producer.close();
    }

    @Test(timeOut = 20000)
    public void testPulsarFunctionStats() throws Exception {

        final String namespacePortion = "io";
        final String replNamespace = tenant + "/" + namespacePortion;
        final String sourceTopic = "persistent://" + replNamespace + "/my-topic1";
        final String sinkTopic = "persistent://" + replNamespace + "/output";
        final String propertyKey = "key";
        final String propertyValue = "value";
        final String functionName = "PulsarSink-test";
        final String subscriptionName = "test-sub";
        admin.namespaces().createNamespace(replNamespace);
        Set<String> clusters = Sets.newHashSet(Lists.newArrayList("use"));
        admin.namespaces().setNamespaceReplicationClusters(replNamespace, clusters);

        // create a producer that creates a topic at broker
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(sourceTopic).create();

        String jarFilePathUrl = getPulsarApiExamplesJar().toURI().toString();
        FunctionConfig functionConfig = createFunctionConfig(tenant, namespacePortion, functionName,
                "my.*", sinkTopic, subscriptionName);
        admin.functions().createFunctionWithUrl(functionConfig, jarFilePathUrl);

        // try to update function to test: update-function functionality
        admin.functions().updateFunctionWithUrl(functionConfig, jarFilePathUrl);

        retryStrategically((test) -> {
            try {
                return admin.topics().getStats(sourceTopic).getSubscriptions().size() == 1;
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 50, 150);
        // validate pulsar sink consumer has started on the topic
        assertEquals(admin.topics().getStats(sourceTopic).getSubscriptions().size(), 1);

        // validate stats are empty
        FunctionRuntimeManager functionRuntimeManager = functionsWorkerService.getFunctionRuntimeManager();
        FunctionStatsImpl functionStats = functionRuntimeManager.getFunctionStats(tenant, namespacePortion,
                functionName, null);
        FunctionStatsImpl functionStatsFromAdmin = (FunctionStatsImpl) admin.functions().getFunctionStats(tenant,
                namespacePortion, functionName);

        assertEquals(functionStats, functionStatsFromAdmin);

        assertEquals(functionStats.getReceivedTotal(), 0);
        assertEquals(functionStats.getProcessedSuccessfullyTotal(), 0);
        assertEquals(functionStats.getSystemExceptionsTotal(), 0);
        assertEquals(functionStats.getUserExceptionsTotal(), 0);
        assertNull(functionStats.avgProcessLatency);
        assertEquals(functionStats.oneMin.getReceivedTotal(), 0);
        assertEquals(functionStats.oneMin.getProcessedSuccessfullyTotal(), 0);
        assertEquals(functionStats.oneMin.getSystemExceptionsTotal(), 0);
        assertEquals(functionStats.oneMin.getUserExceptionsTotal(), 0);
        assertNull(functionStats.oneMin.getAvgProcessLatency());
        assertEquals(functionStats.getAvgProcessLatency(), functionStats.oneMin.getAvgProcessLatency());
        assertNull(functionStats.getLastInvocation());

        assertEquals(functionStats.instances.size(), 1);
        assertEquals(functionStats.instances.get(0).getInstanceId(), 0);
        assertEquals(functionStats.instances.get(0).getMetrics().getReceivedTotal(), 0);
        assertEquals(functionStats.instances.get(0).getMetrics().getProcessedSuccessfullyTotal(), 0);
        assertEquals(functionStats.instances.get(0).getMetrics().getSystemExceptionsTotal(), 0);
        assertEquals(functionStats.instances.get(0).getMetrics().getUserExceptionsTotal(), 0);
        assertNull(functionStats.instances.get(0).getMetrics().getAvgProcessLatency());
        assertEquals(functionStats.instances.get(0).getMetrics().getOneMin().getReceivedTotal(), 0);
        assertEquals(functionStats.instances.get(0).getMetrics().getOneMin().getProcessedSuccessfullyTotal(), 0);
        assertEquals(functionStats.instances.get(0).getMetrics().getOneMin().getSystemExceptionsTotal(), 0);
        assertEquals(functionStats.instances.get(0).getMetrics().getOneMin().getUserExceptionsTotal(), 0);
        assertNull(functionStats.instances.get(0).getMetrics().getOneMin().getAvgProcessLatency());

        assertEquals(functionStats.instances.get(0).getMetrics().getAvgProcessLatency(),
                functionStats.instances.get(0).getMetrics().getOneMin().getAvgProcessLatency());
        assertEquals(functionStats.instances.get(0).getMetrics().getAvgProcessLatency(),
                functionStats.getAvgProcessLatency());

        // validate prometheus metrics empty
        String prometheusMetrics = PulsarFunctionTestUtils.getPrometheusMetrics(pulsar.getListenPortHTTP().get());
        log.info("prometheus metrics: {}", prometheusMetrics);

        Map<String, PulsarFunctionTestUtils.Metric> metrics = PulsarFunctionTestUtils.parseMetrics(prometheusMetrics);
        PulsarFunctionTestUtils.Metric m = metrics.get("pulsar_function_received_total");
        assertEquals(m.tags.get("cluster"), config.getClusterName());
        assertEquals(m.tags.get("instance_id"), "0");
        assertEquals(m.tags.get("name"), functionName);
        assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
        assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, functionName));
        assertEquals(m.value, 0.0);
        m = metrics.get("pulsar_function_received_total_1min");
        assertEquals(m.tags.get("cluster"), config.getClusterName());
        assertEquals(m.tags.get("instance_id"), "0");
        assertEquals(m.tags.get("name"), functionName);
        assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
        assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, functionName));
        assertEquals(m.value, 0.0);
        m = metrics.get("pulsar_function_user_exceptions_total");
        assertEquals(m.tags.get("cluster"), config.getClusterName());
        assertEquals(m.tags.get("instance_id"), "0");
        assertEquals(m.tags.get("name"), functionName);
        assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
        assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, functionName));
        assertEquals(m.value, 0.0);
        m = metrics.get("pulsar_function_user_exceptions_total_1min");
        assertEquals(m.tags.get("cluster"), config.getClusterName());
        assertEquals(m.tags.get("instance_id"), "0");
        assertEquals(m.tags.get("name"), functionName);
        assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
        assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, functionName));
        assertEquals(m.value, 0.0);
        m = metrics.get("pulsar_function_process_latency_ms");
        assertEquals(m.tags.get("cluster"), config.getClusterName());
        assertEquals(m.tags.get("instance_id"), "0");
        assertEquals(m.tags.get("name"), functionName);
        assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
        assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, functionName));
        assertEquals(m.value, Double.NaN);
        m = metrics.get("pulsar_function_process_latency_ms_1min");
        assertEquals(m.tags.get("cluster"), config.getClusterName());
        assertEquals(m.tags.get("instance_id"), "0");
        assertEquals(m.tags.get("name"), functionName);
        assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
        assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, functionName));
        assertEquals(m.value, Double.NaN);
        m = metrics.get("pulsar_function_system_exceptions_total");
        assertEquals(m.tags.get("cluster"), config.getClusterName());
        assertEquals(m.tags.get("instance_id"), "0");
        assertEquals(m.tags.get("name"), functionName);
        assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
        assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, functionName));
        assertEquals(m.value, 0.0);
        m = metrics.get("pulsar_function_system_exceptions_total_1min");
        assertEquals(m.tags.get("cluster"), config.getClusterName());
        assertEquals(m.tags.get("instance_id"), "0");
        assertEquals(m.tags.get("name"), functionName);
        assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
        assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, functionName));
        assertEquals(m.value, 0.0);
        m = metrics.get("pulsar_function_last_invocation");
        assertEquals(m.tags.get("cluster"), config.getClusterName());
        assertEquals(m.tags.get("instance_id"), "0");
        assertEquals(m.tags.get("name"), functionName);
        assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
        assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, functionName));
        assertEquals(m.value, 0.0);
        m = metrics.get("pulsar_function_processed_successfully_total");
        assertEquals(m.tags.get("cluster"), config.getClusterName());
        assertEquals(m.tags.get("instance_id"), "0");
        assertEquals(m.tags.get("name"), functionName);
        assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
        assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, functionName));
        assertEquals(m.value, 0.0);
        m = metrics.get("pulsar_function_processed_successfully_total_1min");
        assertEquals(m.tags.get("cluster"), config.getClusterName());
        assertEquals(m.tags.get("instance_id"), "0");
        assertEquals(m.tags.get("name"), functionName);
        assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
        assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, functionName));
        assertEquals(m.value, 0.0);


        // validate function instance stats empty
        FunctionInstanceStatsDataImpl functionInstanceStats = functionRuntimeManager.getFunctionInstanceStats(tenant,
                namespacePortion, functionName, 0,  null);

        FunctionInstanceStatsDataImpl functionInstanceStatsAdmin = (FunctionInstanceStatsDataImpl) admin.functions().
                getFunctionStats(tenant, namespacePortion, functionName, 0);

        assertEquals(functionInstanceStats, functionInstanceStatsAdmin);
        assertEquals(functionInstanceStats, functionStats.instances.get(0).getMetrics());


        int totalMsgs = 10;
        for (int i = 0; i < totalMsgs; i++) {
            String data = "my-message-" + i;
            producer.newMessage().property(propertyKey, propertyValue).value(data).send();
        }
        retryStrategically((test) -> {
            try {
                SubscriptionStats subStats = admin.topics().getStats(sourceTopic).getSubscriptions().get(subscriptionName);
                return subStats.getUnackedMessages() == 0 && subStats.getMsgThroughputOut() == totalMsgs;
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 5, 200);


        // get stats after producing
        functionStats = functionRuntimeManager.getFunctionStats(tenant, namespacePortion,
                functionName, null);

        functionStatsFromAdmin = (FunctionStatsImpl) admin.functions().getFunctionStats(tenant, namespacePortion,
                functionName);

        assertEquals(functionStats, functionStatsFromAdmin);

        assertEquals(functionStats.getReceivedTotal(), totalMsgs);
        assertEquals(functionStats.getProcessedSuccessfullyTotal(), totalMsgs);
        assertEquals(functionStats.getSystemExceptionsTotal(), 0);
        assertEquals(functionStats.getUserExceptionsTotal(), 0);
        assertTrue(functionStats.avgProcessLatency > 0);
        assertEquals(functionStats.oneMin.getReceivedTotal(), totalMsgs);
        assertEquals(functionStats.oneMin.getProcessedSuccessfullyTotal(), totalMsgs);
        assertEquals(functionStats.oneMin.getSystemExceptionsTotal(), 0);
        assertEquals(functionStats.oneMin.getUserExceptionsTotal(), 0);
        assertTrue(functionStats.oneMin.getAvgProcessLatency() > 0);
        assertEquals(functionStats.getAvgProcessLatency(), functionStats.oneMin.getAvgProcessLatency());
        assertTrue(functionStats.getLastInvocation() > 0);

        assertEquals(functionStats.instances.size(), 1);
        assertEquals(functionStats.instances.get(0).getInstanceId(), 0);
        assertEquals(functionStats.instances.get(0).getMetrics().getReceivedTotal(), totalMsgs);
        assertEquals(functionStats.instances.get(0).getMetrics().getProcessedSuccessfullyTotal(), totalMsgs);
        assertEquals(functionStats.instances.get(0).getMetrics().getSystemExceptionsTotal(), 0);
        assertEquals(functionStats.instances.get(0).getMetrics().getUserExceptionsTotal(), 0);
        assertTrue(functionStats.instances.get(0).getMetrics().getAvgProcessLatency() > 0);
        assertEquals(functionStats.instances.get(0).getMetrics().getOneMin().getReceivedTotal(), totalMsgs);
        assertEquals(functionStats.instances.get(0).getMetrics().getOneMin().getProcessedSuccessfullyTotal(), totalMsgs);
        assertEquals(functionStats.instances.get(0).getMetrics().getOneMin().getSystemExceptionsTotal(), 0);
        assertEquals(functionStats.instances.get(0).getMetrics().getOneMin().getUserExceptionsTotal(), 0);
        assertTrue(functionStats.instances.get(0).getMetrics().getOneMin().getAvgProcessLatency() > 0);

        assertEquals(functionStats.instances.get(0).getMetrics().getAvgProcessLatency(),
                functionStats.instances.get(0).getMetrics().getOneMin().getAvgProcessLatency());
        assertEquals(functionStats.instances.get(0).getMetrics().getAvgProcessLatency(),
                functionStats.getAvgProcessLatency());

        // validate function instance stats
        functionInstanceStats = functionRuntimeManager.getFunctionInstanceStats(tenant, namespacePortion,
                functionName, 0,  null);

        functionInstanceStatsAdmin = (FunctionInstanceStatsDataImpl) admin.functions().getFunctionStats(tenant,
                namespacePortion, functionName, 0);

        assertEquals(functionInstanceStats, functionInstanceStatsAdmin);
        assertEquals(functionInstanceStats, functionStats.instances.get(0).getMetrics());

        // validate prometheus metrics
        prometheusMetrics = PulsarFunctionTestUtils.getPrometheusMetrics(pulsar.getListenPortHTTP().get());
        log.info("prometheus metrics: {}", prometheusMetrics);

        metrics = PulsarFunctionTestUtils.parseMetrics(prometheusMetrics);
        m = metrics.get("pulsar_function_received_total");
        assertEquals(m.tags.get("cluster"), config.getClusterName());
        assertEquals(m.tags.get("instance_id"), "0");
        assertEquals(m.tags.get("name"), functionName);
        assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
        assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, functionName));
        assertEquals(m.value, (double) totalMsgs);
        m = metrics.get("pulsar_function_received_total_1min");
        assertEquals(m.tags.get("cluster"), config.getClusterName());
        assertEquals(m.tags.get("instance_id"), "0");
        assertEquals(m.tags.get("name"), functionName);
        assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
        assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, functionName));
        assertEquals(m.value, (double) totalMsgs);
        m = metrics.get("pulsar_function_user_exceptions_total");
        assertEquals(m.tags.get("cluster"), config.getClusterName());
        assertEquals(m.tags.get("instance_id"), "0");
        assertEquals(m.tags.get("name"), functionName);
        assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
        assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, functionName));
        assertEquals(m.value, 0.0);
        m = metrics.get("pulsar_function_user_exceptions_total_1min");
        assertEquals(m.tags.get("cluster"), config.getClusterName());
        assertEquals(m.tags.get("instance_id"), "0");
        assertEquals(m.tags.get("name"), functionName);
        assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
        assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, functionName));
        assertEquals(m.value, 0.0);
        m = metrics.get("pulsar_function_process_latency_ms");
        assertEquals(m.tags.get("cluster"), config.getClusterName());
        assertEquals(m.tags.get("instance_id"), "0");
        assertEquals(m.tags.get("name"), functionName);
        assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
        assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, functionName));
        assertTrue(m.value > 0.0);
        m = metrics.get("pulsar_function_process_latency_ms_1min");
        assertEquals(m.tags.get("cluster"), config.getClusterName());
        assertEquals(m.tags.get("instance_id"), "0");
        assertEquals(m.tags.get("name"), functionName);
        assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
        assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, functionName));
        assertTrue(m.value > 0.0);
        m = metrics.get("pulsar_function_system_exceptions_total");
        assertEquals(m.tags.get("cluster"), config.getClusterName());
        assertEquals(m.tags.get("instance_id"), "0");
        assertEquals(m.tags.get("name"), functionName);
        assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
        assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, functionName));
        assertEquals(m.value, 0.0);
        m = metrics.get("pulsar_function_system_exceptions_total_1min");
        assertEquals(m.tags.get("cluster"), config.getClusterName());
        assertEquals(m.tags.get("instance_id"), "0");
        assertEquals(m.tags.get("name"), functionName);
        assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
        assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, functionName));
        assertEquals(m.value, 0.0);
        m = metrics.get("pulsar_function_last_invocation");
        assertEquals(m.tags.get("cluster"), config.getClusterName());
        assertEquals(m.tags.get("instance_id"), "0");
        assertEquals(m.tags.get("name"), functionName);
        assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
        assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, functionName));
        assertTrue(m.value > 0.0);
        m = metrics.get("pulsar_function_processed_successfully_total");
        assertEquals(m.tags.get("cluster"), config.getClusterName());
        assertEquals(m.tags.get("instance_id"), "0");
        assertEquals(m.tags.get("name"), functionName);
        assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
        assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, functionName));
        assertEquals(m.value, (double) totalMsgs);
        m = metrics.get("pulsar_function_processed_successfully_total_1min");
        assertEquals(m.tags.get("cluster"), config.getClusterName());
        assertEquals(m.tags.get("instance_id"), "0");
        assertEquals(m.tags.get("name"), functionName);
        assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
        assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, functionName));
        assertEquals(m.value, (double) totalMsgs);

        // delete functions
        admin.functions().deleteFunction(tenant, namespacePortion, functionName);

        retryStrategically((test) -> {
            try {
                return admin.topics().getStats(sourceTopic).getSubscriptions().size() == 0;
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 50, 150);

        // make sure subscriptions are cleanup
        assertEquals(admin.topics().getStats(sourceTopic).getSubscriptions().size(), 0);

        tempDirectory.assertThatFunctionDownloadTempFilesHaveBeenDeleted();
    }

    @Test(timeOut = 20000)
    public void testPulsarFunctionStatus() throws Exception {

        final String namespacePortion = "io";
        final String replNamespace = tenant + "/" + namespacePortion;
        final String sourceTopic = "persistent://" + replNamespace + "/my-topic1";
        final String sinkTopic = "persistent://" + replNamespace + "/output";
        final String propertyKey = "key";
        final String propertyValue = "value";
        final String functionName = "PulsarSink-test";
        final String subscriptionName = "test-sub";
        admin.namespaces().createNamespace(replNamespace);
        Set<String> clusters = Sets.newHashSet(Lists.newArrayList("use"));
        admin.namespaces().setNamespaceReplicationClusters(replNamespace, clusters);

        // create a producer that creates a topic at broker
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(sourceTopic).create();

        String jarFilePathUrl = getPulsarApiExamplesJar().toURI().toString();
        FunctionConfig functionConfig = createFunctionConfig(tenant, namespacePortion, functionName,
                "my.*", sinkTopic, subscriptionName);
        admin.functions().createFunctionWithUrl(functionConfig, jarFilePathUrl);

        // try to update function to test: update-function functionality
        admin.functions().updateFunctionWithUrl(functionConfig, jarFilePathUrl);

        retryStrategically((test) -> {
            try {
                return admin.topics().getStats(sourceTopic).getSubscriptions().size() == 1;
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 50, 150);
        // validate pulsar sink consumer has started on the topic
        assertEquals(admin.topics().getStats(sourceTopic).getSubscriptions().size(), 1);

        int totalMsgs = 10;
        for (int i = 0; i < totalMsgs; i++) {
            String data = "my-message-" + i;
            producer.newMessage().property(propertyKey, propertyValue).value(data).send();
        }
        retryStrategically((test) -> {
            try {
                SubscriptionStats subStats = admin.topics().getStats(sourceTopic).getSubscriptions().get(subscriptionName);
                return subStats.getUnackedMessages() == 0 && subStats.getMsgThroughputOut() == totalMsgs;
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 5, 200);

        FunctionStatus functionStatus = admin.functions().getFunctionStatus(tenant, namespacePortion,
                functionName);

        int numInstances = functionStatus.getNumInstances();
        assertEquals(numInstances, 1);

        FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData status
                = functionStatus.getInstances().get(0).getStatus();

        double count = status.getNumReceived();
        double success = status.getNumSuccessfullyProcessed();
        String ownerWorkerId = status.getWorkerId();
        assertEquals((int)count, totalMsgs);
        assertEquals((int) success, totalMsgs);
        assertEquals(ownerWorkerId, workerId);

        // delete functions
        admin.functions().deleteFunction(tenant, namespacePortion, functionName);

        retryStrategically((test) -> {
            try {
                return admin.topics().getStats(sourceTopic).getSubscriptions().size() == 0;
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 50, 150);

        // make sure subscriptions are cleanup
        assertEquals(admin.topics().getStats(sourceTopic).getSubscriptions().size(), 0);
    }

    @Test(dataProvider = "validRoleName")
    public void testAuthorization(boolean validRoleName) throws Exception {
        final String namespacePortion = "io";
        final String replNamespace = tenant + "/" + namespacePortion;
        final String sinkTopic = "persistent://" + replNamespace + "/output";
        final String functionName = "PulsarSink-test";
        final String subscriptionName = "test-sub";
        admin.namespaces().createNamespace(replNamespace);
        Set<String> clusters = Sets.newHashSet(Lists.newArrayList("use"));
        admin.namespaces().setNamespaceReplicationClusters(replNamespace, clusters);

        String roleName = validRoleName ? "superUser" : "invalid";

        TenantInfo propAdmin = TenantInfo.builder()
                .adminRoles(Collections.singleton(roleName))
                .allowedClusters(Collections.singleton("use"))
                .build();
        admin.tenants().updateTenant(tenant, propAdmin);

        String jarFilePathUrl = getPulsarApiExamplesJar().toURI().toString();
        FunctionConfig functionConfig = createFunctionConfig(tenant, namespacePortion, functionName,
                "my.*", sinkTopic, subscriptionName);
        if (!validRoleName) {
            // create a non-superuser admin to test the api
            admin = spy(
                PulsarAdmin.builder().serviceHttpUrl(pulsar.getWebServiceAddressTls())
                    .tlsTrustCertsFilePath(TLS_TRUST_CERT_FILE_PATH)
                    .allowTlsInsecureConnection(true).build());
            try {
                admin.functions().createFunctionWithUrl(functionConfig, jarFilePathUrl);
            } catch (org.apache.pulsar.client.admin.PulsarAdminException.NotAuthorizedException ne) {
                assertFalse(validRoleName);
            }
        } else {
            try {
                admin.functions().createFunctionWithUrl(functionConfig, jarFilePathUrl);
                assertTrue(validRoleName);
            } catch (org.apache.pulsar.client.admin.PulsarAdminException.NotAuthorizedException ne) {
                fail();
            }
        }

    }

    @Test(timeOut = 20000)
    public void testFunctionStopAndRestartApi() throws Exception {

        final String namespacePortion = "io";
        final String replNamespace = tenant + "/" + namespacePortion;
        final String sourceTopicName = "restartFunction";
        final String sourceTopic = "persistent://" + replNamespace + "/" + sourceTopicName;
        final String sinkTopic = "persistent://" + replNamespace + "/output";
        final String functionName = "PulsarSink-test";
        final String subscriptionName = "test-sub";
        admin.namespaces().createNamespace(replNamespace);
        Set<String> clusters = Sets.newHashSet(Lists.newArrayList("use"));
        admin.namespaces().setNamespaceReplicationClusters(replNamespace, clusters);

        // create source topic
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(sourceTopic).create();

        String jarFilePathUrl = getPulsarApiExamplesJar().toURI().toString();
        FunctionConfig functionConfig = createFunctionConfig(tenant, namespacePortion, functionName,
                sourceTopicName, sinkTopic, subscriptionName);
        admin.functions().createFunctionWithUrl(functionConfig, jarFilePathUrl);

        retryStrategically((test) -> {
            try {
                SubscriptionStats subStats = admin.topics().getStats(sourceTopic).getSubscriptions().get(subscriptionName);
                return subStats != null && subStats.getConsumers().size() == 1;
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 50, 150);

        SubscriptionStats subStats = admin.topics().getStats(sourceTopic).getSubscriptions().get(subscriptionName);
        assertEquals(subStats.getConsumers().size(), 1);

        // it should stop consumer : so, check none of the consumer connected on subscription
        admin.functions().stopFunction(tenant, namespacePortion, functionName);

        retryStrategically((test) -> {
            try {
                SubscriptionStats subStat = admin.topics().getStats(sourceTopic).getSubscriptions().get(subscriptionName);
                return subStat != null && subStat.getConsumers().size() == 0;
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 50, 150);

        subStats = admin.topics().getStats(sourceTopic).getSubscriptions().get(subscriptionName);
        assertEquals(subStats.getConsumers().size(), 0);

        // it should restart consumer : so, check if consumer came up again after restarting function
        admin.functions().restartFunction(tenant, namespacePortion, functionName);

        retryStrategically((test) -> {
            try {
                SubscriptionStats subStat = admin.topics().getStats(sourceTopic).getSubscriptions().get(subscriptionName);
                return subStat != null && subStat.getConsumers().size() == 1;
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 50, 150);

        subStats = admin.topics().getStats(sourceTopic).getSubscriptions().get(subscriptionName);
        assertEquals(subStats.getConsumers().size(), 1);

        producer.close();
    }

    @Test(timeOut = 20000)
    public void testFunctionAutomaticSubCleanup() throws Exception {
        final String namespacePortion = "io";
        final String replNamespace = tenant + "/" + namespacePortion;
        final String sourceTopic = "persistent://" + replNamespace + "/my-topic1";
        final String sinkTopic = "persistent://" + replNamespace + "/output";
        final String propertyKey = "key";
        final String propertyValue = "value";
        final String functionName = "PulsarFunction-test";
        admin.namespaces().createNamespace(replNamespace);
        Set<String> clusters = Sets.newHashSet(Lists.newArrayList("use"));
        admin.namespaces().setNamespaceReplicationClusters(replNamespace, clusters);

        // create a producer that creates a topic at broker
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(sourceTopic).create();

        String jarFilePathUrl = getPulsarApiExamplesJar().toURI().toString();
        FunctionConfig functionConfig = new FunctionConfig();
        functionConfig.setTenant(tenant);
        functionConfig.setNamespace(namespacePortion);
        functionConfig.setName(functionName);
        functionConfig.setParallelism(1);
        functionConfig.setInputs(Collections.singleton(sourceTopic));
        functionConfig.setClassName("org.apache.pulsar.functions.api.examples.ExclamationFunction");
        functionConfig.setOutput(sinkTopic);
        functionConfig.setCleanupSubscription(false);
        functionConfig.setRuntime(FunctionConfig.Runtime.JAVA);

        admin.functions().createFunctionWithUrl(functionConfig, jarFilePathUrl);
        retryStrategically((test) -> {
            try {
                FunctionConfig configure = admin.functions().getFunction(tenant, namespacePortion, functionName);
                return configure != null && configure.getCleanupSubscription() != null;
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 50, 150);
        assertFalse(admin.functions().getFunction(tenant, namespacePortion, functionName).getCleanupSubscription());

        retryStrategically((test) -> {
            try {
                return admin.topics().getStats(sourceTopic).getSubscriptions().size() == 1;
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 50, 150);
        // validate pulsar source consumer has started on the topic
        assertEquals(admin.topics().getStats(sourceTopic).getSubscriptions().size(), 1);

        // test update cleanup subscription
        functionConfig.setCleanupSubscription(true);
        admin.functions().updateFunctionWithUrl(functionConfig, jarFilePathUrl);

        retryStrategically((test) -> {
            try {
                return admin.functions().getFunction(tenant, namespacePortion, functionName).getCleanupSubscription();
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 50, 150);
        assertTrue(admin.functions().getFunction(tenant, namespacePortion, functionName).getCleanupSubscription());

        int totalMsgs = 10;
        for (int i = 0; i < totalMsgs; i++) {
            String data = "my-message-" + i;
            producer.newMessage().property(propertyKey, propertyValue).value(data).send();
        }
        retryStrategically((test) -> {
            try {
                SubscriptionStats subStats = admin.topics().getStats(sourceTopic).getSubscriptions().get(
                        InstanceUtils.getDefaultSubscriptionName(tenant, namespacePortion, functionName));
                return subStats.getUnackedMessages() == 0 && subStats.getMsgThroughputOut() == totalMsgs;
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 5, 200);

        FunctionStatus functionStatus = admin.functions().getFunctionStatus(tenant, namespacePortion,
                functionName);

        int numInstances = functionStatus.getNumInstances();
        assertEquals(numInstances, 1);

        FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData status
                = functionStatus.getInstances().get(0).getStatus();

        double count = status.getNumReceived();
        double success = status.getNumSuccessfullyProcessed();
        String ownerWorkerId = status.getWorkerId();
        assertEquals((int)count, totalMsgs);
        assertEquals((int) success, totalMsgs);
        assertEquals(ownerWorkerId, workerId);

        // delete functions
        admin.functions().deleteFunction(tenant, namespacePortion, functionName);

        retryStrategically((test) -> {
            try {
                return admin.topics().getStats(sourceTopic).getSubscriptions().size() == 0;
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 50, 150);

        // make sure subscriptions are cleanup
        assertEquals(admin.topics().getStats(sourceTopic).getSubscriptions().size(), 0);


        /** test do not cleanup subscription **/
        functionConfig.setCleanupSubscription(false);
        admin.functions().createFunctionWithUrl(functionConfig, jarFilePathUrl);

        retryStrategically((test) -> {
            try {
                return admin.topics().getStats(sourceTopic).getSubscriptions().size() == 1;
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 50, 150);
        // validate pulsar source consumer has started on the topic
        assertEquals(admin.topics().getStats(sourceTopic).getSubscriptions().size(), 1);

        retryStrategically((test) -> {
            try {
                FunctionConfig result = admin.functions().getFunction(tenant, namespacePortion, functionName);
                return !result.getCleanupSubscription();
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 50, 150);
        assertFalse(admin.functions().getFunction(tenant, namespacePortion, functionName).getCleanupSubscription());

        // test update another config and making sure that subscription cleanup remains unchanged
        functionConfig.setParallelism(2);
        admin.functions().updateFunctionWithUrl(functionConfig, jarFilePathUrl);

        retryStrategically((test) -> {
            try {
                FunctionConfig result = admin.functions().getFunction(tenant, namespacePortion, functionName);
                return result.getParallelism() == 2 && !result.getCleanupSubscription();
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 50, 150);
        assertFalse(admin.functions().getFunction(tenant, namespacePortion, functionName).getCleanupSubscription());

        // delete functions
        admin.functions().deleteFunction(tenant, namespacePortion, functionName);

        retryStrategically((test) -> {
            try {
                return admin.topics().getStats(sourceTopic).getSubscriptions().size() == 1;
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 50, 150);

        // make sure subscriptions are cleanup
        assertEquals(admin.topics().getStats(sourceTopic).getSubscriptions().size(), 1);
    }

    @Test(timeOut = 20000)
    public void testMultiTopicFunction() throws Exception {
        final String namespacePortion = "io";
        final String replNamespace = tenant + "/" + namespacePortion;
        final String sourceTopic1 = "persistent://" + replNamespace + "/my-topic1";
        final String sourceTopic2 = "persistent://" + replNamespace + "/my-topic2";
        final String sinkTopic = "persistent://" + replNamespace + "/output";
        final String propertyKey = "key";
        final String propertyValue = "value";
        final String functionName = "PulsarFunction-test";
        admin.namespaces().createNamespace(replNamespace);
        Set<String> clusters = Sets.newHashSet(Lists.newArrayList("use"));
        admin.namespaces().setNamespaceReplicationClusters(replNamespace, clusters);

        // create a producer that creates a topic at broker
        @Cleanup
        Producer<String> producer1 = pulsarClient.newProducer(Schema.STRING).topic(sourceTopic1).create();
        @Cleanup
        Producer<String> producer2 = pulsarClient.newProducer(Schema.STRING).topic(sourceTopic2).create();

        String jarFilePathUrl = getPulsarApiExamplesJar().toURI().toString();
        FunctionConfig functionConfig = new FunctionConfig();
        functionConfig.setTenant(tenant);
        functionConfig.setNamespace(namespacePortion);
        functionConfig.setName(functionName);
        functionConfig.setParallelism(1);
        List<String> topics = new LinkedList<>();
        topics.add(sourceTopic1);
        topics.add(sourceTopic2);
        functionConfig.setInputs(topics);
        functionConfig.setClassName("org.apache.pulsar.functions.api.examples.ExclamationFunction");
        functionConfig.setOutput(sinkTopic);
        functionConfig.setRuntime(FunctionConfig.Runtime.JAVA);

        admin.functions().createFunctionWithUrl(functionConfig, jarFilePathUrl);
        assertTrue(retryStrategically((test) -> {
            try {
                admin.functions().getFunction(tenant, namespacePortion, functionName);
                return true;
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 50, 150));

        assertTrue(retryStrategically((test) -> {
            try {
                return admin.topics().getStats(sourceTopic1).getSubscriptions().size() == 1;
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 50, 150));

        assertTrue(retryStrategically((test) -> {
            try {
                return admin.topics().getStats(sourceTopic2).getSubscriptions().size() == 1;
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 50, 150));

        int totalMsgs = 10;
        for (int i = 0; i < totalMsgs; i++) {
            String data = "my-message-" + i;
            producer1.newMessage().property(propertyKey, propertyValue).value(data).send();
            producer2.newMessage().property(propertyKey, propertyValue).value(data).send();
        }

        assertTrue(retryStrategically((test) -> {
            try {
                SubscriptionStats subStats = admin.topics().getStats(sourceTopic1).getSubscriptions().get(
                        InstanceUtils.getDefaultSubscriptionName(tenant, namespacePortion, functionName));
                return subStats.getUnackedMessages() == 0 && subStats.getMsgThroughputOut() == totalMsgs;
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 5, 200));

        assertTrue(retryStrategically((test) -> {
            try {
                SubscriptionStats subStats = admin.topics().getStats(sourceTopic2).getSubscriptions().get(
                        InstanceUtils.getDefaultSubscriptionName(tenant, namespacePortion, functionName));
                return subStats.getUnackedMessages() == 0 && subStats.getMsgThroughputOut() == totalMsgs;
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 5, 200));

        FunctionStatus functionStatus = admin.functions().getFunctionStatus(tenant, namespacePortion,
                functionName);

        int numInstances = functionStatus.getNumInstances();
        assertEquals(numInstances, 1);

        FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData status
                = functionStatus.getInstances().get(0).getStatus();

        double count = status.getNumReceived();
        double success = status.getNumSuccessfullyProcessed();
        String ownerWorkerId = status.getWorkerId();
        // multiply by 2 since function is reading from two topics
        assertEquals((int)count, totalMsgs * 2);
        assertEquals((int) success, totalMsgs * 2);
        assertEquals(ownerWorkerId, workerId);

        // delete functions
        admin.functions().deleteFunction(tenant, namespacePortion, functionName);

        assertTrue(retryStrategically((test) -> {
            try {
                return admin.topics().getStats(sourceTopic1).getSubscriptions().size() == 0;
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 50, 150));

        assertTrue(retryStrategically((test) -> {
            try {
                return admin.topics().getStats(sourceTopic2).getSubscriptions().size() == 0;
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 50, 150));
    }

    @Test(timeOut = 20000)
    public void testE2EPulsarFunctionMessagePooled() throws Exception {
        final String namespacePortion = "io";
        final String replNamespace = tenant + "/" + namespacePortion;
        final String sourceTopic = "persistent://" + replNamespace + "/my-topic1";
        final String sinkTopic = "persistent://" + replNamespace + "/output";
        final String propertyKey = "key";
        final String propertyValue = "value";
        final String functionName = "PulsarFunction-test";
        final String subscriptionName = "test-sub";
        admin.namespaces().createNamespace(replNamespace);
        Set<String> clusters = Sets.newHashSet(Lists.newArrayList("use"));
        admin.namespaces().setNamespaceReplicationClusters(replNamespace, clusters);

        // create a producer that creates a topic at broker
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(sourceTopic).create();
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING).topic(sinkTopic).subscriptionName("sub").subscribe();

        FunctionConfig functionConfig = new FunctionConfig();
        functionConfig.setTenant(tenant);
        functionConfig.setNamespace(namespacePortion);
        functionConfig.setName(functionName);
        functionConfig.setParallelism(1);
        functionConfig.setSubName(subscriptionName);
        functionConfig.setInputSpecs(Collections.singletonMap(sourceTopic,
                ConsumerConfig.builder().poolMessages(true).build()));
        functionConfig.setAutoAck(true);
        functionConfig.setClassName(ByteBufferFunction.class.getName());
        functionConfig.setRuntime(FunctionConfig.Runtime.JAVA);
        functionConfig.setOutput(sinkTopic);
        functionConfig.setCleanupSubscription(true);
        functionConfig.setProcessingGuarantees(FunctionConfig.ProcessingGuarantees.ATLEAST_ONCE);

        admin.functions().createFunctionWithUrl(functionConfig,
                PulsarFunctionE2ETest.class.getProtectionDomain().getCodeSource().getLocation().toURI().toString());

        Awaitility.await().ignoreExceptions().untilAsserted(() -> {
            TopicStats topicStats = admin.topics().getStats(sinkTopic);
            assertEquals(topicStats.getPublishers().size(), 1);
            assertNotNull(topicStats.getPublishers().get(0).getMetadata());
            assertTrue(topicStats.getPublishers().get(0).getMetadata().containsKey("id"));
            assertEquals(topicStats.getPublishers().get(0).getMetadata().get("id"),
                    String.format("%s/%s/%s", tenant, namespacePortion, functionName));
        });

        Awaitility.await().ignoreExceptions().untilAsserted(() -> {
            // validate pulsar sink consumer has started on the topic
            assertEquals(admin.topics().getStats(sourceTopic).getSubscriptions().size(), 1);
        });

        int totalMsgs = 5;
        for (int i = 0; i < totalMsgs; i++) {
            String data = "my-message-" + i;
            producer.newMessage().property(propertyKey, propertyValue).value(data).send();
        }

        Awaitility.await().ignoreExceptions().untilAsserted(() -> {
            SubscriptionStats subStats = admin.topics().getStats(sourceTopic).getSubscriptions().get(subscriptionName);
            assertEquals(subStats.getUnackedMessages(), 0);
        });

        Message<String> msg = consumer.receive(5, TimeUnit.SECONDS);
        if (msg == null) {
            fail("Should have gotten a message");
        }
        String receivedPropertyValue = msg.getProperty(propertyKey);
        assertEquals(propertyValue, receivedPropertyValue);

        // validate pulsar-sink consumer has consumed all messages and delivered to Pulsar sink but unacked messages
        // due to publish failure
        assertNotEquals(admin.topics().getStats(sourceTopic).getSubscriptions().values().iterator().next().getUnackedMessages(),
                totalMsgs);

        // delete functions
        admin.functions().deleteFunction(tenant, namespacePortion, functionName);

        Awaitility.await().ignoreExceptions().untilAsserted(() -> {
            // make sure subscriptions are cleanup
            assertEquals(admin.topics().getStats(sourceTopic).getSubscriptions().size(), 0);
        });

        tempDirectory.assertThatFunctionDownloadTempFilesHaveBeenDeleted();
    }

    public static class ByteBufferFunction implements org.apache.pulsar.functions.api.Function<ByteBuffer, ByteBuffer> {
        @Override
        public ByteBuffer process(ByteBuffer input, Context context) throws Exception {
            // make sure we are using pooled memory
            assertTrue(input.isDirect());
            return input;
        }
    }
}
