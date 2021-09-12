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
import static org.apache.pulsar.functions.worker.PulsarFunctionLocalRunTest.getPulsarIODataGeneratorNar;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.auth.AuthenticationTls;
import org.apache.pulsar.common.functions.ConsumerConfig;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.functions.Utils;
import org.apache.pulsar.common.io.SinkConfig;
import org.apache.pulsar.common.policies.data.SinkStatus;
import org.apache.pulsar.common.policies.data.SubscriptionStats;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.compaction.TwoPhaseCompactor;
import org.apache.pulsar.functions.LocalRunner;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.functions.utils.FunctionCommon;
import org.apache.pulsar.functions.worker.PulsarFunctionTestUtils;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import lombok.Cleanup;

@Test(groups = "broker-io")
public class PulsarSinkE2ETest extends AbstractPulsarE2ETest {

    @Test
    public void testReadCompactedSink() throws Exception {
        final String namespacePortion = "io";
        final String replNamespace = tenant + "/" + namespacePortion;
        final String sourceTopic = "persistent://" + replNamespace + "/my-topic2";
        final String sinkName = "PulsarFunction-test";
        final String subscriptionName = "test-sub";
        admin.namespaces().createNamespace(replNamespace);
        Set<String> clusters = Sets.newHashSet(Lists.newArrayList("use"));
        admin.namespaces().setNamespaceReplicationClusters(replNamespace, clusters);
        final int messageNum = 20;
        final int maxKeys = 10;
        // 1 Setup producer
        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(sourceTopic)
                .enableBatching(false)
                .messageRoutingMode(MessageRoutingMode.SinglePartition)
                .create();
        pulsarClient.newConsumer()
                .topic(sourceTopic)
                .subscriptionName(subscriptionName)
                .readCompacted(true)
                .subscribe()
                .close();

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

        // 4 Setup sink
        SinkConfig sinkConfig = createSinkConfig(tenant, namespacePortion, sinkName, sourceTopic, subscriptionName);
        sinkConfig.setProcessingGuarantees(FunctionConfig.ProcessingGuarantees.EFFECTIVELY_ONCE);
        Map<String,String> consumerProperties = new HashMap<>();
        consumerProperties.put("readCompacted","true");
        sinkConfig.setInputSpecs(Collections.singletonMap(sourceTopic, ConsumerConfig.builder().consumerProperties(consumerProperties).build()));
        String jarFilePathUrl = getPulsarIODataGeneratorNar().toURI().toString();
        admin.sink().createSinkWithUrl(sinkConfig, jarFilePathUrl);

        // 5 Sink should only read compacted value，so we will only receive compacted messages
        Awaitility.await().ignoreExceptions().untilAsserted(() -> {
            String prometheusMetrics = PulsarFunctionTestUtils.getPrometheusMetrics(pulsar.getListenPortHTTP().get());
            Map<String, PulsarFunctionTestUtils.Metric> metrics = PulsarFunctionTestUtils.parseMetrics(prometheusMetrics);
            PulsarFunctionTestUtils.Metric m = metrics.get("pulsar_sink_received_total");
            assertEquals(m.value, maxKeys);
        });
    }

    @Test(timeOut = 30000)
    public void testPulsarSinkDLQ() throws Exception {
        final String namespacePortion = "io";
        final String replNamespace = tenant + "/" + namespacePortion;
        final String sourceTopic = "persistent://" + replNamespace + "/input";
        final String dlqTopic = sourceTopic+"-DLQ";
        final String sinkName = "PulsarSink-test";
        final String propertyKey = "key";
        final String propertyValue = "value";
        final String subscriptionName = "test-sub";
        admin.namespaces().createNamespace(replNamespace);
        Set<String> clusters = Sets.newHashSet(Lists.newArrayList("use"));
        admin.namespaces().setNamespaceReplicationClusters(replNamespace, clusters);
        // 1 create producer、DLQ consumer
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(sourceTopic).create();
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING).topic(dlqTopic).subscriptionName(subscriptionName).subscribe();

        // 2 setup sink
        SinkConfig sinkConfig = createSinkConfig(tenant, namespacePortion, sinkName, sourceTopic, subscriptionName);
        sinkConfig.setNegativeAckRedeliveryDelayMs(1001L);
        sinkConfig.setProcessingGuarantees(FunctionConfig.ProcessingGuarantees.ATLEAST_ONCE);
        sinkConfig.setMaxMessageRetries(2);
        sinkConfig.setDeadLetterTopic(dlqTopic);
        sinkConfig.setInputSpecs(Collections.singletonMap(sourceTopic, ConsumerConfig.builder().receiverQueueSize(1000).build()));
        sinkConfig.setClassName(SinkForTest.class.getName());
        @Cleanup
        LocalRunner localRunner = LocalRunner.builder()
                .sinkConfig(sinkConfig)
                .clientAuthPlugin(AuthenticationTls.class.getName())
                .clientAuthParams(String.format("tlsCertFile:%s,tlsKeyFile:%s", TLS_CLIENT_CERT_FILE_PATH, TLS_CLIENT_KEY_FILE_PATH))
                .useTls(true)
                .tlsTrustCertFilePath(TLS_TRUST_CERT_FILE_PATH)
                .tlsAllowInsecureConnection(true)
                .tlsHostNameVerificationEnabled(false)
                .brokerServiceUrl(pulsar.getBrokerServiceUrlTls()).build();

        localRunner.start(false);

        retryStrategically((test) -> {
            try {
                TopicStats topicStats = admin.topics().getStats(sourceTopic);

                return topicStats.getSubscriptions().containsKey(subscriptionName)
                        && topicStats.getSubscriptions().get(subscriptionName).getConsumers().size() == 1
                        && topicStats.getSubscriptions().get(subscriptionName).getConsumers().get(0).getAvailablePermits() == 1000;

            } catch (PulsarAdminException e) {
                return false;
            }
        }, 50, 150);

        // 3 send message
        int totalMsgs = 10;
        Set<String> remainingMessagesToReceive = new HashSet<>();
        for (int i = 0; i < totalMsgs; i++) {
            String messageBody = "fail" + i;
            producer.newMessage().property(propertyKey, propertyValue).value(messageBody).send();
            remainingMessagesToReceive.add(messageBody);
        }

        //4 All messages should enter DLQ
        for (int i = 0; i < totalMsgs; i++) {
            Message<String> message = consumer.receive(10, TimeUnit.SECONDS);
            assertNotNull(message);
            remainingMessagesToReceive.remove(message.getValue());
        }

        assertEquals(remainingMessagesToReceive, Collections.emptySet());

        //clean up
        producer.close();
        consumer.close();
    }

    private void testPulsarSinkStats(String jarFilePathUrl) throws Exception {
        testPulsarSinkStats(jarFilePathUrl, null);
    }

    private void testPulsarSinkStats(String jarFilePathUrl, Function<SinkConfig, SinkConfig> override) throws Exception {
        final String namespacePortion = "io";
        final String replNamespace = tenant + "/" + namespacePortion;
        final String sourceTopic = "persistent://" + replNamespace + "/input";
        final String sinkName = "PulsarSink-test";
        final String propertyKey = "key";
        final String propertyValue = "value";
        final String subscriptionName = "test-sub";
        admin.namespaces().createNamespace(replNamespace);
        Set<String> clusters = Sets.newHashSet(Lists.newArrayList("use"));
        admin.namespaces().setNamespaceReplicationClusters(replNamespace, clusters);

        // create a producer that creates a topic at broker
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(sourceTopic).create();

        SinkConfig sinkConfig = createSinkConfig(tenant, namespacePortion, sinkName, sourceTopic, subscriptionName);
        sinkConfig.setInputSpecs(Collections.singletonMap(sourceTopic, ConsumerConfig.builder().receiverQueueSize(1000).build()));
        if (override != null) {
            sinkConfig = override.apply(sinkConfig);
        }


        if (jarFilePathUrl.startsWith(Utils.BUILTIN)) {
            sinkConfig.setArchive(jarFilePathUrl);
            admin.sinks().createSink(sinkConfig, null);
        } else {
            admin.sinks().createSinkWithUrl(sinkConfig, jarFilePathUrl);
        }

        sinkConfig.setInputSpecs(Collections.singletonMap(sourceTopic, ConsumerConfig.builder().receiverQueueSize(523).build()));
        if (override != null) {
            sinkConfig = override.apply(sinkConfig);
        }

        if (jarFilePathUrl.startsWith(Utils.BUILTIN)) {
            sinkConfig.setArchive(jarFilePathUrl);
            admin.sinks().updateSink(sinkConfig, null);
        } else {
            admin.sinks().updateSinkWithUrl(sinkConfig, jarFilePathUrl);
        }

        retryStrategically((test) -> {
            try {
                TopicStats topicStats = admin.topics().getStats(sourceTopic);

                return topicStats.getSubscriptions().containsKey(subscriptionName)
                        && topicStats.getSubscriptions().get(subscriptionName).getConsumers().size() == 1
                        && topicStats.getSubscriptions().get(subscriptionName).getConsumers().get(0).getAvailablePermits() == 523;

            } catch (PulsarAdminException e) {
                return false;
            }
        }, 50, 150);

        TopicStats topicStats = admin.topics().getStats(sourceTopic);
        assertEquals(topicStats.getSubscriptions().size(), 1);
        assertTrue(topicStats.getSubscriptions().containsKey(subscriptionName));
        assertEquals(topicStats.getSubscriptions().get(subscriptionName).getConsumers().size(), 1);
        assertEquals(topicStats.getSubscriptions().get(subscriptionName).getConsumers().get(0).getAvailablePermits(), 523);

        // make sure there are no errors in the sink
        SinkStatus status = admin.sinks().getSinkStatus(tenant, namespacePortion, sinkName);
        status.getInstances().forEach(sinkInstanceStatus -> assertEquals(sinkInstanceStatus.status.numSinkExceptions, 0));
        status.getInstances().forEach(sinkInstanceStatus -> assertEquals(sinkInstanceStatus.status.numSystemExceptions, 0));

        // validate prometheus metrics empty
        String prometheusMetrics = PulsarFunctionTestUtils.getPrometheusMetrics(pulsar.getListenPortHTTP().get());
        log.info("prometheus metrics: {}", prometheusMetrics);

        Map<String, PulsarFunctionTestUtils.Metric> metrics = PulsarFunctionTestUtils.parseMetrics(prometheusMetrics);
        PulsarFunctionTestUtils.Metric m = metrics.get("pulsar_sink_received_total");
        assertEquals(m.tags.get("cluster"), config.getClusterName());
        assertEquals(m.tags.get("instance_id"), "0");
        assertEquals(m.tags.get("name"), sinkName);
        assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
        assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, sinkName));
        assertEquals(m.value, 0.0);
        m = metrics.get("pulsar_sink_received_total_1min");
        assertEquals(m.tags.get("cluster"), config.getClusterName());
        assertEquals(m.tags.get("instance_id"), "0");
        assertEquals(m.tags.get("name"), sinkName);
        assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
        assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, sinkName));
        assertEquals(m.value, 0.0);
        m = metrics.get("pulsar_sink_written_total");
        assertEquals(m.tags.get("cluster"), config.getClusterName());
        assertEquals(m.tags.get("instance_id"), "0");
        assertEquals(m.tags.get("name"), sinkName);
        assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
        assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, sinkName));
        assertEquals(m.value, 0.0);
        m = metrics.get("pulsar_sink_written_total_1min");
        assertEquals(m.tags.get("cluster"), config.getClusterName());
        assertEquals(m.tags.get("instance_id"), "0");
        assertEquals(m.tags.get("name"), sinkName);
        assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
        assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, sinkName));
        assertEquals(m.value, 0.0);
        m = metrics.get("pulsar_sink_sink_exceptions_total");
        assertEquals(m.tags.get("cluster"), config.getClusterName());
        assertEquals(m.tags.get("instance_id"), "0");
        assertEquals(m.tags.get("name"), sinkName);
        assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
        assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, sinkName));
        assertEquals(m.value, 0.0);
        m = metrics.get("pulsar_sink_sink_exceptions_total_1min");
        assertEquals(m.tags.get("cluster"), config.getClusterName());
        assertEquals(m.tags.get("instance_id"), "0");
        assertEquals(m.tags.get("name"), sinkName);
        assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
        assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, sinkName));
        assertEquals(m.value, 0.0);
        m = metrics.get("pulsar_sink_system_exceptions_total");
        assertEquals(m.tags.get("cluster"), config.getClusterName());
        assertEquals(m.tags.get("instance_id"), "0");
        assertEquals(m.tags.get("name"), sinkName);
        assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
        assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, sinkName));
        assertEquals(m.value, 0.0);
        m = metrics.get("pulsar_sink_system_exceptions_total_1min");
        assertEquals(m.tags.get("cluster"), config.getClusterName());
        assertEquals(m.tags.get("instance_id"), "0");
        assertEquals(m.tags.get("name"), sinkName);
        assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
        assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, sinkName));
        assertEquals(m.value, 0.0);
        m = metrics.get("pulsar_sink_last_invocation");
        assertEquals(m.tags.get("cluster"), config.getClusterName());
        assertEquals(m.tags.get("instance_id"), "0");
        assertEquals(m.tags.get("name"), sinkName);
        assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
        assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, sinkName));
        assertEquals(m.value, 0.0);

        int totalMsgs = 10;
        for (int i = 0; i < totalMsgs; i++) {
            String data = "my-message-" + i;
            producer.newMessage().property(propertyKey, propertyValue).value(data).send();
        }
        retryStrategically((test) -> {
            try {
                SubscriptionStats subStats = admin.topics().getStats(sourceTopic).getSubscriptions().get(subscriptionName);
                return subStats.getUnackedMessages() == 0 && subStats.getMsgOutCounter() == totalMsgs;
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 5, 200);

        SubscriptionStats subStats = admin.topics().getStats(sourceTopic).getSubscriptions().get(subscriptionName);
        assertEquals(subStats.getUnackedMessages(), 0);
        assertEquals(subStats.getMsgOutCounter(), totalMsgs);
        assertEquals(subStats.getMsgBacklog(), 0);

        // make sure there are no errors in the sink after producing
        status = admin.sinks().getSinkStatus(tenant, namespacePortion, sinkName);
        status.getInstances().forEach(sinkInstanceStatus -> assertEquals(sinkInstanceStatus.status.numSinkExceptions, 0));
        status.getInstances().forEach(sinkInstanceStatus -> assertEquals(sinkInstanceStatus.status.numSystemExceptions, 0));

        // get stats after producing
        prometheusMetrics = PulsarFunctionTestUtils.getPrometheusMetrics(pulsar.getListenPortHTTP().get());
        log.info("prometheusMetrics: {}", prometheusMetrics);

        metrics = PulsarFunctionTestUtils.parseMetrics(prometheusMetrics);
        m = metrics.get("pulsar_sink_received_total");
        assertEquals(m.tags.get("cluster"), config.getClusterName());
        assertEquals(m.tags.get("instance_id"), "0");
        assertEquals(m.tags.get("name"), sinkName);
        assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
        assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, sinkName));
        assertEquals(m.value, (double) totalMsgs);
        m = metrics.get("pulsar_sink_received_total_1min");
        assertEquals(m.tags.get("cluster"), config.getClusterName());
        assertEquals(m.tags.get("instance_id"), "0");
        assertEquals(m.tags.get("name"), sinkName);
        assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
        assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, sinkName));
        assertEquals(m.value, (double) totalMsgs);
        m = metrics.get("pulsar_sink_written_total");
        assertEquals(m.tags.get("cluster"), config.getClusterName());
        assertEquals(m.tags.get("instance_id"), "0");
        assertEquals(m.tags.get("name"), sinkName);
        assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
        assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, sinkName));
        assertEquals(m.value, (double) totalMsgs);
        m = metrics.get("pulsar_sink_written_total_1min");
        assertEquals(m.tags.get("cluster"), config.getClusterName());
        assertEquals(m.tags.get("instance_id"), "0");
        assertEquals(m.tags.get("name"), sinkName);
        assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
        assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, sinkName));
        assertEquals(m.value, (double) totalMsgs);
        m = metrics.get("pulsar_sink_sink_exceptions_total");
        assertEquals(m.tags.get("cluster"), config.getClusterName());
        assertEquals(m.tags.get("instance_id"), "0");
        assertEquals(m.tags.get("name"), sinkName);
        assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
        assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, sinkName));
        assertEquals(m.value, 0.0);
        m = metrics.get("pulsar_sink_sink_exceptions_total_1min");
        assertEquals(m.tags.get("cluster"), config.getClusterName());
        assertEquals(m.tags.get("instance_id"), "0");
        assertEquals(m.tags.get("name"), sinkName);
        assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
        assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, sinkName));
        assertEquals(m.value, 0.0);
        m = metrics.get("pulsar_sink_system_exceptions_total");
        assertEquals(m.tags.get("cluster"), config.getClusterName());
        assertEquals(m.tags.get("instance_id"), "0");
        assertEquals(m.tags.get("name"), sinkName);
        assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
        assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, sinkName));
        assertEquals(m.value, 0.0);
        m = metrics.get("pulsar_sink_system_exceptions_total_1min");
        assertEquals(m.tags.get("cluster"), config.getClusterName());
        assertEquals(m.tags.get("instance_id"), "0");
        assertEquals(m.tags.get("name"), sinkName);
        assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
        assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, sinkName));
        assertEquals(m.value, 0.0);
        m = metrics.get("pulsar_sink_last_invocation");
        assertEquals(m.tags.get("cluster"), config.getClusterName());
        assertEquals(m.tags.get("instance_id"), "0");
        assertEquals(m.tags.get("name"), sinkName);
        assertEquals(m.tags.get("namespace"), String.format("%s/%s", tenant, namespacePortion));
        assertEquals(m.tags.get("fqfn"), FunctionCommon.getFullyQualifiedName(tenant, namespacePortion, sinkName));
        assertTrue(m.value > 0.0);


        // delete functions
        admin.sinks().deleteSink(tenant, namespacePortion, sinkName);

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

    @Test(timeOut = 20000, groups = "builtin")
    public void testPulsarSinkStatsBuiltin() throws Exception {
        String jarFilePathUrl = String.format("%s://data-generator", Utils.BUILTIN);
        testPulsarSinkStats(jarFilePathUrl);
    }

    @Test(timeOut = 20000, groups = "builtin", expectedExceptions = {PulsarAdminException.class}, expectedExceptionsMessageRegExp = "Built-in sink is not available")
    public void testPulsarSinkStatsBuiltinDoesNotExist() throws Exception {
        String jarFilePathUrl = String.format("%s://foo", Utils.BUILTIN);
        testPulsarSinkStats(jarFilePathUrl);
    }

    @Test(timeOut = 20000)
    public void testPulsarSinkStatsWithFile() throws Exception {
        String jarFilePathUrl = getPulsarIODataGeneratorNar().toURI().toString();
        testPulsarSinkStats(jarFilePathUrl);
    }

    @Test(timeOut = 40000)
    public void testPulsarSinkStatsWithUrl() throws Exception {
        testPulsarSinkStats(fileServer.getUrl("/pulsar-io-data-generator.nar"));
    }

    @Test(timeOut = 20000)
    public void testPulsarSinkPoolMessages() throws Exception {
        String jarFilePathUrl = PulsarSinkE2ETest.class.getProtectionDomain().getCodeSource().getLocation().toURI().toString();
        testPulsarSinkStats(jarFilePathUrl, sinkConfig -> {
            sinkConfig.setClassName(ByteBufferSink.class.getName());
            sinkConfig.getInputSpecs().values().forEach(consumerConfig -> consumerConfig.setPoolMessages(true));
            return sinkConfig;
        });
    }

    public static class ByteBufferSink implements Sink<ByteBuffer> {

        @Override
        public void open(Map map, final SinkContext sinkContext) throws Exception {

        }

        @Override
        public void write(Record<ByteBuffer> record) throws Exception {
            assertTrue(record.getValue().isDirect());
            record.ack();
        }

        @Override
        public void close() throws Exception {

        }
    }

    private static SinkConfig createSinkConfig(String tenant, String namespace, String functionName, String sourceTopic, String subName) {
        SinkConfig sinkConfig = new SinkConfig();
        sinkConfig.setTenant(tenant);
        sinkConfig.setNamespace(namespace);
        sinkConfig.setName(functionName);
        sinkConfig.setParallelism(1);
        sinkConfig.setProcessingGuarantees(FunctionConfig.ProcessingGuarantees.ATLEAST_ONCE);
        sinkConfig.setInputSpecs(Collections.singletonMap(sourceTopic, ConsumerConfig.builder().build()));
        sinkConfig.setSourceSubscriptionName(subName);
        sinkConfig.setCleanupSubscription(true);
        return sinkConfig;
    }

}
