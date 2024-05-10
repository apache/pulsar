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
package org.apache.pulsar.broker.stats;

import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;

import org.apache.pulsar.broker.service.BrokerTestBase;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.ServerCnx;
import org.apache.pulsar.broker.stats.prometheus.PrometheusMetricsClient;
import org.apache.pulsar.broker.stats.prometheus.PrometheusMetricsGenerator;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.api.proto.BaseCommand;
import org.apache.pulsar.common.api.proto.CommandPartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.protocol.Commands;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@Test(groups = "broker")
public class ServerCnxStatsTest extends BrokerTestBase {

    @BeforeMethod(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        super.baseSetup();
        String tenant = "my-tenant";
        admin.tenants().createTenant(tenant, TenantInfo.builder().allowedClusters(Sets.newHashSet("test")).build());
        String namespace = "my-tenant/my-ns";
        admin.namespaces().createNamespace(namespace);
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }


    @Test
    public void testPartitionMetadataRequestMetrics() throws Exception {
        String topic = "persistent://my-tenant/my-ns/my-topic_" + UUID.randomUUID();
        admin.topics().createPartitionedTopic(topic, 3);
        CountDownLatch latch = new CountDownLatch(100);

        try (Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topic)
                .create();
             Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                     .topic(topic)
                     .subscriptionName("my-sub")
                     .messageListener((c, m) -> {
                         try {
                             c.acknowledge(m);
                             latch.countDown();
                         } catch (PulsarClientException e) {
                             e.printStackTrace();
                         }
                     })
                     .subscribe()) {
            for (int a = 0; a < 100; a++) {
                producer.send(UUID.randomUUID().toString());
            }

            latch.await(30, TimeUnit.SECONDS);

            // Mock failed PartitionMetadataRequest
            ByteBuf buf =
                    Commands.newPartitionMetadataRequest("xx://xx/xx/" + UUID.randomUUID(), 100);
            BaseCommand c = new BaseCommand();
            c.parseFrom(ByteBufUtil.getBytes(buf));
            CommandPartitionedTopicMetadata ctm = c.getPartitionMetadata();
            Topic topic1 = pulsar.getBrokerService().getTopic(topic + "-partition-0", false).get().get();
            Map<String, org.apache.pulsar.broker.service.Producer> producers = topic1.getProducers();
            Class<ServerCnx> klass = ServerCnx.class;
            Method method = klass.getDeclaredMethod("handlePartitionMetadataRequest", CommandPartitionedTopicMetadata.class);
            method.setAccessible(true);
            for (org.apache.pulsar.broker.service.Producer p : producers.values()) {
                ServerCnx tcnx = (ServerCnx) p.getCnx();
                method.invoke(tcnx, ctm);
            }

            ByteArrayOutputStream output = new ByteArrayOutputStream();
            PrometheusMetricsGenerator.generate(pulsar, false, false, false, false, output);
            Multimap<String, PrometheusMetricsClient.Metric> metricsMap = PrometheusMetricsClient.parseMetrics(output.toString());
            Collection<PrometheusMetricsClient.Metric> cmdExecutionFailed = metricsMap.get("pulsar_broker_command_execution_failed" + "_total");
            Collection<PrometheusMetricsClient.Metric> cmdExecutionLatency = metricsMap.get("pulsar_broker_command_execution_latency_ms" + "_sum");

            for (PrometheusMetricsClient.Metric m : cmdExecutionFailed) {
                String cluster = m.tags.get("cluster");
                Assert.assertNotNull(cluster);
                Assert.assertEquals(cluster, "test");
                String command = m.tags.get("command");
                Assert.assertNotNull(command);
                if (command.equals(BaseCommand.Type.PARTITIONED_METADATA.name())) {
                    Assert.assertTrue(m.value >= 1);
                }
            }

            for (PrometheusMetricsClient.Metric m : cmdExecutionLatency) {
                String cluster = m.tags.get("cluster");
                Assert.assertNotNull(cluster);
                Assert.assertEquals(cluster, "test");
                String command = m.tags.get("command");
                Assert.assertNotNull(command);
                if (command.equals(BaseCommand.Type.PARTITIONED_METADATA.name())) {
                    Assert.assertTrue(m.value > 0);
                }
            }
        }
    }
}