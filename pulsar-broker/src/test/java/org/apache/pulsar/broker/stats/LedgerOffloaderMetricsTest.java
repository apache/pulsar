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
package org.apache.pulsar.broker.stats;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import com.google.common.collect.Multimap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.io.ByteArrayOutputStream;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import lombok.Cleanup;
import org.apache.bookkeeper.mledger.LedgerOffloader;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.impl.LedgerOffloaderMXBeanImpl;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.BrokerTestBase;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.stats.prometheus.PrometheusMetricsGenerator;
import org.apache.pulsar.common.util.SimpleTextOutputStream;
import org.junit.Assert;
import org.mockito.Mockito;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class LedgerOffloaderMetricsTest  extends BrokerTestBase {
    @BeforeMethod(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        super.baseSetup();


    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }


    public String convertByteBufToString(ByteBuf buf) {
        String str;
        if(buf.hasArray()) {
            str = new String(buf.array(), buf.arrayOffset() + buf.readerIndex(), buf.readableBytes());
        } else {
            byte[] bytes = new byte[buf.readableBytes()];
            buf.getBytes(buf.readerIndex(), bytes);
            str = new String(bytes, 0, buf.readableBytes());
        }
        return str;
    }

    @Test(timeOut = 3000)
    public void testTopicLevelMetrics() throws Exception {
        String ns1 = "prop/ns-abc1";
        admin.namespaces().createNamespace(ns1);

        ByteBuf buf = ByteBufAllocator.DEFAULT.heapBuffer();
        SimpleTextOutputStream stream = new SimpleTextOutputStream(buf);

        String []topics = new String[3];

        LedgerOffloaderMXBeanImpl mbean = new LedgerOffloaderMXBeanImpl("test");
        LedgerOffloader offloader = Mockito.mock(LedgerOffloader.class);
        Topic topic = Mockito.mock(PersistentTopic.class);
        CompletableFuture<Optional<Topic>> topicFuture = new CompletableFuture<>();
        Optional<Topic> topicOptional = Optional.of(topic);
        topicFuture.complete(topicOptional);
        BrokerService brokerService = spy(pulsar.getBrokerService());
        doReturn(brokerService).when(pulsar).getBrokerService();


        for (int i = 0; i < 3; i++) {
            String topicName = "persistent://prop/ns-abc1/testMetrics" + UUID.randomUUID();
            topics[i] = topicName;
            admin.topics().createNonPartitionedTopic(topicName);

            doReturn(topicFuture).when(brokerService).getTopicIfExists(topicName);
            Assert.assertTrue(topic instanceof PersistentTopic);

            ManagedLedger ledgerM = Mockito.mock(ManagedLedger.class);
            doReturn(ledgerM).when(((PersistentTopic) topic)).getManagedLedger();
            ManagedLedgerConfig config = Mockito.mock(ManagedLedgerConfig.class);
            doReturn(config).when(ledgerM).getConfig();
            doReturn(offloader).when(config).getLedgerOffloader();

            Mockito.when(offloader.getStats()).thenReturn(mbean);

            mbean.recordOffloadError(topicName);
            mbean.recordOffloadError(topicName);
            mbean.recordOffloadRate(topicName, 100);
            mbean.recordOffloadTime(topicName, 10000000000L, TimeUnit.NANOSECONDS);
            mbean.recordReadLedgerLatency(topicName, 1000, TimeUnit.NANOSECONDS);
            mbean.recordReadOffloadError(topicName);
            mbean.recordReadOffloadError(topicName);
            mbean.recordReadOffloadIndexLatency(topicName, 1000000L, TimeUnit.NANOSECONDS);
            mbean.recordReadOffloadRate(topicName, 100000);
            mbean.recordStreamingWriteToStorageError(topicName);
            mbean.recordStreamingWriteToStorageError(topicName);
            mbean.recordStreamingWriteToStorageRate(topicName, 1000001);
            mbean.recordWriteToStorageError(topicName);
            mbean.recordWriteToStorageError(topicName);
            mbean.recordWriteToStorageLatency(topicName, 20000, TimeUnit.NANOSECONDS);
        }
        mbean.refreshStats(1, TimeUnit.SECONDS);

        Method parseMetricMethod = PrometheusMetricsGenerator.class.
                getDeclaredMethod("generateLedgerOffloaderMetrics",
                        PulsarService.class, SimpleTextOutputStream.class,
                        boolean.class);
        parseMetricMethod.setAccessible(true);
        parseMetricMethod.invoke(null, pulsar, stream, true);


        String metricsStr = convertByteBufToString(buf);
//        System.out.println(metricsStr);
        Multimap<String, PrometheusMetricsTest.Metric> metrics = PrometheusMetricsTest.parseMetrics(metricsStr);
        String []metricName = new String[]{"pulsar_ledgeroffloader_writeError",
                "pulsar_ledgeroffloader_offloadError", "pulsar_ledgeroffloader_readOffloadError",
                "pulsar_ledgeroffloader_streamingWriteError"};
        for (int s = 0; s < metricName.length; s++) {
            Collection<PrometheusMetricsTest.Metric> metric = metrics.get(metricName[s]);
            for (int i = 0; i < 3; i++) {
                String namespace = ns1;
                String topicName = topics[i];
                LongAdder findNum = new LongAdder();
                metric.forEach(item -> {
                    if (namespace.equals(item.tags.get("namespace")) && topicName.equals(item.tags.get("topic"))) {
                        Assert.assertTrue(item.value == 2);
                        findNum.increment();
                    }
                });
                Assert.assertTrue(findNum.intValue() == 1);
            }
        }
    }

    @Test(timeOut = 3000)
    public void testNamespaceLevelMetrics() throws Exception {
        String ns1 = "prop/ns-abc1";
        String ns2 = "prop/ns-abc2";

        ByteBuf buf = ByteBufAllocator.DEFAULT.heapBuffer();
        SimpleTextOutputStream stream = new SimpleTextOutputStream(buf);

        String []topics = new String[6];

        LedgerOffloaderMXBeanImpl mbean = new LedgerOffloaderMXBeanImpl("test");
        LedgerOffloader offloader = Mockito.mock(LedgerOffloader.class);
        Topic topic = Mockito.mock(PersistentTopic.class);
        CompletableFuture<Optional<Topic>> topicFuture = new CompletableFuture<>();
        Optional<Topic> topicOptional = Optional.of(topic);
        topicFuture.complete(topicOptional);
        BrokerService brokerService = spy(pulsar.getBrokerService());
        doReturn(brokerService).when(pulsar).getBrokerService();

        for (int s = 0; s < 2; s++) {
            String nameSpace = ns1;
            if (s == 1) {
                nameSpace = ns2;
            }
            admin.namespaces().createNamespace(nameSpace);
            String baseTopic1 = "persistent://" + nameSpace + "/testMetrics";
            for (int i = 0; i < 6; i++) {
                String topicName = baseTopic1 + UUID.randomUUID();
                topics[i] = topicName;
                admin.topics().createNonPartitionedTopic(topicName);
                doReturn(topicFuture).when(brokerService).getTopicIfExists(topicName);
                Assert.assertTrue(topic instanceof PersistentTopic);


                ManagedLedger ledgerM = Mockito.mock(ManagedLedger.class);
                doReturn(ledgerM).when(((PersistentTopic) topic)).getManagedLedger();
                ManagedLedgerConfig config = Mockito.mock(ManagedLedgerConfig.class);
                doReturn(config).when(ledgerM).getConfig();
                doReturn(offloader).when(config).getLedgerOffloader();

                Mockito.when(offloader.getStats()).thenReturn(mbean);

                mbean.recordOffloadError(topicName);
                mbean.recordOffloadRate(topicName, 100);
                mbean.recordOffloadTime(topicName, 10000000000L, TimeUnit.NANOSECONDS);
                mbean.recordReadLedgerLatency(topicName, 1000, TimeUnit.NANOSECONDS);
                mbean.recordReadOffloadError(topicName);
                mbean.recordReadOffloadIndexLatency(topicName, 1000000L, TimeUnit.NANOSECONDS);
                mbean.recordReadOffloadRate(topicName, 100000);
                mbean.recordStreamingWriteToStorageError(topicName);
                mbean.recordStreamingWriteToStorageRate(topicName, 1000001);
                mbean.recordWriteToStorageError(topicName);
                mbean.recordWriteToStorageLatency(topicName, 20000, TimeUnit.NANOSECONDS);
            }
        }

        mbean.refreshStats(1, TimeUnit.SECONDS);

        Method parseMetricMethod = PrometheusMetricsGenerator.class.
                getDeclaredMethod("generateLedgerOffloaderMetrics",
                        PulsarService.class, SimpleTextOutputStream.class,
                        boolean.class);
        parseMetricMethod.setAccessible(true);
        parseMetricMethod.invoke(null, pulsar, stream, false);


        String metricsStr = convertByteBufToString(buf);
        System.out.println(convertByteBufToString(buf));
        Multimap<String, PrometheusMetricsTest.Metric> metrics = PrometheusMetricsTest.parseMetrics(metricsStr);
        String []metricName = new String[]{"pulsar_ledgeroffloader_writeError",
                "pulsar_ledgeroffloader_offloadError", "pulsar_ledgeroffloader_readOffloadError",
                "pulsar_ledgeroffloader_streamingWriteError"};
        for (int s = 0; s < metricName.length; s++) {
            Collection<PrometheusMetricsTest.Metric> metric = metrics.get(metricName[s]);
            for (int i = 0; i < 2; i++) {
                String namespace = i == 1 ? ns2 : ns1;
                LongAdder findNum = new LongAdder();
                metric.forEach(item -> {
                    if (namespace.equals(item.tags.get("namespace"))) {
                        Assert.assertTrue(item.value == 6);
                        findNum.increment();
                    }
                });
                Assert.assertTrue(findNum.intValue() == 1);
            }
        }
    }

}
