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
package org.apache.pulsar.functions.worker;

import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerAccessMode;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.testng.annotations.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.fail;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import org.apache.distributedlog.DistributedLogConfiguration;
import java.util.HashSet;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;

public class WorkerUtilsTest {

    @Test
    public void testCreateExclusiveProducerWithRetry() {
        Producer<byte[]> producer = mock(Producer.class);
        ProducerBuilder<byte[]> builder = mock(ProducerBuilder.class);
        when(builder.topic(anyString())).thenReturn(builder);
        when(builder.producerName(anyString())).thenReturn(builder);
        when(builder.enableBatching(anyBoolean())).thenReturn(builder);
        when(builder.blockIfQueueFull(anyBoolean())).thenReturn(builder);
        when(builder.compressionType(any(CompressionType.class))).thenReturn(builder);
        when(builder.sendTimeout(anyInt(), any(TimeUnit.class))).thenReturn(builder);
        when(builder.accessMode(any())).thenReturn(builder);
        when(builder.createAsync()).thenReturn(CompletableFuture.completedFuture(producer));

        PulsarClient pulsarClient = mock(PulsarClient.class);
        when(pulsarClient.newProducer()).thenReturn(builder);

        Producer<byte[]> p = null;
        try {
            p = WorkerUtils.createExclusiveProducerWithRetry(pulsarClient, "test-topic", "test-producer", () -> true, 0);
        } catch (WorkerUtils.NotLeaderAnymore notLeaderAnymore) {
            fail();
        }
        assertNotNull(p);
        verify(builder, times(1)).topic(eq("test-topic"));
        verify(builder, times(1)).producerName(eq("test-producer"));
        verify(builder, times(1)).accessMode(eq(ProducerAccessMode.Exclusive));

        CompletableFuture completableFuture = new CompletableFuture();
        completableFuture.completeExceptionally(new PulsarClientException.ProducerFencedException("test"));
        when(builder.createAsync()).thenReturn(completableFuture);
        try {
            WorkerUtils.createExclusiveProducerWithRetry(pulsarClient, "test-topic", "test-producer", () -> false, 0);
            fail();
        } catch (WorkerUtils.NotLeaderAnymore notLeaderAnymore) {

        }

        AtomicInteger i = new AtomicInteger();
        try {
            WorkerUtils.createExclusiveProducerWithRetry(pulsarClient, "test-topic", "test-producer", new Supplier<Boolean>() {

                @Override
                public Boolean get() {
                    if (i.getAndIncrement() < 6) {
                        return true;
                    }
                    return false;
                }
            }, 0);
            fail();
        } catch (WorkerUtils.NotLeaderAnymore notLeaderAnymore) {

        }
    }

    @Test
    public void testDLogConfiguration() throws URISyntaxException, IOException {
        // The config yml is seeded with a fake bookie config.
        URL yamlUrl = getClass().getClassLoader().getResource("test_worker_config.yml");
        WorkerConfig config = WorkerConfig.load(yamlUrl.toURI().getPath());

        // Map the config.
        DistributedLogConfiguration dlogConf = WorkerUtils.getDlogConf(config);

        // Verify the outcome.
        assertEquals(dlogConf.getString("bkc.testKey"), "fakeValue",
                "The bookkeeper client config mapping should apply.");
    }

    @Test
    public void testProxyRolesInWorkerConfigMapToServiceConfiguration() throws Exception {
        URL yamlUrl = getClass().getClassLoader().getResource("test_worker_config.yml");
        WorkerConfig wc = WorkerConfig.load(yamlUrl.toURI().getPath());
        ServiceConfiguration conf = PulsarConfigurationLoader.convertFrom(wc);
        HashSet<String> proxyRoles = new HashSet<>();
        proxyRoles.add("proxyA");
        proxyRoles.add("proxyB");
        assertEquals(conf.getProxyRoles(), proxyRoles);
    }
}