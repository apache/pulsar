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
package org.apache.pulsar.broker.service;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.netty.buffer.ByteBuf;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.ConnectionPool;
import org.apache.pulsar.client.impl.PulsarServiceNameResolver;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.common.util.netty.EventLoopUtil;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class BrokerServiceThrottlingTest extends BrokerTestBase {

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.baseSetup();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    /**
     * Verifies: updating zk-throttling node reflects broker-maxConcurrentLookupRequest and updates semaphore.
     *
     * @throws Exception
     */
    @Test
    public void testThrottlingLookupRequestSemaphore() throws Exception {
        BrokerService service = pulsar.getBrokerService();
        assertNotEquals(service.lookupRequestSemaphore.get().availablePermits(), 0);
        admin.brokers().updateDynamicConfiguration("maxConcurrentLookupRequest", Integer.toString(0));
        Thread.sleep(1000);
        assertEquals(service.lookupRequestSemaphore.get().availablePermits(), 0);
    }

    /**
     * Broker has maxConcurrentLookupRequest = 0 so, it rejects incoming lookup request and it cause consumer creation
     * failure.
     *
     * @throws Exception
     */
    @Test
    public void testLookupThrottlingForClientByBroker0Permit() throws Exception {

        final String topicName = "persistent://prop/ns-abc/newTopic";

        @Cleanup
        PulsarClient pulsarClient = PulsarClient.builder()
                .serviceUrl(pulsar.getBrokerServiceUrl())
                .statsInterval(0, TimeUnit.SECONDS)
                .build();

        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName("mysub").subscribe();
        consumer.close();

        int newPermits = 0;
        admin.brokers().updateDynamicConfiguration("maxConcurrentLookupRequest", Integer.toString(newPermits));
        // wait config to be updated
        for (int i = 0; i < 5; i++) {
            if (pulsar.getConfiguration().getMaxConcurrentLookupRequest() != newPermits) {
                Thread.sleep(100 + (i * 10));
            } else {
                break;
            }
        }

        try {
            consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName("mysub").subscribe();
            consumer.close();
            fail("It should fail as throttling should not receive any request");
        } catch (org.apache.pulsar.client.api.PulsarClientException.TooManyRequestsException e) {
            // ok as throttling set to 0
        }
    }

    /**
     * Verifies: Broker side throttling:
     *
     * <pre>
     * 1. concurrent_consumer_creation > maxConcurrentLookupRequest at broker
     * 2. few of the consumer creation must fail with TooManyLookupRequestException.
     * </pre>
     *
     * @throws Exception
     */
    @Test
    public void testLookupThrottlingForClientByBroker() throws Exception {
        final String topicName = "persistent://prop/ns-abc/newTopic";

        @Cleanup
        PulsarClient pulsarClient = PulsarClient.builder()
                .serviceUrl(pulsar.getBrokerServiceUrl())
                .statsInterval(0, TimeUnit.SECONDS)
                .ioThreads(20).connectionsPerBroker(20).build();

        int newPermits = 1;
        admin.brokers().updateDynamicConfiguration("maxConcurrentLookupRequest", Integer.toString(newPermits));
        // wait config to be updated
        for (int i = 0; i < 5; i++) {
            if (pulsar.getConfiguration().getMaxConcurrentLookupRequest() != newPermits) {
                Thread.sleep(100 + (i * 10));
            } else {
                break;
            }
        }

        PulsarServiceNameResolver resolver = new PulsarServiceNameResolver();
        resolver.updateServiceUrl(pulsar.getBrokerServiceUrl());
        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setConnectionsPerBroker(20);

        EventLoopGroup eventLoop = EventLoopUtil.newEventLoopGroup(20, false,
                new DefaultThreadFactory("test-pool", Thread.currentThread().isDaemon()));
        ExecutorService executor = Executors.newFixedThreadPool(10);
        try (ConnectionPool pool = new ConnectionPool(conf, eventLoop)) {
            final int totalConsumers = 20;
            List<Future<?>> futures = new ArrayList<>();

            // test for partitionMetadataRequest
            for (int i = 0; i < totalConsumers; i++) {
                long reqId = 0xdeadbeef + i;
                Future<?> f = executor.submit(() -> {
                        ByteBuf request = Commands.newPartitionMetadataRequest(topicName, reqId);
                        pool.getConnection(resolver.resolveHost())
                            .thenCompose(clientCnx -> clientCnx.newLookup(request, reqId))
                            .get();
                        return null;
                    });
                futures.add(f);
            }

            int rejects = 0;
            for (Future<?> f : futures) {
                try {
                    f.get();
                } catch (ExecutionException e) {
                    Throwable rootCause = e;
                    while (rootCause instanceof ExecutionException) {
                        rootCause = rootCause.getCause();
                    }
                    if (rootCause instanceof
                        org.apache.pulsar.client.api.PulsarClientException.TooManyRequestsException) {
                        rejects++;
                    } else {
                        throw e;
                    }
                }
            }
            assertTrue(rejects > 0);
            futures.clear();

            // test for lookup
            for (int i = 0; i < totalConsumers; i++) {
                long reqId = 0xdeadfeef + i;
                Future<?> f = executor.submit(() -> {
                        ByteBuf request = Commands.newLookup(topicName, true, reqId);
                        pool.getConnection(resolver.resolveHost())
                            .thenCompose(clientCnx -> clientCnx.newLookup(request, reqId))
                            .get();
                        return null;
                    });
                futures.add(f);
            }

            rejects = 0;
            for (Future<?> f : futures) {
                try {
                    f.get();
                } catch (ExecutionException e) {
                    Throwable rootCause = e;
                    while (rootCause instanceof ExecutionException) {
                        rootCause = rootCause.getCause();
                    }
                    if (rootCause instanceof
                        org.apache.pulsar.client.api.PulsarClientException.TooManyRequestsException) {
                        rejects++;
                    } else {
                        throw e;
                    }
                }
            }
            assertTrue(rejects > 0);
        } finally {
            executor.shutdownNow();
            eventLoop.shutdownNow();
        }
    }


    /**
     * This testcase make sure that once consumer lost connection with broker, it always reconnects with broker by
     * retrying on throttling-error exception also.
     *
     * <pre>
     * 1. all consumers get connected
     * 2. broker restarts with maxConcurrentLookupRequest = 1
     * 3. consumers reconnect and some get TooManyRequestException and again retries
     * 4. eventually all consumers will successfully connect to broker
     * </pre>
     *
     * @throws Exception
     */
    @Test
    public void testLookupThrottlingForClientByBrokerInternalRetry() throws Exception {
        final String topicName = "persistent://prop/ns-abc/newTopic-" + UUID.randomUUID().toString();

        @Cleanup
        PulsarClient pulsarClient = PulsarClient.builder()
                .serviceUrl(pulsar.getBrokerServiceUrl())
                .statsInterval(0, TimeUnit.SECONDS)
                .ioThreads(20).connectionsPerBroker(20).build();
        upsertLookupPermits(100);
        List<Consumer<byte[]>> consumers = Collections.synchronizedList(Lists.newArrayList());
        @Cleanup("shutdownNow")
        ExecutorService executor = Executors.newFixedThreadPool(10);
        final int totalConsumers = 8;
        CountDownLatch latch = new CountDownLatch(totalConsumers);
        for (int i = 0; i < totalConsumers; i++) {
            executor.execute(() -> {
                try {
                    consumers.add(pulsarClient.newConsumer().topic(topicName).subscriptionName("mysub")
                            .subscriptionType(SubscriptionType.Shared).subscribe());
                } catch (PulsarClientException.TooManyRequestsException e) {
                    // ok
                } catch (Exception e) {
                    fail("it shouldn't failed");
                }
                latch.countDown();
            });
        }
        latch.await();

        admin.brokers().updateDynamicConfiguration("maxConcurrentLookupRequest", "1");
        admin.topics().unload(topicName);

        // wait strategically for all consumers to reconnect
        retryStrategically((test) -> areAllConsumersConnected(consumers), 5, 500);

        int totalConnectedConsumers = 0;
        for (Consumer<byte[]> consumer : consumers) {
            if (consumer.isConnected()) {
                totalConnectedConsumers++;
            }
            consumer.close();

        }
        assertEquals(totalConnectedConsumers, totalConsumers);
    }

    private boolean areAllConsumersConnected(List<Consumer<byte[]>> consumers) {
        for (Consumer<byte[]> consumer : consumers) {
            if (!consumer.isConnected()) {
                return false;
            }
        }
        return true;
    }

    private void upsertLookupPermits(int permits) throws Exception {
        pulsar.getPulsarResources().getDynamicConfigResources().setDynamicConfigurationWithCreate(optMap -> {
            Map<String, String> map = optMap.orElse(new TreeMap<>());
            map.put("maxConcurrentLookupRequest", Integer.toString(permits));
            return map;
        });
    }
}
