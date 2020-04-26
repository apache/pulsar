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

import static org.apache.pulsar.broker.service.BrokerService.BROKER_SERVICE_CONFIGURATION_PATH;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.fail;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.util.ZkUtils;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.ConsumerImpl;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 */
public class BrokerServiceThrottlingTest extends BrokerTestBase {

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.baseSetup();
    }

    @AfterMethod
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

        List<Consumer<byte[]>> successfulConsumers = Collections.synchronizedList(Lists.newArrayList());
        ExecutorService executor = Executors.newFixedThreadPool(10);
        final int totalConsumers = 20;
        CountDownLatch latch = new CountDownLatch(totalConsumers);
        for (int i = 0; i < totalConsumers; i++) {
            executor.execute(() -> {
                try {
                    successfulConsumers.add(pulsarClient.newConsumer().topic(topicName).subscriptionName("mysub")
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

        for (Consumer<?> c : successfulConsumers) {
            if (c != null) {
                c.close();
            }
        }
        pulsarClient.close();
        executor.shutdown();
        assertNotEquals(successfulConsumers.size(), totalConsumers);
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

        PulsarClient pulsarClient = PulsarClient.builder()
                .serviceUrl(pulsar.getBrokerServiceUrl())
                .statsInterval(0, TimeUnit.SECONDS)
                .ioThreads(20).connectionsPerBroker(20).build();
        upsertLookupPermits(100);
        List<Consumer<byte[]>> consumers = Collections.synchronizedList(Lists.newArrayList());
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
        for (int i = 0; i < consumers.size(); i++) {
            if (((ConsumerImpl<?>) consumers.get(i)).isConnected()) {
                totalConnectedConsumers++;
            }
            consumers.get(i).close();

        }
        assertEquals(totalConnectedConsumers, totalConsumers);

        executor.shutdown();
        pulsarClient.close();
    }

    private boolean areAllConsumersConnected(List<Consumer<byte[]>> consumers) {
        for (int i = 0; i < consumers.size(); i++) {
            if (!((ConsumerImpl<?>) consumers.get(i)).isConnected()) {
                return false;
            }
        }
        return true;
    }

    private void upsertLookupPermits(int permits) throws Exception {
        Map<String, String> throttlingMap = Maps.newHashMap();
        throttlingMap.put("maxConcurrentLookupRequest", Integer.toString(permits));
        byte[] content = ObjectMapperFactory.getThreadLocal().writeValueAsBytes(throttlingMap);
        if (mockZooKeeper.exists(BROKER_SERVICE_CONFIGURATION_PATH, false) != null) {
            mockZooKeeper.setData(BROKER_SERVICE_CONFIGURATION_PATH, content, -1);
        } else {
            ZkUtils.createFullPathOptimistic(mockZooKeeper, BROKER_SERVICE_CONFIGURATION_PATH, content,
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
    }
}
