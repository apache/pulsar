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
package org.apache.pulsar.client.api;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import com.google.common.util.concurrent.RateLimiter;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.Cleanup;
import lombok.CustomLog;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.stats.NamespaceStats;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Producer and consumer stats tests that stop their broker or read broker-wide rates and metrics, so they run on a
 * broker of their own. {@link SimpleProducerConsumerStatTest} has the tests that share one.
 */
@Test(groups = "broker-api")
@CustomLog
public class SimpleProducerConsumerStatOwnBrokerTest extends ProducerConsumerBase {

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.internalSetupForStatsTest();
        super.producerBaseSetup();
    }

    @Override
    protected ServiceConfiguration getDefaultConf() {
        ServiceConfiguration conf = super.getDefaultConf();
        // wait for shutdown of the broker, this prevents flakiness which could be caused by metrics being
        // unregistered asynchronously. This impacts the execution of the next test method if this would be happening.
        conf.setBrokerShutdownTimeoutMs(5000L);
        return conf;
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @DataProvider(name = "batch")
    public Object[][] batchMessageDelayMsProvider() {
        return new Object[][] { { 0 }, { 1000 } };
    }

    @DataProvider(name = "batchingEnabled")
    public Object[][] batchingEnabled() {
        return new Object[][] { { true }, { false } };
    }

    @SuppressWarnings("deprecation")
    @Test(dataProvider = "batch")
    public void testSendTimeout(int batchMessageDelayMs) throws Exception {
        log.info().attr("method", methodName).log("Starting test");

        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic("persistent://my-property/my-ns/my-topic5")
                .subscriptionName("my-subscriber-name").subscribe();

        ProducerBuilder<byte[]> producerBuilder = pulsarClient.newProducer()
                .topic("persistent://my-property/my-ns/my-topic5").sendTimeout(1, TimeUnit.SECONDS);
        if (batchMessageDelayMs != 0) {
            producerBuilder.enableBatching(true)
                    .batchingMaxPublishDelay(2L * batchMessageDelayMs, TimeUnit.MILLISECONDS)
                    .batchingMaxMessages(5);
        }

        Producer<byte[]> producer = producerBuilder.create();

        final String message = "my-message";

        // Trigger the send timeout
        stopBroker();

        Future<MessageId> future = producer.sendAsync(message.getBytes());

        try {
            future.get();
            fail("Send operation should have failed");
        } catch (ExecutionException e) {
            // Expected
        }

        startBroker();

        // We should not have received any message. Once the producer, which resends its pending messages when it
        // reconnects, and the consumer are connected again, the subscription has no message for the consumer.
        Awaitility.await().untilAsserted(() -> {
            assertTrue(producer.isConnected());
            assertTrue(consumer.isConnected());
            assertEquals(admin.topics().getStats("persistent://my-property/my-ns/my-topic5", true)
                    .getSubscriptions().get("my-subscriber-name").getMsgBacklog(), 0);
        });
        Message<byte[]> msg = consumer.receive(200, TimeUnit.MILLISECONDS);
        assertNull(msg);
        // The producer's stats timer, which runs every second, records the failed send before the producer is closed
        Awaitility.await().untilAsserted(() -> assertEquals(producer.getStats().getTotalSendFailed(), 1));
        consumer.close();
        producer.close();
        ConsumerStats cStat = consumer.getStats();
        ProducerStats pStat = producer.getStats();
        assertEquals(pStat.getTotalMsgsSent(), 0);
        assertEquals(pStat.getTotalSendFailed(), 1);
        assertEquals(cStat.getTotalMsgsReceived(), 0);
        assertEquals(cStat.getTotalMsgsReceived(), cStat.getTotalAcksSent());
        log.info().attr("method", methodName).log("Exiting test");
    }

    @Test
    public void testBatchMessagesRateOut() throws PulsarClientException, InterruptedException, PulsarAdminException {
        log.info().attr("method", methodName).log("Starting test");
        String topicName = "persistent://my-property/my-ns/testBatchMessagesRateOut";
        double produceRate = 17;
        int batchSize = 5;
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName("my-subscriber-name")
                .subscribe();
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).batchingMaxMessages(batchSize)
                .enableBatching(true).batchingMaxPublishDelay(2, TimeUnit.SECONDS).create();
        AtomicBoolean runTest = new AtomicBoolean(true);
        Thread t1 = new Thread(() -> {
            RateLimiter r = RateLimiter.create(produceRate);
            while (runTest.get()) {
                r.acquire();
                producer.sendAsync("Hello World".getBytes());
                consumer.receiveAsync().thenAccept(consumer::acknowledgeAsync);
            }
        });
        t1.start();
        Thread.sleep(2000); // Two seconds sleep
        runTest.set(false);
        pulsar.getBrokerService().updateRates();
        double actualRate = admin.topics().getStats(topicName).getMsgRateOut();
        assertTrue(actualRate > (produceRate / batchSize));
        consumer.unsubscribe();
        log.info().attr("method", methodName).log("Exiting test");
    }

    @Test
    public void testAddBrokerLatencyStats() throws Exception {

        log.info().attr("method", methodName).log("Starting test");

        ProducerBuilder<byte[]> producerBuilder = pulsarClient.newProducer()
                .topic("persistent://my-property/my-ns/my-topic1");

        Producer<byte[]> producer = producerBuilder.create();

        int numMessages = 120;
        for (int i = 0; i < numMessages; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        pulsar.getBrokerService().updateRates();

        String json = admin.brokerStats().getMetrics();
        JsonArray metrics = new Gson().fromJson(json, JsonArray.class);

        boolean latencyCaptured = false;
        for (int i = 0; i < metrics.size(); i++) {
            try {
                String data = metrics.get(i).getAsJsonObject().get("metrics").toString();
                if (data.contains(NamespaceStats.BRK_ADD_ENTRY_LATENCY_PREFIX)) {
                    JsonObject stat = metrics.get(i).getAsJsonObject().get("metrics").getAsJsonObject();
                    for (String key : stat.keySet()) {
                        if (key.startsWith(NamespaceStats.BRK_ADD_ENTRY_LATENCY_PREFIX)) {
                            double val = stat.get(key).getAsDouble();
                            if (val > 0.0) {
                                latencyCaptured = true;
                            }
                        }
                    }
                    System.out.println(stat.toString());
                }
            } catch (Exception e) {
                //Ok
            }
        }

        assertTrue(latencyCaptured);
        producer.close();
        log.info().attr("method", methodName).log("Exiting test");
    }

    @Test(dataProvider =  "batchingEnabled")
    public void testProducerPendingQueueSizeStats(boolean batchingEnabled) throws Exception {
        log.info().attr("method", methodName).log("Starting test");
        ProducerBuilder<byte[]> producerBuilder = pulsarClient.newProducer()
                .topic("persistent://my-property/my-ns/my-topic1");

        @Cleanup
        Producer<byte[]> producer = producerBuilder.enableBatching(batchingEnabled).create();

        stopBroker();

        int numMessages = 120;
        for (int i = 0; i < numMessages; i++) {
            String message = "my-message-" + i;
            producer.sendAsync(message.getBytes());
        }
        Awaitility.await().timeout(2, TimeUnit.MINUTES)
                .until(() -> producer.getStats().getPendingQueueSize() == numMessages);
        assertEquals(producer.getStats().getPendingQueueSize(), numMessages);
    }
}
