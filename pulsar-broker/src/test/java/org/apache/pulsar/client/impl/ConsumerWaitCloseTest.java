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
package org.apache.pulsar.client.impl;

import static org.testng.Assert.assertEquals;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pulsar.client.api.BatchReceivePolicy;
import org.apache.pulsar.client.api.CloseWaitForJobPolicy;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Messages;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "broker-impl")
public class ConsumerWaitCloseTest extends ProducerConsumerBase {
    private static final long testTimeout = 90000; // 1.5 min
    private static final Logger log = LoggerFactory.getLogger(ConsumerWaitCloseTest.class);


    @BeforeClass(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterMethod(alwaysRun = true)
    public void rest() throws Exception {
        pulsar.getConfiguration().setForceDeleteTenantAllowed(true);
        pulsar.getConfiguration().setForceDeleteNamespaceAllowed(true);

        for (String tenant : admin.tenants().getTenants()) {
            for (String namespace : admin.namespaces().getNamespaces(tenant)) {
                deleteNamespaceWithRetry(namespace, true);
            }
            admin.tenants().deleteTenant(tenant, true);
        }

        for (String cluster : admin.clusters().getClusters()) {
            admin.clusters().deleteCluster(cluster);
        }

        pulsar.getConfiguration().setForceDeleteTenantAllowed(false);
        pulsar.getConfiguration().setForceDeleteNamespaceAllowed(false);

        super.producerBaseSetup();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
    }

    @Test(timeOut = testTimeout)
    public void testWithoutWaitCloseConsumer() throws Exception {

        log.info("-- Starting {} test --", methodName);
        final String topic = buildOneTopic();
        final int numMessages = 5;
        AtomicInteger counter = new AtomicInteger(0);

        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("my-sub")
                .subscribe();

        producerTopic(topic, numMessages);

        for (int i = 0; i < numMessages; i++) {
            Message<String> msg = consumer.receive(1, TimeUnit.SECONDS);
            log.info("consumer message:{}, messageId:{}", msg.getValue(), msg.getMessageId().toString());
            int tempIdx = i;
            new Thread(() -> {
                try {
                    if (tempIdx % 2 == 0) {
                        TimeUnit.SECONDS.sleep(3);
                    } else {
                        TimeUnit.SECONDS.sleep(4);
                    }
                    consumer.acknowledge(msg);
                    counter.incrementAndGet();
                    log.info("ack, messageId:{}", msg.getMessageId().toString());
                } catch (Exception ignored) {
                }
            }).start();
        }
        consumer.close();
        assertEquals(counter.get(), 0);
    }

    @Test(timeOut = testTimeout)
    public void testFailConfig() throws Exception {

        log.info("-- Starting {} test --", methodName);
        final String topic = buildOneTopic();
        Exception exception = null;
        try {
            pulsarClient.newConsumer(Schema.STRING)
                    .topic(topic)
                    .subscriptionName("my-sub")
                    .closeWaitForJob(CloseWaitForJobPolicy.builder()
                            .closeWaitForJob(true)
                            .timeout(1, null)
                            .build())
                    .subscribe();
        }catch (IllegalArgumentException e){
            exception = e;
        }
        assertEquals(exception.getMessage(),"Must set timeout unit for timeout.");


    }

    @Test(timeOut = testTimeout)
    public void testWaitCloseConsumer() throws Exception {

        log.info("-- Starting {} test --", methodName);
        final String topic = buildOneTopic();
        final int numMessages = 5;
        AtomicInteger counter = new AtomicInteger(0);

        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("my-sub")
                .closeWaitForJob(CloseWaitForJobPolicy.builder()
                        .closeWaitForJob(true)
                        .build())
                .subscribe();

        producerTopic(topic, numMessages);

        for (int i = 0; i < numMessages; i++) {
            Message<String> msg = consumer.receive(1, TimeUnit.SECONDS);
            log.info("consumer message:{}, messageId:{}", msg.getValue(), msg.getMessageId().toString());
            int tempIdx = i;
            new Thread(() -> {
                try {
                    if (tempIdx % 2 == 0) {
                        TimeUnit.SECONDS.sleep(1);
                    } else {
                        TimeUnit.SECONDS.sleep(2);
                    }
                    consumer.acknowledge(msg);
                    counter.incrementAndGet();
                    log.info("ack, messageId:{}", msg.getMessageId().toString());
                } catch (Exception ignored) {
                }
            }).start();
        }
        consumer.close();

        assertEquals(counter.get(), numMessages);
    }


    @Test(timeOut = testTimeout)
    public void testWaitCloseConsumerAsync() throws PulsarClientException, InterruptedException, ExecutionException {
        String topic = buildOneTopic();
        String subscriptionName = "my-test-close-consumer";
        final int numMessages = 5;
        AtomicInteger counter = new AtomicInteger(0);

        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName(subscriptionName)
                .subscriptionType(SubscriptionType.Shared)
                .ackTimeout(0, TimeUnit.SECONDS)
                .closeWaitForJob(CloseWaitForJobPolicy.builder()
                        .closeWaitForJob(true)
                        .build())
                .subscribe();

        producerTopic(topic, numMessages);

        for (int i = 0; i < numMessages; i++) {

            Message<String> message = consumer.receiveAsync().get();
            log.info("consumer message:{}, messageId:{}", message.getValue(), message.getMessageId().toString());
            int tempIdx = i;
            new Thread(() -> {
                try {
                    if (tempIdx % 2 == 0) {
                        TimeUnit.SECONDS.sleep(1);
                    } else {
                        TimeUnit.SECONDS.sleep(2);
                    }
                    consumer.acknowledgeAsync(message);
                    counter.incrementAndGet();
                    log.info("ack, messageId:{}", message.getMessageId().toString());
                } catch (Exception ignored) {
                }
            }).start();
        }
        consumer.close();

        assertEquals(counter.get(), numMessages);
    }

    @Test(timeOut = testTimeout)
    public void testCloseBatch() throws PulsarClientException {
        String topic = buildOneTopic();
        String subscriptionName = "my-test-close-consumer";
        final int numMessages = 5;
        AtomicInteger counter = new AtomicInteger(0);

        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName(subscriptionName)
                .subscriptionType(SubscriptionType.Shared)
                .batchReceivePolicy(BatchReceivePolicy.builder()
                        .maxNumMessages(numMessages)
                        .build())
                .closeWaitForJob(CloseWaitForJobPolicy.builder()
                        .closeWaitForJob(true)
                        .build())
                .subscribe();

        producerTopic(topic, numMessages);

        Messages<String> messages = consumer.batchReceive();
        new Thread(() -> {
            try {
                TimeUnit.SECONDS.sleep(2);
                consumer.acknowledge(messages);
                counter.incrementAndGet();
                log.info("ack, messageId:{}", messages.size());
            } catch (Exception ignored) {
            }
        }).start();
        consumer.close();

        assertEquals(counter.get(), 1);
    }


    @Test(timeOut = testTimeout)
    public void testCloseBatchAsync() throws PulsarClientException, ExecutionException, InterruptedException {
        String topic = buildOneTopic();
        String subscriptionName = "my-test-close-consumer";
        final int numMessages = 5;
        AtomicInteger counter = new AtomicInteger(0);

        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName(subscriptionName)
                .subscriptionType(SubscriptionType.Shared)
                .batchReceivePolicy(BatchReceivePolicy.builder()
                        .maxNumMessages(numMessages)
                        .build())
                .closeWaitForJob(CloseWaitForJobPolicy.builder()
                        .closeWaitForJob(true)
                        .build())
                .subscribe();

        producerTopic(topic, numMessages);

        Messages<String> messages = consumer.batchReceiveAsync().get();
        new Thread(() -> {
            try {
                TimeUnit.SECONDS.sleep(2);
                consumer.acknowledge(messages);
                counter.incrementAndGet();
                log.info("ack, messageId:{}", messages.size());
            } catch (Exception ignored) {
            }
        }).start();
        consumer.close();

        assertEquals(counter.get(), 1);
    }


    @Test(timeOut = testTimeout)
    public void testCloseMultiBatch() throws PulsarClientException {
        List<String> listTopic = buildListTopic();
        String subscriptionName = "my-test-close-consumer";
        final int numMessages = 5;
        AtomicInteger counter = new AtomicInteger(0);

        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topics(listTopic)
                .subscriptionName(subscriptionName)
                .subscriptionType(SubscriptionType.Shared)
                .batchReceivePolicy(BatchReceivePolicy.builder()
                        .maxNumMessages(10)
                        .build())
                .closeWaitForJob(CloseWaitForJobPolicy.builder()
                        .closeWaitForJob(true)
                        .build())
                .subscribe();

        for (String topic : listTopic) {
            producerTopic(topic, numMessages);
        }

        Messages<String> messages = consumer.batchReceive();
        log.info("batchReceive size={}", messages.size());
        new Thread(() -> {
            try {
                TimeUnit.SECONDS.sleep(2);
                consumer.acknowledge(messages);
                counter.incrementAndGet();
                log.info("ack, messageId:{}", messages.size());
            } catch (Exception ignored) {
            }
        }).start();
        consumer.close();

        assertEquals(counter.get(), 1);
    }

    @Test(timeOut = testTimeout)
    public void testCloseCumulative() throws PulsarClientException {
        String topic = buildOneTopic();
        String subscriptionName = "my-test-close-consumer-cumulative";
        final int numMessages = 5;
        AtomicInteger counter = new AtomicInteger(0);

        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName(subscriptionName)
                .subscriptionType(SubscriptionType.Exclusive)
                .closeWaitForJob(CloseWaitForJobPolicy.builder()
                        .closeWaitForJob(true)
                        .build())
                .subscribe();

        producerTopic(topic, numMessages);

        for (int i = 0; i < numMessages; i++) {
            Message<String> message = consumer.receive();
            log.info("consumer message:{}, messageId:{}", message.getValue(), message.getMessageId().toString());
            if (i == numMessages - 1) {
                new Thread(() -> {
                    try {
                        TimeUnit.SECONDS.sleep(3);
                        consumer.acknowledgeCumulative(message);
                        counter.incrementAndGet();
                        log.info("ack, messageId:{}", message.getMessageId().toString());
                    } catch (Exception ignored) {
                    }
                }).start();
            }
        }

        consumer.close();

        assertEquals(counter.get(), 1);
    }

    @Test(timeOut = testTimeout)
    public void testCloseNegative() throws PulsarClientException {
        String topic = buildOneTopic();
        String subscriptionName = "my-test-close-consumer-cumulative";
        final int numMessages = 5;
        AtomicInteger counter = new AtomicInteger(0);

        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName(subscriptionName)
                .subscriptionType(SubscriptionType.Shared)
                .ackTimeout(0, TimeUnit.SECONDS)
                .closeWaitForJob(CloseWaitForJobPolicy.builder()
                        .closeWaitForJob(true)
                        .build())
                .subscribe();
        producerTopic(topic, numMessages);

        for (int i = 0; i < numMessages; i++) {

            Message<String> message = consumer.receive();
            log.info("consumer message:{}, messageId:{}", message.getValue(), message.getMessageId().toString());
            int tempIdx = i;
            new Thread(() -> {
                try {
                    if (tempIdx % 2 == 0) {
                        TimeUnit.SECONDS.sleep(1);
                    } else {
                        TimeUnit.SECONDS.sleep(2);
                    }
                    consumer.negativeAcknowledge(message);
                    counter.incrementAndGet();
                    log.info("negative, messageId:{}", message.getMessageId().toString());
                } catch (Exception ignored) {
                }
            }).start();
        }

        consumer.close();

        assertEquals(counter.get(), numMessages);
    }

    @Test()
    public void testCloseMultiNegative() throws PulsarClientException {
        List<String> listTopic = buildListTopic();
        String subscriptionName = "my-test-close-consumer-cumulative";
        final int numMessages = 5;
        AtomicInteger counter = new AtomicInteger(0);

        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topics(listTopic)
                .subscriptionName(subscriptionName)
                .subscriptionType(SubscriptionType.Shared)
                .closeWaitForJob(CloseWaitForJobPolicy.builder()
                        .closeWaitForJob(true)
                        .build())
                .subscribe();

        for (String topic : listTopic) {
            producerTopic(topic, numMessages);
        }

        for (int i = 0; i < numMessages * 2; i++) {

            Message<String> message = consumer.receive();
            log.info("consumer message:{}, messageId:{}", message.getValue(), message.getMessageId().toString());
            int tempIdx = i;
            new Thread(() -> {
                try {
                    if (tempIdx % 2 == 0) {
                        TimeUnit.SECONDS.sleep(1);
                    } else {
                        TimeUnit.SECONDS.sleep(2);
                    }
                    consumer.negativeAcknowledge(message);
                    counter.incrementAndGet();
                    log.info("negative, messageId:{}", message.getMessageId().toString());
                } catch (Exception ignored) {
                }
            }).start();
        }

        consumer.close();

        assertEquals(counter.get(), numMessages * 2);
    }

    @Test(timeOut = testTimeout)
    public void testCloseMultiConsumer() throws PulsarClientException {
        List<String> listTopic = buildListTopic();
        String subscriptionName = "my-test-close-consumer";
        final int numMessages = 5;
        AtomicInteger counter = new AtomicInteger(0);
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topics(listTopic)
                .subscriptionName(subscriptionName)
                .subscriptionType(SubscriptionType.Shared)
                .ackTimeout(0, TimeUnit.SECONDS)
                .closeWaitForJob(CloseWaitForJobPolicy.builder()
                        .closeWaitForJob(true)
                        .build())
                .subscribe();

        for (String topic : listTopic) {
            producerTopic(topic, numMessages);
        }

        for (int i = 0; i < numMessages * 2; i++) {
            Message<String> message = consumer.receive();
            log.info("consumer message:{}, messageId:{}", message.getValue(), message.getMessageId().toString());
            int finalI = i;
            new Thread(() -> {
                try {
                    if (finalI % 2 == 0) {
                        TimeUnit.SECONDS.sleep(1);
                    } else {
                        TimeUnit.SECONDS.sleep(2);
                    }
                    consumer.acknowledge(message);
                    counter.incrementAndGet();
                    log.info("ack, messageId:{}", message.getMessageId().toString());
                } catch (Exception ignored) {
                }
            }).start();
        }

        consumer.close();

        assertEquals(counter.get(), numMessages * 2);
    }

    @Test(timeOut = testTimeout)
    public void testCloseGracefulMultiConsumerAsync()
            throws PulsarClientException, InterruptedException, ExecutionException {
        List<String> listTopic = buildListTopic();
        String subscriptionName = "my-test-close-consumer";
        final int numMessages = 5;
        AtomicInteger counter = new AtomicInteger(0);

        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topics(listTopic)
                .subscriptionName(subscriptionName)
                .subscriptionType(SubscriptionType.Shared)
                .closeWaitForJob(CloseWaitForJobPolicy.builder()
                        .closeWaitForJob(true)
                        .build())
                .subscribe();

        for (String topic : listTopic) {
            producerTopic(topic, numMessages);
        }

        for (int i = 0; i < numMessages * 2; i++) {
            Message<String> message = consumer.receiveAsync().get();
            log.info("consumer message:{}, messageId:{}", message.getValue(), message.getMessageId().toString());
            int tempIdx = i;
            new Thread(() -> {
                try {
                    if (tempIdx % 2 == 0) {
                        TimeUnit.SECONDS.sleep(1);
                    } else {
                        TimeUnit.SECONDS.sleep(2);
                    }
                    consumer.acknowledgeAsync(message);
                    counter.incrementAndGet();
                    log.info("ack, messageId:{}", message.getMessageId().toString());
                } catch (Exception ignored) {
                }
            }).start();
        }

        consumer.close();

        assertEquals(counter.get(), numMessages * 2);
    }


    @Test(timeOut = testTimeout)
    public void testCloseTimeoutMultiConsumer() throws PulsarClientException {
        List<String> listTopic = buildListTopic();
        String subscriptionName = "my-test-close-consumer";
        final int numMessages = 5;
        AtomicInteger counter = new AtomicInteger(0);

        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topics(listTopic)
                .subscriptionName(subscriptionName)
                .subscriptionType(SubscriptionType.Shared)
                .closeWaitForJob(CloseWaitForJobPolicy.builder()
                        .closeWaitForJob(true)
                        .timeout(5, TimeUnit.SECONDS)
                        .build())
                .subscribe();


        for (int i = 0; i < numMessages * 2; i++) {
            Message<String> message = consumer.receive(100, TimeUnit.MILLISECONDS);
            if (message == null) {
                continue;
            }
            log.info("consumer message:{}, messageId:{}", message.getValue(), message.getMessageId().toString());
            int finalI = i;
            new Thread(() -> {
                try {
                    if (finalI % 2 == 0) {
                        TimeUnit.SECONDS.sleep(1);
                    } else {
                        TimeUnit.SECONDS.sleep(2);
                    }
                    consumer.acknowledge(message);
                    counter.incrementAndGet();
                    log.info("ack, messageId:{}", message.getMessageId().toString());
                } catch (Exception ignored) {
                }
            }).start();
        }

        consumer.close();

        assertEquals(counter.get(), 0);
    }

    private String buildOneTopic() {
        return "persistent://public/default/topic1";
    }

    private List<String> buildListTopic() {
        return Arrays.asList("persistent://public/default/topic1", "persistent://public/default/topic2");
    }

    private void producerTopic(String topic, int number) throws PulsarClientException {
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topic)
                .create();
        for (int i = 0; i < number; i++) {
            MessageId hello = producer.send("hello");
            log.info("producer topic:{}, messageId:{}", topic, hello.toString());
        }
        producer.close();
    }


}
