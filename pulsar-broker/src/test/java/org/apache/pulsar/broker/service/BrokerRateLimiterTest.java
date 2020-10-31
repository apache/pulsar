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

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageListener;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

public class BrokerRateLimiterTest extends BrokerTestBase {
    @BeforeMethod
    @Override
    protected void setup() throws Exception {

    }

    @AfterMethod
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test(timeOut = 40000)
    public void testBrokerConsumeThreshold() throws Exception {
        testSimpleConsumerThrottling(false);
        super.internalCleanup();
        Thread.sleep(500);

        testSimpleConsumerThrottling(true);
    }

    /**
     * 1.Whether the rate limiter at the broker level is effective.
     * 2.Update the parameter value of the rate limiter through zk to see if it takes effect.
     * 3.Test disabled the rate limiter.
     */
    public void testSimpleConsumerThrottling(boolean isPartitioned) throws Exception {
        conf.setBrokerDispatchThrottlingRateInMessages(10);
        conf.setBrokerDispatchThrottlingRateInBytes(1024 * 1024 * 1024);
        conf.setBrokerDispatchThrottlingRateTimeMillis(1);
        conf.setDispatchThrottlingOnNonBacklogConsumerEnabled(true);
        super.baseSetup();

        final String topicName = "persistent://prop/ns-abc/testSingleConsumer" + UUID.randomUUID();
        if (isPartitioned) {
            admin.topics().createPartitionedTopic(topicName, 3);
        }
        ConsumeRateLimiterImpl consumeRateLimiter = (ConsumeRateLimiterImpl) pulsar
                .getBrokerService().getBrokerDispatchRateLimiter();
        //close reset monitor first, avoid influence
        closeRestMonitor(consumeRateLimiter);
        //setup consumer and producer
        Producer producer = pulsarClient.newProducer().topic(topicName)
                .enableBatching(false)
                .create();
        Consumer consumer = pulsarClient.newConsumer().topic(topicName)
                .receiverQueueSize(1)
                .ackTimeout(1000, TimeUnit.MILLISECONDS)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscriptionName("my-sub").subscribe();
        // produce and consume some messages
        int msgNum = 100;
        for (int i = 0; i < msgNum; i++) {
            producer.newMessage().value(new byte[10]).send();
        }
        Message message;
        int count = 0;
        while (true) {
            message = consumer.receive(100, TimeUnit.MILLISECONDS);
            if (message == null) {
                break;
            }
            count++;
            if (count > 10) {
                Thread.sleep(10);
            }
        }
        // The max-messages threshold was reached, so no more messages can be received
        Assert.assertTrue(count < msgNum);
        Assert.assertTrue(consumeRateLimiter.isConsumeRateExceeded());
        message = consumer.receive(100, TimeUnit.MILLISECONDS);
        Assert.assertNull(message);
        Assert.assertTrue(getLongAdder(consumeRateLimiter, "currentDispatchRateOnMessage").sum()
                >= getProperty(consumeRateLimiter, "maxMsgRate"));

        // update max byte to 1
        admin.brokers().updateDynamicConfiguration("brokerDispatchThrottlingRateInBytes", "1");
        Thread.sleep(1000);
        // The max-byte threshold was reached
        count = 0;
        while (consumer.receive(100, TimeUnit.MILLISECONDS) != null) {
            Thread.sleep(100);
            count++;
        }
        Assert.assertTrue(count < 5);
        Assert.assertTrue(getLongAdder(consumeRateLimiter, "currentDispatchRateOnMessage").sum()
                < getProperty(consumeRateLimiter, "maxMsgRate"));
        Assert.assertTrue(getLongAdder(consumeRateLimiter, "currentDispatchRateOnByte").sum()
                >= getProperty(consumeRateLimiter, "maxByteRate"));

        //unlimited，so we can consume all the messages
        admin.brokers().updateDynamicConfiguration("brokerDispatchThrottlingRateInMessages", "0");
        admin.brokers().updateDynamicConfiguration("brokerDispatchThrottlingRateInBytes", "0");
        Thread.sleep(1000);
        Assert.assertFalse(consumeRateLimiter.isConsumeRateExceeded());
        count = 0;
        while (true) {
            message = consumer.receive(2000, TimeUnit.MILLISECONDS);
            if (message == null) {
                break;
            }
            consumer.acknowledge(message);
            count++;
        }
        Assert.assertEquals(count, msgNum);

        producer.close();
        consumer.close();
    }

    /**
     * enable broker-level and topic-level rate limiter, broker-level will have higher priority.
     */
    @Test(timeOut = 40000)
    public void testMultiRateLimiter() throws Exception{
        doTestMultiRateLimiter(true);
        super.internalCleanup();
        Thread.sleep(500);
        doTestMultiRateLimiter(false);
    }

    private void doTestMultiRateLimiter(boolean isPartitioned) throws Exception {
        conf.setBrokerDispatchThrottlingRateInMessages(30);
        conf.setBrokerDispatchThrottlingRateInBytes(1024 * 1024 * 1024);
        conf.setBrokerDispatchThrottlingRateTimeMillis(1);
        conf.setDispatchThrottlingOnNonBacklogConsumerEnabled(true);
        conf.setDispatchThrottlingRatePerTopicInMsg(100);
        super.baseSetup();

        final String topic1 = "persistent://prop/ns-abc/testConsume-" + UUID.randomUUID();
        final String topic2 = "persistent://prop/ns-abc/testConsume-" + UUID.randomUUID();
        List<String> topicList = Arrays.asList(topic1, topic2);
        if (isPartitioned) {
            admin.topics().createPartitionedTopic(topic1, 3);
            admin.topics().createPartitionedTopic(topic2, 3);
        }
        ConsumeRateLimiterImpl consumeRateLimiter = (ConsumeRateLimiterImpl) pulsar
                .getBrokerService().getBrokerDispatchRateLimiter();
        //close reset monitor first, avoid influence
        closeRestMonitor(consumeRateLimiter);

        //setup consumer and producer
        List<Producer> producers = new ArrayList<>();
        for (String tp : topicList) {
            Producer producer = pulsarClient.newProducer().topic(tp)
                    .enableBatching(false)
                    .create();
            producers.add(producer);
        }
        List<Consumer> consumers = new ArrayList<>();
        AtomicInteger counter = new AtomicInteger(0);
        for (String tp : topicList) {
            Consumer consumer = pulsarClient.newConsumer().topic(tp)
                    .receiverQueueSize(1)
                    .ackTimeout(1000, TimeUnit.MILLISECONDS)
                    .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                    .messageListener((MessageListener<byte[]>) (consumer1, msg) -> {
                        if (msg != null) {
                            counter.getAndIncrement();
                        }
                    })
                    .subscriptionName("my-sub").subscribe();
            consumers.add(consumer);
        }
        // send some messages
        int messageNum = 200;
        CountDownLatch countDownLatch = new CountDownLatch(messageNum);
        for (Producer producer : producers) {
            for (int j = 0; j < messageNum; j++) {
                producer.newMessage().value(new byte[10]).sendAsync()
                        .whenComplete((res, e) -> countDownLatch.countDown());
            }
        }
        countDownLatch.await();
        // give consumers some time to consume
        Thread.sleep(500);
        Assert.assertTrue(counter.get() < messageNum);
        Assert.assertTrue(consumeRateLimiter.isConsumeRateExceeded());
        Assert.assertTrue(getLongAdder(consumeRateLimiter, "currentDispatchRateOnMessage").sum()
                >= getProperty(consumeRateLimiter, "maxMsgRate"));
    }

    private void closeRestMonitor(ConsumeRateLimiterImpl consumeRateLimiter) throws Exception {
        Field monitorField = ConsumeRateLimiterImpl.class.getSuperclass().getDeclaredField("resetRateMonitor");
        monitorField.setAccessible(true);
        ScheduledThreadPoolExecutor monitor = (ScheduledThreadPoolExecutor) monitorField.get(consumeRateLimiter);
        monitor.shutdownNow();
    }

    private LongAdder getLongAdder(ConsumeRateLimiterImpl consumeRateLimiter, String propertyName) throws Exception {
        Field field = ConsumeRateLimiterImpl.class.getDeclaredField(propertyName);
        field.setAccessible(true);
        return (LongAdder) field.get(consumeRateLimiter);
    }

    private long getProperty(ConsumeRateLimiterImpl consumeRateLimiter, String propertyName) throws Exception {
        Field field = ConsumeRateLimiterImpl.class.getSuperclass().getDeclaredField(propertyName);
        field.setAccessible(true);
        return (long) field.get(consumeRateLimiter);
    }

}
