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

import static org.testng.Assert.assertTrue;
import com.google.common.util.concurrent.Uninterruptibles;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Cleanup;
import org.apache.pulsar.client.util.ExecutorProvider;
import org.apache.pulsar.common.naming.TopicName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "broker-api")
public class MessageListenerExecutorTest extends ProducerConsumerBase {
    private static final Logger log = LoggerFactory.getLogger(MessageListenerExecutorTest.class);

    @BeforeClass(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Override
    protected void customizeNewPulsarClientBuilder(ClientBuilder clientBuilder) {
        // Set listenerThreads to 1 to reproduce the pr more easily in #22861
        clientBuilder.listenerThreads(1);
    }

    @Test
    public void testConsumerMessageListenerExecutorIsolation() throws Exception {
        log.info("-- Starting {} test --", methodName);

        @Cleanup("shutdownNow")
        ExecutorService executor = Executors.newCachedThreadPool();
        List<CompletableFuture<Long>> maxConsumeDelayWithDisableIsolationFutures = new ArrayList<>();
        int loops = 5;
        long consumeSleepTimeMs = 10000;
        for (int i = 0; i < loops; i++) {
            // The first consumer will consume messages with sleep block 1s,
            // and the others will consume messages without sleep block.
            // The maxConsumeDelayWithDisableIsolation of all consumers
            // should be greater than sleepTimeMs cause by disable MessageListenerExecutor.
            CompletableFuture<Long> maxConsumeDelayFuture = startConsumeAndComputeMaxConsumeDelay(
                    "persistent://my-property/my-ns/testConsumerMessageListenerDisableIsolation-" + i,
                    "my-sub-testConsumerMessageListenerDisableIsolation-" + i,
                    i == 0 ? Duration.ofMillis(consumeSleepTimeMs) : Duration.ofMillis(0),
                    false,
                    executor);
            maxConsumeDelayWithDisableIsolationFutures.add(maxConsumeDelayFuture);
        }

        // ensure all consumers consume messages delay more than consumeSleepTimeMs
        boolean allDelayMoreThanConsumeSleepTimeMs = maxConsumeDelayWithDisableIsolationFutures.stream()
                .map(CompletableFuture::join)
                .allMatch(delay -> delay > consumeSleepTimeMs);
        assertTrue(allDelayMoreThanConsumeSleepTimeMs);

        List<CompletableFuture<Long>> maxConsumeDelayWhitEnableIsolationFutures = new ArrayList<>();
        for (int i = 0; i < loops; i++) {
            // The first consumer will consume messages with sleep block 1s,
            // and the others will consume messages without sleep block.
            // The maxConsumeDelayWhitEnableIsolation of the first consumer
            // should be greater than sleepTimeMs, and the others should be
            // less than sleepTimeMs, cause by enable MessageListenerExecutor.
            CompletableFuture<Long> maxConsumeDelayFuture = startConsumeAndComputeMaxConsumeDelay(
                    "persistent://my-property/my-ns/testConsumerMessageListenerEnableIsolation-" + i,
                    "my-sub-testConsumerMessageListenerEnableIsolation-" + i,
                    i == 0 ? Duration.ofMillis(consumeSleepTimeMs) : Duration.ofMillis(0),
                    true,
                    executor);
            maxConsumeDelayWhitEnableIsolationFutures.add(maxConsumeDelayFuture);
        }

        assertTrue(maxConsumeDelayWhitEnableIsolationFutures.get(0).join() > consumeSleepTimeMs);
        boolean remainingAlmostNoDelay = maxConsumeDelayWhitEnableIsolationFutures.stream()
                .skip(1)
                .map(CompletableFuture::join)
                .allMatch(delay -> delay < 1000);
        assertTrue(remainingAlmostNoDelay);

        log.info("-- Exiting {} test --", methodName);
    }

    private CompletableFuture<Long> startConsumeAndComputeMaxConsumeDelay(String topic, String subscriptionName,
                                                                          Duration consumeSleepTime,
                                                                          boolean enableMessageListenerExecutorIsolation,
                                                                          ExecutorService executorService)
            throws Exception {
        int numMessages = 2;
        final CountDownLatch latch = new CountDownLatch(numMessages);
        int numPartitions = 50;
        TopicName nonIsolationTopicName = TopicName.get(topic);
        admin.topics().createPartitionedTopic(nonIsolationTopicName.toString(), numPartitions);

        AtomicLong maxConsumeDelay = new AtomicLong(-1);
        ConsumerBuilder<Long> consumerBuilder =
                pulsarClient.newConsumer(Schema.INT64)
                        .topic(nonIsolationTopicName.toString())
                        .subscriptionName(subscriptionName)
                        .messageListener((c1, msg) -> {
                            Assert.assertNotNull(msg, "Message cannot be null");
                            log.debug("Received message [{}] in the listener", msg.getValue());
                            c1.acknowledgeAsync(msg);
                            maxConsumeDelay.set(Math.max(maxConsumeDelay.get(),
                                    System.currentTimeMillis() - msg.getValue()));
                            if (consumeSleepTime.toMillis() > 0) {
                                Uninterruptibles.sleepUninterruptibly(consumeSleepTime);
                            }
                            latch.countDown();
                        });

        ExecutorService executor = Executors.newSingleThreadExecutor(
                new ExecutorProvider.ExtendedThreadFactory(subscriptionName + "listener-executor-", true));
        if (enableMessageListenerExecutorIsolation) {
            consumerBuilder.messageListenerExecutor((message, runnable) -> executor.execute(runnable));
        }

        Consumer<Long> consumer = consumerBuilder.subscribe();
        ProducerBuilder<Long> producerBuilder = pulsarClient.newProducer(Schema.INT64)
                .topic(nonIsolationTopicName.toString());

        Producer<Long> producer = producerBuilder.create();
        List<Future<MessageId>> futures = new ArrayList<>();

        // Asynchronously produce messages
        for (int i = 0; i < numMessages; i++) {
            Future<MessageId> future = producer.sendAsync(System.currentTimeMillis());
            futures.add(future);
        }

        log.info("Waiting for async publish to complete");
        for (Future<MessageId> future : futures) {
            future.get();
        }

        CompletableFuture<Long> maxDelayFuture = new CompletableFuture<>();

        CompletableFuture.runAsync(() -> {
            try {
                latch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }, executorService).whenCompleteAsync((v, ex) -> {
            maxDelayFuture.complete(maxConsumeDelay.get());
            try {
                producer.close();
                consumer.close();
                executor.shutdownNow();
            } catch (PulsarClientException e) {
                throw new RuntimeException(e);
            }
        });

        return maxDelayFuture;
    }
}
