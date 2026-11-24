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

import static org.assertj.core.api.Assertions.assertThat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerArray;
import org.apache.pulsar.broker.service.SharedPulsarBaseTest;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.FutureUtil;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker-api")
public class ListenerExecutorAllocationBrokerTest extends SharedPulsarBaseTest {

    @DataProvider
    public static Object[][] consumersAndPartitions() {
        // Keep an even one-partition pool: the original bug can use all threads in an odd-sized pool.
        return new Object[][] {{3, 1}, {4, 1}, {3, 2}, {4, 2}};
    }

    @Test(dataProvider = "consumersAndPartitions", timeOut = 120000)
    public void testPartitionedConsumersUseAllListenerThreadsAndRemainPinned(int consumerCount, int partitions)
            throws Exception {
        int listenerThreads = consumerCount * partitions;
        String topic = newTopicName();
        admin.topics().createPartitionedTopic(topic, partitions);
        CountDownLatch entered = new CountDownLatch(listenerThreads);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch[] exited = {new CountDownLatch(listenerThreads), new CountDownLatch(listenerThreads)};
        Map<Integer, Thread> threads = new ConcurrentHashMap<>();
        Queue<Throwable> errors = new ConcurrentLinkedQueue<>();
        AtomicIntegerArray seen = new AtomicIntegerArray(listenerThreads * 2);
        List<CompletableFuture<Void>> acknowledgements = new ArrayList<>();
        for (int i = 0; i < listenerThreads * 2; i++) {
            acknowledgements.add(new CompletableFuture<>());
        }

        try (PulsarClient client = PulsarClient.builder()
                .serviceUrl(getBrokerServiceUrl()).listenerThreads(listenerThreads).build()) {
            List<Consumer<Integer>> consumers = new ArrayList<>();
            List<Producer<Integer>> producers = new ArrayList<>();
            try {
                for (int i = 0; i < consumerCount; i++) {
                    int consumerIndex = i;
                    consumers.add(client.newConsumer(Schema.INT32)
                            .topic(topic)
                            .subscriptionName("subscription-" + i)
                            .subscriptionType(SubscriptionType.Exclusive)
                            .messageListener((consumer, message) -> {
                                int deliveryIndex = -1;
                                boolean firstDelivery = false;
                                try {
                                    int round = message.getValue();
                                    assertThat(round).as("Message round").isBetween(0, 1);
                                    int partitionIndex = TopicName.get(message.getTopicName()).getPartitionIndex();
                                    assertThat(partitionIndex).as("Receiving partition").isBetween(0, partitions - 1);
                                    int assignmentIndex = consumerIndex * partitions + partitionIndex;
                                    deliveryIndex = round * listenerThreads + assignmentIndex;
                                    firstDelivery = seen.compareAndSet(deliveryIndex, 0, 1);
                                    if (firstDelivery) {
                                        if (round == 0) {
                                            threads.put(assignmentIndex, Thread.currentThread());
                                            entered.countDown();
                                            assertThat(release.await(30, TimeUnit.SECONDS))
                                                    .as("First-round callbacks released").isTrue();
                                        } else {
                                            assertThat(Thread.currentThread())
                                                    .as("Consumer %s partition %s retains its thread",
                                                            consumerIndex, partitionIndex)
                                                    .isSameAs(threads.get(assignmentIndex));
                                        }
                                    }
                                    CompletableFuture<Void> acknowledged = firstDelivery
                                            ? acknowledgements.get(deliveryIndex) : null;
                                    consumer.acknowledgeAsync(message).whenComplete((ignored, error) -> {
                                        if (error != null) {
                                            errors.add(error);
                                        }
                                        if (acknowledged != null) {
                                            if (error == null) {
                                                acknowledged.complete(null);
                                            } else {
                                                acknowledged.completeExceptionally(error);
                                            }
                                        }
                                    });
                                } catch (Throwable error) {
                                    if (error instanceof InterruptedException) {
                                        Thread.currentThread().interrupt();
                                    }
                                    errors.add(error);
                                    if (firstDelivery) {
                                        acknowledgements.get(deliveryIndex).completeExceptionally(error);
                                    }
                                } finally {
                                    if (firstDelivery) {
                                        exited[deliveryIndex / listenerThreads].countDown();
                                    }
                                }
                            }).subscribe());
                }

                for (int partition = 0; partition < partitions; partition++) {
                    producers.add(client.newProducer(Schema.INT32)
                            .topic(TopicName.get(topic).getPartition(partition).toString())
                            .enableBatching(false).create());
                }
                // Explicit partition producers exercise every internal consumer in both rounds.
                // Separate subscriptions avoid depending on Shared dispatch fairness.
                try {
                    for (Producer<Integer> producer : producers) {
                        producer.send(0);
                    }
                    assertThat(entered.await(10, TimeUnit.SECONDS))
                            .as("All %s callbacks overlap; entered assignments: %s", listenerThreads, threads.keySet())
                            .isTrue();
                    assertThat(new HashSet<>(threads.values()))
                            .as("Distinct listener threads").hasSize(listenerThreads);
                } finally {
                    // Release before closing any resources, including when the concurrency assertion fails.
                    release.countDown();
                }
                FutureUtil.waitForAll(acknowledgements.subList(0, listenerThreads)).get(10, TimeUnit.SECONDS);
                assertThat(exited[0].await(10, TimeUnit.SECONDS)).as("First-round callbacks completed").isTrue();
                assertThat(errors).as("First-round callback errors").isEmpty();
                for (Producer<Integer> producer : producers) {
                    producer.send(1);
                }
                FutureUtil.waitForAll(acknowledgements.subList(listenerThreads, listenerThreads * 2))
                        .get(10, TimeUnit.SECONDS);
                assertThat(exited[1].await(10, TimeUnit.SECONDS)).as("Second-round callbacks completed").isTrue();
                assertThat(errors).as("Callback and acknowledgement errors").isEmpty();
            } finally {
                release.countDown();
                List<CompletableFuture<Void>> closes = new ArrayList<>();
                for (Consumer<Integer> consumer : consumers) {
                    closes.add(consumer.closeAsync());
                }
                for (Producer<Integer> producer : producers) {
                    closes.add(producer.closeAsync());
                }
                FutureUtil.waitForAll(closes).get(10, TimeUnit.SECONDS);
            }
        }
    }
}
