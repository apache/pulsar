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
import static org.testng.Assert.assertTrue;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import lombok.Cleanup;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.MessageListener;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.naming.TopicName;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Multi-topics consumer tests that each run on a broker of their own. {@link TopicsConsumerImplTest} has the tests
 * that share one. {@code testResubscribeSameTopic} names a topic without a namespace, which resolves to
 * {@code public/default}. {@code testSeekToNewerPosition} seeks by publish time and expects its four messages in four
 * milliseconds and nothing delivered across the seek, which held on brokers of their own (no failure in CI) but not
 * on the shared broker. {@code testSubscriptionMustCompleteWhenOperationTimeoutOnMultipleTopics} needs its subscribes
 * to outlast a 2 ms operation timeout, which they do on a new broker and not on the warm shared one.
 */
@Test(groups = "broker-impl")
public class TopicsConsumerImplOwnBrokerTest extends ProducerConsumerBase {

    @BeforeMethod
    @Override
    public void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testResubscribeSameTopic() throws Exception {
        final String localTopicName = "TopicsConsumerResubscribeSameTopicTest";
        final String localPartitionName = localTopicName + "-partition-0";
        final String topicNameWithNamespace = "public/default/" + localTopicName;
        final String topicNameWithDomain = "persistent://" + topicNameWithNamespace;

        admin.topics().createPartitionedTopic(localTopicName, 2);

        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(localTopicName)
                .subscriptionName("SubscriptionName")
                .subscribe();

        assertTrue(consumer instanceof MultiTopicsConsumerImpl);
        MultiTopicsConsumerImpl<byte[]> multiTopicsConsumer = (MultiTopicsConsumerImpl<byte[]>) consumer;

        multiTopicsConsumer.subscribeAsync(topicNameWithNamespace, false).handle((res, exception) -> {
            assertTrue(exception instanceof PulsarClientException.AlreadyClosedException);
            assertEquals(exception.getMessage(), "Already subscribed to " + topicNameWithNamespace);
            return null;
        }).get();
        multiTopicsConsumer.subscribeAsync(topicNameWithDomain, false).handle((res, exception) -> {
            assertTrue(exception instanceof PulsarClientException.AlreadyClosedException);
            assertEquals(exception.getMessage(), "Already subscribed to " + topicNameWithDomain);
            return null;
        }).get();
        multiTopicsConsumer.subscribeAsync(localPartitionName, false).handle((res, exception) -> {
            assertTrue(exception instanceof PulsarClientException.AlreadyClosedException);
            assertEquals(exception.getMessage(), "Already subscribed to " + localPartitionName);
            return null;
        }).get();

        consumer.unsubscribe();
        consumer.close();
    }

    @DataProvider
    public static Object[][] seekByFunction() {
        return new Object[][] {
                { true }, { false }
        };
    }

    @Test(timeOut = 30000, dataProvider = "seekByFunction")
    public void testSeekToNewerPosition(boolean seekByFunction) throws Exception {
        final var topic1 = TopicName.get(newTopicName()).toString()
                .replace("my-property", "public").replace("my-ns", "default");
        final var topic2 = TopicName.get(newTopicName()).toString()
                .replace("my-property", "public").replace("my-ns", "default");
        @Cleanup final var producer1 = pulsarClient.newProducer(Schema.STRING).topic(topic1).create();
        @Cleanup final var producer2 = pulsarClient.newProducer(Schema.STRING).topic(topic2).create();
        producer1.send("1-0");
        producer2.send("2-0");
        producer1.send("1-1");
        producer2.send("2-1");
        final var consumer1 = pulsarClient.newConsumer(Schema.STRING)
                .topics(Arrays.asList(topic1, topic2)).subscriptionName("sub")
                .ackTimeout(1, TimeUnit.SECONDS)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest).subscribe();
        final var timestamps = new ArrayList<Long>();
        for (int i = 0; i < 4; i++) {
            timestamps.add(consumer1.receive().getPublishTime());
        }
        timestamps.sort(Comparator.naturalOrder());
        final var timestamp = timestamps.get(2);
        consumer1.close();

        final Function<Consumer<String>, CompletableFuture<Void>> seekAsync = consumer -> {
            final var future = seekByFunction ? consumer.seekAsync(__ -> timestamp) : consumer.seekAsync(timestamp);
            assertEquals(((ConsumerBase<String>) consumer).getIncomingMessageSize(), 0L);
            assertEquals(((ConsumerBase<String>) consumer).getTotalIncomingMessages(), 0);
            assertTrue(((ConsumerBase<String>) consumer).getUnAckedMessageTracker().isEmpty());
            return future;
        };

        @Cleanup final var consumer2 = pulsarClient.newConsumer(Schema.STRING)
                .topics(Arrays.asList(topic1, topic2)).subscriptionName("sub-2")
                .ackTimeout(1, TimeUnit.SECONDS)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest).subscribe();
        seekAsync.apply(consumer2).get();
        final var values = new TreeSet<String>();
        for (int i = 0; i < 2; i++) {
            values.add(consumer2.receive().getValue());
        }
        assertEquals(values, new TreeSet<>(Arrays.asList("1-1", "2-1")));

        final var valuesInListener = new CopyOnWriteArrayList<String>();
        @Cleanup final var consumer3 = pulsarClient.newConsumer(Schema.STRING)
                .topics(Arrays.asList(topic1, topic2)).subscriptionName("sub-3")
                .messageListener((MessageListener<String>) (__, msg) -> valuesInListener.add(msg.getValue()))
                .ackTimeout(1, TimeUnit.SECONDS)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest).subscribe();
        seekAsync.apply(consumer3).get();
        if (valuesInListener.isEmpty()) {
            Awaitility.await().untilAsserted(() -> assertEquals(valuesInListener.size(), 2));
            assertEquals(valuesInListener.stream().sorted().toList(), Arrays.asList("1-1", "2-1"));
        } // else: consumer3 has passed messages to the listener before seek, in this case we cannot assume anything

        @Cleanup final var consumer4 = pulsarClient.newConsumer(Schema.STRING)
                .topics(Arrays.asList(topic1, topic2)).subscriptionName("sub-4")
                .ackTimeout(1, TimeUnit.SECONDS)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest).subscribe();
        seekAsync.apply(consumer4).get();
        final var valuesInReceiveAsync = new ArrayList<String>();
        valuesInReceiveAsync.add(consumer4.receiveAsync().get().getValue());
        valuesInReceiveAsync.add(consumer4.receiveAsync().get().getValue());
        assertEquals(valuesInReceiveAsync.stream().sorted().toList(), Arrays.asList("1-1", "2-1"));
    }

    @Test(timeOut = 90000)
    public void testSubscriptionMustCompleteWhenOperationTimeoutOnMultipleTopics() throws PulsarClientException {
        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(lookupUrl.toString())
                .ioThreads(2)
                .listenerThreads(3)
                // Below line: Set this very small so the operation timeout can be triggered
                .operationTimeout(2, TimeUnit.MILLISECONDS)
                .build();

        String topic0 = "public/default/topic0";
        String topic1 = "public/default/topic1";

        for (int i = 0; i < 10; i++) {
            try {
                client.newConsumer(Schema.STRING)
                        .subscriptionName("subName")
                        .topics(Lists.newArrayList(topic0, topic1))
                        .receiverQueueSize(2)
                        .subscriptionType(SubscriptionType.Shared)
                        .ackTimeout(365, TimeUnit.DAYS)
                        .ackTimeoutTickTime(36, TimeUnit.DAYS)
                        .acknowledgmentGroupTime(0, TimeUnit.MILLISECONDS)
                        .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                        .subscribe();
                Thread.sleep(3000);
            } catch (Exception ex) {
                Assert.assertTrue(ex instanceof PulsarClientException.TimeoutException);
            }
        }
    }
}
