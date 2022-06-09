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
package org.apache.pulsar.broker.service.plugin;

import static org.apache.pulsar.broker.BrokerTestUtil.spyWithClassAndConstructorArgs;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.service.AbstractBaseDispatcher;
import org.apache.pulsar.broker.service.AbstractTopic;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.BrokerTestBase;
import org.apache.pulsar.broker.service.Dispatcher;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.nar.NarClassLoader;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker")
@Slf4j
public class FilterEntryTest extends BrokerTestBase {
    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        baseSetup();
    }

    @AfterMethod
    @Override
    protected void cleanup() throws Exception {
        internalCleanup();
    }

    public void testFilter() throws Exception {
        Map<String, String> map = new HashMap<>();
        map.put("1","1");
        map.put("2","2");
        String topic = "persistent://prop/ns-abc/topic" + UUID.randomUUID();
        String subName = "sub";
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING).topic(topic)
                .subscriptionProperties(map)
                .subscriptionName(subName).subscribe();
        // mock entry filters
        PersistentSubscription subscription = (PersistentSubscription) pulsar.getBrokerService()
                .getTopicReference(topic).get().getSubscription(subName);
        Dispatcher dispatcher = subscription.getDispatcher();
        Field field = AbstractBaseDispatcher.class.getDeclaredField("entryFilters");
        field.setAccessible(true);
        NarClassLoader narClassLoader = mock(NarClassLoader.class);
        EntryFilter filter1 = new EntryFilterTest();
        EntryFilterWithClassLoader loader1 = spyWithClassAndConstructorArgs(EntryFilterWithClassLoader.class, filter1, narClassLoader);
        EntryFilter filter2 = new EntryFilter2Test();
        EntryFilterWithClassLoader loader2 = spyWithClassAndConstructorArgs(EntryFilterWithClassLoader.class, filter2, narClassLoader);
        field.set(dispatcher, ImmutableList.of(loader1, loader2));

        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .enableBatching(false).topic(topic).create();
        for (int i = 0; i < 10; i++) {
            producer.send("test");
        }

        int counter = 0;
        while (true) {
            Message<String> message = consumer.receive(1, TimeUnit.SECONDS);
            if (message != null) {
                counter++;
                consumer.acknowledge(message);
            } else {
                break;
            }
        }
        // All normal messages can be received
        assertEquals(10, counter);
        MessageIdImpl lastMsgId = null;
        for (int i = 0; i < 10; i++) {
            lastMsgId = (MessageIdImpl) producer.newMessage().property("REJECT", "").value("1").send();
        }
        counter = 0;
        while (true) {
            Message<String> message = consumer.receive(1, TimeUnit.SECONDS);
            if (message != null) {
                counter++;
                consumer.acknowledge(message);
            } else {
                break;
            }
        }
        // REJECT messages are filtered out
        assertEquals(0, counter);

        // All messages should be acked, check the MarkDeletedPosition
        assertNotNull(lastMsgId);
        MessageIdImpl finalLastMsgId = lastMsgId;
        Awaitility.await().untilAsserted(() -> {
            PositionImpl position = (PositionImpl) subscription.getCursor().getMarkDeletedPosition();
            assertEquals(position.getLedgerId(), finalLastMsgId.getLedgerId());
            assertEquals(position.getEntryId(), finalLastMsgId.getEntryId());
        });
        consumer.close();

        consumer = pulsarClient.newConsumer(Schema.STRING).topic(topic).subscriptionProperties(map)
                .subscriptionName(subName).subscribe();
        for (int i = 0; i < 10; i++) {
            producer.newMessage().property(String.valueOf(i), String.valueOf(i)).value("1").send();
        }
        counter = 0;
        while (true) {
            Message<String> message = consumer.receive(1, TimeUnit.SECONDS);
            if (message != null) {
                counter++;
                consumer.acknowledge(message);
            } else {
                break;
            }
        }
        assertEquals(2, counter);

        producer.close();
        consumer.close();

        BrokerService brokerService = pulsar.getBrokerService();
        Field field1 = BrokerService.class.getDeclaredField("entryFilters");
        field1.setAccessible(true);
        field1.set(brokerService, ImmutableMap.of("1", loader1, "2", loader2));
        cleanup();
        verify(loader1, times(1)).close();
        verify(loader2, times(1)).close();

    }


    @Test
    public void testFilteredMsgCount() throws Throwable {
        String topic = "persistent://prop/ns-abc/topic" + UUID.randomUUID();
        String subName = "sub";

        try (Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .enableBatching(false).topic(topic).create();
             Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING).topic(topic)
                     .subscriptionName(subName).subscribe()) {

            // mock entry filters
            PersistentSubscription subscription = (PersistentSubscription) pulsar.getBrokerService()
                    .getTopicReference(topic).get().getSubscription(subName);
            Dispatcher dispatcher = subscription.getDispatcher();
            Field field = AbstractBaseDispatcher.class.getDeclaredField("entryFilters");
            field.setAccessible(true);
            NarClassLoader narClassLoader = mock(NarClassLoader.class);
            EntryFilter filter1 = new EntryFilterTest();
            EntryFilterWithClassLoader loader1 = spyWithClassAndConstructorArgs(EntryFilterWithClassLoader.class, filter1, narClassLoader);
            EntryFilter filter2 = new EntryFilter2Test();
            EntryFilterWithClassLoader loader2 = spyWithClassAndConstructorArgs(EntryFilterWithClassLoader.class, filter2, narClassLoader);
            field.set(dispatcher, ImmutableList.of(loader1, loader2));

            for (int i = 0; i < 10; i++) {
                producer.send("test");
            }

            for (int i = 0; i < 10; i++) {
                assertNotNull(producer.newMessage().property("REJECT", "").value("1").send());
            }


            int counter = 0;
            while (true) {
                Message<String> message = consumer.receive(10, TimeUnit.SECONDS);
                if (message != null) {
                    counter++;
                    assertEquals(message.getValue(), "test");
                    consumer.acknowledge(message);
                } else {
                    break;
                }
            }

            assertEquals(10, counter);
            AbstractTopic abstractTopic = (AbstractTopic) subscription.getTopic();
            long filtered = abstractTopic.getFilteredEntriesCount();
            assertEquals(filtered, 10);
        }
    }

    @Test
    public void testEntryFilterRescheduleMessageDependingOnConsumerSharedSubscription() throws Throwable {
        String topic = "persistent://prop/ns-abc/topic" + UUID.randomUUID();
        String subName = "sub";

        Map<String, String> metadataConsumer1 = new HashMap<>();
        metadataConsumer1.put("matchValueAccept", "FOR-1");
        metadataConsumer1.put("matchValueReschedule", "FOR-2");

        Map<String, String> metadataConsumer2 = new HashMap<>();
        metadataConsumer2.put("matchValueAccept", "FOR-2");
        metadataConsumer2.put("matchValueReschedule", "FOR-1");

        try (Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .enableBatching(false).topic(topic).create();
             Consumer<String> consumer1 = pulsarClient.newConsumer(Schema.STRING).topic(topic)
                     .subscriptionType(SubscriptionType.Shared)
                     .properties(metadataConsumer1)
                     .consumerName("consumer1")
                     .receiverQueueSize(5)
                     .subscriptionName(subName)
                     .subscribe();
             Consumer<String> consumer2 = pulsarClient.newConsumer(Schema.STRING)
                     .subscriptionType(SubscriptionType.Shared)
                     .properties(metadataConsumer2)
                     .consumerName("consumer2")
                     .topic(topic)
                     .receiverQueueSize(5)
                     .subscriptionName(subName)
                     .subscribe()) {

            // mock entry filters
            PersistentSubscription subscription = (PersistentSubscription) pulsar.getBrokerService()
                    .getTopicReference(topic).get().getSubscription(subName);
            Dispatcher dispatcher = subscription.getDispatcher();
            Field field = AbstractBaseDispatcher.class.getDeclaredField("entryFilters");
            field.setAccessible(true);
            NarClassLoader narClassLoader = mock(NarClassLoader.class);
            EntryFilter filter1 = new EntryFilterTest();
            EntryFilterWithClassLoader loader1 = spyWithClassAndConstructorArgs(EntryFilterWithClassLoader.class, filter1, narClassLoader);
            EntryFilter filter2 = new EntryFilterTest();
            EntryFilterWithClassLoader loader2 = spyWithClassAndConstructorArgs(EntryFilterWithClassLoader.class, filter2, narClassLoader);
            field.set(dispatcher, ImmutableList.of(loader1, loader2));

            int numMessages = 200;
            for (int i = 0; i < numMessages; i++) {
                if (i % 2 == 0) {
                    producer.newMessage()
                            .property("FOR-1", "")
                            .value("consumer-1")
                            .send();
                } else {
                    producer.newMessage()
                            .property("FOR-2", "")
                            .value("consumer-2")
                            .send();
                }
            }

            CompletableFuture<Void> resultConsume1 = new CompletableFuture<>();
            pulsar.getExecutor().submit(() -> {
                        try {
                            // assert that the consumer1 receive all the messages and that such messages
                            // are for consumer1
                            int counter = 0;
                            while (counter < numMessages / 2) {
                                Message<String> message = consumer1.receive(1, TimeUnit.MINUTES);
                                if (message != null) {
                                    log.info("received1 {} - {}", message.getValue(), message.getProperties());
                                    counter++;
                                    assertEquals("consumer-1", message.getValue());
                                    consumer1.acknowledgeAsync(message);
                                } else {
                                    resultConsume1.completeExceptionally(
                                            new Exception("consumer1 did not receive all the messages"));
                                }
                            }
                            resultConsume1.complete(null);
                        } catch (Throwable err) {
                            resultConsume1.completeExceptionally(err);
                        }
                    });

            CompletableFuture<Void> resultConsume2 = new CompletableFuture<>();
            pulsar.getExecutor().submit(() -> {
                try {
                    // assert that the consumer2 receive all the messages and that such messages
                    // are for consumer2
                    int counter = 0;
                    while (counter < numMessages / 2) {
                        Message<String> message = consumer2.receive(1, TimeUnit.MINUTES);
                        if (message != null) {
                            log.info("received2 {} - {}", message.getValue(), message.getProperties());
                            counter++;
                            assertEquals("consumer-2", message.getValue());
                            consumer2.acknowledgeAsync(message);
                        } else {
                            resultConsume2.completeExceptionally(
                                    new Exception("consumer2 did not receive all the messages"));
                        }
                    }
                    resultConsume2.complete(null);
                } catch (Throwable err) {
                    resultConsume1.completeExceptionally(err);
                }
            });
            resultConsume1.get(1, TimeUnit.MINUTES);
            resultConsume2.get(1, TimeUnit.MINUTES);
        }
    }
}
