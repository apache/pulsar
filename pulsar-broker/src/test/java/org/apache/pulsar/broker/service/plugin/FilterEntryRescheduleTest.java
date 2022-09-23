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
import static org.testng.AssertJUnit.assertEquals;
import com.google.common.collect.ImmutableList;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.service.BrokerTestBase;
import org.apache.pulsar.broker.service.Dispatcher;
import org.apache.pulsar.broker.service.EntryFilterSupport;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.nar.NarClassLoader;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "flaky")
@Slf4j
public class FilterEntryRescheduleTest extends BrokerTestBase {
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
        final int numMessages = 200;

        try (Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .enableBatching(false).topic(topic).create();
             Consumer<String> consumer1 = pulsarClient.newConsumer(Schema.STRING).topic(topic)
                     .subscriptionType(SubscriptionType.Shared)
                     .properties(metadataConsumer1)
                     .consumerName("consumer1")
                     .receiverQueueSize(numMessages / 2)
                     .subscriptionName(subName)
                     .subscribe();
             Consumer<String> consumer2 = pulsarClient.newConsumer(Schema.STRING)
                     .subscriptionType(SubscriptionType.Shared)
                     .properties(metadataConsumer2)
                     .consumerName("consumer2")
                     .topic(topic)
                     .receiverQueueSize(numMessages / 2)
                     .subscriptionName(subName)
                     .subscribe()) {

            // mock entry filters
            PersistentSubscription subscription = (PersistentSubscription) pulsar.getBrokerService()
                    .getTopicReference(topic).get().getSubscription(subName);
            Dispatcher dispatcher = subscription.getDispatcher();
            Field field = EntryFilterSupport.class.getDeclaredField("entryFilters");
            field.setAccessible(true);
            NarClassLoader narClassLoader = mock(NarClassLoader.class);
            EntryFilter filter1 = new EntryFilterTest();
            EntryFilterWithClassLoader loader1 =
                    spyWithClassAndConstructorArgs(EntryFilterWithClassLoader.class, filter1, narClassLoader);
            EntryFilter filter2 = new EntryFilterTest();
            EntryFilterWithClassLoader loader2 =
                    spyWithClassAndConstructorArgs(EntryFilterWithClassLoader.class, filter2, narClassLoader);
            field.set(dispatcher, ImmutableList.of(loader1, loader2));

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
                    resultConsume2.completeExceptionally(err);
                }
            });
            resultConsume1.get(1, TimeUnit.MINUTES);
            resultConsume2.get(1, TimeUnit.MINUTES);
        }
    }
}
