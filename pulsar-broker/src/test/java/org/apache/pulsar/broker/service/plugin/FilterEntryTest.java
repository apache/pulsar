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
import static org.apache.pulsar.client.api.SubscriptionInitialPosition.Earliest;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.service.AbstractTopic;
import org.apache.pulsar.broker.service.BrokerTestBase;
import org.apache.pulsar.broker.service.Dispatcher;
import org.apache.pulsar.broker.service.EntryFilterSupport;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.nar.NarClassLoader;
import org.apache.pulsar.common.stats.AnalyzeSubscriptionBacklogResult;
import org.awaitility.Awaitility;
import org.testng.Assert;
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

    @Test
    public void testOverride() throws Exception {
        conf.setAllowOverrideEntryFilters(true);
        String topic = "persistent://prop/ns-abc/topic" + UUID.randomUUID();
        String subName = "sub";
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .enableBatching(false).topic(topic).create();
        for (int i = 0; i < 10; i++) {
            producer.send("test");
        }

        PersistentTopic topicRef = (PersistentTopic) pulsar.getBrokerService()
                .getTopicReference(topic).get();

        // set topic level entry filters
        EntryFilterWithClassLoader mockFilter = mock(EntryFilterWithClassLoader.class);
        when(mockFilter.filterEntry(any(Entry.class), any(FilterContext.class))).thenReturn(
                EntryFilter.FilterResult.REJECT);
        ImmutableMap<String, EntryFilterWithClassLoader> entryFilters = ImmutableMap.of("key", mockFilter);

        Field field = topicRef.getClass().getSuperclass().getDeclaredField("entryFilters");
        field.setAccessible(true);
        field.set(topicRef, entryFilters);

        EntryFilterWithClassLoader mockFilter1 = mock(EntryFilterWithClassLoader.class);
        when(mockFilter1.filterEntry(any(Entry.class), any(FilterContext.class))).thenReturn(
                EntryFilter.FilterResult.ACCEPT);
        ImmutableMap<String, EntryFilterWithClassLoader> entryFilters1 = ImmutableMap.of("key2", mockFilter1);
        Field field2 = pulsar.getBrokerService().getClass().getDeclaredField("entryFilters");
        field2.setAccessible(true);
        field2.set(pulsar.getBrokerService(), entryFilters1);

        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING).topic(topic)
                .subscriptionInitialPosition(Earliest)
                .subscriptionName(subName).subscribe();

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
        assertEquals(0, counter);


        conf.setAllowOverrideEntryFilters(false);
        consumer.close();
        consumer = pulsarClient.newConsumer(Schema.STRING).topic(topic)
                .subscriptionInitialPosition(Earliest)
                .subscriptionName(subName + "1").subscribe();
        int counter1 = 0;
        while (true) {
            Message<String> message = consumer.receive(1, TimeUnit.SECONDS);
            if (message != null) {
                counter1++;
                consumer.acknowledge(message);
            } else {
                break;
            }
        }
        // All normal messages can be received
        assertEquals(10, counter1);
        conf.setAllowOverrideEntryFilters(false);
        consumer.close();
    }

    @Test
    public void testFilter() throws Exception {
        Map<String, String> map = new HashMap<>();
        map.put("1","1");
        map.put("2","2");
        String topic = "persistent://prop/ns-abc/topic" + UUID.randomUUID();
        String subName = "sub";
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING).topic(topic)
                .subscriptionProperties(map)
                .isAckReceiptEnabled(true)
                .subscriptionName(subName).subscribe();
        // mock entry filters
        PersistentSubscription subscription = (PersistentSubscription) pulsar.getBrokerService()
                .getTopicReference(topic).get().getSubscription(subName);
        Dispatcher dispatcher = subscription.getDispatcher();
        Field field = EntryFilterSupport.class.getDeclaredField("entryFilters");
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

        verifyBacklog(topic, subName, 10, 10, 10, 10, 0, 0, 0, 0);

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

        verifyBacklog(topic, subName, 0, 0, 0, 0, 0, 0, 0, 0);

        // stop the consumer
        consumer.close();

        MessageIdImpl lastMsgId = null;
        for (int i = 0; i < 10; i++) {
            lastMsgId = (MessageIdImpl) producer.newMessage().property("REJECT", "").value("1").send();
        }

        // analyze the subscription and predict that
        // 10 messages will be rejected by the filter
        verifyBacklog(topic, subName, 10, 10, 0, 0, 10, 10, 0, 0);

        consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .isAckReceiptEnabled(true)
                .subscriptionProperties(map)
                .subscriptionName(subName)
                .subscribe();

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

        // now the Filter acknoledged the messages on behalf of the Consumer
        // backlog is now zero again
        verifyBacklog(topic, subName, 0, 0, 0, 0, 0, 0, 0, 0);

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

        PersistentTopic topicRef = (PersistentTopic) pulsar.getBrokerService()
                .getTopicReference(topic).get();
        Field field1 = topicRef.getClass().getSuperclass().getDeclaredField("entryFilters");
        field1.setAccessible(true);
        field1.set(topicRef, ImmutableMap.of("1", loader1, "2", loader2));

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
            Field field = EntryFilterSupport.class.getDeclaredField("entryFilters");
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

    private void verifyBacklog(String topic, String subscription,
                               int numEntries, int numMessages,
                               int numEntriesAccepted, int numMessagesAccepted,
                               int numEntriesRejected, int numMessagesRejected,
                               int numEntriesRescheduled, int numMessagesRescheduled
                               ) throws Exception {
        AnalyzeSubscriptionBacklogResult a1
                = admin.topics().analyzeSubscriptionBacklog(topic, subscription, Optional.empty());

        Assert.assertEquals(numEntries, a1.getEntries());
        Assert.assertEquals(numEntriesAccepted, a1.getFilterAcceptedEntries());
        Assert.assertEquals(numEntriesRejected, a1.getFilterRejectedEntries());
        Assert.assertEquals(numEntriesRescheduled, a1.getFilterRescheduledEntries());

        Assert.assertEquals(numMessages, a1.getMessages());
        Assert.assertEquals(numMessagesAccepted, a1.getFilterAcceptedMessages());
        Assert.assertEquals(numMessagesRejected, a1.getFilterRejectedMessages());
        Assert.assertEquals(numMessagesRescheduled, a1.getFilterRescheduledMessages());
    }
}
