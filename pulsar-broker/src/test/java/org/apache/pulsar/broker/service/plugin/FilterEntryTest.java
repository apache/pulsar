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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
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
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.service.AbstractBaseDispatcher;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.BrokerTestBase;
import org.apache.pulsar.broker.service.Dispatcher;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.nar.NarClassLoader;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker")
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

        String topic = "persistent://prop/ns-abc/topic" + UUID.randomUUID();
        String subName = "sub";
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING).topic(topic)
                .subscriptionName(subName).subscribe();
        // mock entry filters
        PersistentSubscription subscription = (PersistentSubscription) pulsar.getBrokerService()
                .getTopicReference(topic).get().getSubscription(subName);
        Dispatcher dispatcher = subscription.getDispatcher();
        Field field = AbstractBaseDispatcher.class.getDeclaredField("entryFilters");
        field.setAccessible(true);
        NarClassLoader narClassLoader = mock(NarClassLoader.class);
        EntryFilter filter1 = new EntryFilterTest();
        EntryFilterWithClassLoader loader1 = spy(new EntryFilterWithClassLoader(filter1, narClassLoader));
        EntryFilter filter2 = new EntryFilter2Test();
        EntryFilterWithClassLoader loader2 = spy(new EntryFilterWithClassLoader(filter2, narClassLoader));
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

        Map<String, String> map = new HashMap<>();
        map.put("1","1");
        map.put("2","2");
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

}
