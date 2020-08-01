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
package org.apache.pulsar.client.impl;

import static org.testng.Assert.assertEquals;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.netty.buffer.Unpooled;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.api.raw.MessageParser;
import org.apache.pulsar.common.api.raw.RawMessage;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class MessageParserTest extends MockedPulsarServiceBaseTest {

    @BeforeMethod
    @Override
    public void setup() throws Exception {
        super.internalSetup();

        admin.clusters().createCluster("test", new ClusterData(pulsar.getWebServiceAddress()));
        admin.tenants().createTenant("my-tenant",
                new TenantInfo(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("test")));
        admin.namespaces().createNamespace("my-tenant/my-ns", Sets.newHashSet("test"));
    }

    @AfterMethod
    @Override
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testWithoutBatches() throws Exception {
        String topic = "persistent://my-tenant/my-ns/my-topic";
        TopicName topicName = TopicName.get(topic);

        int n = 10;

        try (Producer<String> producer = pulsarClient.newProducer(Schema.STRING).enableBatching(false).topic(topic)
                .create()) {
            for (int i = 0; i < n; i++) {
                producer.send("hello-" + i);
            }
        }

        // Read through raw data
        ManagedCursor cursor = ((PersistentTopic) pulsar.getBrokerService().getTopicReference(topic).get())
                .getManagedLedger().newNonDurableCursor(PositionImpl.earliest);

        for (int i = 0; i < n; i++) {
            Entry entry = cursor.readEntriesOrWait(1).get(0);

            List<RawMessage> messages = Lists.newArrayList();

            try {
                MessageParser.parseMessage(topicName, entry.getLedgerId(), entry.getEntryId(), entry.getDataBuffer(),
                        (message) -> {
                            messages.add(message);
                        }, Commands.DEFAULT_MAX_MESSAGE_SIZE);
            } finally {
                entry.release();
            }

            assertEquals(messages.size(), 1);

            assertEquals(messages.get(0).getData(), Unpooled.wrappedBuffer(("hello-" + i).getBytes()));

            messages.forEach(RawMessage::release);
        }
    }

    @Test
    public void testWithBatches() throws Exception {
        String topic = "persistent://my-tenant/my-ns/my-topic-with-batch";
        TopicName topicName = TopicName.get(topic);

        int n = 10;

        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).enableBatching(true)
                .batchingMaxPublishDelay(10, TimeUnit.SECONDS).topic(topic).create();

        ManagedCursor cursor = ((PersistentTopic) pulsar.getBrokerService().getTopicReference(topic).get())
                .getManagedLedger().newNonDurableCursor(PositionImpl.earliest);

        for (int i = 0; i < n - 1; i++) {
            producer.sendAsync("hello-" + i);
        }

        producer.send("hello-" + (n - 1));

        // Read through raw data
        assertEquals(cursor.getNumberOfEntriesInBacklog(false), 1);
        Entry entry = cursor.readEntriesOrWait(1).get(0);

        List<RawMessage> messages = Lists.newArrayList();

        try {
            MessageParser.parseMessage(topicName, entry.getLedgerId(), entry.getEntryId(), entry.getDataBuffer(),
                    (message) -> {
                        messages.add(message);
                    }, Commands.DEFAULT_MAX_MESSAGE_SIZE);
        } finally {
            entry.release();
        }

        assertEquals(messages.size(), 10);

        for (int i = 0; i < n; i++) {
            assertEquals(messages.get(i).getData(), Unpooled.wrappedBuffer(("hello-" + i).getBytes()));
        }

        messages.forEach(RawMessage::release);

        producer.close();
    }
}
