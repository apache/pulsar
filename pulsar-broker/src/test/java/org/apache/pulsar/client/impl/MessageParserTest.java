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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.List;
import java.util.concurrent.TimeUnit;

import lombok.Cleanup;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.api.raw.MessageParser;
import org.apache.pulsar.common.api.raw.RawMessage;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class MessageParserTest extends MockedPulsarServiceBaseTest {

    @BeforeClass
    public void setup() throws Exception {
        super.internalSetup();

        admin.clusters().createCluster("test", ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());
        admin.tenants().createTenant("my-tenant",
                new TenantInfoImpl(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("test")));
        admin.namespaces().createNamespace("my-tenant/my-ns", Sets.newHashSet("test"));
    }

    @AfterClass
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

    @DataProvider(name = "batchingAndCompression")
    public static Object[][] batchingAndCompression() {
        return new Object[][] {
                { true, CompressionType.ZLIB },
                { true, CompressionType.ZSTD },
                { true, CompressionType.SNAPPY },
                { true, CompressionType.LZ4 },
                { true, CompressionType.NONE },
                { false, CompressionType.ZLIB },
                { false, CompressionType.ZSTD },
                { false, CompressionType.SNAPPY },
                { false, CompressionType.LZ4 },
                { false, CompressionType.NONE },
        };
    }

    @Test(dataProvider = "batchingAndCompression")
    public void testParseMessages(boolean batchEnabled, CompressionType compressionType) throws Exception{
        final String topic = "persistent://my-tenant/my-ns/message-parse-test-" + batchEnabled + "-" + compressionType;
        final TopicName topicName = TopicName.get(topic);

        final int n = 10;

        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .compressionType(compressionType)
                .enableBatching(batchEnabled)
                .batchingMaxPublishDelay(10, TimeUnit.SECONDS)
                .topic(topic)
                .create();

        ManagedCursor cursor = ((PersistentTopic) pulsar.getBrokerService().getTopicReference(topic).get())
                .getManagedLedger().newNonDurableCursor(PositionImpl.earliest);

        if (batchEnabled) {
            for (int i = 0; i < n - 1; i++) {
                producer.sendAsync("hello-" + i);
            }

            producer.send("hello-" + (n - 1));
        } else {
            for (int i = 0; i < n; i++) {
                producer.send("Pulsar-" + i);
            }
        }

        if (batchEnabled) {
            Entry entry = cursor.readEntriesOrWait(1).get(0);

            List<RawMessage> messages = Lists.newArrayList();
            ByteBuf headsAndPayload = entry.getDataBuffer();

            try {
                MessageParser.parseMessage(topicName, entry.getLedgerId(), entry.getEntryId(), headsAndPayload,
                        messages::add, Commands.DEFAULT_MAX_MESSAGE_SIZE);
            } finally {
                entry.release();
            }

            assertEquals(messages.size(), 10);

            for (int i = 0; i < n; i++) {
                assertEquals(messages.get(i).getData(), Unpooled.wrappedBuffer(("hello-" + i).getBytes()));
            }

            messages.forEach(msg -> {
                msg.getSchemaVersion();
                msg.release();
            });

            Awaitility.await().atMost(3, TimeUnit.SECONDS).untilAsserted(() -> {
                assertEquals(headsAndPayload.refCnt(), 0);
            });

        } else {
            // Read through raw data
            assertEquals(cursor.getNumberOfEntriesInBacklog(false), n);
            List<Entry> entries = cursor.readEntriesOrWait(n);
            assertEquals(entries.size(), n);

            List<ByteBuf> headsAndPayloadList = Lists.newArrayList();
            List<RawMessage> messages = Lists.newArrayList();
            for (Entry entry : entries) {
                ByteBuf headsAndPayload = entry.getDataBuffer();
                headsAndPayloadList.add(headsAndPayload);
                MessageParser.parseMessage(topicName, entry.getLedgerId(), entry.getEntryId(), entry.getDataBuffer(),
                        messages::add, Commands.DEFAULT_MAX_MESSAGE_SIZE);
                entry.release();
            }

            assertEquals(messages.size(), 10);
            messages.forEach(msg -> {
                msg.getSchemaVersion();
                msg.release();
            });

            for (ByteBuf byteBuf : headsAndPayloadList) {
                Awaitility.await().atMost(3, TimeUnit.SECONDS).untilAsserted(() -> {
                    assertEquals(byteBuf.refCnt(), 0);
                });
            }
        }
    }
}