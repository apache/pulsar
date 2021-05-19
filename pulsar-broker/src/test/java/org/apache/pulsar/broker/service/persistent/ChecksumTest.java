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
package org.apache.pulsar.broker.service.persistent;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.List;

import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.pulsar.broker.service.BrokerTestBase;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.RawMessage;
import org.apache.pulsar.client.api.RawReader;
import org.apache.pulsar.common.protocol.Commands;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.scurrilous.circe.checksum.Crc32cIntChecksum;

import io.netty.buffer.ByteBuf;

@Test(groups = "broker")
public class ChecksumTest extends BrokerTestBase {

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.baseSetup();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void verifyChecksumStoredInManagedLedger() throws Exception {
        final String topicName = "persistent://prop/use/ns-abc/topic0";

        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).create();

        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName).get();

        ManagedLedger ledger = topic.getManagedLedger();
        ManagedCursor cursor = ledger.openCursor("test");

        producer.send("Hello".getBytes());

        List<Entry> entries = cursor.readEntriesOrWait(1);
        assertEquals(entries.size(), 1);

        ByteBuf b = entries.get(0).getDataBuffer();

        assertTrue(Commands.hasChecksum(b));
        int parsedChecksum = Commands.readChecksum(b);
        int computedChecksum = Crc32cIntChecksum.computeChecksum(b);
        assertEquals(parsedChecksum, computedChecksum);

        entries.get(0).release();
        producer.close();
    }

    @Test
    public void verifyChecksumSentToConsumer() throws Exception {
        final String topicName = "persistent://prop/use/ns-abc/topic-1";

        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).create();
        RawReader reader = RawReader.create(pulsarClient, topicName, "sub").get();

        producer.send("Hello".getBytes());

        RawMessage msg = reader.readNextAsync().get();

        ByteBuf b = msg.getHeadersAndPayload();
        assertTrue(Commands.hasChecksum(b));
        int parsedChecksum = Commands.readChecksum(b);
        int computedChecksum = Crc32cIntChecksum.computeChecksum(b);
        assertEquals(parsedChecksum, computedChecksum);

        producer.close();
        reader.closeAsync().get();
    }

}
