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
package org.apache.pulsar.broker.service;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.pulsar.client.api.Range;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.protocol.Commands;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class StickyKeyHashTest {
    private static final String[] KEYS = {"", "device-123456", "käyttäjä-123456", "漢字", "emoji-🚀", "a\ud800b"};

    @DataProvider
    public static Object[][] selectors() {
        return new Object[][]{
                {new ConsistentHashingStickyKeyConsumerSelector(100)},
                {new HashRangeAutoSplitStickyKeyConsumerSelector()},
                {new HashRangeExclusiveStickyKeyConsumerSelector()},
                {new EntryBucketConsumerSelector(List.of(Range.of(0x0000, 0x3FFF), Range.of(0x4000, 0x7FFF),
                        Range.of(0x8000, 0xBFFF), Range.of(0xC000, 0xFFFF)))},
        };
    }

    @Test(dataProvider = "selectors")
    public void testStringKeyHashMatchesItsUtf8BytesHash(StickyKeyConsumerSelector selector) {
        for (String key : KEYS) {
            assertEquals(selector.makeStickyKeyHash(key),
                    selector.makeStickyKeyHash(key.getBytes(StandardCharsets.UTF_8)), key);
        }
    }

    @Test(dataProvider = "selectors")
    public void testEntryStickyKeyHashMatchesTheResolvedStickyKeyHash(StickyKeyConsumerSelector selector) {
        for (String key : KEYS) {
            assertEntryHashMatches(selector, new MessageMetadata().setPartitionKey(key));
            assertEntryHashMatches(selector, new MessageMetadata().setPartitionKey(key)
                    .setOrderingKey("ordering".getBytes(StandardCharsets.UTF_8)));
        }
        assertEntryHashMatches(selector, new MessageMetadata()
                .setPartitionKey(Base64.getEncoder().encodeToString(new byte[]{1, 2, (byte) 0xff}))
                .setPartitionKeyB64Encoded(true));
        assertEntryHashMatches(selector, new MessageMetadata()
                .setOrderingKey("ordering".getBytes(StandardCharsets.UTF_8)));
        assertEntryHashMatches(selector, new MessageMetadata().setProducerName("producer").setSequenceId(1));
        assertEntryHashMatches(selector, new MessageMetadata());

        EntryAndMetadata withoutMetadata = EntryAndMetadata.create(mock(Entry.class), null);
        assertEquals(withoutMetadata.getOrUpdateCachedStickyKeyHash(selector),
                selector.makeStickyKeyHash(Commands.NONE_KEY));
    }

    @Test
    public void testEntryStickyKeyHashIsCached() {
        EntryAndMetadata entry = EntryAndMetadata.create(mock(Entry.class),
                new MessageMetadata().setPartitionKey("device-123456"));
        StickyKeyConsumerSelector selector = new HashRangeAutoSplitStickyKeyConsumerSelector();
        int hash = entry.getOrUpdateCachedStickyKeyHash(selector);

        assertEquals(entry.getOrUpdateCachedStickyKeyHash(new ConsistentHashingStickyKeyConsumerSelector(100) {
            @Override
            public int makeStickyKeyHash(String stickyKey) {
                return hash + 1;
            }
        }), hash);
    }

    private static void assertEntryHashMatches(StickyKeyConsumerSelector selector, MessageMetadata metadata) {
        EntryAndMetadata entry = EntryAndMetadata.create(mock(Entry.class), metadata);
        assertEquals(entry.getOrUpdateCachedStickyKeyHash(selector),
                selector.makeStickyKeyHash(Commands.resolveStickyKey(metadata)), metadata.toString());
    }
}
