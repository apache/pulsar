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
package org.apache.pulsar.common.protocol;

import static org.testng.Assert.assertEquals;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.util.Murmur3_32Hash;
import org.testng.annotations.Test;

public class CommandsStickyKeyHashTest {

    @Test
    public void testHashesTheResolvedStickyKey() {
        List<MessageMetadata> keys = new ArrayList<>();
        for (String key : new String[]{"", "device-123456", "käyttäjä-123456", "漢字", "emoji-🚀"}) {
            keys.add(new MessageMetadata().setPartitionKey(key));
            // the ordering key comes first
            keys.add(new MessageMetadata().setPartitionKey(key)
                    .setOrderingKey("ordering".getBytes(StandardCharsets.UTF_8)));
        }
        for (int length = 0; length < 8; length++) {
            byte[] key = new byte[length];
            for (int i = 0; i < length; i++) {
                key[i] = (byte) (0xf0 + i * 37);
            }
            keys.add(new MessageMetadata().setPartitionKey(Base64.getEncoder().encodeToString(key))
                    .setPartitionKeyB64Encoded(true));
            keys.add(new MessageMetadata().setOrderingKey(key));
        }
        keys.add(new MessageMetadata().setProducerName("producer").setSequenceId(1));
        keys.add(new MessageMetadata());
        for (MessageMetadata metadata : keys) {
            assertEquals(Commands.resolveStickyKeyHash(metadata),
                    Murmur3_32Hash.getInstance().makeHash(Commands.resolveStickyKey(metadata)), metadata.toString());
            // as the broker has it, parsed from the entry, with the keys still in the buffer
            MessageMetadata parsed = parse(metadata);
            assertEquals(Commands.resolveStickyKeyHash(parsed),
                    Murmur3_32Hash.getInstance().makeHash(Commands.resolveStickyKey(parse(metadata))),
                    metadata.toString());
        }
    }

    private static MessageMetadata parse(MessageMetadata metadata) {
        MessageMetadata complete = new MessageMetadata().copyFrom(metadata);
        if (!complete.hasProducerName()) {
            complete.setProducerName("producer");
        }
        if (!complete.hasSequenceId()) {
            complete.setSequenceId(1);
        }
        complete.setPublishTime(1);
        ByteBuf buffer = Unpooled.buffer();
        complete.writeTo(buffer);
        MessageMetadata parsed = new MessageMetadata();
        parsed.parseFrom(buffer, buffer.readableBytes());
        return parsed;
    }
}
