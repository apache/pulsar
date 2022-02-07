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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.Objects;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.common.api.proto.MessageIdData;

public class ChunkMessageIdImpl extends MessageIdImpl implements MessageId {
    private final MessageIdImpl firstChunkMsgId;

    public ChunkMessageIdImpl(MessageIdImpl firstChunkMsgId, MessageIdImpl lastChunkMsgId) {
        super(lastChunkMsgId.getLedgerId(), lastChunkMsgId.getEntryId(), lastChunkMsgId.getPartitionIndex());
        this.firstChunkMsgId = firstChunkMsgId;
    }

    public MessageIdImpl getFirstChunkMessageId() {
        return firstChunkMsgId;
    }

    public MessageIdImpl getLastChunkMessageId() {
        return this;
    }

    @Override
    public String toString() {
        return firstChunkMsgId.toString() + ';' + super.toString();
    }

    @Override
    public byte[] toByteArray() {

        // write last chunk message id
        MessageIdData msgId = super.writeMessageIdData(null, -1, 0);

        // write first chunk message id
        msgId.setFirstChunkMessageId();
        firstChunkMsgId.writeMessageIdData(msgId.getFirstChunkMessageId(), -1, 0);

        int size = msgId.getSerializedSize();
        ByteBuf serialized = Unpooled.buffer(size, size);
        msgId.writeTo(serialized);

        return serialized.array();
    }

    @Override
    public boolean equals(Object o) {
        return super.equals(o);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), firstChunkMsgId.hashCode());
    }

}
