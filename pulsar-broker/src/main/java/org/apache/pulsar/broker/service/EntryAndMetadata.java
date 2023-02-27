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

import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import java.nio.charset.StandardCharsets;
import javax.annotation.Nullable;
import lombok.Getter;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.Position;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.protocol.Commands;

public class EntryAndMetadata implements Entry {

    private final Entry entry;
    @Getter
    @Nullable
    private final MessageMetadata metadata;

    private EntryAndMetadata(final Entry entry, @Nullable final MessageMetadata metadata) {
        this.entry = entry;
        this.metadata = metadata;
    }

    public static EntryAndMetadata create(final Entry entry, final MessageMetadata metadata) {
        return new EntryAndMetadata(entry, metadata);
    }

    @VisibleForTesting
    static EntryAndMetadata create(final Entry entry) {
        return create(entry, Commands.peekAndCopyMessageMetadata(entry.getDataBuffer(), "", -1));
    }

    public byte[] getStickyKey() {
        if (metadata != null) {
            if (metadata.hasOrderingKey()) {
                return metadata.getOrderingKey();
            } else if (metadata.hasPartitionKey()) {
                return metadata.getPartitionKey().getBytes(StandardCharsets.UTF_8);
            }
        }
        return "NONE_KEY".getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public String toString() {
        String s = entry.getLedgerId() + ":" + entry.getEntryId();
        if (metadata != null) {
            s += ("@" + metadata.getProducerName() + "-" + metadata.getSequenceId());
            if (metadata.hasChunkId() && metadata.hasNumChunksFromMsg()) {
                s += ("-" + metadata.getChunkId() + "-" + metadata.getNumChunksFromMsg());
            }
        }
        return s;
    }

    @Override
    public byte[] getData() {
        return entry.getData();
    }

    @Override
    public byte[] getDataAndRelease() {
        return entry.getDataAndRelease();
    }

    @Override
    public int getLength() {
        return entry.getLength();
    }

    @Override
    public ByteBuf getDataBuffer() {
        return entry.getDataBuffer();
    }

    @Override
    public Position getPosition() {
        return entry.getPosition();
    }

    @Override
    public long getLedgerId() {
        return entry.getLedgerId();
    }

    @Override
    public long getEntryId() {
        return entry.getEntryId();
    }

    @Override
    public boolean release() {
        return entry.release();
    }
}
