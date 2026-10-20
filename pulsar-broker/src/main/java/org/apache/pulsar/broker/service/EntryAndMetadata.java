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
import java.util.function.IntSupplier;
import java.util.function.ToIntFunction;
import lombok.Getter;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.EntryReadCountHandler;
import org.apache.bookkeeper.mledger.Position;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.protocol.Commands;
import org.jspecify.annotations.Nullable;

public class EntryAndMetadata implements Entry {
    private static final int STICKY_KEY_HASH_NOT_INITIALIZED = -1;
    private final Entry entry;
    @Getter
    @Nullable
    private final MessageMetadata metadata;
    int stickyKeyHash = STICKY_KEY_HASH_NOT_INITIALIZED;
    // The wrapper belongs to one dispatch batch, even when its metadata is shared across subscriptions.
    @Getter
    private boolean ownedByDispatcher = true;

    private EntryAndMetadata(final Entry entry, @Nullable final MessageMetadata metadata) {
        this.entry = entry;
        this.metadata = metadata;
    }

    public static EntryAndMetadata create(final Entry entry, final MessageMetadata metadata) {
        if (entry instanceof EntryAndMetadata entryAndMetadata) {
            return entryAndMetadata;
        }
        return new EntryAndMetadata(entry, metadata);
    }

    @VisibleForTesting
    public static EntryAndMetadata create(final Entry entry) {
        if (entry instanceof EntryAndMetadata entryAndMetadata) {
            return entryAndMetadata;
        }
        MessageMetadata msgMetadata = entry.getMessageMetadata();
        if (msgMetadata == null) {
            msgMetadata = Commands.peekAndCopyMessageMetadata(entry.getDataBuffer(), "", -1);
        }
        return create(entry, msgMetadata);
    }

    public byte[] getStickyKey() {
        if (metadata != null) {
            return Commands.resolveStickyKey(metadata);
        }
        return Commands.NONE_KEY;
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
        ownedByDispatcher = false;
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
        ownedByDispatcher = false;
        return entry.release();
    }

    /** Mark a successful handoff to Consumer.sendMessages, which releases the entry asynchronously. */
    public void transferToConsumer() {
        ownedByDispatcher = false;
    }

    /**
     * Get cached sticky key hash or calculate it based on the sticky key if it's not cached.
     *
     * @param makeStickyKeyHash function to calculate the sticky key hash
     * @return the sticky key hash
     */
    public int getOrUpdateCachedStickyKeyHash(ToIntFunction<byte[]> makeStickyKeyHash) {
        if (stickyKeyHash == STICKY_KEY_HASH_NOT_INITIALIZED) {
            stickyKeyHash = makeStickyKeyHash.applyAsInt(getStickyKey());
        }
        return stickyKeyHash;
    }

    /**
     * Get the cached sticky key hash or calculate it with the given function, which doesn't need the sticky key.
     */
    public int getOrUpdateCachedStickyKeyHash(IntSupplier makeStickyKeyHash) {
        if (stickyKeyHash == STICKY_KEY_HASH_NOT_INITIALIZED) {
            stickyKeyHash = makeStickyKeyHash.getAsInt();
        }
        return stickyKeyHash;
    }

    /**
     * Get the cached sticky key hash or calculate it with the selector from the key's hash, which hashes the key where
     * it is in the metadata, without copying, decoding or encoding it into a new array.
     */
    public int getOrUpdateCachedStickyKeyHash(StickyKeyConsumerSelector selector) {
        if (stickyKeyHash == STICKY_KEY_HASH_NOT_INITIALIZED) {
            stickyKeyHash = metadata != null
                    ? selector.makeStickyKeyHashOfKeyHash(Commands.resolveStickyKeyHash(metadata))
                    : selector.makeStickyKeyHash(Commands.NONE_KEY);
        }
        return stickyKeyHash;
    }

    /**
     * Get cached sticky key hash or return STICKY_KEY_HASH_NOT_SET if it's not cached.
     *
     * @return the cached sticky key hash or STICKY_KEY_HASH_NOT_SET if it's not cached
     */
    public int getCachedStickyKeyHash() {
        return stickyKeyHash != STICKY_KEY_HASH_NOT_INITIALIZED ? stickyKeyHash
                : StickyKeyConsumerSelector.STICKY_KEY_HASH_NOT_SET;
    }

    @VisibleForTesting
    public Entry unwrap() {
        return entry;
    }

    @Override
    public EntryReadCountHandler getReadCountHandler() {
        return entry.getReadCountHandler();
    }
}
