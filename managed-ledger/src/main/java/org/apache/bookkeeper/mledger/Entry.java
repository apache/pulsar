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
package org.apache.bookkeeper.mledger;

import io.netty.buffer.ByteBuf;
import org.apache.bookkeeper.common.annotation.InterfaceAudience;
import org.apache.bookkeeper.common.annotation.InterfaceStability;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.protocol.Commands;

/**
 * An Entry represent a ledger entry data and its associated position.
 */
@InterfaceAudience.LimitedPrivate
@InterfaceStability.Stable
public interface Entry {

    /**
     * @return the data
     */
    byte[] getData();

    byte[] getDataAndRelease();

    /**
     * @return the entry length in bytes
     */
    int getLength();

    /**
     * @return the data buffer for the entry
     */
    ByteBuf getDataBuffer();

    /**
     * @return the position at which the entry was stored
     */
    Position getPosition();

    /**
     * @return ledgerId of the position
     */
    long getLedgerId();

    /**
     * @return entryId of the position
     */
    long getEntryId();

    /**
     * Release the resources (data) allocated for this entry and recycle if all the resources are deallocated (ref-count
     * of data reached to 0).
     */
    boolean release();

    /**
     * Returns the handler used to track the entry's expected read count for the cacheEvictionByExpectedReadCount
     * strategy (PIP-430). May return null if unsupported by a custom Managed Ledger implementation,
     * or not applicable.
     *
     * @return the read count handler, or null
     */
    default EntryReadCountHandler getReadCountHandler() {
        return null;
    }

    /**
     * Check if the entry has an associated {@link EntryReadCountHandler} and it has remaining expected reads.
     * @return true if the entry has remaining expected reads, false otherwise
     */
    default boolean hasExpectedReads() {
        EntryReadCountHandler readCountHandler = getReadCountHandler();
        if (readCountHandler != null) {
            return readCountHandler.hasExpectedReads();
        }
        return false;
    }

    /**
     * Check if this entry is for the given Position.
     * @param position the position to check against
     * @return true if the entry matches the position, false otherwise
     */
    default boolean matchesPosition(Position position) {
        return position != null && position.compareTo(getLedgerId(), getEntryId()) == 0;
    }

    default MessageMetadata getMessageMetadata() {
        return null;
    }

    /**
     * Returns the timestamp of the entry.
     * @return
     */
    default long getEntryTimestamp() {
        // get broker timestamp first if BrokerEntryMetadata is enabled with AppendBrokerTimestampMetadataInterceptor
        return Commands.peekBrokerEntryMetadataToLong(getDataBuffer(), brokerEntryMetadata -> {
            if (brokerEntryMetadata != null && brokerEntryMetadata.hasBrokerTimestamp()) {
                return brokerEntryMetadata.getBrokerTimestamp();
            }
            // otherwise get the publish_time
            MessageMetadata messageMetadata = getMessageMetadata();
            if (messageMetadata == null) {
                messageMetadata = Commands.peekMessageMetadata(getDataBuffer(), null, -1);
            }
            return messageMetadata.getPublishTime();
        });
    }
}
