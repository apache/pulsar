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
package org.apache.pulsar.client.util;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.impl.MessageIdImpl;

public class MessageIdUtils {
    public static final long getOffset(MessageId messageId) {
        MessageIdImpl msgId = (MessageIdImpl) messageId;
        long ledgerId = msgId.getLedgerId();
        long entryId = msgId.getEntryId();

        // Combine ledger id and entry id to form offset
        // Use less than 32 bits to represent entry id since it will get
        // rolled over way before overflowing the max int range
        long offset = (ledgerId << 28) | entryId;
        return offset;
    }

    public static final MessageId getMessageId(long offset) {
        // Demultiplex ledgerId and entryId from offset
        long ledgerId = offset >>> 28;
        long entryId = offset & 0x0F_FF_FF_FFL;

        return new MessageIdImpl(ledgerId, entryId, -1);
    }
}
