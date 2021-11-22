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


import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.pulsar.client.api.MessageId;

@Data
@NoArgsConstructor
public class ResetCursorData {
    protected long ledgerId;
    protected long entryId;
    protected int partitionIndex = -1;
    protected boolean isExcluded = false;
    protected int batchIndex = -1;

    public ResetCursorData(long ledgerId, long entryId) {
        this.ledgerId = ledgerId;
        this.entryId = entryId;
    }

    public ResetCursorData(long ledgerId, long entryId, boolean isExcluded) {
        this.ledgerId = ledgerId;
        this.entryId = entryId;
        this.isExcluded = isExcluded;
    }

    // Private constructor used only for json deserialization
    private ResetCursorData(String position) {
        if ("latest".equals(position)) {
            this.ledgerId = Long.MAX_VALUE;
            this.entryId = Long.MAX_VALUE;
        } else if ("earliest".equals(position)) {
            this.ledgerId = -1;
            this.entryId = -1;
        } else {
            throw new IllegalArgumentException(
                    String.format("Invalid value %s for the position. Allowed values are [latest, earliest]",
                            position));
        }
    }

    public ResetCursorData(MessageId messageId) {
        if (messageId instanceof BatchMessageIdImpl) {
            BatchMessageIdImpl batchMessageId = (BatchMessageIdImpl) messageId;
            this.ledgerId = batchMessageId.getLedgerId();
            this.entryId = batchMessageId.getEntryId();
            this.batchIndex = batchMessageId.getBatchIndex();
            this.partitionIndex = batchMessageId.partitionIndex;
        } else if (messageId instanceof MessageIdImpl) {
            MessageIdImpl messageIdImpl = (MessageIdImpl) messageId;
            this.ledgerId = messageIdImpl.getLedgerId();
            this.entryId = messageIdImpl.getEntryId();
            this.partitionIndex = messageIdImpl.partitionIndex;
        }  else if (messageId instanceof TopicMessageIdImpl) {
            throw new IllegalArgumentException("Not supported operation on partitioned-topic");
        }
    }

}
