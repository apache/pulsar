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
package org.apache.pulsar.client.admin;

import java.util.List;
import lombok.Getter;
import lombok.Setter;

/**
 * Request DTO used by the admin client to submit a list of message IDs
 * for skipping. It supports multiple formats and is serialized to JSON
 * that the broker understands (polymorphic deserialization on server).
 * <p>
 * Supported types:
 * - type = "byteArray": messageIds is List<String> of base64-encoded MessageId.toByteArray()
 * - type = "messageId": messageIds is List<MessageIdItem> (supports batchIndex)
 */
@Setter
@Getter
public class SkipMessageIdsRequest {
    // optional; default is byteArray on server when messageIds is an array of strings
    private String type;
    // List<String> | List<MessageIdItem>
    private Object messageIds;

    public SkipMessageIdsRequest() {
    }

    public static SkipMessageIdsRequest forByteArrays(List<String> base64MessageIds) {
        SkipMessageIdsRequest r = new SkipMessageIdsRequest();
        r.setType("byteArray");
        r.setMessageIds(base64MessageIds);
        return r;
    }

    public static SkipMessageIdsRequest forMessageIds(List<MessageIdItem> items) {
        SkipMessageIdsRequest r = new SkipMessageIdsRequest();
        r.setType("messageId");
        r.setMessageIds(items);
        return r;
    }

    /**
     * Item representing a messageId as ledgerId, entryId and optional batchIndex.
     */
    @Setter
    @Getter
    public static class MessageIdItem {
        private long ledgerId;
        private long entryId;
        // optional
        private Integer batchIndex;

        public MessageIdItem(long ledgerId, long entryId) {
            this.ledgerId = ledgerId;
            this.entryId = entryId;
        }

        public MessageIdItem(long ledgerId, long entryId, Integer batchIndex) {
            this.ledgerId = ledgerId;
            this.entryId = entryId;
            this.batchIndex = batchIndex;
        }
    }
}
