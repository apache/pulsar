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
package org.apache.pulsar.common.policies.data;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This object is populated using meta data in zookeeper without actually bringing the topic online.
 */
public class PersistentOfflineTopicStats {
    /** Space used to store the messages for the topic (bytes). */
    public long storageSize;

    /** Total number of messages. */
    public long totalMessages;

    /** Total backlog. */
    public long messageBacklog;

    /** Broker host where this stat was generated. */
    public final String brokerName;

    /** Offline topic name. */
    public final String topicName;

    /** Data ledger ids. */
    public List<LedgerDetails> dataLedgerDetails;

    /** Cursor ledger ids and backlog. */
    public Map<String, CursorDetails> cursorDetails;

    /** Timestamp when stat was generated. */
    public Date statGeneratedAt;

    public PersistentOfflineTopicStats(String topicName, String brokerName) {
        this.brokerName = brokerName;
        this.topicName = topicName;
        this.dataLedgerDetails = new ArrayList<>();
        this.cursorDetails = new HashMap<>();
        this.statGeneratedAt = new Date(System.currentTimeMillis());
    }

    public void reset() {
        this.storageSize = 0;
        this.totalMessages = 0;
        this.messageBacklog = 0;
        this.dataLedgerDetails.clear();
        this.cursorDetails.clear();
        this.statGeneratedAt.setTime(System.currentTimeMillis());
    }

    /**
     * Details about a cursor.
     */
    public static class CursorDetails {
        public long cursorBacklog;
        public long cursorLedgerId;

        public CursorDetails(long cursorBacklog, long cursorLedgerId) {
            this.cursorBacklog = cursorBacklog;
            this.cursorLedgerId = cursorLedgerId;
        }
    }

    public void addCursorDetails(String cursor, long backlog, long ledgerId) {
        this.cursorDetails.put(cursor, new CursorDetails(backlog, ledgerId));
    }

    public void addLedgerDetails(long entries, long timestamp, long size, long ledgerId) {
        this.dataLedgerDetails.add(new LedgerDetails(entries, timestamp, size, ledgerId));
    }

    /**
     * Details about a ledger.
     */
    public static class LedgerDetails {
        public long entries;
        public long timestamp;
        public long size;
        public long ledgerId;

        public LedgerDetails(long entries, long timestamp, long size, long ledgerId) {
            this.entries = entries;
            this.timestamp = timestamp;
            this.size = size;
            this.ledgerId = ledgerId;
        }
    }
}
