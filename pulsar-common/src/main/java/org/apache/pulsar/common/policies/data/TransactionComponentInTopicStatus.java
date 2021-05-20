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

import java.util.List;
import lombok.Data;

/**
 * Transaction component in topic status.
 */
@Data
public class TransactionComponentInTopicStatus {

    /** The topic of get transaction component. */
    public String topic;

    /** The transaction buffer status. */
    public TransactionBufferStatus transactionBufferStatus;

    /** The transaction pending ack status. */
    public List<TransactionPendingAckStatus> transactionPendingAckStatuses;

    @Data
    public static class TransactionBufferStatus {
        /** The state of this transaction buffer. */
        public String state;

        /** The max read position of this transaction buffer. */
        public String maxReadPosition;

        /** The last snapshot timestamps of this transaction buffer. */
        public long lastSnapshotTimestamps;
    }

    @Data
    public static class TransactionPendingAckStatus{

        /** The sub name of this pending ack. */
        public String subName;

        /** The state of this pending ack. */
        public String state;
    }
}
