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

import java.util.Map;
import lombok.Data;

@Data
public class TransactionMetadata {

    /** The txnId of this transaction. */
    public String txnId;

    /** The status of this transaction. */
    public String status;

    /** The open time of this transaction. */
    public long openTimestamp;

    /** The timeout of this transaction. */
    public long timeoutAt;

    /** The producedPartitions of this transaction. */
    public Map<String, TransactionInBufferStats> producedPartitions;

    /** The ackedPartitions of this transaction. */
    public Map<String, Map<String, TransactionInPendingAckStats>> ackedPartitions;
}
