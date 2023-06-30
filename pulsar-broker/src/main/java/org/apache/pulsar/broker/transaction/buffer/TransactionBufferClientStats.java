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
package org.apache.pulsar.broker.transaction.buffer;

import org.apache.pulsar.broker.transaction.buffer.impl.TransactionBufferClientStatsImpl;
import org.apache.pulsar.client.impl.transaction.TransactionBufferHandler;

public interface TransactionBufferClientStats {

    void recordAbortFailed(String topic);

    void recordCommitFailed(String topic);

    void recordAbortLatency(String topic, long nanos);

    void recordCommitLatency(String topic, long nanos);

    void close();


    static TransactionBufferClientStats create(boolean exposeTopicMetrics, TransactionBufferHandler handler,
                                               boolean enableTxnCoordinator) {
        return enableTxnCoordinator
                ? TransactionBufferClientStatsImpl.getInstance(exposeTopicMetrics, handler) : NOOP;
    }


    TransactionBufferClientStats NOOP = new TransactionBufferClientStats() {
        @Override
        public void recordAbortFailed(String topic) {

        }

        @Override
        public void recordCommitFailed(String topic) {

        }

        @Override
        public void recordAbortLatency(String topic, long nanos) {

        }

        @Override
        public void recordCommitLatency(String topic, long nanos) {

        }

        @Override
        public void close() {

        }
    };
}
