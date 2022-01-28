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

import io.netty.util.Timeout;
import java.util.Map;
import java.util.Optional;
import org.apache.pulsar.client.api.ConsumerStats;
import org.apache.pulsar.client.api.Message;

public class ConsumerStatsDisabled implements ConsumerStatsRecorder {
    private static final long serialVersionUID = 1L;

    static final ConsumerStatsRecorder INSTANCE = new ConsumerStatsDisabled();

    @Override
    public void updateNumMsgsReceived(Message<?> message) {
        // Do nothing
    }

    @Override
    public void incrementNumReceiveFailed() {
        // Do nothing
    }

    @Override
    public void incrementNumBatchReceiveFailed() {
        // Do nothing
    }

    @Override
    public void incrementNumAcksSent(long numAcks) {
        // Do nothing
    }

    @Override
    public void incrementNumAcksFailed() {
        // Do nothing
    }

    @Override
    public long getNumMsgsReceived() {
        return 0;
    }

    @Override
    public long getNumBytesReceived() {
        return 0;
    }

    @Override
    public long getNumAcksSent() {
        return 0;
    }

    @Override
    public long getNumAcksFailed() {
        return 0;
    }

    @Override
    public long getNumReceiveFailed() {
        return 0;
    }

    @Override
    public long getNumBatchReceiveFailed() {
        return 0;
    }

    @Override
    public long getTotalMsgsReceived() {
        return 0;
    }

    @Override
    public long getTotalBytesReceived() {
        return 0;
    }

    @Override
    public long getTotalReceivedFailed() {
        return 0;
    }

    @Override
    public long getTotaBatchReceivedFailed() {
        return 0;
    }

    @Override
    public long getTotalAcksSent() {
        return 0;
    }

    @Override
    public long getTotalAcksFailed() {
        return 0;
    }

    @Override
    public Integer getMsgNumInReceiverQueue() {
        return null;
    }

    @Override
    public Map<Long, Integer> getMsgNumInSubReceiverQueue() {
        return null;
    }

    @Override
    public double getRateMsgsReceived() {
        return 0;
    }

    @Override
    public double getRateBytesReceived() {
        return 0;
    }

    @Override
    public Optional<Timeout> getStatTimeout() {
        return Optional.empty();
    }

    @Override
    public void reset() {
        // do nothing
    }

    @Override
    public void updateCumulativeStats(ConsumerStats stats) {
        // do nothing
    }
}
