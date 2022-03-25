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

public class ProducerStatsDisabled implements ProducerStatsRecorder {
    private static final long serialVersionUID = 1L;

    static final ProducerStatsRecorder INSTANCE = new ProducerStatsDisabled();

    @Override
    public void incrementSendFailed() {
        // Do nothing
    }

    @Override
    public void incrementSendFailed(long numMsgs) {
        // Do nothing
    }

    @Override
    public void incrementNumAcksReceived(long latencyNs) {
        // Do nothing
    }

    @Override
    public void updateNumMsgsSent(long numMsgs, long totalMsgsSize) {
        // Do nothing
    }

    @Override
    public void cancelStatsTimeout() {
        // Do nothing
    }

    @Override
    public long getNumMsgsSent() {
        return 0;
    }

    @Override
    public long getNumBytesSent() {
        return 0;
    }

    @Override
    public long getNumSendFailed() {
        return 0;
    }

    @Override
    public long getNumAcksReceived() {
        return 0;
    }

    @Override
    public long getTotalMsgsSent() {
        return 0;
    }

    @Override
    public long getTotalBytesSent() {
        return 0;
    }

    @Override
    public long getTotalSendFailed() {
        return 0;
    }

    @Override
    public long getTotalAcksReceived() {
        return 0;
    }

    @Override
    public double getSendMsgsRate() {
        return 0;
    }

    @Override
    public double getSendBytesRate() {
        return 0;
    }

    @Override
    public double getSendLatencyMillis50pct() {
        return 0;
    }

    @Override
    public double getSendLatencyMillis75pct() {
        return 0;
    }

    @Override
    public double getSendLatencyMillis95pct() {
        return 0;
    }

    @Override
    public double getSendLatencyMillis99pct() {
        return 0;
    }

    @Override
    public double getSendLatencyMillis999pct() {
        return 0;
    }

    @Override
    public double getSendLatencyMillisMax() {
        return 0;
    }

    @Override
    public int getPendingQueueSize() {
        return 0;
    }
}
