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
package org.apache.pulsar.broker.stats;

import java.util.concurrent.atomic.LongAdder;

public class BrokerMetrics implements InputMetrics, OutputMetrics {

    private final LongAdder messageInCount = new LongAdder();
    private final LongAdder byteInCount = new LongAdder();

    private final LongAdder messageOutCount = new LongAdder();
    private final LongAdder byteOutCount = new LongAdder();
    private final LongAdder messageAckCount = new LongAdder();

    @Override
    public void recordMessageIn(long messageCount, long byteCount) {
        messageInCount.add(messageCount);
        byteInCount.add(byteCount);
    }

    @Override
    public long getMessageInCount() {
        return messageInCount.sum();
    }

    @Override
    public long getByteInCount() {
        return byteInCount.sum();
    }

    @Override
    public void recordMessageOut(long messageCount, long byteCount) {
        messageOutCount.add(messageCount);
        byteOutCount.add(byteCount);
    }

    @Override
    public long getMessageOutCount() {
        return messageOutCount.sum();
    }

    @Override
    public long getByteOutCount() {
        return byteOutCount.sum();
    }

    @Override
    public void recordMessageAck(long ackCount) {
        messageAckCount.add(ackCount);

    }

    @Override
    public long getMessageAckCount() {
        return messageAckCount.sum();
    }
}
