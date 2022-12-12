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
package org.apache.pulsar.broker.service;

import com.google.common.util.concurrent.AtomicDouble;

/**
 * It starts keep tracking the average messages per entry.
 * The initial value is 0, when new value comes, it will update with
 * avgMessagesPerEntry = avgMessagePerEntry * avgPercent + (1 - avgPercent) * new Value.
 */
public class AvgMessagesPerEntryAccumulator {

    private static final double avgPercent = 0.9;

    private final AtomicDouble avgMessagesPerEntry = new AtomicDouble(0);

    public double getAvgMessagesPerEntry(){
        return avgMessagesPerEntry.get();
    }

    public void setAvgMessagesPerEntry(double avgMessagesPerEntry){
        this.avgMessagesPerEntry.set(avgMessagesPerEntry);
    }

    public void accumulate(int totalMessages, int totalEntries) {
        if (avgMessagesPerEntry.get() < 1) { //valid avgMessagesPerEntry should always >= 1
            // set init value.
            avgMessagesPerEntry.set(1.0 * totalMessages / totalEntries);
        } else {
            avgMessagesPerEntry.set(avgMessagesPerEntry.get() * avgPercent
                    + (1 - avgPercent) * totalMessages / totalEntries);
        }
    }
}
