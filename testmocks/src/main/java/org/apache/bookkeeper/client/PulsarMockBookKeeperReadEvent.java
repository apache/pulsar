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
package org.apache.bookkeeper.client;

import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Event;
import jdk.jfr.Label;

@Label("Pulsar MockBookKeeper read event")
@Description("Records a read event made to Pulsar MockBookKeeper")
@Category("Application")
public class PulsarMockBookKeeperReadEvent extends Event {
    @Label("LedgerId")
    private long ledgerId;
    @Label("FirstEntry")
    private long firstEntry;
    @Label("LastEntry")
    private long lastEntry;
    @Label("NumberOfEntries")
    private int numberOfEntries;

    /**
     * Creates a new custom Java Flight Recorder event for a Pulsar MockBookKeeper read.
     * This is useful when profiling a test with JFR recording or with Async Profiler and its jfrsync option.
     * @param ledgerId ledger id
     * @param firstEntry first entry to read
     * @param lastEntry last entry to read
     */
    public void maybeApplyAndCommit(long ledgerId, long firstEntry, long lastEntry) {
        if (shouldCommit()) {
            this.ledgerId = ledgerId;
            this.firstEntry = firstEntry;
            this.lastEntry = lastEntry;
            this.numberOfEntries = (int) (lastEntry - firstEntry + 1);
            commit();
        }
    }
}
