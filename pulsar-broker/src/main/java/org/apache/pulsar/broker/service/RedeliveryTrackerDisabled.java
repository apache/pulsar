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
package org.apache.pulsar.broker.service;

import java.util.List;
import org.apache.bookkeeper.mledger.Position;

public class RedeliveryTrackerDisabled implements RedeliveryTracker {

    public static final RedeliveryTrackerDisabled REDELIVERY_TRACKER_DISABLED = new RedeliveryTrackerDisabled();

    private RedeliveryTrackerDisabled() {}

    @Override
    public int incrementAndGetRedeliveryCount(Position position) {
        return 0;
    }

    @Override
    public int getRedeliveryCount(Position position) {
        return 0;
    }

    @Override
    public void remove(Position position) {
        // no-op
    }

    @Override
    public void removeBatch(List<Position> positions) {
        // no-op
    }

    @Override
    public void clear() {
        // no-op
    }

    @Override
    public boolean contains(Position position) {
        return false;
    }

    @Override
    public void addIfAbsent(Position position) {
        // no-op
    }
}
