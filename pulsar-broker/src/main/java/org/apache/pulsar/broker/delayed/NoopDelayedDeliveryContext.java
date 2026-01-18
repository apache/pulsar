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
package org.apache.pulsar.broker.delayed;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.mledger.ManagedCursor;

public class NoopDelayedDeliveryContext implements DelayedDeliveryContext {

    private final String name;
    private final ManagedCursor cursor;
    private final AtomicInteger triggerCount = new AtomicInteger();

    public NoopDelayedDeliveryContext(String name, ManagedCursor cursor) {
        this.name = name;
        this.cursor = cursor;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public ManagedCursor getCursor() {
        return cursor;
    }

    @Override
    public void triggerReadMoreEntries() {
        // no-op; for tests/JMH
        triggerCount.incrementAndGet();
    }

    public int getTriggerCount() {
        return triggerCount.get();
    }
}
