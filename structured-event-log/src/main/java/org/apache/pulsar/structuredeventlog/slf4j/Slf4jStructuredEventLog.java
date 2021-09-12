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
package org.apache.pulsar.structuredeventlog.slf4j;

import java.time.Clock;

import org.apache.pulsar.structuredeventlog.Event;
import org.apache.pulsar.structuredeventlog.EventResources;
import org.apache.pulsar.structuredeventlog.EventResourcesImpl;
import org.apache.pulsar.structuredeventlog.StructuredEventLog;

public class Slf4jStructuredEventLog implements StructuredEventLog {
    public static Slf4jStructuredEventLog INSTANCE = new Slf4jStructuredEventLog();
    // Visible for testing
    Clock clock = Clock.systemUTC();

    @Override
    public Event newRootEvent() {
        return new Slf4jEvent(clock, null).traceId(Slf4jEvent.randomId());
    }

    @Override
    public EventResources newEventResources() {
        return new EventResourcesImpl(null);
    }

    @Override
    public Event unstash() {
        throw new UnsupportedOperationException("TODO");
    }
}
