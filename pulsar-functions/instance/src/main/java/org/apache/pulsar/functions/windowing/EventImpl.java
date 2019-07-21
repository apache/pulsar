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
package org.apache.pulsar.functions.windowing;

import lombok.EqualsAndHashCode;
import lombok.ToString;

import org.apache.pulsar.functions.api.Record;

@ToString
@EqualsAndHashCode
public class EventImpl<T> implements Event<T> {
    private final Record<?> record;
    private final T event;
    private final long ts;

    EventImpl(T event, long ts, Record<?> record) {
        this.event = event;
        this.ts = ts;
        this.record = record;
    }

    @Override
    public long getTimestamp() {
        return ts;
    }

    @Override
    public T get() {
        return event;
    }

    @Override
    public boolean isWatermark() {
        return false;
    }

    @Override
    public Record<?> getRecord() {
        return record;
    }
}
