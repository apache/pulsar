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

import org.apache.bookkeeper.mledger.Position;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class InMemoryRedeliveryTracker implements RedeliveryTracker {

    private ConcurrentHashMap<Position, AtomicInteger> trackerCache = new ConcurrentHashMap<>(16);

    @Override
    public int incrementAndGetRedeliveryCount(Position position) {
        trackerCache.putIfAbsent(position, new AtomicInteger(0));
        return trackerCache.get(position).incrementAndGet();
    }

    @Override
    public int getRedeliveryCount(Position position) {
        return trackerCache.getOrDefault(position, new AtomicInteger(0)).get();
    }

    @Override
    public void remove(Position position) {
        trackerCache.remove(position);
    }

    @Override
    public void removeBatch(List<Position> positions) {
        if (positions != null) {
            positions.forEach(this::remove);
        }
    }

    @Override
    public void clear() {
        trackerCache.clear();
    }
}
