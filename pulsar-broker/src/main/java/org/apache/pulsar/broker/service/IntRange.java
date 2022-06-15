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

import io.netty.util.Recycler;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * A recyclable int range.
 */
@RequiredArgsConstructor
public class IntRange {

    private static final Recycler<IntRange> RECYCLER = new Recycler<IntRange>() {
        @Override
        protected IntRange newObject(Handle<IntRange> handle) {
            return new IntRange(handle);
        }
    };

    private final Recycler.Handle<IntRange> handle;

    // The left-closed and right-open range: [start, end)
    @Getter
    private int start;
    @Getter
    private int end;

    @Override
    public boolean equals(Object object) {
        if (!(object instanceof IntRange)) {
            return false;
        }
        IntRange other = (IntRange) object;
        return start == other.start && end == other.end;
    }

    public boolean overlap(int start, int end) {
        assert start < end;
        if (this.start <= start) { // this.start <= start <= end
            return this.end > start;
        } else { // start < this.start <= this.end
            return this.start < end;
        }
    }

    public static IntRange get(int start, int end) {
        assert start < end;
        final IntRange range = RECYCLER.get();
        range.start = start;
        range.end = end;
        return range;
    }

    public void recycle() {
        start = -1;
        end = -1;
        handle.recycle(this);
    }
}
