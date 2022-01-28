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
package org.apache.pulsar.client.api;

import java.util.Objects;
import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;

/**
 * Int range.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class Range {

    private final int start;
    private final int end;


    public Range(int start, int end) {
        if (end < start) {
            throw new IllegalArgumentException("Range end must >= range start.");
        }
        this.start = start;
        this.end = end;
    }

    public static Range of(int start, int end) {
        return new Range(start, end);
    }

    public int getStart() {
        return start;
    }

    public int getEnd() {
        return end;
    }

    public Range intersect(Range range) {
        int start = Math.max(range.getStart(), this.getStart());
        int end = Math.min(range.getEnd(), this.getEnd());
        if (end >= start) {
            return Range.of(start, end);
        } else {
            return null;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Range range = (Range) o;
        return start == range.start && end == range.end;
    }

    @Override
    public int hashCode() {
        return Objects.hash(start, end);
    }

    @Override
    public String toString() {
        return "[" + start + ", " + end + "]";
    }
}
