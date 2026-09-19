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
package org.apache.pulsar.common.scalable;

/**
 * Represents an inclusive hash range [start, end] within a 16-bit hash space (0x0000-0xFFFF).
 */
public record HashRange(int start, int end) implements Comparable<HashRange> {

    public static final int MIN_HASH = 0x0000;
    public static final int MAX_HASH = 0xFFFF;

    public HashRange {
        if (start < MIN_HASH || start > MAX_HASH) {
            throw new IllegalArgumentException("start must be in [0x0000, 0xFFFF], got: " + start);
        }
        if (end < MIN_HASH || end > MAX_HASH) {
            throw new IllegalArgumentException("end must be in [0x0000, 0xFFFF], got: " + end);
        }
        if (end < start) {
            throw new IllegalArgumentException("end must be >= start, got: [" + start + ", " + end + "]");
        }
    }

    public static HashRange of(int start, int end) {
        return new HashRange(start, end);
    }

    public static HashRange full() {
        return new HashRange(MIN_HASH, MAX_HASH);
    }

    public boolean contains(int hash) {
        return hash >= start && hash <= end;
    }

    public boolean contains(HashRange other) {
        return start <= other.start && end >= other.end;
    }

    public boolean isAdjacentTo(HashRange other) {
        return this.end + 1 == other.start || other.end + 1 == this.start;
    }

    public int size() {
        return end - start + 1;
    }

    /**
     * Split this range at the midpoint into two sub-ranges.
     *
     * @return array of two HashRange objects: [start, mid] and [mid+1, end]
     * @throws IllegalStateException if the range cannot be split (size < 2)
     */
    public HashRange[] split() {
        if (size() < 2) {
            throw new IllegalStateException("Cannot split range of size " + size());
        }
        int mid = start + (end - start) / 2;
        return new HashRange[]{
                new HashRange(start, mid),
                new HashRange(mid + 1, end)
        };
    }

    /**
     * Merge this range with an adjacent range.
     *
     * @param other the adjacent range to merge with
     * @return the merged range
     * @throws IllegalArgumentException if ranges are not adjacent
     */
    public HashRange merge(HashRange other) {
        if (this.end + 1 == other.start) {
            return new HashRange(this.start, other.end);
        } else if (other.end + 1 == this.start) {
            return new HashRange(other.start, this.end);
        }
        throw new IllegalArgumentException(
                "Ranges are not adjacent: " + this + " and " + other);
    }

    @Override
    public int compareTo(HashRange o) {
        int result = Integer.compare(start, o.start);
        if (result == 0) {
            result = Integer.compare(end, o.end);
        }
        return result;
    }

    public String toHexString() {
        return String.format("%04x-%04x", start, end);
    }

    public static HashRange fromHexString(String hex) {
        String[] parts = hex.split("-", 2);
        if (parts.length != 2) {
            throw new IllegalArgumentException("Invalid hash range format: " + hex);
        }
        return new HashRange(Integer.parseInt(parts[0], 16), Integer.parseInt(parts[1], 16));
    }

    @Override
    public String toString() {
        return "[" + String.format("%04x", start) + ", " + String.format("%04x", end) + "]";
    }
}
