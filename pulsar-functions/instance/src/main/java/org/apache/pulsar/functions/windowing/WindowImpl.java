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

import java.util.List;

/**
 * Holds the expired, new and current events in a window.
 */
public class WindowImpl<T> implements Window<T> {
    private final List<T> tuples;
    private final List<T> newTuples;
    private final List<T> expiredTuples;
    private final Long startTimestamp;
    private final Long endTimestamp;

    public WindowImpl(List<T> tuples, List<T> newTuples, List<T> expiredTuples,
                      Long startTimestamp, Long endTimestamp) {
        this.tuples = tuples;
        this.newTuples = newTuples;
        this.expiredTuples = expiredTuples;
        this.startTimestamp = startTimestamp;
        this.endTimestamp = endTimestamp;
    }

    @Override
    public List<T> get() {
        return tuples;
    }

    @Override
    public List<T> getNew() {
        return newTuples;
    }

    @Override
    public List<T> getExpired() {
        return expiredTuples;
    }

    @Override
    public Long getStartTimestamp() {
        return startTimestamp;
    }

    @Override
    public Long getEndTimestamp() {
        return endTimestamp;
    }

    @Override
    public String toString() {
        return "TupleWindowImpl{" + "tuples=" + tuples + ", newTuples=" + newTuples + ", expiredTuples="
                + expiredTuples + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        WindowImpl that = (WindowImpl) o;

        if (tuples != null ? !tuples.equals(that.tuples) : that.tuples != null) {
            return false;
        }
        if (newTuples != null ? !newTuples.equals(that.newTuples) : that.newTuples != null) {
            return false;
        }
        return expiredTuples != null ? expiredTuples.equals(that.expiredTuples)
                : that.expiredTuples == null;

    }

    @Override
    public int hashCode() {
        int result = tuples != null ? tuples.hashCode() : 0;
        result = 31 * result + (newTuples != null ? newTuples.hashCode() : 0);
        result = 31 * result + (expiredTuples != null ? expiredTuples.hashCode() : 0);
        return result;
    }
}
