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
package org.apache.bookkeeper.mledger.impl;

import lombok.Data;

@Data
public class EntryCacheCounter {
    private int cacheHitCount;
    private long cacheHitSize;
    private int cacheMissCount;
    private long cacheMissSize;

    private EntryCacheCounter() {
        this.cacheHitCount = 0;
        this.cacheHitSize = 0;
        this.cacheMissCount = 0;
        this.cacheMissSize = 0;
    }

    public static class EntryCacheCounterBuilder {
        private EntryCacheCounter cacheCounter;

        private EntryCacheCounterBuilder() {
            cacheCounter = new EntryCacheCounter();
        }

        public static EntryCacheCounterBuilder getBuilder() {
            return new EntryCacheCounterBuilder();
        }

        public EntryCacheCounterBuilder setCacheHitCount(int cacheHitCount) {
            this.cacheCounter.setCacheHitCount(cacheHitCount);
            return this;
        }

        public EntryCacheCounterBuilder setCacheHitSize(long cacheHitSize) {
            this.cacheCounter.setCacheHitSize(cacheHitSize);
            return this;
        }

        public EntryCacheCounterBuilder setCacheMissCount(int cacheMissCount) {
            this.cacheCounter.setCacheMissCount(cacheMissCount);
            return this;
        }

        public EntryCacheCounterBuilder setCacheMissSize(long cacheMissSize) {
            this.cacheCounter.setCacheMissSize(cacheMissSize);
            return this;
        }

        public EntryCacheCounter build() {
            return cacheCounter;
        }
    }

    public EntryCacheCounterBuilder builder() {
        return new EntryCacheCounterBuilder();
    }

    public void add(EntryCacheCounter entryCacheCounter) {
        this.cacheHitCount += entryCacheCounter.cacheHitCount;
        this.cacheHitSize += entryCacheCounter.cacheHitSize;
        this.cacheMissCount += entryCacheCounter.cacheMissCount;
        this.cacheMissSize += entryCacheCounter.cacheMissSize;
    }
}
