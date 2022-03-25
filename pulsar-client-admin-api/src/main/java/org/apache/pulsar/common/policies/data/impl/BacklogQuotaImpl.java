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
package org.apache.pulsar.common.policies.data.impl;

import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.apache.pulsar.common.policies.data.BacklogQuota;

@ToString
@EqualsAndHashCode
@NoArgsConstructor
public class BacklogQuotaImpl implements BacklogQuota {
    public static final long BYTES_IN_GIGABYTE = 1024 * 1024 * 1024;

    /**
     * backlog quota by size in byte, remains for compatible.
     * for the details: https://github.com/apache/pulsar/pull/13291
     * @since 2.9.1
     */
    @Deprecated
    private long limit;

    /**
     * backlog quota by size in byte.
     */
    private Long limitSize;

    /**
     * backlog quota by time in second.
     */
    private int limitTime;
    private RetentionPolicy policy;

    public BacklogQuotaImpl(long limitSize, int limitTime, RetentionPolicy policy) {
        this.limitSize = limitSize;
        this.limitTime = limitTime;
        this.policy = policy;
    }

    @Deprecated
    public long getLimit() {
        if (limitSize == null) {
            return limit;
        }
        return limitSize;
    }

    @Deprecated
    public void setLimit(long limit) {
        this.limit = limit;
        this.limitSize = limit;
    }

    public long getLimitSize() {
        if (limitSize == null) {
            return limit;
        }
        return limitSize;
    }

    public void setLimitSize(long limitSize) {
        this.limitSize = limitSize;
        this.limit = limitSize;
    }

    public int getLimitTime() {
        return limitTime;
    }

    public void setLimitTime(int limitTime) {
        this.limitTime = limitTime;
    }

    public RetentionPolicy getPolicy() {
        return policy;
    }

    public void setPolicy(RetentionPolicy policy) {
        this.policy = policy;
    }

    public static BacklogQuotaImplBuilder builder() {
        return new BacklogQuotaImplBuilder();
    }

    public static class BacklogQuotaImplBuilder implements BacklogQuota.Builder {
        private long limitSize = -1;
        private int limitTime = -1;
        private RetentionPolicy retentionPolicy;

        public BacklogQuotaImplBuilder limitSize(long limitSize) {
            this.limitSize = limitSize;
            return this;
        }

        public BacklogQuotaImplBuilder limitTime(int limitTime) {
            this.limitTime = limitTime;
            return this;
        }

        public BacklogQuotaImplBuilder retentionPolicy(RetentionPolicy retentionPolicy) {
            this.retentionPolicy = retentionPolicy;
            return this;
        }

        public BacklogQuotaImpl build() {
            return new BacklogQuotaImpl(limitSize, limitTime, retentionPolicy);
        }
    }
}
