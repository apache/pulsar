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
package org.apache.pulsar.common.policies.data;

import com.google.common.base.MoreObjects;
import java.util.Objects;

/**
 * Unit of a backlog quota configuration for a scoped resource in a Pulsar instance.
 *
 * <p>A scoped resource is identified by a {@link BacklogQuotaType} enumeration type which is containing two attributes:
 * <code>limit</code> representing a quota limit in bytes and <code>policy</code> for backlog retention policy.
 */
public class BacklogQuota {
    private long limit;
    private RetentionPolicy policy;

    /**
     * Gets quota limit in bytes.
     *
     * @return quota limit in bytes
     */
    public long getLimit() {
        return limit;
    }

    public RetentionPolicy getPolicy() {
        return policy;
    }

    /**
     * Sets quota limit in bytes.
     *
     * @param limit
     *            quota limit in bytes
     */
    public void setLimit(long limit) {
        this.limit = limit;
    }

    public void setPolicy(RetentionPolicy policy) {
        this.policy = policy;
    }

    protected BacklogQuota() {
    }

    public BacklogQuota(long limit, RetentionPolicy policy) {
        this.limit = limit;
        this.policy = policy;
    }

    @Override
    public int hashCode() {
        return Objects.hash(Long.valueOf(limit), policy);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("limit", limit).add("policy", policy).toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof BacklogQuota) {
            BacklogQuota other = (BacklogQuota) obj;
            return Objects.equals(limit, other.limit) && Objects.equals(policy, other.policy);
        }
        return false;
    };

    /**
     * Identifier to a backlog quota configuration (an instance of {@link BacklogQuota}).
     */
    public enum BacklogQuotaType {
        destination_storage;
    }

    /**
     * Enumeration type determines how to retain backlog against the resource shortages.
     */
    public enum RetentionPolicy {
        /** Policy which holds producer's send request until the resource becomes available (or holding times out). */
        producer_request_hold,

        /** Policy which throws javax.jms.ResourceAllocationException to the producer. */
        producer_exception,

        /** Policy which evicts the oldest message from the slowest consumer's backlog. */
        consumer_backlog_eviction,
    }
}
