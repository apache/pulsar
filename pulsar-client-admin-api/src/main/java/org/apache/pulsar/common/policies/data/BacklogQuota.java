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

import org.apache.pulsar.common.policies.data.impl.BacklogQuotaImpl;

/**
 * Unit of a backlog quota configuration for a scoped resource in a Pulsar instance.
 *
 * <p>A scoped resource is identified by a {@link BacklogQuotaType} enumeration type which is containing two attributes:
 * <code>limit</code> representing a quota limit in bytes and <code>policy</code> for backlog retention policy.
 */
public interface BacklogQuota {

    /**
     * Gets quota limit in size.
     * Remains for compatible
     *
     * @return quota limit in bytes
     */
    @Deprecated
    long getLimit();

    /**
     * Gets quota limit in size.
     *
     * @return quota limit in bytes
     */
    long getLimitSize();

    /**
     * Gets quota limit in time.
     *
     * @return quota limit in second
     */
    int getLimitTime();

    RetentionPolicy getPolicy();

    interface Builder {
        Builder limitSize(long limitSize);

        Builder limitTime(int limitTime);

        Builder retentionPolicy(RetentionPolicy retentionPolicy);

        BacklogQuota build();
    }

    static Builder builder() {
        return BacklogQuotaImpl.builder();
    }

    /**
     * Identifier to a backlog quota configuration (an instance of {@link BacklogQuota}).
     */
    enum BacklogQuotaType {
        destination_storage,
        message_age,
    }

    /**
     * Enumeration type determines how to retain backlog against the resource shortages.
     */
    enum RetentionPolicy {
        /** Policy which holds producer's send request until the resource becomes available (or holding times out). */
        producer_request_hold,

        /** Policy which throws javax.jms.ResourceAllocationException to the producer. */
        producer_exception,

        /** Policy which evicts the oldest message from the slowest consumer's backlog. */
        consumer_backlog_eviction,
    }
}
