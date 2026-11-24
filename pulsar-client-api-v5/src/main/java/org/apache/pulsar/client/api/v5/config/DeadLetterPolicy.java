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
package org.apache.pulsar.client.api.v5.config;

import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * Configuration for the dead letter queue mechanism.
 *
 * <p>When a message has been redelivered more than {@code maxRedeliverCount} times,
 * it is moved to the dead letter topic instead of being redelivered again.
 *
 * <p>Construct via {@link #builder()}.
 */
@EqualsAndHashCode
@ToString
public final class DeadLetterPolicy {

    private final int maxRedeliverCount;
    private final String retryLetterTopic;
    private final String deadLetterTopic;
    private final String initialSubscriptionName;

    private DeadLetterPolicy(int maxRedeliverCount, String retryLetterTopic,
                             String deadLetterTopic, String initialSubscriptionName) {
        if (maxRedeliverCount < 0) {
            throw new IllegalArgumentException("maxRedeliverCount must be >= 0");
        }
        this.maxRedeliverCount = maxRedeliverCount;
        this.retryLetterTopic = retryLetterTopic;
        this.deadLetterTopic = deadLetterTopic;
        this.initialSubscriptionName = initialSubscriptionName;
    }

    /**
     * @return the maximum number of redelivery attempts before sending to the dead letter topic
     */
    public int maxRedeliverCount() {
        return maxRedeliverCount;
    }

    /**
     * @return the custom retry letter topic, or {@code null} for the auto-generated default
     */
    public String retryLetterTopic() {
        return retryLetterTopic;
    }

    /**
     * @return the custom dead letter topic, or {@code null} for the auto-generated default
     */
    public String deadLetterTopic() {
        return deadLetterTopic;
    }

    /**
     * @return the subscription name to create on the dead letter topic, or {@code null} if none
     */
    public String initialSubscriptionName() {
        return initialSubscriptionName;
    }

    /**
     * @return a new builder for constructing a {@link DeadLetterPolicy}
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for {@link DeadLetterPolicy}.
     */
    public static final class Builder {
        private int maxRedeliverCount;
        private String retryLetterTopic;
        private String deadLetterTopic;
        private String initialSubscriptionName;

        private Builder() {
        }

        /**
         * Maximum number of redelivery attempts before sending to the dead letter topic.
         * Required.
         *
         * @param maxRedeliverCount the redelivery threshold
         * @return this builder
         */
        public Builder maxRedeliverCount(int maxRedeliverCount) {
            this.maxRedeliverCount = maxRedeliverCount;
            return this;
        }

        /**
         * Custom retry letter topic. {@code null} (default) auto-generates the name.
         *
         * @param retryLetterTopic the retry letter topic name
         * @return this builder
         */
        public Builder retryLetterTopic(String retryLetterTopic) {
            this.retryLetterTopic = retryLetterTopic;
            return this;
        }

        /**
         * Custom dead letter topic. {@code null} (default) auto-generates the name as
         * {@code <source>-DLQ}.
         *
         * @param deadLetterTopic the dead letter topic name
         * @return this builder
         */
        public Builder deadLetterTopic(String deadLetterTopic) {
            this.deadLetterTopic = deadLetterTopic;
            return this;
        }

        /**
         * Subscription name to create on the dead letter topic, ensuring messages
         * forwarded to the DLQ are retained until consumed.
         *
         * @param initialSubscriptionName the DLQ initial subscription
         * @return this builder
         */
        public Builder initialSubscriptionName(String initialSubscriptionName) {
            this.initialSubscriptionName = initialSubscriptionName;
            return this;
        }

        /**
         * @return a new {@link DeadLetterPolicy} instance
         */
        public DeadLetterPolicy build() {
            return new DeadLetterPolicy(maxRedeliverCount, retryLetterTopic,
                    deadLetterTopic, initialSubscriptionName);
        }
    }
}
