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

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.pulsar.common.policies.data.AutoTopicCreationOverride;
import org.apache.pulsar.common.policies.data.TopicType;

/**
 * Override of autoTopicCreation settings on a namespace level.
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public final class AutoTopicCreationOverrideImpl implements AutoTopicCreationOverride {
    private boolean allowAutoTopicCreation;
    private String topicType;
    private Integer defaultNumPartitions;

    public static boolean isValidOverride(AutoTopicCreationOverride override) {
        if (override == null) {
            return false;
        }
        if (override.isAllowAutoTopicCreation()) {
            if (!TopicType.isValidTopicType(override.getTopicType())) {
                return false;
            }
            if (TopicType.PARTITIONED.toString().equals(override.getTopicType())) {
                if (override.getDefaultNumPartitions() == null) {
                    return false;
                }
                if (!(override.getDefaultNumPartitions() > 0)) {
                    return false;
                }
            } else if (TopicType.NON_PARTITIONED.toString().equals(override.getTopicType())) {
                if (override.getDefaultNumPartitions() != null) {
                    return false;
                }
            }
        }
        return true;
    }

    public static AutoTopicCreationOverrideImplBuilder builder() {
        return new AutoTopicCreationOverrideImplBuilder();
    }

    public static class AutoTopicCreationOverrideImplBuilder implements AutoTopicCreationOverride.Builder {
        private boolean allowAutoTopicCreation;
        private String topicType;
        private Integer defaultNumPartitions;

        public AutoTopicCreationOverrideImplBuilder allowAutoTopicCreation(boolean allowAutoTopicCreation) {
            this.allowAutoTopicCreation = allowAutoTopicCreation;
            return this;
        }

        public AutoTopicCreationOverrideImplBuilder topicType(String topicType) {
            this.topicType = topicType;
            return this;
        }

        public AutoTopicCreationOverrideImplBuilder defaultNumPartitions(Integer defaultNumPartitions) {
            this.defaultNumPartitions = defaultNumPartitions;
            return this;
        }

        public AutoTopicCreationOverrideImpl build() {
            return new AutoTopicCreationOverrideImpl(allowAutoTopicCreation, topicType, defaultNumPartitions);
        }
    }
}
