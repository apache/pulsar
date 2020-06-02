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
 * Override of autoTopicCreation settings on a namespace level.
 */
public class AutoTopicCreationOverride {
    public boolean allowAutoTopicCreation;
    public String topicType;
    public Integer defaultNumPartitions;

    public AutoTopicCreationOverride() {
    }

    public AutoTopicCreationOverride(boolean allowAutoTopicCreation, String topicType,
                                     Integer defaultNumPartitions) {
        this.allowAutoTopicCreation = allowAutoTopicCreation;
        this.topicType = topicType;
        this.defaultNumPartitions = defaultNumPartitions;
    }

    @Override
    public int hashCode() {
        return Objects.hash(allowAutoTopicCreation, topicType, defaultNumPartitions);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof AutoTopicCreationOverride) {
            AutoTopicCreationOverride other = (AutoTopicCreationOverride) obj;
            return Objects.equals(this.allowAutoTopicCreation, other.allowAutoTopicCreation)
                    && Objects.equals(this.topicType, other.topicType)
                    && Objects.equals(this.defaultNumPartitions, other.defaultNumPartitions);
        }
        return false;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("allowAutoTopicCreation", allowAutoTopicCreation)
                .add("topicType", topicType).add("defaultNumPartitions", defaultNumPartitions).toString();
    }

    public static boolean isValidOverride(AutoTopicCreationOverride override) {
        if (override == null) {
            return false;
        }
        if (override.allowAutoTopicCreation) {
            if (!TopicType.isValidTopicType(override.topicType)) {
                return false;
            }
            if (TopicType.PARTITIONED.toString().equals(override.topicType)) {
                if (override.defaultNumPartitions == null) {
                    return false;
                }
                if (!(override.defaultNumPartitions > 0)) {
                    return false;
                }
            } else if (TopicType.NON_PARTITIONED.toString().equals(override.topicType)) {
                if (override.defaultNumPartitions != null) {
                    return false;
                }
            }
        }
        return true;
    }

}
