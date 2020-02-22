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
