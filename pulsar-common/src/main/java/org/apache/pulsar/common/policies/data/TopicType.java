package org.apache.pulsar.common.policies.data;

/**
 * Topic types -- partitioned or non-partitioned.
 */
public enum TopicType {
    PARTITIONED("partitioned"),
    NON_PARTITIONED("non-partitioned");
    private String type;

    TopicType(String type) {
        this.type = type;
    }

    public String toString() {
        return type;
    }

    public static boolean isValidTopicType(String type) {
        for (TopicType topicType : TopicType.values()) {
            if (topicType.toString().equalsIgnoreCase(type)) {
                return true;
            }
        }
        return false;
    }
}
