package org.apache.pulsar.broker.intercept;

import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;

public interface TopicsInterceptProvider {
    /**
     * Intercept call for create topic
     *
     * @param topicName the topic name
     * @param clientRole the role used to create topic
     */
    default void createTopic(TopicName topicName, String clientRole) throws InterceptException {}

    /**
     * Intercept create partitioned topic
     *  @param topicName the topic name
     * @param numPartitions number of partitions to create for this partitioned topic
     * @param clientRole the role used to create partitioned topic
     */
    default void createPartitionedTopic(TopicName topicName, PartitionedTopicMetadata numPartitions, String clientRole) throws InterceptException {}

    /**
     * Intercept update partitioned topic
     *  @param topicName the topic name
     * @param numPartitions number of partitions to update to
     * @param clientRole the role used to update partitioned topic
     */
    default void updatePartitionedTopic(TopicName topicName, PartitionedTopicMetadata numPartitions, String clientRole) throws InterceptException {}
}
