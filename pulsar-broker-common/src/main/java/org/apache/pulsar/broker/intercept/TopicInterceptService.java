package org.apache.pulsar.broker.intercept;

import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;

public class TopicInterceptService {

    private final TopicsInterceptProvider provider;

    public TopicInterceptService(TopicsInterceptProvider topicInterceptProvider) {
        this.provider = topicInterceptProvider;
    }

    /*
     * Intercept create partitioned topic
     * @param topicName the topic name
     * @param partitionedTopicMetadata metadata related to the partioned topic
     * @param clientRole the role used to create partitioned topic
     */
    public void createPartitionedTopic(TopicName topicName, PartitionedTopicMetadata partitionedTopicMetadata, String clientRole) throws InterceptException {
        provider.createPartitionedTopic(topicName, partitionedTopicMetadata, clientRole);
    }

    /**
     * Intercept call for create topic
     *
     * @param topicName the topic name
     * @param clientRole the role used to create topic
     */
    public void createTopic(TopicName topicName, String clientRole) throws InterceptException {
        provider.createTopic(topicName, clientRole);
    }

    /**
     * Intercept update partitioned topic
     * @param topicName the topic name
     * @param partitionedTopicMetadata metadata related to the partioned topic
     * @param clientRole the role used to update partitioned topic
     */
    public void updatePartitionedTopic(TopicName topicName, PartitionedTopicMetadata partitionedTopicMetadata, String clientRole) throws InterceptException {
        provider.updatePartitionedTopic(topicName, partitionedTopicMetadata, clientRole);
    }
}
