package org.apache.pulsar.client.api;

import java.util.Collection;

public interface SelectiveConsumerHandler {

    /**
     * Closes the underlying consumer for the specified topics without affecting the consumers of other topics.
     * If a topic does not exist in the current consumer's subscription list, it will be ignored.
     *
     * @param topicNames A collection of topic names to close the consumers for.
     *                   These should include any necessary partition suffixes if applicable.
     */
    void closeTopicConsumer(Collection<String> topicNames);

    /**
     * Registers and starts a consumer for each of the topics specified if they are not already being consumed.
     * This method facilitates dynamic addition of topic subscriptions to an existing consumer, expanding
     * its topic set without needing to recreate or restart the entire consumer.
     *
     * @param topicNames A collection of topic names for which to start consumers.
     *                   Each topic name should include the necessary partition suffix if the topics are partitioned.
     */
    void addTopicConsumer(Collection<String> topicNames);

}
