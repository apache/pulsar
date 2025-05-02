package org.apache.pulsar.client.impl;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

public interface TopicsChangedListener {

    /***
     * unsubscribe and delete {@link ConsumerImpl} in the {@link MultiTopicsConsumerImpl#consumers} map in
     * {@link MultiTopicsConsumerImpl}.
     * @param removedTopics topic names removed(contains the partition suffix).
     */
    CompletableFuture<Void> onTopicsRemoved(Collection<String> removedTopics);

    /***
     * subscribe and create a list of new {@link ConsumerImpl}, added them to the
     * {@link MultiTopicsConsumerImpl#consumers} map in {@link MultiTopicsConsumerImpl}.
     * @param addedTopics topic names added(contains the partition suffix).
     */
    CompletableFuture<Void> onTopicsAdded(Collection<String> addedTopics);
}
