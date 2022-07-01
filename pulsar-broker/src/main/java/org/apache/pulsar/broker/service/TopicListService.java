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
package org.apache.pulsar.broker.service;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.function.BiConsumer;
import java.util.regex.Pattern;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.broker.resources.TopicResources;
import org.apache.pulsar.common.api.proto.CommandWatchTopicListClose;
import org.apache.pulsar.common.api.proto.ServerError;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.topics.TopicList;
import org.apache.pulsar.common.util.collections.ConcurrentLongHashMap;
import org.apache.pulsar.metadata.api.NotificationType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TopicListService {


    public static class TopicListWatcher implements BiConsumer<String, NotificationType> {

        private final List<String> matchingTopics;
        private final TopicListService topicListService;
        private final long id;
        private final Pattern topicsPattern;

        public TopicListWatcher(TopicListService topicListService, long id,
                                Pattern topicsPattern, List<String> topics) {
            this.topicListService = topicListService;
            this.id = id;
            this.topicsPattern = topicsPattern;
            this.matchingTopics = TopicList.filterTopics(topics, topicsPattern);
        }

        public List<String> getMatchingTopics() {
            return matchingTopics;
        }

        @Override
        public void accept(String topicName, NotificationType notificationType) {
            if (topicsPattern.matcher(topicName).matches()) {
                List<String> newTopics;
                List<String> deletedTopics;
                if (notificationType == NotificationType.Deleted) {
                    newTopics = Collections.emptyList();
                    deletedTopics = Collections.singletonList(topicName);
                    matchingTopics.remove(topicName);
                } else {
                    deletedTopics = Collections.emptyList();
                    newTopics = Collections.singletonList(topicName);
                    matchingTopics.add(topicName);
                }
                String hash = TopicList.calculateHash(matchingTopics);
                topicListService.sendTopicListUpdate(id, hash, deletedTopics, newTopics);
            }
        }
    }


    private static final Logger log = LoggerFactory.getLogger(TopicListService.class);

    private final NamespaceService namespaceService;
    private final TopicResources topicResources;
    private final ServerCnx connection;
    private final boolean enableSubscriptionPatternEvaluation;
    private final int maxSubscriptionPatternLength;
    private final ConcurrentLongHashMap<CompletableFuture<TopicListWatcher>> watchers;


    public TopicListService(PulsarService pulsar, ServerCnx connection,
                            boolean enableSubscriptionPatternEvaluation, int maxSubscriptionPatternLength) {
        this.namespaceService = pulsar.getNamespaceService();
        this.connection = connection;
        this.enableSubscriptionPatternEvaluation = enableSubscriptionPatternEvaluation;
        this.maxSubscriptionPatternLength = maxSubscriptionPatternLength;
        this.watchers = ConcurrentLongHashMap.<CompletableFuture<TopicListWatcher>>newBuilder()
                .expectedItems(8)
                .concurrencyLevel(1)
                .build();
        this.topicResources = pulsar.getPulsarResources().getTopicResources();
    }

    public void inactivate() {
        for (Long watcherId : new HashSet<>(watchers.keys())) {
            deleteTopicListWatcher(watcherId);
        }
    }

    public void handleWatchTopicList(NamespaceName namespaceName, long watcherId, long requestId, Pattern topicsPattern,
                                     String topicsHash, Semaphore lookupSemaphore) {

        if (!enableSubscriptionPatternEvaluation || topicsPattern.pattern().length() > maxSubscriptionPatternLength) {
            String msg = "Unable to create topic list watcher: ";
            if (!enableSubscriptionPatternEvaluation) {
                msg += "Evaluating subscription patterns is disabled.";
            } else {
                msg += "Pattern longer than maximum: " + maxSubscriptionPatternLength;
            }
            log.warn("[{}] {} on namespace {}", connection.getRemoteAddress(), msg, namespaceName);
            connection.getCommandSender().sendErrorResponse(requestId, ServerError.NotAllowedError, msg);
            lookupSemaphore.release();
            return;
        }
        CompletableFuture<TopicListWatcher> watcherFuture = new CompletableFuture<>();
        CompletableFuture<TopicListWatcher> existingWatcherFuture = watchers.putIfAbsent(watcherId, watcherFuture);

        if (existingWatcherFuture != null) {
            if (existingWatcherFuture.isDone() && !existingWatcherFuture.isCompletedExceptionally()) {
                TopicListWatcher watcher = existingWatcherFuture.getNow(null);
                log.info("[{}] Watcher with the same id is already created:"
                                + " watcherId={}, watcher={}",
                        connection.getRemoteAddress(), watcherId, watcher);
                watcherFuture = existingWatcherFuture;
            } else {
                // There was an early request to create a watcher with the same watcherId. This can happen when
                // client timeout is lower the broker timeouts. We need to wait until the previous watcher
                // creation request either completes or fails.
                log.warn("[{}] Watcher with id is already present on the connection,"
                        + " consumerId={}", connection.getRemoteAddress(), watcherId);
                ServerError error;
                if (!existingWatcherFuture.isDone()) {
                    error = ServerError.ServiceNotReady;
                } else {
                    error = ServerError.UnknownError;
                    watchers.remove(watcherId, existingWatcherFuture);
                }
                connection.getCommandSender().sendErrorResponse(requestId, error,
                        "Topic list watcher is already present on the connection");
                lookupSemaphore.release();
                return;
            }
        } else {
            initializeTopicsListWatcher(watcherFuture, namespaceName, watcherId, topicsPattern);
        }


        CompletableFuture<TopicListWatcher> finalWatcherFuture = watcherFuture;
        finalWatcherFuture.thenAccept(watcher -> {
                    List<String> topicList = watcher.getMatchingTopics();
                    String hash = TopicList.calculateHash(topicList);
                    if (hash.equals(topicsHash)) {
                        topicList = Collections.emptyList();
                    }
                    if (log.isDebugEnabled()) {
                        log.debug(
                                "[{}] Received WatchTopicList for namespace [//{}] by {}",
                                connection.getRemoteAddress(), namespaceName, requestId);
                    }
                    connection.getCommandSender().sendWatchTopicListSuccess(requestId, watcherId, hash, topicList);
                    lookupSemaphore.release();
                })
                .exceptionally(ex -> {
                    log.warn("[{}] Error WatchTopicList for namespace [//{}] by {}",
                            connection.getRemoteAddress(), namespaceName, requestId);
                    connection.getCommandSender().sendErrorResponse(requestId,
                            BrokerServiceException.getClientErrorCode(
                                    new BrokerServiceException.ServerMetadataException(ex)), ex.getMessage());
                    watchers.remove(watcherId, finalWatcherFuture);
                    lookupSemaphore.release();
                    return null;
                });
    }


    public void initializeTopicsListWatcher(CompletableFuture<TopicListWatcher> watcherFuture,
            NamespaceName namespace, long watcherId, Pattern topicsPattern) {
        namespaceService.getListOfPersistentTopics(namespace).
                thenApply(topics -> {
                    TopicListWatcher watcher = new TopicListWatcher(this, watcherId, topicsPattern, topics);
                    topicResources.registerPersistentTopicListener(namespace, watcher);
                    return watcher;
                }).
                whenComplete((watcher, exception) -> {
                    if (exception != null) {
                        watcherFuture.completeExceptionally(exception);
                    } else {
                        watcherFuture.complete(watcher);
                    }
                });
    }


    public void handleWatchTopicListClose(CommandWatchTopicListClose commandWatchTopicListClose) {
        long requestId = commandWatchTopicListClose.getRequestId();
        long watcherId = commandWatchTopicListClose.getWatcherId();
        deleteTopicListWatcher(watcherId);
        connection.getCommandSender().sendWatchTopicListSuccess(requestId, watcherId, null, null);
    }

    public void deleteTopicListWatcher(Long watcherId) {
        CompletableFuture<TopicListWatcher> watcherFuture = watchers.get(watcherId);
        if (watcherFuture == null) {
            log.info("[{}] TopicListWatcher was not registered on the connection: {}",
                    watcherId, connection.getRemoteAddress());
            return;
        }

        if (!watcherFuture.isDone() && watcherFuture
                .completeExceptionally(new IllegalStateException("Closed watcher before creation was complete"))) {
            // We have received a request to close the watcher before it was actually completed, we have marked the
            // watcher future as failed and we can tell the client the close operation was successful. When the actual
            // create operation will complete, the new watcher will be discarded.
            log.info("[{}] Closed watcher before its creation was completed. watcherId={}",
                    connection.getRemoteAddress(), watcherId);
            watchers.remove(watcherId);
            return;
        }

        if (watcherFuture.isCompletedExceptionally()) {
            log.info("[{}] Closed watcher that already failed to be created. watcherId={}",
                    connection.getRemoteAddress(), watcherId);
            watchers.remove(watcherId);
            return;
        }

        // Proceed with normal watcher close
        topicResources.deregisterPersistentTopicListener(watcherFuture.getNow(null));
        watchers.remove(watcherId);
        log.info("[{}] Closed watcher, watcherId={}", connection.getRemoteAddress(), watcherId);
    }

    public void sendTopicListUpdate(long watcherId, String topicsHash, List<String> deletedTopics,
                                    List<String> newTopics) {
        connection.getCommandSender().sendWatchTopicListUpdate(watcherId, newTopics, deletedTopics, topicsHash);
    }


}
