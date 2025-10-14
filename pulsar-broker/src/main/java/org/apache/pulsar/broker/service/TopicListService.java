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
package org.apache.pulsar.broker.service;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.broker.resources.TopicResources;
import org.apache.pulsar.common.api.proto.CommandWatchTopicListClose;
import org.apache.pulsar.common.api.proto.ServerError;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.semaphore.AsyncSemaphore;
import org.apache.pulsar.common.topics.TopicList;
import org.apache.pulsar.common.topics.TopicsPattern;
import org.apache.pulsar.common.topics.TopicsPatternFactory;
import org.apache.pulsar.common.util.Backoff;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.Runnables;
import org.apache.pulsar.common.util.collections.ConcurrentLongHashMap;
import org.apache.pulsar.metadata.api.NotificationType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TopicListService {
    private static final int MAX_RETRY_COUNT = 10;

    public static class TopicListWatcher implements BiConsumer<String, NotificationType> {

        /** Topic names which are matching, the topic name contains the partition suffix. **/
        private final List<String> matchingTopics;
        private final TopicListService topicListService;
        private final long id;
        /** The regexp for the topic name(not contains partition suffix). **/
        private final TopicsPattern topicsPattern;

        /***
         * @param topicsPattern The regexp for the topic name(not contains partition suffix).
         */
        public TopicListWatcher(TopicListService topicListService, long id,
                                TopicsPattern topicsPattern, List<String> topics) {
            this.topicListService = topicListService;
            this.id = id;
            this.topicsPattern = topicsPattern;
            this.matchingTopics = TopicList.filterTopics(topics, topicsPattern);
        }

        public List<String> getMatchingTopics() {
            return matchingTopics;
        }

        /***
         * @param topicName topic name which contains partition suffix.
         */
        @Override
        public void accept(String topicName, NotificationType notificationType) {
            String partitionedTopicName = TopicName.get(topicName).getPartitionedTopicName();
            String domainLessTopicName = TopicList.removeTopicDomainScheme(partitionedTopicName);

            if (topicsPattern.matches(domainLessTopicName)) {
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
    private final Backoff retryBackoff;


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
        this.retryBackoff = new Backoff(
                100, TimeUnit.MILLISECONDS,
                25, TimeUnit.SECONDS,
                0, TimeUnit.MILLISECONDS);
    }

    public void inactivate() {
        for (Long watcherId : new HashSet<>(watchers.keys())) {
            deleteTopicListWatcher(watcherId);
        }
    }

    /***
     * @param topicsPatternString The regexp for the topic name
     */
    public void handleWatchTopicList(NamespaceName namespaceName, long watcherId, long requestId,
                                     String topicsPatternString,
                                     TopicsPattern.RegexImplementation topicsPatternRegexImplementation,
                                     String topicsHash,
                                     Semaphore lookupSemaphore) {
        // remove the domain scheme from the topic pattern
        topicsPatternString = TopicList.removeTopicDomainScheme(topicsPatternString);

        if (!enableSubscriptionPatternEvaluation || topicsPatternString.length() > maxSubscriptionPatternLength) {
            String msg = "Unable to create topic list watcher: ";
            if (!enableSubscriptionPatternEvaluation) {
                msg += "Evaluating subscription patterns is disabled.";
            } else {
                msg += "Pattern longer than maximum: " + maxSubscriptionPatternLength;
            }
            log.warn("[{}] {} on namespace {}", connection.toString(), msg, namespaceName);
            connection.getCommandSender().sendErrorResponse(requestId, ServerError.NotAllowedError, msg);
            lookupSemaphore.release();
            return;
        }

        TopicsPattern topicsPattern;
        try {
            topicsPattern = TopicsPatternFactory.create(topicsPatternString, topicsPatternRegexImplementation);
        } catch (Exception e) {
            log.warn("[{}] Unable to create topic list watcher: Invalid pattern: {} on namespace {}",
                    connection.toString(), topicsPatternString, namespaceName);
            connection.getCommandSender().sendErrorResponse(requestId, ServerError.InvalidTopicName,
                    "Invalid topics pattern: " + e.getMessage());
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
                        connection.toString(), watcherId, watcher);
                watcherFuture = existingWatcherFuture;
            } else {
                // There was an early request to create a watcher with the same watcherId. This can happen when
                // client timeout is lower the broker timeouts. We need to wait until the previous watcher
                // creation request either completes or fails.
                log.warn("[{}] Watcher with id is already present on the connection,"
                        + " consumerId={}", connection.toString(), watcherId);
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
                                connection.toString(), namespaceName, requestId);
                    }
                    sendTopicListSuccessWithRetries(watcherId, requestId, topicList, hash);
                    lookupSemaphore.release();
                })
                .exceptionally(ex -> {
                    log.warn("[{}] Error WatchTopicList for namespace [//{}] by {}",
                            connection.toString(), namespaceName, requestId);
                    connection.getCommandSender().sendErrorResponse(requestId,
                            BrokerServiceException.getClientErrorCode(
                                    new BrokerServiceException.ServerMetadataException(ex)), ex.getMessage());
                    watchers.remove(watcherId, finalWatcherFuture);
                    lookupSemaphore.release();
                    return null;
                });
    }

    private void sendTopicListSuccessWithRetries(long watcherId, long requestId, List<String> topicList, String hash) {
        performOperationWithRetries("topic list success", permitAcquireErrorHandler ->
                () -> connection.getCommandSender()
                        .sendWatchTopicListSuccess(requestId, watcherId, hash, topicList, permitAcquireErrorHandler));
    }

    /***
     * @param topicsPattern The regexp for the topic name(not contains partition suffix).
     */
    public void initializeTopicsListWatcher(CompletableFuture<TopicListWatcher> watcherFuture,
            NamespaceName namespace, long watcherId, TopicsPattern topicsPattern) {
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
                        if (!watcherFuture.complete(watcher)) {
                            log.warn("[{}] Watcher future was already completed. Deregistering watcherId={}.",
                                    connection.toString(), watcherId);
                            topicResources.deregisterPersistentTopicListener(watcher);
                        }
                    }
                });
    }


    public void handleWatchTopicListClose(CommandWatchTopicListClose commandWatchTopicListClose) {
        long requestId = commandWatchTopicListClose.getRequestId();
        long watcherId = commandWatchTopicListClose.getWatcherId();
        deleteTopicListWatcher(watcherId);
        connection.getCommandSender().sendSuccessResponse(requestId);
    }

    public void deleteTopicListWatcher(Long watcherId) {
        CompletableFuture<TopicListWatcher> watcherFuture = watchers.get(watcherId);
        if (watcherFuture == null) {
            log.info("[{}] TopicListWatcher was not registered on the connection: {}",
                    watcherId, connection.toString());
            return;
        }

        if (!watcherFuture.isDone() && watcherFuture
                .completeExceptionally(new IllegalStateException("Closed watcher before creation was complete"))) {
            // We have received a request to close the watcher before it was actually completed, we have marked the
            // watcher future as failed and we can tell the client the close operation was successful. When the actual
            // create operation will complete, the new watcher will be discarded.
            log.info("[{}] Closed watcher before its creation was completed. watcherId={}",
                    connection.toString(), watcherId);
            watchers.remove(watcherId);
            return;
        }

        if (watcherFuture.isCompletedExceptionally()) {
            log.info("[{}] Closed watcher that already failed to be created. watcherId={}",
                    connection.toString(), watcherId);
            watchers.remove(watcherId);
            return;
        }

        // Proceed with normal watcher close
        topicResources.deregisterPersistentTopicListener(watcherFuture.getNow(null));
        watchers.remove(watcherId);
        log.info("[{}] Closed watcher, watcherId={}", connection.toString(), watcherId);
    }

    /**
     * @param deletedTopics topic names deleted(contains the partition suffix).
     * @param newTopics topics names added(contains the partition suffix).
     */
    public void sendTopicListUpdate(long watcherId, String topicsHash, List<String> deletedTopics,
                                    List<String> newTopics) {
        performOperationWithRetries("topic list update", permitAcquireErrorHandler ->
                () -> connection.getCommandSender()
                .sendWatchTopicListUpdate(watcherId, newTopics, deletedTopics, topicsHash, permitAcquireErrorHandler));
    }

    // performs an operation with retries, if the operation fails, it will retry after a backoff period
    private void performOperationWithRetries(String operationName,
                                             Function<Consumer<Throwable>, Supplier<CompletableFuture<Void>>>
                                                          asyncOperationFactory) {
        // holds a reference to the operation, this is to resolve a circular dependency between the error handler and
        // the actual operation
        AtomicReference<Runnable> operationRef = new AtomicReference<>();
        // create the error handler for the operation
        Consumer<Throwable> permitAcquireErrorHandler =
                createPermitAcquireErrorHandler(operationName, operationRef);
        // create the async operation using the factory function. Pass the error handler to the factory function.
        Supplier<CompletableFuture<Void>> asyncOperation = asyncOperationFactory.apply(permitAcquireErrorHandler);
        // set the operation to run into the operation reference
        operationRef.set(Runnables.catchingAndLoggingThrowables(() -> {
            asyncOperation.get().thenRun(() -> retryBackoff.reset());
        }));
        // run the operation
        operationRef.get().run();
    }

    // retries an operation up to MAX_RETRY_COUNT times with backoff
    private Consumer<Throwable> createPermitAcquireErrorHandler(String operationName,
                                                                AtomicReference<Runnable> operationRef) {
        ScheduledExecutorService scheduledExecutor = connection.ctx().channel().eventLoop();
        AtomicInteger retryCount = new AtomicInteger(0);
        return t -> {
            Throwable unwrappedException = FutureUtil.unwrapCompletionException(t);
            if (unwrappedException instanceof AsyncSemaphore.PermitAcquireCancelledException
                    || unwrappedException instanceof AsyncSemaphore.PermitAcquireAlreadyClosedException
                    || !connection.isActive()) {
                return;
            }
            if (retryCount.incrementAndGet() < MAX_RETRY_COUNT) {
                long retryDelay = retryBackoff.next();
                log.info("[{}] Cannot acquire direct memory tokens for sending {}. Retry {}/{} in {} ms. {}",
                        connection, operationName, retryCount.get(), MAX_RETRY_COUNT, retryDelay,
                        t.getMessage());
                scheduledExecutor.schedule(operationRef.get(), retryDelay, TimeUnit.MILLISECONDS);
            } else {
                log.warn("[{}] Cannot acquire direct memory tokens for sending {}."
                                + "State will be inconsistent on the client. {}", connection, operationName,
                        t.getMessage());
            }
        };
    }
}
