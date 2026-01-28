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

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.broker.resources.TopicListener;
import org.apache.pulsar.broker.resources.TopicResources;
import org.apache.pulsar.broker.topiclistlimit.TopicListMemoryLimiter;
import org.apache.pulsar.broker.topiclistlimit.TopicListSizeResultCache;
import org.apache.pulsar.common.api.proto.CommandGetTopicsOfNamespace;
import org.apache.pulsar.common.api.proto.CommandWatchTopicListClose;
import org.apache.pulsar.common.api.proto.ServerError;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.semaphore.AsyncDualMemoryLimiter;
import org.apache.pulsar.common.semaphore.AsyncSemaphore;
import org.apache.pulsar.common.topics.TopicList;
import org.apache.pulsar.common.topics.TopicsPattern;
import org.apache.pulsar.common.topics.TopicsPatternFactory;
import org.apache.pulsar.common.util.Backoff;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.collections.ConcurrentLongHashMap;
import org.apache.pulsar.metadata.api.NotificationType;
import org.apache.pulsar.metadata.api.extended.SessionEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TopicListService {
    public static class TopicListWatcher implements TopicListener {
        // upper bound for buffered topic list updates
        private static final int DEFAULT_TOPIC_LIST_UPDATE_MAX_QUEUE_SIZE = 10000;
        /** Topic names which are matching, the topic name contains the partition suffix. **/
        private final Set<String> matchingTopics;
        private final TopicListService topicListService;
        private final long id;
        private final NamespaceName namespace;
        /** The regexp for the topic name(not contains partition suffix). **/
        private final TopicsPattern topicsPattern;
        private final Executor executor;
        private volatile boolean closed = false;
        private boolean sendingInProgress;
        private final BlockingDeque<Runnable> sendTopicListUpdateTasks;
        private boolean updatingTopics;
        private List<String> matchingTopicsBeforeDisconnected;
        private boolean disconnected;
        private List<Runnable> updateCallbacks = new LinkedList<>();

        public TopicListWatcher(TopicListService topicListService, long id,
                                NamespaceName namespace, TopicsPattern topicsPattern, List<String> topics,
                                Executor executor, int topicListUpdateMaxQueueSize) {
            this.topicListService = topicListService;
            this.id = id;
            this.namespace = namespace;
            this.topicsPattern = topicsPattern;
            this.executor = executor;
            this.matchingTopics =
                    TopicList.filterTopics(topics, topicsPattern, Collectors.toCollection(LinkedHashSet::new));
            // start with in progress state since topic list update will be sent first
            this.sendingInProgress = true;
            this.sendTopicListUpdateTasks =
                    new LinkedBlockingDeque<>(topicListUpdateMaxQueueSize);
        }

        public synchronized Collection<String> getMatchingTopics() {
            return new ArrayList<>(matchingTopics);
        }

        /***
         * @param topicName topic name which contains partition suffix.
         */
        @Override
        public synchronized void onTopicEvent(String topicName, NotificationType notificationType) {
            if (closed) {
                return;
            }
            String partitionedTopicName = TopicName.get(topicName).getPartitionedTopicName();
            String domainLessTopicName = TopicList.removeTopicDomainScheme(partitionedTopicName);

            if (topicsPattern.matches(domainLessTopicName)) {
                List<String> newTopics = Collections.emptyList();
                List<String> deletedTopics = Collections.emptyList();
                if (notificationType == NotificationType.Deleted) {
                    if (matchingTopics.remove(topicName)) {
                        deletedTopics = Collections.singletonList(topicName);
                    }
                } else if (notificationType == NotificationType.Created && matchingTopics.add(topicName)) {
                    newTopics = Collections.singletonList(topicName);
                }
                if (!newTopics.isEmpty() || !deletedTopics.isEmpty()) {
                    String hash = TopicList.calculateHash(matchingTopics);
                    sendTopicListUpdate(hash, deletedTopics, newTopics);
                }
            }
        }

        // sends updates one-by-one so that ordering is retained
        private synchronized void sendTopicListUpdate(String hash, List<String> deletedTopics, List<String> newTopics) {
            if (closed) {
                return;
            }
            Runnable task = () -> topicListService.sendTopicListUpdate(id, hash, deletedTopics, newTopics,
                    this::sendingCompleted);
            if (!sendingInProgress) {
                sendingInProgress = true;
                executor.execute(task);
            } else {
                // if sendTopicListSuccess hasn't completed, add to a queue to be executed after it completes
                if (!sendTopicListUpdateTasks.offer(task)) {
                    if (prepareUpdateTopics(null)) {
                        log.warn("Update queue was full for watcher id {} matching {}. Performing full refresh.", id,
                                topicsPattern.inputPattern());
                        executor.execute(() -> topicListService.updateTopicListWatcher(this, null, null));
                    }
                }
            }
        }

        // callback that triggers sending the next possibly buffered update
        @VisibleForTesting
        synchronized void sendingCompleted() {
            if (closed) {
                sendTopicListUpdateTasks.clear();
                return;
            }
            // Execute the next task
            Runnable task = sendTopicListUpdateTasks.poll();
            if (task != null) {
                executor.execute(task);
            } else {
                sendingInProgress = false;
            }
        }

        public synchronized void close() {
            closed = true;
            sendTopicListUpdateTasks.clear();
            updateCallbacks.clear();
        }

        /**
         * Returns true if the topic list update is prepared for execution. It is expected that the caller initiates
         * the update. The callback is registered to be executed after the update, either existing or upcoming is
         * completed.
         * @param afterUpdateCompletionCallback callback to be executed after the update is completed.
         * @return true if an existing update wasn't ongoing and a new update is prepared for execution.
         */
        synchronized boolean prepareUpdateTopics(Runnable afterUpdateCompletionCallback) {
            if (!updatingTopics) {
                updatingTopics = true;
                sendingInProgress = true;
                sendTopicListUpdateTasks.clear();
                matchingTopics.clear();
                if (afterUpdateCompletionCallback != null) {
                    updateCallbacks.add(afterUpdateCompletionCallback);
                }
                return true;
            } else {
                if (afterUpdateCompletionCallback != null) {
                    updateCallbacks.add(afterUpdateCompletionCallback);
                }
                return false;
            }
        }

        synchronized void updateTopics(List<String> topics) {
            if (closed) {
                return;
            }
            matchingTopics.clear();
            TopicList.filterTopicsToStream(topics, topicsPattern).forEach(matchingTopics::add);
            updatingTopics = false;
            if (disconnected) {
                handleNewAndDeletedTopicsWhileDisconnected();
                matchingTopicsBeforeDisconnected = null;
                disconnected = false;
            }
            for (Runnable callback : updateCallbacks) {
                try {
                    callback.run();
                } catch (Exception e) {
                    log.warn("Error executing topic list update callback: {}", callback, e);
                }
            }
            updateCallbacks.clear();
            sendingCompleted();
        }

        private synchronized void handleNewAndDeletedTopicsWhileDisconnected() {
            if (matchingTopicsBeforeDisconnected == null) {
                return;
            }
            List<String> newTopics = new ArrayList<>();
            List<String> deletedTopics = new ArrayList<>();
            Set<String> remainingTopics = new HashSet<>(matchingTopics);
            for (String topic : matchingTopicsBeforeDisconnected) {
                if (!remainingTopics.remove(topic)) {
                    deletedTopics.add(topic);
                }
            }
            newTopics.addAll(remainingTopics);
            if (!newTopics.isEmpty() || !deletedTopics.isEmpty()) {
                String hash = TopicList.calculateHash(matchingTopics);
                sendTopicListUpdate(hash, deletedTopics, newTopics);
            }
        }

        @Override
        public NamespaceName getNamespaceName() {
            return namespace;
        }

        @Override
        public synchronized void onSessionEvent(SessionEvent event) {
            switch (event) {
                case SessionReestablished:
                case Reconnected:
                    executor.execute(() -> {
                        synchronized (this) {
                            // ensure that only one update is triggered when connection is being lost and reconnected
                            // before the updating is complete. The disconnected flag is reseted after the update
                            // completes.
                            if (disconnected) {
                                topicListService.updateTopicListWatcher(this, null, null);
                            }
                        }
                    });
                    break;
                case SessionLost:
                case ConnectionLost:
                    if (!disconnected) {
                        disconnected = true;
                        matchingTopicsBeforeDisconnected = new ArrayList<>(matchingTopics);
                        prepareUpdateTopics(null);
                    }
                    break;
            }
        }
    }


    private static final Logger log = LoggerFactory.getLogger(TopicListService.class);

    private final NamespaceService namespaceService;
    private final TopicResources topicResources;
    private final PulsarService pulsar;
    private final ServerCnx connection;
    private final boolean enableSubscriptionPatternEvaluation;
    private final int maxSubscriptionPatternLength;
    private final int topicListUpdateMaxQueueSize;
    private final ConcurrentLongHashMap<CompletableFuture<TopicListWatcher>> watchers;
    private final Backoff retryBackoff;

    public TopicListService(PulsarService pulsar, ServerCnx connection,
                            boolean enableSubscriptionPatternEvaluation, int maxSubscriptionPatternLength) {
        this(pulsar, connection, enableSubscriptionPatternEvaluation, maxSubscriptionPatternLength,
                TopicListWatcher.DEFAULT_TOPIC_LIST_UPDATE_MAX_QUEUE_SIZE);
    }

    @VisibleForTesting
    public TopicListService(PulsarService pulsar, ServerCnx connection,
                            boolean enableSubscriptionPatternEvaluation, int maxSubscriptionPatternLength,
                            int topicListUpdateMaxQueueSize) {
        this.namespaceService = pulsar.getNamespaceService();
        this.pulsar = pulsar;
        this.connection = connection;
        this.enableSubscriptionPatternEvaluation = enableSubscriptionPatternEvaluation;
        this.maxSubscriptionPatternLength = maxSubscriptionPatternLength;
        this.topicListUpdateMaxQueueSize = topicListUpdateMaxQueueSize;
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
            if (log.isDebugEnabled()) {
                log.debug("[{}] Watcher with the same watcherId={} is already created. Refreshing.", connection,
                        watcherId);
            }
            // use the existing watcher if it's already created
            watcherFuture = existingWatcherFuture.thenCompose(watcher -> {
                CompletableFuture<TopicListWatcher> future = new CompletableFuture<>();
                Runnable callback = () -> future.complete(watcher);
                if (watcher.prepareUpdateTopics(callback)) {
                    updateTopicListWatcher(watcher, null, callback);
                }
                return future;
            });
        } else {
            initializeTopicsListWatcher(watcherFuture, namespaceName, watcherId, topicsPattern);
        }

        CompletableFuture<TopicListWatcher> finalWatcherFuture = watcherFuture;
        finalWatcherFuture.thenAccept(watcher -> {
                    Collection<String> topicList = watcher.getMatchingTopics();
                    String hash = TopicList.calculateHash(topicList);
                    if (hash.equals(topicsHash)) {
                        topicList = Collections.emptyList();
                    }
                    if (log.isDebugEnabled()) {
                        log.debug(
                                "[{}] Received WatchTopicList for namespace [//{}] by {}",
                                connection.toString(), namespaceName, requestId);
                    }
                    sendTopicListSuccessWithPermitAcquiringRetries(watcherId, requestId, topicList, hash,
                            watcher::sendingCompleted, watcher::close);
                    lookupSemaphore.release();
                })
                .exceptionally(ex -> {
                    log.warn("[{}] Error WatchTopicList for namespace [//{}] by {}: {}",
                            connection.toString(), namespaceName, requestId, ex.getMessage());
                    connection.getCommandSender().sendErrorResponse(requestId,
                            BrokerServiceException.getClientErrorCode(
                                    new BrokerServiceException.ServerMetadataException(ex)), ex.getMessage());
                    watchers.remove(watcherId, finalWatcherFuture);
                    lookupSemaphore.release();
                    return null;
                });
    }

    private void sendTopicListSuccessWithPermitAcquiringRetries(long watcherId, long requestId,
                                                                Collection<String> topicList,
                                                                String hash,
                                                                Runnable successfulCompletionCallback,
                                                                Runnable failedCompletionCallback) {
        performOperationWithPermitAcquiringRetries(watcherId, "topic list success", permitAcquireErrorHandler ->
                () -> connection.getCommandSender()
                        .sendWatchTopicListSuccess(requestId, watcherId, hash, topicList, permitAcquireErrorHandler)
                        .whenComplete((__, t) -> {
                            if (t != null) {
                                // this is an unexpected case
                                log.warn("[{}] Failed to send topic list success for watcherId={}. "
                                        + "Watcher is not active.", connection, watcherId, t);
                                failedCompletionCallback.run();
                            } else {
                                // completed successfully, run the callback
                                successfulCompletionCallback.run();
                            }
                        }));
    }

    /***
     * @param topicsPattern The regexp for the topic name(not contains partition suffix).
     */
    public void initializeTopicsListWatcher(CompletableFuture<TopicListWatcher> watcherFuture,
            NamespaceName namespace, long watcherId, TopicsPattern topicsPattern) {
        AtomicReference<TopicListWatcher> watcherRef = new AtomicReference<>();
        Consumer<List<String>> afterListing = topics -> {
            // register watcher immediately so that we don't lose events
            TopicListWatcher watcher =
                    new TopicListWatcher(this, watcherId, namespace, topicsPattern, topics,
                            connection.ctx().executor(), topicListUpdateMaxQueueSize);
            watcherRef.set(watcher);
            topicResources.registerPersistentTopicListener(watcher);
        };
        getTopics(namespace, watcherId, afterListing).whenComplete((topics, exception) -> {
            TopicListWatcher w = watcherRef.get();
            if (exception != null) {
                if (w != null) {
                    w.close();
                    topicResources.deregisterPersistentTopicListener(w);
                }
                Throwable unwrappedException = FutureUtil.unwrapCompletionException(exception);
                if (connection.isActive() && (unwrappedException instanceof AsyncSemaphore.PermitAcquireTimeoutException
                        || unwrappedException instanceof AsyncSemaphore.PermitAcquireQueueFullException)) {
                    // retry with backoff if permit acquisition fails due to timeout or queue full
                    long retryAfterMillis = this.retryBackoff.next();
                    log.info("[{}] {} when initializing topic list watcher watcherId={} for namespace {}. "
                                    + "Retrying in {} " + "ms.", connection, unwrappedException.getMessage(), watcherId,
                            namespace, retryAfterMillis);
                    connection.ctx().executor().schedule(
                            () -> initializeTopicsListWatcher(watcherFuture, namespace, watcherId, topicsPattern),
                            retryAfterMillis, TimeUnit.MILLISECONDS);
                } else {
                    log.warn("[{}] Failed to initialize topic list watcher watcherId={} for namespace {}.", connection,
                            watcherId, namespace, unwrappedException);
                    watcherFuture.completeExceptionally(unwrappedException);
                }
            } else {
                if (!watcherFuture.complete(w)) {
                    log.warn("[{}] Watcher future was already completed. Deregistering " + "watcherId={}.", connection,
                            watcherId);
                    w.close();
                    topicResources.deregisterPersistentTopicListener(w);
                    watchers.remove(watcherId, watcherFuture);
                }
            }
        });
    }

    private CompletableFuture<List<String>> getTopics(NamespaceName namespace, long watcherId) {
        return getTopics(namespace, watcherId, null);
    }

    private CompletableFuture<List<String>> getTopics(NamespaceName namespace, long watcherId,
                                                      Consumer<List<String>> afterListing) {
        BooleanSupplier isPermitRequestCancelled = () -> !connection.isActive() || !watchers.containsKey(watcherId);
        if (isPermitRequestCancelled.getAsBoolean()) {
            return CompletableFuture.failedFuture(
                    new AsyncSemaphore.PermitAcquireCancelledException("Permit acquisition was cancelled"));
        }
        return getTopics(namespace, afterListing, isPermitRequestCancelled);
    }

    private CompletableFuture<List<String>> getTopics(NamespaceName namespace,
                                                      Consumer<List<String>> afterListing,
                                                      BooleanSupplier isPermitRequestCancelled) {
        TopicListSizeResultCache.ResultHolder listSizeHolder = pulsar.getBrokerService().getTopicListSizeResultCache()
                .getTopicListSize(namespace.toString(), CommandGetTopicsOfNamespace.Mode.PERSISTENT);
        AsyncDualMemoryLimiter maxTopicListInFlightLimiter = pulsar.getBrokerService().getMaxTopicListInFlightLimiter();

        return listSizeHolder.getSizeAsync().thenCompose(initialSize -> {
            // use heap size limiter to avoid broker getting overwhelmed by a lot of concurrent topic list requests
            return maxTopicListInFlightLimiter.withAcquiredPermits(initialSize,
                            AsyncDualMemoryLimiter.LimitType.HEAP_MEMORY, isPermitRequestCancelled, initialPermits -> {
                                return namespaceService.getListOfUserTopics(namespace,
                                        CommandGetTopicsOfNamespace.Mode.PERSISTENT).thenComposeAsync(topics -> {
                                    long actualSize = TopicListMemoryLimiter.estimateTopicListSize(topics);
                                    listSizeHolder.updateSize(actualSize);
                                    if (afterListing != null) {
                                        afterListing.accept(topics);
                                    }
                                    if (initialSize != actualSize) {
                                        // use updated permits to slow down responses so that backpressure gets applied
                                        return maxTopicListInFlightLimiter.withUpdatedPermits(initialPermits,
                                                actualSize,
                                                isPermitRequestCancelled, updatedPermits -> {
                                                    // reset retry backoff
                                                    retryBackoff.reset();
                                                    // just return the topics which were already retrieved before
                                                    return CompletableFuture.completedFuture(topics);
                                                }, CompletableFuture::failedFuture);
                                    } else {
                                        // reset retry backoff
                                        retryBackoff.reset();
                                        return CompletableFuture.completedFuture(topics);
                                    }
                                }, connection.ctx().executor());
                            }, CompletableFuture::failedFuture)
                    .thenApplyAsync(Function.identity(), connection.ctx().executor());
        });
    }

    void updateTopicListWatcher(TopicListWatcher watcher, Runnable successCallback, Runnable failureCallback) {
        NamespaceName namespace = watcher.namespace;
        long watcherId = watcher.id;
        getTopics(namespace, watcherId).whenComplete((topics, exception) -> {
            if (exception != null) {
                Throwable unwrappedException = FutureUtil.unwrapCompletionException(exception);
                if (connection.isActive() && !watcher.closed
                        && (unwrappedException instanceof AsyncSemaphore.PermitAcquireTimeoutException
                        || unwrappedException instanceof AsyncSemaphore.PermitAcquireQueueFullException)) {
                    // retry with backoff if permit acquisition fails due to timeout or queue full
                    long retryAfterMillis = this.retryBackoff.next();
                    log.info("[{}] {} when updating topic list watcher watcherId={} for namespace {}. Retrying in {} "
                                    + "ms.", connection, unwrappedException.getMessage(), watcherId, namespace,
                            retryAfterMillis);
                    connection.ctx().executor()
                            .schedule(() -> updateTopicListWatcher(watcher, successCallback, failureCallback),
                                    retryAfterMillis, TimeUnit.MILLISECONDS);
                } else {
                    log.warn("[{}] Failed to update topic list watcher watcherId={} for namespace {}.", connection,
                            watcherId, namespace, unwrappedException);
                    if (failureCallback != null) {
                        failureCallback.run();
                    }
                }
            } else {
                watcher.updateTopics(topics);
                if (successCallback != null) {
                    successCallback.run();
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
        CompletableFuture<TopicListWatcher> watcherFuture = watchers.remove(watcherId);
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
            return;
        }

        // deregister topic listener while avoiding race conditions
        watcherFuture.whenComplete((watcher, t) -> {
            if (watcher != null) {
                topicResources.deregisterPersistentTopicListener(watcher);
                watcher.close();
                log.info("[{}] Closed watcher, watcherId={}", connection.toString(), watcherId);
            } else if (t != null) {
                log.info("[{}] Closed watcher that failed to be created. watcherId={}",
                        connection.toString(), watcherId);
            }
        });
    }

    /**
     * @param deletedTopics topic names deleted(contains the partition suffix).
     * @param newTopics topics names added(contains the partition suffix).
     */
    public void sendTopicListUpdate(long watcherId, String topicsHash, List<String> deletedTopics,
                                    List<String> newTopics, Runnable completionCallback) {
        performOperationWithPermitAcquiringRetries(watcherId, "topic list update", permitAcquireErrorHandler ->
                () -> connection.getCommandSender()
                        .sendWatchTopicListUpdate(watcherId, newTopics, deletedTopics, topicsHash,
                                permitAcquireErrorHandler)
                        .whenComplete((__, t) -> {
                            if (t != null) {
                                // this is an unexpected case
                                log.warn("[{}] Failed to send topic list update for watcherId={}. Watcher will be in "
                                        + "inconsistent state.", connection, watcherId, t);
                            }
                            completionCallback.run();
                        }));
    }

    // performs an operation with infinite permit acquiring retries.
    // If acquiring permits fails, it will retry after a backoff period
    private void performOperationWithPermitAcquiringRetries(long watcherId, String operationName,
                                                            Function<Function<Throwable, CompletableFuture<Void>>,
                                                                    Supplier<CompletableFuture<Void>>>
                                                                    asyncOperationFactory) {
        // holds a reference to the operation, this is to resolve a circular dependency between the error handler and
        // the actual operation
        AtomicReference<Supplier<CompletableFuture<Void>>> operationRef = new AtomicReference<>();
        // create the error handler for the operation
        Function<Throwable, CompletableFuture<Void>> permitAcquireErrorHandler =
                createPermitAcquireErrorHandler(watcherId, operationName, () -> operationRef.get().get());
        // create the async operation using the factory function. Pass the error handler to the factory function.
        Supplier<CompletableFuture<Void>> asyncOperation = asyncOperationFactory.apply(permitAcquireErrorHandler);
        // set the operation to run into the operation reference
        operationRef.set(() -> {
            if (!connection.isActive() || !watchers.containsKey(watcherId)) {
                // do nothing if the connection has already been closed or the watcher has been removed
                return CompletableFuture.completedFuture(null);
            }
            return asyncOperation.get().thenRun(() -> retryBackoff.reset());
        });
        // run the operation
        operationRef.get().get();
    }

    // retries acquiring permits until the connection is closed or the watcher is removed
    private Function<Throwable, CompletableFuture<Void>> createPermitAcquireErrorHandler(long watcherId,
                                                                                         String operationName,
                                                                                         Supplier<CompletableFuture
                                                                                                 <Void>> operationRef) {
        ScheduledExecutorService scheduledExecutor = connection.ctx().channel().eventLoop();
        AtomicInteger retryCount = new AtomicInteger(0);
        return t -> {
            Throwable unwrappedException = FutureUtil.unwrapCompletionException(t);
            if (unwrappedException instanceof AsyncSemaphore.PermitAcquireCancelledException
                    || unwrappedException instanceof AsyncSemaphore.PermitAcquireAlreadyClosedException
                    || !connection.isActive()
                    || !watchers.containsKey(watcherId)) {
                // stop retrying and complete successfully
                return CompletableFuture.completedFuture(null);
            }
            long retryDelay = retryBackoff.next();
            retryCount.incrementAndGet();
            log.info("[{}] Cannot acquire direct memory tokens for sending {}. Retry {} in {} ms. {}", connection,
                    operationName, retryCount.get(), retryDelay, t.getMessage());
            CompletableFuture<Void> future = new CompletableFuture<>();
            scheduledExecutor.schedule(() -> FutureUtil.completeAfter(future, operationRef.get()), retryDelay,
                    TimeUnit.MILLISECONDS);
            return future;
        };
    }

    @VisibleForTesting
    CompletableFuture<TopicListWatcher> getWatcherFuture(long watcherId) {
        return watchers.get(watcherId);
    }
}
