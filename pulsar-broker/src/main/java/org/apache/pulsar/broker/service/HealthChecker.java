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

import com.google.common.util.concurrent.MoreExecutors;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.util.ScheduledExecutorProvider;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicVersion;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.MetadataStoreException;

/**
 * The HealthChecker class provides functionality to monitor and verify the health of a Pulsar broker.
 * It performs health checks by creating test topics, producing and consuming messages to verify broker functionality.
 * This class implements AutoCloseable to ensure proper cleanup of resources when the broker is shut down.
 * Tests are in AdminApiHealthCheckTest class.
 */
@Slf4j
public class HealthChecker implements AutoCloseable{
    /**
     * Suffix used for health check topic names.
     */
    public static final String HEALTH_CHECK_TOPIC_SUFFIX = "healthcheck";
    /**
     * Timeout duration for health check operations.
     * Set to 58 seconds to be shorter than the client's default 60-second timeout,
     * allowing server timeout exceptions to propagate properly to the client.
     */
    private static final Duration DEFAULT_HEALTH_CHECK_READ_TIMEOUT = Duration.ofSeconds(58);
    /**
     * Pre-created timeout exception for health check operations.
     */
    private static final TimeoutException HEALTH_CHECK_TIMEOUT_EXCEPTION =
            FutureUtil.createTimeoutException("Timeout", HealthChecker.class, "healthCheckRecursiveReadNext(...)");
    /**
     * Reference to the main Pulsar service.
     */
    private final PulsarService pulsar;
    /**
     * Topic name for v1 heartbeat checks.
     */
    private final String heartbeatTopicV1;
    /**
     * Topic name for v2 heartbeat checks.
     */
    private final String heartbeatTopicV2;
    /**
     * Pulsar client instance for health check operations.
     * A separate client is needed so that it can be shutdown before the webservice is closed.
     * Pending requests for healthchecks to the /health endpoint can be cancelled this way.
     */
    private final PulsarClient client;
    /**
     * Executor for lookup operations.
     * This is also needed so that pending healthchecks can be properly cancelled at shutdown.
     */
    private final ScheduledExecutorProvider lookupExecutor;
    /**
     * Executor for scheduled tasks.
     * This is also needed so that pending healthchecks can be properly cancelled at shutdown.
     */
    private final ScheduledExecutorProvider scheduledExecutorProvider;
    /**
     * Set of pending health check operations.
     */
    private final Set<CompletableFuture<Void>> pendingFutures = new HashSet<>();

    /**
     * Executor for health check operations.
     */
    private final ScheduledExecutorService healthCheckExecutor;

    private final Duration timeout = DEFAULT_HEALTH_CHECK_READ_TIMEOUT;

    public HealthChecker(PulsarService pulsar) throws PulsarServerException {
        this.pulsar = pulsar;
        this.heartbeatTopicV1 = getHeartbeatTopicName(pulsar.getBrokerId(), pulsar.getConfiguration(), false);
        this.heartbeatTopicV2 = getHeartbeatTopicName(pulsar.getBrokerId(), pulsar.getConfiguration(), true);
        this.lookupExecutor =
                new ScheduledExecutorProvider(1, "health-checker-client-lookup-executor");
        this.scheduledExecutorProvider =
                new ScheduledExecutorProvider(1, "health-checker-client-scheduled-executor");
        this.healthCheckExecutor =
                Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory("health-checker-executor"));
        try {
            this.client = pulsar.createClientImpl(builder -> {
                builder.lookupExecutorProvider(lookupExecutor);
                builder.scheduledExecutorProvider(scheduledExecutorProvider);
            });
        } catch (PulsarClientException e) {
            throw new PulsarServerException("Error creating client for HealthChecker", e);
        }
    }

    private static String getHeartbeatTopicName(String brokerId, ServiceConfiguration configuration, boolean isV2) {
        NamespaceName namespaceName = isV2
                ? NamespaceService.getHeartbeatNamespaceV2(brokerId, configuration)
                : NamespaceService.getHeartbeatNamespace(brokerId, configuration);
        return String.format("persistent://%s/%s", namespaceName, HEALTH_CHECK_TOPIC_SUFFIX);
    }

    /**
     * Performs a health check on the broker by verifying message production and consumption.
     * The health check process includes:
     * 1. Producing a test message
     * 2. Reading the message back to verify end-to-end functionality
     *
     * @param topicVersion The version of the topic to use (V1 or V2)
     * @param clientAppId  The identifier of the client application requesting the health check
     * @return A CompletableFuture that completes when the health check is successful, or completes exceptionally if the
     * check fails
     */
    public CompletableFuture<Void> checkHealth(TopicVersion topicVersion, String clientAppId) {
        final String topicName = topicVersion == TopicVersion.V2 ? heartbeatTopicV2 : heartbeatTopicV1;
        log.info("[{}] Running healthCheck with topic={}", clientAppId, topicName);
        final String messageStr = UUID.randomUUID().toString();
        final String subscriptionName = "healthCheck-" + messageStr;

        CompletableFuture<Void> resultFuture = new CompletableFuture<>();
        healthCheckExecutor.execute(
                () -> doHealthCheck(clientAppId, resultFuture, topicName, subscriptionName, messageStr));
        return resultFuture;
    }

    private void doHealthCheck(String clientAppId, CompletableFuture<Void> resultFuture, String topicName,
                           String subscriptionName, String messageStr) {
        addToPending(resultFuture);
        resultFuture.whenCompleteAsync((result, ex) -> {
            removeFromPending(resultFuture);
        }, healthCheckExecutor);
        try {
            pulsar.getBrokerService().getTopic(topicName, true)
                    .thenComposeAsync(topicOptional -> {
                        if (!topicOptional.isPresent()) {
                            log.error("[{}] Fail to run health check while get topic {}. because get null value.",
                                    clientAppId, topicName);
                            return CompletableFuture.failedFuture(new BrokerServiceException.TopicNotFoundException(
                                    String.format("Topic [%s] not found after create.", topicName)));
                        }
                        return doHealthCheck(clientAppId, topicName, subscriptionName, messageStr, resultFuture);
                    }, healthCheckExecutor).whenComplete((result, t) -> {
                        if (t != null) {
                            resultFuture.completeExceptionally(t);
                        } else {
                            if (!resultFuture.isDone()) {
                                resultFuture.complete(null);
                            }
                        }
                    });
        } catch (Exception e) {
            log.error("[{}] Fail to run health check while get topic {}. because get exception.",
                    clientAppId, topicName, e);
            resultFuture.completeExceptionally(e);
        }
    }

    private synchronized void addToPending(CompletableFuture<Void> resultFuture) {
        pendingFutures.add(resultFuture);
    }

    private synchronized void removeFromPending(CompletableFuture<Void> resultFuture) {
        pendingFutures.remove(resultFuture);
    }

    private CompletableFuture<Void> doHealthCheck(String clientAppId, String topicName, String subscriptionName,
                                                  String messageStr, CompletableFuture<Void> resultFuture) {
        return client.newProducer(Schema.STRING)
                .topic(topicName)
                .sendTimeout((int) timeout.toMillis(), TimeUnit.MILLISECONDS)
                .enableBatching(false)
                .createAsync()
                .thenCompose(producer -> client.newReader(Schema.STRING)
                        .topic(topicName)
                        .subscriptionName(subscriptionName)
                        .startMessageId(MessageId.latest)
                        .createAsync()
                        .exceptionally(createException -> {
                            producer.closeAsync().exceptionally(ex -> {
                                log.error("[{}] Close producer fail while heath check.", clientAppId);
                                return null;
                            });
                            throw FutureUtil.wrapToCompletionException(createException);
                        })
                        .thenCompose(reader -> producer.sendAsync(messageStr)
                                .thenCompose(__ -> FutureUtil.addTimeoutHandling(
                                        healthCheckRecursiveReadNext(reader, messageStr),
                                        timeout, healthCheckExecutor,
                                        () -> HEALTH_CHECK_TIMEOUT_EXCEPTION))
                                .whenCompleteAsync((__, ex) -> {
                                    closeAndReCheck(producer, reader, topicName, subscriptionName, clientAppId)
                                            .whenComplete((unused, innerEx) -> {
                                                if (ex != null) {
                                                    resultFuture.completeExceptionally(ex);
                                                } else {
                                                    resultFuture.complete(null);
                                                }
                                            });
                                }, healthCheckExecutor))
                ).exceptionally(ex -> {
                    resultFuture.completeExceptionally(ex);
                    return null;
                });
    }

    /**
     * Close producer and reader and then to re-check if this operation is success.
     *
     * Re-check
     * - Producer: If close fails we will print error log to notify user.
     * - Consumer: If close fails we will force delete subscription.
     *
     * @param producer         Producer
     * @param reader           Reader
     * @param subscriptionName Subscription name
     */
    private CompletableFuture<Void> closeAndReCheck(Producer<String> producer, Reader<String> reader,
                                                    String topicName, String subscriptionName, String clientAppId) {
        // no matter exception or success, we still need to
        // close producer/reader
        CompletableFuture<Void> producerCloseFuture = producer.closeAsync();
        CompletableFuture<Void> readerCloseFuture = reader.closeAsync();
        List<CompletableFuture<Void>> futures = new ArrayList<>(2);
        futures.add(producerCloseFuture);
        futures.add(readerCloseFuture);
        return FutureUtil.waitForAll(futures)
                .exceptionallyAsync(closeException -> {
                    if (readerCloseFuture.isCompletedExceptionally()) {
                        log.error("[{}] Close reader fail while health check.", clientAppId);
                        Optional<Topic> topic = pulsar.getBrokerService().getTopicReference(topicName);
                        if (topic.isPresent()) {
                            Subscription subscription =
                                    topic.get().getSubscription(subscriptionName);
                            // re-check subscription after reader close
                            if (subscription != null) {
                                log.warn("[{}] Force delete subscription {} "
                                                + "when it still exists after the"
                                                + " reader is closed.",
                                        clientAppId, subscription);
                                subscription.deleteForcefully()
                                        .exceptionally(ex -> {
                                            log.error("[{}] Force delete subscription fail"
                                                            + " while health check",
                                                    clientAppId, ex);
                                            return null;
                                        });
                            }
                        }
                    } else {
                        // producer future fail.
                        log.error("[{}] Close producer fail while heath check.", clientAppId);
                    }
                    return null;
                }, healthCheckExecutor);
    }

    private static CompletableFuture<Void> healthCheckRecursiveReadNext(Reader<String> reader, String content) {
        return reader.readNextAsync()
                .thenCompose(msg -> {
                    if (!Objects.equals(content, msg.getValue())) {
                        return healthCheckRecursiveReadNext(reader, content);
                    }
                    return CompletableFuture.completedFuture(null);
                });
    }

    private void deleteHeartbeatTopics() {
        log.info("forcefully deleting heartbeat topics");
        deleteTopic(heartbeatTopicV1);
        deleteTopic(heartbeatTopicV2);
        log.info("finish forcefully deleting heartbeat topics");
    }

    private void deleteTopic(String topicName) {
        try {
            pulsar.getBrokerService().deleteTopic(topicName, true)
                    .get(pulsar.getConfiguration().getMetadataStoreOperationTimeoutSeconds(), TimeUnit.SECONDS);
        } catch (Exception e) {
            Throwable realCause = e.getCause();
            if (!(realCause instanceof ManagedLedgerException.MetadataNotFoundException
                    || realCause instanceof MetadataStoreException.NotFoundException)) {
                log.error("Errors in deleting heartbeat topic [{}]", topicName, e);
            }
        }
    }

    @Override
    public synchronized void close() throws Exception {
        try {
            scheduledExecutorProvider.shutdownNow();
        } catch (Exception e) {
            log.warn("Failed to shutdown scheduled executor", e);
        }
        try {
            lookupExecutor.shutdownNow();
        } catch (Exception e) {
            log.warn("Failed to shutdown lookup executor", e);
        }
        try {
            client.close();
        } catch (PulsarClientException e) {
            log.warn("Failed to close pulsar client", e);
        }
        for (CompletableFuture<Void> pendingFuture : new ArrayList<>(pendingFutures)) {
            if (!pendingFuture.isDone()) {
                healthCheckExecutor.submit(() -> {
                    try {
                        pendingFuture.completeExceptionally(
                                new PulsarClientException.AlreadyClosedException("HealthChecker is closed"));
                    } catch (Exception e) {
                        log.warn("Failed to complete pending future", e);
                    }
                });
            }
        }
        boolean terminated = MoreExecutors.shutdownAndAwaitTermination(healthCheckExecutor, 10, TimeUnit.SECONDS);
        if (!terminated) {
            log.warn("Failed to shutdown health check executor in 10 seconds");
        }
        deleteHeartbeatTopics();
    }
}
