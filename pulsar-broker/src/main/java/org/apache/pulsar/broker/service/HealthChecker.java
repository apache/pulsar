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

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicVersion;
import org.apache.pulsar.common.util.FutureUtil;

@Slf4j
public class HealthChecker {
    public static final String HEALTH_CHECK_TOPIC_SUFFIX = "healthcheck";
    // there is a timeout of 60 seconds default in the client(readTimeoutMs), so we need to set the timeout
    // a bit shorter than 60 seconds to avoid the client timeout exception thrown before the server timeout exception.
    // or we can't propagate the server timeout exception to the client.
    private static final Duration HEALTH_CHECK_READ_TIMEOUT = Duration.ofSeconds(58);
    private static final TimeoutException HEALTH_CHECK_TIMEOUT_EXCEPTION =
            FutureUtil.createTimeoutException("Timeout", HealthChecker.class, "healthCheckRecursiveReadNext(...)");

    public static String getHeartbeatTopicName(String brokerId, ServiceConfiguration configuration, boolean isV2) {
        NamespaceName namespaceName = isV2
                ? NamespaceService.getHeartbeatNamespaceV2(brokerId, configuration)
                : NamespaceService.getHeartbeatNamespace(brokerId, configuration);
        return String.format("persistent://%s/%s", namespaceName, HEALTH_CHECK_TOPIC_SUFFIX);
    }

    public static CompletableFuture<Void> internalRunHealthCheck(TopicVersion topicVersion, PulsarService pulsar,
                                                                 String clientAppId) {
        NamespaceName namespaceName = (topicVersion == TopicVersion.V2)
                ? NamespaceService.getHeartbeatNamespaceV2(pulsar.getAdvertisedAddress(), pulsar.getConfiguration())
                : NamespaceService.getHeartbeatNamespace(pulsar.getAdvertisedAddress(), pulsar.getConfiguration());
        String brokerId = pulsar.getBrokerId();
        final String topicName =
                getHeartbeatTopicName(brokerId, pulsar.getConfiguration(), (topicVersion == TopicVersion.V2));
        log.info("[{}] Running healthCheck with topic={}", clientAppId, topicName);
        final String messageStr = UUID.randomUUID().toString();
        final String subscriptionName = "healthCheck-" + messageStr;
        // create non-partitioned topic manually and close the previous reader if present.
        return pulsar.getBrokerService().getTopic(topicName, true)
                .thenCompose(topicOptional -> {
                    if (!topicOptional.isPresent()) {
                        log.error("[{}] Fail to run health check while get topic {}. because get null value.",
                                clientAppId, topicName);
                        return CompletableFuture.failedFuture(new BrokerServiceException.TopicNotFoundException(
                                String.format("Topic [%s] not found after create.", topicName)));
                    }
                    PulsarClient client;
                    try {
                        client = pulsar.getClient();
                    } catch (PulsarServerException e) {
                        log.error("[{}] Fail to run health check while get client.", clientAppId);
                        return CompletableFuture.failedFuture(e);
                    }
                    CompletableFuture<Void> resultFuture = new CompletableFuture<>();
                    client.newProducer(Schema.STRING).topic(topicName).createAsync()
                            .thenCompose(producer -> client.newReader(Schema.STRING).topic(topicName)
                                    .subscriptionName(subscriptionName)
                                    .startMessageId(MessageId.latest)
                                    .createAsync().exceptionally(createException -> {
                                        producer.closeAsync().exceptionally(ex -> {
                                            log.error("[{}] Close producer fail while heath check.", clientAppId);
                                            return null;
                                        });
                                        throw FutureUtil.wrapToCompletionException(createException);
                                    }).thenCompose(reader -> producer.sendAsync(messageStr)
                                            .thenCompose(__ -> FutureUtil.addTimeoutHandling(
                                                    healthCheckRecursiveReadNext(reader, messageStr),
                                                    HEALTH_CHECK_READ_TIMEOUT, pulsar.getBrokerService().executor(),
                                                    () -> HEALTH_CHECK_TIMEOUT_EXCEPTION))
                                            .whenComplete((__, ex) -> {
                                                        closeAndReCheck(producer, reader, topicOptional.get(),
                                                                subscriptionName,
                                                                clientAppId)
                                                                .whenComplete((unused, innerEx) -> {
                                                                    if (ex != null) {
                                                                        resultFuture.completeExceptionally(ex);
                                                                    } else {
                                                                        resultFuture.complete(null);
                                                                    }
                                                                });
                                                    }
                                            ))
                            ).exceptionally(ex -> {
                                resultFuture.completeExceptionally(ex);
                                return null;
                            });
                    return resultFuture;
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
     * @param topic            Topic
     * @param subscriptionName Subscription name
     */
    private static CompletableFuture<Void> closeAndReCheck(Producer<String> producer, Reader<String> reader,
                                                           Topic topic, String subscriptionName, String clientAppId) {
        // no matter exception or success, we still need to
        // close producer/reader
        CompletableFuture<Void> producerFuture = producer.closeAsync();
        CompletableFuture<Void> readerFuture = reader.closeAsync();
        List<CompletableFuture<Void>> futures = new ArrayList<>(2);
        futures.add(producerFuture);
        futures.add(readerFuture);
        return FutureUtil.waitForAll(Collections.unmodifiableList(futures))
                .exceptionally(closeException -> {
                    if (readerFuture.isCompletedExceptionally()) {
                        log.error("[{}] Close reader fail while heath check.", clientAppId);
                        Subscription subscription =
                                topic.getSubscription(subscriptionName);
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
                    } else {
                        // producer future fail.
                        log.error("[{}] Close producer fail while heath check.", clientAppId);
                    }
                    return null;
                });
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
}
