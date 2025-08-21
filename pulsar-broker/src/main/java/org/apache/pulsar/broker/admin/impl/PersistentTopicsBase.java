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
package org.apache.pulsar.broker.admin.impl;

import static org.apache.pulsar.common.naming.SystemTopicNames.isSystemTopic;
import static org.apache.pulsar.common.naming.SystemTopicNames.isTransactionCoordinatorAssign;
import static org.apache.pulsar.common.naming.SystemTopicNames.isTransactionInternalName;
import static org.apache.pulsar.common.naming.TopicName.PARTITIONED_TOPIC_SUFFIX;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectReader;
import com.github.zafarkhaja.semver.Version;
import com.google.common.base.Throwables;
import com.google.common.collect.Sets;
import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nonnull;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.StreamingOutput;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ManagedLedgerInfoCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerInfo;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.ScanOutcome;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerOfflineBacklog;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.ManagedLedgerInfo.LedgerInfo;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.admin.AdminResource;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.authorization.AuthorizationService;
import org.apache.pulsar.broker.event.data.SubscriptionCreateEventData;
import org.apache.pulsar.broker.event.data.SubscriptionDeleteEventData;
import org.apache.pulsar.broker.event.data.SubscriptionSeekEventData;
import org.apache.pulsar.broker.event.data.TopicDeleteEventData;
import org.apache.pulsar.broker.event.data.TopicMetadataUpdateEventData;
import org.apache.pulsar.broker.service.AnalyzeBacklogResult;
import org.apache.pulsar.broker.service.BrokerService.TopicLoadingContext;
import org.apache.pulsar.broker.service.BrokerServiceException.AlreadyRunningException;
import org.apache.pulsar.broker.service.BrokerServiceException.SubscriptionBusyException;
import org.apache.pulsar.broker.service.BrokerServiceException.SubscriptionInvalidCursorPosition;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.TopicEventsListener.EventStage;
import org.apache.pulsar.broker.service.TopicEventsListener.TopicEvent;
import org.apache.pulsar.broker.service.persistent.PersistentReplicator;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.client.admin.LongRunningProcessStatus;
import org.apache.pulsar.client.admin.OffloadProcessStatus;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.PulsarAdminException.NotFoundException;
import org.apache.pulsar.client.admin.PulsarAdminException.PreconditionFailedException;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.api.proto.BrokerEntryMetadata;
import org.apache.pulsar.common.api.proto.CommandSubscribe.InitialPosition;
import org.apache.pulsar.common.api.proto.CommandSubscribe.SubType;
import org.apache.pulsar.common.api.proto.EncryptionKeys;
import org.apache.pulsar.common.api.proto.KeyValue;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.compression.CompressionCodec;
import org.apache.pulsar.common.compression.CompressionCodecProvider;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.PartitionedManagedLedgerInfo;
import org.apache.pulsar.common.naming.SystemTopicNames;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.AuthPolicies;
import org.apache.pulsar.common.policies.data.AutoSubscriptionCreationOverride;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.DelayedDeliveryPolicies;
import org.apache.pulsar.common.policies.data.DispatchRate;
import org.apache.pulsar.common.policies.data.EntryFilters;
import org.apache.pulsar.common.policies.data.InactiveTopicPolicies;
import org.apache.pulsar.common.policies.data.NamespaceOperation;
import org.apache.pulsar.common.policies.data.OffloadPoliciesImpl;
import org.apache.pulsar.common.policies.data.PartitionedTopicInternalStats;
import org.apache.pulsar.common.policies.data.PersistencePolicies;
import org.apache.pulsar.common.policies.data.PersistentOfflineTopicStats;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.PolicyName;
import org.apache.pulsar.common.policies.data.PolicyOperation;
import org.apache.pulsar.common.policies.data.PublishRate;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.SchemaCompatibilityStrategy;
import org.apache.pulsar.common.policies.data.SubscribeRate;
import org.apache.pulsar.common.policies.data.SubscriptionPolicies;
import org.apache.pulsar.common.policies.data.TopicOperation;
import org.apache.pulsar.common.policies.data.TopicPolicies;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.apache.pulsar.common.policies.data.impl.AutoSubscriptionCreationOverrideImpl;
import org.apache.pulsar.common.policies.data.impl.BacklogQuotaImpl;
import org.apache.pulsar.common.policies.data.impl.DispatchRateImpl;
import org.apache.pulsar.common.policies.data.stats.PartitionedTopicStatsImpl;
import org.apache.pulsar.common.policies.data.stats.TopicStatsImpl;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.stats.AnalyzeSubscriptionBacklogResult;
import org.apache.pulsar.common.util.DateFormatter;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.collections.BitSetRecyclable;
import org.apache.pulsar.compaction.Compactor;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.transaction.coordinator.TransactionCoordinatorID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
public class PersistentTopicsBase extends AdminResource {
    private static final Logger log = LoggerFactory.getLogger(PersistentTopicsBase.class);

    private static final int OFFLINE_TOPIC_STAT_TTL_MINS = 10;
    private static final String DEPRECATED_CLIENT_VERSION_PREFIX = "Pulsar-CPP-v";
    private static final Version LEAST_SUPPORTED_CLIENT_VERSION_PREFIX = Version.forIntegers(1, 21);

    protected CompletableFuture<List<String>> internalGetListAsync(Optional<String> bundle) {
        return validateNamespaceOperationAsync(namespaceName, NamespaceOperation.GET_TOPICS)
            .thenCompose(__ -> namespaceResources().namespaceExistsAsync(namespaceName))
            .thenAccept(exists -> {
                if (!exists) {
                    throw new RestException(Status.NOT_FOUND, "Namespace does not exist");
                }
            })
            .thenCompose(__ -> topicResources().listPersistentTopicsAsync(namespaceName))
            .thenApply(topics ->
                topics.stream()
                    .filter(topic -> {
                        if (isTransactionInternalName(TopicName.get(topic))) {
                            return false;
                        }
                        if (bundle.isPresent()) {
                            NamespaceBundle b = pulsar().getNamespaceService().getNamespaceBundleFactory()
                                .getBundle(TopicName.get(topic));
                            return b != null && bundle.get().equals(b.getBundleRange());
                        }
                        return true;
                    })
                    .collect(Collectors.toList())
            );
    }

    protected CompletableFuture<List<String>> internalGetListAsync() {
        return validateNamespaceOperationAsync(namespaceName, NamespaceOperation.GET_TOPICS)
                .thenCompose(__ -> namespaceResources().namespaceExistsAsync(namespaceName))
                .thenAccept(exists -> {
                    if (!exists) {
                        throw new RestException(Status.NOT_FOUND, "Namespace does not exist");
                    }
                })
                .thenCompose(__ -> topicResources().listPersistentTopicsAsync(namespaceName))
                .thenApply(topics -> topics.stream().filter(topic ->
                        !isTransactionInternalName(TopicName.get(topic))).collect(Collectors.toList()));
    }

    protected CompletableFuture<List<String>> internalGetPartitionedTopicListAsync() {
        return validateNamespaceOperationAsync(namespaceName, NamespaceOperation.GET_TOPICS)
                .thenCompose(__ -> namespaceResources().namespaceExistsAsync(namespaceName))
                .thenCompose(namespaceExists -> {
                    // Validate that namespace exists, throws 404 if it doesn't exist
                    if (!namespaceExists) {
                        log.warn("[{}] Failed to get partitioned topic list {}: Namespace does not exist",
                                clientAppId(), namespaceName);
                        throw new RestException(Status.NOT_FOUND, "Namespace does not exist");
                    } else {
                        return getPartitionedTopicListAsync(TopicDomain.getEnum(domain()));
                    }
                });
    }

    protected CompletableFuture<Map<String, Set<AuthAction>>> internalGetPermissionsOnTopic() {
        // This operation should be reading from zookeeper and it should be allowed without having admin privileges
        CompletableFuture<Void> validateAccessForTenantCf =
                validateAdminAccessForTenantAsync(namespaceName.getTenant());

        var checkIfTopicExists = !pulsar().getConfiguration().isAllowAclChangesOnNonExistentTopics();
        if (checkIfTopicExists) {
            validateAccessForTenantCf = validateAccessForTenantCf
                    .thenCompose(__ -> internalCheckTopicExists(topicName));
        }

        return validateAccessForTenantCf
                .thenCompose(__ -> namespaceResources().getPoliciesAsync(namespaceName)
            .thenApply(policies -> {
                if (!policies.isPresent()) {
                    throw new RestException(Status.NOT_FOUND, "Namespace does not exist");
                }

                Map<String, Set<AuthAction>> permissions = new HashMap<>();
                String topicUri = topicName.toString();
                AuthPolicies auth = policies.get().auth_policies;
                // First add namespace level permissions
                auth.getNamespaceAuthentication().forEach(permissions::put);

                // Then add topic level permissions
                if (auth.getTopicAuthentication().containsKey(topicUri)) {
                    for (Map.Entry<String, Set<AuthAction>> entry :
                            auth.getTopicAuthentication().get(topicUri).entrySet()) {
                        String role = entry.getKey();
                        Set<AuthAction> topicPermissions = entry.getValue();

                        if (!permissions.containsKey(role)) {
                            permissions.put(role, topicPermissions);
                        } else {
                            // Do the union between namespace and topic level
                            Set<AuthAction> union = Sets.union(permissions.get(role), topicPermissions);
                            permissions.put(role, union);
                        }
                    }
                }
                return permissions;
            }));
    }

    protected void validateCreateTopic(TopicName topicName) {
        if (isTransactionInternalName(topicName)) {
            log.warn("Forbidden to create transaction internal topic: {}", topicName);
            throw new RestException(Status.BAD_REQUEST, "Cannot create topic in system topic format!");
        }
    }

    public void validateAdminOperationOnTopic(boolean authoritative) {
        validateAdminAccessForTenant(topicName.getTenant());
        validateTopicOwnership(topicName, authoritative);
    }

    private CompletableFuture<Void> grantPermissionsAsync(TopicName topicUri, String role, Set<AuthAction> actions) {
        AuthorizationService authService = pulsar().getBrokerService().getAuthorizationService();
        if (null != authService) {
            return authService.grantPermissionAsync(topicUri, actions, role, null/*additional auth-data json*/)
                    .thenAccept(__ -> log.info("[{}] Successfully granted access for role {}: {} - topic {}",
                            clientAppId(), role, actions, topicUri))
                    .exceptionally(ex -> {
                        Throwable realCause = FutureUtil.unwrapCompletionException(ex);
                        //The IllegalArgumentException and the IllegalStateException were historically thrown by the
                        // grantPermissionAsync method, so we catch them here to ensure backwards compatibility.
                        if (realCause instanceof MetadataStoreException.NotFoundException
                                || realCause instanceof IllegalArgumentException) {
                            log.warn("[{}] Failed to set permissions for topic {}: Namespace does not exist",
                                    clientAppId(), topicUri, realCause);
                            throw new RestException(Status.NOT_FOUND, "Topic's namespace does not exist");
                        } else if (realCause instanceof MetadataStoreException.BadVersionException
                                || realCause instanceof IllegalStateException) {
                            log.warn("[{}] Failed to set permissions for topic {}: {}", clientAppId(), topicUri,
                                    realCause.getMessage(), realCause);
                            throw new RestException(Status.CONFLICT, "Concurrent modification");
                        } else {
                            log.error("[{}] Failed to get permissions for topic {}", clientAppId(), topicUri,
                                    realCause);
                            throw new RestException(realCause);
                        }
                    });
        } else {
            String msg = "Authorization is not enabled";
            return FutureUtil.failedFuture(new RestException(Status.NOT_IMPLEMENTED, msg));
        }
    }

    protected void internalGrantPermissionsOnTopic(final AsyncResponse asyncResponse, String role,
                                                   Set<AuthAction> actions) {
        // This operation should be reading from zookeeper and it should be allowed without having admin privileges
        CompletableFuture<Void> validateAccessForTenantCf = validateAdminAccessForTenantAsync(namespaceName.getTenant())
                .thenCompose(__ -> validatePoliciesReadOnlyAccessAsync());

        var checkIfTopicExists = !pulsar().getConfiguration().isAllowAclChangesOnNonExistentTopics();
        if (checkIfTopicExists) {
            validateAccessForTenantCf = validateAccessForTenantCf
                    .thenCompose(__ -> internalCheckTopicExists(topicName));
        }

        validateAccessForTenantCf
                .thenCompose(unused1 -> grantPermissionsAsync(topicName, role, actions))
                .thenAccept(unused -> asyncResponse.resume(Response.noContent().build()))
                .exceptionally(ex -> {
                    Throwable realCause = FutureUtil.unwrapCompletionException(ex);
                    log.error("[{}] Failed to get permissions for topic {}", clientAppId(), topicName, realCause);
                    resumeAsyncResponseExceptionally(asyncResponse, realCause);
                    return null;
                });
    }

    private CompletableFuture<Void> revokePermissionsAsync(String topicUri, String role, boolean force) {
        return namespaceResources().getPoliciesAsync(namespaceName).thenCompose(
                policiesOptional -> {
                    Policies policies = policiesOptional.orElseThrow(() ->
                            new RestException(Status.NOT_FOUND, "Namespace does not exist"));
                    if (!policies.auth_policies.getTopicAuthentication().containsKey(topicUri)
                            || !policies.auth_policies.getTopicAuthentication().get(topicUri).containsKey(role)) {
                        log.warn("[{}] Failed to revoke permission from role {} on topic: Not set at topic level {}",
                                clientAppId(), role, topicUri);
                        if (force) {
                            return CompletableFuture.completedFuture(null);
                        } else {
                            return FutureUtil.failedFuture(new RestException(Status.PRECONDITION_FAILED,
                                    "Permissions are not set at the topic level"));
                        }
                    }
                    // Write the new policies to metadata store
                    return namespaceResources().setPoliciesAsync(namespaceName, p -> {
                        p.auth_policies.getTopicAuthentication().computeIfPresent(topicUri, (k, roles) -> {
                            roles.remove(role);
                            if (roles.isEmpty()) {
                                return null;
                            }
                            return roles;
                        });
                        return p;
                    }).thenAccept(__ ->
                            log.info("[{}] Successfully revoke access for role {} - topic {}", clientAppId(), role,
                            topicUri)
                    );
                }
        );
    }

    protected void internalRevokePermissionsOnTopic(AsyncResponse asyncResponse, String role) {
        // This operation should be reading from zookeeper and it should be allowed without having admin privileges
        CompletableFuture<Void> validateAccessForTenantCf =
                validateAdminAccessForTenantAsync(namespaceName.getTenant())
                        .thenCompose(__ -> validatePoliciesReadOnlyAccessAsync());

        var checkIfTopicExists = !pulsar().getConfiguration().isAllowAclChangesOnNonExistentTopics();
        if (checkIfTopicExists) {
            validateAccessForTenantCf = validateAccessForTenantCf
                    .thenCompose(__ -> internalCheckTopicExists(topicName));
        }

        validateAccessForTenantCf
                .thenCompose(unused1 -> getPartitionedTopicMetadataAsync(topicName, true, false)
                .thenCompose(metadata -> {
                    int numPartitions = metadata.partitions;
                    CompletableFuture<Void> future = CompletableFuture.completedFuture(null);
                    if (numPartitions > 0) {
                        for (int i = 0; i < numPartitions; i++) {
                            TopicName topicNamePartition = topicName.getPartition(i);
                            future = future.thenComposeAsync(unused ->
                                    revokePermissionsAsync(topicNamePartition.toString(), role, true));
                        }
                    }
                    return future.thenComposeAsync(unused -> revokePermissionsAsync(topicName.toString(), role, false))
                            .thenAccept(unused -> asyncResponse.resume(Response.noContent().build()));
                })
            ).exceptionally(ex -> {
                    Throwable realCause = FutureUtil.unwrapCompletionException(ex);
                    log.error("[{}] Failed to revoke permissions for topic {}", clientAppId(), topicName, realCause);
                    resumeAsyncResponseExceptionally(asyncResponse, realCause);
                    return null;
                });
    }

    protected CompletableFuture<Void> internalCreateNonPartitionedTopicAsync(boolean authoritative,
                                                     Map<String, String> properties) {
        CompletableFuture<Void> ret = validateNonPartitionTopicNameAsync(topicName.getLocalName());
        if (topicName.isGlobal()) {
            ret = ret.thenCompose(__ -> validateGlobalNamespaceOwnershipAsync(namespaceName));
        }
        return ret.thenCompose(__ -> validateTopicOwnershipAsync(topicName, authoritative))
           .thenCompose(__ -> validateNamespaceOperationAsync(topicName.getNamespaceObject(),
                   NamespaceOperation.CREATE_TOPIC))
           .thenCompose(__ -> getPartitionedTopicMetadataAsync(topicName, false, false))
           .thenAccept(partitionMetadata -> {
               if (partitionMetadata.partitions > 0) {
                   log.warn("[{}] Partitioned topic with the same name already exists {}", clientAppId(), topicName);
                   throw new RestException(Status.CONFLICT, "This topic already exists");
               }
           })
           .thenCompose(__ -> pulsar().getBrokerService().getTopicIfExists(topicName.toString()))
           .thenCompose(existedTopic -> {
               if (existedTopic.isPresent()) {
                   log.warn("[{}] Topic {} already exists", clientAppId(), topicName);
                   throw new RestException(Status.CONFLICT, "This topic already exists");
               }
               return pulsar().getBrokerService()
                       .getTopic(TopicLoadingContext.builder()
                               .topic(topicName.toString())
                               .createIfMissing(true)
                               .properties(properties)
                               .clientVersion(getClientVersion())
                               .build()
                       );
           })
           .thenAccept(__ -> log.info("[{}] Successfully created non-partitioned topic {}", clientAppId(), topicName));
    }

    /**
     * It updates number of partitions of an existing partitioned topic. It requires partitioned-topic to
     * already exist and number of new partitions must be greater than existing number of partitions. Decrementing
     * number of partitions requires deletion of topic which is not supported.
     *
     * @exception RestException Unprocessable entity, status code: 422. throw it when some pre-check failed.
     * @exception RestException Internal Server Error, status code: 500. throw it when get unknown Exception
     */
    protected @Nonnull CompletableFuture<Void> internalUpdatePartitionedTopicAsync(int expectPartitions,
                                                                                   boolean updateLocal,
                                                                                   boolean force) {
        PulsarService pulsarService = pulsar();
        return pulsarService.getBrokerService().fetchPartitionedTopicMetadataAsync(topicName)
            .thenCompose(partitionedTopicMetadata -> {
                int currentMetadataPartitions = partitionedTopicMetadata.partitions;
                if (currentMetadataPartitions <= 0) {
                    throw new RestException(Status.CONFLICT /* Unprocessable entity*/,
                            String.format("Topic %s is not the partitioned topic.", topicName));
                }
                if (expectPartitions < currentMetadataPartitions) {
                    throw new RestException(422 /* Unprocessable entity*/,
                            String.format("Expect partitions %s can't less than current partitions %s.",
                                    expectPartitions, currentMetadataPartitions));
                }
                int brokerMaximumPartitionsPerTopic = pulsarService.getConfiguration()
                        .getMaxNumPartitionsPerPartitionedTopic();
                if (brokerMaximumPartitionsPerTopic > 0 && expectPartitions > brokerMaximumPartitionsPerTopic) {
                    throw new RestException(422 /* Unprocessable entity*/,
                            String.format("Expect partitions %s grater than maximum partitions per topic %s",
                                    expectPartitions, brokerMaximumPartitionsPerTopic));
                }
                final PulsarAdmin admin;
                try {
                    admin = pulsarService.getAdminClient();
                } catch (PulsarServerException ex) {
                    throw new RestException(Status.INTERNAL_SERVER_ERROR, Throwables.getRootCause(ex));
                }
                final CompletableFuture<Void> checkFuture;
                if (!force) {
                    checkFuture = admin.topics().getListAsync(topicName.getNamespace(), topicName.getDomain())
                            .thenAccept(topics -> {
                                final List<TopicName> existingPartitions = topics.stream()
                                        .map(TopicName::get)
                                        .filter(candidateTopicName -> candidateTopicName.getPartitionedTopicName()
                                                .equals(topicName.getPartitionedTopicName()))
                                        .collect(Collectors.toList());
                                    // Check whether exist unexpected partition
                                    Optional<Integer> maximumPartitionIndex = existingPartitions.stream()
                                            .map(TopicName::getPartitionIndex)
                                            .max(Integer::compareTo);
                                    if (maximumPartitionIndex.isPresent()
                                            && maximumPartitionIndex.get() >= currentMetadataPartitions) {
                                        List<String> unexpectedPartitions = existingPartitions.stream()
                                                .filter(candidateTopicName ->
                                                        candidateTopicName
                                                                .getPartitionIndex() > currentMetadataPartitions)
                                                .map(TopicName::toString)
                                                .collect(Collectors.toList());
                                        throw new RestException(Status.CONFLICT,
                                                String.format(
                                                        "Exist unexpected topic partition(partition index grater than"
                                                                + " current metadata maximum index %s) %s ",
                                                        currentMetadataPartitions,
                                                        StringUtils.join(unexpectedPartitions, ",")));
                                    }
                            });
                } else {
                    checkFuture = CompletableFuture.completedFuture(null);
                }
                return checkFuture.thenCompose(topics -> {
                    final CompletableFuture<Void> updateMetadataFuture = (expectPartitions == currentMetadataPartitions)
                            // current metadata partitions is equals to expect partitions
                            ? CompletableFuture.completedFuture(null)
                            // update current cluster topic metadata
                            : namespaceResources().getPartitionedTopicResources()
                            .updatePartitionedTopicAsync(topicName, m ->
                                    new PartitionedTopicMetadata(expectPartitions, m.properties))
                            .thenRun(() -> newTopicEvent(topicName, TopicEvent.TOPIC_METADATA_UPDATE)
                                    .data(TopicMetadataUpdateEventData.builder()
                                            .oldPartitions(currentMetadataPartitions)
                                            .newPartitions(expectPartitions)
                                            .build())
                                    .dispatch());
                    return updateMetadataFuture
                    // create missing partitions
                    .thenCompose(__ -> tryCreatePartitionsAsync(expectPartitions))
                    // because we should consider the compatibility.
                    // Copy subscriptions from partition 0 instead of being created by the customer
                    .thenCompose(__ ->
                            admin.topics().getStatsAsync(topicName.getPartition(0).toString())
                                    .thenCompose(stats -> {
                                        List<CompletableFuture<Void>> futures = stats.getSubscriptions().entrySet()
                                                // We must not re-create non-durable subscriptions on the new partitions
                                                .stream().filter(entry -> entry.getValue().isDurable())
                                                .map(entry -> {
                                                    final List<CompletableFuture<Void>> innerFutures =
                                                            new ArrayList<>(expectPartitions);
                                                    for (int i = 0; i < expectPartitions; i++) {
                                                        innerFutures.add(admin.topics().createSubscriptionAsync(
                                                                        topicName.getPartition(i).toString(),
                                                                        entry.getKey(), MessageId.earliest,
                                                                        entry.getValue().isReplicated(),
                                                                        entry.getValue().getSubscriptionProperties())
                                                                .exceptionally(ex -> {
                                                                    Throwable rc =
                                                                            FutureUtil.unwrapCompletionException(ex);
                                                                    if (!(rc instanceof PulsarAdminException
                                                                            .ConflictException)) {
                                                                        log.warn("[{}] got an error while copying"
                                                                                        + " the subscription to the"
                                                                                        + " partition {}.", topicName,
                                                                                Throwables.getRootCause(rc));
                                                                        throw FutureUtil.wrapToCompletionException(rc);
                                                                    }
                                                                    // Ignore subscription already exist exception
                                                                    return null;
                                                                }));
                                                    }
                                                    return FutureUtil.waitForAll(innerFutures);
                                                }).collect(Collectors.toList());
                                        return FutureUtil.waitForAll(futures);
                                    })
                    );
                }).thenCompose(__ -> {
                    if (updateLocal || !topicName.isGlobal()) {
                        return CompletableFuture.completedFuture(null);
                    }
                    // update remote cluster
                    return namespaceResources().getPoliciesAsync(namespaceName)
                            .thenCompose(policies -> {
                                if (!policies.isPresent()) {
                                    return CompletableFuture.completedFuture(null);
                                }
                                // Combine namespace level policies and topic level policies.
                                Set<String> replicationClusters = policies.get().replication_clusters;
                                TopicPolicies topicPolicies =
                                        pulsarService.getTopicPoliciesService().getTopicPoliciesIfExists(topicName);
                                if (topicPolicies != null && topicPolicies.getReplicationClusters() != null) {
                                    replicationClusters = topicPolicies.getReplicationClustersSet();
                                }
                                // Do check replicated clusters.
                                if (replicationClusters.size() == 0) {
                                    return CompletableFuture.completedFuture(null);
                                }
                                boolean containsCurrentCluster =
                                        replicationClusters.contains(pulsar().getConfig().getClusterName());
                                if (!containsCurrentCluster) {
                                    log.error("[{}] local cluster is not part of replicated cluster for namespace {}",
                                            clientAppId(), topicName);
                                    throw new RestException(422,
                                            "Local cluster is not part of replicate cluster list");
                                }
                                if (replicationClusters.size() == 1) {
                                    // The replication clusters just has the current cluster itself.
                                    return CompletableFuture.completedFuture(null);
                                }
                                // Do sync operation to other clusters.
                                List<CompletableFuture<Void>> futures = replicationClusters.stream()
                                        .map(replicationCluster -> admin.clusters().getClusterAsync(replicationCluster)
                                                .thenCompose(clusterData -> pulsarService.getBrokerService()
                                                    .getClusterPulsarAdmin(replicationCluster, Optional.of(clusterData))
                                                        .topics().updatePartitionedTopicAsync(topicName.toString(),
                                                            expectPartitions, true, force)
                                                        .exceptionally(ex -> {
                                                            log.warn("[{}][{}] Update remote cluster partition fail.",
                                                                        topicName, replicationCluster, ex);
                                                            throw FutureUtil.wrapToCompletionException(ex);
                                                        })
                                                )
                                        ).collect(Collectors.toList());
                                return FutureUtil.waitForAll(futures);
                            });
                });
            });
    }

    protected void internalCreateMissedPartitions(AsyncResponse asyncResponse) {
        getPartitionedTopicMetadataAsync(topicName, false, false).thenAccept(metadata -> {
            if (metadata != null && metadata.partitions > 0) {
                CompletableFuture<Void> future = validateNamespaceOperationAsync(topicName.getNamespaceObject(),
                        NamespaceOperation.CREATE_TOPIC);
                future.thenCompose(__ -> tryCreatePartitionsAsync(metadata.partitions)).thenAccept(v -> {
                    asyncResponse.resume(Response.noContent().build());
                }).exceptionally(e -> {
                    log.error("[{}] Failed to create partitions for topic {}", clientAppId(), topicName);
                    resumeAsyncResponseExceptionally(asyncResponse, e);
                    return null;
                });
            } else {
                throw new RestException(Status.NOT_FOUND, String.format("Topic %s does not exist", topicName));
            }
        }).exceptionally(ex -> {
            // If the exception is not redirect exception we need to log it.
            if (!isRedirectException(ex)) {
                log.error("[{}] Failed to create partitions for topic {}",
                        clientAppId(), topicName);
            }
            resumeAsyncResponseExceptionally(asyncResponse, ex);
            return null;
        });
    }

    protected CompletableFuture<Void> internalSetDelayedDeliveryPolicies(DelayedDeliveryPolicies deliveryPolicies,
                                                                         boolean isGlobal) {
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
            .thenCompose(op -> {
                TopicPolicies topicPolicies = op.orElseGet(TopicPolicies::new);
                topicPolicies.setIsGlobal(isGlobal);
                topicPolicies.setDelayedDeliveryEnabled(deliveryPolicies == null ? null : deliveryPolicies.isActive());
                topicPolicies.setDelayedDeliveryTickTimeMillis(
                        deliveryPolicies == null ? null : deliveryPolicies.getTickTime());
                return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, topicPolicies);
            });
    }

    protected PartitionedTopicMetadata internalGetPartitionedMetadata(boolean authoritative,
                                                                      boolean checkAllowAutoCreation) {
        return sync(() -> internalGetPartitionedMetadataAsync(authoritative, checkAllowAutoCreation));
    }

    protected CompletableFuture<PartitionedTopicMetadata> internalGetPartitionedMetadataAsync(
                                                                          boolean authoritative,
                                                                          boolean checkAllowAutoCreation) {
        return getPartitionedTopicMetadataAsync(topicName, authoritative, checkAllowAutoCreation)
                .thenCompose(metadata -> {
                    if (metadata.partitions > 1) {
                        // Some clients does not support partitioned topic.
                        return internalValidateClientVersionAsync().thenApply(__ -> metadata);
                    } else if (metadata.partitions == 1) {
                        return CompletableFuture.completedFuture(metadata);
                    } else {
                        // metadata.partitions == 0
                        // The topic may be a non-partitioned topic, so check if it exists here.
                        // However, when checkAllowAutoCreation is true, the client will create the topic if
                        // it doesn't exist. In this case, `partitions == 0` means the automatically created topic
                        // is a non-partitioned topic so we shouldn't check if the topic exists.
                        return pulsar().getBrokerService().isAllowAutoTopicCreationAsync(topicName)
                                .thenCompose(brokerAllowAutoTopicCreation -> {
                            if (checkAllowAutoCreation && brokerAllowAutoTopicCreation) {
                                // Whether it exists or not, auto create a non-partitioned topic by client.
                                return CompletableFuture.completedFuture(metadata);
                            } else {
                                // If it does not exist, response a Not Found error.
                                // Otherwise, response a non-partitioned metadata.
                                return internalCheckNonPartitionedTopicExists(topicName).thenApply(__ -> metadata);
                            }
                        });
                    }
                });
    }

    protected CompletableFuture<Map<String, String>> internalGetPropertiesAsync(boolean authoritative) {
        return validateTopicOwnershipAsync(topicName, authoritative)
                .thenCompose(__ -> validateTopicOperationAsync(topicName, TopicOperation.GET_METADATA))
                .thenCompose(__ -> {
                    if (topicName.isPartitioned()) {
                        return getPropertiesAsync();
                    }
                    return pulsar().getBrokerService().fetchPartitionedTopicMetadataAsync(topicName)
                            .thenCompose(metadata -> {
                                if (metadata.partitions == 0) {
                                    return getPropertiesAsync();
                                }
                                return CompletableFuture.completedFuture(metadata.properties);
                            });
                });
    }

    private CompletableFuture<Map<String, String>> getPropertiesAsync() {
        return pulsar().getBrokerService().getTopicIfExists(topicName.toString())
                .thenApply(opt -> {
                    if (!opt.isPresent()) {
                        throw new RestException(Status.NOT_FOUND,
                                getTopicNotFoundErrorMessage(topicName.toString()));
                    }
                    return ((PersistentTopic) opt.get()).getManagedLedger().getProperties();
        });
    }

    protected CompletableFuture<Void> internalUpdatePropertiesAsync(boolean authoritative,
                                                                    Map<String, String> properties) {
        if (properties == null || properties.isEmpty()) {
            log.warn("[{}] [{}] properties is empty, ignore update", clientAppId(), topicName);
            return CompletableFuture.completedFuture(null);
        }
        return validateTopicOwnershipAsync(topicName, authoritative)
            .thenCompose(__ -> validateTopicOperationAsync(topicName, TopicOperation.UPDATE_METADATA))
            .thenCompose(__ -> {
                if (topicName.isPartitioned()) {
                    return internalUpdateNonPartitionedTopicProperties(properties);
                } else {
                    return pulsar().getBrokerService().fetchPartitionedTopicMetadataAsync(topicName)
                        .thenCompose(metadata -> {
                            if (metadata.partitions == 0) {
                                return internalUpdateNonPartitionedTopicProperties(properties);
                            }
                            return namespaceResources()
                                .getPartitionedTopicResources().updatePartitionedTopicAsync(topicName,
                                    p -> new PartitionedTopicMetadata(p.partitions,
                                        p.properties == null ? properties
                                            : MapUtils.putAll(p.properties, properties.entrySet().toArray())));
                        });
                }
            }).thenAccept(__ ->
                log.info("[{}] [{}] update properties success with properties {}",
                    clientAppId(), topicName, properties));
    }

    private CompletableFuture<Void> internalUpdateNonPartitionedTopicProperties(Map<String, String> properties) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        pulsar().getBrokerService().getTopicIfExists(topicName.toString())
            .thenAccept(opt -> {
                if (!opt.isPresent()) {
                    throw new RestException(Status.NOT_FOUND,
                        getTopicNotFoundErrorMessage(topicName.toString()));
                }
                ManagedLedger managedLedger = ((PersistentTopic) opt.get()).getManagedLedger();
                managedLedger.asyncSetProperties(properties, new AsyncCallbacks.UpdatePropertiesCallback() {

                    @Override
                    public void updatePropertiesComplete(Map<String, String> properties, Object ctx) {
                        if (managedLedger.getConfig().getProperties() == null) {
                            managedLedger.getConfig().setProperties(new HashMap<>());
                        }
                        managedLedger.getConfig().getProperties().putAll(properties);

                        future.complete(null);
                    }

                    @Override
                    public void updatePropertiesFailed(ManagedLedgerException exception, Object ctx) {
                        future.completeExceptionally(exception);
                    }
                }, null);
            });
        return future;
    }

    protected CompletableFuture<Void> internalRemovePropertiesAsync(boolean authoritative, String key) {
        return validateTopicOwnershipAsync(topicName, authoritative)
                .thenCompose(__ -> validateTopicOperationAsync(topicName, TopicOperation.DELETE_METADATA))
                .thenCompose(__ -> {
                    if (topicName.isPartitioned()) {
                        return internalRemoveNonPartitionedTopicProperties(key);
                    } else {
                        return pulsar().getBrokerService().fetchPartitionedTopicMetadataAsync(topicName)
                                .thenCompose(metadata -> {
                                    if (metadata.partitions == 0) {
                                        return internalRemoveNonPartitionedTopicProperties(key);
                                    }
                                    return namespaceResources()
                                            .getPartitionedTopicResources().updatePartitionedTopicAsync(topicName,
                                                    p -> {
                                                        if (p.properties != null) {
                                                            p.properties.remove(key);
                                                        }
                                                        return new PartitionedTopicMetadata(p.partitions, p.properties);
                                                    });
                                });
                    }
                }).thenAccept(__ ->
                        log.info("[{}] remove [{}] properties success with key {}",
                                clientAppId(), topicName, key));
    }

    private CompletableFuture<Void> internalRemoveNonPartitionedTopicProperties(String key) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        pulsar().getBrokerService().getTopicIfExists(topicName.toString())
                .thenAccept(opt -> {
                    if (!opt.isPresent()) {
                        throw new RestException(Status.NOT_FOUND,
                                getTopicNotFoundErrorMessage(topicName.toString()));
                    }
                    ManagedLedger managedLedger = ((PersistentTopic) opt.get()).getManagedLedger();
                    managedLedger.asyncDeleteProperty(key, new AsyncCallbacks.UpdatePropertiesCallback() {

                        @Override
                        public void updatePropertiesComplete(Map<String, String> properties, Object ctx) {
                            future.complete(null);
                        }

                        @Override
                        public void updatePropertiesFailed(ManagedLedgerException exception, Object ctx) {
                            future.completeExceptionally(exception);
                        }
                    }, null);
                });
        return future;
    }

    protected CompletableFuture<Void> internalCheckTopicExists(TopicName topicName) {
        return pulsar().getNamespaceService().checkTopicExistsAsync(topicName)
                .thenAccept(info -> {
                    boolean exists = info.isExists();
                    info.recycle();
                    if (!exists) {
                        throw new RestException(Status.NOT_FOUND, getTopicNotFoundErrorMessage(topicName.toString()));
                    }
                });
    }

    protected CompletableFuture<Void> internalCheckNonPartitionedTopicExists(TopicName topicName) {
        return pulsar().getNamespaceService().checkNonPartitionedTopicExists(topicName)
                .thenAccept(exist -> {
                    if (!exist) {
                        throw new RestException(Status.NOT_FOUND, getTopicNotFoundErrorMessage(topicName.toString()));
                    }
                });
    }

    protected void internalDeletePartitionedTopic(AsyncResponse asyncResponse,
                                                  boolean authoritative,
                                                  boolean force) {
        newTopicEvent(topicName, TopicEvent.DELETE)
                .stage(EventStage.BEFORE).data(TopicDeleteEventData.builder().force(force).build())
                .dispatch();
        validateTopicOwnershipAsync(topicName, authoritative)
                .thenCompose(__ -> validateNamespaceOperationAsync(topicName.getNamespaceObject(),
                        NamespaceOperation.DELETE_TOPIC))
                .thenCompose(__ -> pulsar().getBrokerService()
                        .fetchPartitionedTopicMetadataAsync(topicName)
                        .thenCompose(partitionedMeta -> {
                            final int numPartitions = partitionedMeta.partitions;
                            if (numPartitions < 1) {
                                return pulsar().getNamespaceService().checkNonPartitionedTopicExists(topicName)
                                        .thenApply(exists -> {
                                    if (exists) {
                                        throw new RestException(Response.Status.CONFLICT,
                                                String.format("%s is a non-partitioned topic. Instead of calling"
                                                        + " delete-partitioned-topic please call delete.", topicName));
                                    } else {
                                        throw new RestException(Status.NOT_FOUND,
                                                String.format("Topic %s not found.", topicName));
                                    }
                                });
                            }
                            return internalRemovePartitionsAuthenticationPoliciesAsync(numPartitions)
                                    .thenCompose(unused -> internalRemovePartitionsTopicAsync(numPartitions, force));
                        })
                // Only tries to delete the znode for partitioned topic when all its partitions are successfully deleted
                ).thenCompose(ignore ->
                        pulsar().getBrokerService().deleteSchema(topicName).exceptionally(ex -> null)
                ).thenCompose(ignore ->
                        pulsar().getTopicPoliciesService().deleteTopicPoliciesAsync(topicName).exceptionally(ex -> null)
                ).thenCompose(__ -> getPulsarResources().getNamespaceResources().getPartitionedTopicResources()
                        .runWithMarkDeleteAsync(topicName, () -> namespaceResources()
                                .getPartitionedTopicResources().deletePartitionedTopicAsync(topicName)))
                .thenAccept(__ -> {
                    log.info("[{}] Deleted partitioned topic {}", clientAppId(), topicName);
                    asyncResponse.resume(Response.noContent().build());
                    newTopicEvent(topicName, TopicEvent.DELETE)
                            .data(TopicDeleteEventData.builder().force(force).build()).dispatch();
                }).exceptionally(ex -> {
                    Throwable realCause = FutureUtil.unwrapCompletionException(ex);
                    if (realCause instanceof PreconditionFailedException) {
                        asyncResponse.resume(
                                new RestException(Status.PRECONDITION_FAILED,
                                        "Topic has active producers/subscriptions"));
                    } else if (realCause instanceof WebApplicationException){
                        asyncResponse.resume(realCause);
                    } else if (realCause instanceof MetadataStoreException.NotFoundException) {
                        log.warn("Namespace policies of {} not found", topicName.getNamespaceObject());
                        asyncResponse.resume(new RestException(
                                new RestException(Status.NOT_FOUND,
                                        getPartitionedTopicNotFoundErrorMessage(topicName.toString()))));
                    } else if (realCause instanceof PulsarAdminException) {
                        asyncResponse.resume(new RestException((PulsarAdminException) realCause));
                    } else if (realCause instanceof MetadataStoreException.BadVersionException) {
                        asyncResponse.resume(new RestException(
                                new RestException(Status.CONFLICT, "Concurrent modification")));
                    } else {
                        // If the exception is not redirect exception we need to log it.
                        if (!isRedirectException(ex)) {
                            log.error("[{}] Fail to Delete partitioned topic {}", clientAppId(), topicName, realCause);
                        }
                        asyncResponse.resume(new RestException(realCause));
                    }
                    return null;
                });
    }

    private CompletableFuture<Void> internalRemovePartitionsTopicAsync(int numPartitions, boolean force) {
        return pulsar().getPulsarResources().getNamespaceResources().getPartitionedTopicResources()
                .runWithMarkDeleteAsync(topicName,
                    () -> internalRemovePartitionsTopicNoAutocreationDisableAsync(numPartitions, force));
    }

    private CompletableFuture<Void> internalRemovePartitionsTopicNoAutocreationDisableAsync(int numPartitions,
                                                                                            boolean force) {
        return FutureUtil.waitForAll(IntStream.range(0, numPartitions)
                .mapToObj(i -> {
                    TopicName topicNamePartition = topicName.getPartition(i);
                    try {
                        CompletableFuture<Void> future = new CompletableFuture<>();
                        pulsar().getAdminClient().topics()
                                .deleteAsync(topicNamePartition.toString(), force)
                                .whenComplete((r, ex) -> {
                                    if (ex != null) {
                                        Throwable realCause = FutureUtil.unwrapCompletionException(ex);
                                        if (realCause instanceof NotFoundException){
                                            // if the sub-topic is not found, the client might not have called
                                            // create producer or it might have been deleted earlier,
                                            // so we ignore the 404 error.
                                            // For all other exception,
                                            // we fail the delete partition method even if a single
                                            // partition is failed to be deleted
                                            if (log.isDebugEnabled()) {
                                                log.debug("[{}] Partition not found: {}", clientAppId(),
                                                        topicNamePartition);
                                            }
                                            future.complete(null);
                                        } else {
                                            log.error("[{}] Failed to delete partition {}", clientAppId(),
                                                    topicNamePartition, realCause);
                                            future.completeExceptionally(realCause);
                                        }
                                    } else {
                                        future.complete(null);
                                    }
                                });
                        return future;
                    } catch (PulsarServerException ex) {
                        log.error("[{}] Failed to get admin client while delete partition {}",
                                clientAppId(), topicNamePartition, ex);
                        return FutureUtil.failedFuture(ex);
                    }
                }).collect(Collectors.toList()));
    }

    private CompletableFuture<Void> internalRemovePartitionsAuthenticationPoliciesAsync(int numPartitions) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        pulsar().getPulsarResources().getNamespaceResources()
                .setPoliciesAsync(topicName.getNamespaceObject(), p -> {
                    IntStream.range(0, numPartitions)
                            .forEach(i -> p.auth_policies.getTopicAuthentication()
                                    .remove(topicName.getPartition(i).toString()));
                    p.auth_policies.getTopicAuthentication().remove(topicName.toString());
                    return p;
                })
                .whenComplete((r, ex) -> {
                    if (ex != null){
                        Throwable realCause = FutureUtil.unwrapCompletionException(ex);
                        if (realCause instanceof MetadataStoreException.NotFoundException) {
                            log.warn("Namespace policies of {} not found", topicName.getNamespaceObject());
                            future.complete(null);
                        } else {
                            log.error("Failed to delete authentication policies for partitioned topic {}",
                                    topicName, ex);
                            future.completeExceptionally(realCause);
                        }
                    } else {
                        log.info("Successfully delete authentication policies for partitioned topic {}", topicName);
                        future.complete(null);
                    }
                });
        return future;
    }

    protected void internalUnloadTopic(AsyncResponse asyncResponse, boolean authoritative) {
        log.info("[{}] Unloading topic {}", clientAppId(), topicName);
        CompletableFuture<Void> future = validateTopicOperationAsync(topicName, TopicOperation.UNLOAD);
        future.thenCompose(__ -> {
            if (topicName.isGlobal()) {
                return validateGlobalNamespaceOwnershipAsync(namespaceName);
            }
            return CompletableFuture.completedFuture(null);
        }).thenAccept(__ -> {
           // If the topic name is a partition name, no need to get partition topic metadata again
           if (topicName.isPartitioned()) {
               if (isTransactionCoordinatorAssign(topicName)) {
                   internalUnloadTransactionCoordinatorAsync(asyncResponse, authoritative);
               } else {
                   internalUnloadNonPartitionedTopicAsync(asyncResponse, authoritative);
               }
           } else {
               getPartitionedTopicMetadataAsync(topicName, authoritative, false)
                       .thenAccept(meta -> {
                           if (meta.partitions > 0) {
                               final List<CompletableFuture<Void>> futures = new ArrayList<>(meta.partitions);
                               for (int i = 0; i < meta.partitions; i++) {
                                   TopicName topicNamePartition = topicName.getPartition(i);
                                   try {
                                       futures.add(pulsar().getAdminClient().topics().unloadAsync(
                                               topicNamePartition.toString()));
                                   } catch (Exception e) {
                                       log.error("[{}] Failed to unload topic {}", clientAppId(),
                                               topicNamePartition, e);
                                       asyncResponse.resume(new RestException(e));
                                       return;
                                   }
                               }

                               FutureUtil.waitForAll(futures).handle((result, exception) -> {
                                   if (exception != null) {
                                       Throwable th = exception.getCause();
                                       if (th instanceof NotFoundException) {
                                           asyncResponse.resume(new RestException(Status.NOT_FOUND, th.getMessage()));
                                       } else if (th instanceof WebApplicationException) {
                                           asyncResponse.resume(th);
                                       } else {
                                           log.error("[{}] Failed to unload topic {}", clientAppId(), topicName,
                                                   exception);
                                           asyncResponse.resume(new RestException(exception));
                                       }
                                   } else {
                                       asyncResponse.resume(Response.noContent().build());
                                   }
                                   return null;
                               });
                           } else {
                               internalUnloadNonPartitionedTopicAsync(asyncResponse, authoritative);
                           }
                       }).exceptionally(ex -> {
                           // If the exception is not redirect exception we need to log it.
                           if (isNot307And404Exception(ex)) {
                               log.error("[{}] Failed to get partitioned metadata while unloading topic {}",
                                       clientAppId(), topicName, ex);
                           }
                           resumeAsyncResponseExceptionally(asyncResponse, ex);
                           return null;
                       });
           }
       }).exceptionally(ex -> {
           // If the exception is not redirect exception we need to log it.
           if (isNot307And404Exception(ex)) {
               log.error("[{}] Failed to validate the global namespace ownership while unloading topic {}",
                       clientAppId(), topicName, ex);
           }
           resumeAsyncResponseExceptionally(asyncResponse, ex);
           return null;
       });
    }

    protected CompletableFuture<DelayedDeliveryPolicies> internalGetDelayedDeliveryPolicies(boolean applied,
                                                                                            boolean isGlobal) {
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
            .thenApply(op -> {
                TopicPolicies policies = op.orElseGet(TopicPolicies::new);
                DelayedDeliveryPolicies delayedDeliveryPolicies = null;
                if (policies.isDelayedDeliveryEnabledSet() && policies.isDelayedDeliveryTickTimeMillisSet()) {
                    delayedDeliveryPolicies = DelayedDeliveryPolicies.builder()
                            .tickTime(policies.getDelayedDeliveryTickTimeMillis())
                            .active(policies.getDelayedDeliveryEnabled())
                            .build();
                }
                if (delayedDeliveryPolicies == null && applied) {
                    delayedDeliveryPolicies = getNamespacePolicies(namespaceName).delayed_delivery_policies;
                    if (delayedDeliveryPolicies == null) {
                        delayedDeliveryPolicies = DelayedDeliveryPolicies.builder()
                                .tickTime(pulsar().getConfiguration().getDelayedDeliveryTickTimeMillis())
                                .active(pulsar().getConfiguration().isDelayedDeliveryEnabled())
                                .build();
                    }
                }
                return delayedDeliveryPolicies;
            });
    }

    protected CompletableFuture<OffloadPoliciesImpl> internalGetOffloadPolicies(boolean applied, boolean isGlobal) {
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
            .thenApply(op -> {
                OffloadPoliciesImpl offloadPolicies = op.map(TopicPolicies::getOffloadPolicies).orElse(null);
                if (applied) {
                    OffloadPoliciesImpl namespacePolicy =
                            (OffloadPoliciesImpl) getNamespacePolicies(namespaceName).offload_policies;
                    offloadPolicies = OffloadPoliciesImpl.mergeConfiguration(offloadPolicies
                            , namespacePolicy, pulsar().getConfiguration().getProperties());
                }
                return offloadPolicies;
            });
    }

    protected CompletableFuture<Void> internalSetOffloadPolicies
            (OffloadPoliciesImpl offloadPolicies, boolean isGlobal) {
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
            .thenCompose(op -> {
                TopicPolicies topicPolicies = op.orElseGet(TopicPolicies::new);
                topicPolicies.setOffloadPolicies(offloadPolicies);
                topicPolicies.setIsGlobal(isGlobal);
                return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, topicPolicies);
            });
    }

    protected CompletableFuture<InactiveTopicPolicies> internalGetInactiveTopicPolicies
            (boolean applied, boolean isGlobal) {
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
            .thenApply(op -> op.map(TopicPolicies::getInactiveTopicPolicies)
                .orElseGet(() -> {
                    if (applied) {
                        InactiveTopicPolicies policies = getNamespacePolicies(namespaceName).inactive_topic_policies;
                        return policies == null ? new InactiveTopicPolicies(
                                config().getBrokerDeleteInactiveTopicsMode(),
                                config().getBrokerDeleteInactiveTopicsMaxInactiveDurationSeconds(),
                                config().isBrokerDeleteInactiveTopicsEnabled()) : policies;
                    }
                    return null;
                }));
    }

    protected CompletableFuture<Void> internalSetInactiveTopicPolicies
            (InactiveTopicPolicies inactiveTopicPolicies, boolean isGlobal) {
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
            .thenCompose(op -> {
                TopicPolicies topicPolicies = op.orElseGet(TopicPolicies::new);
                topicPolicies.setIsGlobal(isGlobal);
                topicPolicies.setInactiveTopicPolicies(inactiveTopicPolicies);
                return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, topicPolicies);
            });
    }

    protected CompletableFuture<Integer> internalGetMaxUnackedMessagesOnSubscription(boolean applied,
                                                                                     boolean isGlobal) {
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
            .thenApply(op -> op.map(TopicPolicies::getMaxUnackedMessagesOnSubscription)
                .orElseGet(() -> {
                    if (applied) {
                        Integer maxUnackedNum = getNamespacePolicies(namespaceName)
                                .max_unacked_messages_per_subscription;
                        return maxUnackedNum == null ? config().getMaxUnackedMessagesPerSubscription() : maxUnackedNum;
                    }
                    return null;
                }));
    }

    protected CompletableFuture<Void> internalSetMaxUnackedMessagesOnSubscription(Integer maxUnackedNum,
                                                                                  boolean isGlobal) {
        if (maxUnackedNum != null && maxUnackedNum < 0) {
            throw new RestException(Status.PRECONDITION_FAILED,
                    "maxUnackedNum must be 0 or more");
        }

        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
            .thenCompose(op -> {
                TopicPolicies topicPolicies = op.orElseGet(TopicPolicies::new);
                topicPolicies.setMaxUnackedMessagesOnSubscription(maxUnackedNum);
                topicPolicies.setIsGlobal(isGlobal);
                return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, topicPolicies);
            });
    }

    protected CompletableFuture<Integer> internalGetMaxUnackedMessagesOnConsumer(boolean applied, boolean isGlobal) {
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
            .thenApply(op -> op.map(TopicPolicies::getMaxUnackedMessagesOnConsumer)
                .orElseGet(() -> {
                    if (applied) {
                        Integer maxUnacked = getNamespacePolicies(namespaceName).max_unacked_messages_per_consumer;
                        return maxUnacked == null ? config().getMaxUnackedMessagesPerConsumer() : maxUnacked;
                    }
                    return null;
                }));
    }

    protected CompletableFuture<Void> internalSetMaxUnackedMessagesOnConsumer(Integer maxUnackedNum,
                                                                              boolean isGlobal) {
        if (maxUnackedNum != null && maxUnackedNum < 0) {
            throw new RestException(Status.PRECONDITION_FAILED,
                    "maxUnackedNum must be 0 or more");
        }

        return getTopicPoliciesAsyncWithRetry(topicName)
            .thenCompose(op -> {
                TopicPolicies topicPolicies = op.orElseGet(TopicPolicies::new);
                topicPolicies.setMaxUnackedMessagesOnConsumer(maxUnackedNum);
                topicPolicies.setIsGlobal(isGlobal);
                return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, topicPolicies);
            });
    }

    protected CompletableFuture<Void> internalSetDeduplicationSnapshotInterval(Integer interval, boolean isGlobal) {
        if (interval != null && interval < 0) {
            throw new RestException(Status.PRECONDITION_FAILED, "interval must be 0 or more");
        }
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
            .thenCompose(op -> {
                TopicPolicies policies = op.orElseGet(TopicPolicies::new);
                policies.setDeduplicationSnapshotIntervalSeconds(interval);
                policies.setIsGlobal(isGlobal);
                return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, policies);
            });
    }

    private void internalUnloadNonPartitionedTopicAsync(AsyncResponse asyncResponse, boolean authoritative) {
        validateTopicOwnershipAsync(topicName, authoritative)
                .thenCompose(__ -> getTopicReferenceAsync(topicName))
                .thenCompose(topic -> topic.close(false))
                .thenRun(() -> {
                    log.info("[{}] Successfully unloaded topic {}", clientAppId(), topicName);
                    asyncResponse.resume(Response.noContent().build());
                })
                .exceptionally(ex -> {
                    // If the exception is not redirect exception we need to log it.
                    if (isNot307And404Exception(ex)) {
                        log.error("[{}] Failed to unload topic {}, {}", clientAppId(), topicName, ex);
                    }
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    private void internalUnloadTransactionCoordinatorAsync(AsyncResponse asyncResponse, boolean authoritative) {
        validateTopicOwnershipAsync(topicName, authoritative)
                .thenCompose(v -> pulsar()
                                .getTransactionMetadataStoreService()
                                .removeTransactionMetadataStore(
                                        TransactionCoordinatorID.get(topicName.getPartitionIndex())))
                        .thenRun(() -> {
                            log.info("[{}] Successfully unloaded tc {}", clientAppId(),
                                    topicName.getPartitionIndex());
                            asyncResponse.resume(Response.noContent().build());
                        })
                .exceptionally(ex -> {
                    // If the exception is not redirect exception we need to log it.
                    if (isNot307And404Exception(ex)) {
                        log.error("[{}] Failed to unload tc {},{}", clientAppId(),
                                topicName.getPartitionIndex(), ex);
                    }
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    protected CompletableFuture<Void> internalDeleteTopicAsync(boolean authoritative, boolean force) {
        return validateNamespaceOperationAsync(topicName.getNamespaceObject(), NamespaceOperation.DELETE_TOPIC)
                .thenCompose(__ -> validateTopicOwnershipAsync(topicName, authoritative))
                .thenCompose(__ -> pulsar().getBrokerService().deleteTopic(topicName.toString(), force));
    }

    /**
     * There has a known bug will make Pulsar misidentifies "tp-partition-0-DLQ-partition-0" as "tp-partition-0-DLQ".
     * You can see the details from PR https://github.com/apache/pulsar/pull/19841.
     * This method is a quick fix and will be removed in master branch after #19841 and PIP 263 are done.
     */
    private boolean isUnexpectedTopicName(PartitionedTopicMetadata topicMetadata) {
        if (!topicName.toString().contains(PARTITIONED_TOPIC_SUFFIX)){
            return false;
        }
        if (topicMetadata.partitions <= 0){
            return false;
        }
        return topicName.getPartition(0).toString().equals(topicName.toString());
    }

    protected void internalGetSubscriptions(AsyncResponse asyncResponse, boolean authoritative) {
        CompletableFuture<Void> future;
        if (topicName.isGlobal()) {
            future = validateGlobalNamespaceOwnershipAsync(namespaceName);
        } else {
            future = CompletableFuture.completedFuture(null);
        }
        future.thenCompose(__ ->
                validateTopicOwnershipAsync(topicName, authoritative)
                .thenCompose(unused -> validateTopicOperationAsync(topicName, TopicOperation.GET_SUBSCRIPTIONS))
                .thenAccept(unused1 -> {
                    // If the topic name is a partition name, no need to get partition topic metadata again
                    if (topicName.isPartitioned()) {
                        internalGetSubscriptionsForNonPartitionedTopic(asyncResponse);
                    } else {
                        getPartitionedTopicMetadataAsync(topicName, authoritative, false)
                                .thenAccept(partitionMetadata -> {
                            if (partitionMetadata.partitions > 0 && !isUnexpectedTopicName(partitionMetadata)) {
                                try {
                                    final Set<String> subscriptions =
                                            Collections.newSetFromMap(
                                                    new ConcurrentHashMap<>(partitionMetadata.partitions));
                                    final List<CompletableFuture<Object>> subscriptionFutures = new ArrayList<>();
                                    if (topicName.getDomain() == TopicDomain.persistent) {
                                        final Map<Integer, CompletableFuture<Boolean>> existsFutures =
                                                new ConcurrentHashMap<>(partitionMetadata.partitions);
                                        for (int i = 0; i < partitionMetadata.partitions; i++) {
                                            existsFutures.put(i,
                                                    topicResources().persistentTopicExists(topicName.getPartition(i)));
                                        }
                                        FutureUtil.waitForAll(new ArrayList<>(existsFutures.values()))
                                                .thenApply(unused2 ->
                                                existsFutures.entrySet().stream().filter(e -> e.getValue().join())
                                                        .map(item -> topicName.getPartition(item.getKey()).toString())
                                                        .collect(Collectors.toList())
                                        ).thenAccept(topics -> {
                                            if (log.isDebugEnabled()) {
                                                log.debug("activeTopics : {}", topics);
                                            }
                                            topics.forEach(topic -> {
                                                try {
                                                    CompletableFuture<List<String>> subscriptionsAsync = pulsar()
                                                            .getAdminClient()
                                                            .topics().getSubscriptionsAsync(topic);
                                                    subscriptionFutures.add(subscriptionsAsync
                                                            .thenApply(subscriptions::addAll));
                                                } catch (PulsarServerException e) {
                                                    throw new RestException(e);
                                                }
                                            });
                                        }).thenAccept(unused3 -> resumeAsyncResponse(asyncResponse,
                                                        subscriptions, subscriptionFutures));
                                    } else {
                                        for (int i = 0; i < partitionMetadata.partitions; i++) {
                                            CompletableFuture<List<String>> subscriptionsAsync = pulsar()
                                                    .getAdminClient().topics()
                                                    .getSubscriptionsAsync(topicName.getPartition(i).toString());
                                            subscriptionFutures.add(subscriptionsAsync
                                                    .thenApply(subscriptions::addAll));
                                        }
                                        resumeAsyncResponse(asyncResponse, subscriptions, subscriptionFutures);
                                    }
                                } catch (Exception e) {
                                    log.error("[{}] Failed to get list of subscriptions for {}",
                                            clientAppId(), topicName, e);
                                    asyncResponse.resume(e);
                                }
                            } else {
                                internalGetSubscriptionsForNonPartitionedTopic(asyncResponse);
                            }
                        }).exceptionally(ex -> {
                            // If the exception is not redirect exception we need to log it.
                            if (isNot307And404Exception(ex)) {
                                log.error("[{}] Failed to get partitioned topic metadata while get"
                                        + " subscriptions for topic {}", clientAppId(), topicName, ex);
                            }
                            resumeAsyncResponseExceptionally(asyncResponse, ex);
                            return null;
                        });
                    }
                }).exceptionally(ex -> {
                    // If the exception is not redirect exception we need to log it.
                    if (isNot307And404Exception(ex)) {
                        log.error("[{}] Failed to validate the global namespace/topic ownership while get subscriptions"
                                + " for topic {}", clientAppId(), topicName, ex);
                    }
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                })
        ).exceptionally(ex -> {
            // If the exception is not redirect exception we need to log it.
            if (isNot307And404Exception(ex)) {
                log.error("[{}] Failed to get subscriptions for {}", clientAppId(), topicName, ex);
            }
            resumeAsyncResponseExceptionally(asyncResponse, ex);
            return null;
        });
    }

    private void resumeAsyncResponse(AsyncResponse asyncResponse, Set<String> subscriptions,
                                     List<CompletableFuture<Object>> subscriptionFutures) {
        FutureUtil.waitForAll(subscriptionFutures).whenComplete((r, ex) -> {
            if (ex != null) {
                log.warn("[{}] Failed to get list of subscriptions for {}: {}", clientAppId(),
                        topicName, ex.getMessage());
                if (ex instanceof PulsarAdminException) {
                    PulsarAdminException pae = (PulsarAdminException) ex;
                    if (pae.getStatusCode() == Status.NOT_FOUND.getStatusCode()) {
                        asyncResponse.resume(new RestException(Status.NOT_FOUND,
                                "Internal topics have not been generated yet"));
                        return;
                    } else {
                        asyncResponse.resume(new RestException(pae));
                        return;
                    }
                } else {
                    asyncResponse.resume(new RestException(ex));
                    return;
                }
            } else {
                asyncResponse.resume(new ArrayList<>(subscriptions));
            }
        });
    }

    private void internalGetSubscriptionsForNonPartitionedTopic(AsyncResponse asyncResponse) {
        getTopicReferenceAsync(topicName)
                .thenAccept(topic -> asyncResponse.resume(new ArrayList<>(topic.getSubscriptions().keys())))
                .exceptionally(ex -> {
                    // If the exception is not redirect exception we need to log it.
                    if (isNot307And404Exception(ex)) {
                        log.error("[{}] Failed to get list of subscriptions for {}", clientAppId(), topicName, ex);
                    }
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    protected CompletableFuture<? extends TopicStats> internalGetStatsAsync(boolean authoritative,
                                                                            boolean getPreciseBacklog,
                                                                            boolean subscriptionBacklogSize,
                                                                            boolean getEarliestTimeInBacklog) {
        CompletableFuture<Void> future;

        if (topicName.isGlobal()) {
            future = validateGlobalNamespaceOwnershipAsync(namespaceName);
        } else {
            future = CompletableFuture.completedFuture(null);
        }

        return future.thenCompose(__ -> validateTopicOwnershipAsync(topicName, authoritative))
                .thenComposeAsync(__ -> validateTopicOperationAsync(topicName, TopicOperation.GET_STATS))
                .thenCompose(__ -> getTopicReferenceAsync(topicName))
                .thenCompose(topic -> topic.asyncGetStats(getPreciseBacklog, subscriptionBacklogSize,
                        getEarliestTimeInBacklog));
    }

    protected CompletableFuture<PersistentTopicInternalStats> internalGetInternalStatsAsync(boolean authoritative,
                                                                                            boolean metadata) {
        CompletableFuture<Void> ret;
        if (topicName.isGlobal()) {
            ret = validateGlobalNamespaceOwnershipAsync(namespaceName);
        } else {
            ret = CompletableFuture.completedFuture(null);
        }
        return ret.thenCompose(__ -> validateTopicOwnershipAsync(topicName, authoritative))
                .thenCompose(__ -> validateTopicOperationAsync(topicName, TopicOperation.GET_STATS))
                .thenCompose(__ -> {
                    if (metadata) {
                        return validateTopicOperationAsync(topicName, TopicOperation.GET_METADATA);
                    }
                    return CompletableFuture.completedFuture(null);
                })
                .thenCompose(__ -> getTopicReferenceAsync(topicName))
                .thenCompose(topic -> topic.getInternalStats(metadata));
    }

    protected void internalGetManagedLedgerInfo(AsyncResponse asyncResponse, boolean authoritative) {
        CompletableFuture<Void> future = validateTopicOperationAsync(topicName, TopicOperation.GET_STATS);
        future.thenCompose(__ -> {
            if (topicName.isGlobal()) {
                return validateGlobalNamespaceOwnershipAsync(namespaceName);
            }
            return CompletableFuture.completedFuture(null);
        }).thenAccept(__ -> {
            // If the topic name is a partition name, no need to get partition topic metadata again
            if (topicName.isPartitioned()) {
                internalGetManagedLedgerInfoForNonPartitionedTopic(asyncResponse);
            } else {
                getPartitionedTopicMetadataAsync(topicName, authoritative, false)
                        .thenAccept(partitionMetadata -> {
                    if (partitionMetadata.partitions > 0) {
                        final List<CompletableFuture<Pair<String, ManagedLedgerInfo>>> futures =
                                new ArrayList<>(partitionMetadata.partitions);
                        for (int i = 0; i < partitionMetadata.partitions; i++) {
                            TopicName topicNamePartition = topicName.getPartition(i);
                            try {
                                final ObjectReader managedLedgerInfoReader = objectReader()
                                        .forType(ManagedLedgerInfo.class);
                                futures.add(pulsar().getAdminClient().topics()
                                        .getInternalInfoAsync(topicNamePartition.toString())
                                        .thenApply((response) -> {
                                            try {
                                                return Pair.of(topicNamePartition.toString(), managedLedgerInfoReader
                                                        .readValue(response));
                                            } catch (JsonProcessingException e) {
                                                throw new UncheckedIOException(e);
                                            }
                                        }));
                            } catch (PulsarServerException e) {
                                log.error("[{}] Failed to get admin client while get managed info for {}" ,
                                        clientAppId(), topicNamePartition, e);
                                throw new RestException(e);
                            }
                        }
                        FutureUtil.waitForAll(futures).whenComplete((result, exception) -> {
                            if (exception != null) {
                                Throwable t = exception.getCause();
                                if (t instanceof NotFoundException) {
                                    asyncResponse.resume(new RestException(Status.NOT_FOUND,
                                            getTopicNotFoundErrorMessage(topicName.toString())));
                                } else {
                                    log.error("[{}] Failed to get managed info for {}", clientAppId(), topicName, t);
                                    asyncResponse.resume(new RestException(t));
                                }
                            } else {
                                PartitionedManagedLedgerInfo partitionedManagedLedgerInfo =
                                        new PartitionedManagedLedgerInfo();
                                for (CompletableFuture<Pair<String, ManagedLedgerInfo>> infoFuture : futures) {
                                    Pair<String, ManagedLedgerInfo> info = infoFuture.getNow(null);
                                    partitionedManagedLedgerInfo.partitions.put(info.getKey(), info.getValue());
                                }
                                asyncResponse.resume((StreamingOutput) output -> {
                                    objectWriter().writeValue(output, partitionedManagedLedgerInfo);
                                });
                            }
                        });
                    } else {
                        internalGetManagedLedgerInfoForNonPartitionedTopic(asyncResponse);
                    }
                }).exceptionally(ex -> {
                    // If the exception is not redirect exception we need to log it.
                    if (isNot307And404Exception(ex)) {
                        log.error("[{}] Failed to get partitioned metadata while get managed info for {}",
                                clientAppId(), topicName, ex);
                    }
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
            }
        }).exceptionally(ex -> {
            // If the exception is not redirect exception we need to log it.
            if (isNot307And404Exception(ex)) {
                log.error("[{}] Failed to validate the global namespace ownership while get managed info for {}",
                        clientAppId(), topicName, ex);
            }
            resumeAsyncResponseExceptionally(asyncResponse, ex);
            return null;
        });
    }

    protected void internalGetManagedLedgerInfoForNonPartitionedTopic(AsyncResponse asyncResponse) {
        validateTopicOperationAsync(topicName, TopicOperation.GET_STATS)
                .thenAccept(__ -> {
                    String managedLedger = topicName.getPersistenceNamingEncoding();
                    pulsar().getManagedLedgerFactory()
                            .asyncGetManagedLedgerInfo(managedLedger, new ManagedLedgerInfoCallback() {
                        @Override
                        public void getInfoComplete(ManagedLedgerInfo info, Object ctx) {
                            asyncResponse.resume((StreamingOutput) output -> {
                                objectWriter().writeValue(output, info);
                            });
                        }
                        @Override
                        public void getInfoFailed(ManagedLedgerException exception, Object ctx) {
                            asyncResponse.resume(exception);
                        }
                    }, null);
                }).exceptionally(ex -> {
                    log.error("[{}] Failed to get managed info for {}", clientAppId(), topicName, ex);
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });

    }

    protected void internalGetPartitionedStats(AsyncResponse asyncResponse, boolean authoritative, boolean perPartition,
                                               boolean getPreciseBacklog, boolean subscriptionBacklogSize,
                                               boolean getEarliestTimeInBacklog) {
        CompletableFuture<Void> future = validateTopicOperationAsync(topicName, TopicOperation.GET_STATS);
        future.thenCompose(__ -> {
            if (topicName.isGlobal()) {
                return validateGlobalNamespaceOwnershipAsync(namespaceName);
            }
            return  CompletableFuture.completedFuture(null);
        }).thenCompose(__ -> getPartitionedTopicMetadataAsync(topicName,
                authoritative, false)).thenAccept(partitionMetadata -> {
            if (partitionMetadata.partitions == 0) {
                asyncResponse.resume(new RestException(Status.NOT_FOUND,
                        getPartitionedTopicNotFoundErrorMessage(topicName.toString())));
                return;
            }
            PartitionedTopicStatsImpl stats = new PartitionedTopicStatsImpl(partitionMetadata);
            List<CompletableFuture<TopicStats>> topicStatsFutureList = new ArrayList<>(partitionMetadata.partitions);
            for (int i = 0; i < partitionMetadata.partitions; i++) {
                TopicName partition = topicName.getPartition(i);
                topicStatsFutureList.add(
                    pulsar().getNamespaceService()
                        .isServiceUnitOwnedAsync(partition)
                        .thenCompose(owned -> {
                            if (owned) {
                                return getTopicReferenceAsync(partition)
                                        .thenCompose(
                                                ref -> ref.asyncGetStats(getPreciseBacklog, subscriptionBacklogSize,
                                                        getEarliestTimeInBacklog))
                                        .thenApply(s -> (TopicStats) s);
                            } else {
                                try {
                                    return pulsar().getAdminClient().topics().getStatsAsync(
                                        partition.toString(), getPreciseBacklog, subscriptionBacklogSize,
                                        getEarliestTimeInBacklog);
                                } catch (PulsarServerException e) {
                                    return FutureUtil.failedFuture(e);
                                }
                            }
                        })
                );
            }

            FutureUtil.waitForAll(topicStatsFutureList).handle((result, exception) -> {
                CompletableFuture<TopicStats> statFuture = null;
                for (int i = 0; i < topicStatsFutureList.size(); i++) {
                    statFuture = topicStatsFutureList.get(i);
                    if (statFuture.isDone() && !statFuture.isCompletedExceptionally()) {
                        try {
                            stats.add(statFuture.get());
                            if (perPartition) {
                                stats.getPartitions().put(topicName.getPartition(i).toString(), statFuture.get());
                            }
                        } catch (Exception e) {
                            asyncResponse.resume(new RestException(e));
                            return null;
                        }
                    }
                }
                if (perPartition && stats.partitions.isEmpty()) {
                    namespaceResources().getPartitionedTopicResources()
                            .partitionedTopicExistsAsync(topicName)
                            .thenAccept(exists -> {
                                if (exists) {
                                    stats.partitions.put(topicName.toString(), new TopicStatsImpl());
                                    asyncResponse.resume(stats);
                                } else {
                                    asyncResponse.resume(
                                            new RestException(Status.NOT_FOUND,
                                                    "Internal topics have not been generated yet"));
                                }
                            });
                } else {
                    asyncResponse.resume(stats);
                }
                return null;
            });
        }).exceptionally(ex -> {
            // If the exception is not redirect exception we need to log it.
            if (isNot307And404Exception(ex)) {
                log.error("[{}] Failed to get partitioned internal stats for {}", clientAppId(), topicName, ex);
            }
            resumeAsyncResponseExceptionally(asyncResponse, ex);
            return null;
        });
    }

    protected void internalGetPartitionedStatsInternal(AsyncResponse asyncResponse, boolean authoritative) {
        CompletableFuture<Void> future = validateTopicOperationAsync(topicName, TopicOperation.GET_STATS);
        future.thenCompose(__ -> {
            if (topicName.isGlobal()) {
                return validateGlobalNamespaceOwnershipAsync(namespaceName);
            } else {
                return CompletableFuture.completedFuture(null);
            }
        }).thenCompose(__ -> getPartitionedTopicMetadataAsync(topicName, authoritative, false))
        .thenAccept(partitionMetadata -> {
            if (partitionMetadata.partitions == 0) {
                asyncResponse.resume(new RestException(Status.NOT_FOUND,
                        getPartitionedTopicNotFoundErrorMessage(topicName.toString())));
                return;
            }

            PartitionedTopicInternalStats stats = new PartitionedTopicInternalStats(partitionMetadata);

            List<CompletableFuture<PersistentTopicInternalStats>> topicStatsFutureList = new ArrayList<>();
            for (int i = 0; i < partitionMetadata.partitions; i++) {
                try {
                    topicStatsFutureList.add(pulsar().getAdminClient().topics()
                            .getInternalStatsAsync((topicName.getPartition(i).toString()), false));
                } catch (PulsarServerException e) {
                    asyncResponse.resume(new RestException(e));
                    return;
                }
            }

            FutureUtil.waitForAll(topicStatsFutureList).handle((result, exception) -> {
                CompletableFuture<PersistentTopicInternalStats> statFuture = null;
                for (int i = 0; i < topicStatsFutureList.size(); i++) {
                    statFuture = topicStatsFutureList.get(i);
                    if (statFuture.isDone() && !statFuture.isCompletedExceptionally()) {
                        try {
                            stats.partitions.put(topicName.getPartition(i).toString(), statFuture.get());
                        } catch (Exception e) {
                            asyncResponse.resume(new RestException(e));
                            return null;
                        }
                    }
                }
                asyncResponse.resume(!stats.partitions.isEmpty() ? stats
                        : new RestException(Status.NOT_FOUND, "Internal topics have not been generated yet"));
                return null;
            });
        }).exceptionally(ex -> {
            // If the exception is not redirect exception we need to log it.
            if (isNot307And404Exception(ex)) {
                log.error("[{}] Failed to get partitioned internal stats for {}", clientAppId(), topicName, ex);
            }
            resumeAsyncResponseExceptionally(asyncResponse, ex);
            return null;
        });
    }

    protected CompletableFuture<Void> internalDeleteSubscriptionAsync(String subName,
                                                                      boolean authoritative,
                                                                      boolean force) {
        CompletableFuture<Void> future;
        if (topicName.isGlobal()) {
            future = validateGlobalNamespaceOwnershipAsync(namespaceName);
        } else {
            future = CompletableFuture.completedFuture(null);
        }

        return future
                .thenCompose((__) -> validateTopicOperationAsync(topicName, TopicOperation.UNSUBSCRIBE, subName))
                .thenCompose(__ -> {
            if (topicName.isPartitioned()) {
                return internalDeleteSubscriptionForNonPartitionedTopicAsync(subName, authoritative, force);
            } else {
                return getPartitionedTopicMetadataAsync(topicName,
                        authoritative, false).thenCompose(partitionMetadata -> {
                    if (partitionMetadata.partitions > 0) {
                        final List<CompletableFuture<Void>> futures = new ArrayList<>();
                        PulsarAdmin adminClient;
                        try {
                            adminClient = pulsar().getAdminClient();
                        } catch (PulsarServerException e) {
                            return CompletableFuture.failedFuture(e);
                        }
                        for (int i = 0; i < partitionMetadata.partitions; i++) {
                            TopicName topicNamePartition = topicName.getPartition(i);
                            futures.add(adminClient.topics()
                                    .deleteSubscriptionAsync(topicNamePartition.toString(), subName, force));
                        }

                        return FutureUtil.waitForAll(futures).handle((result, exception) -> {
                            if (exception != null) {
                                Throwable t = exception.getCause();
                                if (t instanceof NotFoundException) {
                                    throw new RestException(Status.NOT_FOUND,
                                            "Subscription not found");
                                } else if (t instanceof PreconditionFailedException) {
                                    throw new RestException(Status.PRECONDITION_FAILED,
                                            "Subscription has active connected consumers");
                                } else {
                                    throw new RestException(t);
                                }
                            }
                            return null;
                        });
                    }
                    return internalDeleteSubscriptionForNonPartitionedTopicAsync(subName, authoritative, force);
                });
            }
        });
    }

    // Note: this method expects the caller to check authorization
    private CompletableFuture<Void> internalDeleteSubscriptionForNonPartitionedTopicAsync(String subName,
                                                                                          boolean authoritative,
                                                                                          boolean force) {
        return validateTopicOwnershipAsync(topicName, authoritative)
                .thenCompose(__ -> getTopicReferenceAsync(topicName))
                .thenCompose((topic) -> {
                    Subscription sub = topic.getSubscription(subName);
                    if (sub == null) {
                        throw new RestException(Status.NOT_FOUND,
                                getSubNotFoundErrorMessage(topicName.toString(), subName));
                    }
                    CompletableFuture<Void> future = force ? sub.deleteForcefully() : sub.delete();
                    return future.thenRun(() -> newTopicEvent(topicName, TopicEvent.SUBSCRIPTION_DELETE)
                            .data(SubscriptionDeleteEventData.builder()
                                    .subscriptionName(subName)
                                    .subscriptionType(sub.getType())
                                    .force(force)
                                    .build())
                            .dispatch());
                });
    }

    private void internalAnalyzeSubscriptionBacklogForNonPartitionedTopic(AsyncResponse asyncResponse,
                                                                          String subName,
                                                                          Optional<Position> position,
                                                                          boolean authoritative) {
        validateTopicOwnershipAsync(topicName, authoritative)
                .thenCompose(__ -> validateTopicOperationAsync(topicName, TopicOperation.CONSUME, subName))
                .thenCompose(__ -> getTopicReferenceAsync(topicName))
                .thenCompose(topic -> {
                            Subscription sub = topic.getSubscription(subName);
                            if (sub == null) {
                                throw new RestException(Status.NOT_FOUND,
                                        getSubNotFoundErrorMessage(topicName.toString(), subName));
                            }
                            return sub.analyzeBacklog(position);
                        })
                .thenAccept((AnalyzeBacklogResult rawResult) -> {

                        AnalyzeSubscriptionBacklogResult result = new AnalyzeSubscriptionBacklogResult();

                        if (rawResult.getFirstPosition() != null) {
                            result.setFirstMessageId(
                                    rawResult.getFirstPosition().getLedgerId()
                                    + ":"
                                    + rawResult.getFirstPosition().getEntryId());
                        }

                        if (rawResult.getLastPosition() != null) {
                            result.setLastMessageId(rawResult.getLastPosition().getLedgerId()
                                    + ":"
                                    + rawResult.getLastPosition().getEntryId());
                        }

                        result.setEntries(rawResult.getEntries());
                        result.setMessages(rawResult.getMessages());

                        result.setFilterAcceptedEntries(rawResult.getFilterAcceptedEntries());
                        result.setFilterRejectedEntries(rawResult.getFilterRejectedEntries());
                        result.setFilterRescheduledEntries(rawResult.getFilterRescheduledEntries());

                        result.setFilterAcceptedMessages(rawResult.getFilterAcceptedMessages());
                        result.setFilterRejectedMessages(rawResult.getFilterRejectedMessages());
                        result.setFilterRescheduledMessages(rawResult.getFilterRescheduledMessages());
                        result.setAborted(rawResult.getScanOutcome() != ScanOutcome.COMPLETED);
                        log.info("[{}] analyzeBacklog topic {} subscription {} result {}", clientAppId(), subName,
                            topicName, result);
                        asyncResponse.resume(result);
                }).exceptionally(ex -> {
                    Throwable cause = ex.getCause();
                    // If the exception is not redirect exception we need to log it.
                    if (isNot307And404Exception(ex)) {
                        log.error("[{}] Failed to analyze subscription backlog {} {}",
                                clientAppId(), topicName, subName, cause);
                    }
                    resumeAsyncResponseExceptionally(asyncResponse, cause);
                    return null;
                });
    }
    private void internalUpdateSubscriptionPropertiesForNonPartitionedTopic(AsyncResponse asyncResponse,
                                      String subName, Map<String, String> subscriptionProperties,
                                      boolean authoritative) {
        validateTopicOwnershipAsync(topicName, authoritative)
                .thenCompose(__ -> validateTopicOperationAsync(topicName, TopicOperation.CONSUME, subName))
                .thenCompose(__ -> getTopicReferenceAsync(topicName))
                .thenCompose(topic -> {
                    Subscription sub = topic.getSubscription(subName);
                    if (sub == null) {
                        throw new RestException(Status.NOT_FOUND,
                                getSubNotFoundErrorMessage(topicName.toString(), subName));
                    }
                    return sub.updateSubscriptionProperties(subscriptionProperties);
                }).thenRun(() -> {
                    log.info("[{}][{}] Updated subscription {}", clientAppId(), topicName, subName);
                    asyncResponse.resume(Response.noContent().build());
                }).exceptionally(ex -> {
                    Throwable cause = ex.getCause();
                    // If the exception is not redirect exception we need to log it.
                    if (isNot307And404Exception(ex)) {
                        log.error("[{}] Failed to update subscription {} {}", clientAppId(), topicName, subName, cause);
                    }
                    asyncResponse.resume(new RestException(cause));
                    return null;
                });
    }

    private void internalGetSubscriptionPropertiesForNonPartitionedTopic(AsyncResponse asyncResponse,
                                                                            String subName,
                                                                            boolean authoritative) {
        validateTopicOwnershipAsync(topicName, authoritative)
                .thenCompose(__ -> validateTopicOperationAsync(topicName, TopicOperation.CONSUME, subName))
                .thenCompose(__ -> getTopicReferenceAsync(topicName))
                .thenApply((Topic topic) -> {
                    Subscription sub = topic.getSubscription(subName);
                    if (sub == null) {
                        throw new RestException(Status.NOT_FOUND,
                                getSubNotFoundErrorMessage(topicName.toString(), subName));
                    }
                    return sub.getSubscriptionProperties();
                }).thenAccept((Map<String, String> properties) -> {
                    if (properties == null) {
                        properties = Collections.emptyMap();
                    }
                    asyncResponse.resume(Response.ok(properties).build());
                }).exceptionally(ex -> {
                    Throwable cause = ex.getCause();
                    // If the exception is not redirect exception we need to log it.
                    if (isNot307And404Exception(ex)) {
                        log.error("[{}] Failed to update subscription {} {}", clientAppId(), topicName, subName, cause);
                    }
                    asyncResponse.resume(new RestException(cause));
                    return null;
                });
    }

    protected void internalDeleteSubscriptionForcefully(AsyncResponse asyncResponse,
                                                        String subName, boolean authoritative) {
        CompletableFuture<Void> future;
        if (topicName.isGlobal()) {
            future = validateGlobalNamespaceOwnershipAsync(namespaceName);
        } else {
            future = CompletableFuture.completedFuture(null);
        }

        future.thenAccept(__ -> {
            if (topicName.isPartitioned()) {
                internalDeleteSubscriptionForNonPartitionedTopicForcefully(asyncResponse, subName, authoritative);
            } else {
                getPartitionedTopicMetadataAsync(topicName,
                        authoritative, false).thenAccept(partitionMetadata -> {
                    if (partitionMetadata.partitions > 0) {
                        final List<CompletableFuture<Void>> futures = new ArrayList<>();

                        for (int i = 0; i < partitionMetadata.partitions; i++) {
                            TopicName topicNamePartition = topicName.getPartition(i);
                            try {
                                futures.add(pulsar().getAdminClient().topics()
                                        .deleteSubscriptionAsync(topicNamePartition.toString(), subName, true));
                            } catch (Exception e) {
                                log.error("[{}] Failed to delete subscription forcefully {} {}",
                                        clientAppId(), topicNamePartition, subName,
                                        e);
                                asyncResponse.resume(new RestException(e));
                                return;
                            }
                        }

                        FutureUtil.waitForAll(futures).handle((result, exception) -> {
                            if (exception != null) {
                                Throwable t = exception.getCause();
                                if (t instanceof NotFoundException) {
                                    asyncResponse.resume(new RestException(Status.NOT_FOUND,
                                            getSubNotFoundErrorMessage(topicName.toString(), subName)));
                                    return null;
                                } else {
                                    log.error("[{}] Failed to delete subscription forcefully {} {}",
                                            clientAppId(), topicName, subName, t);
                                    asyncResponse.resume(new RestException(t));
                                    return null;
                                }
                            }

                            asyncResponse.resume(Response.noContent().build());
                            return null;
                        });
                    } else {
                        internalDeleteSubscriptionForNonPartitionedTopicForcefully(asyncResponse, subName,
                                authoritative);
                    }
                }).exceptionally(ex -> {
                    // If the exception is not redirect exception we need to log it.
                    if (!isRedirectException(ex)) {
                        log.error("[{}] Failed to delete subscription forcefully {} from topic {}",
                                clientAppId(), subName, topicName, ex);
                    }
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
            }
        }).exceptionally(ex -> {
            // If the exception is not redirect exception we need to log it.
            if (!isRedirectException(ex)) {
                log.error("[{}] Failed to delete subscription {} from topic {}",
                        clientAppId(), subName, topicName, ex);
            }
            resumeAsyncResponseExceptionally(asyncResponse, ex);
            return null;
        });
    }

    private void internalDeleteSubscriptionForNonPartitionedTopicForcefully(AsyncResponse asyncResponse,
                                                                            String subName, boolean authoritative) {
        validateTopicOwnershipAsync(topicName, authoritative)
                .thenCompose(__ -> validateTopicOperationAsync(topicName, TopicOperation.UNSUBSCRIBE, subName))
                .thenCompose(__ -> getTopicReferenceAsync(topicName))
                .thenCompose(topic -> {
                    Subscription sub = topic.getSubscription(subName);
                    if (sub == null) {
                        throw new RestException(Status.NOT_FOUND,
                                getSubNotFoundErrorMessage(topicName.toString(), subName));
                    }
                    return sub.deleteForcefully();
                }).thenRun(() -> {
                    log.info("[{}][{}] Deleted subscription forcefully {}", clientAppId(), topicName, subName);
                    asyncResponse.resume(Response.noContent().build());
                }).exceptionally(ex -> {
                    // If the exception is not redirect exception we need to log it.
                    if (!isRedirectException(ex)) {
                        log.error("[{}] Failed to delete subscription forcefully {} {}",
                                clientAppId(), topicName, subName, ex);
                    }
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    protected void internalSkipAllMessages(AsyncResponse asyncResponse, String subName, boolean authoritative) {
        CompletableFuture<Void> future;
        if (topicName.isGlobal()) {
            future = validateGlobalNamespaceOwnershipAsync(namespaceName);
        } else {
            future = CompletableFuture.completedFuture(null);
        }

        future.thenCompose(__ -> validateTopicOwnershipAsync(topicName, authoritative))
            .thenCompose(__ -> validateTopicOperationAsync(topicName, TopicOperation.SKIP, subName))
            .thenCompose(__ -> {
                // If the topic name is a partition name, no need to get partition topic metadata again
                if (topicName.isPartitioned()) {
                    return internalSkipAllMessagesForNonPartitionedTopicAsync(asyncResponse, subName);
                } else {
                    return getPartitionedTopicMetadataAsync(topicName,
                        authoritative, false).thenCompose(partitionMetadata -> {
                        if (partitionMetadata.partitions > 0) {
                            final List<CompletableFuture<Void>> futures = new ArrayList<>();

                            for (int i = 0; i < partitionMetadata.partitions; i++) {
                                TopicName topicNamePartition = topicName.getPartition(i);
                                try {
                                    futures.add(pulsar()
                                        .getAdminClient()
                                        .topics()
                                        .skipAllMessagesAsync(topicNamePartition.toString(),
                                            subName));
                                } catch (Exception e) {
                                    log.error("[{}] Failed to skip all messages {} {}",
                                        clientAppId(), topicNamePartition, subName, e);
                                    asyncResponse.resume(new RestException(e));
                                    return CompletableFuture.completedFuture(null);
                                }
                            }

                            return FutureUtil.waitForAll(futures).handle((result, exception) -> {
                                if (exception != null) {
                                    Throwable t = exception.getCause();
                                    if (t instanceof NotFoundException) {
                                        asyncResponse.resume(
                                            new RestException(Status.NOT_FOUND,
                                                    getSubNotFoundErrorMessage(topicName.toString(), subName)));
                                    } else {
                                        log.error("[{}] Failed to skip all messages {} {}",
                                            clientAppId(), topicName, subName, t);
                                        asyncResponse.resume(new RestException(t));
                                    }
                                    return null;
                                }
                                asyncResponse.resume(Response.noContent().build());
                                return null;
                            });
                        } else {
                            return internalSkipAllMessagesForNonPartitionedTopicAsync(asyncResponse, subName);
                        }
                    });
                }
            }).exceptionally(ex -> {
                // If the exception is not redirect exception we need to log it.
                if (isNot307And404Exception(ex)) {
                    log.error("[{}] Failed to skip all messages for subscription {} on topic {}",
                            clientAppId(), subName, topicName, ex);
                }
                resumeAsyncResponseExceptionally(asyncResponse, ex);
                return null;
            });
    }

    private CompletableFuture<Void> internalSkipAllMessagesForNonPartitionedTopicAsync(AsyncResponse asyncResponse,
                                                                                       String subName) {
        return getTopicReferenceAsync(topicName).thenCompose(t -> {
                    PersistentTopic topic = (PersistentTopic) t;
                    BiConsumer<Void, Throwable> biConsumer = (v, ex) -> {
                        if (ex != null) {
                            asyncResponse.resume(new RestException(ex));
                            log.error("[{}] Failed to skip all messages {} {}",
                                clientAppId(), topicName, subName, ex);
                        } else {
                            asyncResponse.resume(Response.noContent().build());
                            log.info("[{}] Cleared backlog on {} {}", clientAppId(), topicName, subName);
                        }
                    };
                    if (subName.startsWith(topic.getReplicatorPrefix())) {
                        String remoteCluster = PersistentReplicator.getRemoteCluster(subName);
                        PersistentReplicator repl =
                            (PersistentReplicator) topic.getPersistentReplicator(remoteCluster);
                        if (repl == null) {
                            asyncResponse.resume(new RestException(Status.NOT_FOUND,
                                    getSubNotFoundErrorMessage(topicName.toString(), subName)));
                            return CompletableFuture.completedFuture(null);
                        }
                        return repl.clearBacklog().whenComplete(biConsumer);
                    } else {
                        PersistentSubscription sub = topic.getSubscription(subName);
                        if (sub == null) {
                            asyncResponse.resume(new RestException(Status.NOT_FOUND,
                                    getSubNotFoundErrorMessage(topicName.toString(), subName)));
                            return CompletableFuture.completedFuture(null);
                        }
                        return sub.clearBacklog().whenComplete(biConsumer);
                    }
                }).exceptionally(ex -> {
                    // If the exception is not redirect exception we need to log it.
                    if (isNot307And404Exception(ex)) {
                        log.error("[{}] Failed to skip all messages for subscription {} on topic {}",
                                clientAppId(), subName, topicName, ex);
                    }
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    protected void internalSkipMessages(AsyncResponse asyncResponse, String subName, int numMessages,
                                        boolean authoritative) {
        CompletableFuture<Void> future;
        if (topicName.isGlobal()) {
            future = validateGlobalNamespaceOwnershipAsync(namespaceName);
        } else {
            future = CompletableFuture.completedFuture(null);
        }
        future.thenCompose(__ -> validateTopicOwnershipAsync(topicName, authoritative))
                .thenCompose(__ -> validateTopicOperationAsync(topicName, TopicOperation.SKIP, subName))
                .thenCompose(__ -> getPartitionedTopicMetadataAsync(topicName, authoritative, false)
                     .thenCompose(partitionMetadata -> {
                         if (partitionMetadata.partitions > 0) {
                             String msg = "Skip messages on a partitioned topic is not allowed";
                             log.warn("[{}] {} {} {}", clientAppId(), msg, topicName, subName);
                             throw new  RestException(Status.METHOD_NOT_ALLOWED, msg);
                         }
                         return getTopicReferenceAsync(topicName).thenCompose(t -> {
                             PersistentTopic topic = (PersistentTopic) t;
                             if (topic == null) {
                                 throw new RestException(new RestException(Status.NOT_FOUND,
                                         getTopicNotFoundErrorMessage(topicName.toString())));
                             }
                             if (subName.startsWith(topic.getReplicatorPrefix())) {
                                 String remoteCluster = PersistentReplicator.getRemoteCluster(subName);
                                 PersistentReplicator repl =
                                         (PersistentReplicator) topic.getPersistentReplicator(remoteCluster);
                                 if (repl == null) {
                                     return FutureUtil.failedFuture(
                                             new RestException(Status.NOT_FOUND, "Replicator not found"));
                                 }
                                 return repl.skipMessages(numMessages).thenAccept(unused -> {
                                     log.info("[{}] Skipped {} messages on {} {}", clientAppId(), numMessages,
                                             topicName, subName);
                                     asyncResponse.resume(Response.noContent().build());
                                     }
                                 );
                             } else {
                                 PersistentSubscription sub = topic.getSubscription(subName);
                                 if (sub == null) {
                                     return FutureUtil.failedFuture(
                                             new RestException(Status.NOT_FOUND,
                                                     getSubNotFoundErrorMessage(topicName.toString(), subName)));
                                 }
                                 return sub.skipMessages(numMessages).thenAccept(unused -> {
                                     log.info("[{}] Skipped {} messages on {} {}", clientAppId(), numMessages,
                                             topicName, subName);
                                     asyncResponse.resume(Response.noContent().build());
                                     }
                                 );
                             }
                         });
                     })
                ).exceptionally(ex -> {
                    // If the exception is not redirect exception we need to log it.
                    if (isNot307And404Exception(ex)) {
                        log.error("[{}] Failed to skip {} messages {} {}", clientAppId(), numMessages, topicName,
                                subName, ex);
                    }
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    protected void internalExpireMessagesForAllSubscriptions(AsyncResponse asyncResponse, int expireTimeInSeconds,
            boolean authoritative) {
        CompletableFuture<Void> future;
        if (topicName.isGlobal()) {
            future = validateGlobalNamespaceOwnershipAsync(namespaceName);
        } else {
            future = CompletableFuture.completedFuture(null);
        }
        future.thenCompose(__ ->
            getPartitionedTopicMetadataAsync(topicName, authoritative, false)
                .thenAccept(partitionMetadata -> {
                    if (topicName.isPartitioned()) {
                        internalExpireMessagesForAllSubscriptionsForNonPartitionedTopic(asyncResponse,
                                partitionMetadata, expireTimeInSeconds, authoritative);
                    } else {
                        if (partitionMetadata.partitions > 0) {
                            final List<CompletableFuture<Void>> futures = new ArrayList<>(partitionMetadata.partitions);

                            // expire messages for each partition topic
                            for (int i = 0; i < partitionMetadata.partitions; i++) {
                                TopicName topicNamePartition = topicName.getPartition(i);
                                try {
                                    futures.add(pulsar()
                                            .getAdminClient()
                                            .topics()
                                            .expireMessagesForAllSubscriptionsAsync(
                                                    topicNamePartition.toString(), expireTimeInSeconds));
                                } catch (Exception e) {
                                    log.error("[{}] Failed to expire messages up to {} on {}",
                                            clientAppId(), expireTimeInSeconds,
                                            topicNamePartition, e);
                                    asyncResponse.resume(new RestException(e));
                                    return;
                                }
                            }

                            FutureUtil.waitForAll(futures).handle((result, exception) -> {
                                if (exception != null) {
                                    Throwable t = FutureUtil.unwrapCompletionException(exception);
                                    if (t instanceof PulsarAdminException) {
                                        log.warn("[{}] Failed to expire messages up to {} on {}: {}", clientAppId(),
                                                expireTimeInSeconds, topicName, t.toString());
                                    } else {
                                        log.error("[{}] Failed to expire messages up to {} on {}", clientAppId(),
                                                expireTimeInSeconds, topicName, t);
                                    }
                                    resumeAsyncResponseExceptionally(asyncResponse, t);
                                    return null;
                                }
                                asyncResponse.resume(Response.noContent().build());
                                return null;
                            });
                        } else {
                            internalExpireMessagesForAllSubscriptionsForNonPartitionedTopic(asyncResponse,
                                    partitionMetadata, expireTimeInSeconds, authoritative);
                        }
                    }
                }
             )
        ).exceptionally(ex -> {
            // If the exception is not redirect exception we need to log it.
            if (isNot307And404Exception(ex)) {
                log.error("[{}] Failed to expire messages for all subscription on topic {}", clientAppId(), topicName,
                        ex);
            }
            resumeAsyncResponseExceptionally(asyncResponse, ex);
            return null;
        });

    }

    private void internalExpireMessagesForAllSubscriptionsForNonPartitionedTopic(AsyncResponse asyncResponse,
                                                                                 PartitionedTopicMetadata
                                                                                 partitionMetadata,
                                                                                 int expireTimeInSeconds,
                                                                                 boolean authoritative) {
        // validate ownership and redirect if current broker is not owner
        validateTopicOwnershipAsync(topicName, authoritative)
                .thenCompose(__ -> validateTopicOperationAsync(topicName, TopicOperation.EXPIRE_MESSAGES))
                .thenCompose(__ -> getTopicReferenceAsync(topicName).thenAccept(t -> {
                     if (t == null) {
                         resumeAsyncResponseExceptionally(asyncResponse, new RestException(Status.NOT_FOUND,
                                 getTopicNotFoundErrorMessage(topicName.toString())));
                         return;
                     }
                    if (!(t instanceof PersistentTopic)) {
                        resumeAsyncResponseExceptionally(asyncResponse, new RestException(Status.METHOD_NOT_ALLOWED,
                                "Expire messages for all subscriptions on a non-persistent topic is not allowed"));
                        return;
                    }
                    PersistentTopic topic = (PersistentTopic) t;
                    final List<CompletableFuture<Void>> futures =
                            new ArrayList<>((int) topic.getReplicators().size());
                    List<String> subNames =
                            new ArrayList<>((int) topic.getSubscriptions().size());
                    subNames.addAll(topic.getSubscriptions().keys().stream().filter(
                            subName -> !subName.equals(Compactor.COMPACTION_SUBSCRIPTION)).toList());
                    for (int i = 0; i < subNames.size(); i++) {
                        try {
                            futures.add(internalExpireMessagesByTimestampForSinglePartitionAsync(partitionMetadata,
                                    subNames.get(i), expireTimeInSeconds));
                        } catch (Exception e) {
                            log.error("[{}] Failed to expire messages for all subscription up to {} on {}",
                                    clientAppId(), expireTimeInSeconds, topicName, e);
                            asyncResponse.resume(new RestException(e));
                            return;
                        }
                    }

                    FutureUtil.waitForAll(futures).handle((result, exception) -> {
                        if (exception != null) {
                            Throwable throwable = FutureUtil.unwrapCompletionException(exception);
                            if (throwable instanceof RestException) {
                                log.warn("[{}] Failed to expire messages for all subscription up to {} on {}: {}",
                                        clientAppId(), expireTimeInSeconds, topicName, throwable.toString());
                            } else {
                                log.error("[{}] Failed to expire messages for all subscription up to {} on {}",
                                        clientAppId(), expireTimeInSeconds, topicName, throwable);
                            }
                            resumeAsyncResponseExceptionally(asyncResponse, throwable);
                            return null;
                        }
                        asyncResponse.resume(Response.noContent().build());
                        return null;
                    });
                        })
                ).exceptionally(ex -> {
            // If the exception is not redirect exception we need to log it.
            if (isNot307And404Exception(ex)) {
                log.error("[{}] Failed to expire messages for all subscription up to {} on {}", clientAppId(),
                        expireTimeInSeconds, topicName, ex);
            }
            resumeAsyncResponseExceptionally(asyncResponse, ex);
            return null;
        });
    }

    protected CompletableFuture<Void> internalResetCursorAsync(String subName, long timestamp,
            boolean authoritative) {
        CompletableFuture<Void> future;
        if (topicName.isGlobal()) {
            future = validateGlobalNamespaceOwnershipAsync(namespaceName);
        } else {
            future = CompletableFuture.completedFuture(null);
        }
        return future
            .thenCompose(__ -> validateTopicOwnershipAsync(topicName, authoritative))
            .thenCompose(__ -> validateTopicOperationAsync(topicName, TopicOperation.RESET_CURSOR, subName))
            .thenCompose(__ -> {
                // If the topic name is a partition name, no need to get partition topic metadata again
                if (topicName.isPartitioned()) {
                    return internalResetCursorForNonPartitionedTopic(subName, timestamp, authoritative);
                } else {
                    return internalResetCursorForPartitionedTopic(subName, timestamp, authoritative);
                }
            });
    }

    private CompletableFuture<Void> internalResetCursorForPartitionedTopic(String subName, long timestamp,
                                                                           boolean authoritative) {
        return getPartitionedTopicMetadataAsync(topicName, authoritative, false)
            .thenCompose(partitionMetadata -> {
                final int numPartitions = partitionMetadata.partitions;
                if (numPartitions > 0) {
                    final CompletableFuture<Void> future = new CompletableFuture<>();
                    final AtomicInteger count = new AtomicInteger(numPartitions);
                    final AtomicInteger failureCount = new AtomicInteger(0);
                    final AtomicReference<Throwable> partitionException = new AtomicReference<>();

                    for (int i = 0; i < numPartitions; i++) {
                        TopicName topicNamePartition = topicName.getPartition(i);
                        try {
                            pulsar().getAdminClient().topics()
                                .resetCursorAsync(topicNamePartition.toString(),
                                    subName, timestamp).handle((r, ex) -> {
                                    if (ex != null) {
                                        if (ex instanceof PreconditionFailedException) {
                                            // throw the last exception if all partitions get this error
                                            // any other exception on partition is reported back to user
                                            failureCount.incrementAndGet();
                                            partitionException.set(ex);
                                        } else {
                                            log.warn("[{}] [{}] Failed to reset cursor on subscription {} to time {}",
                                                clientAppId(), topicNamePartition, subName, timestamp, ex);
                                            future.completeExceptionally(ex);
                                            return null;
                                        }
                                    }

                                    if (count.decrementAndGet() == 0) {
                                        future.complete(null);
                                    }

                                    return null;
                                });
                        } catch (Exception e) {
                            log.warn("[{}] [{}] Failed to reset cursor on subscription {} to time {}", clientAppId(),
                                topicNamePartition, subName, timestamp, e);
                            future.completeExceptionally(e);
                        }
                    }

                    return future.whenComplete((r, ex) -> {
                        // report an error to user if unable to reset for all partitions
                        if (failureCount.get() == numPartitions) {
                            log.warn("[{}] [{}] Failed to reset cursor on subscription {} to time {}",
                                clientAppId(), topicName,
                                subName, timestamp, partitionException.get());
                            throw new RestException(Status.PRECONDITION_FAILED, partitionException.get().getMessage());
                        } else if (failureCount.get() > 0) {
                            log.warn("[{}] [{}] Partial errors for reset cursor on subscription {} to time {}",
                                clientAppId(), topicName, subName, timestamp, partitionException.get());
                        }
                    });
                } else {
                    return internalResetCursorForNonPartitionedTopic(subName, timestamp, authoritative);
                }
            });
    }

    private CompletableFuture<Void> internalResetCursorForNonPartitionedTopic(String subName, long timestamp,
                                       boolean authoritative) {
        return validateTopicOwnershipAsync(topicName, authoritative)
            .thenCompose(__ -> validateTopicOperationAsync(topicName, TopicOperation.RESET_CURSOR, subName))
            .thenCompose(__ -> {
                log.info("[{}] [{}] Received reset cursor on subscription {} to time {}",
                    clientAppId(), topicName, subName, timestamp);
                return getTopicReferenceAsync(topicName);
            })
            .thenCompose(topic -> {
                Subscription sub = topic.getSubscription(subName);
                if (sub == null) {
                    throw new RestException(Status.NOT_FOUND,
                        getSubNotFoundErrorMessage(topicName.toString(), subName));
                }
                return sub.resetCursor(timestamp)
                        .thenRun(() -> newTopicEvent(topicName, TopicEvent.SUBSCRIPTION_SEEK)
                                .data(SubscriptionSeekEventData.builder()
                                        .subscriptionName(subName)
                                        .subscriptionType(sub.getType())
                                        .timestamp(timestamp)
                                        .build())
                                .dispatch()
                        );
            })
            .thenRun(() ->
                log.info("[{}][{}] Reset cursor on subscription {} to time {}",
                    clientAppId(), topicName, subName, timestamp));
    }

    protected void internalCreateSubscription(AsyncResponse asyncResponse, String subscriptionName,
            MessageIdImpl messageId, boolean authoritative, boolean replicated, Map<String, String> properties) {
        CompletableFuture<Void> ret = validateTopicOperationAsync(topicName, TopicOperation.SUBSCRIBE,
                subscriptionName);
        ret.thenCompose(__ -> {
            if (topicName.isGlobal()) {
                return validateGlobalNamespaceOwnershipAsync(namespaceName);
            }
            return CompletableFuture.completedFuture(null);
        }).thenAccept(__ -> {
            final MessageIdImpl targetMessageId = messageId == null ? (MessageIdImpl) MessageId.latest : messageId;
            log.info("[{}][{}] Creating subscription {} at message id {} with properties {}", clientAppId(),
                    topicName, subscriptionName, targetMessageId, properties);
            // If the topic name is a partition name, no need to get partition topic metadata again
            if (topicName.isPartitioned()) {
                internalCreateSubscriptionForNonPartitionedTopic(asyncResponse,
                        subscriptionName, targetMessageId, authoritative, replicated, properties);
            } else {
                pulsar().getBrokerService().isAllowAutoTopicCreationAsync(topicName)
                        .thenCompose(allowAutoTopicCreation -> getPartitionedTopicMetadataAsync(topicName,
                                authoritative, allowAutoTopicCreation).thenAccept(partitionMetadata -> {
                            final int numPartitions = partitionMetadata.partitions;
                            if (partitionMetadata.partitions > 0 && !isUnexpectedTopicName(partitionMetadata)) {
                                final CompletableFuture<Void> future = new CompletableFuture<>();
                                final AtomicInteger count = new AtomicInteger(numPartitions);
                                final AtomicInteger failureCount = new AtomicInteger(0);
                                final AtomicReference<Throwable> partitionException = new AtomicReference<>();

                                // Create the subscription on each partition
                                for (int i = 0; i < numPartitions; i++) {
                                    TopicName topicNamePartition = topicName.getPartition(i);
                                    try {
                                        pulsar().getAdminClient().topics()
                                                .createSubscriptionAsync(topicNamePartition.toString(),
                                                        subscriptionName, targetMessageId, false, properties)
                                                .handle((r, ex) -> {
                                                    if (ex != null) {
                                                        // fail the operation on unknown exception or
                                                        // if all the partitioned failed due to
                                                        // subscription-already-exist
                                                        if (failureCount.incrementAndGet() == numPartitions
                                                                || !(ex instanceof PulsarAdminException
                                                                        .ConflictException)) {
                                                            partitionException.set(ex);
                                                        }
                                                    }

                                                    if (count.decrementAndGet() == 0) {
                                                        future.complete(null);
                                                    }

                                                    return null;
                                                });
                                    } catch (Exception e) {
                                        log.warn("[{}] [{}] Failed to create subscription {} at message id {}",
                                              clientAppId(), topicNamePartition, subscriptionName, targetMessageId, e);
                                        future.completeExceptionally(e);
                                    }
                                }

                                future.whenComplete((r, ex) -> {
                                    if (ex != null) {
                                        if (ex instanceof PulsarAdminException) {
                                            asyncResponse.resume(new RestException((PulsarAdminException) ex));
                                            return;
                                        } else {
                                            asyncResponse.resume(new RestException(ex));
                                            return;
                                        }
                                    }

                                    if (partitionException.get() != null) {
                                        log.warn("[{}] [{}] Failed to create subscription {} at message id {}",
                                                clientAppId(), topicName,
                                                subscriptionName, targetMessageId, partitionException.get());
                                        if (partitionException.get() instanceof PulsarAdminException) {
                                            asyncResponse.resume(
                                                    new RestException((PulsarAdminException) partitionException.get()));
                                            return;
                                        } else {
                                            asyncResponse.resume(new RestException(partitionException.get()));
                                            return;
                                        }
                                    }

                                    asyncResponse.resume(Response.noContent().build());
                                });
                            } else {
                                internalCreateSubscriptionForNonPartitionedTopic(asyncResponse,
                                        subscriptionName, targetMessageId, authoritative, replicated, properties);
                            }

                        })).exceptionally(ex -> {
                    // If the exception is not redirect exception we need to log it.
                    if (isNot307And404Exception(ex)) {
                        log.error("[{}] Failed to create subscription {} on topic {}",
                                clientAppId(), subscriptionName, topicName, ex);
                    }
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
            }
        }).exceptionally(ex -> {
            // If the exception is not redirect exception we need to log it.
            if (isNot307And404Exception(ex)) {
                log.error("[{}] Failed to create subscription {} on topic {}",
                        clientAppId(), subscriptionName, topicName, ex);
            }
            resumeAsyncResponseExceptionally(asyncResponse, ex);
            return null;
        });
    }

    private void internalCreateSubscriptionForNonPartitionedTopic(
            AsyncResponse asyncResponse, String subscriptionName,
            MessageIdImpl targetMessageId, boolean authoritative, boolean replicated,
            Map<String, String> properties) {

        validateTopicOwnershipAsync(topicName, authoritative)
                .thenCompose(__ -> validateTopicOperationAsync(topicName, TopicOperation.SUBSCRIBE, subscriptionName))
                .thenCompose(__ -> pulsar().getBrokerService().isAllowAutoTopicCreationAsync(topicName))
                .thenCompose(isAllowAutoTopicCreation -> pulsar().getBrokerService()
                        .getTopic(topicName.toString(), isAllowAutoTopicCreation))
                .thenApply(optTopic -> {
                    if (optTopic.isPresent()) {
                        return optTopic.get();
                    } else {
                        throw new RestException(Status.PRECONDITION_FAILED,
                                "Topic does not exist and cannot be auto-created");
                    }
        }).thenCompose(topic -> {
            if (topic.getSubscriptions().containsKey(subscriptionName)) {
                throw new RestException(Status.CONFLICT, "Subscription already exists for topic");
            }

            return topic.createSubscription(subscriptionName, InitialPosition.Latest, replicated, properties)
                    .thenApply((s) -> {
                        newTopicEvent(topicName, TopicEvent.SUBSCRIPTION_CREATE)
                                .data(SubscriptionCreateEventData.builder()
                                        .subscriptionName(subscriptionName)
                                        .durable(true)
                                        .replicateSubscriptionState(replicated)
                                        .subscriptionInitialPosition(InitialPosition.Latest)
                                        .messageId(targetMessageId.toString())
                                        .build())
                                .dispatch();
                        return s;
                    });
        }).thenCompose(subscription -> {
            // Mark the cursor as "inactive" as it was created without a real consumer connected
            ((PersistentSubscription) subscription).deactivateCursor();
            return subscription.resetCursor(
                    PositionImpl.get(targetMessageId.getLedgerId(), targetMessageId.getEntryId()));
        }).thenRun(() -> {
            log.info("[{}][{}] Successfully created subscription {} at message id {}", clientAppId(),
                    topicName, subscriptionName, targetMessageId);
            asyncResponse.resume(Response.noContent().build());
        }).exceptionally(ex -> {
            Throwable t = (ex instanceof CompletionException ? ex.getCause() : ex);
            if (!(t instanceof WebApplicationException)) {
                log.warn("[{}][{}] Failed to create subscription {} at message id {}", clientAppId(), topicName,
                        subscriptionName, targetMessageId, t);
            }

            if (t instanceof SubscriptionInvalidCursorPosition) {
                asyncResponse.resume(new RestException(Status.PRECONDITION_FAILED,
                        "Unable to find position for position specified: " + t.getMessage()));
            } else if (t instanceof SubscriptionBusyException) {
                asyncResponse.resume(new RestException(Status.PRECONDITION_FAILED,
                        "Failed for Subscription Busy: " + t.getMessage()));
            } else {
                resumeAsyncResponseExceptionally(asyncResponse, t);
            }
            return null;
        });
    }

    protected void internalUpdateSubscriptionProperties(AsyncResponse asyncResponse, String subName,
                                                        Map<String, String> subscriptionProperties,
                                                        boolean authoritative) {
        CompletableFuture<Void> future = validateTopicOperationAsync(topicName, TopicOperation.SUBSCRIBE, subName);
        future.thenCompose(__ -> {
            if (topicName.isGlobal()) {
                return validateGlobalNamespaceOwnershipAsync(namespaceName);
            }
            return CompletableFuture.completedFuture(null);
        }).thenCompose(__ -> validateTopicOwnershipAsync(topicName, authoritative)).thenAccept(__ -> {
            if (topicName.isPartitioned()) {
                internalUpdateSubscriptionPropertiesForNonPartitionedTopic(asyncResponse, subName,
                        subscriptionProperties, authoritative);
            } else {
                getPartitionedTopicMetadataAsync(topicName,
                        authoritative, false).thenAcceptAsync(partitionMetadata -> {
                    if (partitionMetadata.partitions > 0) {
                        final List<CompletableFuture<Void>> futures = new ArrayList<>();

                        for (int i = 0; i < partitionMetadata.partitions; i++) {
                            TopicName topicNamePartition = topicName.getPartition(i);
                            try {
                                futures.add(pulsar().getAdminClient().topics()
                                        .updateSubscriptionPropertiesAsync(topicNamePartition.toString(),
                                                subName, subscriptionProperties));
                            } catch (Exception e) {
                                log.error("[{}] Failed to update properties for subscription {} {}",
                                        clientAppId(), topicNamePartition, subName,
                                        e);
                                asyncResponse.resume(new RestException(e));
                                return;
                            }
                        }

                        FutureUtil.waitForAll(futures).handle((result, exception) -> {
                            if (exception != null) {
                                Throwable t = exception.getCause();
                                if (t instanceof NotFoundException) {
                                    asyncResponse.resume(new RestException(Status.NOT_FOUND,
                                            getSubNotFoundErrorMessage(topicName.toString(), subName)));
                                    return null;
                                } else if (t instanceof PreconditionFailedException) {
                                    asyncResponse.resume(new RestException(Status.PRECONDITION_FAILED,
                                            "Subscription has active connected consumers"));
                                    return null;
                                } else {
                                    log.error("[{}] Failed to update properties for subscription {} {}",
                                            clientAppId(), topicName, subName, t);
                                    asyncResponse.resume(new RestException(t));
                                    return null;
                                }
                            }

                            asyncResponse.resume(Response.noContent().build());
                            return null;
                        });
                    } else {
                        internalUpdateSubscriptionPropertiesForNonPartitionedTopic(asyncResponse, subName,
                                subscriptionProperties, authoritative);
                    }
                }, pulsar().getExecutor()).exceptionally(ex -> {
                    log.error("[{}] Failed to update properties for subscription {} from topic {}",
                            clientAppId(), subName, topicName, ex);
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
            }
        }).exceptionally(ex -> {
            // If the exception is not redirect exception we need to log it.
            if (isNot307And404Exception(ex)) {
                log.error("[{}] Failed to update subscription {} from topic {}",
                        clientAppId(), subName, topicName, ex);
            }
            resumeAsyncResponseExceptionally(asyncResponse, ex);
            return null;
        });
    }

    protected void internalAnalyzeSubscriptionBacklog(AsyncResponse asyncResponse, String subName,
                                                      Optional<Position> position,
                                                      boolean authoritative) {
        CompletableFuture<Void> future = validateTopicOperationAsync(topicName, TopicOperation.CONSUME, subName);
        future.thenCompose(__ -> {
            if (topicName.isGlobal()) {
                return validateGlobalNamespaceOwnershipAsync(namespaceName);
            }
            return CompletableFuture.completedFuture(null);
        }).thenCompose(__ -> validateTopicOwnershipAsync(topicName, authoritative))
                .thenCompose(__ -> {
                    if (topicName.isPartitioned()) {
                        return CompletableFuture.completedFuture(null);
                    } else {
                        return getPartitionedTopicMetadataAsync(topicName, authoritative, false)
                                .thenAccept(metadata -> {
                                    if (metadata.partitions > 0) {
                                        throw new RestException(Status.METHOD_NOT_ALLOWED,
                                                "Analyze backlog on a partitioned topic is not allowed, "
                                                        + "please try do it on specific topic partition");
                                    }
                                });
                    }
                })
                .thenAccept(__ -> {
                   internalAnalyzeSubscriptionBacklogForNonPartitionedTopic(asyncResponse, subName,
                           position, authoritative);
                })
                .exceptionally(ex -> {
                    // If the exception is not redirect exception we need to log it.
                    if (isNot307And404Exception(ex)) {
                        log.error("[{}] Failed to analyze back log of subscription {} from topic {}",
                                clientAppId(), subName, topicName, ex);
                    }
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
        });
    }

    protected void internalGetSubscriptionProperties(AsyncResponse asyncResponse, String subName,
                                                        boolean authoritative) {
        CompletableFuture<Void> future = validateTopicOperationAsync(topicName, TopicOperation.CONSUME, subName);
        future.thenCompose(__ -> {
            if (topicName.isGlobal()) {
                return validateGlobalNamespaceOwnershipAsync(namespaceName);
            }
            return CompletableFuture.completedFuture(null);
        }).thenCompose(__ -> validateTopicOwnershipAsync(topicName, authoritative)).thenAccept(__ -> {
            if (topicName.isPartitioned()) {
                internalGetSubscriptionPropertiesForNonPartitionedTopic(asyncResponse, subName,
                        authoritative);
            } else {
                getPartitionedTopicMetadataAsync(topicName,
                        authoritative, false).thenAcceptAsync(partitionMetadata -> {
                    if (partitionMetadata.partitions > 0) {
                        final List<CompletableFuture<Map<String, String>>> futures = new ArrayList<>();

                        for (int i = 0; i < partitionMetadata.partitions; i++) {
                            TopicName topicNamePartition = topicName.getPartition(i);
                            try {
                                futures.add(pulsar().getAdminClient().topics()
                                        .getSubscriptionPropertiesAsync(topicNamePartition.toString(),
                                                subName));
                            } catch (Exception e) {
                                log.error("[{}] Failed to update properties for subscription {} {}",
                                        clientAppId(), topicNamePartition, subName,
                                        e);
                                asyncResponse.resume(new RestException(e));
                                return;
                            }
                        }

                        FutureUtil.waitForAll(futures).handle((result, exception) -> {
                            if (exception != null) {
                                Throwable t = exception.getCause();
                                if (t instanceof NotFoundException) {
                                    asyncResponse.resume(new RestException(Status.NOT_FOUND,
                                            getSubNotFoundErrorMessage(topicName.toString(), subName)));
                                    return null;
                                } else {
                                    log.error("[{}] Failed to get properties for subscription {} {}",
                                            clientAppId(), topicName, subName, t);
                                    asyncResponse.resume(new RestException(t));
                                    return null;
                                }
                            }

                            Map<String, String> aggregatedResult = new HashMap<>();
                            futures.forEach(f -> {
                                // in theory all the partitions have the same properties
                                try {
                                    aggregatedResult.putAll(f.get());
                                } catch (Exception impossible) {
                                    // we already waited for this Future
                                    asyncResponse.resume(new RestException(impossible));
                                }
                            });

                            asyncResponse.resume(Response.ok(aggregatedResult).build());
                            return null;
                        });
                    } else {
                        internalGetSubscriptionPropertiesForNonPartitionedTopic(asyncResponse, subName,
                                authoritative);
                    }
                }, pulsar().getExecutor()).exceptionally(ex -> {
                    log.error("[{}] Failed to update properties for subscription {} from topic {}",
                            clientAppId(), subName, topicName, ex);
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
            }
        }).exceptionally(ex -> {
            // If the exception is not redirect exception we need to log it.
            if (isNot307And404Exception(ex)) {
                log.error("[{}] Failed to update subscription {} from topic {}",
                        clientAppId(), subName, topicName, ex);
            }
            resumeAsyncResponseExceptionally(asyncResponse, ex);
            return null;
        });
    }

    protected void internalResetCursorOnPosition(AsyncResponse asyncResponse, String subName, boolean authoritative,
            MessageIdImpl messageId, boolean isExcluded, int batchIndex) {
        CompletableFuture<Void> ret;
        // If the topic name is a partition name, no need to get partition topic metadata again
        if (!topicName.isPartitioned()) {
            ret = getPartitionedTopicMetadataAsync(topicName, authoritative, false)
                    .thenCompose(topicMetadata -> {
                        if (topicMetadata.partitions > 0) {
                            log.warn("[{}] Not supported operation on partitioned-topic {} {}",
                                    clientAppId(), topicName, subName);
                            throw new CompletionException(new RestException(Status.METHOD_NOT_ALLOWED,
                                    "Reset-cursor at position is not allowed for partitioned-topic"));
                        }
                        return CompletableFuture.completedFuture(null);
                    });
        } else {
            ret = CompletableFuture.completedFuture(null);
        }

        CompletableFuture<Void> future;
        if (topicName.isGlobal()) {
            future = ret.thenCompose(__ -> validateGlobalNamespaceOwnershipAsync(namespaceName));
        } else {
            future = CompletableFuture.completedFuture(null);
        }
        future.thenAccept(__ -> {
            log.info("[{}][{}] received reset cursor on subscription {} to position {}", clientAppId(), topicName,
                    subName, messageId);
            validateTopicOwnershipAsync(topicName, authoritative)
                    .thenCompose(ignore ->
                            validateTopicOperationAsync(topicName, TopicOperation.RESET_CURSOR, subName))
                    .thenCompose(ignore -> getTopicReferenceAsync(topicName))
                    .thenAccept(topic -> {
                        if (topic == null) {
                            asyncResponse.resume(new RestException(Status.NOT_FOUND,
                                    getTopicNotFoundErrorMessage(topicName.toString())));
                            return;
                        }
                        PersistentSubscription sub = ((PersistentTopic) topic).getSubscription(subName);
                        if (sub == null) {
                            asyncResponse.resume(new RestException(Status.NOT_FOUND,
                                    getSubNotFoundErrorMessage(topicName.toString(), subName)));
                            return;
                        }
                        CompletableFuture<Integer> batchSizeFuture = new CompletableFuture<>();
                        getEntryBatchSize(batchSizeFuture, (PersistentTopic) topic, messageId, batchIndex);
                        batchSizeFuture.thenAccept(bi -> {
                            PositionImpl seekPosition = calculatePositionAckSet(isExcluded, bi, batchIndex,
                                    messageId);
                            sub.resetCursor(seekPosition).thenRun(() -> {
                                log.info("[{}][{}] successfully reset cursor on subscription {}"
                                                + " to position {}", clientAppId(),
                                        topicName, subName, messageId);
                                asyncResponse.resume(Response.noContent().build());
                                newTopicEvent(topicName, TopicEvent.SUBSCRIPTION_SEEK)
                                        .data(SubscriptionSeekEventData.builder()
                                                .subscriptionName(subName)
                                                .subscriptionType(sub.getType())
                                                .messageId(messageId.toString())
                                                .build())
                                        .dispatch();
                            }).exceptionally(ex -> {
                                Throwable t = (ex instanceof CompletionException ? ex.getCause() : ex);
                                log.warn("[{}][{}] Failed to reset cursor on subscription {}"
                                                + " to position {}", clientAppId(),
                                        topicName, subName, messageId, t);
                                if (t instanceof SubscriptionInvalidCursorPosition) {
                                    asyncResponse.resume(new RestException(Status.PRECONDITION_FAILED,
                                            "Unable to find position for position specified: "
                                                    + t.getMessage()));
                                } else if (t instanceof SubscriptionBusyException) {
                                    asyncResponse.resume(new RestException(Status.PRECONDITION_FAILED,
                                            "Failed for Subscription Busy: " + t.getMessage()));
                                } else {
                                    resumeAsyncResponseExceptionally(asyncResponse, t);
                                }
                                return null;
                            });
                        }).exceptionally(e -> {
                            asyncResponse.resume(e);
                            return null;
                        });
                    }).exceptionally(ex -> {
                        // If the exception is not redirect exception we need to log it.
                        if (isNot307And404Exception(ex)) {
                            log.warn("[{}][{}] Failed to reset cursor on subscription {} to position {}",
                                    clientAppId(), topicName, subName, messageId, ex.getCause());
                        }
                        resumeAsyncResponseExceptionally(asyncResponse, ex.getCause());
                        return null;
                    });
        }).exceptionally(ex -> {
            // If the exception is not redirect exception we need to log it.
            if (isNot307And404Exception(ex)) {
                log.warn("[{}][{}] Failed to reset cursor on subscription {} to position {}",
                        clientAppId(), topicName, subName, messageId, ex.getCause());
            }
            resumeAsyncResponseExceptionally(asyncResponse, ex.getCause());
            return null;
        });
    }

    private void getEntryBatchSize(CompletableFuture<Integer> batchSizeFuture, PersistentTopic topic,
                                   MessageIdImpl messageId, int batchIndex) {
        if (batchIndex >= 0) {
            try {
                ManagedLedgerImpl ledger = (ManagedLedgerImpl) topic.getManagedLedger();
                ledger.asyncReadEntry(new PositionImpl(messageId.getLedgerId(),
                        messageId.getEntryId()), new AsyncCallbacks.ReadEntryCallback() {
                    @Override
                    public void readEntryFailed(ManagedLedgerException exception, Object ctx) {
                        // Since we can't read the message from the storage layer,
                        // it might be an already delete message ID or an invalid message ID
                        // We should fall back to non batch index seek.
                        batchSizeFuture.complete(0);
                    }

                    @Override
                    public void readEntryComplete(Entry entry, Object ctx) {
                        try {
                            try {
                                if (entry == null) {
                                    batchSizeFuture.complete(0);
                                } else {
                                    MessageMetadata metadata =
                                            Commands.parseMessageMetadata(entry.getDataBuffer());
                                    batchSizeFuture.complete(metadata.getNumMessagesInBatch());
                                }
                            } catch (Exception e) {
                                batchSizeFuture.completeExceptionally(new RestException(e));
                            }
                        } finally {
                            if (entry != null) {
                                entry.release();
                            }
                        }
                    }

                    @Override
                    public String toString() {
                        return String.format("Topic [%s] get entry batch size",
                                PersistentTopicsBase.this.topicName);
                    }
                }, null);
            } catch (NullPointerException npe) {
                batchSizeFuture.completeExceptionally(new RestException(Status.NOT_FOUND, "Message not found"));
            } catch (Exception exception) {
                log.error("[{}] Failed to get message with ledgerId {} entryId {} from {}",
                        clientAppId(), messageId.getLedgerId(), messageId.getEntryId(), topicName, exception);
                batchSizeFuture.completeExceptionally(new RestException(exception));
            }
        } else {
            batchSizeFuture.complete(0);
        }
    }

    private PositionImpl calculatePositionAckSet(boolean isExcluded, int batchSize,
                                                 int batchIndex, MessageIdImpl messageId) {
        PositionImpl seekPosition;
        if (batchSize > 0) {
            long[] ackSet;
            BitSetRecyclable bitSet = BitSetRecyclable.create();
            bitSet.set(0, batchSize);
            if (isExcluded) {
                bitSet.clear(0, Math.max(batchIndex + 1, 0));
                if (bitSet.length() > 0) {
                    ackSet = bitSet.toLongArray();
                    seekPosition = PositionImpl.get(messageId.getLedgerId(),
                            messageId.getEntryId(), ackSet);
                } else {
                    seekPosition = PositionImpl.get(messageId.getLedgerId(), messageId.getEntryId());
                    seekPosition = seekPosition.getNext();
                }
            } else {
                if (batchIndex - 1 >= 0) {
                    bitSet.clear(0, batchIndex);
                    ackSet = bitSet.toLongArray();
                    seekPosition = PositionImpl.get(messageId.getLedgerId(),
                            messageId.getEntryId(), ackSet);
                } else {
                    seekPosition = PositionImpl.get(messageId.getLedgerId(), messageId.getEntryId());
                }
            }
            bitSet.recycle();
        } else {
            seekPosition = PositionImpl.get(messageId.getLedgerId(), messageId.getEntryId());
            seekPosition = isExcluded ? seekPosition.getNext() : seekPosition;
        }
        return seekPosition;
    }

    protected CompletableFuture<Response> internalGetMessageById(long ledgerId, long entryId, boolean authoritative) {
        CompletableFuture<Void> future;
        if (topicName.isGlobal()) {
            future = validateGlobalNamespaceOwnershipAsync(namespaceName);
        } else {
            future = CompletableFuture.completedFuture(null);
        }
        return future.thenCompose(__ -> {
            if (topicName.isPartitioned()) {
                return CompletableFuture.completedFuture(null);
            } else {
                return getPartitionedTopicMetadataAsync(topicName, authoritative, false)
                        .thenAccept(topicMetadata -> {
                            if (topicMetadata.partitions > 0) {
                                log.warn("[{}] Not supported getMessageById operation on partitioned-topic {}",
                                        clientAppId(), topicName);
                                throw new RestException(Status.METHOD_NOT_ALLOWED,
                                        "GetMessageById is not allowed on partitioned-topic");
                            }
                        });

            }
        })
        .thenCompose(ignore -> validateTopicOwnershipAsync(topicName, authoritative))
        .thenCompose(__ -> validateTopicOperationAsync(topicName, TopicOperation.PEEK_MESSAGES))
        .thenCompose(__ -> getTopicReferenceAsync(topicName))
        .thenCompose(topic -> {
            CompletableFuture<Response> results = new CompletableFuture<>();
            ManagedLedgerImpl ledger =
                    (ManagedLedgerImpl) ((PersistentTopic) topic).getManagedLedger();
            ledger.asyncReadEntry(new PositionImpl(ledgerId, entryId),
                    new AsyncCallbacks.ReadEntryCallback() {
                        @Override
                        public void readEntryFailed(ManagedLedgerException exception,
                                                    Object ctx) {
                            if (exception instanceof ManagedLedgerException.LedgerNotExistException) {
                                results.completeExceptionally(
                                        new RestException(Status.NOT_FOUND, "Message id not found"));
                                return;
                            }
                            results.completeExceptionally(new RestException(exception));
                        }

                        @Override
                        public void readEntryComplete(Entry entry, Object ctx) {
                            try {
                                results.complete(generateResponseWithEntry(entry, (PersistentTopic) topic));
                            } catch (IOException exception) {
                                results.completeExceptionally(new RestException(exception));
                            } finally {
                                if (entry != null) {
                                    entry.release();
                                }
                            }
                        }

                        @Override
                        public String toString() {
                            return String.format("Topic [%s] internal get message by id",
                                    PersistentTopicsBase.this.topicName);
                        }
                    }, null);
            return results;
        });
    }

    protected CompletableFuture<MessageId> internalGetMessageIdByTimestampAsync(long timestamp, boolean authoritative) {
        CompletableFuture<Void> future;
        if (topicName.isGlobal()) {
            future = validateGlobalNamespaceOwnershipAsync(namespaceName);
        } else {
            future = CompletableFuture.completedFuture(null);
        }

        return future.thenCompose(__ -> {
                if (topicName.isPartitioned()) {
                    return CompletableFuture.completedFuture(null);
                } else {
                    return getPartitionedTopicMetadataAsync(topicName, authoritative, false)
                        .thenAccept(metadata -> {
                            if (metadata.partitions > 0) {
                                throw new RestException(Status.METHOD_NOT_ALLOWED,
                                    "Get message ID by timestamp on a partitioned topic is not allowed, "
                                        + "please try do it on specific topic partition");
                            }
                        });
                }
            }).thenCompose(__ -> validateTopicOwnershipAsync(topicName, authoritative))
            .thenCompose(__ -> validateTopicOperationAsync(topicName, TopicOperation.PEEK_MESSAGES))
            .thenCompose(__ -> getTopicReferenceAsync(topicName))
            .thenCompose(topic -> {
                if (!(topic instanceof PersistentTopic)) {
                    log.error("[{}] Not supported operation of non-persistent topic {} ", clientAppId(), topicName);
                    throw new RestException(Status.METHOD_NOT_ALLOWED,
                        "Get message ID by timestamp on a non-persistent topic is not allowed");
                }
                ManagedLedger ledger = ((PersistentTopic) topic).getManagedLedger();
                return ledger.asyncFindPosition(entry -> {
                    try {
                        long entryTimestamp = Commands.getEntryTimestamp(entry.getDataBuffer());
                        return MessageImpl.isEntryPublishedEarlierThan(entryTimestamp, timestamp);
                    } catch (Exception e) {
                        log.error("[{}] Error deserializing message for message position find", topicName, e);
                    } finally {
                        entry.release();
                    }
                    return false;
                }).thenApply(position -> {
                    if (position == null) {
                        return null;
                    } else {
                        return new MessageIdImpl(position.getLedgerId(), position.getEntryId(),
                            topicName.getPartitionIndex());
                    }
                });
            });
    }

    protected CompletableFuture<Response> internalPeekNthMessageAsync(String subName, int messagePosition,
                                                                      boolean authoritative) {
        CompletableFuture<Void> ret = validateTopicOperationAsync(topicName, TopicOperation.PEEK_MESSAGES, subName);
        return ret.thenCompose(__ -> {
            // If the topic name is a partition name, no need to get partition topic metadata again
            if (!topicName.isPartitioned()) {
                return getPartitionedTopicMetadataAsync(topicName, authoritative, false)
                        .thenCompose(topicMetadata -> {
                            if (topicMetadata.partitions > 0) {
                                throw new RestException(Status.METHOD_NOT_ALLOWED,
                                        "Peek messages on a partitioned topic is not allowed");
                            }
                            return CompletableFuture.completedFuture(null);
                        });
            } else {
                return CompletableFuture.completedFuture(null);
            }
        }).thenCompose(__ -> validateTopicOwnershipAsync(topicName, authoritative))
        .thenCompose(__ -> getTopicReferenceAsync(topicName))
        .thenCompose(topic -> {
            CompletableFuture<Entry> entry;
            if (!(topic instanceof PersistentTopic)) {
                log.error("[{}] Not supported operation of non-persistent topic {} {}", clientAppId(),
                        topicName, subName);
                throw new RestException(Status.METHOD_NOT_ALLOWED,
                        "Peek messages on a non-persistent topic is not allowed");
            } else {
                if (subName.startsWith(((PersistentTopic) topic).getReplicatorPrefix())) {
                    PersistentReplicator repl = getReplicatorReference(subName, (PersistentTopic) topic);
                    entry = repl.peekNthMessage(messagePosition);
                } else {
                    PersistentSubscription sub =
                            (PersistentSubscription) getSubscriptionReference(subName, (PersistentTopic) topic);
                    entry = sub.peekNthMessage(messagePosition);
                }
            }
            return entry.thenApply(e -> Pair.of(e, (PersistentTopic) topic));
        }).thenCompose(entryTopicPair -> {
            Entry entry = entryTopicPair.getLeft();
            PersistentTopic persistentTopic = entryTopicPair.getRight();
            try {
                Response response = generateResponseWithEntry(entry, persistentTopic);
                return CompletableFuture.completedFuture(response);
            } catch (NullPointerException npe) {
                throw new RestException(Status.NOT_FOUND, "Message not found");
            } catch (Exception exception) {
                log.error("[{}] Failed to peek message at position {} from {} {}", clientAppId(),
                        messagePosition, topicName, subName, exception);
                throw new RestException(exception);
            } finally {
                if (entry != null) {
                    entry.release();
                }
            }
        });
    }

    protected CompletableFuture<Response> internalExamineMessageAsync(String initialPosition, long messagePosition,
                                                                      boolean authoritative) {
        CompletableFuture<Void> ret;
        if (topicName.isGlobal()) {
            ret = validateGlobalNamespaceOwnershipAsync(namespaceName);
        } else {
            ret = CompletableFuture.completedFuture(null);
        }

        ret = ret.thenCompose(__ -> validateTopicOwnershipAsync(topicName, authoritative));
        long messagePositionLocal = messagePosition < 1 ? 1 : messagePosition;
        String initialPositionLocal = initialPosition == null ? "latest" : initialPosition;
        if (!topicName.isPartitioned()) {
            ret = ret.thenCompose(__ -> getPartitionedTopicMetadataAsync(topicName, authoritative, false))
                    .thenCompose(partitionedTopicMetadata -> {
                        if (partitionedTopicMetadata.partitions > 0) {
                            throw new RestException(Status.METHOD_NOT_ALLOWED,
                                    "Examine messages on a partitioned topic is not allowed, "
                                            + "please try examine message on specific topic partition");
                        } else {
                            return CompletableFuture.completedFuture(null);
                        }
                    });
        }
        return ret.thenCompose(__ -> getTopicReferenceAsync(topicName))
                .thenCompose(topic -> {
                    if (!(topic instanceof PersistentTopic)) {
                        log.error("[{}] Not supported operation of non-persistent topic {} ", clientAppId(), topicName);
                        throw new RestException(Status.METHOD_NOT_ALLOWED,
                                "Examine messages on a non-persistent topic is not allowed");
                    }
                    try {
                        PersistentTopic persistentTopic = (PersistentTopic) topic;
                        long totalMessage = persistentTopic.getNumberOfEntries();
                        if (totalMessage <= 0) {
                            throw new RestException(Status.PRECONDITION_FAILED,
                                    "Could not examine messages due to the total message is zero");
                        }
                        PositionImpl startPosition = persistentTopic.getFirstPosition();

                        long messageToSkip = initialPositionLocal.equals("earliest") ? messagePositionLocal :
                                totalMessage - messagePositionLocal + 1;
                        CompletableFuture<Entry> future = new CompletableFuture<>();
                        PositionImpl readPosition = persistentTopic.getPositionAfterN(startPosition, messageToSkip);
                        persistentTopic.asyncReadEntry(readPosition, new AsyncCallbacks.ReadEntryCallback() {
                            @Override
                            public void readEntryComplete(Entry entry, Object ctx) {
                                future.complete(entry);
                            }

                            @Override
                            public void readEntryFailed(ManagedLedgerException exception, Object ctx) {
                                future.completeExceptionally(exception);
                            }

                            @Override
                            public String toString() {
                                return String.format("Topic [%s] internal examine message async",
                                        PersistentTopicsBase.this.topicName);
                            }
                        }, null);
                        return future.thenApply(entry -> Pair.of(entry, (PersistentTopic) topic));
                    } catch (ManagedLedgerException exception) {
                        log.error("[{}] Failed to examine message at position {} from {} due to {}", clientAppId(),
                                messagePosition,
                                topicName, exception);
                        throw new RestException(exception);
                    }

                }).thenApply(entryTopicPair -> {
                    Entry entry = entryTopicPair.getLeft();
                    PersistentTopic persistentTopic = entryTopicPair.getRight();
                    try {
                        return generateResponseWithEntry(entry, persistentTopic);
                    } catch (IOException exception) {
                        throw new RestException(exception);
                    } finally {
                        if (entry != null) {
                            entry.release();
                        }
                    }
                });
    }

    private Response generateResponseWithEntry(Entry entry, PersistentTopic persistentTopic) throws IOException {
        checkNotNull(entry);
        PositionImpl pos = (PositionImpl) entry.getPosition();
        ByteBuf metadataAndPayload = entry.getDataBuffer();

        long totalSize = metadataAndPayload.readableBytes();
        BrokerEntryMetadata brokerEntryMetadata = Commands.peekBrokerEntryMetadataIfExist(metadataAndPayload);
        MessageMetadata metadata = Commands.parseMessageMetadata(metadataAndPayload);

        ResponseBuilder responseBuilder = Response.ok();
        responseBuilder.header("X-Pulsar-Message-ID", pos.toString());
        for (KeyValue keyValue : metadata.getPropertiesList()) {
            responseBuilder.header("X-Pulsar-PROPERTY-" + keyValue.getKey(), keyValue.getValue());
        }
        if (brokerEntryMetadata != null) {
            if (brokerEntryMetadata.hasBrokerTimestamp()) {
                responseBuilder.header("X-Pulsar-Broker-Entry-METADATA-timestamp",
                        DateFormatter.format(brokerEntryMetadata.getBrokerTimestamp()));
            }
            if (brokerEntryMetadata.hasIndex()) {
                responseBuilder.header("X-Pulsar-Broker-Entry-METADATA-index", brokerEntryMetadata.getIndex());
            }
        }
        if (metadata.hasPublishTime()) {
            responseBuilder.header("X-Pulsar-publish-time", DateFormatter.format(metadata.getPublishTime()));
        }
        if (metadata.hasEventTime()) {
            responseBuilder.header("X-Pulsar-event-time", DateFormatter.format(metadata.getEventTime()));
        }
        if (metadata.hasDeliverAtTime()) {
            responseBuilder.header("X-Pulsar-deliver-at-time", DateFormatter.format(metadata.getDeliverAtTime()));
        }
        if (metadata.hasNumMessagesInBatch()) {
            responseBuilder.header("X-Pulsar-num-batch-message", metadata.getNumMessagesInBatch());
            responseBuilder.header("X-Pulsar-batch-size", totalSize
                    - metadata.getSerializedSize());
        }
        if (metadata.hasNullValue()) {
            responseBuilder.header("X-Pulsar-null-value", metadata.isNullValue());
        }
        if (metadata.hasNumChunksFromMsg()) {
            responseBuilder.header("X-Pulsar-PROPERTY-TOTAL-CHUNKS", Integer.toString(metadata.getNumChunksFromMsg()));
            responseBuilder.header("X-Pulsar-PROPERTY-CHUNK-ID", Integer.toString(metadata.getChunkId()));
        }
        responseBuilder.header("X-Pulsar-Is-Encrypted", metadata.getEncryptionKeysCount() > 0);

        if (metadata.hasProducerName()) {
            responseBuilder.header("X-Pulsar-producer-name", metadata.getProducerName());
        }
        if (metadata.hasSequenceId()) {
            responseBuilder.header("X-Pulsar-sequence-id", metadata.getSequenceId());
        }
        if (metadata.hasReplicatedFrom()) {
            responseBuilder.header("X-Pulsar-replicated-from", metadata.getReplicatedFrom());
        }
        for (String replicatedTo : metadata.getReplicateTosList()) {
            responseBuilder.header("X-Pulsar-replicated-to", replicatedTo);
        }
        if (metadata.hasPartitionKey()) {
            responseBuilder.header("X-Pulsar-partition-key", metadata.getPartitionKey());
        }
        if (metadata.hasCompression()) {
            responseBuilder.header("X-Pulsar-compression", metadata.getCompression());
        }
        if (metadata.hasUncompressedSize()) {
            responseBuilder.header("X-Pulsar-uncompressed-size", metadata.getUncompressedSize());
        }
        if (metadata.hasEncryptionAlgo()) {
            responseBuilder.header("X-Pulsar-encryption-algo", metadata.getEncryptionAlgo());
        }
        for (EncryptionKeys encryptionKeys : metadata.getEncryptionKeysList()) {
            responseBuilder.header("X-Pulsar-Base64-encryption-keys",
                    Base64.getEncoder().encodeToString(encryptionKeys.toByteArray()));
        }
        if (metadata.hasEncryptionParam()) {
            responseBuilder.header("X-Pulsar-Base64-encryption-param",
                    Base64.getEncoder().encodeToString(metadata.getEncryptionParam()));
        }
        if (metadata.hasSchemaVersion()) {
            responseBuilder.header("X-Pulsar-Base64-schema-version",
                    Base64.getEncoder().encodeToString(metadata.getSchemaVersion()));
        }
        if (metadata.hasPartitionKeyB64Encoded()) {
            responseBuilder.header("X-Pulsar-partition-key-b64-encoded", metadata.isPartitionKeyB64Encoded());
        }
        if (metadata.hasOrderingKey()) {
            responseBuilder.header("X-Pulsar-Base64-ordering-key",
                    Base64.getEncoder().encodeToString(metadata.getOrderingKey()));
        }
        if (metadata.hasMarkerType()) {
            responseBuilder.header("X-Pulsar-marker-type", metadata.getMarkerType());
        }
        if (metadata.hasTxnidLeastBits()) {
            responseBuilder.header("X-Pulsar-txnid-least-bits", metadata.getTxnidLeastBits());
        }
        if (metadata.hasTxnidMostBits()) {
            responseBuilder.header("X-Pulsar-txnid-most-bits", metadata.getTxnidMostBits());
        }
        if (metadata.hasHighestSequenceId()) {
            responseBuilder.header("X-Pulsar-highest-sequence-id", metadata.getHighestSequenceId());
        }
        if (metadata.hasUuid()) {
            responseBuilder.header("X-Pulsar-uuid", metadata.getUuid());
        }
        if (metadata.hasNumChunksFromMsg()) {
            responseBuilder.header("X-Pulsar-num-chunks-from-msg", metadata.getNumChunksFromMsg());
        }
        if (metadata.hasTotalChunkMsgSize()) {
            responseBuilder.header("X-Pulsar-total-chunk-msg-size", metadata.getTotalChunkMsgSize());
        }
        if (metadata.hasChunkId()) {
            responseBuilder.header("X-Pulsar-chunk-id", metadata.getChunkId());
        }
        if (metadata.hasNullPartitionKey()) {
            responseBuilder.header("X-Pulsar-null-partition-key", metadata.isNullPartitionKey());
        }
        if (metadata.hasTxnidMostBits() && metadata.hasTxnidLeastBits()) {
            TxnID txnID = new TxnID(metadata.getTxnidMostBits(), metadata.getTxnidLeastBits());
            boolean isTxnAborted = persistentTopic.isTxnAborted(txnID, (PositionImpl) entry.getPosition());
            responseBuilder.header("X-Pulsar-txn-aborted", isTxnAborted);
        }
        boolean isTxnUncommitted = ((PositionImpl) entry.getPosition())
                .compareTo(persistentTopic.getMaxReadPosition()) > 0;
        responseBuilder.header("X-Pulsar-txn-uncommitted", isTxnUncommitted);

        // Decode if needed
        CompressionCodec codec = CompressionCodecProvider.getCompressionCodec(metadata.getCompression());
        ByteBuf uncompressedPayload = codec.decode(metadataAndPayload, metadata.getUncompressedSize());

        // Copy into a heap buffer for output stream compatibility
        ByteBuf data = PulsarByteBufAllocator.DEFAULT.heapBuffer(uncompressedPayload.readableBytes(),
                uncompressedPayload.readableBytes());
        data.writeBytes(uncompressedPayload);
        uncompressedPayload.release();

        StreamingOutput stream = output -> {
            output.write(data.array(), data.arrayOffset(), data.readableBytes());
            data.release();
        };

        return responseBuilder.entity(stream).build();
    }

    protected CompletableFuture<PersistentOfflineTopicStats> internalGetBacklogAsync(boolean authoritative) {
        CompletableFuture<Void> ret;
        if (topicName.isGlobal()) {
            ret = validateGlobalNamespaceOwnershipAsync(namespaceName);
        } else {
            ret = CompletableFuture.completedFuture(null);
        }
        // Validate that namespace exists, throw 404 if it doesn't exist
        // note that we do not want to load the topic and hence skip authorization check
        return ret.thenCompose(__ -> namespaceResources().getPoliciesAsync(namespaceName))
                .thenCompose(__ -> {
                    PersistentOfflineTopicStats offlineTopicStats =
                            pulsar().getBrokerService().getOfflineTopicStat(topicName);
                    if (offlineTopicStats != null) {
                        // offline topic stat has a cost - so use cached value until TTL
                        long elapsedMs = System.currentTimeMillis() - offlineTopicStats.statGeneratedAt.getTime();
                        if (TimeUnit.MINUTES.convert(elapsedMs, TimeUnit.MILLISECONDS) < OFFLINE_TOPIC_STAT_TTL_MINS) {
                            return CompletableFuture.completedFuture(offlineTopicStats);
                        }
                    }

                    return pulsar().getBrokerService().getManagedLedgerConfig(topicName)
                            .thenCompose(config -> {
                                ManagedLedgerOfflineBacklog offlineTopicBacklog =
                                        new ManagedLedgerOfflineBacklog(config.getDigestType(), config.getPassword(),
                                                pulsar().getAdvertisedAddress(), false);
                                try {
                                    PersistentOfflineTopicStats estimateOfflineTopicStats =
                                            offlineTopicBacklog.estimateUnloadedTopicBacklog(
                                                    (ManagedLedgerFactoryImpl) pulsar().getManagedLedgerFactory(),
                                                    topicName);
                                    pulsar().getBrokerService()
                                            .cacheOfflineTopicStats(topicName, estimateOfflineTopicStats);
                                    return CompletableFuture.completedFuture(estimateOfflineTopicStats);
                                } catch (Exception e) {
                                    throw new RestException(e);
                                }
                            });

                });
    }

    protected CompletableFuture<Map<BacklogQuota.BacklogQuotaType, BacklogQuota>> internalGetBacklogQuota(
            boolean applied, boolean isGlobal) {
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
            .thenApply(op -> {
                Map<BacklogQuota.BacklogQuotaType, BacklogQuota> quotaMap = op
                        .map(TopicPolicies::getBackLogQuotaMap)
                        .map(map -> {
                            HashMap<BacklogQuota.BacklogQuotaType, BacklogQuota> hashMap = new HashMap<>();
                            map.forEach((key, value) -> hashMap.put(BacklogQuota.BacklogQuotaType.valueOf(key), value));
                            return hashMap;
                        }).orElse(new HashMap<>());
                if (applied && quotaMap.isEmpty()) {
                    quotaMap = getNamespacePolicies(namespaceName).backlog_quota_map;
                    if (quotaMap.isEmpty()) {
                        for (BacklogQuota.BacklogQuotaType backlogQuotaType : BacklogQuota.BacklogQuotaType.values()) {
                            quotaMap.put(
                                    backlogQuotaType,
                                    namespaceBacklogQuota(namespaceName, backlogQuotaType)
                            );
                        }
                    }
                }
                return quotaMap;
        });
    }

    protected void internalGetBacklogSizeByMessageId(AsyncResponse asyncResponse,
                                                     MessageIdImpl messageId, boolean authoritative) {
        CompletableFuture<Void> ret;
        // If the topic name is a partition name, no need to get partition topic metadata again
        if (!topicName.isPartitioned()) {
            ret = getPartitionedTopicMetadataAsync(topicName, authoritative, false)
                    .thenCompose(topicMetadata -> {
                        if (topicMetadata.partitions > 0) {
                            log.warn("[{}] Not supported calculate backlog size operation on partitioned-topic {}",
                                    clientAppId(), topicName);
                            asyncResponse.resume(new RestException(Status.METHOD_NOT_ALLOWED,
                                    "calculate backlog size is not allowed for partitioned-topic"));
                        }
                        return CompletableFuture.completedFuture(null);
                    });
        } else {
            ret = CompletableFuture.completedFuture(null);
        }
        CompletableFuture<Void> future;
        if (topicName.isGlobal()) {
            future = ret.thenCompose(__ -> validateGlobalNamespaceOwnershipAsync(namespaceName));
        } else {
            future = ret;
        }
        future.thenAccept(__ -> validateTopicOwnershipAsync(topicName, authoritative)
                .thenCompose(unused -> validateTopicOperationAsync(topicName,
                        TopicOperation.GET_BACKLOG_SIZE))
                .thenCompose(unused -> getTopicReferenceAsync(topicName))
                .thenAccept(t -> {
                    PersistentTopic topic = (PersistentTopic) t;
                    PositionImpl pos = new PositionImpl(messageId.getLedgerId(),
                            messageId.getEntryId());
                    if (topic == null) {
                        asyncResponse.resume(new RestException(Status.NOT_FOUND,
                                getTopicNotFoundErrorMessage(topicName.toString())));
                        return;
                    }
                    ManagedLedgerImpl managedLedger =
                            (ManagedLedgerImpl) topic.getManagedLedger();
                    if (messageId.getLedgerId() == -1) {
                        asyncResponse.resume(managedLedger.getTotalSize());
                    } else {
                        asyncResponse.resume(managedLedger.getEstimatedBacklogSize(pos));
                    }
                }).exceptionally(ex -> {
                    // If the exception is not redirect exception we need to log it.
                    if (isNot307And404Exception(ex)) {
                        log.error("[{}] Failed to get backlog size for topic {}", clientAppId(),
                                topicName, ex);
                    }
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                })).exceptionally(ex -> {
                        // If the exception is not redirect exception we need to log it.
                        if (isNot307And404Exception(ex)) {
                            log.error("[{}] Failed to validate global namespace ownership "
                                    + "to get backlog size for topic {}", clientAppId(), topicName, ex);
                        }
                        resumeAsyncResponseExceptionally(asyncResponse, ex);
                        return null;
                });
    }

    protected CompletableFuture<Void> internalSetBacklogQuota(BacklogQuota.BacklogQuotaType backlogQuotaType,
                                                              BacklogQuotaImpl backlogQuota, boolean isGlobal) {
        BacklogQuota.BacklogQuotaType finalBacklogQuotaType = backlogQuotaType == null
                ? BacklogQuota.BacklogQuotaType.destination_storage : backlogQuotaType;
        try {
            // Null value means delete backlog quota.
            if (backlogQuota != null) {
                backlogQuota.validate();
            }
        } catch (IllegalArgumentException e) {
            RestException restException = new RestException(Status.BAD_REQUEST, String.format("Set namespace[%s]"
                + " backlog quota failed because the data validation failed. %s", namespaceName, e.getMessage()));
            return CompletableFuture.failedFuture(restException);
        }

        return validateTopicPolicyOperationAsync(topicName, PolicyName.BACKLOG, PolicyOperation.WRITE)
                .thenCompose(__ -> validatePoliciesReadOnlyAccessAsync())
                .thenCompose(__ -> getTopicPoliciesAsyncWithRetry(topicName, isGlobal))
                .thenCompose(op -> {
                    TopicPolicies topicPolicies = op.orElseGet(TopicPolicies::new);
                    return getRetentionPoliciesAsync(topicName, topicPolicies)
                            .thenCompose(retentionPolicies -> {
                                if (!checkBacklogQuota(backlogQuota, retentionPolicies)) {
                                    log.warn(
                                            "[{}] Failed to update backlog configuration for topic {}: conflicts with"
                                                    + " retention quota",
                                            clientAppId(), topicName);
                                    return FutureUtil.failedFuture(new RestException(Status.PRECONDITION_FAILED,
                                            "Backlog Quota exceeds configured retention quota for topic. "
                                                    + "Please increase retention quota and retry"));
                                }
                                if (backlogQuota != null) {
                                    topicPolicies.getBackLogQuotaMap().put(finalBacklogQuotaType.name(), backlogQuota);
                                } else {
                                    topicPolicies.getBackLogQuotaMap().remove(finalBacklogQuotaType.name());
                                }
                                Map<String, BacklogQuotaImpl> backLogQuotaMap = topicPolicies.getBackLogQuotaMap();
                                topicPolicies.setIsGlobal(isGlobal);
                                return pulsar().getTopicPoliciesService()
                                        .updateTopicPoliciesAsync(topicName, topicPolicies)
                                        .thenRun(() -> {
                                            try {
                                                log.info(
                                                        "[{}] Successfully updated backlog quota map: namespace={}, "
                                                                + "topic={}, map={}",
                                                        clientAppId(),
                                                        namespaceName,
                                                        topicName.getLocalName(),
                                                        objectWriter().writeValueAsString(backLogQuotaMap));
                                            } catch (JsonProcessingException ignore) {
                                            }
                                        });
                            });
                });
    }

    protected CompletableFuture<Void> internalSetReplicationClusters(List<String> clusterIds) {
        if (CollectionUtils.isEmpty(clusterIds)) {
            return CompletableFuture.failedFuture(new RestException(Status.PRECONDITION_FAILED,
                    "ClusterIds should not be null or empty"));
        }
        Set<String> replicationClusters = Sets.newHashSet(clusterIds);
        return validateTopicPolicyOperationAsync(topicName, PolicyName.REPLICATION, PolicyOperation.WRITE)
                .thenCompose(__ -> validatePoliciesReadOnlyAccessAsync())
                .thenAccept(__ -> {
                    if (CollectionUtils.isEmpty(clusterIds)) {
                        throw new RestException(Status.PRECONDITION_FAILED, "ClusterIds should not be null or empty");
                    }
                }).thenCompose(__ -> clustersAsync())
                .thenCompose(clusters -> {
                    List<CompletableFuture<Void>> futures = new ArrayList<>(replicationClusters.size());
                    for (String clusterId : replicationClusters) {
                        if (!clusters.contains(clusterId)) {
                            throw new RestException(Status.FORBIDDEN, "Invalid cluster id: " + clusterId);
                        }
                        futures.add(validatePeerClusterConflictAsync(clusterId, replicationClusters));
                        futures.add(validateClusterForTenantAsync(namespaceName.getTenant(), clusterId));
                    }
                    return FutureUtil.waitForAll(futures);
                }).thenCompose(__ -> {
                    if (!pulsar().getConfig().isCreateTopicToRemoteClusterForReplication()) {
                        log.info("[{}] Skip creating partitioned for topic {} for the remote clusters {}",
                                clientAppId(), topicName, replicationClusters.stream().filter(v ->
                                        !pulsar().getConfig().getClusterName().equals(v)).collect(Collectors.toList()));
                        return CompletableFuture.completedFuture(null);
                    }
                    // Sync to create partitioned topic on the remote cluster if needed.
                    TopicName topicNameWithoutPartition = TopicName.get(topicName.getPartitionedTopicName());
                    return pulsar().getPulsarResources().getNamespaceResources().getPartitionedTopicResources()
                        .getPartitionedTopicMetadataAsync(topicNameWithoutPartition).thenCompose(topicMetaOp -> {
                            // Skip to create topic if the topic is non-partitioned, because the replicator will create
                            // it automatically.
                            if (topicMetaOp.isEmpty()) {
                                return CompletableFuture.completedFuture(null);
                            }
                            return FutureUtil.waitForAll(
                                    internalCreatePartitionedTopicToReplicatedClustersInBackground(replicationClusters,
                                    topicMetaOp.get().partitions).values());
                        });
                }).thenCompose(__ ->
                    getTopicPoliciesAsyncWithRetry(topicName).thenCompose(op -> {
                            TopicPolicies topicPolicies = op.orElseGet(TopicPolicies::new);
                            topicPolicies.setReplicationClusters(clusterIds);
                            return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, topicPolicies)
                                    .thenRun(() -> {
                                        log.info("[{}] Successfully set replication clusters for namespace={}, "
                                                        + "topic={}, clusters={}",
                                                clientAppId(),
                                                namespaceName,
                                                topicName.getLocalName(),
                                                topicPolicies.getReplicationClusters());
                                    });
                        }
                ));
    }

    protected CompletableFuture<Void> internalRemoveReplicationClusters() {
        return validateTopicPolicyOperationAsync(topicName, PolicyName.REPLICATION, PolicyOperation.WRITE)
                .thenCompose(__ -> validatePoliciesReadOnlyAccessAsync())
                .thenCompose(__ -> getTopicPoliciesAsyncWithRetry(topicName).thenCompose(op -> {
                            TopicPolicies topicPolicies = op.orElseGet(TopicPolicies::new);
                            topicPolicies.setReplicationClusters(null);
                            return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, topicPolicies)
                                    .thenRun(() -> {
                                        log.info("[{}] Successfully set replication clusters for namespace={}, "
                                                        + "topic={}, clusters={}",
                                                clientAppId(),
                                                namespaceName,
                                                topicName.getLocalName(),
                                                topicPolicies.getReplicationClusters());
                                    });
                        })
                );
    }

    protected CompletableFuture<Boolean> internalGetDeduplication(boolean applied, boolean isGlobal) {
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
            .thenApply(op -> op.map(TopicPolicies::getDeduplicationEnabled)
                .orElseGet(() -> {
                    if (applied) {
                        Boolean enabled = getNamespacePolicies(namespaceName).deduplicationEnabled;
                        return enabled == null ? config().isBrokerDeduplicationEnabled() : enabled;
                    }
                    return null;
                }));
    }

    protected CompletableFuture<Void> internalSetDeduplication(Boolean enabled, boolean isGlobal) {
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
            .thenCompose(op -> {
                TopicPolicies topicPolicies = op.orElseGet(TopicPolicies::new);
                topicPolicies.setDeduplicationEnabled(enabled);
                topicPolicies.setIsGlobal(isGlobal);
                return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, topicPolicies);
            });
    }

    protected CompletableFuture<Void> internalSetMessageTTL(Integer ttlInSecond, boolean isGlobal) {
        //Validate message ttl value.
        if (ttlInSecond != null && ttlInSecond < 0) {
            return FutureUtil.failedFuture(new RestException(Status.PRECONDITION_FAILED,
                    "Invalid value for message TTL"));
        }

        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
            .thenCompose(op -> {
                TopicPolicies topicPolicies = op.orElseGet(TopicPolicies::new);
                topicPolicies.setMessageTTLInSeconds(ttlInSecond);
                topicPolicies.setIsGlobal(isGlobal);
                return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, topicPolicies)
                        .thenRun(() ->
                                log.info("[{}] Successfully set topic message ttl: namespace={}, topic={}, ttl={}",
                                        clientAppId(), namespaceName, topicName.getLocalName(), ttlInSecond));
            });
    }

    private CompletableFuture<RetentionPolicies> getRetentionPoliciesAsync(TopicName topicName,
                                                                           TopicPolicies topicPolicies) {
        RetentionPolicies retentionPolicies = topicPolicies.getRetentionPolicies();
        if (retentionPolicies != null) {
            return CompletableFuture.completedFuture(retentionPolicies);
        }
        return getNamespacePoliciesAsync(topicName.getNamespaceObject())
                .thenApply(policies -> policies.retention_policies);
    }

    protected CompletableFuture<RetentionPolicies> internalGetRetention(boolean applied, boolean isGlobal) {
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
            .thenApply(op -> op.map(TopicPolicies::getRetentionPolicies).orElseGet(() -> {
                if (applied) {
                    RetentionPolicies policies = getNamespacePolicies(namespaceName).retention_policies;
                    return policies == null ? new RetentionPolicies(
                            config().getDefaultRetentionTimeInMinutes(), config().getDefaultRetentionSizeInMB())
                            : policies;
                }
                return null;
            }));
    }

    protected CompletableFuture<Void> internalSetRetention(RetentionPolicies retention, boolean isGlobal) {
        validateRetentionPolicies(retention);
        if (retention == null) {
            return CompletableFuture.completedFuture(null);
        }
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
            .thenCompose(op -> {
                TopicPolicies topicPolicies = op.orElseGet(TopicPolicies::new);
                for (BacklogQuota.BacklogQuotaType backlogQuotaType : BacklogQuota.BacklogQuotaType.values()) {
                    BacklogQuota backlogQuota = topicPolicies.getBackLogQuotaMap().get(backlogQuotaType.name());
                    if (backlogQuota == null) {
                        Policies policies = getNamespacePolicies(topicName.getNamespaceObject());
                        backlogQuota = policies.backlog_quota_map.get(backlogQuotaType);
                    }
                    if (!checkBacklogQuota(backlogQuota, retention)) {
                        log.warn(
                                "[{}] Failed to update retention quota configuration for topic {}: "
                                        + "conflicts with retention quota",
                                clientAppId(), topicName);
                        return FutureUtil.failedFuture(new RestException(Status.PRECONDITION_FAILED,
                                "Retention Quota must exceed configured backlog quota for topic. "
                                        + "Please increase retention quota and retry"));
                    }
                }
                topicPolicies.setRetentionPolicies(retention);
                topicPolicies.setIsGlobal(isGlobal);
                return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, topicPolicies);
            });
    }

    protected CompletableFuture<Void> internalRemoveRetention(boolean isGlobal) {
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
                .thenCompose(op -> {
                    if (!op.isPresent()) {
                        return CompletableFuture.completedFuture(null);
                    }
                    op.get().setRetentionPolicies(null);
                    return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, op.get());
                });
    }

    protected CompletableFuture<PersistencePolicies> internalGetPersistence(boolean applied, boolean isGlobal) {
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
            .thenApply(op -> op.map(TopicPolicies::getPersistence)
                .orElseGet(() -> {
                    if (applied) {
                        PersistencePolicies namespacePolicy = getNamespacePolicies(namespaceName)
                                .persistence;
                        return namespacePolicy == null
                                ? new PersistencePolicies(
                                pulsar().getConfiguration().getManagedLedgerDefaultEnsembleSize(),
                                pulsar().getConfiguration().getManagedLedgerDefaultWriteQuorum(),
                                pulsar().getConfiguration().getManagedLedgerDefaultAckQuorum(),
                                pulsar().getConfiguration().getManagedLedgerDefaultMarkDeleteRateLimit())
                                : namespacePolicy;
                    }
                    return null;
                }));
    }

    protected CompletableFuture<Void> internalSetPersistence(PersistencePolicies persistencePolicies,
                                                             boolean isGlobal) {
        validatePersistencePolicies(persistencePolicies);
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
            .thenCompose(op -> {
                TopicPolicies topicPolicies = op.orElseGet(TopicPolicies::new);
                topicPolicies.setPersistence(persistencePolicies);
                topicPolicies.setIsGlobal(isGlobal);
                return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, topicPolicies);
            });
    }

    protected CompletableFuture<Void> internalRemovePersistence(boolean isGlobal) {
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
            .thenCompose(op -> {
                if (!op.isPresent()) {
                    return CompletableFuture.completedFuture(null);
                }
                op.get().setPersistence(null);
                op.get().setIsGlobal(isGlobal);
                return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, op.get());
            });
    }

    protected CompletableFuture<Void> internalSetMaxMessageSize(Integer maxMessageSize, boolean isGlobal) {
        if (maxMessageSize != null && (maxMessageSize < 0 || maxMessageSize > config().getMaxMessageSize())) {
            throw new RestException(Status.PRECONDITION_FAILED
                    , "topic-level maxMessageSize must be greater than or equal to 0 "
                    + "and must be smaller than that in the broker-level");
        }

        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
            .thenCompose(op -> {
                TopicPolicies topicPolicies = op.orElseGet(TopicPolicies::new);
                topicPolicies.setMaxMessageSize(maxMessageSize);
                topicPolicies.setIsGlobal(isGlobal);
                return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, topicPolicies);
            });
    }

    protected CompletableFuture<Optional<Integer>> internalGetMaxMessageSize(boolean isGlobal) {
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
                .thenApply(op -> op.map(TopicPolicies::getMaxMessageSize));
    }

    protected CompletableFuture<Integer> internalGetMaxProducers(boolean applied, boolean isGlobal) {
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
            .thenApply(op -> op.map(TopicPolicies::getMaxProducerPerTopic)
                .orElseGet(() -> {
                    if (applied) {
                        Integer maxProducer = getNamespacePolicies(namespaceName).max_producers_per_topic;
                        return maxProducer == null ? config().getMaxProducersPerTopic() : maxProducer;
                    }
                    return null;
                }));
    }

    protected CompletableFuture<Void> internalSetMaxProducers(Integer maxProducers, boolean isGlobal) {
        if (maxProducers != null && maxProducers < 0) {
            throw new RestException(Status.PRECONDITION_FAILED,
                    "maxProducers must be 0 or more");
        }
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
            .thenCompose(op -> {
                TopicPolicies topicPolicies = op.orElseGet(TopicPolicies::new);
                topicPolicies.setMaxProducerPerTopic(maxProducers);
                topicPolicies.setIsGlobal(isGlobal);
                return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, topicPolicies);
            });

    }

    protected CompletableFuture<Optional<Integer>> internalGetMaxSubscriptionsPerTopic(boolean isGlobal) {
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
                .thenApply(op -> op.map(TopicPolicies::getMaxSubscriptionsPerTopic));
    }

    protected CompletableFuture<Void> internalSetMaxSubscriptionsPerTopic(Integer maxSubscriptionsPerTopic,
                                                                          boolean isGlobal) {
        if (maxSubscriptionsPerTopic != null && maxSubscriptionsPerTopic < 0) {
            throw new RestException(Status.PRECONDITION_FAILED,
                    "maxSubscriptionsPerTopic must be 0 or more");
        }

        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
            .thenCompose(op -> {
                TopicPolicies topicPolicies = op.orElseGet(TopicPolicies::new);
                topicPolicies.setMaxSubscriptionsPerTopic(maxSubscriptionsPerTopic);
                topicPolicies.setIsGlobal(isGlobal);
                return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, topicPolicies);
            });
    }

    protected CompletableFuture<DispatchRateImpl> internalGetReplicatorDispatchRate(String cluster, boolean applied,
                                                                                    boolean isGlobal) {
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
                .thenApply(op -> op.map(n -> {
                    // Prioritize getting the dispatch rate from the replicatorDispatchRateMap if a specific cluster
                    // is provided.
                    // If the cluster is empty, it means the user has not explicitly set a rate for a particular
                    // cluster,
                    // so we still attempt to retrieve the value from the replicatorDispatchRateMap using the current
                    // cluster.
                    // If `applied` is true, we also need to consider the default cluster rate and finally fallback
                    // to `getReplicatorDispatchRate()` for backward compatibility.
                    Map<String, DispatchRateImpl> dispatchRateMap = new HashMap<>();
                    if (n.getReplicatorDispatchRateMap() != null) {
                        dispatchRateMap = n.getReplicatorDispatchRateMap();
                    }
                    DispatchRateImpl dispatchRate = dispatchRateMap.get(getReplicatorDispatchRateKey(cluster));
                    if (dispatchRate != null) {
                        return dispatchRate;
                    }

                    if (applied || StringUtils.isEmpty(cluster)) {
                        dispatchRate =
                                dispatchRateMap.get(pulsar().getConfiguration().getClusterName());
                        if (dispatchRate != null) {
                            return dispatchRate;
                        }
                        // Backward compatibility.
                        return n.getReplicatorDispatchRate();
                    }
                    return null;
                }).orElseGet(() -> {
                    if (!applied) {
                        return null;
                    }
                    Map<String, DispatchRateImpl> replicatorDispatchRate =
                            getNamespacePolicies(namespaceName).replicatorDispatchRate;
                    DispatchRateImpl namespacePolicy =
                            replicatorDispatchRate.getOrDefault(getReplicatorDispatchRateKey(cluster),
                                    replicatorDispatchRate.get(pulsar().getConfiguration().getClusterName()));
                    return namespacePolicy == null ? replicatorDispatchRate() : namespacePolicy;
                }));
    }

    protected CompletableFuture<Void> internalSetReplicatorDispatchRate(String cluster, DispatchRateImpl dispatchRate,
                                                                        boolean isGlobal) {
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
                .thenCompose(op -> {
                    TopicPolicies topicPolicies = op.orElseGet(TopicPolicies::new);
                    if (topicPolicies.getReplicatorDispatchRateMap() == null) {
                        topicPolicies.setReplicatorDispatchRateMap(new HashMap<>());
                    }
                    if (dispatchRate == null) {
                        topicPolicies.getReplicatorDispatchRateMap()
                                .remove(getReplicatorDispatchRateKey(cluster));
                    } else {
                        topicPolicies.getReplicatorDispatchRateMap()
                                .put(getReplicatorDispatchRateKey(cluster), dispatchRate);
                    }
                    if (StringUtils.isEmpty(cluster)) {
                        topicPolicies.setReplicatorDispatchRate(dispatchRate);
                    }
                    topicPolicies.setIsGlobal(isGlobal);
                    return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, topicPolicies);
                });
    }

    protected CompletableFuture<Void> preValidation(boolean authoritative) {
        if (!config().isTopicLevelPoliciesEnabled()) {
            return FutureUtil.failedFuture(new RestException(Status.METHOD_NOT_ALLOWED,
                    "Topic level policies is disabled, to enable the topic level policy and retry."));
        }
        if (topicName.isPartitioned()) {
            return FutureUtil.failedFuture(new RestException(Status.PRECONDITION_FAILED,
                    "Not allowed to set/get topic policy for a partition"));
        }
        CompletableFuture<Void> ret;
        if (topicName.isGlobal()) {
            ret = validateGlobalNamespaceOwnershipAsync(namespaceName);
        } else {
            ret = CompletableFuture.completedFuture(null);
        }
        return ret
                .thenCompose(__ -> checkTopicExistsWithTypeAsync(topicName))
                .thenCompose(topicExistsInfo -> {
                    if (!topicExistsInfo.isExists()) {
                        topicExistsInfo.recycle();
                        throw new RestException(Status.NOT_FOUND, getTopicNotFoundErrorMessage(topicName.toString()));
                    } else {
                        return getPartitionedTopicMetadataAsync(topicName, false, false)
                            .thenCompose(metadata -> {
                                if (metadata.partitions > 0) {
                                    return validateTopicOwnershipAsync(TopicName.get(topicName.toString()
                                    + PARTITIONED_TOPIC_SUFFIX + 0), authoritative);
                                } else {
                                    return validateTopicOwnershipAsync(topicName, authoritative);
                                }
                        });
                    }
        });
    }

    protected CompletableFuture<Void> internalRemoveMaxProducers(boolean isGlobal) {
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
            .thenCompose(op -> {
                if (!op.isPresent()) {
                    return CompletableFuture.completedFuture(null);
                }
                op.get().setMaxProducerPerTopic(null);
                op.get().setIsGlobal(isGlobal);
                return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, op.get());
            });
    }

    protected CompletableFuture<Integer> internalGetMaxConsumers(boolean applied, boolean isGlobal) {
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
            .thenApply(op -> op.map(TopicPolicies::getMaxConsumerPerTopic)
                .orElseGet(() -> {
                    if (applied) {
                        Integer maxConsumer = getNamespacePolicies(namespaceName).max_consumers_per_topic;
                        return maxConsumer == null ? config().getMaxConsumersPerTopic() : maxConsumer;
                    }
                    return null;
                }));
    }

    protected CompletableFuture<Void> internalSetMaxConsumers(Integer maxConsumers, boolean isGlobal) {
        if (maxConsumers != null && maxConsumers < 0) {
            throw new RestException(Status.PRECONDITION_FAILED,
                    "maxConsumers must be 0 or more");
        }
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
            .thenCompose(op -> {
                TopicPolicies topicPolicies = op.orElseGet(TopicPolicies::new);
                topicPolicies.setMaxConsumerPerTopic(maxConsumers);
                topicPolicies.setIsGlobal(isGlobal);
                return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, topicPolicies);
            });
    }

    protected CompletableFuture<Void> internalRemoveMaxConsumers(boolean isGlobal) {
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
            .thenCompose(op -> {
                if (!op.isPresent()) {
                    return CompletableFuture.completedFuture(null);
                }
                op.get().setMaxConsumerPerTopic(null);
                op.get().setIsGlobal(isGlobal);
                return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, op.get());
            });

    }

    protected CompletableFuture<MessageId> internalTerminateAsync(boolean authoritative) {
        if (SystemTopicNames.isSystemTopic(topicName)) {
            return FutureUtil.failedFuture(new RestException(Status.METHOD_NOT_ALLOWED,
                    "Termination of a system topic is not allowed"));
        }

        CompletableFuture<Void> ret;
        if (topicName.isGlobal()) {
            ret = validateGlobalNamespaceOwnershipAsync(namespaceName);
        } else {
            ret = CompletableFuture.completedFuture(null);
        }
        return ret.thenCompose(__ -> validateTopicOwnershipAsync(topicName, authoritative))
                .thenCompose(__ -> validateTopicOperationAsync(topicName, TopicOperation.TERMINATE))
                .thenCompose(__ -> getPartitionedTopicMetadataAsync(topicName, authoritative, false))
                .thenAccept(partitionMetadata -> {
                    if (partitionMetadata.partitions > 0) {
                        throw new RestException(Status.METHOD_NOT_ALLOWED,
                                "Termination of a partitioned topic is not allowed");
                    }
                })
                .thenCompose(__ -> getTopicReferenceAsync(topicName))
                .thenCompose(topic -> {
                    if (!(topic instanceof PersistentTopic)) {
                        throw new RestException(Status.METHOD_NOT_ALLOWED,
                                "Termination of a non-persistent topic is not allowed");
                    }
                    return ((PersistentTopic) topic).terminate();
                });
    }

    protected void internalTerminatePartitionedTopic(AsyncResponse asyncResponse, boolean authoritative) {
        if (topicName.isPartitioned()) {
            String msg = "Termination of a non-partitioned topic is not allowed using partitioned-terminate"
                    + ", please use terminate commands";
            log.error("[{}] [{}] {}", clientAppId(), topicName, msg);
            asyncResponse.resume(new RestException(Status.METHOD_NOT_ALLOWED, msg));
            return;
        }

        CompletableFuture<Void> future;
        if (topicName.isGlobal()) {
            future = validateGlobalNamespaceOwnershipAsync(namespaceName);
        } else {
            future = CompletableFuture.completedFuture(null);
        }

        future.thenCompose(__ -> validateTopicOperationAsync(topicName, TopicOperation.TERMINATE)
                .thenCompose(unused -> getPartitionedTopicMetadataAsync(topicName, authoritative, false))
                .thenAccept(partitionMetadata -> {
                    if (partitionMetadata.partitions == 0) {
                        String msg = "Termination of a non-partitioned topic is not allowed using partitioned-terminate"
                                + ", please use terminate commands";
                        log.error("[{}] [{}] {}", clientAppId(), topicName, msg);
                        asyncResponse.resume(new RestException(Status.METHOD_NOT_ALLOWED, msg));
                        return;
                    }
                    if (partitionMetadata.partitions > 0) {
                        Map<Integer, MessageId> messageIds = new ConcurrentHashMap<>(partitionMetadata.partitions);
                        final List<CompletableFuture<MessageId>> futures =
                                new ArrayList<>(partitionMetadata.partitions);

                        for (int i = 0; i < partitionMetadata.partitions; i++) {
                            TopicName topicNamePartition = topicName.getPartition(i);
                            try {
                                int finalI = i;
                                futures.add(pulsar().getAdminClient().topics()
                                        .terminateTopicAsync(topicNamePartition.toString())
                                        .whenComplete((messageId, throwable) -> {
                                            if (throwable != null) {
                                                log.error("[{}] Failed to terminate topic {}", clientAppId(),
                                                        topicNamePartition, throwable);
                                                asyncResponse.resume(new RestException(throwable));
                                            }
                                            messageIds.put(finalI, messageId);
                                        }));
                            } catch (Exception e) {
                                log.error("[{}] Failed to terminate topic {}", clientAppId(), topicNamePartition,
                                        e);
                                throw new RestException(e);
                            }
                        }
                        FutureUtil.waitForAll(futures).handle((result, exception) -> {
                            if (exception != null) {
                                Throwable t = exception.getCause();
                                if (t instanceof NotFoundException) {
                                    asyncResponse.resume(new RestException(Status.NOT_FOUND,
                                            getTopicNotFoundErrorMessage(topicName.toString())));
                                } else {
                                    log.error("[{}] Failed to terminate topic {}", clientAppId(), topicName, t);
                                    asyncResponse.resume(new RestException(t));
                                }
                            }
                            asyncResponse.resume(messageIds);
                            return null;
                        });
                    }
                }).exceptionally(ex -> {
                    // If the exception is not redirect exception we need to log it.
                    if (isNot307And404Exception(ex)) {
                        log.error("[{}] Failed to terminate topic {}", clientAppId(), topicName, ex);
                    }
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                })
        ).exceptionally(ex -> {
            // If the exception is not redirect exception we need to log it.
            if (isNot307And404Exception(ex)) {
                log.error("[{}] Failed to terminate topic {}", clientAppId(), topicName, ex);
            }
            resumeAsyncResponseExceptionally(asyncResponse, ex);
            return null;
        });
    }

    protected void internalExpireMessagesByTimestamp(AsyncResponse asyncResponse, String subName,
                                                     int expireTimeInSeconds, boolean authoritative) {
        CompletableFuture<Void> future;
        if (topicName.isGlobal()) {
            future = validateGlobalNamespaceOwnershipAsync(namespaceName);
        } else {
            future = CompletableFuture.completedFuture(null);
        }
        future.thenCompose(__ ->
                validateTopicOwnershipAsync(topicName, authoritative)
                .thenCompose(unused -> validateTopicOperationAsync(topicName, TopicOperation.EXPIRE_MESSAGES, subName))
                .thenCompose(unused2 ->
                        // If the topic name is a partition name, no need to get partition topic metadata again
                        getPartitionedTopicMetadataAsync(topicName, authoritative, false)
                                .thenCompose(partitionMetadata -> {
                                    if (topicName.isPartitioned()) {
                                        return internalExpireMessagesByTimestampForSinglePartitionAsync
                                                (partitionMetadata, subName, expireTimeInSeconds)
                                                .thenAccept(unused3 ->
                                                        asyncResponse.resume(Response.noContent().build()));
                                    } else {
                                        if (partitionMetadata.partitions > 0) {
                                            return CompletableFuture.completedFuture(null).thenAccept(unused -> {
                                                final List<CompletableFuture<Void>> futures = new ArrayList<>();

                                                // expire messages for each partition topic
                                                for (int i = 0; i < partitionMetadata.partitions; i++) {
                                                    TopicName topicNamePartition = topicName.getPartition(i);
                                                    try {
                                                        futures.add(pulsar()
                                                                .getAdminClient()
                                                                .topics()
                                                                .expireMessagesAsync(topicNamePartition.toString(),
                                                                        subName, expireTimeInSeconds));
                                                    } catch (Exception e) {
                                                        log.error("[{}] Failed to expire messages up to {} on {}",
                                                                clientAppId(),
                                                                expireTimeInSeconds, topicNamePartition, e);
                                                        asyncResponse.resume(new RestException(e));
                                                        return;
                                                    }
                                                }

                                                FutureUtil.waitForAll(futures).handle((result, exception) -> {
                                                    if (exception != null) {
                                                        Throwable t = FutureUtil.unwrapCompletionException(exception);
                                                        if (t instanceof NotFoundException) {
                                                            asyncResponse.resume(new RestException(Status.NOT_FOUND,
                                                                    getSubNotFoundErrorMessage(topicName.toString(),
                                                                            subName)));
                                                            return null;
                                                        } else {
                                                            if (t instanceof PulsarAdminException) {
                                                                log.warn("[{}] Failed to expire messages up "
                                                                        + "to {} on {}: {}", clientAppId(),
                                                                                expireTimeInSeconds, topicName,
                                                                                        t.toString());
                                                            } else {
                                                                log.error("[{}] Failed to expire messages up "
                                                                        + "to {} on {}", clientAppId(),
                                                                                expireTimeInSeconds, topicName, t);
                                                            }
                                                            resumeAsyncResponseExceptionally(asyncResponse, t);
                                                            return null;
                                                        }
                                                    }
                                                    asyncResponse.resume(Response.noContent().build());
                                                    return null;
                                                });
                                            });
                                        } else {
                                            return internalExpireMessagesByTimestampForSinglePartitionAsync
                                                    (partitionMetadata, subName, expireTimeInSeconds)
                                                    .thenAccept(unused ->
                                                            asyncResponse.resume(Response.noContent().build()));
                                        }
                                    }
                                }))

        ).exceptionally(ex -> {
            Throwable cause = FutureUtil.unwrapCompletionException(ex);
            // If the exception is not redirect exception we need to log it.
            if (isNot307And404Exception(cause)) {
                if (cause instanceof RestException) {
                    log.warn("[{}] Failed to expire messages up to {} on {}: {}", clientAppId(), expireTimeInSeconds,
                            topicName, cause.toString());
                } else {
                    log.error("[{}] Failed to expire messages up to {} on {}", clientAppId(), expireTimeInSeconds,
                            topicName, cause);
                }
            }
            resumeAsyncResponseExceptionally(asyncResponse, cause);
            return null;
        });
    }

    private CompletableFuture<Void> internalExpireMessagesByTimestampForSinglePartitionAsync(
            PartitionedTopicMetadata partitionMetadata, String subName, int expireTimeInSeconds) {
        if (!topicName.isPartitioned() && partitionMetadata.partitions > 0) {
            String msg = "This method should not be called for partitioned topic";
            return FutureUtil.failedFuture(new IllegalStateException(msg));
        } else {
            final CompletableFuture<Void> resultFuture = new CompletableFuture<>();
            getTopicReferenceAsync(topicName).thenAccept(t -> {
                 if (t == null) {
                     resultFuture.completeExceptionally(new RestException(Status.NOT_FOUND,
                             getTopicNotFoundErrorMessage(topicName.toString())));
                     return;
                 }
                if (!(t instanceof PersistentTopic)) {
                    resultFuture.completeExceptionally(new RestException(Status.METHOD_NOT_ALLOWED,
                            "Expire messages on a non-persistent topic is not allowed"));
                    return;
                }
                PersistentTopic topic = (PersistentTopic) t;

                boolean issued;
                if (subName.startsWith(topic.getReplicatorPrefix())) {
                    String remoteCluster = PersistentReplicator.getRemoteCluster(subName);
                    PersistentReplicator repl = (PersistentReplicator) topic
                            .getPersistentReplicator(remoteCluster);
                    if (repl == null) {
                        resultFuture.completeExceptionally(
                                new RestException(Status.NOT_FOUND, "Replicator not found"));
                        return;
                    }
                    issued = repl.expireMessages(expireTimeInSeconds);
                } else {
                    PersistentSubscription sub = topic.getSubscription(subName);
                    if (sub == null) {
                        resultFuture.completeExceptionally(
                                new RestException(Status.NOT_FOUND,
                                        getSubNotFoundErrorMessage(topicName.toString(), subName)));
                        return;
                    }
                    issued = sub.expireMessages(expireTimeInSeconds);
                }
                if (issued) {
                    log.info("[{}] Message expire started up to {} on {} {}", clientAppId(),
                            expireTimeInSeconds, topicName, subName);
                    resultFuture.complete(null);
                } else {
                    if (log.isDebugEnabled()) {
                        log.debug("Expire message by timestamp not issued on topic {} for subscription {} "
                                + "due to ongoing message expiration not finished or subscription almost"
                                + " catch up. If it's performed on a partitioned topic operation might "
                                + "succeeded on other partitions, please check stats of individual "
                                + "partition.", topicName, subName);
                    }
                    resultFuture.completeExceptionally(new RestException(Status.CONFLICT, "Expire message "
                            + "by timestamp not issued on topic " + topicName + " for subscription "
                            + subName + " due to ongoing message expiration not finished or subscription "
                            + "almost catch  up. If it's performed on a partitioned topic operation might"
                            + " succeeded on other partitions, please check stats of individual partition."
                    ));
                    return;
                }
            }).exceptionally(e -> {
                resultFuture.completeExceptionally(FutureUtil.unwrapCompletionException(e));
                return null;
            });
            return resultFuture;
        }
    }

    protected void internalExpireMessagesByPosition(AsyncResponse asyncResponse, String subName, boolean authoritative,
                                                 MessageIdImpl messageId, boolean isExcluded, int batchIndex) {
        if (messageId.getPartitionIndex() != topicName.getPartitionIndex()) {
            String msg = "Invalid parameter for expire message by position, partition index of "
                    + "passed in message position doesn't match partition index for the topic";
            log.warn("[{}] {} {}({}).", clientAppId(), msg, topicName, messageId);
            asyncResponse.resume(new RestException(Status.PRECONDITION_FAILED, msg));
            return;
        }
        CompletableFuture<Void> ret;
        // If the topic name is a partition name, no need to get partition topic metadata again
        if (!topicName.isPartitioned()) {
            ret = getPartitionedTopicMetadataAsync(topicName, authoritative, false)
                    .thenCompose(topicMetadata -> {
                        if (topicMetadata.partitions > 0) {
                            String msg = "Expire message at position is not supported for partitioned-topic";
                            log.warn("[{}] {} {}({}) {}", clientAppId(), msg, topicName, messageId, subName);
                            asyncResponse.resume(new RestException(Status.METHOD_NOT_ALLOWED, msg));
                        }
                        return CompletableFuture.completedFuture(null);
                    });
        } else {
            ret = CompletableFuture.completedFuture(null);
        }
        CompletableFuture<Void> future;
        if (topicName.isGlobal()) {
            future = ret.thenCompose(__ -> validateGlobalNamespaceOwnershipAsync(namespaceName));
        } else {
            future = ret;
        }

        future.thenCompose(__ -> validateTopicOwnershipAsync(topicName, authoritative))
                .thenCompose(__ -> validateTopicOperationAsync(topicName, TopicOperation.EXPIRE_MESSAGES, subName))
                .thenCompose(__ -> {
                    log.info("[{}][{}] Received expire messages on subscription {} to position {}", clientAppId(),
                            topicName, subName, messageId);
                    return internalExpireMessagesNonPartitionedTopicByPosition(asyncResponse, subName,
                            messageId, isExcluded, batchIndex);
                }).exceptionally(ex -> {
                    // If the exception is not redirect exception we need to log it.
                    if (isNot307And404Exception(ex)) {
                        log.error("[{}] Failed to expire messages up to {} on subscription {} to position {}",
                                clientAppId(), topicName, subName, messageId, ex);
                    }
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    private CompletableFuture<Void> internalExpireMessagesNonPartitionedTopicByPosition(AsyncResponse asyncResponse,
                                                                                        String subName,
                                                                                        MessageIdImpl messageId,
                                                                                        boolean isExcluded,
                                                                                        int batchIndex) {
        return getTopicReferenceAsync(topicName).thenAccept(t -> {
            PersistentTopic topic = (PersistentTopic) t;
            if (topic == null) {
                asyncResponse.resume(new RestException(Status.NOT_FOUND,
                        getTopicNotFoundErrorMessage(topicName.toString())));
                return;
            }
            try {
                PersistentSubscription sub = topic.getSubscription(subName);
                if (sub == null) {
                    asyncResponse.resume(new RestException(Status.NOT_FOUND,
                            getSubNotFoundErrorMessage(topicName.toString(), subName)));
                    return;
                }
                CompletableFuture<Integer> batchSizeFuture = new CompletableFuture<>();
                getEntryBatchSize(batchSizeFuture, topic, messageId, batchIndex);
                batchSizeFuture.thenAccept(bi -> {
                    PositionImpl position = calculatePositionAckSet(isExcluded, bi, batchIndex, messageId);
                    boolean issued;
                    try {
                        if (subName.startsWith(topic.getReplicatorPrefix())) {
                            String remoteCluster = PersistentReplicator.getRemoteCluster(subName);
                            PersistentReplicator repl = (PersistentReplicator)
                                    topic.getPersistentReplicator(remoteCluster);
                            if (repl == null) {
                                asyncResponse.resume(new RestException(Status.NOT_FOUND,
                                        "Replicator not found"));
                                return;
                            }
                            issued = repl.expireMessages(position);
                        } else {
                            issued = sub.expireMessages(position);
                        }
                        if (issued) {
                            log.info("[{}] Message expire started up to {} on {} {}", clientAppId(), position,
                                    topicName, subName);
                        } else {
                            if (log.isDebugEnabled()) {
                                log.debug("Expire message by position not issued on topic {} for subscription {} "
                                        + "due to ongoing message expiration not finished or subscription almost "
                                        + "catch up.", topicName, subName);
                            }
                            throw new RestException(Status.CONFLICT, "Expire message by position not issued on topic "
                                    + topicName + " for subscription " + subName + " due to ongoing"
                                    + " message expiration not finished or invalid message position provided.");
                        }
                    } catch (RestException exception) {
                        throw exception;
                    } catch (Exception exception) {
                        throw new RestException(exception);
                    }
                    asyncResponse.resume(Response.noContent().build());
                }).exceptionally(e -> {
                    Throwable throwable = FutureUtil.unwrapCompletionException(e);
                    if (throwable instanceof RestException) {
                        log.warn("[{}] Failed to expire messages up to {} on {} with subscription {}: {}",
                                clientAppId(), messageId, topicName, subName, throwable.toString());
                    } else {
                        log.error("[{}] Failed to expire messages up to {} on {} with subscription {}", clientAppId(),
                                messageId, topicName, subName, throwable);
                    }
                    resumeAsyncResponseExceptionally(asyncResponse, throwable);
                    return null;
                });
            } catch (Exception e) {
                log.warn("[{}][{}] Failed to expire messages up to {} on subscription {} to position {}",
                        clientAppId(), topicName, messageId, subName, messageId, e);
                resumeAsyncResponseExceptionally(asyncResponse, e);
            }
        }).exceptionally(ex -> {
            Throwable cause = FutureUtil.unwrapCompletionException(ex);
            if (cause instanceof RestException) {
                log.warn("[{}] Failed to expire messages up to {} on subscription {} to position {}: {}", clientAppId(),
                        topicName, subName, messageId, cause.toString());
            } else {
                log.error("[{}] Failed to expire messages up to {} on subscription {} to position {}", clientAppId(),
                        topicName, subName, messageId, cause);
            }
            resumeAsyncResponseExceptionally(asyncResponse, cause);
            return null;
        });
    }

    protected void internalTriggerCompaction(AsyncResponse asyncResponse, boolean authoritative) {
        log.info("[{}] Trigger compaction on topic {}", clientAppId(), topicName);
        CompletableFuture<Void> future = validateTopicOperationAsync(topicName, TopicOperation.COMPACT);
        future.thenCompose(__ -> {
            if (topicName.isGlobal()) {
                return validateGlobalNamespaceOwnershipAsync(namespaceName);
            } else {
                return CompletableFuture.completedFuture(null);
            }
        }).thenAccept(__ -> {
            // If the topic name is a partition name, no need to get partition topic metadata again
            if (topicName.isPartitioned()) {
                internalTriggerCompactionNonPartitionedTopic(asyncResponse, authoritative);
            } else {
                getPartitionedTopicMetadataAsync(topicName, authoritative, false)
                        .thenAccept(partitionMetadata -> {
                    final int numPartitions = partitionMetadata.partitions;
                    if (numPartitions > 0) {
                        final List<CompletableFuture<Void>> futures = new ArrayList<>(numPartitions);

                        for (int i = 0; i < numPartitions; i++) {
                            TopicName topicNamePartition = topicName.getPartition(i);
                            try {
                                futures.add(pulsar()
                                        .getAdminClient()
                                        .topics()
                                        .triggerCompactionAsync(topicNamePartition.toString()));
                            } catch (Exception e) {
                                log.error("[{}] Failed to trigger compaction on topic {}",
                                        clientAppId(), topicNamePartition, e);
                                asyncResponse.resume(new RestException(e));
                                return;
                            }
                        }

                        FutureUtil.waitForAll(futures).handle((result, exception) -> {
                            if (exception != null) {
                                Throwable th = exception.getCause();
                                if (th instanceof NotFoundException) {
                                    asyncResponse.resume(new RestException(Status.NOT_FOUND, th.getMessage()));
                                    return null;
                                } else if (th instanceof WebApplicationException) {
                                    asyncResponse.resume(th);
                                    return null;
                                } else {
                                    log.error("[{}] Failed to trigger compaction on topic {}",
                                            clientAppId(), topicName, exception);
                                    asyncResponse.resume(new RestException(exception));
                                    return null;
                                }
                            }
                            asyncResponse.resume(Response.noContent().build());
                            return null;
                        });
                    } else {
                        internalTriggerCompactionNonPartitionedTopic(asyncResponse, authoritative);
                    }
                }).exceptionally(ex -> {
                    // If the exception is not redirect exception we need to log it.
                    if (isNot307And404Exception(ex)) {
                        log.error("[{}] Failed to trigger compaction on topic {}", clientAppId(), topicName, ex);
                    }
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
            }
        }).exceptionally(ex -> {
            // If the exception is not redirect exception we need to log it.
            if (isNot307And404Exception(ex)) {
                log.error("[{}] Failed to validate global namespace ownership to trigger compaction on topic {}",
                        clientAppId(), topicName, ex);
            }
            resumeAsyncResponseExceptionally(asyncResponse, ex);
            return null;
        });
    }

    protected void internalTriggerCompactionNonPartitionedTopic(AsyncResponse asyncResponse, boolean authoritative) {
        validateTopicOwnershipAsync(topicName, authoritative)
                .thenCompose(__ -> validateTopicOperationAsync(topicName, TopicOperation.COMPACT))
                .thenCompose(__ -> getTopicReferenceAsync(topicName))
                .thenAccept(topic -> {
                    try {
                        ((PersistentTopic) topic).triggerCompaction();
                        asyncResponse.resume(Response.noContent().build());
                    } catch (AlreadyRunningException e) {
                        resumeAsyncResponseExceptionally(asyncResponse,
                                new RestException(Status.CONFLICT, e.getMessage()));
                        return;
                    } catch (Exception e) {
                        log.error("[{}] Failed to trigger compaction on topic {}", clientAppId(),
                                topicName, e);
                        resumeAsyncResponseExceptionally(asyncResponse, new RestException(e));
                        return;
                    }
                }).exceptionally(ex -> {
                    // If the exception is not redirect exception we need to log it.
                    if (isNot307And404Exception(ex)) {
                        log.error("[{}] Failed to trigger compaction for {}", clientAppId(), topicName, ex);
                    }
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                }
        );
    }

    protected CompletableFuture<LongRunningProcessStatus> internalCompactionStatusAsync(boolean authoritative) {
        return validateTopicOwnershipAsync(topicName, authoritative)
                .thenCompose(__ -> validateTopicOperationAsync(topicName, TopicOperation.COMPACT))
                .thenCompose(__ -> getTopicReferenceAsync(topicName))
                .thenApply(topic -> ((PersistentTopic) topic).compactionStatus());
    }

    protected void internalTriggerOffload(AsyncResponse asyncResponse,
                                          boolean authoritative, MessageIdImpl messageId) {
        validateTopicOwnershipAsync(topicName, authoritative)
                .thenCompose(__ -> validateTopicOperationAsync(topicName, TopicOperation.OFFLOAD))
                .thenCompose(__ -> getTopicReferenceAsync(topicName))
                .thenAccept(topic -> {
                    try {
                        ((PersistentTopic) topic).triggerOffload(messageId);
                        asyncResponse.resume(Response.noContent().build());
                    } catch (AlreadyRunningException e) {
                        resumeAsyncResponseExceptionally(asyncResponse,
                                new RestException(Status.CONFLICT, e.getMessage()));
                        return;
                    } catch (Exception e) {
                        log.warn("Unexpected error triggering offload", e);
                        resumeAsyncResponseExceptionally(asyncResponse, new RestException(e));
                        return;
                    }
                }).exceptionally(ex -> {
                    // If the exception is not redirect exception we need to log it.
                    if (isNot307And404Exception(ex)) {
                        log.error("[{}] Failed to trigger offload for {}", clientAppId(), topicName, ex);
                    }
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    protected void internalOffloadStatus(AsyncResponse asyncResponse, boolean authoritative) {
        validateTopicOwnershipAsync(topicName, authoritative)
                .thenCompose(__ -> validateTopicOperationAsync(topicName, TopicOperation.OFFLOAD))
                .thenCompose(__ -> getTopicReferenceAsync(topicName))
                .thenAccept(topic -> {
                    OffloadProcessStatus offloadProcessStatus = ((PersistentTopic) topic).offloadStatus();
                    asyncResponse.resume(offloadProcessStatus);
                }).exceptionally(ex -> {
                    // If the exception is not redirect exception we need to log it.
                    if (isNot307And404Exception(ex)) {
                        log.error("[{}] Failed to offload status on topic {}", clientAppId(), topicName, ex);
                    }
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    public CompletableFuture<PartitionedTopicMetadata> getPartitionedTopicMetadata(
            PulsarService pulsar, String clientAppId, String originalPrincipal,
            AuthenticationDataSource authenticationData, TopicName topicName) {
        CompletableFuture<PartitionedTopicMetadata> metadataFuture = new CompletableFuture<>();
        CompletableFuture<Void> authorizationFuture = new CompletableFuture<>();
        checkAuthorizationAsync(pulsar, topicName, clientAppId, authenticationData)
                .thenRun(() -> authorizationFuture.complete(null))
                .exceptionally(e -> {
                    Throwable throwable = FutureUtil.unwrapCompletionException(e);
                    if (throwable instanceof RestException) {
                        validateAdminAccessForTenantAsync(pulsar,
                                clientAppId, originalPrincipal, topicName.getTenant(), authenticationData)
                                .thenRun(() -> {
                                    authorizationFuture.complete(null);
                                }).exceptionally(ex -> {
                                    Throwable throwable2 = FutureUtil.unwrapCompletionException(ex);
                                    if (throwable2 instanceof RestException) {
                                        log.warn("Failed to authorize {} on topic {}", clientAppId, topicName);
                                        authorizationFuture.completeExceptionally(new PulsarClientException(
                                                String.format("Authorization failed %s on topic %s with error %s",
                                                clientAppId, topicName, throwable2.getMessage())));
                                    } else {
                                        authorizationFuture.completeExceptionally(throwable2);
                                    }
                                    return null;
                                });
                    } else {
                        // throw without wrapping to PulsarClientException that considers: unknown error marked as
                        // internal server error
                        log.warn("Failed to authorize {} on topic {}", clientAppId, topicName, throwable);
                        authorizationFuture.completeExceptionally(throwable);
                    }
                    return null;
                });

        // validates global-namespace contains local/peer cluster: if peer/local cluster present then lookup can
        // serve/redirect request else fail partitioned-metadata-request so, client fails while creating
        // producer/consumer
        // It is necessary for system topic operations because system topics are used to store metadata
        // and other vital information. Even after namespace starting deletion,,
        // we need to access the metadata of system topics to create readers and clean up topic data.
        // If we don't do this, it can prevent namespace deletion due to inaccessible readers.
        authorizationFuture.thenCompose(__ ->
                        checkLocalOrGetPeerReplicationCluster(pulsar, topicName.getNamespaceObject(),
                                SystemTopicNames.isSystemTopic(topicName)))
                .thenCompose(res ->
                        pulsar.getBrokerService().fetchPartitionedTopicMetadataCheckAllowAutoCreationAsync(topicName))
                .thenAccept(metadata -> {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] Total number of partitions for topic {} is {}", clientAppId, topicName,
                                metadata.partitions);
                    }
                    metadataFuture.complete(metadata);
                })
                .exceptionally(e -> {
                    metadataFuture.completeExceptionally(FutureUtil.unwrapCompletionException(e));
                    return null;
                });
        return metadataFuture;
    }

    /**
     * Get partitioned topic metadata without checking the permission.
     */
    public static CompletableFuture<PartitionedTopicMetadata> unsafeGetPartitionedTopicMetadataAsync(
        PulsarService pulsar, TopicName topicName) {
        CompletableFuture<PartitionedTopicMetadata> metadataFuture = new CompletableFuture();

        // validates global-namespace contains local/peer cluster: if peer/local cluster present then lookup can
        // serve/redirect request else fail partitioned-metadata-request so, client fails while creating
        // producer/consumer
        // It is necessary for system topic operations because system topics are used to store metadata
        // and other vital information. Even after namespace starting deletion,,
        // we need to access the metadata of system topics to create readers and clean up topic data.
        // If we don't do this, it can prevent namespace deletion due to inaccessible readers.
        checkLocalOrGetPeerReplicationCluster(pulsar, topicName.getNamespaceObject(), isSystemTopic(topicName))
            .thenCompose(res -> pulsar.getBrokerService()
                .fetchPartitionedTopicMetadataCheckAllowAutoCreationAsync(topicName))
            .thenAccept(metadata -> {
                if (log.isDebugEnabled()) {
                    log.debug("Total number of partitions for topic {} is {}", topicName,
                        metadata.partitions);
                }
                metadataFuture.complete(metadata);
            }).exceptionally(ex -> {
            metadataFuture.completeExceptionally(ex.getCause());
            return null;
        });

        return metadataFuture;
    }

    private CompletableFuture<Topic> getTopicReferenceAsync(TopicName topicName) {
        return pulsar().getBrokerService().getTopicIfExists(topicName.toString())
                .thenCompose(optTopic -> optTopic
                        .map(CompletableFuture::completedFuture)
                        .orElseGet(() -> topicNotFoundReasonAsync(topicName)));
    }

    private CompletableFuture<Topic> topicNotFoundReasonAsync(TopicName topicName) {
        if (!topicName.isPartitioned()) {
            return FutureUtil.failedFuture(new RestException(Status.NOT_FOUND,
                    getTopicNotFoundErrorMessage(topicName.toString())));
        }

        return getPartitionedTopicMetadataAsync(
                TopicName.get(topicName.getPartitionedTopicName()), false, false)
                .thenAccept(partitionedTopicMetadata -> {
                    if (partitionedTopicMetadata == null || partitionedTopicMetadata.partitions == 0) {
                        final String topicErrorType = partitionedTopicMetadata
                                == null ? "has no metadata" : "has zero partitions";
                        throw new RestException(Status.NOT_FOUND, String.format(
                                "Partitioned Topic not found: %s %s", topicName.toString(), topicErrorType));
                    }
                })
                .thenCompose(__ -> internalGetListAsync(Optional.empty()))
                .thenApply(topics -> {
                    if (!topics.contains(topicName.toString())) {
                        throw new RestException(Status.NOT_FOUND, "Topic partitions were not yet created");
                    }
                    throw new RestException(Status.NOT_FOUND,
                        getPartitionedTopicNotFoundErrorMessage(topicName.toString()));
            });
    }

    /**
     * Get the Subscription object reference from the Topic reference.
     */
    private Subscription getSubscriptionReference(String subName, PersistentTopic topic) {
        try {
            Subscription sub = topic.getSubscription(subName);
            if (sub == null) {
                sub = topic.createSubscription(subName,
                        InitialPosition.Earliest, false, null).get();
            }

            return checkNotNull(sub);
        } catch (Exception e) {
            throw new RestException(Status.NOT_FOUND, getSubNotFoundErrorMessage(topicName.toString(), subName));
        }
    }

    /**
     * Get the Replicator object reference from the Topic reference.
     */
    private PersistentReplicator getReplicatorReference(String replName, PersistentTopic topic) {
        try {
            String remoteCluster = PersistentReplicator.getRemoteCluster(replName);
            PersistentReplicator repl = (PersistentReplicator) topic.getPersistentReplicator(remoteCluster);
            return checkNotNull(repl);
        } catch (Exception e) {
            throw new RestException(Status.NOT_FOUND, "Replicator not found");
        }
    }

    // as described at : (PR: #836) CPP-client old client lib should not be allowed to connect on partitioned-topic.
    // So, all requests from old-cpp-client (< v1.21) must be rejected.
    // Pulsar client-java lib always passes user-agent as X-Java-$version.
    // However, cpp-client older than v1.20 (PR #765) never used to pass it.
    // So, request without user-agent and Pulsar-CPP-vX (X < 1.21) must be rejected
    protected CompletableFuture<Void> internalValidateClientVersionAsync() {
        if (!pulsar().getConfiguration().isClientLibraryVersionCheckEnabled()) {
            return CompletableFuture.completedFuture(null);
        }
        final String userAgent = httpRequest.getHeader("User-Agent");
        if (StringUtils.isBlank(userAgent)) {
            return FutureUtil.failedFuture(new RestException(Status.METHOD_NOT_ALLOWED,
                    "Client lib is not compatible to"
                            + " access partitioned metadata: version in user-agent is not present"));
        }
        // Version < 1.20 for cpp-client is not allowed
        if (userAgent.contains(DEPRECATED_CLIENT_VERSION_PREFIX)) {
            try {
                // Version < 1.20 for cpp-client is not allowed
                String[] tokens = userAgent.split(DEPRECATED_CLIENT_VERSION_PREFIX);
                String[] splits = tokens.length > 1 ? tokens[1].split("-")[0].trim().split("\\.") : null;
                if (splits != null && splits.length > 1) {
                    if (LEAST_SUPPORTED_CLIENT_VERSION_PREFIX.getMajorVersion() > Integer.parseInt(splits[0])
                            || LEAST_SUPPORTED_CLIENT_VERSION_PREFIX.getMinorVersion() > Integer.parseInt(splits[1])) {
                        return FutureUtil.failedFuture(new RestException(Status.METHOD_NOT_ALLOWED,
                                "Client lib is not compatible to access partitioned metadata: version " + userAgent
                                        + " is not supported"));
                    }
                }
            } catch (Exception e) {
                log.warn("[{}] Failed to parse version {} ", clientAppId(), userAgent);
            }
        }
        return CompletableFuture.completedFuture(null);
    }

    /**
     * Validate update of number of partition for partitioned topic.
     * If there's already non partition topic with same name and contains partition suffix "-partition-"
     * followed by numeric value X then the new number of partition of that partitioned topic can not be greater
     * than that X else that non partition topic will essentially be overwritten and cause unexpected consequence.
     *
     * @param topicName
     */
    private CompletableFuture<Void> validatePartitionTopicUpdateAsync(String topicName, int numberOfPartition) {
        return internalGetListAsync().thenCompose(existingTopicList -> {
            TopicName partitionTopicName = TopicName.get(domain(), namespaceName, topicName);
            String prefix = partitionTopicName.getPartitionedTopicName() + PARTITIONED_TOPIC_SUFFIX;
            return getPartitionedTopicMetadataAsync(partitionTopicName, false, false)
                    .thenAccept(metadata -> {
                        int oldPartition = metadata.partitions;
                        for (String existingTopicName : existingTopicList) {
                            if (existingTopicName.startsWith(prefix)) {
                                try {
                                    long suffix = Long.parseLong(existingTopicName.substring(
                                            existingTopicName.indexOf(PARTITIONED_TOPIC_SUFFIX)
                                                    + PARTITIONED_TOPIC_SUFFIX.length()));
                                    // Skip partition of partitioned topic by making sure
                                    // the numeric suffix greater than old partition number.
                                    if (suffix >= oldPartition && suffix <= (long) numberOfPartition) {
                                        log.warn(
                                                "[{}] Already have non partition topic {} which contains partition"
                                                        + " suffix '-partition-' and end with numeric value smaller"
                                                        + " than the new number of partition. Update of partitioned"
                                                        + " topic {} could cause conflict.",
                                                clientAppId(),
                                                existingTopicName, topicName);
                                        throw new RestException(Status.PRECONDITION_FAILED,
                                                "Already have non partition topic " + existingTopicName
                                                        + " which contains partition suffix '-partition-' "
                                                        + "and end with numeric value and end with numeric value"
                                                        + " smaller than the new number of partition. Update of"
                                                        + " partitioned topic " + topicName + " could cause conflict.");
                                    }
                                } catch (NumberFormatException e) {
                                    // Do nothing, if value after partition suffix is not pure numeric value,
                                    // as it can't conflict with internal created partitioned topic's name.
                                }
                            }
                        }
                    });
        });
    }

    /**
     * Validate non partition topic name,
     * Validation will fail and throw RestException if
     * 1) Topic name contains partition suffix "-partition-" and the remaining part follow the partition
     * suffix is numeric value larger than the number of partition if there's already a partition topic with same
     * name(the part before suffix "-partition-").
     * 2)Topic name contains partition suffix "-partition-" and the remaining part follow the partition
     * suffix is numeric value but there isn't a partitioned topic with same name.
     *
     * @param topicName
     */
    private CompletableFuture<Void> validateNonPartitionTopicNameAsync(String topicName) {
        CompletableFuture<Void> ret = CompletableFuture.completedFuture(null);
        if (topicName.contains(PARTITIONED_TOPIC_SUFFIX)) {
            try {
                // First check if what's after suffix "-partition-" is number or not, if not number then can create.
                int partitionIndex = topicName.indexOf(PARTITIONED_TOPIC_SUFFIX);
                long suffix = Long.parseLong(topicName.substring(partitionIndex
                        + PARTITIONED_TOPIC_SUFFIX.length()));
                TopicName partitionTopicName = TopicName.get(domain(),
                        namespaceName, topicName.substring(0, partitionIndex));
                ret = getPartitionedTopicMetadataAsync(partitionTopicName, false, false)
                        .thenAccept(metadata -> {
                            // Partition topic index is 0 to (number of partition - 1)
                            if (metadata.partitions > 0 && suffix >= (long) metadata.partitions) {
                                log.warn("[{}] Can't create topic {} with \"-partition-\" followed by"
                                                + " a number smaller then number of partition of partitioned topic {}.",
                                        clientAppId(), topicName, partitionTopicName.getLocalName());
                                throw new RestException(Status.PRECONDITION_FAILED,
                                        "Can't create topic " + topicName + " with \"-partition-\" followed by"
                                                + " a number smaller then number of partition of partitioned topic "
                                                + partitionTopicName.getLocalName());
                            } else if (metadata.partitions == 0) {
                                log.warn("[{}] Can't create topic {} with \"-partition-\" followed by"
                                                + " numeric value if there isn't a partitioned topic {} created.",
                                        clientAppId(), topicName, partitionTopicName.getLocalName());
                                throw new RestException(Status.PRECONDITION_FAILED,
                                        "Can't create topic " + topicName + " with \"-partition-\" followed by"
                                                + " numeric value if there isn't a partitioned topic "
                                                + partitionTopicName.getLocalName() + " created.");
                            }
                            // If there is a  partitioned topic with the same name and numeric suffix is smaller
                            // than the number of partition for that partitioned topic, validation will pass.
                        });
            } catch (NumberFormatException e) {
                // Do nothing, if value after partition suffix is not pure numeric value,
                // as it can't conflict if user want to create partitioned topic with same
                // topic name prefix in the future.
            }
        }
        return ret;
    }

    protected void internalGetLastMessageId(AsyncResponse asyncResponse, boolean authoritative) {
        validateTopicOwnershipAsync(topicName, authoritative)
                .thenCompose(__ -> validateTopicOperationAsync(topicName, TopicOperation.PEEK_MESSAGES))
                .thenCompose(__ -> getTopicReferenceAsync(topicName))
                .thenAccept(topic -> {
                    if (topic == null) {
                        asyncResponse.resume(new RestException(Status.NOT_FOUND,
                                getTopicNotFoundErrorMessage(topicName.toString())));
                        return;
                    }
                    if (!(topic instanceof PersistentTopic)) {
                        log.error("[{}] Not supported operation of non-persistent topic {}", clientAppId(), topicName);
                        asyncResponse.resume(new RestException(Status.METHOD_NOT_ALLOWED,
                                "GetLastMessageId on a non-persistent topic is not allowed"));
                        return;
                    }
                    topic.getLastMessageId().whenComplete((v, e) -> {
                        if (e != null) {
                            asyncResponse.resume(new RestException(Status.INTERNAL_SERVER_ERROR, e.getMessage()));
                        } else {
                            asyncResponse.resume(v);
                        }
                    });
                }).exceptionally(ex -> {
                    // If the exception is not redirect exception we need to log it.
                    if (isNot307And404Exception(ex)) {
                        log.error("[{}] Failed to get last messageId {}", clientAppId(), topicName, ex);
                    }
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
        });
    }
    protected CompletableFuture<Void> internalTrimTopic(AsyncResponse asyncResponse, boolean authoritative) {
        if (!topicName.isPersistent()) {
            log.info("[{}] Trim on a non-persistent topic {} is not allowed", clientAppId(), topicName);
            asyncResponse.resume(new RestException(Status.METHOD_NOT_ALLOWED,
                    "Trim on a non-persistent topic is not allowed"));
            return null;
        }
        CompletableFuture<Void> future = validateTopicOperationAsync(topicName, TopicOperation.TRIM_TOPIC);
        if (topicName.isPartitioned()) {
            return future.thenCompose((x)
                    -> trimNonPartitionedTopic(asyncResponse, topicName, authoritative));
        }
        return future
                .thenCompose(__ -> pulsar().getBrokerService().fetchPartitionedTopicMetadataAsync(topicName))
                .thenCompose(metadata -> {
                    if (metadata.partitions > 0) {
                        return trimPartitionedTopic(asyncResponse, metadata);
                    }
                    return trimNonPartitionedTopic(asyncResponse, topicName, authoritative);
                });
    }

    private CompletableFuture<Void> trimNonPartitionedTopic(AsyncResponse asyncResponse,
                                                            TopicName topicName, boolean authoritative) {
        return  validateTopicOwnershipAsync(topicName, authoritative)
                .thenCompose(__ -> getTopicReferenceAsync(topicName))
                .thenCompose(topic -> {
                    if (!(topic instanceof PersistentTopic persistentTopic)) {
                        log.info("[{}] Trim on a non-persistent topic {} is not allowed", clientAppId(), topicName);
                        asyncResponse.resume(new RestException(Status.METHOD_NOT_ALLOWED,
                                "Trim on a non-persistent topic is not allowed"));
                        return CompletableFuture.completedFuture(null);
                    }
                    ManagedLedger managedLedger = persistentTopic.getManagedLedger();
                    if (managedLedger == null) {
                        asyncResponse.resume(null);
                        return CompletableFuture.completedFuture(null);
                    }
                    CompletableFuture<List<LedgerInfo>> result = managedLedger.asyncTrimConsumedLedgers();
                    return result
                            .thenRun(() -> {
                            })
                            .whenComplete((res, e) -> {
                                if (e != null) {
                                    asyncResponse.resume(e);
                                } else {
                                    asyncResponse.resume(Response.noContent().build());
                                }
                            });
                });
    }

    private CompletableFuture<Void> trimPartitionedTopic(AsyncResponse asyncResponse,
                                                         PartitionedTopicMetadata metadata) {
        List<CompletableFuture<Void>> futures = new ArrayList<>(metadata.partitions);
        for (int i = 0; i < metadata.partitions; i++) {
            TopicName topicNamePartition = topicName.getPartition(i);
            try {
                futures.add(pulsar().getAdminClient().topics().trimTopicAsync(topicNamePartition.toString()));
            } catch (Exception e) {
                log.error("[{}] Failed to trim topic {}", clientAppId(), topicNamePartition, e);
                throw new RestException(e);
            }
        }
        return FutureUtil.waitForAll(futures).thenAccept(asyncResponse::resume);
    }

    protected CompletableFuture<DispatchRateImpl> internalGetDispatchRate(boolean applied, boolean isGlobal) {
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
            .thenApply(op -> op.map(TopicPolicies::getDispatchRate)
                .orElseGet(() -> {
                    if (applied) {
                        DispatchRateImpl namespacePolicy = getNamespacePolicies(namespaceName)
                                .topicDispatchRate.get(pulsar().getConfiguration().getClusterName());
                        return namespacePolicy == null ? dispatchRate() : namespacePolicy;
                    }
                    return null;
                }));
    }

    protected CompletableFuture<Void> internalSetDispatchRate(DispatchRateImpl dispatchRate, boolean isGlobal) {
        if (dispatchRate == null) {
            return CompletableFuture.completedFuture(null);
        }
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
            .thenCompose(op -> {
                TopicPolicies topicPolicies = op.orElseGet(TopicPolicies::new);
                topicPolicies.setDispatchRate(dispatchRate);
                topicPolicies.setIsGlobal(isGlobal);
                return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, topicPolicies);
            });
    }

    protected CompletableFuture<Void> internalRemoveDispatchRate(boolean isGlobal) {
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
            .thenCompose(op -> {
                if (!op.isPresent()) {
                    return CompletableFuture.completedFuture(null);
                }
                TopicPolicies topicPolicies = op.get();
                topicPolicies.setDispatchRate(null);
                topicPolicies.setIsGlobal(isGlobal);
                return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, op.get());
            });
    }

    protected CompletableFuture<DispatchRate> internalGetSubscriptionDispatchRate(boolean applied, boolean isGlobal) {
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
            .thenApply(op -> op.map(TopicPolicies::getSubscriptionDispatchRate)
                .orElseGet(() -> {
                    if (applied) {
                        DispatchRateImpl namespacePolicy = getNamespacePolicies(namespaceName)
                                .subscriptionDispatchRate.get(pulsar().getConfiguration().getClusterName());
                        return namespacePolicy == null ? subscriptionDispatchRate() : namespacePolicy;
                    }
                    return null;
                }));
    }

    protected CompletableFuture<Void> internalSetSubscriptionDispatchRate
            (DispatchRateImpl dispatchRate, boolean isGlobal) {
        if (dispatchRate == null) {
            return CompletableFuture.completedFuture(null);
        }
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
            .thenCompose(op -> {
                TopicPolicies topicPolicies = op.orElseGet(TopicPolicies::new);
                topicPolicies.setSubscriptionDispatchRate(dispatchRate);
                topicPolicies.setIsGlobal(isGlobal);
                return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, topicPolicies);
            });
    }

    protected CompletableFuture<Void> internalRemoveSubscriptionDispatchRate(boolean isGlobal) {
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
            .thenCompose(op -> {
                if (!op.isPresent()) {
                    return CompletableFuture.completedFuture(null);
                }
                TopicPolicies topicPolicies = op.get();
                topicPolicies.setSubscriptionDispatchRate(null);
                topicPolicies.setIsGlobal(isGlobal);
                return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, op.get());
            });
    }

    protected CompletableFuture<DispatchRate> internalGetSubscriptionLevelDispatchRate(String subName, boolean applied,
                                                                                       boolean isGlobal) {
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
                .thenCompose(otp -> {
                    DispatchRateImpl rate = otp.map(tp -> tp.getSubscriptionPolicies().get(subName))
                            .map(SubscriptionPolicies::getDispatchRate)
                            .orElse(null);
                    if (applied && rate == null) {
                        return internalGetSubscriptionDispatchRate(true, isGlobal);
                    } else {
                        return CompletableFuture.completedFuture(rate);
                    }
                });
    }

    protected CompletableFuture<Void> internalSetSubscriptionLevelDispatchRate(String subName,
                                                                               DispatchRateImpl dispatchRate,
                                                                               boolean isGlobal) {
        final DispatchRateImpl newDispatchRate = DispatchRateImpl.normalize(dispatchRate);
        if (newDispatchRate == null) {
            return CompletableFuture.completedFuture(null);
        }
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
                .thenCompose(op -> {
                    TopicPolicies topicPolicies = op.orElseGet(TopicPolicies::new);
                    topicPolicies.setIsGlobal(isGlobal);
                    topicPolicies.getSubscriptionPolicies()
                            .computeIfAbsent(subName, k -> new SubscriptionPolicies())
                            .setDispatchRate(newDispatchRate);
                    return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, topicPolicies);
                });
    }

    protected CompletableFuture<Void> internalRemoveSubscriptionLevelDispatchRate(String subName, boolean isGlobal) {
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
            .thenCompose(op -> {
                if (!op.isPresent()) {
                    return CompletableFuture.completedFuture(null);
                }
                TopicPolicies topicPolicies = op.get();
                SubscriptionPolicies sp = topicPolicies.getSubscriptionPolicies().get(subName);
                if (sp == null) {
                    return CompletableFuture.completedFuture(null);
                }
                sp.setDispatchRate(null);
                if (sp.checkEmpty()) {
                    // cleanup empty SubscriptionPolicies
                    topicPolicies.getSubscriptionPolicies().remove(subName, sp);
                }
                topicPolicies.setIsGlobal(isGlobal);
                return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, op.get());
            });
    }


    protected CompletableFuture<Optional<Integer>> internalGetMaxConsumersPerSubscription(boolean isGlobal) {
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
                .thenApply(op -> op.map(TopicPolicies::getMaxConsumersPerSubscription));
    }

    protected CompletableFuture<Void> internalSetMaxConsumersPerSubscription(
            Integer maxConsumersPerSubscription, boolean isGlobal) {
        if (maxConsumersPerSubscription != null && maxConsumersPerSubscription < 0) {
            throw new RestException(Status.PRECONDITION_FAILED, "Invalid value for maxConsumersPerSubscription");
        }
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
            .thenCompose(op -> {
                TopicPolicies topicPolicies = op.orElseGet(TopicPolicies::new);
                topicPolicies.setMaxConsumersPerSubscription(maxConsumersPerSubscription);
                topicPolicies.setIsGlobal(isGlobal);
                return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, topicPolicies);
            });
    }

    protected CompletableFuture<Void> internalRemoveMaxConsumersPerSubscription(boolean isGlobal) {
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
            .thenCompose(op -> {
                if (!op.isPresent()) {
                    return CompletableFuture.completedFuture(null);
                }
                op.get().setMaxConsumersPerSubscription(null);
                op.get().setIsGlobal(isGlobal);
                return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, op.get());
            });
    }

    protected CompletableFuture<Long> internalGetCompactionThreshold(boolean applied, boolean isGlobal) {
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
            .thenApply(op -> op.map(TopicPolicies::getCompactionThreshold)
                .orElseGet(() -> {
                    if (applied) {
                        Long namespacePolicy = getNamespacePolicies(namespaceName).compaction_threshold;
                        return namespacePolicy == null
                                ? pulsar().getConfiguration().getBrokerServiceCompactionThresholdInBytes()
                                : namespacePolicy;
                    }
                    return null;
                }));
    }

    protected CompletableFuture<Void> internalSetCompactionThreshold(Long compactionThreshold, boolean isGlobal) {
        if (compactionThreshold != null && compactionThreshold < 0) {
            throw new RestException(Status.PRECONDITION_FAILED, "Invalid value for compactionThreshold");
        }

        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
            .thenCompose(op -> {
                TopicPolicies topicPolicies = op.orElseGet(TopicPolicies::new);
                topicPolicies.setCompactionThreshold(compactionThreshold);
                topicPolicies.setIsGlobal(isGlobal);
                return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, topicPolicies);
            });

    }

    protected CompletableFuture<Void> internalRemoveCompactionThreshold(boolean isGlobal) {
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
            .thenCompose(op -> {
                if (!op.isPresent()) {
                    return CompletableFuture.completedFuture(null);
                }
                TopicPolicies topicPolicies = op.get();
                topicPolicies.setCompactionThreshold(null);
                topicPolicies.setIsGlobal(isGlobal);
                return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, op.get());
            });
    }

    protected CompletableFuture<Optional<PublishRate>> internalGetPublishRate(boolean isGlobal) {
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
            .thenApply(op -> op.map(TopicPolicies::getPublishRate));
    }

    protected CompletableFuture<Void> internalSetPublishRate(PublishRate publishRate, boolean isGlobal) {
        if (publishRate == null) {
            return CompletableFuture.completedFuture(null);
        }
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
            .thenCompose(op -> {
                TopicPolicies topicPolicies = op.orElseGet(TopicPolicies::new);
                topicPolicies.setPublishRate(publishRate);
                topicPolicies.setIsGlobal(isGlobal);
                return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, topicPolicies);
            });
    }

    protected CompletableFuture<Optional<List<SubType>>> internalGetSubscriptionTypesEnabled(boolean isGlobal) {
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
            .thenApply(op -> op.map(TopicPolicies::getSubscriptionTypesEnabled));
    }

    protected CompletableFuture<Void> internalSetSubscriptionTypesEnabled(
            Set<SubscriptionType> subscriptionTypesEnabled, boolean isGlobal) {
        List<SubType> subTypes = new ArrayList<>();
        subscriptionTypesEnabled.forEach(subscriptionType -> subTypes.add(SubType.valueOf(subscriptionType.name())));
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
            .thenCompose(op -> {
                TopicPolicies topicPolicies = op.orElseGet(TopicPolicies::new);
                topicPolicies.setSubscriptionTypesEnabled(subTypes);
                topicPolicies.setIsGlobal(isGlobal);
                return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, topicPolicies);
            });
    }

    protected CompletableFuture<Void> internalRemoveSubscriptionTypesEnabled(boolean isGlobal) {
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
                .thenCompose(op -> {
                    if (!op.isPresent()) {
                        return CompletableFuture.completedFuture(null);
                    }
                    op.get().setSubscriptionTypesEnabled(new ArrayList<>());
                    op.get().setIsGlobal(isGlobal);
                    return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, op.get());
                });
    }

    protected CompletableFuture<Void> internalRemovePublishRate(boolean isGlobal) {
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
            .thenCompose(op -> {
                if (!op.isPresent()) {
                    return CompletableFuture.completedFuture(null);
                }
                op.get().setPublishRate(null);
                op.get().setIsGlobal(isGlobal);
                return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, op.get());
            });
    }

    protected CompletableFuture<SubscribeRate> internalGetSubscribeRate(boolean applied, boolean isGlobal) {
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
            .thenApply(op -> op.map(TopicPolicies::getSubscribeRate)
                .orElseGet(() -> {
                    if (applied) {
                        SubscribeRate namespacePolicy = getNamespacePolicies(namespaceName)
                                .clusterSubscribeRate.get(pulsar().getConfiguration().getClusterName());
                        return namespacePolicy == null ? subscribeRate() : namespacePolicy;
                    }
                    return null;
                }));
    }

    protected CompletableFuture<Void> internalSetSubscribeRate(SubscribeRate subscribeRate, boolean isGlobal) {
        if (subscribeRate == null) {
            return CompletableFuture.completedFuture(null);
        }
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
            .thenCompose(op -> {
                TopicPolicies topicPolicies = op.orElseGet(TopicPolicies::new);
                topicPolicies.setSubscribeRate(subscribeRate);
                topicPolicies.setIsGlobal(isGlobal);
                return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, topicPolicies);
            });
    }

    protected CompletableFuture<Void> internalRemoveSubscribeRate(boolean isGlobal) {
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
            .thenCompose(op -> {
                if (!op.isPresent()) {
                    return CompletableFuture.completedFuture(null);
                }
                op.get().setSubscribeRate(null);
                op.get().setIsGlobal(isGlobal);
                return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, op.get());
            });
    }

    protected void handleTopicPolicyException(String methodName, Throwable thr, AsyncResponse asyncResponse) {
        Throwable cause = thr.getCause();
        if (isNot307And4xxException(cause)) {
            log.error("[{}] Failed to perform {} on topic {}",
                    clientAppId(), methodName, topicName, cause);
        }
        resumeAsyncResponseExceptionally(asyncResponse, cause);
    }

    protected CompletableFuture<Void> internalTruncateNonPartitionedTopicAsync(boolean authoritative) {
        return validateAdminAccessForTenantAsync(topicName.getTenant())
            .thenCompose(__ -> validateTopicOwnershipAsync(topicName, authoritative))
            .thenCompose(__ -> getTopicReferenceAsync(topicName))
            .thenCompose(Topic::truncate);
    }

    protected CompletableFuture<Void> internalTruncateTopicAsync(boolean authoritative) {

        // If the topic name is a partition name, no need to get partition topic metadata again
        if (topicName.isPartitioned()) {
            return internalTruncateNonPartitionedTopicAsync(authoritative);
        } else {
            return getPartitionedTopicMetadataAsync(topicName, authoritative, false).thenCompose(meta -> {
                if (meta.partitions > 0) {
                    final List<CompletableFuture<Void>> futures = new ArrayList<>(meta.partitions);
                    for (int i = 0; i < meta.partitions; i++) {
                        TopicName topicNamePartition = topicName.getPartition(i);
                        try {
                            futures.add(
                                pulsar().getAdminClient().topics()
                                .truncateAsync(topicNamePartition.toString()));
                        } catch (Exception e) {
                            log.error("[{}] Failed to truncate topic {}", clientAppId(), topicNamePartition, e);
                            return FutureUtil.failedFuture(new RestException(e));
                        }
                    }
                    return FutureUtil.waitForAll(futures);
                } else {
                    return internalTruncateNonPartitionedTopicAsync(authoritative);
                }
            });
        }
    }

    protected CompletableFuture<Void> internalSetEnableReplicatedSubscription(Boolean enabled,
                                                                              boolean isGlobal) {
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
                .thenCompose(op -> {
                    TopicPolicies topicPolicies = op.orElseGet(TopicPolicies::new);
                    topicPolicies.setReplicateSubscriptionState(enabled);
                    topicPolicies.setIsGlobal(isGlobal);
                    return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, topicPolicies);
                });
    }

    protected CompletableFuture<Boolean> internalGetReplicateSubscriptionState(boolean applied,
                                                                                  boolean isGlobal) {
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
                .thenApply(op -> op.map(TopicPolicies::getReplicateSubscriptionState)
                        .orElseGet(() -> {
                            if (applied) {
                                return getNamespacePolicies(namespaceName).replicate_subscription_state;
                            }
                            return null;
                        }));
    }

    protected void internalSetReplicatedSubscriptionStatus(AsyncResponse asyncResponse, String subName,
            boolean authoritative, Boolean enabled) {
        log.info("[{}] Attempting to change replicated subscription status to {} - {} {}", clientAppId(), enabled,
                topicName, subName);

        // Reject the request if the topic is not persistent
        if (!topicName.isPersistent()) {
            asyncResponse.resume(new RestException(Status.METHOD_NOT_ALLOWED,
                    "Cannot enable/disable replicated subscriptions on non-persistent topics"));
            return;
        }

        // Reject the request if the topic is not global
        if (!topicName.isGlobal()) {
            asyncResponse.resume(new RestException(Status.METHOD_NOT_ALLOWED,
                    "Cannot enable/disable replicated subscriptions on non-global topics"));
            return;
        }

        // 1.Permission to consume this topic is required
        // 2.Redirect the request to the peer-cluster if the local cluster is not included in the replication clusters
        CompletableFuture<Void> validateFuture =
                validateTopicOperationAsync(topicName, TopicOperation.SET_REPLICATED_SUBSCRIPTION_STATUS, subName)
                        .thenCompose(__ -> validateGlobalNamespaceOwnershipAsync(namespaceName));


        CompletableFuture<Void> resultFuture;
        // If the topic name is a partition name, no need to get partition topic metadata again
        if (topicName.isPartitioned()) {
            resultFuture = validateFuture.thenAccept(
                    __ -> internalSetReplicatedSubscriptionStatusForNonPartitionedTopic(asyncResponse, subName,
                            authoritative, enabled));
        } else {
            resultFuture = validateFuture.
                    thenCompose(__ -> getPartitionedTopicMetadataAsync(topicName, authoritative, false))
                    .thenAccept(partitionMetadata -> {
                        if (partitionMetadata.partitions > 0) {
                            final List<CompletableFuture<Void>> futures = new ArrayList<>();

                            for (int i = 0; i < partitionMetadata.partitions; i++) {
                                TopicName topicNamePartition = topicName.getPartition(i);
                                try {
                                    futures.add(pulsar().getAdminClient().topics().setReplicatedSubscriptionStatusAsync(
                                            topicNamePartition.toString(), subName, enabled));
                                } catch (Exception e) {
                                    log.warn("[{}] Failed to change replicated subscription status to {} - {} {}",
                                            clientAppId(), enabled, topicNamePartition, subName, e);
                                    resumeAsyncResponseExceptionally(asyncResponse, e);
                                    return;
                                }
                            }

                            FutureUtil.waitForAll(futures).handle((result, exception) -> {
                                if (exception != null) {
                                    Throwable t = exception.getCause();
                                    if (t instanceof NotFoundException) {
                                        asyncResponse
                                                .resume(new RestException(Status.NOT_FOUND,
                                                        "Topic or subscription not found"));
                                        return null;
                                    } else if (t instanceof PreconditionFailedException) {
                                        asyncResponse.resume(new RestException(Status.PRECONDITION_FAILED,
                                                "Cannot enable/disable replicated subscriptions on non-global topics"));
                                        return null;
                                    } else {
                                        log.warn("[{}] Failed to change replicated subscription status to {} - {} {}",
                                                clientAppId(), enabled, topicName, subName, t);
                                        asyncResponse.resume(new RestException(t));
                                        return null;
                                    }
                                }

                                asyncResponse.resume(Response.noContent().build());
                                return null;
                            });
                        } else {
                            internalSetReplicatedSubscriptionStatusForNonPartitionedTopic(asyncResponse, subName,
                                    authoritative, enabled);
                        }
                    });
        }

        resultFuture.exceptionally(ex -> {
            // If the exception is not redirect exception we need to log it.
            if (isNot307And404Exception(ex)) {
                log.warn("[{}] Failed to change replicated subscription status to {} - {} {}", clientAppId(), enabled,
                        topicName, subName, ex);
            }
            resumeAsyncResponseExceptionally(asyncResponse, ex);
            return null;
        });
    }

    private void internalSetReplicatedSubscriptionStatusForNonPartitionedTopic(
            AsyncResponse asyncResponse, String subName, boolean authoritative, Boolean enabled) {
        // Redirect the request to the appropriate broker if this broker is not the owner of the topic
        validateTopicOwnershipAsync(topicName, authoritative)
                .thenCompose(__ -> getTopicReferenceAsync(topicName))
                .thenAccept(topic -> {
                            if (topic == null) {
                                asyncResponse.resume(new RestException(Status.NOT_FOUND,
                                        getTopicNotFoundErrorMessage(topicName.toString())));
                                return;
                            }

                            Subscription sub = topic.getSubscription(subName);
                            if (sub == null) {
                                asyncResponse.resume(new RestException(Status.NOT_FOUND,
                                        getSubNotFoundErrorMessage(topicName.toString(), subName)));
                                return;
                            }

                            if (topic instanceof PersistentTopic && sub instanceof PersistentSubscription) {
                                if (!((PersistentSubscription) sub).setReplicated(enabled)) {
                                    asyncResponse.resume(
                                            new RestException(Status.INTERNAL_SERVER_ERROR,
                                                    "Failed to update cursor properties"));
                                    return;
                                }

                                ((PersistentTopic) topic).checkReplicatedSubscriptionControllerState();
                                log.info("[{}] Changed replicated subscription status to {} - {} {}", clientAppId(),
                                        enabled, topicName, subName);
                                asyncResponse.resume(Response.noContent().build());
                            } else {
                                asyncResponse.resume(new RestException(Status.METHOD_NOT_ALLOWED,
                                        "Cannot enable/disable replicated subscriptions on non-persistent topics"));
                            }
                        }
                ).exceptionally(ex -> {
                    // If the exception is not redirect exception we need to log it.
                    if (isNot307And404Exception(ex)) {
                        log.error("[{}] Failed to set replicated subscription status on {} {}", clientAppId(),
                                topicName, subName, ex);
                    }
                    resumeAsyncResponseExceptionally(asyncResponse, ex);
                    return null;
                });
    }

    protected void internalGetReplicatedSubscriptionStatus(AsyncResponse asyncResponse,
                                                           String subName,
                                                           boolean authoritative) {
        log.info("[{}] Attempting to get replicated subscription status on {} {}", clientAppId(), topicName, subName);

        // Reject the request if the topic is not persistent
        if (!topicName.isPersistent()) {
            asyncResponse.resume(new RestException(Status.METHOD_NOT_ALLOWED,
                    "Cannot get replicated subscriptions on non-persistent topics"));
            return;
        }

        // Reject the request if the topic is not global
        if (!topicName.isGlobal()) {
            asyncResponse.resume(new RestException(Status.METHOD_NOT_ALLOWED,
                    "Cannot get replicated subscriptions on non-global topics"));
            return;
        }

        // Permission to consume this topic is required
        CompletableFuture<Void> validateFuture =
                validateTopicOperationAsync(topicName, TopicOperation.GET_REPLICATED_SUBSCRIPTION_STATUS, subName);

        CompletableFuture<Void> resultFuture;
        // If the topic name is a partition name, no need to get partition topic metadata again
        if (topicName.isPartitioned()) {
            resultFuture = validateFuture.thenAccept(
                    __ -> internalGetReplicatedSubscriptionStatusForNonPartitionedTopic(asyncResponse,
                            subName, authoritative));
        } else {
            resultFuture = validateFuture
                    .thenCompose(__ -> getPartitionedTopicMetadataAsync(topicName, authoritative, false))
                    .thenAccept(partitionMetadata -> {
                        if (partitionMetadata.partitions > 0) {
                            List<CompletableFuture<Void>> futures = new ArrayList<>(partitionMetadata.partitions);
                            Map<String, Boolean> status = new ConcurrentHashMap<>(partitionMetadata.partitions);

                            for (int i = 0; i < partitionMetadata.partitions; i++) {
                                TopicName partition = topicName.getPartition(i);
                                futures.add(
                                    pulsar().getNamespaceService().isServiceUnitOwnedAsync(partition)
                                    .thenCompose(owned -> {
                                        if (owned) {
                                            return getReplicatedSubscriptionStatusFromLocalBroker(partition, subName);
                                        } else {
                                            try {
                                                return pulsar().getAdminClient().topics()
                                                    .getReplicatedSubscriptionStatusAsync(partition.toString(), subName)
                                                    .whenComplete((__, throwable) -> {
                                                        if (throwable != null) {
                                                            log.error("[{}] Failed to get replicated subscriptions on"
                                                                    + " {} {}",
                                                                clientAppId(), partition, subName, throwable);
                                                        }
                                                    });
                                            } catch (Exception e) {
                                                log.warn("[{}] Failed to get replicated subscription status on {} {}",
                                                    clientAppId(), partition, subName, e);
                                                return FutureUtil.failedFuture(e);
                                            }
                                        }
                                    }).thenAccept(status::putAll)
                                );
                            }

                            FutureUtil.waitForAll(futures).handle((result, exception) -> {
                                if (exception != null) {
                                    Throwable t = exception.getCause();
                                    if (t instanceof NotFoundException) {
                                        asyncResponse.resume(new RestException(Status.NOT_FOUND,
                                                "Topic or subscription not found"));
                                    } else if (t instanceof PreconditionFailedException) {
                                        asyncResponse.resume(new RestException(Status.PRECONDITION_FAILED,
                                                "Cannot get replicated subscriptions on non-global topics"));
                                    } else {
                                        log.error("[{}] Failed to get replicated subscription status on {} {}",
                                                clientAppId(), topicName, subName, t);
                                        asyncResponse.resume(new RestException(t));
                                    }
                                    return null;
                                }
                                asyncResponse.resume(status);
                                return null;
                            });
                        } else {
                            internalGetReplicatedSubscriptionStatusForNonPartitionedTopic(asyncResponse, subName,
                                    authoritative);
                        }
                    });
        }

        resultFuture.exceptionally(ex -> {
            if (isNot307And404Exception(ex)) {
                log.error("[{}] Failed to get replicated subscription status on {} {}", clientAppId(),
                        topicName, subName, ex);
            }
            resumeAsyncResponseExceptionally(asyncResponse, ex);
            return null;
        });
    }

    private CompletableFuture<Map<String, Boolean>> getReplicatedSubscriptionStatusFromLocalBroker(
            TopicName localTopicName,
            String subName) {
        return getTopicReferenceAsync(localTopicName).thenCompose(topic -> {
            Subscription sub = topic.getSubscription(subName);
            if (sub == null) {
                return FutureUtil.failedFuture(new RestException(Status.NOT_FOUND,
                    getSubNotFoundErrorMessage(localTopicName.toString(), subName)));
            }
            if (topic instanceof PersistentTopic && sub instanceof PersistentSubscription) {
                return CompletableFuture.completedFuture(
                        Collections.singletonMap(localTopicName.toString(), sub.isReplicated()));
            } else {
                return FutureUtil.failedFuture(new RestException(Status.METHOD_NOT_ALLOWED,
                    "Cannot get replicated subscriptions on non-persistent topics"));
            }
        });
    }

    private void internalGetReplicatedSubscriptionStatusForNonPartitionedTopic(
                                                                               AsyncResponse asyncResponse,
                                                                               String subName,
                                                                               boolean authoritative) {
        // Redirect the request to the appropriate broker if this broker is not the owner of the topic
        validateTopicOwnershipAsync(topicName, authoritative)
                .thenCompose(__ -> getReplicatedSubscriptionStatusFromLocalBroker(topicName, subName))
                .whenComplete((res, e) -> {
                    if (e != null) {
                        Throwable cause = FutureUtil.unwrapCompletionException(e);
                        log.error("[{}] Failed to get replicated subscription status on {} {}", clientAppId(),
                            topicName, subName, cause);
                        resumeAsyncResponseExceptionally(asyncResponse, e);
                    } else {
                        asyncResponse.resume(res);
                    }
                });
    }

    protected CompletableFuture<SchemaCompatibilityStrategy> internalGetSchemaCompatibilityStrategy(boolean applied) {
        CompletableFuture<Void> future = validateTopicPolicyOperationAsync(topicName,
                PolicyName.SCHEMA_COMPATIBILITY_STRATEGY, PolicyOperation.READ);
        if (applied) {
            return future.thenCompose(__ -> getSchemaCompatibilityStrategyAsync());
        }
        return future
                .thenCompose(n -> getTopicPoliciesAsyncWithRetry(topicName).thenApply(op -> {
                    if (!op.isPresent()) {
                        return null;
                    }
                    SchemaCompatibilityStrategy strategy = op.get().getSchemaCompatibilityStrategy();
                    return SchemaCompatibilityStrategy.isUndefined(strategy) ? null : strategy;
                }));
    }

    protected CompletableFuture<Void> internalSetSchemaCompatibilityStrategy(SchemaCompatibilityStrategy strategy) {
        return validateTopicPolicyOperationAsync(topicName,
                PolicyName.SCHEMA_COMPATIBILITY_STRATEGY,
                PolicyOperation.WRITE)
                .thenCompose((__) -> getTopicPoliciesAsyncWithRetry(topicName)
                        .thenCompose(op -> {
                            TopicPolicies topicPolicies = op.orElseGet(TopicPolicies::new);
                            topicPolicies.setSchemaCompatibilityStrategy(
                                    strategy == SchemaCompatibilityStrategy.UNDEFINED ? null : strategy);
                            return pulsar().getTopicPoliciesService()
                                    .updateTopicPoliciesAsync(topicName, topicPolicies);
                        }));
    }

    protected CompletableFuture<Boolean> internalGetSchemaValidationEnforced(boolean applied) {
        return getTopicPoliciesAsyncWithRetry(topicName)
                .thenApply(op -> op.map(TopicPolicies::getSchemaValidationEnforced).orElseGet(() -> {
                    if (applied) {
                        boolean namespacePolicy = getNamespacePolicies(namespaceName).schema_validation_enforced;
                        return namespacePolicy || pulsar().getConfiguration().isSchemaValidationEnforced();
                    }
                    return false;
                }));
    }

    protected CompletableFuture<Void> internalSetSchemaValidationEnforced(boolean schemaValidationEnforced) {
        return getTopicPoliciesAsyncWithRetry(topicName)
                .thenCompose(op -> {
                    TopicPolicies topicPolicies = op.orElseGet(TopicPolicies::new);
                    topicPolicies.setSchemaValidationEnforced(schemaValidationEnforced);
                    return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, topicPolicies);
                });
    }

    protected CompletableFuture<EntryFilters> internalGetEntryFilters(boolean applied, boolean isGlobal) {
        return validateTopicPolicyOperationAsync(topicName, PolicyName.ENTRY_FILTERS, PolicyOperation.READ)
                .thenCompose(__ -> {
                    if (!applied) {
                        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
                                .thenApply(op -> op.map(TopicPolicies::getEntryFilters).orElse(null));
                    }
                    if (!pulsar().getConfiguration().isAllowOverrideEntryFilters()) {
                        return CompletableFuture.completedFuture(new EntryFilters(String.join(",",
                                pulsar().getConfiguration().getEntryFilterNames())));
                    }
                    return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
                            .thenApply(op -> op.map(TopicPolicies::getEntryFilters))
                            .thenCompose(policyEntryFilters -> {
                                if (policyEntryFilters.isPresent()) {
                                    return CompletableFuture.completedFuture(policyEntryFilters.get());
                                }
                                return getNamespacePoliciesAsync(namespaceName)
                                        .thenApply(policies -> policies.entryFilters)
                                        .thenCompose(nsEntryFilters -> {
                                            if (nsEntryFilters != null) {
                                                return CompletableFuture.completedFuture(nsEntryFilters);
                                            }
                                            return CompletableFuture.completedFuture(new EntryFilters(String.join(",",
                                                    pulsar().getConfiguration().getEntryFilterNames())));
                                        });
                            });
                });
    }

    protected CompletableFuture<Void> internalSetEntryFilters(EntryFilters entryFilters,
                                                              boolean isGlobal) {

        return validateTopicPolicyOperationAsync(topicName, PolicyName.ENTRY_FILTERS, PolicyOperation.WRITE)
                .thenAccept(__ -> validateEntryFilters(entryFilters))
                .thenCompose(__ -> getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
                .thenCompose(op -> {
                    TopicPolicies topicPolicies = op.orElseGet(TopicPolicies::new);
                    topicPolicies.setEntryFilters(entryFilters);
                    topicPolicies.setIsGlobal(isGlobal);
                    return pulsar().getTopicPoliciesService()
                            .updateTopicPoliciesAsync(topicName, topicPolicies);
                }));
    }

    protected CompletableFuture<Void> internalRemoveEntryFilters(boolean isGlobal) {
        return validateTopicPolicyOperationAsync(topicName, PolicyName.ENTRY_FILTERS, PolicyOperation.WRITE)
                .thenCompose(__ ->
                        getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
                        .thenCompose(op -> {
                            if (!op.isPresent()) {
                                return CompletableFuture.completedFuture(null);
                            }
                            op.get().setEntryFilters(null);
                            op.get().setIsGlobal(isGlobal);
                            return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, op.get());
                        }));
    }

    protected CompletableFuture<Void> validateShadowTopics(List<String> shadowTopics) {
        List<CompletableFuture<Void>> futures = new ArrayList<>(shadowTopics.size());
        for (String shadowTopic : shadowTopics) {
            try {
                TopicName shadowTopicName = TopicName.get(shadowTopic);
                if (!shadowTopicName.isPersistent()) {
                    return FutureUtil.failedFuture(new RestException(Status.PRECONDITION_FAILED,
                            "Only persistent topic can be set as shadow topic"));
                }
                futures.add(pulsar().getNamespaceService().checkTopicExistsAsync(shadowTopicName)
                        .thenAccept(info -> {
                            boolean exists = info.isExists();
                            info.recycle();
                            if (!exists) {
                                throw new RestException(Status.PRECONDITION_FAILED,
                                        "Shadow topic [" + shadowTopic + "] not exists.");
                            }
                        }));
            } catch (IllegalArgumentException e) {
                return FutureUtil.failedFuture(new RestException(Status.FORBIDDEN,
                        "Invalid shadow topic name: " + shadowTopic));
            }
        }
        return FutureUtil.waitForAll(futures);
    }

    protected CompletableFuture<Void> internalSetShadowTopic(List<String> shadowTopics) {
        if (!topicName.isPersistent()) {
            return FutureUtil.failedFuture(new RestException(Status.PRECONDITION_FAILED,
                    "Only persistent source topic is supported with shadow topics."));
        }
        if (CollectionUtils.isEmpty(shadowTopics)) {
            return FutureUtil.failedFuture(new RestException(Status.PRECONDITION_FAILED,
                    "Cannot specify empty shadow topics, please use remove command instead."));
        }
        return validateTopicPolicyOperationAsync(topicName, PolicyName.SHADOW_TOPIC, PolicyOperation.WRITE)
                .thenCompose(__ -> validatePoliciesReadOnlyAccessAsync())
                .thenCompose(__ -> validateShadowTopics(shadowTopics))
                .thenCompose(__ -> getTopicPoliciesAsyncWithRetry(topicName))
                .thenCompose(op -> {
                    TopicPolicies topicPolicies = op.orElseGet(TopicPolicies::new);
                    topicPolicies.setShadowTopics(shadowTopics);
                    return pulsar().getTopicPoliciesService().
                            updateTopicPoliciesAsync(topicName, topicPolicies);
                });
    }

    protected CompletableFuture<Void> internalDeleteShadowTopics() {
        return validateTopicPolicyOperationAsync(topicName, PolicyName.SHADOW_TOPIC, PolicyOperation.WRITE)
                .thenCompose(__ -> validatePoliciesReadOnlyAccessAsync())
                .thenCompose(shadowTopicName -> getTopicPoliciesAsyncWithRetry(topicName))
                .thenCompose(op -> {
                    TopicPolicies topicPolicies = op.orElseGet(TopicPolicies::new);
                    List<String> shadowTopics = topicPolicies.getShadowTopics();
                    if (CollectionUtils.isEmpty(shadowTopics)) {
                        return CompletableFuture.completedFuture(null);
                    }
                    topicPolicies.setShadowTopics(null);
                    return pulsar().getTopicPoliciesService().
                            updateTopicPoliciesAsync(topicName, topicPolicies);
                });
    }

    protected CompletableFuture<Void> internalSetAutoSubscriptionCreation(
            AutoSubscriptionCreationOverrideImpl autoSubscriptionCreationOverride, boolean isGlobal) {
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
                .thenCompose(op -> {
                    TopicPolicies topicPolicies = op.orElseGet(TopicPolicies::new);
                    topicPolicies.setAutoSubscriptionCreationOverride(autoSubscriptionCreationOverride);
                    topicPolicies.setIsGlobal(isGlobal);
                    return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, topicPolicies);
                });
    }

    protected CompletableFuture<AutoSubscriptionCreationOverride> internalGetAutoSubscriptionCreation(boolean applied,
                                                                                                    boolean isGlobal) {
        return getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
                .thenApply(op -> op.map(TopicPolicies::getAutoSubscriptionCreationOverride)
                        .orElseGet(() -> {
                            if (applied) {
                                AutoSubscriptionCreationOverride namespacePolicy = getNamespacePolicies(namespaceName)
                                        .autoSubscriptionCreationOverride;
                                return namespacePolicy == null ? autoSubscriptionCreationOverride()
                                        : (AutoSubscriptionCreationOverrideImpl) namespacePolicy;
                            }
                            return null;
                        }));
    }

    protected CompletableFuture<Void> internalSetResourceGroup(String resourceGroupName, boolean isGlobal) {
        boolean isDelete = StringUtils.isEmpty(resourceGroupName);
        return validateTopicPolicyOperationAsync(topicName, PolicyName.RESOURCEGROUP, PolicyOperation.WRITE)
                .thenCompose(__ -> {
                    if (isDelete) {
                        return CompletableFuture.completedFuture(true);
                    }
                    return resourceGroupResources().resourceGroupExistsAsync(resourceGroupName);
                })
                .thenCompose(exists -> {
                    if (!exists) {
                        return FutureUtil.failedFuture(new RestException(Status.NOT_FOUND,
                                "ResourceGroup does not exist"));
                    }
                    return getTopicPoliciesAsyncWithRetry(topicName, isGlobal).thenCompose(op -> {
                        TopicPolicies topicPolicies = op.orElseGet(TopicPolicies::new);
                        topicPolicies.setResourceGroupName(isDelete ? null : resourceGroupName);
                        topicPolicies.setIsGlobal(isGlobal);
                        return pulsar().getTopicPoliciesService().updateTopicPoliciesAsync(topicName, topicPolicies);
                    });
                });
    }

    protected CompletableFuture<String> internalGetResourceGroup(boolean applied, boolean isGlobal) {
        return validateTopicPolicyOperationAsync(topicName, PolicyName.RESOURCEGROUP, PolicyOperation.READ)
                .thenCompose(__ -> getTopicPoliciesAsyncWithRetry(topicName, isGlobal)
                        .thenApply(op -> op.map(TopicPolicies::getResourceGroupName)
                                .orElseGet(() -> {
                                    if (applied) {
                                        return getNamespacePolicies(namespaceName).resource_group_name;
                                    }
                                    return null;
                                })
                        ));
    }
}
