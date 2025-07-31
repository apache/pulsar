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
package org.apache.pulsar.broker.lookup;

import static org.apache.pulsar.common.protocol.Commands.newLookupErrorResponse;
import static org.apache.pulsar.common.protocol.Commands.newLookupResponse;
import io.netty.buffer.ByteBuf;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import javax.ws.rs.Encoded;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.admin.AdminResource;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.event.data.TopicLookupEventData;
import org.apache.pulsar.broker.event.data.TopicLookupEventData.TopicLookupEventDataBuilder;
import org.apache.pulsar.broker.namespace.LookupOptions;
import org.apache.pulsar.broker.service.ServerCnx;
import org.apache.pulsar.broker.service.TopicEventsListener.TopicEvent;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.common.api.proto.CommandLookupTopicResponse.LookupType;
import org.apache.pulsar.common.api.proto.ServerError;
import org.apache.pulsar.common.lookup.data.LookupData;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.SystemTopicNames;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.NamespaceOperation;
import org.apache.pulsar.common.policies.data.TopicOperation;
import org.apache.pulsar.common.util.Codec;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TopicLookupBase extends AdminResource {

    private static final String LOOKUP_PATH_V1 = "/lookup/v2/destination/";
    private static final String LOOKUP_PATH_V2 = "/lookup/v2/topic/";

    protected CompletableFuture<LookupData> internalLookupTopicAsync(final TopicName topicName, boolean authoritative,
                                                                     String listenerName) {
        if (!pulsar().getBrokerService().getLookupRequestSemaphore().tryAcquire()) {
            log.warn("No broker was found available for topic {}", topicName);
            return FutureUtil.failedFuture(new WebApplicationException(Response.Status.SERVICE_UNAVAILABLE));
        }
        return validateClusterOwnershipAsync(topicName.getCluster())
                .thenCompose(__ -> validateGlobalNamespaceOwnershipAsync(topicName.getNamespaceObject()))
                .thenCompose(__ -> validateTopicOperationAsync(topicName, TopicOperation.LOOKUP, null))
                .thenCompose(__ -> {
                    // Case-1: Non-persistent topic.
                    // Currently, it's hard to check the non-persistent-non-partitioned topic, because it only exists
                    // in the broker, it doesn't have metadata. If the topic is non-persistent and non-partitioned,
                    // we'll return the true flag. So either it is a partitioned topic or not, the result will be true.
                    if (!topicName.isPersistent()) {
                        return CompletableFuture.completedFuture(true);
                    }
                    // Case-2: Persistent topic.
                    return pulsar().getNamespaceService().checkTopicExistsAsync(topicName).thenCompose(info -> {
                        boolean exists = info.isExists();
                        info.recycle();
                        if (exists) {
                            return CompletableFuture.completedFuture(true);
                        }
                        return pulsar().getBrokerService().isAllowAutoTopicCreationAsync(topicName);
                    });
                })
                .thenCompose(exist -> {
                    if (!exist) {
                        throw new RestException(Response.Status.NOT_FOUND,
                                String.format("Topic not found %s", topicName.toString()));
                    }
                    CompletableFuture<Optional<LookupResult>> lookupFuture = pulsar().getNamespaceService()
                            .getBrokerServiceUrlAsync(topicName,
                                    LookupOptions.builder()
                                            .advertisedListenerName(listenerName)
                                            .authoritative(authoritative)
                                            .loadTopicsInBundle(false)
                                            .build());

                    return lookupFuture.thenApply(optionalResult -> {
                        if (optionalResult == null || !optionalResult.isPresent()) {
                            log.warn("No broker was found available for topic {}", topicName);
                            throw new WebApplicationException(Response.Status.SERVICE_UNAVAILABLE);
                        }

                        LookupResult result = optionalResult.get();
                        // We have found either a broker that owns the topic, or a broker to
                        // which we should redirect the client to
                        if (result.isRedirect()) {
                            boolean newAuthoritative = result.isAuthoritativeRedirect();
                            URI redirect;
                            try {
                                String redirectUrl = isRequestHttps() ? result.getLookupData().getHttpUrlTls()
                                        : result.getLookupData().getHttpUrl();
                                if (redirectUrl == null) {
                                    log.error("Redirected cluster's service url is not configured");
                                    throw new RestException(Response.Status.PRECONDITION_FAILED,
                                            "Redirected cluster's service url is not configured.");
                                }
                                String lookupPath = topicName.isV2() ? LOOKUP_PATH_V2 : LOOKUP_PATH_V1;
                                String path = String.format("%s%s%s?authoritative=%s",
                                        redirectUrl, lookupPath, topicName.getLookupName(), newAuthoritative);
                                path = listenerName == null ? path : path + "&listenerName=" + listenerName;
                                redirect = new URI(path);
                            } catch (URISyntaxException e) {
                                log.error("Error in preparing redirect url for {}: {}", topicName, e.getMessage(), e);
                                throw new RestException(Response.Status.PRECONDITION_FAILED, e.getMessage());
                            }
                            if (log.isDebugEnabled()) {
                                log.debug("Redirect lookup for topic {} to {}", topicName, redirect);
                            }
                            TopicLookupEventDataBuilder eventDataBuilder =
                                    TopicLookupEventData.builder().authoritative(newAuthoritative);
                            if (isRequestHttps()) {
                                eventDataBuilder.httpUrlTls(redirect.toString());
                            } else {
                                eventDataBuilder.httpUrl(redirect.toString());
                            }
                            newTopicEvent(topicName, TopicEvent.LOOKUP)
                                    .data(eventDataBuilder.build())
                                    .dispatch();
                            throw new WebApplicationException(Response.temporaryRedirect(redirect).build());
                        } else {
                            // Found broker owning the topic
                            if (log.isDebugEnabled()) {
                                log.debug("Lookup succeeded for topic {} -- broker: {}", topicName,
                                        result.getLookupData());
                            }
                            pulsar().getBrokerService().getLookupRequestSemaphore().release();
                            newTopicEvent(topicName, TopicEvent.LOOKUP)
                                    .data(TopicLookupEventData.builder()
                                            .authoritative(authoritative)
                                            .redirect(false)
                                            .httpUrl(result.getLookupData().getHttpUrl())
                                            .httpUrlTls(result.getLookupData().getHttpUrlTls())
                                            .brokerUrl(result.getLookupData().getBrokerUrl())
                                            .brokerUrlTls(result.getLookupData().getBrokerUrlTls())
                                            .build())
                                    .dispatch();
                            return result.getLookupData();
                        }
                    });
                }).exceptionally(ex -> {
                    pulsar().getBrokerService().getLookupRequestSemaphore().release();
                    throw FutureUtil.wrapToCompletionException(ex);
                });
    }

    protected String internalGetNamespaceBundle(TopicName topicName) {
        validateNamespaceOperation(topicName.getNamespaceObject(), NamespaceOperation.GET_BUNDLE);
        try {
            NamespaceBundle bundle = pulsar().getNamespaceService().getBundle(topicName);
            return bundle.getBundleRange();
        } catch (Exception e) {
            log.error("[{}] Failed to get namespace bundle for {}", clientAppId(), topicName, e);
            throw new RestException(e);
        }
    }

    /**
     * Lookup broker-service address for a given namespace-bundle which contains given topic.
     *
     * a. Returns broker-address if namespace-bundle is already owned by any broker
     * b. If current-broker receives lookup-request and if it's not a leader then current broker redirects request
     * to leader by returning leader-service address.
     * c. If current-broker is leader then it finds out least-loaded broker to
     * own namespace bundle and redirects request
     * by returning least-loaded broker.
     * d. If current-broker receives request to own the namespace-bundle then
     * it owns a bundle and returns success(connect)
     * response to client.
     *
     * @param pulsarService
     * @param topicName
     * @param authoritative
     * @param clientAppId
     * @param requestId
     * @return
     */
    @Deprecated
    public static CompletableFuture<ByteBuf> lookupTopicAsync(PulsarService pulsarService, TopicName topicName,
            boolean authoritative, String clientAppId, AuthenticationDataSource authenticationData, long requestId) {
        return lookupTopicAsync(pulsarService, topicName, authoritative, clientAppId,
                authenticationData, requestId, null, null);
    }

    /**
     *
     * Lookup broker-service address for a given namespace-bundle which contains given topic.
     *
     * a. Returns broker-address if namespace-bundle is already owned by any broker
     * b. If current-broker receives lookup-request and if it's not a leader then current broker redirects request
     *    to leader by returning leader-service address.
     * c. If current-broker is leader then it finds out least-loaded broker
     *    to own namespace bundle and redirects request
     *    by returning least-loaded broker.
     * d. If current-broker receives request to own the namespace-bundle then
     *    it owns a bundle and returns success(connect)
     *    response to client.
     *
     * @param pulsarService
     * @param topicName
     * @param authoritative
     * @param clientAppId
     * @param requestId
     * @param advertisedListenerName
     * @return
     */
    @Deprecated
    public static CompletableFuture<ByteBuf> lookupTopicAsync(PulsarService pulsarService, TopicName topicName,
                                                              boolean authoritative, String clientAppId,
                                                              AuthenticationDataSource authenticationData,
                                                              long requestId, final String advertisedListenerName) {
        return lookupTopicAsync(pulsarService, topicName, authoritative, clientAppId, authenticationData, requestId,
                advertisedListenerName, null);
    }

    public static CompletableFuture<ByteBuf> lookupTopicAsync(PulsarService pulsarService, TopicName topicName,
                                                              boolean authoritative, String clientAppId,
                                                              AuthenticationDataSource authenticationData,
                                                              long requestId, final String advertisedListenerName,
                                                              ServerCnx cnx) {

        final CompletableFuture<ByteBuf> validationFuture = new CompletableFuture<>();
        final CompletableFuture<ByteBuf> lookupfuture = new CompletableFuture<>();
        final String cluster = topicName.getCluster();

        // (1) validate cluster
        getClusterDataIfDifferentCluster(pulsarService, cluster, clientAppId).thenAccept(differentClusterData -> {

            if (differentClusterData != null) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Redirecting the lookup call to {}/{} cluster={}", clientAppId,
                            differentClusterData.getBrokerServiceUrl(), differentClusterData.getBrokerServiceUrlTls(),
                            cluster);
                }
                if (cnx != null) {
                    cnx.newTopicEvent(topicName.toString(), TopicEvent.LOOKUP)
                            .data(TopicLookupEventData.builder()
                                    .authoritative(authoritative)
                                    .httpUrl(differentClusterData.getBrokerServiceUrl())
                                    .httpUrlTls(differentClusterData.getBrokerServiceUrlTls())
                                    .brokerUrl(differentClusterData.getBrokerServiceUrl())
                                    .brokerUrlTls(differentClusterData.getBrokerServiceUrlTls())
                                    .authoritative(true)
                                    .redirect(true)
                                    .proxyThroughServiceUrl(false)
                                    .build())
                            .dispatch();
                }
                validationFuture.complete(newLookupResponse(differentClusterData.getBrokerServiceUrl(),
                        differentClusterData.getBrokerServiceUrlTls(), true, LookupType.Redirect,
                        requestId, false));
            } else {
                // (2) authorize client
                checkAuthorizationAsync(pulsarService, topicName, clientAppId, authenticationData).thenRun(() -> {
                        // (3) validate global namespace
                        // It is necessary for system topic operations because system topics are used to store metadata
                        // and other vital information. Even after namespace starting deletion,
                        // we need to access the metadata of system topics to create readers and clean up topic data.
                        // If we don't do this, it can prevent namespace deletion due to inaccessible readers.
                        checkLocalOrGetPeerReplicationCluster(pulsarService,
                                topicName.getNamespaceObject(), SystemTopicNames.isSystemTopic(topicName))
                                .thenAccept(peerClusterData -> {
                                    if (peerClusterData == null) {
                                        // (4) all validation passed: initiate lookup
                                        validationFuture.complete(null);
                                        return;
                                    }
                                    // if peer-cluster-data is present it means namespace is owned by that peer-cluster
                                    // and request should be redirect to the peer-cluster
                                    if (StringUtils.isBlank(peerClusterData.getBrokerServiceUrl())
                                            && StringUtils.isBlank(peerClusterData.getBrokerServiceUrlTls())) {
                                        validationFuture.complete(newLookupErrorResponse(ServerError.MetadataError,
                                                "Redirected cluster's brokerService url is not configured",
                                                requestId));
                                        return;
                                    }
                                    validationFuture.complete(newLookupResponse(peerClusterData.getBrokerServiceUrl(),
                                            peerClusterData.getBrokerServiceUrlTls(), true,
                                            LookupType.Redirect, requestId,
                                            false));
                        }).exceptionally(ex -> {
                            Throwable throwable = FutureUtil.unwrapCompletionException(ex);
                            if (throwable instanceof RestException restException){
                                if (restException.getResponse().getStatus()
                                        == Response.Status.NOT_FOUND.getStatusCode()) {
                                    validationFuture.complete(
                                            newLookupErrorResponse(ServerError.TopicNotFound,
                                                    throwable.getMessage(), requestId));
                                    return null;
                                }
                            }
                            validationFuture.complete(
                                    newLookupErrorResponse(ServerError.MetadataError,
                                            throwable.getMessage(), requestId));
                            return null;
                        });
                    })
                    .exceptionally(e -> {
                        Throwable throwable = FutureUtil.unwrapCompletionException(e);
                        if (throwable instanceof RestException) {
                            log.warn("Failed to authorized {} on cluster {}", clientAppId, topicName);
                            validationFuture.complete(newLookupErrorResponse(ServerError.AuthorizationError,
                                    throwable.getMessage(), requestId));
                        } else {
                            log.warn("Unknown error while authorizing {} on cluster {}", clientAppId, topicName);
                            validationFuture.completeExceptionally(throwable);
                        }
                        return null;
                    });
            }
        }).exceptionally(ex -> {
            validationFuture.completeExceptionally(FutureUtil.unwrapCompletionException(ex));
            return null;
        });

        // Initiate lookup once validation completes
        validationFuture.thenAccept(validationFailureResponse -> {
            if (validationFailureResponse != null) {
                lookupfuture.complete(validationFailureResponse);
            } else {
                LookupOptions options = LookupOptions.builder()
                        .authoritative(authoritative)
                        .advertisedListenerName(advertisedListenerName)
                        .loadTopicsInBundle(true)
                        .build();
                pulsarService.getNamespaceService().getBrokerServiceUrlAsync(topicName, options)
                        .thenAccept(lookupResult -> {

                            if (log.isDebugEnabled()) {
                                log.debug("[{}] Lookup result {}", topicName.toString(), lookupResult);
                            }

                            if (!lookupResult.isPresent()) {
                                lookupfuture.complete(newLookupErrorResponse(ServerError.ServiceNotReady,
                                        "No broker was available to own " + topicName, requestId));
                                return;
                            }

                            LookupData lookupData = lookupResult.get().getLookupData();
                            printWarnLogIfLookupResUnexpected(topicName, lookupData, options, pulsarService);
                            boolean proxyThroughServiceUrl;
                            boolean newAuthoritative;
                            if (lookupResult.get().isRedirect()) {
                                proxyThroughServiceUrl = false;
                                newAuthoritative = lookupResult.get().isAuthoritativeRedirect();
                                lookupfuture.complete(
                                        newLookupResponse(lookupData.getBrokerUrl(), lookupData.getBrokerUrlTls(),
                                                newAuthoritative, LookupType.Redirect, requestId,
                                                proxyThroughServiceUrl));
                            } else {
                                ServiceConfiguration conf = pulsarService.getConfiguration();
                                proxyThroughServiceUrl = shouldRedirectThroughServiceUrl(conf, lookupData);
                                newAuthoritative = true;
                                lookupfuture.complete(newLookupResponse(lookupData.getBrokerUrl(),
                                        lookupData.getBrokerUrlTls(), newAuthoritative /* authoritative */,
                                        LookupType.Connect,
                                        requestId, proxyThroughServiceUrl));
                            }
                            if (cnx != null) {
                                lookupfuture.whenComplete((__, ex) -> {
                                    if (ex == null) {
                                        cnx.newTopicEvent(topicName.toString(), TopicEvent.LOOKUP)
                                                .data(TopicLookupEventData.builder()
                                                        .authoritative(newAuthoritative)
                                                        .httpUrl(lookupData.getHttpUrl())
                                                        .httpUrlTls(lookupData.getHttpUrlTls())
                                                        .brokerUrl(lookupData.getBrokerUrl())
                                                        .brokerUrlTls(lookupData.getBrokerUrlTls())
                                                        .redirect(lookupResult.get().isRedirect())
                                                        .proxyThroughServiceUrl(proxyThroughServiceUrl)
                                                        .build())
                                                .dispatch();
                                    }
                                });
                            }
                        }).exceptionally(ex -> {
                            handleLookupError(lookupfuture, topicName.toString(), clientAppId, requestId, ex);
                            return null;
                        });
            }
        }).exceptionally(ex -> {
            handleLookupError(lookupfuture, topicName.toString(), clientAppId, requestId, ex);
            return null;
        });

        return lookupfuture;
    }

    /**
     * Check if a internal client will get a null lookup result.
     */
    private static void printWarnLogIfLookupResUnexpected(TopicName topic, LookupData lookupData, LookupOptions options,
                                                          PulsarService pulsar) {
        if (!pulsar.getBrokerService().isSystemTopic(topic)) {
            return;
        }
        boolean tlsEnabled = pulsar.getConfig().isBrokerClientTlsEnabled();
        if (!tlsEnabled && StringUtils.isBlank(lookupData.getBrokerUrl())) {
            log.warn("[{}] Unexpected lookup result: brokerUrl is required when TLS isn't enabled. options: {},"
                + " result {}", topic, options, lookupData);
        } else if (tlsEnabled && StringUtils.isBlank(lookupData.getBrokerUrlTls())) {
            log.warn("[{}] Unexpected lookup result: brokerUrlTls is required when TLS is enabled. options: {},"
                    + " result {}", topic, options, lookupData);
        }
    }

    private static void handleLookupError(CompletableFuture<ByteBuf> lookupFuture, String topicName, String clientAppId,
                                   long requestId, Throwable ex){
        Throwable unwrapEx = FutureUtil.unwrapCompletionException(ex);
        final String errorMsg = unwrapEx.getMessage();
        if (unwrapEx instanceof PulsarServerException) {
            unwrapEx = FutureUtil.unwrapCompletionException(unwrapEx.getCause());
        }
        if (unwrapEx instanceof IllegalStateException) {
            // Current broker still hold the bundle's lock, but the bundle is being unloading.
            log.info("Failed to lookup {} for topic {} with error {}", clientAppId, topicName, errorMsg);
            lookupFuture.complete(newLookupErrorResponse(ServerError.MetadataError, errorMsg, requestId));
        } else if (unwrapEx instanceof MetadataStoreException) {
            // Load bundle ownership or acquire lock failed.
            // Differ with "IllegalStateException", print warning log.
            log.warn("Failed to lookup {} for topic {} with error {}", clientAppId, topicName, errorMsg);
            lookupFuture.complete(newLookupErrorResponse(ServerError.MetadataError, errorMsg, requestId));
        } else {
            log.warn("Failed to lookup {} for topic {} with error {}", clientAppId, topicName, errorMsg);
            lookupFuture.complete(newLookupErrorResponse(ServerError.ServiceNotReady, errorMsg, requestId));
        }
    }

    protected TopicName getTopicName(String topicDomain, String tenant, String cluster, String namespace,
            @Encoded String encodedTopic) {
        String decodedName = Codec.decode(encodedTopic);
        return TopicName.get(TopicDomain.getEnum(topicDomain).value(), tenant, cluster, namespace, decodedName);
    }

    protected TopicName getTopicName(String topicDomain, String tenant, String namespace,
            @Encoded String encodedTopic) {
        String decodedName = Codec.decode(encodedTopic);
        return TopicName.get(TopicDomain.getEnum(topicDomain).value(), tenant, namespace, decodedName);
    }

    private static boolean shouldRedirectThroughServiceUrl(ServiceConfiguration conf, LookupData lookupData) {
        // When running in standalone mode we want to redirect the client through the service URL,
        // if the advertised address is a loopback address (see PulsarStandaloneStarter).
        if (!conf.isRunningStandalone()) {
            return false;
        }
        if (!StringUtils.isEmpty(lookupData.getBrokerUrl())) {
            try {
                URI host = URI.create(lookupData.getBrokerUrl());
                return InetAddress.getByName(host.getHost()).isLoopbackAddress();
            } catch (Exception e) {
                log.info("Failed to resolve advertised address {}: {}", lookupData.getBrokerUrl(), e.getMessage());
                return false;
            }
        }
        if (!StringUtils.isEmpty(lookupData.getBrokerUrlTls())) {
            try {
                URI host = URI.create(lookupData.getBrokerUrlTls());
                return InetAddress.getByName(host.getHost()).isLoopbackAddress();
            } catch (Exception e) {
                log.info("Failed to resolve advertised address {}: {}", lookupData.getBrokerUrlTls(), e.getMessage());
                return false;
            }
        }
        return false;
    }

    private static final Logger log = LoggerFactory.getLogger(TopicLookupBase.class);
}
