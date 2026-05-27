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
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import javax.ws.rs.Encoded;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.namespace.LookupOptions;
import org.apache.pulsar.broker.web.PulsarWebResource;
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

public class TopicLookupBase extends PulsarWebResource {

    private static final io.github.merlimat.slog.Logger LOG =
            io.github.merlimat.slog.Logger.get(TopicLookupBase.class);

    private static final String LOOKUP_PATH = "/lookup/v2/topic/";

    protected CompletableFuture<LookupData> internalLookupTopicAsync(final TopicName topicName, boolean authoritative,
                                                                     String listenerName) {
        if (!pulsar().getBrokerService().getLookupRequestSemaphore().tryAcquire()) {
            log.warn().attr("topic", topicName).log("No broker was found available for topic");
            return FutureUtil.failedFuture(new WebApplicationException(Response.Status.SERVICE_UNAVAILABLE));
        }
        return validateGlobalNamespaceOwnershipAsync(topicName.getNamespaceObject())
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
                            log.warn().attr("topic", topicName).log("No broker was found available for topic");
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
                                String lookupPath = LOOKUP_PATH;
                                String path = String.format("%s%s%s?authoritative=%s",
                                        redirectUrl, lookupPath, topicName.getLookupName(), newAuthoritative);
                                path = listenerName == null ? path : path + "&listenerName=" + listenerName;
                                redirect = new URI(path);
                            } catch (URISyntaxException e) {
                                log.error()
                                        .attr("topic", topicName)
                                        .exception(e)
                                        .log("Error in preparing redirect url");
                                throw new RestException(Response.Status.PRECONDITION_FAILED, e.getMessage());
                            }
                                log.debug()
                                        .attr("topic", topicName)
                                        .attr("redirect", redirect)
                                        .log("Redirect lookup for topic");
                                throw new WebApplicationException(
                                        Response.temporaryRedirect(redirect).build());
                        } else {
                            // Found broker owning the topic
                                log.debug()
                                        .attr("topic", topicName)
                                        .attr("broker", result.getLookupData())
                                        .log("Lookup succeeded for topic - broker");
                                                        pulsar().getBrokerService().getLookupRequestSemaphore()
                                                                .release();
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
            log.error()
                    .attr("topic", topicName)
                    .exception(e)
                    .log("Failed to get namespace bundle");
            throw new RestException(e);
        }
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
    public static CompletableFuture<ByteBuf> lookupTopicAsync(PulsarService pulsarService, TopicName topicName,
                                                              boolean authoritative, String clientAppId,
                                                              String originalPrinciple,
                                                              AuthenticationDataSource authenticationData,
                                                              AuthenticationDataSource originalAuthenticationData,
                                                              long requestId, final String advertisedListenerName,
                                                              Map<String, String> properties) {

        final CompletableFuture<ByteBuf> validationFuture = new CompletableFuture<>();
        final CompletableFuture<ByteBuf> lookupfuture = new CompletableFuture<>();

        // (1) authorize client
        checkAuthorizationAsync(pulsarService, topicName, clientAppId, originalPrinciple,
                authenticationData, originalAuthenticationData).thenRun(() -> {
                    // (2) validate global namespace
                    // It is necessary for system topic operations because system topics are used to store metadata
                    // and other vital information. Even after namespace starting deletion,
                    // we need to access the metadata of system topics to create readers and clean up topic data.
                    // If we don't do this, it can prevent namespace deletion due to inaccessible readers.
                    checkLocalOrGetPeerReplicationCluster(pulsarService,
                            topicName.getNamespaceObject(), SystemTopicNames.isSystemTopic(topicName))
                            .thenAccept(peerClusterData -> {
                                if (peerClusterData == null) {
                                    // (3) all validation passed: initiate lookup
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
                        LOG.warn()
                                .attr("authorized", clientAppId)
                                .attr("topic", topicName)
                                .log("Failed to authorized on topic");
                        validationFuture.complete(newLookupErrorResponse(ServerError.AuthorizationError,
                                throwable.getMessage(), requestId));
                    } else {
                        LOG.warn()
                                .attr("authorizing", clientAppId)
                                .attr("topic", topicName)
                                .log("Unknown error while authorizing on topic");
                        validationFuture.completeExceptionally(throwable);
                    }
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
                        .properties(properties)
                        .build();
                pulsarService.getNamespaceService().getBrokerServiceUrlAsync(topicName, options)
                        .thenAccept(lookupResult -> {
                                LOG.debug()
                                        .attr("toString", topicName.toString())
                                        .attr("result", lookupResult)
                                        .log("Lookup result");
                                                        if (!lookupResult.isPresent()) {
                                lookupfuture.complete(newLookupErrorResponse(ServerError.ServiceNotReady,
                                        "No broker was available to own " + topicName, requestId));
                                return;
                            }

                            LookupData lookupData = lookupResult.get().getLookupData();
                            printWarnLogIfLookupResUnexpected(topicName, lookupData, options, pulsarService);
                            if (lookupResult.get().isRedirect()) {
                                boolean newAuthoritative = lookupResult.get().isAuthoritativeRedirect();
                                lookupfuture.complete(
                                        newLookupResponse(lookupData.getBrokerUrl(), lookupData.getBrokerUrlTls(),
                                                newAuthoritative, LookupType.Redirect, requestId, false));
                            } else {
                                ServiceConfiguration conf = pulsarService.getConfiguration();
                                lookupfuture.complete(newLookupResponse(lookupData.getBrokerUrl(),
                                        lookupData.getBrokerUrlTls(), true /* authoritative */, LookupType.Connect,
                                        requestId, shouldRedirectThroughServiceUrl(conf, lookupData)));
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
        if (SystemTopicNames.TRANSACTION_COORDINATOR_ASSIGN.getPartitionedTopicName()
                .equals(topic.getPartitionedTopicName())) {
            return;
        }
        boolean tlsEnabled = pulsar.getConfig().isBrokerClientTlsEnabled();
        if (!tlsEnabled && StringUtils.isBlank(lookupData.getBrokerUrl())) {
            LOG.warn()
                    .attr("topic", topic)
                    .attr("options", options)
                    .attr("result", lookupData)
                    .log("Unexpected lookup result: brokerUrl is required when TLS isn't enabled");
        } else if (tlsEnabled && StringUtils.isBlank(lookupData.getBrokerUrlTls())) {
            LOG.warn()
                    .attr("topic", topic)
                    .attr("options", options)
                    .attr("result", lookupData)
                    .log("Unexpected lookup result: brokerUrlTls is required when TLS is enabled");
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
            LOG.info()
                    .attr("topic", topicName)
                    .attr("errorMessage", errorMsg)
                    .log("Failed to lookup topic");
            lookupFuture.complete(newLookupErrorResponse(ServerError.MetadataError, errorMsg, requestId));
        } else if (unwrapEx instanceof MetadataStoreException) {
            // Load bundle ownership or acquire lock failed.
            // Differ with "IllegalStateException", print warning LOG.
            LOG.warn()
                    .attr("topic", topicName)
                    .attr("errorMessage", errorMsg)
                    .log("Failed to lookup topic");
            lookupFuture.complete(newLookupErrorResponse(ServerError.MetadataError, errorMsg, requestId));
        } else {
            LOG.warn()
                    .attr("topic", topicName)
                    .attr("errorMessage", errorMsg)
                    .log("Failed to lookup topic");
            lookupFuture.complete(newLookupErrorResponse(ServerError.ServiceNotReady, errorMsg, requestId));
        }
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
                LOG.info()
                        .attr("address", lookupData.getBrokerUrl())
                        .exceptionMessage(e)
                        .log("Failed to resolve advertised address");
                return false;
            }
        }
        if (!StringUtils.isEmpty(lookupData.getBrokerUrlTls())) {
            try {
                URI host = URI.create(lookupData.getBrokerUrlTls());
                return InetAddress.getByName(host.getHost()).isLoopbackAddress();
            } catch (Exception e) {
                LOG.info()
                        .attr("address", lookupData.getBrokerUrlTls())
                        .exceptionMessage(e)
                        .log("Failed to resolve advertised address");
                return false;
            }
        }
        return false;
    }
}
