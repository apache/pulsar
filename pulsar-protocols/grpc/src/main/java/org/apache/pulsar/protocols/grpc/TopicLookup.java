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
package org.apache.pulsar.protocols.grpc;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.loadbalance.LoadManager;
import org.apache.pulsar.broker.web.PulsarWebResource;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.common.lookup.data.LookupData;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.protocols.grpc.api.CommandLookupTopicResponse;
import org.apache.pulsar.protocols.grpc.api.CommandLookupTopicResponse.LookupType;
import org.apache.pulsar.protocols.grpc.api.ServerError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.StringReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static org.apache.pulsar.protocols.grpc.Commands.newLookupResponse;
import static org.apache.pulsar.protocols.grpc.Commands.newStatusException;
import static org.apache.pulsar.protocols.grpc.Constants.*;

public class TopicLookup extends PulsarWebResource {

    /**
     *
     * Lookup broker-service address for a given namespace-bundle which contains given topic.
     *
     * a. Returns broker-address if namespace-bundle is already owned by any broker b. If current-broker receives
     * lookup-request and if it's not a leader then current broker redirects request to leader by returning
     * leader-service address. c. If current-broker is leader then it finds out least-loaded broker to own namespace
     * bundle and redirects request by returning least-loaded broker. d. If current-broker receives request to own the
     * namespace-bundle then it owns a bundle and returns success(connect) response to client.
     *
     */
    public static CompletableFuture<CommandLookupTopicResponse> lookupTopicAsync(PulsarService pulsarService, TopicName topicName,
            boolean authoritative, String clientAppId, AuthenticationDataSource authenticationData) {

        final CompletableFuture<CommandLookupTopicResponse> validationFuture = new CompletableFuture<>();
        final CompletableFuture<CommandLookupTopicResponse> lookupfuture = new CompletableFuture<>();
        final String cluster = topicName.getCluster();

        // (1) validate cluster
        getClusterDataIfDifferentCluster(pulsarService, cluster, clientAppId).thenAccept(differentClusterData -> {

            if (differentClusterData != null) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Redirecting the lookup call to {}/{} cluster={}", clientAppId,
                            differentClusterData.getBrokerServiceUrl(), differentClusterData.getBrokerServiceUrlTls(),
                            cluster);
                }
                lookupTopicGrpcData(pulsarService, validationFuture, differentClusterData.getServiceUrl(),
                        differentClusterData.getServiceUrlTls(), true, LookupType.Redirect,
                        false);
            } else {
                // (2) authorize client
                try {
                    checkAuthorization(pulsarService, topicName, clientAppId, authenticationData);
                } catch (RestException authException) {
                    log.warn("Failed to authorized {} on cluster {}", clientAppId, topicName.toString());
                    validationFuture.completeExceptionally(
                            newStatusException(Status.PERMISSION_DENIED, authException, ServerError.AuthorizationError));
                    return;
                } catch (Exception e) {
                    log.warn("Unknown error while authorizing {} on cluster {}", clientAppId, topicName.toString());
                    validationFuture.completeExceptionally(e);
                    return;
                }
                // (3) validate global namespace
                checkLocalOrGetPeerReplicationCluster(pulsarService, topicName.getNamespaceObject())
                        .thenAccept(peerClusterData -> {
                            if (peerClusterData == null) {
                                // (4) all validation passed: initiate lookup
                                validationFuture.complete(null);
                                return;
                            }
                            // if peer-cluster-data is present it means namespace is owned by that peer-cluster and
                            // request should be redirect to the peer-cluster
                            if (StringUtils.isBlank(peerClusterData.getBrokerServiceUrl())
                                    && StringUtils.isBlank(peerClusterData.getBrokerServiceUrl())) {
                                validationFuture.completeExceptionally(
                                        newStatusException(Status.INVALID_ARGUMENT,
                                                "Redirected cluster's brokerService url is not configured", null,
                                                ServerError.MetadataError)
                                );
                                return;
                            }
                            lookupTopicGrpcData(pulsarService, validationFuture, peerClusterData.getServiceUrl(),
                                    peerClusterData.getServiceUrlTls(), true, LookupType.Redirect,
                                    false);

                        }).exceptionally(ex -> {
                    validationFuture.completeExceptionally(
                            newStatusException(Status.INVALID_ARGUMENT, ex, ServerError.MetadataError));
                    return null;
                });
            }
        }).exceptionally(ex -> {
            validationFuture.completeExceptionally(ex);
            return null;
        });

        // Initiate lookup once validation completes
        validationFuture.thenAccept(validationFailureResponse -> {
            if (validationFailureResponse != null) {
                lookupfuture.complete(validationFailureResponse);
            } else {
                pulsarService.getNamespaceService().getBrokerServiceUrlAsync(topicName, authoritative)
                        .thenAccept(lookupResult -> {

                            if (log.isDebugEnabled()) {
                                log.debug("[{}] Lookup result {}", topicName.toString(), lookupResult);
                            }

                            if (!lookupResult.isPresent()) {
                                lookupfuture.completeExceptionally(
                                        newStatusException(Status.UNAVAILABLE,
                                                "No broker was available to own " + topicName, null, ServerError.ServiceNotReady));
                                return;
                            }

                            LookupData lookupData = lookupResult.get().getLookupData();
                            if (lookupResult.get().isRedirect()) {
                                boolean newAuthoritative = isLeaderBroker(pulsarService);
                                lookupTopicGrpcData(pulsarService, lookupfuture, lookupData.getHttpUrl(),
                                        lookupData.getHttpUrlTls(), newAuthoritative, LookupType.Redirect,
                                        false);
                            } else {
                                // When running in standalone mode we want to redirect the client through the service
                                // url, so that the advertised address configuration is not relevant anymore.
                                boolean redirectThroughServiceUrl = pulsarService.getConfiguration()
                                        .isRunningStandalone();

                                lookupTopicGrpcData(pulsarService, lookupfuture, lookupData.getHttpUrl(),
                                        lookupData.getHttpUrlTls(), true, LookupType.Connect,
                                        redirectThroughServiceUrl);
                            }
                        }).exceptionally(ex -> handleLookupException(topicName, clientAppId, lookupfuture, ex));
            }

        }).exceptionally(ex -> {
            if (ex instanceof StatusRuntimeException) {
                lookupfuture.completeExceptionally(ex);
            } else {
                handleLookupException(topicName, clientAppId, lookupfuture, ex);
            }
            return null;
        });

        return lookupfuture;
    }

    private static Void handleLookupException(TopicName topicName, String clientAppId, CompletableFuture<CommandLookupTopicResponse> lookupfuture, Throwable ex) {
        if (ex instanceof CompletionException && ex.getCause() instanceof IllegalStateException) {
            log.info("Failed to lookup {} for topic {} with error {}", clientAppId,
                    topicName.toString(), ex.getCause().getMessage());
        } else {
            log.warn("Failed to lookup {} for topic {} with error {}", clientAppId,
                    topicName.toString(), ex.getMessage(), ex);
        }
        lookupfuture.completeExceptionally(
                newStatusException(Status.UNAVAILABLE, ex, ServerError.ServiceNotReady));
        return null;
    }

    private static void lookupTopicGrpcData(PulsarService pulsarService,
            CompletableFuture<CommandLookupTopicResponse> lookupfuture, String serviceUrl, String serviceUrlTls, boolean authoritative,
            CommandLookupTopicResponse.LookupType type, boolean redirectThroughServiceUrl) {
        try {
            String lookupServiceUrl = serviceUrl != null ? serviceUrl : serviceUrlTls;
            URI uri = new URI(lookupServiceUrl);
            String path = String.format("%s/%s:%s", LoadManager.LOADBALANCE_BROKERS_ROOT, uri.getHost(),
                    uri.getPort());
            pulsarService.getLocalZkCache().getDataAsync(path, pulsarService.getLoadManager().get().getLoadReportDeserializer()).thenAccept(reportData -> {
                Optional<String> grpcData = reportData.flatMap(serviceLookupData -> serviceLookupData.getProtocol("grpc"));
                if (grpcData.isPresent()) {
                    String props = grpcData.get();
                    Properties properties = new Properties();
                    try {
                        properties.load(new StringReader(props.replaceAll(";", "\n")));
                        String grpcHost = properties.getProperty(GRPC_SERVICE_HOST_PROPERTY_NAME);
                        String grpcServicePortProp = properties.getProperty(GRPC_SERVICE_PORT_PROPERTY_NAME);
                        Integer grpcServicePort =
                                grpcServicePortProp != null ? Integer.valueOf(grpcServicePortProp) : null;
                        String grpcServicePortTlsProp = properties.getProperty(GRPC_SERVICE_PORT_TLS_PROPERTY_NAME);
                        Integer grpcServicePortTls =
                                grpcServicePortTlsProp != null ? Integer.valueOf(grpcServicePortTlsProp) : null;
                        if (grpcHost != null) {
                            lookupfuture.complete(newLookupResponse(grpcHost, grpcServicePort, grpcServicePortTls,
                                    authoritative, type, redirectThroughServiceUrl));
                            return;
                        }

                    } catch (Exception e) {
                        log.error("Couldn't parse grpc data", e);
                    }
                }
                lookupfuture.completeExceptionally(newStatusException(Status.UNAVAILABLE,
                        "Couldn't get gRPC protocol data on broker owning the topic", null, ServerError.MetadataError));
            }).exceptionally(ex -> {
                lookupfuture.completeExceptionally(newStatusException(Status.UNAVAILABLE,
                        "Couldn't read load report on broker owning the topic", null, ServerError.MetadataError));
                return null;
            });
        } catch (URISyntaxException e) {
            lookupfuture.completeExceptionally(newStatusException(Status.INTERNAL,
                    "Invalid broker URL", null, ServerError.UnknownError));
        }
    }

    private static final Logger log = LoggerFactory.getLogger(TopicLookup.class);
}
