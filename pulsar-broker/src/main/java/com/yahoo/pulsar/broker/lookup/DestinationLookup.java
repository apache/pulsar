/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.pulsar.broker.lookup;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.CompletableFuture;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.Encoded;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yahoo.pulsar.broker.web.NoSwaggerDocumentation;
import com.yahoo.pulsar.broker.web.PulsarWebResource;
import com.yahoo.pulsar.common.naming.DestinationName;
import com.yahoo.pulsar.broker.PulsarService;
import com.yahoo.pulsar.broker.web.RestException;
import com.yahoo.pulsar.client.api.PulsarClientException;

import static com.yahoo.pulsar.common.api.Commands.newLookupResponse;
import com.yahoo.pulsar.common.api.proto.PulsarApi.CommandLookupTopicResponse.LookupType;
import com.yahoo.pulsar.common.api.proto.PulsarApi.ServerError;
import com.yahoo.pulsar.common.lookup.data.LookupData;
import com.yahoo.pulsar.common.policies.data.ClusterData;
import com.yahoo.pulsar.common.util.Codec;

import static com.google.common.base.Preconditions.checkNotNull;

import io.netty.buffer.ByteBuf;

@Path("/v2/destination/")
@NoSwaggerDocumentation
public class DestinationLookup extends PulsarWebResource {

    @GET
    @Path("persistent/{property}/{cluster}/{namespace}/{dest}")
    @Produces(MediaType.APPLICATION_JSON)
    public void lookupDestinationAsync(@PathParam("property") String property, @PathParam("cluster") String cluster,
            @PathParam("namespace") String namespace, @PathParam("dest") @Encoded String dest,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
            @Suspended AsyncResponse asyncResponse) {
        dest = Codec.decode(dest);
        DestinationName topic = DestinationName.get("persistent", property, cluster, namespace, dest);
        
        if (!pulsar().getBrokerService().getLookupRequestSemaphore().tryAcquire()) {
            log.warn("No broker was found available for topic {}", topic);
            asyncResponse.resume(new WebApplicationException(Response.Status.SERVICE_UNAVAILABLE));
            return;
        }

        try {
            validateClusterOwnership(topic.getCluster());
            checkConnect(topic);
            validateReplicationSettingsOnNamespace(pulsar(), topic.getNamespaceObject());
        } catch (WebApplicationException we) {
            // Validation checks failed
            log.error("Validation check failed: {}", we.getMessage());
            completeLookupResponseExceptionally(asyncResponse, we);
            return;
        } catch (Throwable t) {
            // Validation checks failed with unknown error
            log.error("Validation check failed: {}", t.getMessage(), t);
            completeLookupResponseExceptionally(asyncResponse, new RestException(t));
            return;
        }

        CompletableFuture<LookupResult> lookupFuture = pulsar().getNamespaceService().getBrokerServiceUrlAsync(topic,
                authoritative);

        lookupFuture.thenAccept(result -> {
            if (result == null) {
                log.warn("No broker was found available for topic {}", topic);
                completeLookupResponseExceptionally(asyncResponse, new WebApplicationException(Response.Status.SERVICE_UNAVAILABLE));
                return;
            }

            // We have found either a broker that owns the topic, or a broker to which we should redirect the client to
            if (result.isRedirect()) {
                boolean newAuthoritative = this.isLeaderBroker();
                URI redirect;
                try {
                    String redirectUrl = isRequestHttps() ? result.getLookupData().getHttpUrlTls()
                            : result.getLookupData().getHttpUrl();
                    redirect = new URI(String.format("%s%s%s?authoritative=%s", redirectUrl, "/lookup/v2/destination/",
                            topic.getLookupName(), newAuthoritative));
                } catch (URISyntaxException e) {
                    log.error("Error in preparing redirect url for {}: {}", topic, e.getMessage(), e);
                    completeLookupResponseExceptionally(asyncResponse, e);
                    return;
                }
                if (log.isDebugEnabled()) {
                    log.debug("Redirect lookup for topic {} to {}", topic, redirect);
                }
                completeLookupResponseExceptionally(asyncResponse, new WebApplicationException(Response.temporaryRedirect(redirect).build()));

            } else {
                // Found broker owning the topic
                if (log.isDebugEnabled()) {
                    log.debug("Lookup succeeded for topic {} -- broker: {}", topic, result.getLookupData());
                }
                completeLookupResponseSuccessfully(asyncResponse, result.getLookupData());
            }
        }).exceptionally(exception -> {
            log.warn("Failed to lookup broker for topic {}: {}", topic, exception.getMessage(), exception);
            completeLookupResponseExceptionally(asyncResponse, exception);
            return null;
        });

    }


    /**
     * 
     * Lookup broker-service address for a given namespace-bundle which contains given topic.
     * 
     * a. Returns broker-address if namespace-bundle is already owned by any broker
     * b. If current-broker receives lookup-request and if it's not a leader
     * then current broker redirects request to leader by returning leader-service address. 
     * c. If current-broker is leader then it finds out least-loaded broker to own namespace bundle and 
     * redirects request by returning least-loaded broker.
     * d. If current-broker receives request to own the namespace-bundle then it owns a bundle and returns 
     * success(connect) response to client.
     * 
     * @param pulsarService
     * @param fqdn
     * @param authoritative
     * @param clientAppId
     * @param requestId
     * @return
     */
    public static CompletableFuture<ByteBuf> lookupDestinationAsync(PulsarService pulsarService, DestinationName fqdn, boolean authoritative,
            String clientAppId, long requestId) {

        final CompletableFuture<ByteBuf> validationFuture = new CompletableFuture<>();
        final CompletableFuture<ByteBuf> lookupfuture = new CompletableFuture<>();
        final String cluster = fqdn.getCluster();

        // (1) validate cluster
        getClusterDataIfDifferentCluster(pulsarService, cluster, clientAppId).thenAccept(differentClusterData -> {

            if (differentClusterData != null) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Redirecting the lookup call to {}/{} cluster={}", clientAppId,
                            differentClusterData.getBrokerServiceUrl(), differentClusterData.getBrokerServiceUrlTls(), cluster);
                }
                validationFuture.complete(newLookupResponse(differentClusterData.getBrokerServiceUrl(),
                        differentClusterData.getBrokerServiceUrlTls(), true, LookupType.Redirect, requestId));
            } else {
                // (2) authorize client
                try {
                    checkAuthorization(pulsarService, fqdn, clientAppId);
                } catch (RestException authException) {
                    log.warn("Failed to authorized {} on cluster {}", clientAppId, fqdn.toString());
                    validationFuture.complete(
                            newLookupResponse(ServerError.AuthorizationError, authException.getMessage(), requestId));
                    return;
                } catch (Exception e) {
                    log.warn("Unknown error while authorizing {} on cluster {}", clientAppId, fqdn.toString());
                    validationFuture.completeExceptionally(e);
                    return;
                }
                // (3) validate global namespace
                validateReplicationSettingsOnNamespaceAsync(pulsarService, fqdn.getNamespaceObject())
                        .thenAccept(success -> {
                            // (4) all validation passed: initiate lookup
                            validationFuture.complete(null);
                        }).exceptionally(ex -> {
                            validationFuture
                                    .complete(newLookupResponse(ServerError.MetadataError, ex.getMessage(), requestId));
                            return null;
                        });
            }
        }).exceptionally(ex -> {
            validationFuture.completeExceptionally(ex);
            return null;
        });

        // Initiate lookup once validation completes
        validationFuture.thenAccept(validaitonFailureResponse -> {
            if (validaitonFailureResponse != null) {
                lookupfuture.complete(validaitonFailureResponse);
            } else {
                pulsarService.getNamespaceService().getBrokerServiceUrlAsync(fqdn, authoritative)
                        .thenAccept(lookupResult -> {

                            if (log.isDebugEnabled()) {
                                log.debug("[{}] Lookup result {}", fqdn.toString(), lookupResult);
                            }

                            LookupData lookupData = lookupResult.getLookupData();
                            if (lookupResult.isRedirect()) {
                                boolean newAuthoritative = isLeaderBroker(pulsarService);
                                lookupfuture.complete(
                                        newLookupResponse(lookupData.getBrokerUrl(), lookupData.getBrokerUrlTls(),
                                                newAuthoritative, LookupType.Redirect, requestId));
                            } else {
                                lookupfuture.complete(
                                        newLookupResponse(lookupData.getBrokerUrl(), lookupData.getBrokerUrlTls(),
                                                true /* authoritative */, LookupType.Connect, requestId));
                            }
                        }).exceptionally(e -> {
                            log.warn("Failed to lookup {} for topic {} with error {}", clientAppId, fqdn.toString(),
                                    e.getMessage(), e);
                            lookupfuture.complete(
                                    newLookupResponse(ServerError.ServiceNotReady, e.getMessage(), requestId));
                            return null;
                        });
            }

        }).exceptionally(ex -> {
            log.warn("Failed to lookup {} for topic {} with error {}", clientAppId, fqdn.toString(), ex.getMessage(),
                    ex);
            lookupfuture.complete(newLookupResponse(ServerError.ServiceNotReady, ex.getMessage(), requestId));
            return null;
        });

        return lookupfuture;
    }

    private void completeLookupResponseExceptionally(AsyncResponse asyncResponse, Throwable t) {
        pulsar().getBrokerService().getLookupRequestSemaphore().release();
        asyncResponse.resume(t);
    }

    private void completeLookupResponseSuccessfully(AsyncResponse asyncResponse, LookupData lookupData) {
        pulsar().getBrokerService().getLookupRequestSemaphore().release();
        asyncResponse.resume(lookupData);
    }
    
    private static final Logger log = LoggerFactory.getLogger(DestinationLookup.class);
}
