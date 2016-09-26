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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yahoo.pulsar.broker.web.NoSwaggerDocumentation;
import com.yahoo.pulsar.broker.web.PulsarWebResource;
import com.yahoo.pulsar.common.naming.DestinationName;

@Path("/v2/destination/")
@NoSwaggerDocumentation
public class DestinationLookup extends PulsarWebResource {

    @GET
    @Path("persistent/{property}/{cluster}/{namespace}/{dest}")
    @Produces(MediaType.APPLICATION_JSON)
    public void lookupDestinationAsync(@PathParam("property") String property, @PathParam("cluster") String cluster,
            @PathParam("namespace") String namespace, @PathParam("dest") String dest,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
            @Suspended AsyncResponse asyncResponse) {

        DestinationName topic = DestinationName.get("persistent", property, cluster, namespace, dest);

        try {
            validateClusterOwnership(topic.getCluster());
            checkConnect(topic);
            validateReplicationSettingsOnNamespace(topic.getNamespaceObject());
        } catch (Throwable t) {
            // Validation checks failed
            log.error("Validation check failed: {}", t.getMessage());
            asyncResponse.resume(t);
            return;
        }

        CompletableFuture<LookupResult> lookupFuture = pulsar().getNamespaceService().getBrokerServiceUrlAsync(topic,
                authoritative);

        lookupFuture.thenAccept(result -> {
            if (result == null) {
                log.warn("No broker was found available for topic {}", topic);
                asyncResponse.resume(new WebApplicationException(Response.Status.SERVICE_UNAVAILABLE));
                return;
            }

            // We have found either a broker that owns the topic, or a broker to which we should redirect the client to
            if (result.isHttpRedirect()) {
                boolean newAuthoritative = this.isLeaderBroker();
                URI redirect;
                try {
                    redirect = new URI(String.format("%s%s%s?authoritative=%s", result.getHttpRedirectAddress(),
                            "/lookup/v2/destination/", topic.getLookupName(), newAuthoritative));
                } catch (URISyntaxException e) {
                    log.error("Error in preparing redirect url for {}: {}", topic, e.getMessage(), e);
                    asyncResponse.resume(e);
                    return;
                }

                if (log.isDebugEnabled()) {
                    log.debug("Redirect lookup for topic {} to {}", topic, redirect);
                }

                asyncResponse.resume(new WebApplicationException(Response.temporaryRedirect(redirect).build()));
            } else {
                // Found broker owning the topic
                if (log.isDebugEnabled()) {
                    log.debug("Lookup succeeded for topic {} -- broker: {}", topic, result.getLookupData());
                }
                asyncResponse.resume(result.getLookupData());
            }
        }).exceptionally(exception -> {
            log.warn("Failed to lookup broker for topic {}: {}", topic, exception.getMessage(), exception);
            asyncResponse.resume(exception);
            return null;
        });
    }

    private static final Logger log = LoggerFactory.getLogger(DestinationLookup.class);
}
