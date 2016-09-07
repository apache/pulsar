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

import static com.google.common.base.Preconditions.checkNotNull;

import java.net.URI;
import java.net.URISyntaxException;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yahoo.pulsar.common.lookup.data.LookupData;
import com.yahoo.pulsar.common.naming.DestinationName;
import com.yahoo.pulsar.broker.web.NoSwaggerDocumentation;

@Path("/v2/destination/")
@NoSwaggerDocumentation
public class DestinationLookup extends LookupResource {

    @GET
    @Path("persistent/{property}/{cluster}/{namespace}/{dest}")
    @Produces(MediaType.APPLICATION_JSON)
    public LookupData lookupDestination(@PathParam("property") String property, @PathParam("cluster") String cluster,
            @PathParam("namespace") String namespace, @PathParam("dest") String dest,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) throws URISyntaxException {
        DestinationName fqdn = DestinationName.get("persistent", property, cluster, namespace, dest);
        return lookupFQDN(fqdn, authoritative);
    }

    private LookupData lookupFQDN(DestinationName fqdn, boolean authoritative) throws URISyntaxException {
        validateClusterOwnership(fqdn.getCluster());
        checkConnect(fqdn);
        validateReplicationSettingsOnNamespace(fqdn.getNamespaceObject());
        try {
            LookupResult result = pulsar().getNamespaceService().getBrokerServiceUrl(fqdn, authoritative);
            checkNotNull(result);
            if (result.isHttpRedirect()) {
                boolean newAuthoritative = this.isLeaderBroker();
                URI redirect = new URI(String.format("%s%s%s?authoritative=%s", result.getHttpRedirectAddress(),
                        "/lookup/v2/destination/", fqdn.getLookupName(), newAuthoritative));
                log.debug("Redirect lookup to {}", redirect);
                throw new WebApplicationException(Response.temporaryRedirect(redirect).build());
            } else {
                return result.getLookupData();
            }
        } catch (WebApplicationException wae) {
            throw wae;
        } catch (Exception e) { // expect only NPE but catching exception, okay?
            log.warn("lookupFQDN : Service busy, unable to lookup namespace [{}] Internal Exception Message:[{}]",
                    fqdn.toString(), e.getMessage(), e);
            throw new WebApplicationException(Response.Status.SERVICE_UNAVAILABLE);
        }
    }

    private static final Logger log = LoggerFactory.getLogger(DestinationLookup.class);
}
