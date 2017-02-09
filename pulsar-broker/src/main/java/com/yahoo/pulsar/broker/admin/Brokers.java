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
package com.yahoo.pulsar.broker.admin;

import java.util.Map;
import java.util.Set;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import com.yahoo.pulsar.broker.loadbalance.impl.SimpleLoadManagerImpl;
import com.yahoo.pulsar.broker.web.RestException;
import com.yahoo.pulsar.common.policies.data.NamespaceOwnershipStatus;

@Path("/brokers")
@Api(value = "/brokers", description = "Brokers admin apis", tags = "brokers")
@Produces(MediaType.APPLICATION_JSON)
public class Brokers extends AdminResource {
    private static final Logger LOG = LoggerFactory.getLogger(Brokers.class);

    @GET
    @Path("/{cluster}")
    @ApiOperation(value = "Get the list of active brokers (web service addresses) in the cluster.", response = String.class, responseContainer = "Set")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Cluster doesn't exist") })
    public Set<String> getActiveBrokers(@PathParam("cluster") String cluster) throws Exception {
        validateSuperUserAccess();
        validateClusterOwnership(cluster);

        try {
            // Add Native brokers
            return pulsar().getLoadManager().getActiveBrokers();
        } catch (Exception e) {
            LOG.error(String.format("[%s] Failed to get active broker list: cluster=%s", clientAppId(), cluster), e);
            throw new RestException(e);
        }
    }

    @GET
    @Path("/{cluster}/{broker}/ownedNamespaces")
    @ApiOperation(value = "Get the list of namespaces served by the specific broker", response = NamespaceOwnershipStatus.class, responseContainer = "Map")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Cluster doesn't exist") })
    public Map<String, NamespaceOwnershipStatus> getOwnedNamespaes(@PathParam("cluster") String cluster,
            @PathParam("broker") String broker) throws Exception {
        validateSuperUserAccess();
        validateClusterOwnership(cluster);
        validateBrokerName(broker);

        try {
            // now we validated that this is the broker specified in the request
            return pulsar().getNamespaceService().getOwnedNameSpacesStatus();
        } catch (Exception e) {
            LOG.error("[{}] Failed to get the namespace ownership status. cluster={}, broker={}", clientAppId(),
                    cluster, broker);
            throw new RestException(e);
        }
    }
}
