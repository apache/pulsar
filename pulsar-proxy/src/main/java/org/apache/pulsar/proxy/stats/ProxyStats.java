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
package org.apache.pulsar.proxy.stats;

import static java.util.concurrent.TimeUnit.SECONDS;
import io.netty.channel.Channel;
import io.swagger.v3.oas.annotations.Hidden;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.servlet.ServletContext;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response.Status;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import lombok.CustomLog;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.authentication.AuthenticationParameters;
import org.apache.pulsar.broker.web.AuthenticationFilter;
import org.apache.pulsar.proxy.server.ProxyService;

@CustomLog
@Path("/")
@Tag(name = "proxy-stats", description = "Stats for proxy")
@Hidden
@Produces(MediaType.APPLICATION_JSON)
@SuppressWarnings("deprecation")
public class ProxyStats {
    public static final String ATTRIBUTE_PULSAR_PROXY_NAME = "pulsar-proxy";

    private ProxyService service;

    @Context
    protected ServletContext servletContext;
    @Context
    protected HttpServletRequest httpRequest;

    @GET
    @Path("/connections")
    @Operation(summary = "Proxy stats api to get info for live connections")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Proxy stats api to get info for live connections",
                    content = @Content(array = @ArraySchema(schema = @Schema(implementation = List.class)))),
            @ApiResponse(responseCode = "503", description = "Proxy service is not initialized") })
    public List<ConnectionStats> metrics() {
        throwIfNotSuperUser("metrics");
        List<ConnectionStats> stats = new ArrayList<>();
        proxyService().getClientCnxs().forEach(cnx -> {
            if (cnx.getDirectProxyHandler() == null) {
                return;
            }
            double requestRate = cnx.getDirectProxyHandler().getInboundChannelRequestsRate().getRate();
            double byteRate = cnx.getDirectProxyHandler().getInboundChannelRequestsRate().getValueRate();
            Channel inboundChannel = cnx.getDirectProxyHandler().getInboundChannel();
            Channel outboundChannel = cnx.getDirectProxyHandler().getOutboundChannel();
            stats.add(new ConnectionStats(requestRate, byteRate, inboundChannel, outboundChannel));
        });
        return stats;
    }

    @GET
    @Path("/topics")
    @Operation(summary = "Proxy topic stats api")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Proxy topic stats api",
                    content = @Content(schema = @Schema(type = "object"),
                            additionalPropertiesSchema = @Schema(implementation = Map.class))),
            @ApiResponse(responseCode = "412", description = "Proxy logging should be > 2 to capture topic stats"),
            @ApiResponse(responseCode = "503", description = "Proxy service is not initialized") })
    public Map<String, TopicStats> topics() {
        throwIfNotSuperUser("topics");
        Optional<Integer> logLevel = proxyService().getConfiguration().getProxyLogLevel();
        if (!logLevel.isPresent() || logLevel.get() < 2) {
            throw new RestException(Status.PRECONDITION_FAILED, "Proxy doesn't have logging level 2");
        }
        return proxyService().getTopicStats();
    }

    @POST
    @Path("/logging/{logLevel}")
    @Operation(hidden = true, summary = "Change proxy logging level dynamically",
            description = "It only changes the log level in memory; change it in the config file to persist the change")
    @ApiResponses(value = { @ApiResponse(responseCode = "412", description = "Proxy log level can be [0-2]"), })
    public void updateProxyLogLevel(@PathParam("logLevel") int logLevel) {
        throwIfNotSuperUser("updateProxyLogLevel");
        if (logLevel < 0 || logLevel > 2) {
            throw new RestException(Status.PRECONDITION_FAILED, "Proxy log level can be only [0-2]");
        }
        proxyService().setProxyLogLevel(logLevel);
    }

    @GET
    @Path("/logging")
    @Operation(hidden = true, summary = "Get proxy logging")
    public int getProxyLogLevel(@PathParam("logLevel") int logLevel) {
        throwIfNotSuperUser("getProxyLogLevel");
        return proxyService().getProxyLogLevel();
    }

    protected ProxyService proxyService() {
        if (service == null) {
            service = (ProxyService) servletContext.getAttribute(ATTRIBUTE_PULSAR_PROXY_NAME);
            if (service == null) {
                throw new RestException(Status.SERVICE_UNAVAILABLE, "Proxy service is not initialized");
            }
        }
        return service;
    }

    private void throwIfNotSuperUser(String action) {
        if (proxyService().getConfiguration().isAuthorizationEnabled()) {
            AuthenticationParameters authParams = AuthenticationParameters.builder()
                    .clientRole((String) httpRequest.getAttribute(AuthenticationFilter.AuthenticatedRoleAttributeName))
                    .clientAuthenticationDataSource((AuthenticationDataSource)
                            httpRequest.getAttribute(AuthenticationFilter.AuthenticatedDataAttributeName))
                    .build();
            try {
                if (authParams.getClientRole() == null
                        || !proxyService().getAuthorizationService().isSuperUser(authParams).get(30, SECONDS)) {
                    log.error()
                            .attr("authParams", authParams.getClientRole())
                            .attr("action", action)
                            .log("Client with role [] is not authorized to");
                    throw new org.apache.pulsar.common.util.RestException(Status.UNAUTHORIZED,
                            "Client is not authorized to perform operation");
                }
            } catch (ExecutionException | TimeoutException | InterruptedException e) {
                log.warn()
                        .attr("timeoutSeconds", 30)
                        .attr("authParams", authParams.getClientRole())
                        .log("Time-out while checking the role is a super user role");
                throw new org.apache.pulsar.common.util.RestException(Status.INTERNAL_SERVER_ERROR, e.getMessage());
            }
        }
    }
}
