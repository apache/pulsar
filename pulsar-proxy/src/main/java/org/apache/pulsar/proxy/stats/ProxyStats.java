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
package org.apache.pulsar.proxy.stats;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.servlet.ServletContext;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response.Status;

import org.apache.pulsar.proxy.server.ProxyService;

import com.google.common.collect.Lists;

import io.netty.channel.Channel;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

@Path("/")
@Api(value = "/proxy-stats", description = "Stats for proxy", tags = "proxy-stats", hidden = true)
@Produces(MediaType.APPLICATION_JSON)
public class ProxyStats {

    public static final String ATTRIBUTE_PULSAR_PROXY_NAME = "pulsar-proxy";

    private ProxyService service;

    @Context
    protected ServletContext servletContext;

    @GET
    @Path("/connections")
    @ApiOperation(value = "Proxy stats api to get info for live connections", response = List.class, responseContainer = "List")
    @ApiResponses(value = { @ApiResponse(code = 503, message = "Proxy service is not initialized") })
    public List<ConnectionStats> metrics() {
        List<ConnectionStats> stats = Lists.newArrayList();
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
    @ApiOperation(value = "Proxy topic stats api", response = Map.class, responseContainer = "Map")
    @ApiResponses(value = { @ApiResponse(code = 412, message = "Proxy logging should be > 2 to capture topic stats"),
            @ApiResponse(code = 503, message = "Proxy service is not initialized") })
    public Map<String, TopicStats> topics() {

        Optional<Integer> logLevel = proxyService().getConfiguration().getProxyLogLevel();
        if (!logLevel.isPresent() || logLevel.get() < 2) {
            throw new RestException(Status.PRECONDITION_FAILED, "Proxy doesn't have logging level 2");
        }
        return proxyService().getTopicStats();
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
}
