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
package org.apache.pulsar.websocket.admin;

import static org.apache.pulsar.common.util.Codec.decode;

import java.util.Collection;
import java.util.Map;

import javax.ws.rs.Encoded;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response.Status;

import org.apache.pulsar.common.naming.DestinationName;
import org.apache.pulsar.common.stats.Metrics;
import org.apache.pulsar.websocket.stats.ProxyTopicStat;
import org.apache.pulsar.websocket.stats.ProxyTopicStat.ConsumerStats;
import org.apache.pulsar.websocket.stats.ProxyTopicStat.ProducerStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import jersey.repackaged.com.google.common.collect.Maps;

@Path("/proxy-stats")
@Api(value = "/proxy", description = "Stats for web-socket proxy", tags = "proxy-stats")
@Produces(MediaType.APPLICATION_JSON)
public class WebSocketProxyStats extends WebSocketWebResource {
    private static final Logger LOG = LoggerFactory.getLogger(WebSocketProxyStats.class);

    @GET
    @Path("/metrics")
    @ApiOperation(value = "Gets the metrics for Monitoring", notes = "Requested should be executed by Monitoring agent on each proxy to fetch the metrics", response = Metrics.class, responseContainer = "List")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission") })
    public Collection<Metrics> getMetrics() throws Exception {
        // Ensure super user access only
        validateSuperUserAccess();
        try {
            return service().getProxyStats().getMetrics();
        } catch (Exception e) {
            LOG.error("[{}] Failed to generate metrics", clientAppId(), e);
            throw new RestException(e);
        }
    }

    @GET
    @Path("/{property}/{cluster}/{namespace}/{destination}/stats")
    @ApiOperation(value = "Get the stats for the topic.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic does not exist") })
    public ProxyTopicStat getStats(@PathParam("property") String property, @PathParam("cluster") String cluster,
            @PathParam("namespace") String namespace, @PathParam("destination") @Encoded String destination) {
        destination = decode(destination);
        DestinationName dn = DestinationName.get("persistent", property, cluster, namespace, destination);
        validateUserAccess(dn);
        ProxyTopicStat stats = getStat(dn.toString());
        if (stats == null) {
            throw new RestException(Status.NOT_FOUND, "Topic does not exist");
        }
        return stats;
    }

    @GET
    @Path("/stats")
    @ApiOperation(value = "Get the stats for the topic.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission") })
    public Map<String, ProxyTopicStat> getProxyStats() {
        validateSuperUserAccess();
        return getStat();
    }

    public ProxyTopicStat getStat(String topicName) {

        if (!service().getProducers().containsKey(topicName) && !service().getConsumers().containsKey(topicName)) {
            LOG.warn("topic doesn't exist {}", topicName);
            throw new RestException(Status.NOT_FOUND, "Topic does not exist");
        }
        ProxyTopicStat topicStat = new ProxyTopicStat();
        service().getProducers().get(topicName).forEach(handler -> {
            ProducerStats stat = new ProducerStats(handler);
            topicStat.producerStats.add(stat);

        });

        service().getConsumers().get(topicName).forEach(handler -> {
            topicStat.consumerStats.add(new ConsumerStats(handler));
        });
        return topicStat;
    }

    public Map<String, ProxyTopicStat> getStat() {

        Map<String, ProxyTopicStat> statMap = Maps.newHashMap();

        service().getProducers().forEach((topicName, handlers) -> {
            ProxyTopicStat topicStat = statMap.computeIfAbsent(topicName, t -> new ProxyTopicStat());
            handlers.forEach(handler -> topicStat.producerStats.add(new ProducerStats(handler)));
            statMap.put(topicName, topicStat);
        });
        service().getConsumers().forEach((topicName, handlers) -> {
            ProxyTopicStat topicStat = statMap.computeIfAbsent(topicName, t -> new ProxyTopicStat());
            handlers.forEach(handler -> topicStat.consumerStats.add(new ConsumerStats(handler)));
            statMap.put(topicName, topicStat);
        });

        return statMap;
    }
}
