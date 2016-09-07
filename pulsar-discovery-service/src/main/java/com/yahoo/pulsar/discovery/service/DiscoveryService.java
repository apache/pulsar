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
package com.yahoo.pulsar.discovery.service;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.System.getProperty;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.commons.collections.CollectionUtils;
import org.glassfish.jersey.server.ContainerRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.wordnik.swagger.annotations.ApiOperation;

/**
 * Acts a load-balancer that receives any incoming request and discover active-available broker in round-robin manner
 * and redirect request to that broker.
 * <p>
 * Accepts any {@value GET, PUT, POST} request and redirects to available broker-server to serve the request
 * </p>
 *
 */
@Singleton
@Path("{apiPath:.*}")
@Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.APPLICATION_OCTET_STREAM,
        MediaType.MULTIPART_FORM_DATA })
@Consumes({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.APPLICATION_OCTET_STREAM,
        MediaType.MULTIPART_FORM_DATA })
public class DiscoveryService {

    public static final String LOADBALANCE_BROKERS_ROOT = "/loadbalance/brokers";
    private static final AtomicInteger count = new AtomicInteger();
    private static final ZookeeperCacheLoader zkCache = getZookeeperCacheLoader();

    @POST
    @ApiOperation(value = "Redirect POST request to broker")
    public Response post(@Context ContainerRequest headers) {
        return redirect(headers);
    }

    @GET
    @ApiOperation(value = "Redirect GET request to broker")
    public Response get(@Context ContainerRequest headers) throws IOException {
        return redirect(headers);
    }

    @PUT
    @ApiOperation(value = "Redirect PUT request to broker")
    public Response put(@Context ContainerRequest headers) {
        return redirect(headers);
    }

    /**
     * redirect request to given direct http uri path
     * 
     * @param path
     * @return
     */
    protected Response redirect(ContainerRequest headers) {
        checkNotNull(headers);
        String scheme = headers.getBaseUri().getScheme();
        String path = headers.getPath(false);
        URI location;
        try {
            String url = (new StringBuilder(scheme)).append("://").append(nextBroker()).append("/").append(path)
                    .toString();
            location = new URI(url);
        } catch (URISyntaxException e) {
            log.warn("No broker found in zookeeper {}", e.getMessage(), e);
            throw new RestException(Status.SERVICE_UNAVAILABLE, "Broker is not available");
        }
        return Response.temporaryRedirect(location).build();
    }

    /**
     * Find next broke url in round-robin
     * 
     * @return
     */
    public String nextBroker() {
        if (!CollectionUtils.isEmpty(availableActiveBrokers())) {
            int next = count.getAndIncrement() % availableActiveBrokers().size();
            return availableActiveBrokers().get(next);
        } else {
            throw new RestException(Status.SERVICE_UNAVAILABLE, "No active broker is available");
        }
    }

    public List<String> availableActiveBrokers() {
        return zkCache.getAvailableActiveBrokers();
    }

    /**
     * initialize {@link ZookeeperCacheLoader} instance by creating ZooKeeper connection and broker connection
     * 
     * @return {@link ZookeeperCacheLoader} to fetch available broker list
     */
    private static ZookeeperCacheLoader getZookeeperCacheLoader() {
        String zookeeperServers = checkNotNull(getProperty("zookeeperServers"), "zookeeperServers property not set");
        ZookeeperCacheLoader zkCacheLoader;
        try {
            zkCacheLoader = new ZookeeperCacheLoader(zookeeperServers);
        } catch (InterruptedException e) {
            log.warn("Failed to fetch broker list from ZooKeeper, {}", e.getMessage(), e);
            throw new RestException(Status.INTERNAL_SERVER_ERROR, e.getMessage());
        } catch (IOException e) {
            log.warn("Failed to create ZooKeeper session, {}", e.getMessage(), e);
            throw new RestException(Status.INTERNAL_SERVER_ERROR, e.getMessage());
        }
        return zkCacheLoader;
    }

    private static final Logger log = LoggerFactory.getLogger(DiscoveryService.class);
}
