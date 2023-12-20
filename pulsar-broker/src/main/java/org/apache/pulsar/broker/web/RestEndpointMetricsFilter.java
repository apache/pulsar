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
package org.apache.pulsar.broker.web;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.prometheus.client.Counter;
import io.prometheus.client.Histogram;
import java.io.IOException;
import java.util.Stack;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.core.Response;
import org.glassfish.jersey.server.internal.routing.UriRoutingContext;
import org.glassfish.jersey.server.model.Resource;
import org.glassfish.jersey.server.model.ResourceMethod;
import org.jetbrains.annotations.NotNull;

public class RestEndpointMetricsFilter implements ContainerResponseFilter, ContainerRequestFilter {
    private static final LoadingCache<ResourceMethod, String> CACHE = CacheBuilder
            .newBuilder()
            .maximumSize(100)
            .build(new CacheLoader<>() {
                @Override
                public @NotNull String load(@NotNull ResourceMethod method) throws Exception {
                    return getRestPath(method);
                }
            });

    private static final Histogram LATENCY = Histogram
            .build("pulsar_broker_rest_endpoint_latency", "-")
            .unit("ms")
            .labelNames("path", "method")
            .buckets(10D, 20D, 50D, 100D, 200D, 500D, 1000D, 2000D)
            .register();
    private static final Counter FAILED = Counter
            .build("pulsar_broker_rest_endpoint_failed", "-")
            .labelNames("path", "method", "code")
            .register();

    private static final String REQUEST_START_TIME = "requestStartTime";

    @Override
    public void filter(ContainerRequestContext req, ContainerResponseContext resp) throws IOException {
        String path;
        try {
            UriRoutingContext info = (UriRoutingContext) req.getUriInfo();
            ResourceMethod rm = info.getMatchedResourceMethod();
            path = CACHE.get(rm);
        } catch (Throwable ex) {
            path = "UNKNOWN";
        }

        String method = req.getMethod();
        Response.StatusType status = resp.getStatusInfo();
        if (status.getStatusCode() < Response.Status.BAD_REQUEST.getStatusCode()) {
            long start = req.getProperty(REQUEST_START_TIME) == null
                    ? System.currentTimeMillis() : (long) req.getProperty(REQUEST_START_TIME);
            LATENCY.labels(path, method).observe(System.currentTimeMillis() - start);
        } else {
            FAILED.labels(path, method, String.valueOf(status.getStatusCode())).inc();
        }
    }

    @Override
    public void filter(ContainerRequestContext req) throws IOException {
        // Set the request start time into properties.
        req.setProperty(REQUEST_START_TIME, System.currentTimeMillis());
    }

    private static String getRestPath(ResourceMethod method) {
        try {
            StringBuilder fullPath = new StringBuilder();
            Stack<String> pathStack = new Stack<>();
            Resource parent = method.getParent();

            while (true) {
                String path = parent.getPath();
                parent = parent.getParent();
                if (parent == null) {
                    if (!path.endsWith("/") && !pathStack.peek().startsWith("/")) {
                        pathStack.push("/");
                    }
                    pathStack.push(path);
                    break;
                }
                pathStack.push(path);

            }
            while (!pathStack.isEmpty()) {
                fullPath.append(pathStack.pop().replace("{", ":").replace("}", ""));
            }
            return fullPath.toString();
        } catch (Exception ex) {
            return "UNKNOWN";
        }
    }
}
