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
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Stack;
import javax.validation.constraints.NotNull;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.core.Response;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.stats.PulsarBrokerOpenTelemetry;
import org.glassfish.jersey.server.internal.routing.UriRoutingContext;
import org.glassfish.jersey.server.model.Resource;
import org.glassfish.jersey.server.model.ResourceMethod;

public class RestEndpointMetricsFilter implements ContainerResponseFilter, ContainerRequestFilter {
    private final LoadingCache<ResourceMethod, String> cache = CacheBuilder
            .newBuilder()
            .maximumSize(100)
            .expireAfterAccess(Duration.ofMinutes(1))
            .build(new CacheLoader<>() {
                @Override
                public @NotNull String load(@NotNull ResourceMethod method) throws Exception {
                    return getRestPath(method);
                }
            });

    private static final String REQUEST_START_TIME = "requestStartTime";
    private static final AttributeKey<String> PATH = AttributeKey.stringKey("path");
    private static final AttributeKey<String> METHOD = AttributeKey.stringKey("method");
    private static final AttributeKey<String> CODE = AttributeKey.stringKey("code");

    private final DoubleHistogram latency;
    private final LongCounter failed;

    private RestEndpointMetricsFilter(PulsarService pulsar) {
        PulsarBrokerOpenTelemetry telemetry = pulsar.getOpenTelemetry();
        Meter meter = telemetry.getMeter();
        latency = meter.histogramBuilder("pulsar_broker_rest_endpoint_latency")
                .setDescription("-")
                .setUnit("ms")
                .setExplicitBucketBoundariesAdvice(List.of(10D, 20D, 50D, 100D, 200D, 500D, 1000D, 2000D))
                .build();
        failed = meter.counterBuilder("pulsar_broker_rest_endpoint_failed")
                .setDescription("-")
                .build();
    }

    private static volatile RestEndpointMetricsFilter instance;

    public static synchronized RestEndpointMetricsFilter create(PulsarService pulsar) {
            if (instance == null) {
                instance = new RestEndpointMetricsFilter(pulsar);
            }
            return instance;
    }

    @Override
    public void filter(ContainerRequestContext req, ContainerResponseContext resp) throws IOException {
        String path;
        try {
            UriRoutingContext info = (UriRoutingContext) req.getUriInfo();
            ResourceMethod rm = info.getMatchedResourceMethod();
            path = cache.get(rm);
        } catch (Throwable ex) {
            path = "UNKNOWN";
        }

        String method = req.getMethod();
        Response.StatusType status = resp.getStatusInfo();
        // record failure
        if (status.getStatusCode() >= Response.Status.BAD_REQUEST.getStatusCode()) {
            recordFailure(path, method, status.getStatusCode());
            return;
        }
        // record success
        Object o = req.getProperty(REQUEST_START_TIME);
        if (o instanceof Long start) {
            recordSuccess(path, method, System.currentTimeMillis() - start);
        }
    }

    @Override
    public void filter(ContainerRequestContext req) throws IOException {
        // Set the request start time into properties.
        req.setProperty(REQUEST_START_TIME, System.currentTimeMillis());
    }


    private void recordSuccess(String path, String method, long duration) {
        Attributes attributes = Attributes.of(PATH, path, METHOD, method);
        latency.record(duration, attributes);
    }

    private void recordFailure(String path, String method, int code) {
        Attributes attributes = Attributes.of(PATH, path, METHOD, method, CODE, String.valueOf(code));
        failed.add(1, attributes);
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
