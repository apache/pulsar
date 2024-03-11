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

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.semconv.SemanticAttributes;
import java.io.IOException;
import java.util.List;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.core.Response;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.pulsar.broker.stats.PulsarBrokerOpenTelemetry;
import org.glassfish.jersey.server.internal.routing.UriRoutingContext;
import org.glassfish.jersey.server.model.ResourceMethod;
import org.glassfish.jersey.uri.UriTemplate;

public class RestEndpointMetricsFilter implements ContainerResponseFilter, ContainerRequestFilter {
    private static final String REQUEST_START_TIME = "requestStartTime";
    private static final AttributeKey<String> PATH = SemanticAttributes.URL_PATH;
    private static final AttributeKey<String> METHOD = SemanticAttributes.HTTP_REQUEST_METHOD;
    private static final AttributeKey<Long> CODE = SemanticAttributes.HTTP_RESPONSE_STATUS_CODE;

    private final DoubleHistogram latency;

    private RestEndpointMetricsFilter(PulsarBrokerOpenTelemetry openTelemetry) {
        Meter meter = openTelemetry.getMeter();
        latency = meter.histogramBuilder("pulsar_broker_rest_endpoint_latency")
                .setDescription("Latency of REST endpoints in Pulsar broker")
                .setUnit("ms")
                .setExplicitBucketBoundariesAdvice(List.of(10D, 20D, 50D, 100D, 200D, 500D, 1000D, 2000D))
                .build();
    }

    private static volatile RestEndpointMetricsFilter instance;

    public static synchronized RestEndpointMetricsFilter create(PulsarBrokerOpenTelemetry openTelemetry) {
        if (instance == null) {
            instance = new RestEndpointMetricsFilter(openTelemetry);
        }
        return instance;
    }

    @Override
    public void filter(ContainerRequestContext req, ContainerResponseContext resp) throws IOException {
        Response.StatusType status = resp.getStatusInfo();
        int statusCode = status.getStatusCode();
        Attributes attrs;
        try {
            UriRoutingContext info = (UriRoutingContext) req.getUriInfo();
            attrs = getRequestAttributes(info, statusCode);
        } catch (Throwable ex) {
            attrs = Attributes.of(PATH, "UNKNOWN", METHOD, req.getMethod(), CODE, (long) statusCode);
        }

        Object o = req.getProperty(REQUEST_START_TIME);
        if (o instanceof Long start) {
            long duration = System.currentTimeMillis() - start;
            this.latency.record(duration, attrs);
        }
    }

    @Override
    public void filter(ContainerRequestContext req) throws IOException {
        // Set the request start time into properties.
        req.setProperty(REQUEST_START_TIME, System.currentTimeMillis());
    }

    private static Attributes getRequestAttributes(UriRoutingContext ctx, long statusCode) {
        List<UriTemplate> templates = ctx.getMatchedTemplates();
        ResourceMethod method = ctx.getMatchedResourceMethod();
        String httpMethod = method == null ? "UNKNOWN" : method.getHttpMethod();
        if (CollectionUtils.isEmpty(templates)) {
            return Attributes.of(PATH, "UNKNOWN", METHOD, httpMethod, CODE, statusCode);
        }
        UriTemplate[] arr = templates.toArray(new UriTemplate[0]);
        int idx = arr.length - 1;
        StringBuilder builder = new StringBuilder();
        for (; idx >= 0; idx--) {
            builder.append(arr[idx].getTemplate());
        }
        String template = builder.toString().replace("{", ":").replace("}", "");
        return Attributes.of(PATH, template, METHOD, httpMethod, CODE, statusCode);
    }
}
