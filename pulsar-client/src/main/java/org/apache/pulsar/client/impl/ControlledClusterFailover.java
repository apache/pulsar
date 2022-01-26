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
package org.apache.pulsar.client.impl;

import static org.apache.pulsar.common.util.Runnables.catchingAndLoggingThrowables;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.Data;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.ControlledClusterFailoverBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.ServiceUrlProvider;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.checkerframework.checker.nullness.qual.Nullable;

@Slf4j
public class ControlledClusterFailover implements ServiceUrlProvider {
    private PulsarClientImpl pulsarClient;
    private volatile String currentPulsarServiceUrl;
    private volatile ControlledConfiguration currentControlledConfiguration;
    private final ScheduledExecutorService executor;
    private final long interval;
    private ObjectMapper objectMapper = null;
    private final CloseableHttpClient httpClient;
    private final HttpUriRequest request;
    private final ResponseHandler<String> responseHandler;

    private ControlledClusterFailover(ControlledClusterFailoverBuilderImpl builder) throws IOException {
        this.currentPulsarServiceUrl = builder.defaultServiceUrl;
        this.interval = builder.interval;
        this.executor = Executors.newSingleThreadScheduledExecutor(
                new DefaultThreadFactory("pulsar-service-provider"));
        this.httpClient = HttpClients.custom().build();

        RequestBuilder requestBuilder = RequestBuilder.get()
            .setUri(builder.urlProvider)
            .setHeader(HttpHeaders.CONTENT_TYPE, "application/json");

        if (builder.header != null && !builder.header.isEmpty()) {
            builder.header.forEach(requestBuilder::setHeader);
        }
        this.request = requestBuilder.build();
        responseHandler = httpResponse -> {
            int status = httpResponse.getStatusLine().getStatusCode();
            if (status >= 200 && status < 300) {
                HttpEntity entity = httpResponse.getEntity();
                return entity != null ? EntityUtils.toString(entity) : null;
            } else {
                log.warn("Unexpected response status: {}", status);
                return null;
            }
        };
    }

    public HttpUriRequest getRequest() {
        return this.request;
    }

    @Override
    public void initialize(PulsarClient client) {
        this.pulsarClient = (PulsarClientImpl) client;

        // start to check service url every 30 seconds
        this.executor.scheduleAtFixedRate(catchingAndLoggingThrowables(() -> {
            ControlledConfiguration controlledConfiguration = null;
            try {
                controlledConfiguration = fetchControlledConfiguration();
                if (controlledConfiguration != null
                        && !Strings.isNullOrEmpty(controlledConfiguration.getServiceUrl())
                        && !controlledConfiguration.equals(currentControlledConfiguration)) {
                    log.info("Switch Pulsar service url from {} to {}",
                            currentControlledConfiguration, controlledConfiguration.toString());

                    Authentication authentication = null;
                    if (!Strings.isNullOrEmpty(controlledConfiguration.authPluginClassName)
                            && !Strings.isNullOrEmpty(controlledConfiguration.getAuthParamsString())) {
                        authentication = AuthenticationFactory.create(controlledConfiguration.getAuthPluginClassName(),
                                controlledConfiguration.getAuthParamsString());
                    }

                    String tlsTrustCertsFilePath = controlledConfiguration.getTlsTrustCertsFilePath();
                    String serviceUrl = controlledConfiguration.getServiceUrl();

                    if (authentication != null) {
                        pulsarClient.updateAuthentication(authentication);
                    }

                    if (!Strings.isNullOrEmpty(tlsTrustCertsFilePath)) {
                        pulsarClient.updateTlsTrustCertsFilePath(tlsTrustCertsFilePath);
                    }

                    pulsarClient.updateServiceUrl(serviceUrl);
                    currentPulsarServiceUrl = serviceUrl;
                    currentControlledConfiguration = controlledConfiguration;
                }
            } catch (IOException e) {
                log.error("Failed to switch new Pulsar url, current: {}, new: {}",
                        currentControlledConfiguration, controlledConfiguration, e);
            }
        }), interval, interval, TimeUnit.MILLISECONDS);
    }

    public String getCurrentPulsarServiceUrl() {
        return this.currentPulsarServiceUrl;
    }

    protected ControlledConfiguration fetchControlledConfiguration() throws IOException {
        // call the service to get service URL
        try {
            String jsonStr = httpClient.execute(request, responseHandler);
            return getObjectMapper().readValue(jsonStr, ControlledConfiguration.class);
        } catch (IOException e) {
            log.warn("Failed to fetch controlled configuration. ", e);
            return null;
        }
    }

    private ObjectMapper getObjectMapper() {
        if (objectMapper == null) {
            objectMapper = new ObjectMapper();
        }
        return objectMapper;
    }

    @Data
    protected static class ControlledConfiguration {
        private String serviceUrl;
        private String tlsTrustCertsFilePath;

        private String authPluginClassName;
        private String authParamsString;

        public String toJson() {
            ObjectMapper objectMapper = ObjectMapperFactory.getThreadLocal();
            try {
                return objectMapper.writeValueAsString(this);
            } catch (JsonProcessingException e) {
                log.warn("Failed to write as json. ", e);
                return null;
            }
        }
    }

    @Override
    public String getServiceUrl() {
        return this.currentPulsarServiceUrl;
    }

    @Override
    public void close() {
        this.executor.shutdown();
        if (httpClient != null) {
            try {
                httpClient.close();
            } catch (IOException e) {
                log.error("Failed to close http client.");
            }
        }
    }

    public static class ControlledClusterFailoverBuilderImpl implements ControlledClusterFailoverBuilder {
        private String defaultServiceUrl;
        private String urlProvider;
        private Map<String, String> header = null;
        private long interval = 30_000;

        @Override
        public ControlledClusterFailoverBuilder defaultServiceUrl(@NonNull String serviceUrl) {
            this.defaultServiceUrl = serviceUrl;
            return this;
        }

        @Override
        public ControlledClusterFailoverBuilder urlProvider(@NonNull String urlProvider) {
            this.urlProvider = urlProvider;
            return this;
        }

        @Override
        public ControlledClusterFailoverBuilder urlProviderHeader(Map<String, String> header) {
            this.header = header;
            return this;
        }

        @Override
        public ControlledClusterFailoverBuilder checkInterval(long interval, @NonNull TimeUnit timeUnit) {
            this.interval = timeUnit.toMillis(interval);
            return this;
        }

        @Override
        public ServiceUrlProvider build() throws IOException {
            Objects.requireNonNull(defaultServiceUrl, "default service url shouldn't be null");
            Objects.requireNonNull(urlProvider, "urlProvider shouldn't be null");
            checkArgument(interval > 0, "checkInterval should > 0");

            return new ControlledClusterFailover(this);
        }

        public static void checkArgument(boolean expression, @Nullable Object errorMessage) {
            if (!expression) {
                throw new IllegalArgumentException(String.valueOf(errorMessage));
            }
        }
    }

    public static ControlledClusterFailoverBuilder builder() {
        return new ControlledClusterFailoverBuilderImpl();
    }
}
