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
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.Data;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.ControlledClusterFailoverBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.ServiceUrlProvider;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.checkerframework.checker.nullness.qual.Nullable;

@Slf4j
public class ControlledClusterFailover implements ServiceUrlProvider {
    private PulsarClient pulsarClient;
    private volatile String currentPulsarServiceUrl;
    private volatile ControlledConfiguration currentControlledConfiguration;
    private final URL pulsarUrlProvider;
    private final ScheduledExecutorService executor;
    private long interval;
    private ObjectMapper objectMapper = null;

    private ControlledClusterFailover(String defaultServiceUrl, String urlProvider, long interval) throws IOException {
        this.currentPulsarServiceUrl = defaultServiceUrl;
        this.pulsarUrlProvider = new URL(urlProvider);
        this.interval = interval;
        this.executor = Executors.newSingleThreadScheduledExecutor(
                new DefaultThreadFactory("pulsar-service-provider"));
    }

    @Override
    public void initialize(PulsarClient client) {
        this.pulsarClient = client;

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

    public URL getPulsarUrlProvider() {
        return this.pulsarUrlProvider;
    }

    protected ControlledConfiguration fetchControlledConfiguration() throws IOException {
        // call the service to get service URL
        InputStream inputStream = null;
        try {
            URLConnection conn = pulsarUrlProvider.openConnection();
            inputStream = conn.getInputStream();
            String jsonStr = new String(IOUtils.toByteArray(inputStream), StandardCharsets.UTF_8);
            ObjectMapper objectMapper = getObjectMapper();
            return objectMapper.readValue(jsonStr, ControlledConfiguration.class);
        } catch (IOException e) {
            log.warn("Failed to fetch controlled configuration. ", e);
            return null;
        } finally {
            if (inputStream != null) {
                inputStream.close();
            }
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

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof ControlledConfiguration) {
                ControlledConfiguration other = (ControlledConfiguration) obj;
                return Objects.equals(serviceUrl, other.serviceUrl)
                        && Objects.equals(tlsTrustCertsFilePath, other.tlsTrustCertsFilePath)
                        && Objects.equals(authPluginClassName, other.authPluginClassName)
                        && Objects.equals(authParamsString, other.authParamsString);
            }

            return false;
        }

        @Override
        public int hashCode() {
            return Objects.hash(serviceUrl,
                    tlsTrustCertsFilePath,
                    authPluginClassName,
                    authParamsString);
        }
    }

    @Override
    public String getServiceUrl() {
        return this.currentPulsarServiceUrl;
    }

    @Override
    public void close() {
        this.executor.shutdown();
    }

    public static class ControlledClusterFailoverBuilderImpl implements ControlledClusterFailoverBuilder {
        private String defaultServiceUrl;
        private String urlProvider;
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
        public ControlledClusterFailoverBuilder checkInterval(long interval, @NonNull TimeUnit timeUnit) {
            this.interval = timeUnit.toMillis(interval);
            return this;
        }

        @Override
        public ServiceUrlProvider build() throws IOException {
            Objects.requireNonNull(defaultServiceUrl, "default service url shouldn't be null");
            Objects.requireNonNull(urlProvider, "urlProvider shouldn't be null");
            checkArgument(interval >= 0, "checkInterval should >= 0");

            return new ControlledClusterFailover(defaultServiceUrl, urlProvider, interval);
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
