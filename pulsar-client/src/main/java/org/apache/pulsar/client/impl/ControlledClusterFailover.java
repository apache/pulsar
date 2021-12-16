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
import io.netty.util.concurrent.DefaultThreadFactory;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.pulsar.client.api.ControlledClusterFailoverBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.ServiceUrlProvider;

@Slf4j
@Data
public class ControlledClusterFailover implements ServiceUrlProvider {
    private PulsarClient pulsarClient;
    private volatile String currentPulsarServiceUrl;
    private final URL pulsarUrlProvider;
    private final ScheduledExecutorService executor;

    private ControlledClusterFailover(String defaultServiceUrl, String urlProvider) throws IOException {
        this.currentPulsarServiceUrl = defaultServiceUrl;
        this.pulsarUrlProvider = new URL(urlProvider);
        this.executor = Executors.newSingleThreadScheduledExecutor(
                new DefaultThreadFactory("pulsar-service-provider"));
    }

    @Override
    public void initialize(PulsarClient client) {
        this.pulsarClient = client;

        // start to check service url every 30 seconds
        this.executor.scheduleAtFixedRate(catchingAndLoggingThrowables(() -> {
            String newPulsarUrl = null;
            try {
                newPulsarUrl = fetchServiceUrl();
                if (!currentPulsarServiceUrl.equals(newPulsarUrl)) {
                    log.info("Switch Pulsar service url from {} to {}", currentPulsarServiceUrl, newPulsarUrl);
                    pulsarClient.updateServiceUrl(newPulsarUrl);
                    currentPulsarServiceUrl = newPulsarUrl;
                }
            } catch (IOException e) {
                log.error("Failed to switch new Pulsar URL, current: {}, new: {}",
                        currentPulsarServiceUrl, newPulsarUrl, e);
            }
        }), 30_000, 30_000, TimeUnit.MILLISECONDS);
    }

    private String fetchServiceUrl() throws IOException {
        // call the service to get service URL
        InputStream inputStream = null;
        try {
            URLConnection conn = pulsarUrlProvider.openConnection();
            inputStream = conn.getInputStream();
            return new String(IOUtils.toByteArray(inputStream), StandardCharsets.UTF_8);
        } finally {
            if (inputStream != null) {
                inputStream.close();
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
    }

    public static class ControlledClusterFailoverBuilderImpl implements ControlledClusterFailoverBuilder {
        private String defaultServiceUrl;
        private String urlProvider;

        public ControlledClusterFailoverBuilder defaultServiceUrl(String serviceUrl) {
            this.defaultServiceUrl = serviceUrl;
            return this;
        }

        public ControlledClusterFailoverBuilder urlProvider(String urlProvider) {
            this.urlProvider = urlProvider;
            return this;
        }

        public ServiceUrlProvider build() throws IOException {
            return new ControlledClusterFailover(defaultServiceUrl, urlProvider);
        }
    }

    public static ControlledClusterFailoverBuilder builder() {
        return new ControlledClusterFailoverBuilderImpl();
    }
}
