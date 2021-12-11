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

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import java.util.Timer;
import java.util.TimerTask;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.ServiceUrlProvider;

@Slf4j
public class ControlledClusterFailover implements ServiceUrlProvider {
    private PulsarClient pulsarClient;
    private volatile String currentPulsarServiceUrl;
    private final String defaultServiceUrl;
    private final URL pulsarUrlProvider;
    private final Timer timer;

    private ControlledClusterFailover(String defaultServiceUrl, String urlProvider) throws IOException {
        this.defaultServiceUrl = defaultServiceUrl;
        this.pulsarUrlProvider = new URL(urlProvider);
        this.timer = new Timer("pulsar-service-provider");
    }

    @Override
    public void initialize(PulsarClient client) {
        this.pulsarClient = client;
        this.currentPulsarServiceUrl = defaultServiceUrl;

        // start to check service url every 30 seconds
        this.timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                String newPulsarUrl = null;
                try {
                    newPulsarUrl = fetchServiceUrl();
                    if (!currentPulsarServiceUrl.equals(newPulsarUrl)) {
                        pulsarClient.updateServiceUrl(newPulsarUrl);
                        currentPulsarServiceUrl = newPulsarUrl;
                    }
                } catch (IOException e) {
                    log.error("Failed to switch new Pulsar URL, current: {}, new: {}",
                            currentPulsarServiceUrl, newPulsarUrl, e);
                }
            }
        }, 30_000, 30_000);
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
        this.timer.cancel();
    }

    public static class Builder{
        private String defaultServiceUrl;
        private String urlProvider;

        public Builder defaultServiceUrl(String defaultServiceUrl) {
            this.defaultServiceUrl = defaultServiceUrl;
            return this;
        }

        public Builder urlProvider(String urlProvider) {
            this.urlProvider = urlProvider;
            return this;
        }

        public ControlledClusterFailover build() throws IOException {
            return new ControlledClusterFailover(defaultServiceUrl, urlProvider);
        }
    }
}
