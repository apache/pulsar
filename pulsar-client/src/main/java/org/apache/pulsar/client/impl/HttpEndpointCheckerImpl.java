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
package org.apache.pulsar.client.impl;

import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.AsyncHttpClientConfig;
import org.asynchttpclient.DefaultAsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClientConfig;
import org.asynchttpclient.Request;
import org.asynchttpclient.channel.DefaultKeepAliveStrategy;

/**
 * Implementation of {@link EndpointChecker} that uses a binary protocol to check the health of endpoints.
 * check service url for {@link HttpLookupService}.
 */
@Slf4j
class HttpEndpointCheckerImpl implements EndpointChecker {
    private final AsyncHttpClient httpClient;
    private final int healthCheckTimeoutMs;

    HttpEndpointCheckerImpl(long healthCheckTimeoutMs) {
        this.healthCheckTimeoutMs = (int) healthCheckTimeoutMs;
        if (this.healthCheckTimeoutMs <= 0) {
            throw new IllegalArgumentException("Health check timeout must be greater than zero");
        }
        DefaultAsyncHttpClientConfig.Builder confBuilder = new DefaultAsyncHttpClientConfig.Builder();
        confBuilder.setConnectTimeout(this.healthCheckTimeoutMs);
        AsyncHttpClientConfig config = confBuilder.build();
        this.httpClient = new DefaultAsyncHttpClient(config);
    }

    @Override
    public boolean isHealthy(InetSocketAddress address) {
        if (address == null) {
            return false;
        }
        try {
            String url = String.format("http://%s:%d", address.getHostString(), address.getPort());
            httpClient.prepareConnect(url).execute().get(healthCheckTimeoutMs, TimeUnit.MILLISECONDS);
            return true;
        } catch (Exception e) {
            log.info("Health check failed for address {}: {}", address, e.getMessage());
            return false;
        }
    }
}
