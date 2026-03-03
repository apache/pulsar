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
package org.apache.pulsar.client.impl.auth.oauth2;

import io.netty.handler.ssl.SslContextBuilder;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.util.Map;
import javax.net.ssl.SSLException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.PulsarVersion;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.auth.oauth2.protocol.DefaultMetadataResolver;
import org.apache.pulsar.client.impl.auth.oauth2.protocol.Metadata;
import org.apache.pulsar.client.impl.auth.oauth2.protocol.MetadataResolver;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClientConfig;

/**
 * An abstract OAuth 2.0 authorization flow.
 */
@Slf4j
abstract class FlowBase implements Flow {

    public static final String CONFIG_PARAM_CONNECT_TIMEOUT = "connectTimeout";
    public static final String CONFIG_PARAM_READ_TIMEOUT = "readTimeout";
    public static final String CONFIG_PARAM_TRUST_CERTS_FILE_PATH = "trustCertsFilePath";
    public static final String CONFIG_PARAM_WELL_KNOWN_METADATA_PATH = "wellKnownMetadataPath";

    protected static final Duration DEFAULT_CONNECT_TIMEOUT = Duration.ofSeconds(10);
    protected static final Duration DEFAULT_READ_TIMEOUT = Duration.ofSeconds(30);

    private static final long serialVersionUID = 1L;

    protected final URL issuerUrl;
    protected final AsyncHttpClient httpClient;
    protected final String wellKnownMetadataPath;

    protected transient Metadata metadata;

    protected FlowBase(URL issuerUrl, Duration connectTimeout, Duration readTimeout, String trustCertsFilePath,
                       String wellKnownMetadataPath) {
        this.issuerUrl = issuerUrl;
        this.httpClient = defaultHttpClient(readTimeout, connectTimeout, trustCertsFilePath);
        this.wellKnownMetadataPath = wellKnownMetadataPath;
    }

    private AsyncHttpClient defaultHttpClient(Duration readTimeout, Duration connectTimeout,
                                              String trustCertsFilePath) {
        DefaultAsyncHttpClientConfig.Builder confBuilder = new DefaultAsyncHttpClientConfig.Builder();
        confBuilder.setCookieStore(null);
        confBuilder.setUseProxyProperties(true);
        confBuilder.setFollowRedirect(true);
        confBuilder.setConnectTimeout(
                getParameterDurationToMillis(CONFIG_PARAM_CONNECT_TIMEOUT, connectTimeout,
                        DEFAULT_CONNECT_TIMEOUT));
        confBuilder.setReadTimeout(
                getParameterDurationToMillis(CONFIG_PARAM_READ_TIMEOUT, readTimeout, DEFAULT_READ_TIMEOUT));
        confBuilder.setUserAgent(String.format("Pulsar-Java-v%s", PulsarVersion.getVersion()));
        if (StringUtils.isNotBlank(trustCertsFilePath)) {
            try {
                confBuilder.setSslContext(SslContextBuilder.forClient()
                        .trustManager(new File(trustCertsFilePath))
                        .build());
            } catch (SSLException e) {
                log.error("Could not set " + CONFIG_PARAM_TRUST_CERTS_FILE_PATH, e);
            }
        }
        return new DefaultAsyncHttpClient(confBuilder.build());
    }

    private int getParameterDurationToMillis(String name, Duration value, Duration defaultValue) {
        Duration duration;
        if (value == null) {
            if (log.isDebugEnabled()) {
                log.debug("Configuration for [{}] is using the default value: [{}]", name, defaultValue);
            }
            duration = defaultValue;
        } else {
            if (log.isDebugEnabled()) {
                log.debug("Configuration for [{}] is: [{}]", name, value);
            }
            duration = value;
        }

        return (int) duration.toMillis();
    }

    public void initialize() throws PulsarClientException {
        try {
            this.metadata = createMetadataResolver().resolve();
        } catch (IOException e) {
            log.error("Unable to retrieve OAuth 2.0 server metadata", e);
            throw new PulsarClientException.AuthenticationException("Unable to retrieve OAuth 2.0 server metadata");
        }
    }

    protected MetadataResolver createMetadataResolver() {
        return DefaultMetadataResolver.fromIssuerUrl(issuerUrl, httpClient, wellKnownMetadataPath);
    }

    static String parseParameterString(Map<String, String> params, String name) {
        String s = params.get(name);
        if (StringUtils.isEmpty(s)) {
            throw new IllegalArgumentException("Required configuration parameter: " + name);
        }
        return s;
    }

    static URL parseParameterUrl(Map<String, String> params, String name) {
        String s = params.get(name);
        if (StringUtils.isEmpty(s)) {
            throw new IllegalArgumentException("Required configuration parameter: " + name);
        }
        try {
            return new URL(s);
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException("Malformed configuration parameter: " + name);
        }
    }

    static Duration parseParameterDuration(Map<String, String> params, String name) {
        String value = params.get(name);
        if (StringUtils.isNotBlank(value)) {
            try {
                return Duration.parse(value);
            } catch (DateTimeParseException e) {
                throw new IllegalArgumentException("Malformed configuration parameter: " + name, e);
            }
        }
        return null;
    }

    @Override
    public void close() throws Exception {
        httpClient.close();
    }
}
