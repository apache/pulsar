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

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.util.Map;
import lombok.CustomLog;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.AuthenticationHttpClient;
import org.apache.pulsar.client.api.AuthenticationHttpClientConfig;
import org.apache.pulsar.client.api.AuthenticationHttpClientFactory;
import org.apache.pulsar.client.api.AuthenticationInitContext;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.auth.DefaultAuthenticationHttpClientFactory;
import org.apache.pulsar.client.impl.auth.oauth2.protocol.DefaultMetadataResolver;
import org.apache.pulsar.client.impl.auth.oauth2.protocol.Metadata;
import org.apache.pulsar.client.impl.auth.oauth2.protocol.MetadataResolver;

/**
 * An abstract OAuth 2.0 authorization flow.
 */
@CustomLog
abstract class FlowBase implements Flow {

    public static final String CONFIG_PARAM_CONNECT_TIMEOUT = "connectTimeout";
    public static final String CONFIG_PARAM_READ_TIMEOUT = "readTimeout";
    public static final String CONFIG_PARAM_TRUST_CERTS_FILE_PATH = "trustCertsFilePath";
    public static final String CONFIG_PARAM_CERT_FILE = "tlsCertFile";
    public static final String CONFIG_PARAM_TLS_KEY_FILE = "tlsKeyFile";
    public static final String CONFIG_PARAM_AUTO_CERT_REFRESH_DURATION = "autoCertRefreshDuration";
    public static final String CONFIG_PARAM_WELL_KNOWN_METADATA_PATH = "wellKnownMetadataPath";

    private static final long serialVersionUID = 1L;

    protected final URL issuerUrl;
    private final Duration connectTimeout;
    private final Duration readTimeout;
    private final String trustCertsFilePath;
    private final String certFile;
    private final String keyFile;
    private final Duration autoCertRefreshDuration;
    protected final String wellKnownMetadataPath;

    protected transient Metadata metadata;
    private transient AuthenticationHttpClient httpClient;
    private transient AuthenticationHttpClientFactory httpClientFactory;

    protected FlowBase(URL issuerUrl, Duration connectTimeout, Duration readTimeout, String trustCertsFilePath,
                       String certFile, String keyFile, Duration autoCertRefreshDuration,
                       String wellKnownMetadataPath) {
        this.issuerUrl = issuerUrl;
        this.connectTimeout = connectTimeout;
        this.readTimeout = readTimeout;
        this.trustCertsFilePath = trustCertsFilePath;
        this.certFile = certFile;
        this.keyFile = keyFile;
        this.autoCertRefreshDuration = autoCertRefreshDuration;
        this.wellKnownMetadataPath = wellKnownMetadataPath;
    }

    protected synchronized AuthenticationHttpClient getHttpClient() throws PulsarClientException {
        if (httpClient == null) {
            AuthenticationHttpClientFactory factory = httpClientFactory == null
                    ? new DefaultAuthenticationHttpClientFactory(null, null, null)
                    : httpClientFactory;
            httpClient = factory.create(AuthenticationHttpClientConfig.builder()
                    .connectTimeout(connectTimeout)
                    .readTimeout(readTimeout)
                    .trustCertsFilePath(trustCertsFilePath)
                    .tlsCertificateFilePath(certFile)
                    .tlsKeyFilePath(keyFile)
                    .autoCertRefreshDuration(autoCertRefreshDuration)
                    .build());
        }
        return httpClient;
    }

    public void initialize() throws PulsarClientException {
        try {
            this.metadata = createMetadataResolver().resolve();
        } catch (IOException e) {
            log.error().exception(e).log("Unable to retrieve OAuth 2.0 server metadata");
            throw new PulsarClientException.AuthenticationException("Unable to retrieve OAuth 2.0 server metadata");
        }
    }

    @Override
    public void initialize(AuthenticationInitContext context) throws PulsarClientException {
        httpClientFactory = context == null
                ? null
                : context.getService(AuthenticationHttpClientFactory.class).orElse(null);
        initialize();
    }

    protected MetadataResolver createMetadataResolver() throws PulsarClientException {
        return DefaultMetadataResolver.fromIssuerUrl(issuerUrl, getHttpClient(), wellKnownMetadataPath);
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
        if (httpClient != null) {
            httpClient.close();
        }
    }
}
