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
import java.util.Optional;
import lombok.CustomLog;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.PulsarVersion;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.auth.oauth2.protocol.DefaultMetadataResolver;
import org.apache.pulsar.client.impl.auth.oauth2.protocol.Metadata;
import org.apache.pulsar.client.impl.auth.oauth2.protocol.MetadataResolver;
import org.apache.pulsar.http.PulsarHttpClient;
import org.apache.pulsar.http.PulsarHttpClientConfig;
import org.apache.pulsar.http.PulsarHttpClientFactory;
import org.apache.pulsar.tls.TlsPolicy;
import org.apache.pulsar.tls.TlsPurpose;

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

    protected static final Duration DEFAULT_CONNECT_TIMEOUT = Duration.ofSeconds(10);
    protected static final Duration DEFAULT_READ_TIMEOUT = Duration.ofSeconds(30);
    protected static final Duration DEFAULT_AUTO_CERT_REFRESH_DURATION = Duration.ofSeconds(300);

    private static final long serialVersionUID = 1L;

    protected final URL issuerUrl;
    private final Duration connectTimeout;
    private final Duration readTimeout;
    private final String trustCertsFilePath;
    private final String certFile;
    private final String keyFile;
    private final long autoCertRefreshSeconds;
    protected final String wellKnownMetadataPath;

    protected transient Metadata metadata;
    // PIP-478: the framework HTTP client factory, late-bound by AuthenticationOAuth2 from the owning
    // PulsarClient/PulsarAdmin's ClientAuthenticationServices before initialize(); null when the plugin runs
    // standalone (outside any client/admin), in which case getHttpClient() self-provisions one (see below).
    private transient PulsarHttpClientFactory httpClientFactory;
    // The self-provisioned factory for standalone use (outside any client/admin), built lazily and owned by
    // this flow; null unless getHttpClient() falls back to it. See StandaloneOAuth2HttpClientFactory.
    private transient StandaloneOAuth2HttpClientFactory standaloneHttpClientFactory;
    private transient PulsarHttpClient pulsarHttpClient;

    protected FlowBase(URL issuerUrl, Duration connectTimeout, Duration readTimeout, String trustCertsFilePath,
                       String certFile, String keyFile, Duration autoCertRefreshDuration,
                       String wellKnownMetadataPath) {
        this.issuerUrl = issuerUrl;
        this.connectTimeout = connectTimeout;
        this.readTimeout = readTimeout;
        this.trustCertsFilePath = trustCertsFilePath;
        this.certFile = certFile;
        this.keyFile = keyFile;
        this.autoCertRefreshSeconds = getParameterDurationToSeconds(CONFIG_PARAM_AUTO_CERT_REFRESH_DURATION,
                autoCertRefreshDuration, DEFAULT_AUTO_CERT_REFRESH_DURATION);
        this.wellKnownMetadataPath = wellKnownMetadataPath;
        // PIP-478: the HTTP client is not built eagerly here. The flow is constructed during
        // configure() — before the client's framework services (the shared HTTP client factory) are bound —
        // so the client is created lazily on first use (initialize()), once the factory is available.
    }

    /**
     * Late-bind the framework HTTP client factory (PIP-478). Called by
     * {@code AuthenticationOAuth2.bindClientAuthenticationServices(...)} before {@code initialize()}, so the
     * lazily-built client is the framework-managed one that shares the owning client's resources.
     *
     * @param httpClientFactory the owning client/admin's framework factory (never {@code null} in practice)
     */
    void bindHttpClientFactory(PulsarHttpClientFactory httpClientFactory) {
        this.httpClientFactory = httpClientFactory;
    }

    protected synchronized PulsarHttpClient getHttpClient() {
        if (pulsarHttpClient == null) {
            // Normally a framework-managed HTTP client sharing the owning PulsarClient/PulsarAdmin's event loop
            // / timer / DNS resolver (bound via bindHttpClientFactory). When this flow carries its own IdP TLS
            // material it is folded into the CLIENT_OAUTH2 policy the framework client serves (see
            // AuthenticationOAuth2.idpTlsPolicy / ClientTlsFactorySupport).
            pulsarHttpClient = resolveHttpClientFactory().newHttpClient(oauth2ClientConfig());
        }
        return pulsarHttpClient;
    }

    /**
     * The HTTP client factory used to acquire OAuth2 tokens: the framework factory bound by the owning
     * {@code PulsarClient}/{@code PulsarAdmin} on the normal path, or a self-provisioned standalone factory
     * when this flow runs outside any client/admin. The proxy, the broker's broker-client and the CLI tools
     * create {@code AuthenticationOAuth2} directly (via {@code AuthenticationFactory.create(...).start()}) and
     * never bind a factory, so {@code FlowBase} self-provisions its own HTTP client here to preserve
     * backward-compatible standalone support on the new SPI ({@link StandaloneOAuth2HttpClientFactory}).
     */
    private PulsarHttpClientFactory resolveHttpClientFactory() {
        if (httpClientFactory != null) {
            return httpClientFactory;
        }
        if (standaloneHttpClientFactory == null) {
            standaloneHttpClientFactory = new StandaloneOAuth2HttpClientFactory(idpTlsPolicy(),
                    (int) autoCertRefreshSeconds,
                    "standalone-oauth2-" + Integer.toHexString(System.identityHashCode(this)));
        }
        return standaloneHttpClientFactory;
    }

    /** @return whether this flow was configured with its own OAuth2 IdP TLS material (trust / cert / key). */
    private boolean hasOwnTlsMaterial() {
        return StringUtils.isNotBlank(trustCertsFilePath) || StringUtils.isNotBlank(certFile)
                || StringUtils.isNotBlank(keyFile);
    }

    /**
     * The IdP TLS material this flow carries (the {@code trustCertsFilePath} / {@code tlsCertFile} /
     * {@code tlsKeyFile} parameters), folded into a {@link TlsPurpose#CLIENT_OAUTH2} {@link TlsPolicy} so the
     * framework HTTP client can serve IdP mTLS / custom trust on the new PIP-478 TLS path (issue
     * #24944). Empty when the flow carries no IdP TLS material — the OAuth2 call then resolves to the system
     * default trust store (CLIENT_OAUTH2's empty fallback).
     *
     * @return the CLIENT_OAUTH2 policy composed from this flow's IdP material, or empty
     */
    Optional<TlsPolicy> idpTlsPolicy() {
        if (!hasOwnTlsMaterial()) {
            return Optional.empty();
        }
        TlsPolicy.Builder builder = TlsPolicy.builder().format(TlsPolicy.Format.PEM);
        if (StringUtils.isNotBlank(trustCertsFilePath)) {
            builder.trustCertsFilePath(trustCertsFilePath);
        }
        if (StringUtils.isNotBlank(certFile) && StringUtils.isNotBlank(keyFile)) {
            builder.certificateFilePath(certFile).keyFilePath(keyFile);
        }
        return Optional.of(builder.build());
    }

    private PulsarHttpClientConfig oauth2ClientConfig() {
        Duration connect = getParameterDuration(CONFIG_PARAM_CONNECT_TIMEOUT, connectTimeout, DEFAULT_CONNECT_TIMEOUT);
        Duration read = getParameterDuration(CONFIG_PARAM_READ_TIMEOUT, readTimeout, DEFAULT_READ_TIMEOUT);
        return PulsarHttpClientConfig.builder(TlsPurpose.CLIENT_OAUTH2)
                .connectTimeout(connect)
                .readTimeout(read)
                .requestTimeout(read)
                .userAgent(String.format("Pulsar-Java-v%s", PulsarVersion.getVersion()))
                .build();
    }

    private long getParameterDurationToSeconds(String name, Duration value, Duration defaultValue) {
        return getParameterDuration(name, value, defaultValue).getSeconds();
    }

    private Duration getParameterDuration(String name, Duration value, Duration defaultValue) {
        Duration duration;
        if (value == null) {
            log.debug().attr("name", name)
                    .attr("defaultValue", defaultValue)
                    .log("Configuration is using the default value");
            duration = defaultValue;
        } else {
            log.debug().attr("name", name).attr("value", value).log("Configuration");
            duration = value;
        }
        return duration;
    }

    public void initialize() throws PulsarClientException {
        try {
            this.metadata = createMetadataResolver().resolve();
        } catch (IOException e) {
            log.error().exception(e).log("Unable to retrieve OAuth 2.0 server metadata");
            throw new PulsarClientException.AuthenticationException("Unable to retrieve OAuth 2.0 server metadata");
        }
    }

    protected MetadataResolver createMetadataResolver() {
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
    // Synchronized to pair with the synchronized getHttpClient()/resolveHttpClientFactory() lazy init:
    // otherwise a close() racing the first fetch reads pulsarHttpClient/standaloneHttpClientFactory without the
    // monitor and can miss (and thus leak) a client or standalone factory that the racing fetch just created.
    public synchronized void close() throws Exception {
        if (pulsarHttpClient != null) {
            // Idempotent: releases the framework client (which the framework factory also closes on client
            // shutdown).
            pulsarHttpClient.close();
        }
        if (standaloneHttpClientFactory != null) {
            // Only set when this flow self-provisioned a standalone factory (no owning client/admin); releases
            // its framework client, IdP TLS factory and rotation scheduler.
            standaloneHttpClientFactory.close();
        }
    }
}
