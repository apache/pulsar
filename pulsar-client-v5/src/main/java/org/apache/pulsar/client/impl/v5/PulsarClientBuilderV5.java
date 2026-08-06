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
package org.apache.pulsar.client.impl.v5;

import io.opentelemetry.api.OpenTelemetry;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.api.KeyStoreParams;
import org.apache.pulsar.client.api.v5.PulsarClient;
import org.apache.pulsar.client.api.v5.PulsarClientBuilder;
import org.apache.pulsar.client.api.v5.PulsarClientException;
import org.apache.pulsar.client.api.v5.auth.Authentication;
import org.apache.pulsar.client.api.v5.config.ConnectionPolicy;
import org.apache.pulsar.client.api.v5.config.MemorySize;
import org.apache.pulsar.client.api.v5.config.TransactionPolicy;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.auth.AuthenticationDisabled;
import org.apache.pulsar.client.impl.auth.AuthenticationKeyStoreTls;
import org.apache.pulsar.client.impl.auth.AuthenticationTls;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.v5.auth.LegacyV4AuthenticationAdapter;
import org.apache.pulsar.client.impl.v5.auth.V5ToV4AuthenticationAdapter;
import org.apache.pulsar.tls.PulsarTlsFactory;
import org.apache.pulsar.tls.TlsPolicy;
import org.apache.pulsar.tls.TlsPurpose;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * V5 implementation of PulsarClientBuilder.
 * Builds a v4 ClientConfigurationData internally and wraps the v4 PulsarClientImpl.
 */
final class PulsarClientBuilderV5 implements PulsarClientBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(PulsarClientBuilderV5.class);

    private final ClientConfigurationData conf = new ClientConfigurationData();
    private String description;
    private Duration transactionTimeout;
    // The configured v5 authentication, resolved into the v4 conf.authentication at build() (so a bridged
    // v4 plugin can be probed on the application thread — off the event loop — to fold its TLS material and
    // to decide whether it must stay wrapped for credential off-loading). Null when no auth is configured.
    private Authentication v5Authentication;

    PulsarClientBuilderV5() {
        conf.setStatsIntervalSeconds(0);
        // v5 SDK transactions use the metadata-store (PIP-473) coordinator. This internal flag
        // routes the underlying v4 TC client to it, keeping v5 transactions independent from any
        // v4 SDK clients (which use the legacy coordinator) on the same cluster.
        conf.setScalableTransactions(true);
    }

    @Override
    public PulsarClient build() throws PulsarClientException {
        try {
            applyAuthentication();
            var v4Client = new PulsarClientImpl(conf);
            return new PulsarClientV5(v4Client, description, transactionTimeout);
        } catch (org.apache.pulsar.client.api.PulsarClientException e) {
            throw new PulsarClientException(e.getMessage(), e);
        }
    }

    @Override
    public PulsarClientBuilder serviceUrl(String serviceUrl) {
        validatePulsarServiceUrl(serviceUrl, "serviceUrl");
        conf.setServiceUrl(serviceUrl);
        return this;
    }

    @Override
    public PulsarClientBuilder authentication(Authentication authentication) {
        this.v5Authentication = authentication;
        return this;
    }

    @Override
    public PulsarClientBuilder authentication(String authPluginClassName, String authParamsString)
            throws PulsarClientException {
        // Preserve the serializable string form (authPluginClassName + authParams) AND eagerly build the
        // plugin, mirroring the v4 ClientBuilderImpl. The build is v5-aware (PIP-478 In-Scope #2): a
        // v5-native plugin deployed by class name is instantiated + configured, instead of being blind-cast
        // to the v4 Authentication SPI (which threw ClassCastException). A legacy v4 class keeps the
        // existing v4 path. The resolution to the v4 conf.authentication happens at build().
        conf.setAuthPluginClassName(authPluginClassName);
        conf.setAuthParams(authParamsString);
        try {
            this.v5Authentication = V5AuthenticationLoader.create(authPluginClassName, authParamsString);
        } catch (org.apache.pulsar.client.api.PulsarClientException e) {
            throw new PulsarClientException(e.getMessage(), e);
        }
        return this;
    }

    /**
     * Resolve the configured v5 {@link Authentication} into the v4 {@code Authentication} the underlying
     * {@link PulsarClientImpl} consumes, folding any bridged v4 TLS material into the client TLS policy and
     * choosing how the plugin is driven. Runs at {@link #build()} on the application thread — off the Netty
     * event loop — where probing a bridged plugin for its material is safe.
     *
     * <p>A bridged v4 plugin (from {@code AuthenticationFactory.token/tls/create}) that performs <b>no
     * credential I/O</b> — the built-in TLS-material plugins (their material is folded into the client TLS
     * configuration; the binary handshake carries an empty payload) and disabled auth — is driven raw on the
     * v4 client. A credential-fetching v4 plugin, and every genuinely v5-native plugin, is exposed through
     * {@link V5ToV4AuthenticationAdapter} so its (possibly blocking) {@code getAuthData} off-loads to the
     * blocking executor instead of running on the Netty event loop (PIP-478: v4 credential calls are
     * ALWAYS off-loaded). The adapter implements {@code ClientAuthenticationServicesAware}, so the client
     * late-binds real executors / HTTP client factory / client instance id into it before {@code start()}
     * (PIP-478).
     */
    private void applyAuthentication() {
        if (v5Authentication == null) {
            return;
        }
        org.apache.pulsar.client.api.Authentication bridgedV4 =
                LegacyV4AuthenticationAdapter.unwrapV4(v5Authentication).orElse(null);
        boolean runRaw = bridgedV4 != null && resolveBridgedV4(bridgedV4);
        conf.setAuthentication(runRaw
                ? bridgedV4
                : new V5ToV4AuthenticationAdapter(v5Authentication));
    }

    /**
     * Resolve the configured authentication and return the v4 {@code Authentication} the client would drive.
     * Package-private for tests that assert the unwrap-vs-offload decision without standing up a broker
     * (VisibleForTesting; Guava's annotation is not on this module's classpath).
     *
     * @return the resolved v4 authentication (a raw v4 engine, or a wrapping {@code V5ToV4AuthenticationAdapter})
     */
    org.apache.pulsar.client.api.Authentication resolveAuthenticationForTest() {
        applyAuthentication();
        return conf.getAuthentication();
    }

    /**
     * For a bridged v4 plugin: fold its TLS material into {@link TlsPurpose#CLIENT_DEFAULT} (when a
     * {@code tlsPolicy} is configured) and decide whether it can be driven raw on the v4 client.
     *
     * @param v4 the bridged v4 plugin recovered from the wrapping adapter
     * @return {@code true} when the raw v4 engine is correct — it performs no credential I/O (a built-in
     *         TLS-material plugin or disabled auth); {@code false} when it fetches a command/HTTP credential
     *         and must stay wrapped so the call off-loads
     */
    private boolean resolveBridgedV4(org.apache.pulsar.client.api.Authentication v4) {
        boolean foldTls = conf.getTlsPolicyMap() != null;
        if (v4 instanceof AuthenticationTls tls) {
            if (foldTls && tls.getCertFilePath() != null && tls.getKeyFilePath() != null) {
                mergeClientDefault(base -> pemBuilder(base)
                        .certificateFilePath(tls.getCertFilePath())
                        .keyFilePath(tls.getKeyFilePath())
                        .build());
            }
            return true;
        }
        if (v4 instanceof AuthenticationKeyStoreTls keyStoreTls) {
            if (foldTls && keyStoreTls.getKeyStoreParams() != null) {
                KeyStoreParams ks = keyStoreTls.getKeyStoreParams();
                mergeClientDefault(base -> keyStoreBuilder(base)
                        .keyStorePath(ks.getKeyStorePath())
                        .keyStorePassword(ks.getKeyStorePassword())
                        .keyStoreType(ks.getKeyStoreType())
                        .build());
            }
            return true;
        }
        if (v4 instanceof AuthenticationDisabled) {
            return true;
        }
        return resolveGenericV4(v4, foldTls);
    }

    /**
     * Probe a generic (non-built-in) bridged v4 plugin once at build time (application thread, off the event
     * loop): fold any file-based TLS material into {@link TlsPurpose#CLIENT_DEFAULT} when a {@code tlsPolicy}
     * is configured, and report whether it can run raw. A plugin that exposes only in-memory cert/key
     * material is logged rather than silently dropped (it cannot be represented in the file-path
     * {@link TlsPolicy}).
     *
     * @param v4      the bridged v4 authentication plugin (not a built-in TLS class)
     * @param foldTls whether a {@code tlsPolicy} is configured, so TLS material should be folded
     * @return {@code true} when the plugin must be driven raw on the v4 client — it performs no credential
     *         I/O, or it carries TLS material that can only reach the transport through the legacy path;
     *         {@code false} when it should stay wrapped for credential off-load
     */
    @SuppressWarnings("deprecation")
    private boolean resolveGenericV4(org.apache.pulsar.client.api.Authentication v4, boolean foldTls) {
        final AuthenticationDataProvider data;
        try {
            data = v4.getAuthData();
        } catch (Exception e) {
            // A plugin that cannot produce auth data at build time (e.g. a not-yet-reachable credential
            // endpoint) contributes no TLS material here; keep it wrapped so any later credential I/O
            // off-loads instead of running on the event loop.
            LOG.debug("Could not probe v4 authentication plugin {} at build time", v4.getClass().getName(), e);
            return false;
        }
        if (data == null) {
            return false;
        }
        boolean hasCredentialIo = data.hasDataFromCommand() || data.hasDataForHttp();
        if (data.hasDataForTls()) {
            if (foldTls) {
                // tlsPolicy is configured: fold the plugin's file material into the client TLS factory path,
                // so the transport reads TLS from the factory and the plugin may still be wrapped for
                // credential off-load.
                foldGenericV4TlsMaterial(data, v4.getClass().getName());
                return !hasCredentialIo;
            }
            // No tlsPolicy: the legacy v4 TLS path reads the plugin's material directly from getAuthData(),
            // so it MUST run raw or its client certificate would be dropped (the wrapping adapter's
            // synthesized v4 provider carries only the binary credential, not TLS material). A plugin that
            // also fetches a credential therefore forgoes off-load here — the two cannot both hold on the
            // legacy path; configuring tlsPolicy(...) restores off-load by moving TLS to the factory path.
            if (hasCredentialIo) {
                LOG.warn("Bridged v4 authentication plugin {} provides both TLS material and a fetched "
                        + "credential but no tlsPolicy(...) is configured; it runs inline (no credential "
                        + "off-load) so its client certificate is still presented on the legacy TLS path. "
                        + "Configure tlsPolicy(...) to off-load the credential.", v4.getClass().getName());
            }
            return true;
        }
        return !hasCredentialIo;
    }

    @Override
    public PulsarClientBuilder operationTimeout(Duration timeout) {
        conf.setOperationTimeoutMs(timeout.toMillis());
        return this;
    }

    @Override
    public PulsarClientBuilder connectionPolicy(ConnectionPolicy policy) {
        conf.setConnectionTimeoutMs((int) policy.connectionTimeout().toMillis());
        conf.setConnectionsPerBroker(policy.connectionsPerBroker());
        conf.setUseTcpNoDelay(policy.enableTcpNoDelay());
        conf.setKeepAliveIntervalSeconds((int) policy.keepAliveInterval().toSeconds());
        conf.setConnectionMaxIdleSeconds((int) policy.connectionMaxIdleTime().toSeconds());
        conf.setNumIoThreads(policy.ioThreads());
        conf.setNumListenerThreads(policy.callbackThreads());
        if (policy.proxyServiceUrl() != null) {
            validatePulsarServiceUrl(policy.proxyServiceUrl(), "ConnectionPolicy.proxyServiceUrl");
            conf.setProxyServiceUrl(policy.proxyServiceUrl());
            if (policy.proxyProtocol() != null) {
                conf.setProxyProtocol(
                        org.apache.pulsar.client.api.ProxyProtocol.valueOf(policy.proxyProtocol().name()));
            }
        }
        // BackoffPolicy adaptation will be implemented when the v4 client exposes
        // a public way to override the reconnection backoff.
        return this;
    }

    @Override
    public PulsarClientBuilder transactionPolicy(TransactionPolicy policy) {
        conf.setEnableTransaction(true);
        this.transactionTimeout = policy.timeout();
        return this;
    }

    @Override
    public PulsarClientBuilder tlsPolicy(TlsPolicy policy) {
        return tlsPolicy(TlsPurpose.CLIENT_DEFAULT, policy);
    }

    @Override
    public PulsarClientBuilder tlsPolicy(TlsPurpose purpose, TlsPolicy policy) {
        if (purpose == null || policy == null) {
            throw new IllegalArgumentException("tlsPolicy purpose and policy must not be null");
        }
        conf.setUseTls(true);
        Map<TlsPurpose, TlsPolicy> map = conf.getTlsPolicyMap();
        if (map == null) {
            map = new LinkedHashMap<>();
            conf.setTlsPolicyMap(map);
        }
        map.put(purpose, policy);
        return this;
    }

    @Override
    public PulsarClientBuilder tlsFactory(PulsarTlsFactory factory) {
        if (factory == null) {
            throw new IllegalArgumentException("tlsFactory must not be null");
        }
        conf.setUseTls(true);
        conf.setTlsFactory(factory);
        return this;
    }

    /**
     * Fold a bridged third-party v4 plugin's file-based TLS material into {@link TlsPurpose#CLIENT_DEFAULT}
     * (PIP-478). The plugin's {@code getAuthData()} has already been probed by
     * {@link #resolveGenericV4} on the application thread (off the event loop), and {@code data} is known to
     * report {@code hasDataForTls()}. Only <em>file-based</em> material (PEM cert/key file paths or a
     * keystore) can be represented in the file-path {@link TlsPolicy}; a plugin that exposes only in-memory
     * cert/key material is logged rather than silently dropped, since it cannot be folded on this path.
     *
     * @param data       the already-probed auth data reporting TLS material
     * @param pluginName the bridged plugin's class name, for logging
     */
    @SuppressWarnings("deprecation")
    private void foldGenericV4TlsMaterial(AuthenticationDataProvider data, String pluginName) {
        String certPath = data.getTlsCertificateFilePath();
        String keyPath = data.getTlsPrivateKeyFilePath();
        KeyStoreParams ks = data.getTlsKeyStoreParams();
        if (isNotBlank(certPath) && isNotBlank(keyPath)) {
            mergeClientDefault(base -> pemBuilder(base).certificateFilePath(certPath).keyFilePath(keyPath).build());
        } else if (ks != null && isNotBlank(ks.getKeyStorePath())) {
            mergeClientDefault(base -> keyStoreBuilder(base)
                    .keyStorePath(ks.getKeyStorePath())
                    .keyStorePassword(ks.getKeyStorePassword())
                    .keyStoreType(ks.getKeyStoreType())
                    .build());
        } else {
            LOG.warn("Bridged v4 authentication plugin {} reports TLS material (hasDataForTls()) but exposes "
                    + "only in-memory cert/key, which cannot be represented in the file-path client TLS "
                    + "policy; its material will not be folded into the transport. Configure it via "
                    + "tlsPolicy(...) or a file-based plugin.", pluginName);
        }
    }

    private static boolean isNotBlank(String s) {
        return s != null && !s.isBlank();
    }

    private void mergeClientDefault(java.util.function.Function<TlsPolicy, TlsPolicy> merge) {
        Map<TlsPurpose, TlsPolicy> map = conf.getTlsPolicyMap();
        TlsPolicy base = map.get(TlsPurpose.CLIENT_DEFAULT);
        map.put(TlsPurpose.CLIENT_DEFAULT, merge.apply(base));
    }

    /** Copy the trust material and flags of {@code base} (if any) into a PEM-format builder. */
    private static TlsPolicy.Builder pemBuilder(TlsPolicy base) {
        TlsPolicy.Builder b = copyFlags(base).format(TlsPolicy.Format.PEM);
        if (base != null && base.format() == TlsPolicy.Format.PEM) {
            b.trustCertsFilePath(base.trustCertsFilePath());
        } else if (base != null && isNotBlank(base.trustStorePath())) {
            // Cross-format fold: the tlsPolicy(...) carries a keystore truststore but the auth plugin's client
            // identity is PEM. A PEM policy has no truststore field, so folding here would silently drop the
            // configured trust anchors and fall back to the system trust store. Fail loud (matching
            // TlsPolicy.build()'s fail-loud format validation) rather than silently broadening/breaking trust.
            throw new IllegalArgumentException("Cross-format TLS material: tlsPolicy(...) configures a keystore "
                    + "truststore (trustStorePath) but the authentication plugin supplies a PEM client "
                    + "certificate/key. Folding these would silently drop the configured truststore. Configure the "
                    + "trust material and the client identity in the same format (both PEM, or both keystore).");
        }
        return b;
    }

    /** Copy the trust material and flags of {@code base} (if any) into a keystore-format builder. */
    private static TlsPolicy.Builder keyStoreBuilder(TlsPolicy base) {
        TlsPolicy.Builder b = copyFlags(base).format(TlsPolicy.Format.KEYSTORE);
        if (base != null && base.format() == TlsPolicy.Format.KEYSTORE) {
            // Preserve the base truststore (path, password, and TYPE): folding the auth plugin's keystore must
            // not clobber the truststore type configured via tlsPolicy(...) — the keystore and truststore may
            // use different types (e.g. a PKCS12 keystore with a JKS truststore).
            b.trustStorePath(base.trustStorePath())
                    .trustStorePassword(base.trustStorePassword())
                    .trustStoreType(base.trustStoreType());
        } else if (base != null && isNotBlank(base.trustCertsFilePath())) {
            // Cross-format fold: the tlsPolicy(...) carries a PEM truststore (trustCertsFilePath) but the auth
            // plugin's client identity is a keystore. A keystore policy has no PEM trust field, so folding here
            // would silently drop the configured trust anchors and fall back to the system trust store. Fail loud
            // (matching TlsPolicy.build()'s fail-loud format validation) rather than silently broadening/breaking
            // trust.
            throw new IllegalArgumentException("Cross-format TLS material: tlsPolicy(...) configures a PEM "
                    + "truststore (trustCertsFilePath) but the authentication plugin supplies a keystore client "
                    + "certificate/key. Folding these would silently drop the configured truststore. Configure the "
                    + "trust material and the client identity in the same format (both PEM, or both keystore).");
        }
        return b;
    }

    private static TlsPolicy.Builder copyFlags(TlsPolicy base) {
        TlsPolicy.Builder b = TlsPolicy.builder();
        if (base != null) {
            b.allowInsecureConnection(base.allowInsecureConnection())
                    .enableHostnameVerification(base.enableHostnameVerification())
                    .protocols(base.protocols())
                    .ciphers(base.ciphers())
                    // Preserve the pinned JSSE (SSLContext) provider across the fold. A FIPS deployment
                    // pins it via tlsPolicy(...); dropping it here would let the transport fall back to
                    // the default JDK engine, silently defeating the pin.
                    .jsseProvider(base.jsseProvider());
        }
        return b;
    }

    @Override
    public PulsarClientBuilder openTelemetry(OpenTelemetry openTelemetry) {
        conf.setOpenTelemetry(openTelemetry);
        return this;
    }

    @Override
    public PulsarClientBuilder memoryLimit(MemorySize size) {
        conf.setMemoryLimitBytes(size.bytes());
        return this;
    }

    @Override
    public PulsarClientBuilder listenerName(String name) {
        conf.setListenerName(name);
        return this;
    }

    @Override
    public PulsarClientBuilder description(String description) {
        this.description = description;
        conf.setDescription(description);
        return this;
    }

    /** @return the underlying v4 configuration data; for tests in this package only. */
    ClientConfigurationData getConfForTesting() {
        return conf;
    }

    /**
     * Reject anything that isn't the broker binary protocol. The most common
     * mistake is passing the admin/web service URL ({@code http://...}) where a
     * broker URL is expected — call that out specifically. The v4 client used to
     * silently fail far downstream with cryptic connection errors; here we fail
     * fast at configure time with a message the user can act on.
     */
    private static void validatePulsarServiceUrl(String url, String fieldName) {
        if (url == null || url.isBlank()) {
            throw new IllegalArgumentException(fieldName + " must not be null or blank");
        }
        if (url.startsWith("pulsar://") || url.startsWith("pulsar+ssl://")) {
            return;
        }
        if (url.startsWith("http://") || url.startsWith("https://")) {
            throw new IllegalArgumentException(fieldName + " must use the broker binary protocol "
                    + "(pulsar:// or pulsar+ssl://); got '" + url + "'. This looks like the admin/web "
                    + "service URL — pass the broker service URL instead (typically port 6650, or "
                    + "6651 for TLS).");
        }
        throw new IllegalArgumentException(fieldName + " must use the broker binary protocol "
                + "(pulsar:// or pulsar+ssl://); got '" + url + "'.");
    }
}
