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
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
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
import org.apache.pulsar.client.impl.auth.v5.LegacyV4AuthenticationAdapter;
import org.apache.pulsar.client.impl.auth.v5.V5AuthenticationLoader;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
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
    // PIP-478: the factory instances build() has handed to a client. Adoption is a hand-over, not a share —
    // see the check in build(). Identity, not equals: a factory is an instance with a lifecycle, not a value.
    // Unsynchronized, unlike the admin builder's: this builder has no clone(), so the record never spans more
    // than the one builder that owns it, and two threads reaching it at once are already racing on the
    // configuration this builder mutates.
    private final Set<PulsarTlsFactory> adoptedTlsFactories =
            Collections.newSetFromMap(new IdentityHashMap<>());

    PulsarClientBuilderV5() {
        conf.setStatsIntervalSeconds(0);
        // v5 SDK transactions use the metadata-store (PIP-473) coordinator. This internal flag
        // routes the underlying v4 TC client to it, keeping v5 transactions independent from any
        // v4 SDK clients (which use the legacy coordinator) on the same cluster.
        conf.setScalableTransactions(true);
    }

    @Override
    public PulsarClient build() throws PulsarClientException {
        PulsarTlsFactory adopting = rejectAnAlreadyAdoptedTlsFactory();
        ClientConfigurationData clientConf = conf.clone();
        try {
            // PIP-478: hand the client its own configuration object, as ClientBuilderImpl.build() does.
            // PulsarClientImpl stores the TLS factory it composes back onto this object
            // (setupClientTlsFactory), so sharing the builder's instance would make a second build() adopt
            // and re-initialize the first client's live factory, and closing either client would then close
            // TLS for the other. This path matters more than the v4 one: tlsFactory(...) and tlsPolicy(...)
            // exist only here, so it is the only builder that can reach the adopted-factory arm
            // deliberately, and it is what turns on the fail-fast probe whose failure handler closes the
            // factory.
            //
            // The copy is taken first, and applyAuthentication() then resolves into it, because that step
            // writes too: it puts the resolved plugin in one of the two authentication slots and folds a
            // bridged v4 plugin's certificate and key into CLIENT_DEFAULT. Run against the builder those
            // writes outlive the client — the next build() would keep the previous plugin in the other slot
            // and present the previous plugin's client certificate. clone() is shallow, so the policy map
            // has to be copied separately for the fold to land only on this client.
            if (clientConf.getTlsPolicyMap() != null) {
                clientConf.setTlsPolicyMap(new LinkedHashMap<>(clientConf.getTlsPolicyMap()));
            }
            applyAuthentication(clientConf);
            var v4Client = new PulsarClientImpl(clientConf);
            return new PulsarClientV5(v4Client, description, transactionTimeout);
        } catch (org.apache.pulsar.client.api.PulsarClientException e) {
            throw new PulsarClientException(e.getMessage(), e);
        } finally {
            // Whether the factory was consumed is read off the copy the client was given rather than
            // inferred from how the build ended: a build that failed after the framework took the instance
            // has spent it (initialized, and closed again on the way out) exactly as a successful one has,
            // while one that failed before — no serviceUrl, say — has not touched it and must leave the
            // builder able to retry with it.
            if (clientConf.isTlsFactoryAdopted()) {
                recordAdoption(adopting);
            }
        }
    }

    /**
     * PIP-478: reject a second client built from a {@link PulsarTlsFactory} instance this builder has
     * already handed over.
     *
     * <p>Copying the configuration keeps each client's <em>composed</em> factory to itself, but an adopted
     * one is the caller's instance and the copy carries the same reference. Handing it to a second client
     * would have the framework break the SPI contract on the caller's behalf twice over — {@code initialize}
     * is called "exactly once" and {@code close} "at most once" on {@link PulsarTlsFactory} — and would leave
     * whichever client is closed second serving TLS from a closed factory while still reporting itself open.
     * Failing here says so at the one point where the mistake is still cheap to fix.
     *
     * <p>What counts as handed over is what the framework says it took, not whether the build succeeded:
     * {@code ClientTlsFactorySupport} marks the configuration when it adopts the instance, and from that
     * point the instance is spent whichever way the build ends. A build that failed before it — the caller
     * forgot {@code serviceUrl}, and {@code PulsarClientImpl} rejects that before resolving any TLS — leaves
     * the instance untouched and the builder usable, which matters because {@code tlsFactory(null)} throws,
     * so there is otherwise no way to clear the slot and retry.
     *
     * @return the instance about to be adopted, to be passed to {@link #recordAdoption} whatever the outcome,
     *         or {@code null} when no factory is configured
     */
    private PulsarTlsFactory rejectAnAlreadyAdoptedTlsFactory() {
        PulsarTlsFactory adopting = conf.getTlsFactory();
        if (adopting != null && adoptedTlsFactories.contains(adopting)) {
            throw new IllegalStateException("the PulsarTlsFactory passed to tlsFactory(...) has already been "
                    + "adopted by a client built from this builder. The client initializes that instance and "
                    + "closes it with itself, so it cannot be handed to a second client — closing either one "
                    + "would break TLS for the other. Call tlsFactory(...) with a fresh instance before "
                    + "building again.");
        }
        return adopting;
    }

    /**
     * Record that {@code adopted} has been handed over. Every instance is remembered, not just the last one,
     * so cycling back to an earlier factory is caught too; the builder therefore retains one reference per
     * build it performed, which is bounded by the factories the caller created in the first place.
     *
     * @param adopted the adopted instance, or {@code null} when no factory was configured
     */
    private void recordAdoption(PulsarTlsFactory adopted) {
        if (adopted != null) {
            adoptedTlsFactories.add(adopted);
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
     * <p>A bridged v4 plugin (from {@code AuthenticationFactory.token/tls/create}) is unwrapped back to the
     * raw v4 instance and stored in the v4 slot, which stays the single owner of its lifecycle — the client
     * starts it, closes it, and reads its TLS material and OAuth2 IdP trust from there. Any TLS material it
     * carries is folded into the client's policy map first, on this thread. A genuinely v5-native plugin goes
     * to the v5 slot instead.
     *
     * <p>Off-loading is not decided here. Since the inversion the client drives one resolved v5
     * {@code Authentication} for every binary connection, and the driver it builds is what keeps credential
     * work off the event loop: a bridged v4 plugin reaches it through {@code LegacyV4AuthenticationAdapter},
     * whose credential calls always run on the blocking executor.
     *
     * @param target the configuration to resolve into — the client's own copy at {@link #build()}, so neither
     *               slot nor the folded policy outlives the client into the next build
     */
    private void applyAuthentication(ClientConfigurationData target) {
        if (v5Authentication == null) {
            return;
        }
        org.apache.pulsar.client.api.Authentication bridgedV4 =
                LegacyV4AuthenticationAdapter.unwrapV4(v5Authentication).orElse(null);
        if (bridgedV4 != null) {
            // A legacy v4 plugin configured through the v5 builder. Fold any TLS material it carries into
            // the client's policy map, then hand the raw plugin to the v4 slot: that slot stays the one the
            // client starts, closes, and reads for TLS material and the OAuth2 IdP trust. The v5 body the
            // client drives is resolved from it during client construction, so the plugin's lifecycle has
            // exactly one owner.
            resolveBridgedV4(target, bridgedV4);
            target.setAuthentication(bridgedV4);
            return;
        }
        target.setV5Authentication(v5Authentication);
    }

    /**
     * Resolve the configured authentication and return the v4 {@code Authentication} slot's contents.
     * Package-private for tests that assert the unwrap decision without standing up a broker
     * (VisibleForTesting; Guava's annotation is not on this module's classpath). Resolves into the builder's
     * own configuration, which is where those tests then read the folded policy from; {@link #build()}
     * resolves into the client's copy instead.
     *
     * @return the raw v4 plugin when a bridged v4 plugin was configured, else {@code null} — a genuinely
     *         v5-native plugin occupies the v5 slot and leaves this one empty
     */
    org.apache.pulsar.client.api.Authentication resolveAuthenticationForTest() {
        applyAuthentication(conf);
        return conf.getAuthentication();
    }

    /**
     * For a bridged v4 plugin: fold its TLS material into {@link TlsPurpose#CLIENT_DEFAULT} when a
     * {@code tlsPolicy} is configured.
     *
     * <p>This used to also decide whether the plugin could be driven <em>raw</em> on the v4 client, returning
     * that decision to the caller. Since the inversion there is no such choice — the client resolves one v5
     * body from the v4 slot and drives it through {@code LegacyV4AuthenticationAdapter}, whose credential
     * calls always off-load — so the decision, and the return value carrying it, are gone.
     *
     * @param target the configuration to fold into
     * @param v4     the bridged v4 plugin recovered from the wrapping adapter
     */
    private void resolveBridgedV4(ClientConfigurationData target,
                                  org.apache.pulsar.client.api.Authentication v4) {
        boolean foldTls = target.getTlsPolicyMap() != null;
        if (v4 instanceof AuthenticationTls tls) {
            if (foldTls && tls.getCertFilePath() != null && tls.getKeyFilePath() != null) {
                mergeClientDefault(target, base -> pemBuilder(base)
                        .certificateFilePath(tls.getCertFilePath())
                        .keyFilePath(tls.getKeyFilePath())
                        .build());
            }
            return;
        }
        if (v4 instanceof AuthenticationKeyStoreTls keyStoreTls) {
            if (foldTls && keyStoreTls.getKeyStoreParams() != null) {
                KeyStoreParams ks = keyStoreTls.getKeyStoreParams();
                mergeClientDefault(target, base -> keyStoreBuilder(base)
                        .keyStorePath(ks.getKeyStorePath())
                        .keyStorePassword(ks.getKeyStorePassword())
                        .keyStoreType(ks.getKeyStoreType())
                        .build());
            }
            return;
        }
        if (v4 instanceof AuthenticationDisabled) {
            return;
        }
        resolveGenericV4(target, v4, foldTls);
    }

    /**
     * Fold a generic (non-built-in) bridged v4 plugin's file-based TLS material into
     * {@link TlsPurpose#CLIENT_DEFAULT}. Probed once at build time, on the application thread and off the
     * event loop. A plugin that exposes only in-memory cert/key material is logged rather than silently
     * dropped (it cannot be represented in the file-path {@link TlsPolicy}).
     *
     * <p>Nothing is probed when no {@code tlsPolicy} is configured: there is nowhere to fold material into,
     * the legacy v4 TLS path reads the plugin's material directly from {@code getAuthData()} anyway, and
     * skipping the call avoids an eager credential fetch at build time.
     *
     * @param target  the configuration to fold into
     * @param v4      the bridged v4 authentication plugin (not a built-in TLS class)
     * @param foldTls whether a {@code tlsPolicy} is configured, so TLS material should be folded
     */
    @SuppressWarnings("deprecation")
    private void resolveGenericV4(ClientConfigurationData target,
                                  org.apache.pulsar.client.api.Authentication v4, boolean foldTls) {
        if (!foldTls) {
            return;
        }
        final AuthenticationDataProvider data;
        try {
            data = v4.getAuthData();
        } catch (Exception e) {
            // A plugin that cannot produce auth data at build time (e.g. a not-yet-reachable credential
            // endpoint) contributes no TLS material here. Its credential is unaffected: the client resolves
            // it later, through the adapter, which off-loads.
            LOG.debug("Could not probe v4 authentication plugin {} at build time", v4.getClass().getName(), e);
            return;
        }
        if (data != null && data.hasDataForTls()) {
            foldGenericV4TlsMaterial(target, data, v4.getClass().getName());
        }
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
        // useTls governs the BINARY BROKER TRANSPORT only, so a policy for a different trust domain must not
        // turn it on. Configuring CLIENT_OAUTH2 (the identity provider) against a plaintext pulsar:// broker
        // is a legitimate combination — it was enabling TLS toward the broker and failing the connection.
        // The policy map itself is what triggers TLS-factory creation, so a non-transport purpose still gets
        // its factory without touching the transport.
        //
        // CLIENT_DEFAULT alone, not every client-role purpose: pip-478.md calls this "one narrow addition",
        // being the only v5 expression of the legacy client.conf useTls=true with a plain pulsar:// URL.
        // BROKER_CLIENT and plugin-minted purposes (TlsPurpose.client("...")) are client-role too, and
        // enabling the transport for them is the same defect this guard was added to fix.
        if (TlsPurpose.CLIENT_DEFAULT.equals(purpose)) {
            conf.setUseTls(true);
        }
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
        // Same rule as tlsPolicy above, and pip-478.md states it for this method by name: a factory supplies
        // material for every purpose WITHOUT enabling transport TLS. Forcing useTls here made an adopted
        // factory on a plaintext pulsar:// URL — the CLIENT_OAUTH2-only case the SPI exists to serve —
        // attempt a TLS handshake against the plaintext broker port. The adopted factory is still composed
        // and initialized: PulsarClientImpl.needsClientTlsFactory() has its own arm for conf.getTlsFactory().
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
     * @param target     the configuration to fold into
     * @param data       the already-probed auth data reporting TLS material
     * @param pluginName the bridged plugin's class name, for logging
     */
    @SuppressWarnings("deprecation")
    private void foldGenericV4TlsMaterial(ClientConfigurationData target, AuthenticationDataProvider data,
                                          String pluginName) {
        String certPath = data.getTlsCertificateFilePath();
        String keyPath = data.getTlsPrivateKeyFilePath();
        KeyStoreParams ks = data.getTlsKeyStoreParams();
        if (isNotBlank(certPath) && isNotBlank(keyPath)) {
            mergeClientDefault(target,
                    base -> pemBuilder(base).certificateFilePath(certPath).keyFilePath(keyPath).build());
        } else if (ks != null && isNotBlank(ks.getKeyStorePath())) {
            mergeClientDefault(target, base -> keyStoreBuilder(base)
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

    private void mergeClientDefault(ClientConfigurationData target,
                                    java.util.function.Function<TlsPolicy, TlsPolicy> merge) {
        Map<TlsPurpose, TlsPolicy> map = target.getTlsPolicyMap();
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
                    .jsseProvider(base.jsseProvider())
                    // Same reasoning for the material axis: a FIPS deployment pins BCFIPS alongside BCJSSE,
                    // and dropping half the pair is as bad as dropping both.
                    .jcaProvider(base.jcaProvider());
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
