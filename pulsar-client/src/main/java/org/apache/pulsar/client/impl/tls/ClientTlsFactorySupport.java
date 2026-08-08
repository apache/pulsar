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
package org.apache.pulsar.client.impl.tls;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.annotations.VisibleForTesting;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslProvider;
import io.opentelemetry.api.OpenTelemetry;
import java.time.Clock;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.impl.auth.AuthenticationKeyStoreTls;
import org.apache.pulsar.client.impl.auth.AuthenticationTls;
import org.apache.pulsar.client.impl.auth.oauth2.AuthenticationOAuth2;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.common.tls.impl.FileBasedTlsFactory;
import org.apache.pulsar.common.tls.impl.FileBasedTlsFactorySettings;
import org.apache.pulsar.common.tls.impl.TlsContextAcquisition;
import org.apache.pulsar.common.tls.impl.TlsSynthesisSpec;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.tls.PulsarTlsFactory;
import org.apache.pulsar.tls.TlsEndpoint;
import org.apache.pulsar.tls.TlsFactoryInitContext;
import org.apache.pulsar.tls.TlsHandle;
import org.apache.pulsar.tls.TlsPolicy;
import org.apache.pulsar.tls.TlsPurpose;

/**
 * Client-side counterpart of the server's {@code TlsFactorySupport} (which lives in
 * {@code pulsar-broker-common}, off the client classpath): the
 * {@code ClientConfigurationData}&rarr;{@link TlsPolicy} composition, the {@code sslProvider}&rarr;Netty
 * engine mapping, and the default {@link FileBasedTlsFactory} construction the v4 client's transport uses.
 * Since the PIP-337 {@code PulsarSslFactory} removal this is the only client TLS path: a TLS
 * client always builds a {@link PulsarTlsFactory} (an adopted v5 {@code tlsFactory(...)} or the built-in
 * file-based default composed from the {@code tls*} fields).
 *
 * <p><b>Fail-fast probing.</b> Eager probing of the statically-needed client purpose
 * ({@link TlsPurpose#CLIENT_DEFAULT}) is a <em>v5-builder-path</em> behaviour only: it runs when the v5
 * builder configured TLS, so a bad path fails the client build with an actionable error. A plain v4 client's
 * config stays lazily loaded, so unmodified v4 tests that lazily tolerated a bad TLS field are not broken.
 */
public final class ClientTlsFactorySupport {

    /** Reserved factory-class value selecting the built-in file-based factory (mirrors the server side). */
    private static final String DEFAULT_FACTORY = "default";

    private ClientTlsFactorySupport() {
    }

    /**
     * Map the v4 {@code sslProvider} string to the Netty engine provider. Only the literal OpenSSL
     * providers select {@code OPENSSL}; every other value (JCE provider names, {@code null}) is JDK.
     *
     * @param sslProvider the configured {@code sslProvider} string
     * @return the Netty {@link SslProvider}
     */
    public static SslProvider engineProvider(String sslProvider) {
        if (sslProvider != null) {
            String p = sslProvider.trim();
            if ("OPENSSL".equalsIgnoreCase(p) || "OPENSSL_REFCNT".equalsIgnoreCase(p)) {
                return SslProvider.OPENSSL;
            }
        }
        return SslProvider.JDK;
    }

    /**
     * Resolve the client's JSSE (SSLContext) provider name for the {@code CLIENT_DEFAULT} {@link TlsPolicy}
     * (PIP-478). Precedence:
     * <ol>
     *   <li>an explicit {@code jsseProvider} config key wins;</li>
     *   <li>otherwise the v4 {@code sslProvider} value is split along two axes — the Netty
     *       {@link SslProvider} engine literals ({@code JDK} / {@code OPENSSL} / {@code OPENSSL_REFCNT},
     *       case-insensitive) stay on the engine axis (handled by {@link #engineProvider(String)}) and leave
     *       {@code jsseProvider} null; any other non-blank value is treated as a JSSE provider name and routed
     *       here, restoring the v4 behavior of honoring that named provider (via the JDK engine) rather than
     *       silently dropping it on upgrade.</li>
     * </ol>
     *
     * @param conf the client configuration
     * @return the JSSE provider name, or {@code null} when none applies
     */
    static String resolveClientJsseProvider(ClientConfigurationData conf) {
        if (StringUtils.isNotBlank(conf.getJsseProvider())) {
            return conf.getJsseProvider().trim();
        }
        String sslProvider = conf.getSslProvider();
        if (StringUtils.isNotBlank(sslProvider) && !isSslProviderEngineLiteral(sslProvider)) {
            return sslProvider.trim();
        }
        return null;
    }

    /**
     * Whether the {@code sslProvider} string names a Netty {@link SslProvider} engine literal ({@code JDK} /
     * {@code OPENSSL} / {@code OPENSSL_REFCNT}, case-insensitive) rather than a JSSE (SSLContext) provider name.
     * Engine literals stay on the engine axis ({@link #engineProvider(String)} maps them to the Netty engine);
     * only other values are routed to the {@code jsseProvider} axis by {@link #resolveClientJsseProvider}. This
     * keeps a valid v4 {@code sslProvider=JDK} on the engine axis instead of misrouting it to a (non-existent)
     * JSSE provider named "JDK", which would fail loudly.
     *
     * @param sslProvider the configured {@code sslProvider} string (may be null/blank)
     * @return whether it is a Netty {@link SslProvider} engine literal
     */
    private static boolean isSslProviderEngineLiteral(String sslProvider) {
        if (StringUtils.isBlank(sslProvider)) {
            return false;
        }
        try {
            SslProvider.valueOf(sslProvider.trim().toUpperCase(Locale.ROOT));
            return true;
        } catch (IllegalArgumentException notAnEngineLiteral) {
            return false;
        }
    }

    /**
     * Compose the {@link TlsPurpose#CLIENT_DEFAULT} {@link TlsPolicy} from the client's {@code tls*}
     * configuration fields. {@code sslProvider} is deliberately NOT part of the policy (it is a
     * factory-engine setting).
     *
     * @param conf the client configuration
     * @return the client-default TLS policy
     */
    public static TlsPolicy clientDefaultPolicy(ClientConfigurationData conf) {
        TlsPolicy.Builder b = TlsPolicy.builder()
                .allowInsecureConnection(conf.isTlsAllowInsecureConnection())
                .enableHostnameVerification(conf.isTlsHostnameVerificationEnable())
                .protocols(toList(conf.getTlsProtocols()))
                .ciphers(toList(conf.getTlsCiphers()))
                .jsseProvider(resolveClientJsseProvider(conf));
        if (conf.isUseKeyStoreTls()) {
            // Map the keystore and truststore types independently (v4 parity): a mixed setup such as a PKCS12
            // keystore with a JKS truststore must load each store with its own type.
            b.format(TlsPolicy.Format.KEYSTORE)
                    .trustStorePath(conf.getTlsTrustStorePath())
                    .trustStorePassword(conf.getTlsTrustStorePassword())
                    .keyStorePath(conf.getTlsKeyStorePath())
                    .keyStorePassword(conf.getTlsKeyStorePassword())
                    .keyStoreType(conf.getTlsKeyStoreType())
                    .trustStoreType(conf.getTlsTrustStoreType());
        } else {
            b.format(TlsPolicy.Format.PEM)
                    .trustCertsFilePath(conf.getTlsTrustCertsFilePath())
                    .certificateFilePath(conf.getTlsCertificateFilePath())
                    .keyFilePath(conf.getTlsKeyFilePath());
        }
        return b.build();
    }

    /**
     * Resolve the effective client TLS factory, initialized and (on the v5-builder path) probed. Since the
     * PIP-337 removal this always returns a non-null factory. Selection precedence (PIP-478):
     * <ol>
     *   <li>a v5-builder-adopted {@code tlsFactory} instance (owned/closed by the client), or a v5
     *       {@code tlsPolicyMap};</li>
     *   <li>the v4 by-name selector {@code tlsFactoryClassName} — a custom {@link PulsarTlsFactory}
     *       instantiated reflectively via its public no-arg constructor, parameterized by
     *       {@code tlsFactoryConfig} (JSON object or {@code key=value} list), the by-name successor of the
     *       removed PIP-337 {@code sslFactoryPlugin};</li>
     *   <li>the built-in file-based factory composed from the {@code tls*} fields.</li>
     * </ol>
     *
     * @param conf             the client configuration
     * @param scheduler        the framework scheduler (for material rotation polling)
     * @param blockingExecutor the framework executor for blocking material loading
     * @param openTelemetry    the telemetry root
     * @return the initialized factory (never {@code null})
     * @throws Exception if the factory cannot be built / initialized, or (v5-builder path) the fail-fast
     *         probe of {@link TlsPurpose#CLIENT_DEFAULT} fails
     */
    public static PulsarTlsFactory resolveClientTlsFactory(ClientConfigurationData conf,
            ScheduledExecutorService scheduler, Executor blockingExecutor, OpenTelemetry openTelemetry)
            throws Exception {
        return resolveClientTlsFactory(conf, scheduler, blockingExecutor, openTelemetry, false);
    }

    /**
     * As {@link #resolveClientTlsFactory(ClientConfigurationData, ScheduledExecutorService, Executor,
     * OpenTelemetry)}, but for a server component's own outbound broker-client lookup transport
     * ({@code brokerClientPurpose = true}) a custom by-name factory is wrapped so an incoming
     * {@link TlsPurpose#CLIENT_DEFAULT} request from the client transport resolves under the fixed
     * {@link TlsPurpose#BROKER_CLIENT} purpose (PIP-478). This is the proxy&rarr;broker binary lookup path,
     * which reuses {@code tlsFactoryClassName} to carry the {@code brokerClientTlsFactoryClassName} selection:
     * the transport hardcodes {@code CLIENT_DEFAULT}, but the stable-API contract resolves all server-component
     * outbound Pulsar-client traffic under {@code BROKER_CLIENT}, so a compliant custom factory serving only
     * {@code BROKER_CLIENT} would otherwise return empty for {@code CLIENT_DEFAULT}. A plain application client
     * ({@code brokerClientPurpose = false}) keeps {@code CLIENT_DEFAULT} as its genuine default. Only the custom
     * by-name factory is wrapped; the built-in file-based path already composes {@code CLIENT_DEFAULT} from the
     * broker-client material.
     *
     * @param brokerClientPurpose whether the resolved factory serves a broker-client outbound transport
     */
    public static PulsarTlsFactory resolveClientTlsFactory(ClientConfigurationData conf,
            ScheduledExecutorService scheduler, Executor blockingExecutor, OpenTelemetry openTelemetry,
            boolean brokerClientPurpose) throws Exception {
        boolean v5BuilderPath = conf.getTlsFactory() != null || conf.getTlsPolicyMap() != null;
        PulsarTlsFactory factory;
        Map<String, String> initParams;
        if (conf.getTlsFactory() != null) {
            // The v5 builder adopted a custom factory; the client owns and closes it.
            factory = conf.getTlsFactory();
            initParams = conf.getTlsFactoryParams() == null ? Map.of() : Map.copyOf(conf.getTlsFactoryParams());
        } else if (conf.getTlsPolicyMap() == null && isCustomFactoryClass(conf.getTlsFactoryClassName())) {
            // PIP-478 v4 by-name successor: no v5-builder instance/policy map is set, and tlsFactoryClassName
            // names a custom PulsarTlsFactory. Instantiate it reflectively and parse tlsFactoryConfig JSON into
            // its init params (mirrors the server-side TlsFactorySupport). The factory self-sources material;
            // the tls* file/keystore composition below does not apply. On a broker-client outbound transport the
            // instance is wrapped so its transport-requested CLIENT_DEFAULT resolves under BROKER_CLIENT.
            PulsarTlsFactory named = instantiateNamedFactory(conf.getTlsFactoryClassName(), "tlsFactoryClassName");
            factory = brokerClientPurpose ? wrapBrokerClientPurpose(named) : named;
            initParams = parseFactoryConfig(conf.getTlsFactoryConfig());
        } else {
            // Fold the auth plugin's TLS identity (AuthenticationTls / AuthenticationKeyStoreTls) into
            // CLIENT_DEFAULT (auth-cert-wins), so a plugin-supplied client certificate reaches the transport
            // now that this is the only client TLS path (PIP-478). The supplier is registered
            // unconditionally and evaluated lazily at context-build time (per connection, off the event loop),
            // so client construction never eagerly calls getAuthData() — no eager OAuth2 token fetch.
            factory = new FileBasedTlsFactory(composePolicies(conf), settings(conf),
                    clientDefaultAuthMaterialSuppliers(conf));
            initParams = conf.getTlsFactoryParams() == null ? Map.of() : Map.copyOf(conf.getTlsFactoryParams());
        }
        initializeBlocking(factory, initContext(initParams, scheduler, blockingExecutor, openTelemetry));
        // Fail-fast probe only on the v5-builder path.
        if (v5BuilderPath) {
            probe(factory, TlsPurpose.CLIENT_DEFAULT,
                    TlsSynthesisSpec.client(conf.isTlsHostnameVerificationEnable()));
        }
        return factory;
    }

    /**
     * Whether a factory class name selects a custom (reflectively-instantiated) {@link PulsarTlsFactory}
     * rather than the built-in file-based default: a non-blank value that is neither the reserved
     * {@code "default"} sentinel nor the default factory's own FQCN.
     *
     * @param factoryClassName the configured factory class name
     * @return whether it names a custom factory
     */
    static boolean isCustomFactoryClass(String factoryClassName) {
        if (StringUtils.isBlank(factoryClassName)) {
            return false;
        }
        String name = factoryClassName.trim();
        return !DEFAULT_FACTORY.equalsIgnoreCase(name) && !FileBasedTlsFactory.class.getName().equals(name);
    }

    /**
     * Reflectively instantiate a named {@link PulsarTlsFactory} via its public no-arg constructor, failing
     * loudly (an {@link IllegalArgumentException} naming the config key) when it cannot be instantiated.
     *
     * @param factoryClassName the factory class name to instantiate
     * @param configKeyName    the config key naming it (for the failure message)
     * @return the instantiated factory
     */
    static PulsarTlsFactory instantiateNamedFactory(String factoryClassName, String configKeyName) {
        String name = factoryClassName.trim();
        // FIX G: honor the thread context classloader (PulsarAdminImpl sets it to the plugin loader via
        // setContextClassLoader) so a factory class visible only through a custom TCCL is found. Plain
        // Class.forName(name) uses only this class's defining loader and misses TCCL-only classes. Fall back
        // to the defining loader when no TCCL is set.
        ClassLoader tccl = Thread.currentThread().getContextClassLoader();
        try {
            Class<?> clazz = tccl != null ? Class.forName(name, true, tccl) : Class.forName(name);
            return (PulsarTlsFactory) clazz.getConstructor().newInstance();
        } catch (ReflectiveOperationException e) {
            throw new IllegalArgumentException(
                    "Could not instantiate " + configKeyName + " '" + factoryClassName + "'", e);
        }
    }

    /**
     * Parse a {@code tlsFactoryConfig} string into the factory init params map (mirrors the server-side
     * {@code TlsFactorySupport.parseFactoryConfig}). A blank value yields an empty map; a value starting with
     * <code>{</code> is parsed as a JSON object; otherwise it is parsed as a comma-separated
     * {@code key=value} list.
     *
     * @param tlsFactoryConfig the configured factory params (may be null/blank)
     * @return an immutable params map (possibly empty)
     */
    static Map<String, String> parseFactoryConfig(String tlsFactoryConfig) {
        if (StringUtils.isBlank(tlsFactoryConfig)) {
            return Map.of();
        }
        String trimmed = tlsFactoryConfig.trim();
        if (trimmed.startsWith("{")) {
            try {
                Map<String, String> parsed = ObjectMapperFactory.getMapper().reader()
                        .forType(new TypeReference<Map<String, String>>() {})
                        .readValue(trimmed);
                return parsed == null ? Map.of() : Map.copyOf(parsed);
            } catch (Exception e) {
                throw new IllegalArgumentException("Failed to parse tlsFactoryConfig as a JSON object", e);
            }
        }
        Map<String, String> map = new LinkedHashMap<>();
        for (String pair : trimmed.split(",")) {
            String entry = pair.trim();
            if (entry.isEmpty()) {
                continue;
            }
            int eq = entry.indexOf('=');
            if (eq < 0) {
                map.put(entry, "");
            } else {
                map.put(entry.substring(0, eq).trim(), entry.substring(eq + 1).trim());
            }
        }
        return Map.copyOf(map);
    }

    /**
     * Build the (uninitialized) broker-client {@link PulsarTlsFactory} for a server component's own outbound
     * Pulsar client — geo-replication and cluster-internal lookup connections (the {@code BROKER_CLIENT}
     * purpose, PIP-478). The broker-client material is already mapped onto the config's
     * {@code tls*} fields (per-cluster {@code ClusterData.brokerClientTls*} first, else the broker's
     * {@code brokerClient*} {@code ServiceConfiguration}), so this composes {@link TlsPurpose#CLIENT_DEFAULT}
     * — the purpose the outbound transport requests — from those fields, additionally folding the
     * component's broker-client {@code Authentication} TLS material via the 3c
     * {@link FileBasedTlsFactory#authMaterialSupplier authMaterialSupplier} (auth-cert-wins) so an
     * {@code AuthenticationTls} broker-client identity reaches the transport on the new path.
     *
     * <p>The caller stores the result on {@code conf} via {@code setTlsFactory}; the owning
     * {@code PulsarClientImpl} adopts, initializes (with its shared scheduler / executor / OpenTelemetry) and
     * closes it — one factory per outbound client, so per-cluster material is served without minting
     * per-cluster purposes. A non-default {@code factoryClassName} names a custom {@link PulsarTlsFactory},
     * instantiated reflectively (it self-sources material; the config/auth composition below does not apply,
     * and {@code brokerClientTlsFactoryConfig} params are not yet plumbed to the client init context).
     *
     * @param conf            the outbound client configuration (its {@code tls*} fields hold the broker-client
     *                        material; {@code getAuthentication()} the broker-client authentication)
     * @param factoryClassName the {@code brokerClientTlsFactoryClassName} value ({@code default}/blank selects
     *                        the built-in file-based factory)
     * @return an uninitialized {@link PulsarTlsFactory}
     */
    public static PulsarTlsFactory brokerClientTlsFactory(ClientConfigurationData conf, String factoryClassName) {
        if (isCustomFactoryClass(factoryClassName)) {
            // The client transport (PulsarChannelInitializer) requests CLIENT_DEFAULT, but a custom broker-client
            // factory resolves under the fixed BROKER_CLIENT purpose (PIP-478 stable-API contract), so wrap it to
            // translate CLIENT_DEFAULT -> BROKER_CLIENT before delegating.
            return wrapBrokerClientPurpose(
                    instantiateNamedFactory(factoryClassName, "brokerClientTlsFactoryClassName"));
        }
        // Broker-client (server startup / replication client creation): register the broker-client TLS fold
        // when the plugin carries TLS material.
        TlsPolicy policy = clientDefaultPolicy(conf);
        Map<TlsPurpose, Supplier<AuthenticationDataProvider>> authSuppliers = Map.of();
        Authentication auth = conf.getAuthentication();
        if (auth != null && shouldFoldBrokerClientAuthTls(auth)) {
            authSuppliers = Map.of(TlsPurpose.CLIENT_DEFAULT, FileBasedTlsFactory.authMaterialSupplier(auth));
        }
        return new FileBasedTlsFactory(Map.of(TlsPurpose.CLIENT_DEFAULT, policy), settings(conf), authSuppliers);
    }

    /**
     * Whether to register the broker-client {@code BROKER_CLIENT} TLS fold for this plugin. The
     * built-in TLS-auth plugins are matched by TYPE ({@code AuthenticationTls} / {@code AuthenticationKeyStoreTls})
     * so a transient read failure at factory build — the key file briefly missing while it is being rotated —
     * does not permanently skip the fold and disable rotation; {@code AuthProvidedMaterialSource} keeps the
     * last-good material and retries per poll. For an unknown custom plugin fall back to the material probe,
     * preserving support for third-party plugins that supply TLS material without extending the built-in types.
     */
    private static boolean shouldFoldBrokerClientAuthTls(Authentication auth) {
        return isTlsAuthPlugin(auth) || authHasTlsMaterial(auth);
    }

    /**
     * The auth-cert-wins {@code authMaterialSupplier} fold for the application/admin client's
     * {@link TlsPurpose#CLIENT_DEFAULT}: a plugin-supplied client identity (e.g. {@code AuthenticationTls} /
     * {@code AuthenticationKeyStoreTls}) reaches the transport on the new path. Unlike the broker-client
     * path there is no eager probe — the supplier is evaluated lazily at context build/reload, so client
     * construction never calls {@code getAuthData()}. The fold is registered when the identity must come from
     * the plugin (no {@code tls*} key material configured) or when a TLS-auth plugin is present alongside
     * {@code tls*} files (auth-cert-wins); a non-TLS plugin is not consulted when {@code tls*} files supply
     * the identity, so its {@code getAuthData()} stays untouched. {@code AuthProvidedMaterialSource} applies
     * auth-cert-wins only when the plugin actually carries TLS material.
     */
    private static Map<TlsPurpose, Supplier<AuthenticationDataProvider>> clientDefaultAuthMaterialSuppliers(
            ClientConfigurationData conf) {
        Authentication auth = conf.getAuthentication();
        if (auth == null) {
            return Map.of();
        }
        if (hasConfiguredClientKeyMaterial(conf) && !isTlsAuthPlugin(auth)) {
            // The client already has its own tls* cert/key (or keystore) identity, and the auth plugin is not
            // a TLS-auth plugin, so it is not consulted for TLS material — its getAuthData() is not called
            // (e.g. a non-TLS plugin, per TlsProducerConsumerTest.testTlsWithFakeAuthentication, which asserts
            // getAuthData() is never called). A genuine TLS-auth plugin, however, still overrides the tls*
            // files (auth-cert-wins, the removed PIP-337 behavior) — see below.
            return Map.of();
        }
        // Register the fold when the identity must come from the auth plugin (no tls* key material) OR when a
        // TLS-auth plugin (AuthenticationTls / AuthenticationKeyStoreTls) is present alongside tls* files: the
        // plugin certificate wins over the tls* files (auth-cert-wins), restoring the removed PIP-337
        // precedence so a broker/proxy whose broker-client identity comes from an AuthenticationTls plugin
        // presents that certificate rather than its configured brokerClient* cert files (issue: the internal
        // broker-client otherwise presented a server-only-EKU cert and was rejected for TLS client auth).
        // The supplier is evaluated lazily at CLIENT_DEFAULT build/reload (off the event loop), so client
        // construction never eagerly calls getAuthData().
        Supplier<AuthenticationDataProvider> supplier = FileBasedTlsFactory.authMaterialSupplier(auth);
        if (isTlsAuthPlugin(auth)) {
            // A TLS-auth plugin owns the client identity, so register the supplier directly (no swallow): a
            // transient read failure — e.g. the key file briefly missing while it is being rotated, per
            // AdminApiTlsAuthTest.testCertRefreshForPulsarAdmin — must propagate so AuthProvidedMaterialSource
            // keeps the last-good certificate. Swallowing it to null would instead fold nothing and downgrade
            // the identity to "no client certificate", which strands a pooled admin/HTTP connection that
            // completed a handshake presenting no cert (rejected 401 and, being non-5xx, kept alive and
            // reused) so the rotated certificate would never take effect.
            return Map.of(TlsPurpose.CLIENT_DEFAULT, supplier);
        }
        // A non-TLS plugin (token / OAuth2) has no client certificate to fold; when it is registered here it is
        // because no tls* key material is configured. Swallow a lookup failure to null so client build does not
        // fail (e.g. an OAuth2 plugin whose getAuthData() acquires a token and fails when the plugin is not
        // bound to a client); AuthProvidedMaterialSource then keeps the empty base material.
        return Map.of(TlsPurpose.CLIENT_DEFAULT, () -> {
            try {
                return supplier.get();
            } catch (RuntimeException e) {
                return null;
            }
        });
    }

    /**
     * Whether an authentication plugin is one of the built-in TLS-auth plugins that carry a client
     * certificate identity ({@link AuthenticationTls} / {@link AuthenticationKeyStoreTls}). Such a plugin's
     * certificate overrides the configured {@code tls*} key material (auth-cert-wins); any other plugin is
     * consulted for TLS material only when no {@code tls*} key material is configured, so its
     * {@code getAuthData()} is not called when the client already has its own certificate files.
     */
    private static boolean isTlsAuthPlugin(Authentication auth) {
        return auth instanceof AuthenticationTls || auth instanceof AuthenticationKeyStoreTls;
    }

    private static boolean hasConfiguredClientKeyMaterial(ClientConfigurationData conf) {
        if (conf.isUseKeyStoreTls()) {
            return StringUtils.isNotBlank(conf.getTlsKeyStorePath());
        }
        return StringUtils.isNotBlank(conf.getTlsCertificateFilePath())
                && StringUtils.isNotBlank(conf.getTlsKeyFilePath());
    }

    /**
     * Probe whether an authentication plugin carries TLS key material, so the broker-client fold registers
     * an {@code authMaterialSupplier} only for a genuine TLS-auth plugin (e.g. {@code AuthenticationTls}) and
     * not for a token/none plugin whose per-poll {@code getAuthData()} would be wasted work. Runs once at
     * factory-build time on the caller thread (broker startup / replication client creation), never on an
     * event loop.
     *
     * @param auth the broker-client authentication (never {@code null})
     * @return whether it exposes TLS material
     */
    @SuppressWarnings("deprecation")
    private static boolean authHasTlsMaterial(Authentication auth) {
        try {
            AuthenticationDataProvider data = auth.getAuthData();
            return data != null && data.hasDataForTls();
        } catch (Exception e) {
            return false;
        }
    }

    private static Map<TlsPurpose, TlsPolicy> composePolicies(ClientConfigurationData conf) {
        Map<TlsPurpose, TlsPolicy> policies = new LinkedHashMap<>();
        if (conf.getTlsPolicyMap() != null) {
            policies.putAll(conf.getTlsPolicyMap());
        }
        // Always ensure CLIENT_DEFAULT resolves — derive it from the conf fields if the v5 builder did
        // not supply one explicitly.
        policies.putIfAbsent(TlsPurpose.CLIENT_DEFAULT, clientDefaultPolicy(conf));
        // PIP-478: fold an OAuth2 plugin's own IdP TLS material (trustCerts / cert / key params,
        // issue #24944) into CLIENT_OAUTH2, so the framework HTTP client serves IdP mTLS / custom trust on
        // the new path instead of the deprecated private client. Only when the plugin carries IdP TLS
        // material; otherwise CLIENT_OAUTH2 keeps resolving to the system default (its empty fallback). The
        // plugin is reachable here because conf.getAuthentication() is set before this runs (v5 build /
        // client construction).
        foldOAuth2IdpPolicy(conf, policies);
        return policies;
    }

    private static void foldOAuth2IdpPolicy(ClientConfigurationData conf, Map<TlsPurpose, TlsPolicy> policies) {
        if (conf.getAuthentication() instanceof AuthenticationOAuth2 oauth2) {
            oauth2.idpTlsPolicy().ifPresent(policy -> policies.putIfAbsent(TlsPurpose.CLIENT_OAUTH2, policy));
        }
    }

    private static FileBasedTlsFactorySettings settings(ClientConfigurationData conf) {
        int refresh = conf.getAutoCertRefreshSeconds();
        return FileBasedTlsFactorySettings.builder()
                .engineProvider(engineProvider(conf.getSslProvider()))
                // Client purposes never build server contexts, so requireTrustedClientCert is irrelevant.
                .refreshIntervalSeconds(refresh)
                .build();
    }

    /**
     * Fail-fast probe: build one instance of the purpose and dispose it, surfacing a configuration error
     * (e.g. a missing cert file) as an actionable {@link IllegalArgumentException}. Acquisition goes through
     * {@link TlsContextAcquisition}, so a custom factory that supplies only the JDK {@code SSLContext}
     * fallback probes successfully via the framework-synthesized Netty context.
     *
     * @param factory   the initialized factory
     * @param purpose   the purpose to probe
     * @param synthesis the settings baked into a synthesized Netty context on the fallback path
     */
    public static void probe(PulsarTlsFactory factory, TlsPurpose purpose, TlsSynthesisSpec synthesis) {
        try {
            Optional<TlsHandle<SslContext>> handle =
                    TlsContextAcquisition.acquireNettyContext(factory, purpose, synthesis).get();
            if (handle.isEmpty()) {
                throw new IllegalStateException("Client TLS factory " + factory.getClass().getName()
                        + " supplied no Netty SslContext for purpose " + purpose);
            }
            handle.get().dispose();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while probing the client TLS factory", e);
        } catch (ExecutionException | CompletionException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            throw new IllegalArgumentException("Client TLS configuration is invalid for purpose " + purpose
                    + ": " + cause.getMessage(), cause);
        }
    }

    private static void initializeBlocking(PulsarTlsFactory factory, TlsFactoryInitContext context)
            throws Exception {
        try {
            factory.initialize(context).get();
        } catch (ExecutionException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            if (cause instanceof Exception ex) {
                throw ex;
            }
            throw new RuntimeException(cause);
        }
    }

    private static TlsFactoryInitContext initContext(Map<String, String> params,
            ScheduledExecutorService scheduler, Executor blockingExecutor, OpenTelemetry openTelemetry) {
        OpenTelemetry ot = openTelemetry == null ? OpenTelemetry.noop() : openTelemetry;
        // PIP-478: deliver the factory params to a custom factory — the broker-supplied params (from
        // brokerClientTlsFactoryConfig) or the v4 client's parsed tlsFactoryConfig; empty for the default
        // file-based factory, which ignores them.
        Map<String, String> safeParams = params == null ? Map.of() : Map.copyOf(params);
        return new TlsFactoryInitContext() {
            @Override
            public Map<String, String> params() {
                return safeParams;
            }

            @Override
            public ScheduledExecutorService scheduler() {
                return scheduler;
            }

            @Override
            public Executor blockingExecutor() {
                return blockingExecutor;
            }

            @Override
            public Clock clock() {
                return Clock.systemUTC();
            }

            @Override
            public OpenTelemetry openTelemetry() {
                return ot;
            }
        };
    }

    /**
     * Wrap a custom broker-client {@link PulsarTlsFactory} so an incoming {@link TlsPurpose#CLIENT_DEFAULT}
     * request — the purpose the outbound client transport hardcodes — is resolved under the fixed
     * {@link TlsPurpose#BROKER_CLIENT} purpose the PIP-478 stable-API contract assigns to a server component's
     * own outbound Pulsar-client traffic (geo-replication, proxy&rarr;broker, websocket&rarr;broker,
     * functions-worker&rarr;broker). Without this a compliant custom factory serving only {@code BROKER_CLIENT}
     * would return {@code empty()} for {@code CLIENT_DEFAULT} (hard connect failure), or mirror the default's
     * unconfigured-client-purpose fallback and silently downgrade to a no-client-certificate handshake. Only
     * custom factories are wrapped; the built-in file-based factory already maps the broker-client material to
     * whatever purpose the transport requests.
     *
     * @param delegate the custom broker-client factory
     * @return a factory translating {@code CLIENT_DEFAULT} to {@code BROKER_CLIENT}
     */
    @VisibleForTesting
    static PulsarTlsFactory wrapBrokerClientPurpose(PulsarTlsFactory delegate) {
        return new BrokerClientPurposeFactory(delegate);
    }

    /**
     * Delegating {@link PulsarTlsFactory} that translates an incoming {@link TlsPurpose#CLIENT_DEFAULT} request
     * to {@link TlsPurpose#BROKER_CLIENT} across all {@code createInstance} overloads before delegating, leaving
     * every other purpose untouched; {@link #initialize} and {@link #close} pass through. See
     * {@link #wrapBrokerClientPurpose} for why.
     */
    private static final class BrokerClientPurposeFactory implements PulsarTlsFactory {
        private final PulsarTlsFactory delegate;

        BrokerClientPurposeFactory(PulsarTlsFactory delegate) {
            this.delegate = delegate;
        }

        private static TlsPurpose translate(TlsPurpose purpose) {
            return TlsPurpose.CLIENT_DEFAULT.equals(purpose) ? TlsPurpose.BROKER_CLIENT : purpose;
        }

        @Override
        public CompletableFuture<Void> initialize(TlsFactoryInitContext context) {
            return delegate.initialize(context);
        }

        @Override
        public <T> CompletableFuture<Optional<TlsHandle<T>>> createInstance(
                TlsPurpose purpose, Class<T> instanceClass) {
            return delegate.createInstance(translate(purpose), instanceClass);
        }

        @Override
        public <T> CompletableFuture<Optional<TlsHandle<T>>> createInstance(
                TlsPurpose purpose, TlsEndpoint endpoint, Class<T> instanceClass) {
            return delegate.createInstance(translate(purpose), endpoint, instanceClass);
        }

        @Override
        public <T> CompletableFuture<Optional<TlsHandle<T>>> createInstance(
                TlsPurpose purpose, Class<T> instanceClass, Consumer<T> onLoadOrReload) {
            return delegate.createInstance(translate(purpose), instanceClass, onLoadOrReload);
        }

        @Override
        public void close() {
            delegate.close();
        }
    }

    private static List<String> toList(Set<String> values) {
        return values == null ? List.of() : new ArrayList<>(values);
    }
}
