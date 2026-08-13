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
package org.apache.pulsar.common.tls.impl;

import io.netty.handler.ssl.ApplicationProtocolConfig;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.IdentityCipherSuiteFilter;
import io.netty.handler.ssl.JdkSslContext;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import java.security.GeneralSecurityException;
import java.security.KeyStoreException;
import java.security.Provider;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import lombok.CustomLog;
import org.apache.pulsar.common.util.tls.JcaProviders;
import org.apache.pulsar.common.util.tls.JdkSslContexts;
import org.apache.pulsar.tls.TlsPolicy;

/**
 * Builds the well-known TLS instance classes from loaded {@link TlsMaterial} and a {@link TlsPolicy}
 * for the default {@code FileBasedTlsFactory}, and synthesizes the richer objects from a JDK
 * {@code SSLContext} for the framework fallback (PIP-478).
 *
 * <p>This class owns TLS context assembly end-to-end, composing the focused low-level primitives rather
 * than a general-purpose helper: it consumes already-parsed in-memory {@link TlsMaterial} (PEM and keystore
 * parsing lives in the material sources), builds the JDK {@code SSLContext} through {@link JdkSslContexts},
 * and assembles the Netty {@link SslContext} objects inline through {@link SslContextBuilder}. The Netty
 * contexts are built here rather than delegated because two per-context settings must be baked at build time
 * from the in-memory material:
 * <ul>
 *   <li><b>Client hostname verification</b> — set via
 *       {@link SslContextBuilder#endpointIdentificationAlgorithm(String)} to {@code "HTTPS"} when the
 *       policy enables it. It cannot be left to a per-{@code SSLEngine} override: Netty's OpenSSL
 *       backend fixes the endpoint-identification algorithm at build time.</li>
 *   <li><b>Server insecure trust</b> — {@code policy.allowInsecureConnection()} on a
 *       server context installs {@link InsecureTrustManagerFactory#INSTANCE} so an untrusted client
 *       certificate still completes the handshake and remains available for TLS authentication, while
 *       client-auth stays {@link ClientAuth#REQUIRE}/{@link ClientAuth#OPTIONAL} (never
 *       {@link ClientAuth#NONE}, per {@code tlsRequireTrustedClientCert} semantics).</li>
 * </ul>
 */
@CustomLog
public final class TlsContexts {

    /**
     * The default enabled TLS protocol set, applied whenever the effective protocol list is empty — a policy
     * with no {@code protocols()}, or a synthesized context whose factory companion set none. This preserves
     * the {@code {TLSv1.3, TLSv1.2}} floor v4's {@code DefaultPulsarSslFactory} forced at engine build:
     * without it the PIP-478 path would silently defer to the JVM/provider default protocol set, a
     * security-relevant drift on upgrade.
     */
    public static final List<String> DEFAULT_ENABLED_PROTOCOLS = List.of("TLSv1.3", "TLSv1.2");

    // WARN-once dedup for the insecure (trust-all) mode, keyed by a hash of the policy so a rotation's
    // context rebuilds (and both the Netty and JDK builds of the same policy) log at most once.
    //
    // Deliberately NOT keyed by the TlsPolicy itself: the policy carries keyStorePassword/trustStorePassword,
    // and this set is static and never cleared, so retaining policies would pin plaintext passwords for the
    // JVM lifetime — once per distinct insecure policy a long-lived process ever builds. A hash costs at most
    // one suppressed warning on a collision. The set is also capped, because a process that builds many
    // varying insecure client policies would otherwise grow it without bound.
    private static final int INSECURE_WARNED_CAPACITY = 1024;
    private static final Set<Integer> INSECURE_WARNED = ConcurrentHashMap.newKeySet();

    private TlsContexts() {
    }

    /**
     * Log a one-time WARN when an insecure (trust-all) TLS policy is applied at context build: peer
     * certificates are not validated. Deduplicated per policy value so rotation rebuilds do not spam the log.
     */
    private static void warnInsecureModeOnce(TlsPolicy policy) {
        if (!policy.allowInsecureConnection()) {
            return;
        }
        // Past the cap, warn every time rather than silently stopping: a flood of identical warnings is a
        // better failure than losing the signal that peer verification is off.
        if (INSECURE_WARNED.size() < INSECURE_WARNED_CAPACITY && !INSECURE_WARNED.add(policy.hashCode())) {
            return;
        }
        log.warn().log("TLS insecure mode is enabled (allowInsecureConnection=true): peer certificates are "
                + "NOT validated (trust-all). This disables authentication of the remote peer and must be "
                + "used only for testing.");
    }

    /**
     * Build a client Netty {@link SslContext}, baking hostname verification when the policy enables it.
     *
     * @param material the loaded client material
     * @param policy   the policy (flags, protocols, ciphers)
     * @param provider the Netty SSL provider (engine selection)
     * @return the built client context
     * @throws Exception if the context cannot be built
     */
    static SslContext buildNettyClientContext(TlsMaterial material, TlsPolicy policy, SslProvider provider)
            throws Exception {
        SslContextBuilder builder = SslContextBuilder.forClient();
        applyEngineProvider(builder, policy, provider);
        applyTrust(builder, material, policy);
        applyKeyManager(builder, material, policy);
        applyCiphersAndProtocols(builder, policy);
        // Netty 4.2 defaults client contexts to "HTTPS" endpoint identification, so the algorithm must be
        // set explicitly both ways: hostname verification is the policy's decision alone, and an engine
        // default must not verify hostnames when the policy disabled it.
        builder.endpointIdentificationAlgorithm(policy.enableHostnameVerification() ? "HTTPS" : null);
        return builder.build();
    }

    /**
     * Build a server Netty {@link SslContext} honoring the insecure rule.
     *
     * @param material                 the loaded server material
     * @param policy                   the policy (flags, protocols, ciphers)
     * @param provider                 the Netty SSL provider (engine selection)
     * @param requireTrustedClientCert whether to require (vs. merely request) a trusted client cert
     * @return the built server context
     * @throws Exception if the context cannot be built
     */
    static SslContext buildNettyServerContext(TlsMaterial material, TlsPolicy policy, SslProvider provider,
                                              boolean requireTrustedClientCert) throws Exception {
        // Keystore material builds the server identity from the whole keystore (multi-alias selection); PEM
        // material has a single server key + chain, routed through a KeyManagerFactory only when a provider
        // axis is pinned (see applyKeyManager).
        SslContextBuilder builder = serverBuilder(material, policy);
        applyEngineProvider(builder, policy, provider);
        applyTrust(builder, material, policy);
        applyCiphersAndProtocols(builder, policy);
        // Never drop to ClientAuth.NONE, even when insecure: a captured client cert powers TLS auth.
        builder.clientAuth(requireTrustedClientCert ? ClientAuth.REQUIRE : ClientAuth.OPTIONAL);
        return builder.build();
    }

    /**
     * Build a JDK {@link SSLContext} for the material (the universal fallback and non-Netty consumers).
     *
     * @param material the loaded material
     * @param policy   the policy (only the insecure flag affects a JDK context)
     * @return the built JDK context
     * @throws Exception if the context cannot be built
     */
    static SSLContext buildJdkContext(TlsMaterial material, TlsPolicy policy) throws Exception {
        warnInsecureModeOnce(policy);
        // Resolve both provider axes once per build: the JSSE provider (SSLContext/KeyManagerFactory/
        // TrustManagerFactory) and the JCA provider (KeyStore/CertificateFactory), so the build sees one
        // consistent pair and each ServiceLoader walk happens once.
        Provider jsseProvider = resolveJsseProvider(policy);
        Provider jcaProvider = resolveJcaProvider(policy);
        // Keystore material builds KeyManagers from the whole keystore so JSSE preserves multi-alias selection;
        // PEM material has a single key + chain. Honor a pinned jsseProvider (a JSSE java.security.Provider) on
        // the JDK-engine path (the web/Jetty and fallback consumers), so a FIPS JSSE provider (BCJSSE) builds the
        // SSLContext — and the KeyManagerFactory — too (Goal #5).
        if (material.hasKeyStoreEntries()) {
            KeyManagerFactory kmf = buildKeyManagerFactory(material, jsseProvider, jcaProvider);
            return JdkSslContexts.createSslContextWithProvider(policy.allowInsecureConnection(),
                    material.trustCertsArray(), kmf.getKeyManagers(), jsseProvider, jcaProvider);
        }
        return JdkSslContexts.createSslContextWithProvider(policy.allowInsecureConnection(),
                material.trustCertsArray(), material.keyCertChainArray(), material.privateKey(), jsseProvider,
                jcaProvider);
    }

    /**
     * Build a {@link KeyManagerFactory} from keystore material's full key-entry set (multi-alias selection),
     * reconstructing an in-memory keystore from the value-typed entries and honoring a pinned
     * {@code jsseProvider} on the factory (the round-1 provider-negotiated algorithm selection).
     */
    private static KeyManagerFactory buildKeyManagerFactory(TlsMaterial material, TlsPolicy policy) throws Exception {
        return buildKeyManagerFactory(material, resolveJsseProvider(policy), resolveJcaProvider(policy));
    }

    private static KeyManagerFactory buildKeyManagerFactory(TlsMaterial material, Provider jsseProvider,
                                                            Provider jcaProvider) throws Exception {
        // The carrier keystore and the password its entries are stored under travel together, so the two can
        // never drift apart; the password is generated per build and zeroed once the factory has copied it.
        TlsKeyStoreLoader.InMemoryKeyStore carrier =
                TlsKeyStoreLoader.toInMemoryKeyStore(material.keyEntries(), jcaProvider);
        try {
            return JdkSslContexts.createKeyManagerFactory(carrier.keyStore(), carrier.password(), jsseProvider);
        } finally {
            Arrays.fill(carrier.password(), '\0');
        }
    }

    /**
     * Apply the effective Netty engine to the builder. When the policy pins a {@code jsseProvider} (a JSSE
     * {@link Provider} name), use the JDK engine with that provider installed as the SSLContext provider —
     * overriding the factory-level engine choice, since Netty's native OpenSSL engine cannot delegate to an
     * arbitrary JSSE provider (PIP-478). Otherwise use the passed-in engine provider.
     */
    private static void applyEngineProvider(SslContextBuilder builder, TlsPolicy policy, SslProvider engineProvider) {
        Provider jsseProvider = resolveJsseProvider(policy);
        if (jsseProvider != null) {
            builder.sslProvider(SslProvider.JDK).sslContextProvider(jsseProvider);
        } else {
            builder.sslProvider(engineProvider);
        }
    }

    /**
     * Resolve the policy's {@code jsseProvider} to a {@link Provider} (fail-loud via {@link JcaProviders}), or
     * {@code null} when the policy pins no JSSE provider.
     */
    private static Provider resolveJsseProvider(TlsPolicy policy) {
        String name = policy.jsseProvider();
        return (name == null || name.isBlank()) ? null : JcaProviders.resolveNamedProvider(name);
    }

    /**
     * Resolve the policy's {@code jcaProvider} — the material axis (KeyStore/CertificateFactory), never the
     * JSSE service types — to a {@link Provider} (fail-loud via {@link JcaProviders}), or {@code null} when
     * the policy pins none.
     */
    private static Provider resolveJcaProvider(TlsPolicy policy) {
        return JcaProviders.resolveNamedProvider(policy.jcaProvider());
    }

    /**
     * Wrap a JDK {@link SSLContext} as a Netty {@link SslContext} via {@link JdkSslContext}. Used by the
     * framework when a custom factory returns {@code empty()} for the Netty class but supplies the JDK
     * {@code SSLContext} fallback.
     *
     * @param sslContext               the JDK context to wrap
     * @param isClient                 whether the wrapped context is for client-mode engines
     * @param requireTrustedClientCert server-side client-auth requirement (ignored when {@code isClient})
     * @return a Netty context backed by the JDK context
     */
    public static SslContext synthesizeNettyFromJdk(SSLContext sslContext, boolean isClient,
                                                    boolean requireTrustedClientCert) {
        ClientAuth clientAuth = isClient ? ClientAuth.NONE
                : (requireTrustedClientCert ? ClientAuth.REQUIRE : ClientAuth.OPTIONAL);
        return new JdkSslContext(sslContext, isClient, null, IdentityCipherSuiteFilter.INSTANCE,
                ApplicationProtocolConfig.DISABLED, clientAuth, null, false);
    }

    /**
     * Synthesize a <em>client</em> Netty {@link SslContext} from a JDK {@link SSLContext}, applying the
     * factory's engine-policy {@code SSLParameters} companion (when present) and the consumer's
     * hostname-verification flag to the produced engines. Used by the framework when a custom factory returns
     * {@code empty()} for the Netty class on a client purpose but supplies the JDK {@code SSLContext} fallback.
     *
     * <p>Engine-level policy cannot be encoded in the JDK {@code SSLContext} itself — the enabled protocols
     * and cipher suites, algorithm constraints, application protocols, and the endpoint-identification
     * algorithm are all {@link SSLParameters} settings — so the composed baseline is applied per engine by a
     * {@link SynthesizedEngineSslContext} wrapper. This mirrors the native path, where
     * {@link #buildNettyClientContext} bakes the same settings into the context via {@code SslContextBuilder};
     * consumers rely on the context to carry the policy and never re-apply it per connection.
     *
     * <p>Merge order (PIP-478): the factory {@code SSLParameters} form the baseline (non-null members only);
     * for {@code endpointIdentificationAlgorithm} the factory's value wins when set, otherwise
     * {@code enableHostnameVerification} applies {@code "HTTPS"}; SNI {@code serverNames} are never taken from
     * the factory (the per-connection SNI wins). When the factory companion sets no enabled protocols, the
     * {@link #DEFAULT_ENABLED_PROTOCOLS} floor is applied so a synthesized client engine matches the native
     * path's default.
     *
     * @param sslContext                 the JDK context to wrap
     * @param enableHostnameVerification whether the client policy enables hostname verification
     * @param factoryBaseline            the factory's engine-policy companion, or {@code null} if none supplied
     * @return a Netty client context backed by the JDK context, carrying the composed engine policy
     */
    public static SslContext synthesizeNettyClientFromJdk(SSLContext sslContext,
                                                          boolean enableHostnameVerification,
                                                          SSLParameters factoryBaseline) {
        SslContext clientContext = synthesizeNettyFromJdk(sslContext, true, false);
        SSLParameters overlay = composeClientOverlay(factoryBaseline, enableHostnameVerification);
        return new SynthesizedEngineSslContext(clientContext, overlay, false);
    }

    /**
     * Synthesize a <em>server</em> Netty {@link SslContext} from a JDK {@link SSLContext}, applying the
     * factory's engine-policy {@code SSLParameters} companion (when present) to the produced engines. Used by
     * the framework when a custom factory returns {@code empty()} for the Netty class on a server purpose but
     * supplies the JDK {@code SSLContext} fallback.
     *
     * <p>The consumer's {@code requireTrustedClientCert} is mapped onto the base context as usual
     * ({@link ClientAuth#REQUIRE}/{@link ClientAuth#OPTIONAL}). When a companion is supplied its non-null
     * baseline members (protocols/ciphers/algorithm-constraints/application-protocols) are overlaid per engine
     * and — merge rule 4 — its {@code needClientAuth}/{@code wantClientAuth} are authoritative for the
     * client-auth mode; with no companion the base context's client-auth mode is left untouched. In both cases,
     * when no enabled protocols were set the {@link #DEFAULT_ENABLED_PROTOCOLS} floor is applied.
     *
     * @param sslContext               the JDK context to wrap
     * @param requireTrustedClientCert the consumer's client-auth requirement (used when no companion supplied)
     * @param factoryBaseline          the factory's engine-policy companion, or {@code null} if none supplied
     * @return a Netty server context backed by the JDK context, carrying the composed engine policy
     */
    public static SslContext synthesizeNettyServerFromJdk(SSLContext sslContext,
                                                          boolean requireTrustedClientCert,
                                                          SSLParameters factoryBaseline) {
        SslContext serverContext = synthesizeNettyFromJdk(sslContext, false, requireTrustedClientCert);
        // Apply the companion's client-auth mode (merge rule 4) only when a companion was supplied; with none,
        // the base context already carries the consumer's client-auth mode and the overlay pins only protocols.
        SSLParameters overlay = factoryBaseline == null ? new SSLParameters() : copyBaselineMembers(factoryBaseline);
        applyDefaultProtocolsIfUnset(overlay);
        return new SynthesizedEngineSslContext(serverContext, overlay, factoryBaseline != null);
    }

    /**
     * Compose the per-engine overlay for a client purpose. Always non-null: at minimum it pins the
     * {@link #DEFAULT_ENABLED_PROTOCOLS} floor, plus the factory's baseline members and — merge rule 2 —
     * the endpoint-identification algorithm (factory wins when set, else the consumer's hostname-verification
     * flag applies {@code "HTTPS"}).
     */
    private static SSLParameters composeClientOverlay(SSLParameters factoryBaseline,
                                                      boolean enableHostnameVerification) {
        SSLParameters overlay = factoryBaseline == null ? new SSLParameters() : copyBaselineMembers(factoryBaseline);
        applyDefaultProtocolsIfUnset(overlay);
        // Merge rule 2: the factory's endpointIdentificationAlgorithm wins when set; otherwise the consumer's
        // hostname-verification flag applies "HTTPS".
        if (overlay.getEndpointIdentificationAlgorithm() == null && enableHostnameVerification) {
            overlay.setEndpointIdentificationAlgorithm("HTTPS");
        }
        return overlay;
    }

    /** Pin {@link #DEFAULT_ENABLED_PROTOCOLS} on an overlay whose enabled protocols were left unset (B3). */
    private static void applyDefaultProtocolsIfUnset(SSLParameters overlay) {
        if (overlay.getProtocols() == null) {
            overlay.setProtocols(DEFAULT_ENABLED_PROTOCOLS.toArray(new String[0]));
        }
    }

    /**
     * Take the framework's single defensive snapshot of a factory-supplied {@link SSLParameters} — a mutable
     * object — copying only the engine-baseline members the framework overlays per engine. SNI
     * {@code serverNames} are deliberately excluded (merge rule 3: the per-connection SNI always wins). The
     * returned object is owned by the framework and never mutated after composition, so a later mutation of
     * the factory's original object cannot affect an already-acquired context.
     */
    private static SSLParameters copyBaselineMembers(SSLParameters source) {
        SSLParameters overlay = new SSLParameters();
        if (source.getProtocols() != null) {
            overlay.setProtocols(source.getProtocols().clone());
        }
        if (source.getCipherSuites() != null) {
            overlay.setCipherSuites(source.getCipherSuites().clone());
        }
        if (source.getAlgorithmConstraints() != null) {
            overlay.setAlgorithmConstraints(source.getAlgorithmConstraints());
        }
        if (source.getApplicationProtocols() != null) {
            overlay.setApplicationProtocols(source.getApplicationProtocols().clone());
        }
        if (source.getEndpointIdentificationAlgorithm() != null) {
            overlay.setEndpointIdentificationAlgorithm(source.getEndpointIdentificationAlgorithm());
        }
        // Client-auth mode is carried on the overlay for server purposes (applied by
        // SynthesizedEngineSslContext when applyClientAuth is set); needClientAuth wins over wantClientAuth.
        if (source.getNeedClientAuth()) {
            overlay.setNeedClientAuth(true);
        } else if (source.getWantClientAuth()) {
            overlay.setWantClientAuth(true);
        }
        return overlay;
    }

    /**
     * Install the trust anchors on a Netty builder.
     *
     * <p>Handing Netty the raw certificate array is the historical (and cheapest) path, but Netty then builds
     * the carrier {@code KeyStore} and the {@code TrustManagerFactory} through the JVM provider search order —
     * {@code SslContext.buildTrustManagerFactory} calls {@code TrustManagerFactory.getInstance(alg)} with no
     * provider — which silently defeats a {@code jsseProvider}/{@code jcaProvider} pin, since
     * {@code sslContextProvider} pins only the {@code SSLContext} itself. So when either axis is pinned, build
     * the trust managers here through the pinned providers and hand Netty the resulting {@code TrustManager}
     * instead. With neither axis pinned the raw path is kept exactly as before.
     */
    private static void applyTrust(SslContextBuilder builder, TlsMaterial material, TlsPolicy policy)
            throws GeneralSecurityException {
        if (policy.allowInsecureConnection()) {
            warnInsecureModeOnce(policy);
            builder.trustManager(InsecureTrustManagerFactory.INSTANCE);
            return;
        }
        Provider jsseProvider = resolveJsseProvider(policy);
        Provider jcaProvider = resolveJcaProvider(policy);
        if (jsseProvider == null && jcaProvider == null) {
            if (material.trustCertsArray().length > 0) {
                builder.trustManager(material.trustCertsArray());
            }
            // else: leave the platform default trust manager (system trust store).
            return;
        }
        // Note this runs even with no configured anchors, where createTrustManagers initializes the factory
        // from a null keystore (the platform default anchors). Returning early instead would leave Netty to
        // build the factory itself, so the SAME policy would validate peer chains inside the pinned provider
        // on the JDK context but through the platform provider on the Netty one — the anchor set would match
        // but the validator would not, which is exactly what a pinned FIPS posture must not allow.
        TrustManager[] trustManagers = JdkSslContexts.createTrustManagers(material.trustCertsArray(), false,
                jsseProvider, jcaProvider);
        builder.trustManager(singleX509TrustManager(trustManagers));
    }

    /**
     * Netty's {@code trustManager(TrustManager)} accepts exactly one manager (it wraps it in a
     * {@code TrustManagerFactory}), and a {@code TrustManagerFactory} initialized from a keystore returns a
     * single {@code X509TrustManager}, so pick that one and fail loudly rather than silently dropping trust if
     * a provider ever returns something unexpected.
     */
    private static TrustManager singleX509TrustManager(TrustManager[] trustManagers)
            throws GeneralSecurityException {
        for (TrustManager trustManager : trustManagers) {
            if (trustManager instanceof X509TrustManager) {
                return trustManager;
            }
        }
        throw new KeyStoreException("The pinned provider's TrustManagerFactory returned no X509TrustManager; "
                + "trust cannot be established. Managers: " + Arrays.toString(trustManagers));
    }

    /**
     * Install the client identity on a Netty builder. Keystore material may hold several identities (e.g.
     * RSA + EC, or identities issued by different accepted CAs), so the whole set goes to a
     * {@link KeyManagerFactory} and JSSE selects one by the peer's requested key type / acceptable issuers.
     * PEM material has a single identity (key + chain) and keeps Netty's raw overload unless a provider axis
     * is pinned, in which case it too must go through a factory this class builds (see {@link #applyTrust}).
     */
    private static void applyKeyManager(SslContextBuilder builder, TlsMaterial material, TlsPolicy policy)
            throws Exception {
        if (material.hasKeyStoreEntries()) {
            builder.keyManager(buildKeyManagerFactory(material, policy));
        } else if (material.hasKeyMaterial()) {
            KeyManagerFactory pinned = pinnedPemKeyManagerFactory(material, policy);
            if (pinned != null) {
                builder.keyManager(pinned);
            } else {
                builder.keyManager(material.privateKey(), material.keyCertChainArray());
            }
        }
    }

    /**
     * Start a server builder on the identity in {@code material}, mirroring {@link #applyKeyManager}'s choice
     * between the factory and raw forms.
     */
    private static SslContextBuilder serverBuilder(TlsMaterial material, TlsPolicy policy) throws Exception {
        if (material.hasKeyStoreEntries()) {
            return SslContextBuilder.forServer(buildKeyManagerFactory(material, policy));
        }
        KeyManagerFactory pinned = pinnedPemKeyManagerFactory(material, policy);
        return pinned != null ? SslContextBuilder.forServer(pinned)
                : SslContextBuilder.forServer(material.privateKey(), material.keyCertChainArray());
    }

    /**
     * A provider-pinned {@link KeyManagerFactory} over PEM material, or {@code null} when neither provider
     * axis is pinned and the historical raw-material path should be kept.
     */
    private static KeyManagerFactory pinnedPemKeyManagerFactory(TlsMaterial material, TlsPolicy policy)
            throws GeneralSecurityException {
        Provider jsseProvider = resolveJsseProvider(policy);
        Provider jcaProvider = resolveJcaProvider(policy);
        if (jsseProvider == null && jcaProvider == null) {
            return null;
        }
        return JdkSslContexts.createKeyManagerFactory(material.privateKey(), material.keyCertChainArray(),
                jsseProvider, jcaProvider);
    }

    private static void applyCiphersAndProtocols(SslContextBuilder builder, TlsPolicy policy) {
        Set<String> ciphers = toSet(policy.ciphers());
        Set<String> protocols = toSet(policy.protocols());
        if (ciphers != null) {
            builder.ciphers(ciphers);
        }
        // Pin the enabled protocols even when the policy configured none, preserving the {TLSv1.3, TLSv1.2}
        // floor v4's DefaultPulsarSslFactory forced rather than deferring to the provider default.
        String[] enabledProtocols = protocols != null
                ? protocols.toArray(new String[0])
                : DEFAULT_ENABLED_PROTOCOLS.toArray(new String[0]);
        builder.protocols(enabledProtocols);
    }

    private static Set<String> toSet(List<String> values) {
        if (values == null || values.isEmpty()) {
            return null;
        }
        return new LinkedHashSet<>(values);
    }
}
