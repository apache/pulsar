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
package org.apache.pulsar.broker.tls;

import com.google.common.annotations.VisibleForTesting;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.common.tls.impl.FileBasedTlsFactory;
import org.apache.pulsar.common.tls.impl.FileBasedTlsFactorySettings;
import org.apache.pulsar.tls.TlsPolicy;
import org.apache.pulsar.tls.TlsPurpose;

/**
 * The broker's default {@code PulsarTlsFactory}: a thin {@link FileBasedTlsFactory} whose purpose&rarr;
 * policy map is composed from a {@link ServiceConfiguration} (PIP-478).
 *
 * <p>It lives in {@code pulsar-broker-common} — which owns the Jetty integration and the broker
 * configuration — so that neither the SPI module nor {@code pulsar-common} carries broker-config
 * knowledge. The class only <em>composes</em> the map; wiring it into the broker/proxy/web services is
 * a later stage.
 */
public class DefaultBrokerTlsFactory extends FileBasedTlsFactory {

    /**
     * Construct the broker factory from an already-composed purpose&rarr;policy map.
     *
     * @param policies the composed purpose&rarr;policy map
     * @param settings the factory-wide engine/refresh/client-auth settings
     */
    public DefaultBrokerTlsFactory(Map<TlsPurpose, TlsPolicy> policies, FileBasedTlsFactorySettings settings) {
        super(policies, settings);
    }

    /**
     * Construct a broker factory that additionally folds the broker-client authentication's TLS material
     * over the {@link TlsPurpose#BROKER_CLIENT} file policy (PIP-478).
     *
     * @param policies              the composed purpose&rarr;policy map
     * @param settings              the factory-wide engine/refresh/client-auth settings
     * @param authMaterialSuppliers per-purpose broker-client authentication material suppliers (may be empty)
     */
    public DefaultBrokerTlsFactory(Map<TlsPurpose, TlsPolicy> policies, FileBasedTlsFactorySettings settings,
            Map<TlsPurpose, Supplier<AuthenticationDataProvider>> authMaterialSuppliers) {
        super(policies, settings, authMaterialSuppliers);
    }

    /**
     * Compose a {@link DefaultBrokerTlsFactory} from a {@link ServiceConfiguration}.
     *
     * @param conf the broker service configuration
     * @return an uninitialized broker factory (call {@code initialize(...)} before use)
     */
    public static DefaultBrokerTlsFactory fromServiceConfiguration(ServiceConfiguration conf) {
        return fromServiceConfiguration(conf, null);
    }

    /**
     * Compose a {@link DefaultBrokerTlsFactory} from a {@link ServiceConfiguration}, optionally folding the
     * broker's broker-client {@link Authentication} TLS material over the {@link TlsPurpose#BROKER_CLIENT}
     * file policy (PIP-478) for the broker's own outbound (broker-to-broker / replication) connections.
     *
     * <p>The broker's server material is registered for the {@link TlsPurpose#BROKER},
     * {@link TlsPurpose#PROXY} and {@link TlsPurpose#WEB} purposes; the broker's outbound
     * (broker-to-broker) material for {@link TlsPurpose#BROKER_CLIENT}. When {@code brokerClientAuth} is
     * supplied and a broker-client authentication plugin is configured, its in-memory cert/key override the
     * {@code brokerClient*} file paths (auth-cert-wins), so the broker presents the right identity to its
     * peers — the server-side mirror of the client TLS override hook.
     *
     * @param conf             the broker service configuration
     * @param brokerClientAuth the broker's broker-client authentication, or {@code null} for no fold
     * @return an uninitialized broker factory (call {@code initialize(...)} before use)
     */
    public static DefaultBrokerTlsFactory fromServiceConfiguration(ServiceConfiguration conf,
            Authentication brokerClientAuth) {
        Map<TlsPurpose, TlsPolicy> policies = new LinkedHashMap<>();
        TlsPolicy serverPolicy = serverPolicy(conf);
        policies.put(TlsPurpose.BROKER, serverPolicy);
        policies.put(TlsPurpose.PROXY, serverPolicy);
        policies.put(TlsPurpose.WEB, webPolicy(conf));
        policies.put(TlsPurpose.BROKER_CLIENT, brokerClientPolicy(conf));

        FileBasedTlsFactorySettings settings = FileBasedTlsFactorySettings.builder()
                .requireTrustedClientCert(conf.isTlsRequireTrustedClientCertOnConnect())
                .refreshIntervalSeconds(refreshIntervalSeconds(conf))
                // Engine selection (JDK vs. OpenSSL) mapped from the broker's tlsProvider field: an explicit
                // engine literal is honored verbatim, a JSSE provider name selects no engine, and an unset
                // value takes the native engine as OPENSSL_REFCNT when tcnative is available (see
                // TlsFactorySupport.engineProvider).
                .engineProvider(TlsFactorySupport.engineProvider(conf.getTlsProvider()))
                .build();
        Map<TlsPurpose, Supplier<AuthenticationDataProvider>> authSuppliers =
                (brokerClientAuth != null && StringUtils.isNotBlank(conf.getBrokerClientAuthenticationPlugin()))
                        ? Map.of(TlsPurpose.BROKER_CLIENT, FileBasedTlsFactory.authMaterialSupplier(brokerClientAuth))
                        : Map.of();
        return new DefaultBrokerTlsFactory(policies, settings, authSuppliers);
    }

    /**
     * The {@link TlsPurpose#WEB} policy. The web listener has its own provider/protocol/cipher keys, and
     * they take precedence over the binary-listener ones when set — {@code webServiceTlsProvider},
     * {@code webServiceTlsProtocols}, {@code webServiceTlsCiphers} — falling back to {@code tlsProvider},
     * {@code tlsProtocols}, {@code tlsCiphers} when they are not. Material (PEM or keystore) and the
     * insecure flag are shared with the binary listener, as they are today.
     *
     * <p>A configured provider is <em>pinned</em>: if it cannot be resolved, startup fails rather than
     * silently ignoring the configuration. That is why {@code webServiceTlsProvider} no longer ships a
     * default of {@code Conscrypt}. Under PIP-337 the key only reached Jetty's
     * {@code SslContextFactory.setProvider(...)}, which is inert on a factory that overrides
     * {@code getSslContext()} with a pre-built context — so the shipped default never actually selected a
     * provider. Honoring it here makes it real, and Conscrypt's uber jar carries native libraries for
     * x86_64 only: keeping the default would break the web listener out of the box on aarch64 (Apple
     * silicon, ARM servers) and s390x. Unset, the JVM default applies, which is what deployments have
     * effectively been running all along; an operator who wants Conscrypt still sets it explicitly.
     */
    @VisibleForTesting
    static TlsPolicy webPolicy(ServiceConfiguration conf) {
        return serverPolicy(conf,
                firstNonBlank(conf.getWebServiceTlsProvider(), conf.getTlsProvider()),
                firstNonEmpty(conf.getWebServiceTlsProtocols(), conf.getTlsProtocols()),
                firstNonEmpty(conf.getWebServiceTlsCiphers(), conf.getTlsCiphers()));
    }

    @VisibleForTesting
    static TlsPolicy serverPolicy(ServiceConfiguration conf) {
        return serverPolicy(conf, conf.getTlsProvider(), conf.getTlsProtocols(), conf.getTlsCiphers());
    }

    private static String firstNonBlank(String preferred, String fallback) {
        return StringUtils.isNotBlank(preferred) ? preferred : fallback;
    }

    private static Set<String> firstNonEmpty(Set<String> preferred, Set<String> fallback) {
        return preferred != null && !preferred.isEmpty() ? preferred : fallback;
    }

    private static TlsPolicy serverPolicy(ServiceConfiguration conf, String provider, Set<String> protocols,
                                          Set<String> ciphers) {
        // enableHostnameVerification is pinned OFF on server-role policies rather than mapped from
        // tlsHostnameVerificationEnabled. That key is the broker's OUTBOUND setting ("whether the hostname
        // is validated when the broker creates a TLS connection with other brokers"), and every existing
        // consumer of it in the codebase configures an outbound client; it IS honored here, on the
        // TlsPurpose.BROKER_CLIENT policy composed by brokerClientPolicy() below. Mapping it onto a
        // server-role policy (BROKER / PROXY / WEB) would mean the opposite thing — verifying the peer's
        // hostname where the peer is the connecting CLIENT. It is inert either way today, since only the
        // client context builders read the flag, but a server policy carrying it would invite a future
        // consumer to turn endpoint identification on for a server engine. It has to be set explicitly:
        // TlsPolicy.Builder defaults the flag to true (secure-by-default for clients), so simply omitting
        // it would leave every server policy claiming hostname verification is on. Locked down by
        // DefaultBrokerTlsPolicyTest#hostnameVerificationIsAnOutboundSettingAppliedToTheBrokerClientPolicy.
        TlsPolicy.Builder builder = TlsPolicy.builder()
                .allowInsecureConnection(conf.isTlsAllowInsecureConnection())
                .enableHostnameVerification(false)
                .protocols(toList(protocols))
                .ciphers(toList(ciphers))
                // v4 parity: the provider key is overloaded. An engine literal (JDK/OPENSSL/OPENSSL_REFCNT)
                // selects the Netty engine above and yields null here; any other value is a JSSE provider
                // name (e.g. Conscrypt), which v4 used to build the SSLContext, so route it to that axis.
                .jsseProvider(TlsFactorySupport.resolveJsseProvider(null, provider));
        if (conf.isTlsEnabledWithKeyStore()) {
            builder.format(TlsPolicy.Format.KEYSTORE)
                    .keyStoreType(conf.getTlsKeyStoreType())
                    .trustStoreType(conf.getTlsTrustStoreType())
                    .keyStorePath(conf.getTlsKeyStore())
                    .keyStorePassword(conf.getTlsKeyStorePassword())
                    .trustStorePath(conf.getTlsTrustStore())
                    .trustStorePassword(conf.getTlsTrustStorePassword());
        } else {
            builder.format(TlsPolicy.Format.PEM)
                    .trustCertsFilePath(conf.getTlsTrustCertsFilePath())
                    .certificateFilePath(conf.getTlsCertificateFilePath())
                    .keyFilePath(conf.getTlsKeyFilePath());
        }
        return builder.build();
    }

    @VisibleForTesting
    static TlsPolicy brokerClientPolicy(ServiceConfiguration conf) {
        // Outbound broker-client connections reuse the shared insecure/hostname-verification flags.
        TlsPolicy.Builder builder = TlsPolicy.builder()
                .allowInsecureConnection(conf.isTlsAllowInsecureConnection())
                .enableHostnameVerification(conf.isTlsHostnameVerificationEnabled())
                .protocols(toList(conf.getBrokerClientTlsProtocols()))
                .ciphers(toList(conf.getBrokerClientTlsCiphers()))
                // The outbound leg has its own provider setting, on the same two axes as tlsProvider above.
                .jsseProvider(TlsFactorySupport.resolveJsseProvider(null, conf.getBrokerClientSslProvider()));
        if (conf.isBrokerClientTlsEnabledWithKeyStore()) {
            builder.format(TlsPolicy.Format.KEYSTORE)
                    .keyStoreType(conf.getBrokerClientTlsKeyStoreType())
                    .trustStoreType(conf.getBrokerClientTlsTrustStoreType())
                    .keyStorePath(conf.getBrokerClientTlsKeyStore())
                    .keyStorePassword(conf.getBrokerClientTlsKeyStorePassword())
                    .trustStorePath(conf.getBrokerClientTlsTrustStore())
                    .trustStorePassword(conf.getBrokerClientTlsTrustStorePassword());
        } else {
            builder.format(TlsPolicy.Format.PEM)
                    .trustCertsFilePath(conf.getBrokerClientTrustCertsFilePath())
                    .certificateFilePath(conf.getBrokerClientCertificateFilePath())
                    .keyFilePath(conf.getBrokerClientKeyFilePath());
        }
        return builder.build();
    }

    /**
     * Map {@code tlsCertRefreshCheckDurationSec} onto the factory's poll interval, preserving the v4 meaning of
     * a non-positive value: every v4 consumer guards its refresh task with {@code > 0}, and
     * {@link FileBasedTlsFactorySettings} likewise documents {@code <= 0} as "no background poll". Pass it
     * through rather than substituting the default, so an operator who set {@code 0} still gets no poll.
     *
     * <p>Note that {@code 0} therefore disables rotation for the subscribing server purposes, exactly as it
     * already does on the PIP-337 path. The config key's "set 0 to check on every new connection" wording
     * describes only the one-shot acquisition paths, which re-stat per request; it has not applied to the
     * server listeners since they moved to a shared, periodically-refreshed context.
     */
    private static int refreshIntervalSeconds(ServiceConfiguration conf) {
        long configured = conf.getTlsCertRefreshCheckDurationSec();
        if (configured <= 0) {
            return 0;
        }
        return (int) Math.min(configured, Integer.MAX_VALUE);
    }

    private static List<String> toList(Set<String> values) {
        return values == null ? List.of() : List.copyOf(values);
    }
}
