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
        policies.put(TlsPurpose.WEB, serverPolicy);
        policies.put(TlsPurpose.BROKER_CLIENT, brokerClientPolicy(conf));

        FileBasedTlsFactorySettings settings = FileBasedTlsFactorySettings.builder()
                .requireTrustedClientCert(conf.isTlsRequireTrustedClientCertOnConnect())
                .refreshIntervalSeconds(refreshIntervalSeconds(conf))
                // Engine selection (JDK vs. OpenSSL) mapped from the broker's tlsProvider field (PIP-478
                // stage 2b): only an explicit OPENSSL value selects the native engine; JCE provider names
                // and null keep the JDK engine.
                .engineProvider(TlsFactorySupport.engineProvider(conf.getTlsProvider()))
                .build();
        Map<TlsPurpose, Supplier<AuthenticationDataProvider>> authSuppliers =
                (brokerClientAuth != null && StringUtils.isNotBlank(conf.getBrokerClientAuthenticationPlugin()))
                        ? Map.of(TlsPurpose.BROKER_CLIENT, FileBasedTlsFactory.authMaterialSupplier(brokerClientAuth))
                        : Map.of();
        return new DefaultBrokerTlsFactory(policies, settings, authSuppliers);
    }

    private static TlsPolicy serverPolicy(ServiceConfiguration conf) {
        TlsPolicy.Builder builder = TlsPolicy.builder()
                .allowInsecureConnection(conf.isTlsAllowInsecureConnection())
                .enableHostnameVerification(conf.isTlsHostnameVerificationEnabled())
                .protocols(toList(conf.getTlsProtocols()))
                .ciphers(toList(conf.getTlsCiphers()));
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

    private static TlsPolicy brokerClientPolicy(ServiceConfiguration conf) {
        // Outbound broker-client connections reuse the shared insecure/hostname-verification flags.
        TlsPolicy.Builder builder = TlsPolicy.builder()
                .allowInsecureConnection(conf.isTlsAllowInsecureConnection())
                .enableHostnameVerification(conf.isTlsHostnameVerificationEnabled())
                .protocols(toList(conf.getBrokerClientTlsProtocols()))
                .ciphers(toList(conf.getBrokerClientTlsCiphers()));
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

    private static int refreshIntervalSeconds(ServiceConfiguration conf) {
        long configured = conf.getTlsCertRefreshCheckDurationSec();
        if (configured <= 0) {
            return FileBasedTlsFactorySettings.DEFAULT_REFRESH_INTERVAL_SECONDS;
        }
        return (int) Math.min(configured, Integer.MAX_VALUE);
    }

    private static List<String> toList(Set<String> values) {
        return values == null ? List.of() : List.copyOf(values);
    }
}
