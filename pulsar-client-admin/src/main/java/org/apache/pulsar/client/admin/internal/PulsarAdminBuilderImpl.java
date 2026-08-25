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
package org.apache.pulsar.client.admin.internal;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminBuilder;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.PulsarClientException.UnsupportedAuthenticationException;
import org.apache.pulsar.client.api.PulsarClientSharedResources;
import org.apache.pulsar.client.api.Socks5ProxyScope;
import org.apache.pulsar.client.impl.PulsarClientSharedResourcesImpl;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.conf.ConfigurationDataUtils;
import org.apache.pulsar.tls.PulsarTlsFactory;

public class PulsarAdminBuilderImpl implements PulsarAdminBuilder {

    @Getter
    protected ClientConfigurationData conf;

    private ClassLoader clientBuilderClassLoader = null;
    private boolean acceptGzipCompression = true;
    private transient PulsarClientSharedResourcesImpl sharedResources;
    // PIP-478: the factory instances build() has handed to an admin. Adoption is a hand-over, not a share —
    // see the check in build(). Shared with every clone of this builder, since they share the instances too
    // (clone() is shallow). Identity, not equals: a factory is an instance with a lifecycle, not a value.
    // Synchronized because sharing it is what makes a clone-per-thread builder no longer independent.
    private final transient Set<PulsarTlsFactory> adoptedTlsFactories;

    @Override
    public PulsarAdmin build() throws PulsarClientException {
        PulsarTlsFactory adopting = rejectAnAlreadyAdoptedTlsFactory();
        // PIP-478 (#26398): hand the admin its own configuration object, as ClientBuilderImpl.build() does.
        // PulsarAdminImpl writes into the configuration it is given — foldOAuth2IdpPolicy installs the
        // OAuth2 plugin's IdP TLS policy under CLIENT_OAUTH2 — so sharing the builder's instance made that
        // fold a one-time event: a second build() with different OAuth2 credentials found the first admin's
        // policy already there and kept it, resolving tokens against IdP B while trusting IdP A's material,
        // with no error and no log line. Every caller that mutates the builder's configuration does so
        // before build(), so the copy carries their changes.
        ClientConfigurationData adminConf = conf.clone();
        try {
            return new PulsarAdminImpl(conf.getServiceUrl(), adminConf,
                    clientBuilderClassLoader, acceptGzipCompression, sharedResources);
        } finally {
            // Whether the factory was consumed is read off the copy the admin was given rather than inferred
            // from how the build ended: the constructor initializes an adopted instance and closes it again
            // on the way out (the constructed=false branch, which is what
            // aFailedAdminBuildDoesNotLeakTheResolvedFactory pins), so a build that failed after that has
            // spent it as much as a successful one, while one that failed before — no service URL, which
            // PulsarAdminImpl rejects in its first statement — has not touched it.
            if (adminConf.isTlsFactoryAdopted()) {
                recordAdoption(adopting);
            }
        }
    }

    /**
     * PIP-478: reject a second admin built from a {@link PulsarTlsFactory} instance this builder, or a
     * builder it was cloned from, has already handed over — as {@code PulsarClientBuilderV5.build()} does for
     * the client.
     *
     * <p>There is no {@code tlsFactory(...)} on the public admin builder; adoption is reachable through
     * {@link #getConf()} and is how the broker and the functions worker route their own admin clients onto a
     * custom factory ({@code PulsarService.applyBrokerClientTlsFactoryToAdmin},
     * {@code WorkerUtils.applyBrokerClientTlsFactoryToAdmin}). Ownership transfers with the configuration:
     * {@code AsyncHttpConnectorProvider} initializes the instance and closes it with the admin, so handing it
     * to a second admin would {@code initialize} it twice and {@code close} it twice — the SPI says exactly
     * once and at most once — and leave whichever admin is closed second on a closed factory. Copying the
     * configuration at {@code build()} does not help: {@code clone()} is shallow, so the copy carries the
     * same instance.
     *
     * <p>What counts as handed over is what the framework says it took, not whether the build succeeded —
     * see the note at the call site.
     *
     * @return the instance about to be adopted, to be passed to {@link #recordAdoption} whatever the outcome,
     *         or {@code null} when no factory is configured
     */
    private PulsarTlsFactory rejectAnAlreadyAdoptedTlsFactory() {
        PulsarTlsFactory adopting = conf.getTlsFactory();
        if (adopting != null && adoptedTlsFactories.contains(adopting)) {
            throw new IllegalStateException("the PulsarTlsFactory on this admin builder's configuration has "
                    + "already been adopted by an admin built from it. The admin initializes that instance "
                    + "and closes it with itself, so it cannot be handed to a second admin — closing either "
                    + "one would break TLS for the other. Set a fresh instance before building again.");
        }
        return adopting;
    }

    /**
     * Record that {@code adopted} has been handed over. Every instance is remembered, not just the last one,
     * so cycling back to an earlier factory is caught too; the builder family therefore retains one reference
     * per build performed, which is bounded by the factories the caller created in the first place.
     *
     * @param adopted the adopted instance, or {@code null} when no factory is configured
     */
    private void recordAdoption(PulsarTlsFactory adopted) {
        if (adopted != null) {
            adoptedTlsFactories.add(adopted);
        }
    }

    public PulsarAdminBuilderImpl() {
        this.conf = new ClientConfigurationData();
        this.conf.setConnectionsPerBroker(16);
        // Admin traffic is HTTP-only; default the scope to HTTP_ONLY so that a configured
        // SOCKS5 proxy is applied to HTTP requests without requiring an explicit scope call.
        this.conf.setSocks5ProxyScope(Socks5ProxyScope.HTTP_ONLY);
        this.adoptedTlsFactories =
                Collections.synchronizedSet(Collections.newSetFromMap(new IdentityHashMap<>()));
    }

    private PulsarAdminBuilderImpl(ClientConfigurationData conf, Set<PulsarTlsFactory> adoptedTlsFactories) {
        this.conf = conf;
        this.adoptedTlsFactories = adoptedTlsFactories;
    }

    @Override
    public PulsarAdminBuilder clone() {
        // PIP-478: the copy carries the same adopted factory instances (clone() is shallow), so it shares the
        // record of which of them have been handed over — in both orders. Copying the record instead would
        // leave a builder cloned *before* its first build with an empty one, and both copies would then adopt
        // the same instance.
        PulsarAdminBuilderImpl pulsarAdminBuilder = new PulsarAdminBuilderImpl(conf.clone(), adoptedTlsFactories);
        pulsarAdminBuilder.clientBuilderClassLoader = clientBuilderClassLoader;
        pulsarAdminBuilder.acceptGzipCompression = acceptGzipCompression;
        return pulsarAdminBuilder;
    }

    @Override
    public PulsarAdminBuilder loadConf(Map<String, Object> config) {
        // PIP-478: reject a stale, removed PIP-337 sslFactoryPlugin key with an actionable message.
        ConfigurationDataUtils.rejectRemovedPip337TlsFactoryKeys(config);
        conf = ConfigurationDataUtils.loadData(config, conf, ClientConfigurationData.class);
        setAuthenticationFromPropsIfAvailable(conf);
        if (config.containsKey("acceptGzipCompression")) {
            Object acceptGzipCompressionObj = config.get("acceptGzipCompression");
            if (acceptGzipCompressionObj instanceof Boolean) {
                acceptGzipCompression = (Boolean) acceptGzipCompressionObj;
            } else {
                acceptGzipCompression = Boolean.parseBoolean(acceptGzipCompressionObj.toString());
            }
        }
        // in ClientConfigurationData, the maxConnectionsPerHost maps to connectionsPerBroker
        if (config.containsKey("maxConnectionsPerHost")) {
            Object maxConnectionsPerHostObj = config.get("maxConnectionsPerHost");
            if (maxConnectionsPerHostObj instanceof Integer) {
                maxConnectionsPerHost((Integer) maxConnectionsPerHostObj);
            } else {
                maxConnectionsPerHost(Integer.parseInt(maxConnectionsPerHostObj.toString()));
            }
        }
        return this;
    }

    @Override
    public PulsarAdminBuilder serviceHttpUrl(String serviceHttpUrl) {
        conf.setServiceUrl(serviceHttpUrl);
        return this;
    }

    @Override
    public PulsarAdminBuilder authentication(Authentication authentication) {
        conf.setAuthentication(authentication);
        return this;
    }

    @Override
    public PulsarAdminBuilder authentication(String authPluginClassName, Map<String, String> authParams)
            throws UnsupportedAuthenticationException {
        conf.setAuthentication(AuthenticationFactory.create(authPluginClassName, authParams));
        return this;
    }

    @Override
    public PulsarAdminBuilder authentication(String authPluginClassName, String authParamsString)
            throws UnsupportedAuthenticationException {
        conf.setAuthentication(AuthenticationFactory.create(authPluginClassName, authParamsString));
        return this;
    }

    private void setAuthenticationFromPropsIfAvailable(ClientConfigurationData clientConfig) {
        String authPluginClass = clientConfig.getAuthPluginClassName();
        String authParams = clientConfig.getAuthParams();
        Map<String, String> authParamMap = clientConfig.getAuthParamMap();
        if (StringUtils.isBlank(authPluginClass) || (StringUtils.isBlank(authParams) && authParamMap == null)) {
            return;
        }
        try {
            if (StringUtils.isNotBlank(authParams)) {
                authentication(authPluginClass, authParams);
            } else if (authParamMap != null) {
                authentication(authPluginClass, authParamMap);
            }
        } catch (UnsupportedAuthenticationException ex) {
            throw new RuntimeException("Failed to create authentication: " + ex.getMessage(), ex);
        }
    }

    @Override
    public PulsarAdminBuilder tlsKeyFilePath(String tlsKeyFilePath) {
        conf.setTlsKeyFilePath(tlsKeyFilePath);
        return this;
    }

    @Override
    public PulsarAdminBuilder tlsCertificateFilePath(String tlsCertificateFilePath) {
        conf.setTlsCertificateFilePath(tlsCertificateFilePath);
        return this;
    }

    @Override
    public PulsarAdminBuilder tlsTrustCertsFilePath(String tlsTrustCertsFilePath) {
        conf.setTlsTrustCertsFilePath(tlsTrustCertsFilePath);
        return this;
    }

    @Override
    public PulsarAdminBuilder allowTlsInsecureConnection(boolean allowTlsInsecureConnection) {
        conf.setTlsAllowInsecureConnection(allowTlsInsecureConnection);
        return this;
    }

    @Override
    public PulsarAdminBuilder enableTlsHostnameVerification(boolean enableTlsHostnameVerification) {
        conf.setTlsHostnameVerificationEnable(enableTlsHostnameVerification);
        return this;
    }

    @Override
    public PulsarAdminBuilder useKeyStoreTls(boolean useKeyStoreTls) {
        conf.setUseKeyStoreTls(useKeyStoreTls);
        return this;
    }

    @Override
    public PulsarAdminBuilder sslProvider(String sslProvider) {
        conf.setSslProvider(sslProvider);
        return this;
    }

    @Override
    public PulsarAdminBuilder tlsKeyStoreType(String tlsKeyStoreType) {
        conf.setTlsKeyStoreType(tlsKeyStoreType);
        return this;
    }

    @Override
    public PulsarAdminBuilder tlsKeyStorePath(String tlsTrustStorePath) {
        conf.setTlsKeyStorePath(tlsTrustStorePath);
        return this;
    }

    @Override
    public PulsarAdminBuilder tlsKeyStorePassword(String tlsKeyStorePassword) {
        conf.setTlsKeyStorePassword(tlsKeyStorePassword);
        return this;
    }

    @Override
    public PulsarAdminBuilder tlsTrustStoreType(String tlsTrustStoreType) {
        conf.setTlsTrustStoreType(tlsTrustStoreType);
        return this;
    }

    @Override
    public PulsarAdminBuilder tlsTrustStorePath(String tlsTrustStorePath) {
        conf.setTlsTrustStorePath(tlsTrustStorePath);
        return this;
    }

    @Override
    public PulsarAdminBuilder tlsTrustStorePassword(String tlsTrustStorePassword) {
        conf.setTlsTrustStorePassword(tlsTrustStorePassword);
        return this;
    }

    @Override
    public PulsarAdminBuilder tlsCiphers(Set<String> tlsCiphers) {
        conf.setTlsCiphers(tlsCiphers);
        return this;
    }

    @Override
    public PulsarAdminBuilder tlsFactoryClassName(String tlsFactoryClassName) {
        conf.setTlsFactoryClassName(StringUtils.isBlank(tlsFactoryClassName) ? "" : tlsFactoryClassName);
        return this;
    }

    @Override
    public PulsarAdminBuilder tlsFactoryConfig(String tlsFactoryConfig) {
        conf.setTlsFactoryConfig(StringUtils.isBlank(tlsFactoryConfig) ? "" : tlsFactoryConfig);
        return this;
    }

    @Override
    public PulsarAdminBuilder tlsProtocols(Set<String> tlsProtocols) {
        conf.setTlsProtocols(tlsProtocols);
        return this;
    }

    @Override
    public PulsarAdminBuilder connectionTimeout(int connectionTimeout, TimeUnit connectionTimeoutUnit) {
        this.conf.setConnectionTimeoutMs((int) connectionTimeoutUnit.toMillis(connectionTimeout));
        return this;
    }

    @Override
    public PulsarAdminBuilder readTimeout(int readTimeout, TimeUnit readTimeoutUnit) {
        this.conf.setReadTimeoutMs((int) readTimeoutUnit.toMillis(readTimeout));
        return this;
    }

    @Override
    public PulsarAdminBuilder requestTimeout(int requestTimeout, TimeUnit requestTimeoutUnit) {
        this.conf.setRequestTimeoutMs((int) requestTimeoutUnit.toMillis(requestTimeout));
        return this;
    }

    @Override
    public PulsarAdminBuilder autoCertRefreshTime(int autoCertRefreshTime, TimeUnit autoCertRefreshTimeUnit) {
        this.conf.setAutoCertRefreshSeconds((int) autoCertRefreshTimeUnit.toSeconds(autoCertRefreshTime));
        return this;
    }

    @Override
    public PulsarAdminBuilder setContextClassLoader(ClassLoader clientBuilderClassLoader) {
        this.clientBuilderClassLoader = clientBuilderClassLoader;
        return this;
    }

    @Override
    public PulsarAdminBuilder acceptGzipCompression(boolean acceptGzipCompression) {
        this.acceptGzipCompression = acceptGzipCompression;
        return this;
    }

    @Override
    public PulsarAdminBuilder maxConnectionsPerHost(int maxConnectionsPerHost) {
        // reuse the same configuration as the client, however for the admin client, the connection
        // is usually established to a cluster address and not to a broker address
        this.conf.setConnectionsPerBroker(maxConnectionsPerHost);
        return this;
    }

    @Override
    public PulsarAdminBuilder connectionMaxIdleSeconds(int connectionMaxIdleSeconds) {
        this.conf.setConnectionMaxIdleSeconds(connectionMaxIdleSeconds);
        return this;
    }

    @Override
    public PulsarAdminBuilder description(String description) {
        if (description != null && description.length() > 64) {
            throw new IllegalArgumentException("description should be at most 64 characters");
        }
        this.conf.setDescription(description);
        return this;
    }

    @Override
    public PulsarAdminBuilder socks5ProxyAddress(InetSocketAddress socks5ProxyAddress) {
        this.conf.setSocks5ProxyAddress(socks5ProxyAddress);
        return this;
    }

    @Override
    public PulsarAdminBuilder socks5ProxyUsername(String socks5ProxyUsername) {
        this.conf.setSocks5ProxyUsername(socks5ProxyUsername);
        return this;
    }

    @Override
    public PulsarAdminBuilder socks5ProxyPassword(String socks5ProxyPassword) {
        this.conf.setSocks5ProxyPassword(socks5ProxyPassword);
        return this;
    }

    @Override
    public PulsarAdminBuilder sharedResources(PulsarClientSharedResources sharedResources) {
        this.sharedResources = (PulsarClientSharedResourcesImpl) sharedResources;
        return this;
    }
}
