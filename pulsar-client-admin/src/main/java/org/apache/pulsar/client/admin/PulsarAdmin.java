/**
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
package org.apache.pulsar.client.admin;

import java.io.Closeable;
import java.io.IOException;
import java.net.URL;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;

import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.admin.internal.BookiesImpl;
import org.apache.pulsar.client.admin.internal.BrokerStatsImpl;
import org.apache.pulsar.client.admin.internal.BrokersImpl;
import org.apache.pulsar.client.admin.internal.ClustersImpl;
import org.apache.pulsar.client.admin.internal.FunctionsImpl;
import org.apache.pulsar.client.admin.internal.JacksonConfigurator;
import org.apache.pulsar.client.admin.internal.LookupImpl;
import org.apache.pulsar.client.admin.internal.NamespacesImpl;
import org.apache.pulsar.client.admin.internal.NonPersistentTopicsImpl;
import org.apache.pulsar.client.admin.internal.ProxyStatsImpl;
import org.apache.pulsar.client.admin.internal.PulsarAdminBuilderImpl;
import org.apache.pulsar.client.admin.internal.ResourceQuotasImpl;
import org.apache.pulsar.client.admin.internal.SchemasImpl;
import org.apache.pulsar.client.admin.internal.SinksImpl;
import org.apache.pulsar.client.admin.internal.SourcesImpl;
import org.apache.pulsar.client.admin.internal.TenantsImpl;
import org.apache.pulsar.client.admin.internal.TopicsImpl;
import org.apache.pulsar.client.admin.internal.WorkerImpl;
import org.apache.pulsar.client.admin.internal.http.AsyncHttpConnector;
import org.apache.pulsar.client.admin.internal.http.AsyncHttpConnectorProvider;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.auth.AuthenticationDisabled;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.common.net.ServiceURI;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

/**
 * Pulsar client admin API client.
 */
@SuppressWarnings("deprecation")
public class PulsarAdmin implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(PulsarAdmin.class);

    public static final int DEFAULT_CONNECT_TIMEOUT_SECONDS = 60;
    public static final int DEFAULT_READ_TIMEOUT_SECONDS = 60;
    public static final int DEFAULT_REQUEST_TIMEOUT_SECONDS = 300;

    private final Clusters clusters;
    private final Brokers brokers;
    private final BrokerStats brokerStats;
    private final ProxyStats proxyStats;
    private final Tenants tenants;
    private final Properties properties;
    private final Namespaces namespaces;
    private final Bookies bookies;
    private final TopicsImpl topics;
    private final NonPersistentTopics nonPersistentTopics;
    private final ResourceQuotas resourceQuotas;
    private final ClientConfigurationData clientConfigData;
    private final Client client;
    private final AsyncHttpConnector asyncHttpConnector;
    private final String serviceUrl;
    private final Lookup lookups;
    private final Functions functions;
    private final Sources sources;
    private final Sinks sinks;
    private final Worker worker;
    private final Schemas schemas;
    protected final WebTarget root;
    protected final Authentication auth;
    private final int connectTimeout;
    private final TimeUnit connectTimeoutUnit;
    private final int readTimeout;
    private final TimeUnit readTimeoutUnit;
    private final int requestTimeout;
    private final TimeUnit requestTimeoutUnit;

    static {
        /**
         * The presence of slf4j-jdk14.jar, that is the jul binding for SLF4J, will force SLF4J calls to be delegated to
         * jul. On the other hand, the presence of jul-to-slf4j.jar, plus the installation of SLF4JBridgeHandler, by
         * invoking "SLF4JBridgeHandler.install()" will route jul records to SLF4J. Thus, if both jar are present
         * simultaneously (and SLF4JBridgeHandler is installed), slf4j calls will be delegated to jul and jul records
         * will be routed to SLF4J, resulting in an endless loop. We avoid this loop by detecting if slf4j-jdk14 is used
         * in the client class path. If slf4j-jdk14 is found, we don't use the slf4j bridge.
         */
        try {
            Class.forName("org.slf4j.impl.JDK14LoggerFactory");
        } catch (Exception ex) {
            // Setup the bridge for java.util.logging to SLF4J
            SLF4JBridgeHandler.removeHandlersForRootLogger();
            SLF4JBridgeHandler.install();
        }
    }

    /**
     * Creates a builder to construct an instance of {@link PulsarAdmin}.
     */
    public static PulsarAdminBuilder builder() {
        return new PulsarAdminBuilderImpl();
    }


    public PulsarAdmin(String serviceUrl, ClientConfigurationData clientConfigData) throws PulsarClientException {
        this(serviceUrl, clientConfigData, DEFAULT_CONNECT_TIMEOUT_SECONDS, TimeUnit.SECONDS,
                DEFAULT_READ_TIMEOUT_SECONDS, TimeUnit.SECONDS,
                DEFAULT_REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS, null);

    }

    public PulsarAdmin(String serviceUrl,
                       ClientConfigurationData clientConfigData,
                       int connectTimeout,
                       TimeUnit connectTimeoutUnit,
                       int readTimeout,
                       TimeUnit readTimeoutUnit,
                       int requestTimeout,
                       TimeUnit requestTimeoutUnit,
                       ClassLoader clientBuilderClassLoader) throws PulsarClientException {
        this.connectTimeout = connectTimeout;
        this.connectTimeoutUnit = connectTimeoutUnit;
        this.readTimeout = readTimeout;
        this.readTimeoutUnit = readTimeoutUnit;
        this.requestTimeout = requestTimeout;
        this.requestTimeoutUnit = requestTimeoutUnit;
        this.clientConfigData = clientConfigData;
        this.auth = clientConfigData != null ? clientConfigData.getAuthentication() : new AuthenticationDisabled();
        LOG.debug("created: serviceUrl={}, authMethodName={}", serviceUrl,
                auth != null ? auth.getAuthMethodName() : null);

        if (auth != null) {
            auth.start();
        }

        if (StringUtils.isBlank(clientConfigData.getServiceUrl())) {
            clientConfigData.setServiceUrl(serviceUrl);
        }

        AsyncHttpConnectorProvider asyncConnectorProvider = new AsyncHttpConnectorProvider(clientConfigData);

        ClientConfig httpConfig = new ClientConfig();
        httpConfig.property(ClientProperties.FOLLOW_REDIRECTS, true);
        httpConfig.property(ClientProperties.ASYNC_THREADPOOL_SIZE, 8);
        httpConfig.register(MultiPartFeature.class);
        httpConfig.connectorProvider(asyncConnectorProvider);

        ClassLoader originalCtxLoader = null;
        if (clientBuilderClassLoader != null) {
            originalCtxLoader = Thread.currentThread().getContextClassLoader();
            Thread.currentThread().setContextClassLoader(clientBuilderClassLoader);
        }

        ClientBuilder clientBuilder = ClientBuilder.newBuilder()
                .withConfig(httpConfig)
                .connectTimeout(this.connectTimeout, this.connectTimeoutUnit)
                .readTimeout(this.readTimeout, this.readTimeoutUnit)
                .register(JacksonConfigurator.class).register(JacksonFeature.class);

        boolean useTls = clientConfigData.getServiceUrl().startsWith("https://");

        this.client = clientBuilder.build();

        this.serviceUrl = serviceUrl;
        ServiceURI serviceUri = ServiceURI.create(serviceUrl);
        root = client.target(String.format("%s://%s"
                , serviceUri.getServiceScheme()
                , serviceUri.getServiceHosts()[ThreadLocalRandom.current()
                        .nextInt(serviceUri.getServiceHosts().length)]));

        this.asyncHttpConnector = asyncConnectorProvider.getConnector(
                Math.toIntExact(connectTimeoutUnit.toMillis(this.connectTimeout)),
                Math.toIntExact(readTimeoutUnit.toMillis(this.readTimeout)),
                Math.toIntExact(requestTimeoutUnit.toMillis(this.requestTimeout)));

        long readTimeoutMs = readTimeoutUnit.toMillis(this.readTimeout);
        this.clusters = new ClustersImpl(root, auth, readTimeoutMs);
        this.brokers = new BrokersImpl(root, auth, readTimeoutMs);
        this.brokerStats = new BrokerStatsImpl(root, auth, readTimeoutMs);
        this.proxyStats = new ProxyStatsImpl(root, auth, readTimeoutMs);
        this.tenants = new TenantsImpl(root, auth, readTimeoutMs);
        this.properties = new TenantsImpl(root, auth, readTimeoutMs);
        this.namespaces = new NamespacesImpl(root, auth, readTimeoutMs);
        this.topics = new TopicsImpl(root, auth, readTimeoutMs);
        this.nonPersistentTopics = new NonPersistentTopicsImpl(root, auth, readTimeoutMs);
        this.resourceQuotas = new ResourceQuotasImpl(root, auth, readTimeoutMs);
        this.lookups = new LookupImpl(root, auth, useTls, readTimeoutMs);
        this.functions = new FunctionsImpl(root, auth, asyncHttpConnector.getHttpClient(), readTimeoutMs);
        this.sources = new SourcesImpl(root, auth, asyncHttpConnector.getHttpClient(), readTimeoutMs);
        this.sinks = new SinksImpl(root, auth, asyncHttpConnector.getHttpClient(), readTimeoutMs);
        this.worker = new WorkerImpl(root, auth, readTimeoutMs);
        this.schemas = new SchemasImpl(root, auth, readTimeoutMs);
        this.bookies = new BookiesImpl(root, auth, readTimeoutMs);

        if (originalCtxLoader != null) {
            Thread.currentThread().setContextClassLoader(originalCtxLoader);
        }
    }

    /**
     * Construct a new Pulsar Admin client object.
     * <p/>
     * This client object can be used to perform many subsquent API calls
     *
     * @param serviceUrl
     *            the Pulsar service URL (eg. "http://my-broker.example.com:8080")
     * @param auth
     *            the Authentication object to be used to talk with Pulsar
     * @deprecated Since 2.0. Use {@link #builder()} to construct a new {@link PulsarAdmin} instance.
     */
    @Deprecated
    public PulsarAdmin(URL serviceUrl, Authentication auth) throws PulsarClientException {
        this(serviceUrl.toString(), getConfigData(auth));
    }

    private static ClientConfigurationData getConfigData(Authentication auth) {
        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setAuthentication(auth);
        return conf;
    }

    /**
     * Construct a new Pulsar Admin client object.
     * <p/>
     * This client object can be used to perform many subsquent API calls
     *
     * @param serviceUrl
     *            the Pulsar URL (eg. "http://my-broker.example.com:8080")
     * @param authPluginClassName
     *            name of the Authentication-Plugin you want to use
     * @param authParamsString
     *            string which represents parameters for the Authentication-Plugin, e.g., "key1:val1,key2:val2"
     * @deprecated Since 2.0. Use {@link #builder()} to construct a new {@link PulsarAdmin} instance.
     */
    @Deprecated
    public PulsarAdmin(URL serviceUrl, String authPluginClassName, String authParamsString)
            throws PulsarClientException {
        this(serviceUrl, AuthenticationFactory.create(authPluginClassName, authParamsString));
    }

    /**
     * Construct a new Pulsar Admin client object.
     * <p/>
     * This client object can be used to perform many subsquent API calls
     *
     * @param serviceUrl
     *            the Pulsar URL (eg. "http://my-broker.example.com:8080")
     * @param authPluginClassName
     *            name of the Authentication-Plugin you want to use
     * @param authParams
     *            map which represents parameters for the Authentication-Plugin
     * @deprecated Since 2.0. Use {@link #builder()} to construct a new {@link PulsarAdmin} instance.
     */
    @Deprecated
    public PulsarAdmin(URL serviceUrl, String authPluginClassName, Map<String, String> authParams)
            throws PulsarClientException {
        this(serviceUrl, AuthenticationFactory.create(authPluginClassName, authParams));
    }

    /**
     * @return the clusters management object
     */
    public Clusters clusters() {
        return clusters;
    }

    /**
     * @return the brokers management object
     */
    public Brokers brokers() {
        return brokers;
    }

    /**
     * @return the tenants management object
     */
    public Tenants tenants() {
        return tenants;
    }

    /**
     *
     * @deprecated since 2.0. See {@link #tenants()}
     */
    @Deprecated
    public Properties properties() {
        return properties;
    }

    /**
     * @return the namespaces management object
     */
    public Namespaces namespaces() {
        return namespaces;
    }

    public Topics topics() {
        return topics;
    }

    /**
     * @return the bookies management object
     */
    public Bookies bookies() {
        return bookies;
    }

    /**
     * @return the persistentTopics management object
     * @deprecated Since 2.0. See {@link #topics()}
     */
    @Deprecated
    public NonPersistentTopics nonPersistentTopics() {
        return nonPersistentTopics;
    }

    /**
     * @return the resource quota management object
     */
    public ResourceQuotas resourceQuotas() {
        return resourceQuotas;
    }

    /**
     * @return does a looks up for the broker serving the topic
     */
    public Lookup lookups() {
        return lookups;
    }

    /**
     *
     * @return the functions management object
     */
    public Functions functions() {
        return functions;
    }

    /**
     * @return the sources management object
     * @deprecated in favor of {@link #sources()}
     */
    @Deprecated
    public Source source() {
        return (Source) sources;
    }

    public Sources sources() {
        return sources;
    }

    /**
     * @return the sinks management object
     * @deprecated in favor of {@link #sinks}
     */
    @Deprecated
    public Sink sink() {
        return (Sink) sinks;
    }

    /**
     * @return the sinks management object
     */
    public Sinks sinks() {
        return sinks;
    }

    /**
    * @return the Worker stats
    */
   public Worker worker() {
       return worker;
   }

    /**
     * @return the broker statics
     */
    public BrokerStats brokerStats() {
        return brokerStats;
    }

    /**
     * @return the proxy statics
     */
    public ProxyStats proxyStats() {
        return proxyStats;
    }

    /**
     * @return the service HTTP URL that is being used
     */
    public String getServiceUrl() {
        return serviceUrl;
    }

    /**
     * @return the client Configuration Data that is being used
     */
    public ClientConfigurationData getClientConfigData() {
        return clientConfigData;
    }

    /**
     * @return the schemas
     */
    public Schemas schemas() {
        return schemas;
    }

    /**
     * Close the Pulsar admin client to release all the resources.
     */
    @Override
    public void close() {
        try {
            if (auth != null) {
                auth.close();
            }
        } catch (IOException e) {
            LOG.error("Failed to close the authentication service", e);
        }
        client.close();

        asyncHttpConnector.close();
    }
}
