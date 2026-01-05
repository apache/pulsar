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

import static com.google.common.base.Preconditions.checkArgument;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.net.URL;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.admin.Bookies;
import org.apache.pulsar.client.admin.BrokerStats;
import org.apache.pulsar.client.admin.Brokers;
import org.apache.pulsar.client.admin.Clusters;
import org.apache.pulsar.client.admin.Functions;
import org.apache.pulsar.client.admin.Lookup;
import org.apache.pulsar.client.admin.Namespaces;
import org.apache.pulsar.client.admin.NonPersistentTopics;
import org.apache.pulsar.client.admin.Packages;
import org.apache.pulsar.client.admin.Properties;
import org.apache.pulsar.client.admin.ProxyStats;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.ResourceGroups;
import org.apache.pulsar.client.admin.ResourceQuotas;
import org.apache.pulsar.client.admin.Schemas;
import org.apache.pulsar.client.admin.Sink;
import org.apache.pulsar.client.admin.Sinks;
import org.apache.pulsar.client.admin.Source;
import org.apache.pulsar.client.admin.Sources;
import org.apache.pulsar.client.admin.Tenants;
import org.apache.pulsar.client.admin.TopicPolicies;
import org.apache.pulsar.client.admin.Topics;
import org.apache.pulsar.client.admin.Transactions;
import org.apache.pulsar.client.admin.Worker;
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

/**
 * Pulsar client admin API client.
 */
@SuppressWarnings("deprecation")
public class PulsarAdminImpl implements PulsarAdmin {
    private static final Logger LOG = LoggerFactory.getLogger(PulsarAdmin.class);

    public static final int DEFAULT_REQUEST_TIMEOUT_SECONDS = 300;

    private final Clusters clusters;
    private final Brokers brokers;
    private final BrokerStats brokerStats;
    private final ProxyStats proxyStats;
    private final Tenants tenants;
    private final ResourceGroups resourcegroups;
    private final Properties properties;
    private final Namespaces namespaces;
    private final Bookies bookies;
    private final TopicsImpl topics;
    private final TopicPolicies localTopicPolicies;
    private final TopicPolicies globalTopicPolicies;
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
    private final Packages packages;
    private final Transactions transactions;
    protected final WebTarget root;
    protected final Authentication auth;

    public PulsarAdminImpl(String serviceUrl, ClientConfigurationData clientConfigData,
                           ClassLoader clientBuilderClassLoader) throws PulsarClientException {
        checkArgument(StringUtils.isNotBlank(serviceUrl), "Service URL needs to be specified");

        this.clientConfigData = clientConfigData;
        this.auth = clientConfigData != null ? clientConfigData.getAuthentication() : new AuthenticationDisabled();
        LOG.debug("created: serviceUrl={}, authMethodName={}", serviceUrl, auth.getAuthMethodName());

        this.auth.start();

        if (clientConfigData != null && StringUtils.isBlank(clientConfigData.getServiceUrl())) {
            clientConfigData.setServiceUrl(serviceUrl);
        }

        AsyncHttpConnectorProvider asyncConnectorProvider = new AsyncHttpConnectorProvider(clientConfigData,
                clientConfigData.getAutoCertRefreshSeconds());

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
                .connectTimeout(this.clientConfigData.getConnectionTimeoutMs(), TimeUnit.MILLISECONDS)
                .readTimeout(this.clientConfigData.getReadTimeoutMs(), TimeUnit.MILLISECONDS)
                .register(JacksonConfigurator.class).register(JacksonFeature.class);

        boolean useTls = clientConfigData.getServiceUrl().startsWith("https://");

        this.client = clientBuilder.build();

        this.serviceUrl = serviceUrl;
        ServiceURI serviceUri = ServiceURI.create(serviceUrl);
        root = client.target(serviceUri.selectOne());

        this.asyncHttpConnector = asyncConnectorProvider.getConnector(
                Math.toIntExact(clientConfigData.getConnectionTimeoutMs()),
                Math.toIntExact(clientConfigData.getReadTimeoutMs()),
                Math.toIntExact(clientConfigData.getRequestTimeoutMs()),
                clientConfigData.getAutoCertRefreshSeconds());

        long requestTimeoutMs = clientConfigData.getRequestTimeoutMs();
        this.clusters = new ClustersImpl(root, auth, requestTimeoutMs);
        this.brokers = new BrokersImpl(root, auth, requestTimeoutMs);
        this.brokerStats = new BrokerStatsImpl(root, auth, requestTimeoutMs);
        this.proxyStats = new ProxyStatsImpl(root, auth, requestTimeoutMs);
        this.tenants = new TenantsImpl(root, auth, requestTimeoutMs);
        this.resourcegroups = new ResourceGroupsImpl(root, auth, requestTimeoutMs);
        this.properties = new TenantsImpl(root, auth, requestTimeoutMs);
        this.namespaces = new NamespacesImpl(root, auth, requestTimeoutMs);
        this.topics = new TopicsImpl(root, auth, requestTimeoutMs);
        this.localTopicPolicies = new TopicPoliciesImpl(root, auth, requestTimeoutMs, false);
        this.globalTopicPolicies = new TopicPoliciesImpl(root, auth, requestTimeoutMs, true);
        this.nonPersistentTopics = new NonPersistentTopicsImpl(root, auth, requestTimeoutMs);
        this.resourceQuotas = new ResourceQuotasImpl(root, auth, requestTimeoutMs);
        this.lookups = new LookupImpl(root, auth, useTls, requestTimeoutMs, topics);
        this.functions = new FunctionsImpl(root, auth, asyncHttpConnector, requestTimeoutMs);
        this.sources = new SourcesImpl(root, auth, asyncHttpConnector, requestTimeoutMs);
        this.sinks = new SinksImpl(root, auth, asyncHttpConnector, requestTimeoutMs);
        this.worker = new WorkerImpl(root, auth, requestTimeoutMs);
        this.schemas = new SchemasImpl(root, auth, requestTimeoutMs);
        this.bookies = new BookiesImpl(root, auth, requestTimeoutMs);
        this.packages = new PackagesImpl(root, auth, asyncHttpConnector, requestTimeoutMs);
        this.transactions = new TransactionsImpl(root, auth, requestTimeoutMs);

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
     *            the Pulsar service URL (eg. 'http://my-broker.example.com:8080')
     * @param auth
     *            the Authentication object to be used to talk with Pulsar
     * @deprecated Since 2.0. Use {@link #builder()} to construct a new {@link PulsarAdmin} instance.
     */
    @Deprecated
    public PulsarAdminImpl(URL serviceUrl, Authentication auth) throws PulsarClientException {
        this(serviceUrl.toString(), getConfigData(auth), null);
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
     *            the Pulsar URL (eg. 'http://my-broker.example.com:8080')
     * @param authPluginClassName
     *            name of the Authentication-Plugin you want to use
     * @param authParamsString
     *            string which represents parameters for the Authentication-Plugin, e.g., "key1:val1,key2:val2"
     * @deprecated Since 2.0. Use {@link #builder()} to construct a new {@link PulsarAdmin} instance.
     */
    @Deprecated
    public PulsarAdminImpl(URL serviceUrl, String authPluginClassName, String authParamsString)
            throws PulsarClientException {
        this(serviceUrl, AuthenticationFactory.create(authPluginClassName, authParamsString));
    }

    /**
     * Construct a new Pulsar Admin client object.
     * <p/>
     * This client object can be used to perform many subsquent API calls
     *
     * @param serviceUrl
     *            the Pulsar URL (eg. 'http://my-broker.example.com:8080')
     * @param authPluginClassName
     *            name of the Authentication-Plugin you want to use
     * @param authParams
     *            map which represents parameters for the Authentication-Plugin
     * @deprecated Since 2.0. Use {@link #builder()} to construct a new {@link PulsarAdmin} instance.
     */
    @Deprecated
    public PulsarAdminImpl(URL serviceUrl, String authPluginClassName, Map<String, String> authParams)
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
     * @return the resourcegroups management object
     */
    public ResourceGroups resourcegroups() {
        return resourcegroups;
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

    @Override
    public TopicPolicies topicPolicies() {
        return localTopicPolicies;
    }

    @Override
    public TopicPolicies topicPolicies(boolean isGlobal) {
        return isGlobal ? globalTopicPolicies : localTopicPolicies;
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
     * @return the packages management object
     */
    public Packages packages() {
        return packages;
    }

    @Override
    public Transactions transactions() {
        return transactions;
    }

    /**
     * Close the Pulsar admin client to release all the resources.
     */
    @Override
    public void close() {
        try {
            auth.close();
        } catch (IOException e) {
            LOG.error("Failed to close the authentication service", e);
        }
        client.close();

        asyncHttpConnector.close();
    }

    @VisibleForTesting
     WebTarget getRoot() {
        return root;
    }
}
