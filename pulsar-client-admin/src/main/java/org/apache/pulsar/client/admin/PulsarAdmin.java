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
import java.security.cert.X509Certificate;
import java.util.Map;

import javax.net.ssl.SSLContext;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;

import org.apache.pulsar.client.admin.internal.BrokerStatsImpl;
import org.apache.pulsar.client.admin.internal.BrokersImpl;
import org.apache.pulsar.client.admin.internal.ClustersImpl;
import org.apache.pulsar.client.admin.internal.JacksonConfigurator;
import org.apache.pulsar.client.admin.internal.LookupImpl;
import org.apache.pulsar.client.admin.internal.NamespacesImpl;
import org.apache.pulsar.client.admin.internal.PersistentTopicsImpl;
import org.apache.pulsar.client.admin.internal.PropertiesImpl;
import org.apache.pulsar.client.admin.internal.ResourceQuotasImpl;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.ClientConfiguration;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.auth.AuthenticationDisabled;
import org.apache.pulsar.common.util.SecurityUtility;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

/**
 * Pulsar client admin API client.
 */
public class PulsarAdmin implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(PulsarAdmin.class);

    private final Clusters clusters;
    private final Brokers brokers;
    private final BrokerStats brokerStats;
    private final Properties properties;
    private final Namespaces namespaces;
    private final PersistentTopics persistentTopics;
    private final ResourceQuotas resourceQuotas;

    private final Client client;
    private final URL serviceUrl;
    private final WebTarget web;
    private final Lookup lookups;
    private final Authentication auth;

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
     * Construct a new Pulsar Admin client object.
     * <p>
     * This client object can be used to perform many subsquent API calls
     *
     * @param serviceUrl
     *            the Pulsar service URL (eg. "http://my-broker.example.com:8080")
     * @param pulsarConfig
     *            the ClientConfiguration object to be used to talk with Pulsar
     */
    public PulsarAdmin(URL serviceUrl, ClientConfiguration pulsarConfig) throws PulsarClientException {
        this.auth = pulsarConfig != null ? pulsarConfig.getAuthentication() : new AuthenticationDisabled();
        LOG.debug("created: serviceUrl={}, authMethodName={}", serviceUrl,
                auth != null ? auth.getAuthMethodName() : null);

        if (auth != null) {
            auth.start();
        }

        ClientConfig httpConfig = new ClientConfig();
        httpConfig.property(ClientProperties.FOLLOW_REDIRECTS, true);
        httpConfig.property(ClientProperties.ASYNC_THREADPOOL_SIZE, 8);

        ClientBuilder clientBuilder = ClientBuilder.newBuilder().withConfig(httpConfig)
                .register(JacksonConfigurator.class).register(JacksonFeature.class);

        if (pulsarConfig != null && pulsarConfig.isUseTls()) {
            try {
                SSLContext sslCtx = null;

                X509Certificate trustCertificates[] = SecurityUtility
                        .loadCertificatesFromPemFile(pulsarConfig.getTlsTrustCertsFilePath());

                // Set private key and certificate if available
                AuthenticationDataProvider authData = auth.getAuthData();
                if (authData.hasDataForTls()) {
                    sslCtx = SecurityUtility.createSslContext(pulsarConfig.isTlsAllowInsecureConnection(),
                            trustCertificates, authData.getTlsCertificates(), authData.getTlsPrivateKey());
                } else {
                    sslCtx = SecurityUtility.createSslContext(pulsarConfig.isTlsAllowInsecureConnection(),
                            trustCertificates);
                }

                clientBuilder.sslContext(sslCtx);
            } catch (Exception e) {
                try {
                    if (auth != null) {
                        auth.close();
                    }
                } catch (IOException ioe) {
                    LOG.error("Failed to close the authentication service", ioe);
                }
                throw new PulsarClientException.InvalidConfigurationException(e.getMessage());
            }
        }

        this.client = clientBuilder.build();

        this.serviceUrl = serviceUrl;
        WebTarget root = client.target(serviceUrl.toString());
        web = root.path("/admin");

        this.clusters = new ClustersImpl(web, auth);
        this.brokers = new BrokersImpl(web, auth);
        this.brokerStats = new BrokerStatsImpl(web, auth);
        this.properties = new PropertiesImpl(web, auth);
        this.namespaces = new NamespacesImpl(web, auth);
        this.persistentTopics = new PersistentTopicsImpl(web, auth);
        this.resourceQuotas = new ResourceQuotasImpl(web, auth);
        this.lookups = new LookupImpl(root, auth, pulsarConfig.isUseTls());
    }

    /**
     * Construct a new Pulsar Admin client object.
     * <p>
     * This client object can be used to perform many subsquent API calls
     *
     * @param serviceUrl
     *            the Pulsar service URL (eg. "http://my-broker.example.com:8080")
     * @param auth
     *            the Authentication object to be used to talk with Pulsar
     */
    public PulsarAdmin(URL serviceUrl, Authentication auth) throws PulsarClientException {
        this(serviceUrl, new ClientConfiguration() {
            private static final long serialVersionUID = 1L;
            {
                setAuthentication(auth);
            }
        });
    }

    /**
     * Construct a new Pulsar Admin client object.
     * <p>
     * This client object can be used to perform many subsquent API calls
     *
     * @param serviceUrl
     *            the Pulsar URL (eg. "http://my-broker.example.com:8080")
     * @param authPluginClassName
     *            name of the Authentication-Plugin you want to use
     * @param authParamsString
     *            string which represents parameters for the Authentication-Plugin, e.g., "key1:val1,key2:val2"
     */
    public PulsarAdmin(URL serviceUrl, String authPluginClassName, String authParamsString) throws PulsarClientException {
        this(serviceUrl, AuthenticationFactory.create(authPluginClassName, authParamsString));
    }

    /**
     * Construct a new Pulsar Admin client object.
     * <p>
     * This client object can be used to perform many subsquent API calls
     *
     * @param serviceUrl
     *            the Pulsar URL (eg. "http://my-broker.example.com:8080")
     * @param authPluginClassName
     *            name of the Authentication-Plugin you want to use
     * @param authParams
     *            map which represents parameters for the Authentication-Plugin
     */
    public PulsarAdmin(URL serviceUrl, String authPluginClassName, Map<String, String> authParams) throws PulsarClientException {
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
     * @return the properties management object
     */
    public Properties properties() {
        return properties;
    }

    /**
     * @return the namespaces management object
     */
    public Namespaces namespaces() {
        return namespaces;
    }

    /**
     * @return the persistentTopics management object
     */
    public PersistentTopics persistentTopics() {
        return persistentTopics;
    }

    /**
     * @return the resource quota management object
     */
    public ResourceQuotas resourceQuotas() {
        return resourceQuotas;
    }
    
    /**
     * @return does a looks up for the broker serving the destination
     */
    public Lookup lookups() {
        return lookups;
    }
    
    /**
     * @return the broker statics
     */
    public BrokerStats brokerStats() {
        return brokerStats;
    }

    /**
     * @return the service URL that is being used
     */
    public URL getServiceUrl() {
        return serviceUrl;
    }

    /**
     * Close the Pulsar admin client to release all the resources
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
    }
}
