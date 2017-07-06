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
package org.apache.pulsar.websocket;

import static org.apache.commons.lang3.StringUtils.isNotBlank;

import java.io.Closeable;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.servlet.ServletException;
import javax.websocket.DeploymentException;

import org.apache.bookkeeper.util.OrderedSafeExecutor;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.broker.authorization.AuthorizationManager;
import org.apache.pulsar.broker.cache.ConfigurationCacheService;
import org.apache.pulsar.client.api.ClientConfiguration;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashMap;
import org.apache.pulsar.websocket.service.WebSocketProxyConfiguration;
import org.apache.pulsar.websocket.stats.ProxyStats;
import org.apache.pulsar.zookeeper.GlobalZooKeeperCache;
import org.apache.pulsar.zookeeper.ZooKeeperCache;
import org.apache.pulsar.zookeeper.ZooKeeperClientFactory;
import org.apache.pulsar.zookeeper.ZookeeperClientFactoryImpl;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.internal.Lists;

import io.netty.util.concurrent.DefaultThreadFactory;

/**
 * Socket proxy server which initializes other dependent services and starts server by opening web-socket end-point url.
 *
 */
public class WebSocketService implements Closeable {

    public static final int MaxTextFrameSize = 1024 * 1024;

    AuthenticationService authenticationService;
    AuthorizationManager authorizationManager;
    PulsarClient pulsarClient;

    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(
            WebSocketProxyConfiguration.WEBSOCKET_SERVICE_THREADS, new DefaultThreadFactory("pulsar-websocket"));
    private final OrderedSafeExecutor orderedExecutor = new OrderedSafeExecutor(
            WebSocketProxyConfiguration.GLOBAL_ZK_THREADS, "pulsar-websocket-ordered");
    private GlobalZooKeeperCache globalZkCache;
    private ZooKeeperClientFactory zkClientFactory;
    private ServiceConfiguration config;
    private ConfigurationCacheService configurationCacheService;

    private ClusterData localCluster;
    private final ConcurrentOpenHashMap<String, List<ProducerHandler>> topicProducerMap;
    private final ConcurrentOpenHashMap<String, List<ConsumerHandler>> topicConsumerMap;
    private final ProxyStats proxyStats;

    public WebSocketService(WebSocketProxyConfiguration config) {
        this(createClusterData(config), createServiceConfiguration(config));
    }

    public WebSocketService(ClusterData localCluster, ServiceConfiguration config) {
        this.config = config;
        this.localCluster = localCluster;
        this.topicProducerMap = new ConcurrentOpenHashMap<>();
        this.topicConsumerMap = new ConcurrentOpenHashMap<>();
        this.proxyStats = new ProxyStats(this);
    }

    public void start() throws PulsarServerException, PulsarClientException, MalformedURLException, ServletException,
            DeploymentException {

        if (isNotBlank(config.getGlobalZookeeperServers())) {
            this.globalZkCache = new GlobalZooKeeperCache(getZooKeeperClientFactory(),
                    (int) config.getZooKeeperSessionTimeoutMillis(), config.getGlobalZookeeperServers(),
                    this.orderedExecutor, this.executor);
            try {
                this.globalZkCache.start();
            } catch (IOException e) {
                throw new PulsarServerException(e);
            }
            this.configurationCacheService = new ConfigurationCacheService(getGlobalZkCache());
            log.info("Global Zookeeper cache started");
        }

        // start authorizationManager
        if (config.isAuthorizationEnabled()) {
            if (configurationCacheService == null) {
                throw new PulsarServerException(
                        "Failed to initialize authorization manager due to empty GlobalZookeeperServers");
            }
            authorizationManager = new AuthorizationManager(this.config, configurationCacheService);
        }
        // start authentication service
        authenticationService = new AuthenticationService(this.config);
        log.info("Pulsar WebSocket Service started");
    }

    @Override
    public void close() throws IOException {
        if (pulsarClient != null) {
            pulsarClient.close();
        }

        if (authenticationService != null) {
            authenticationService.close();
        }

        if (globalZkCache != null) {
            globalZkCache.close();
        }

        executor.shutdown();
        orderedExecutor.shutdown();
    }

    public AuthenticationService getAuthenticationService() {
        return authenticationService;
    }

    public AuthorizationManager getAuthorizationManager() {
        return authorizationManager;
    }

    public ZooKeeperCache getGlobalZkCache() {
        return globalZkCache;
    }

    public ZooKeeperClientFactory getZooKeeperClientFactory() {
        if (zkClientFactory == null) {
            zkClientFactory = new ZookeeperClientFactoryImpl();
        }
        // Return default factory
        return zkClientFactory;
    }

    public synchronized PulsarClient getPulsarClient() throws IOException {
        // Do lazy initialization of client
        if (pulsarClient == null) {
            if (localCluster == null) {
                // If not explicitly set, read clusters data from ZK
                localCluster = retrieveClusterData();
            }
            pulsarClient = createClientInstance(localCluster);
        }
        return pulsarClient;
    }

    private PulsarClient createClientInstance(ClusterData clusterData) throws IOException {
        ClientConfiguration clientConf = new ClientConfiguration();
        clientConf.setStatsInterval(0, TimeUnit.SECONDS);
        clientConf.setUseTls(config.isTlsEnabled());
        clientConf.setTlsAllowInsecureConnection(config.isTlsAllowInsecureConnection());
        clientConf.setTlsTrustCertsFilePath(config.getTlsTrustCertsFilePath());
        clientConf.setIoThreads(config.getWebSocketNumIoThreads());
        clientConf.setConnectionsPerBroker(config.getWebSocketConnectionsPerBroker());

        if (config.isAuthenticationEnabled()) {
            clientConf.setAuthentication(config.getBrokerClientAuthenticationPlugin(),
                    config.getBrokerClientAuthenticationParameters());
        }

        if (config.isTlsEnabled()) {
            if (isNotBlank(clusterData.getBrokerServiceUrlTls())) {
                return PulsarClient.create(clusterData.getBrokerServiceUrlTls(), clientConf);
            } else if (isNotBlank(clusterData.getServiceUrlTls())) {
                return PulsarClient.create(clusterData.getServiceUrlTls(), clientConf);
            }
        } else if (isNotBlank(clusterData.getBrokerServiceUrl())) {
            return PulsarClient.create(clusterData.getBrokerServiceUrl(), clientConf);
        }
        return PulsarClient.create(clusterData.getServiceUrl(), clientConf);
    }

    private static ClusterData createClusterData(WebSocketProxyConfiguration config) {
        if (isNotBlank(config.getBrokerServiceUrl()) || isNotBlank(config.getBrokerServiceUrlTls())) {
            return new ClusterData(config.getServiceUrl(), config.getServiceUrlTls(), config.getBrokerServiceUrl(),
                    config.getBrokerServiceUrlTls());
        } else if (isNotBlank(config.getServiceUrl()) || isNotBlank(config.getServiceUrlTls())) {
            return new ClusterData(config.getServiceUrl(), config.getServiceUrlTls());
        } else {
            return null;
        }
    }

    private static ServiceConfiguration createServiceConfiguration(WebSocketProxyConfiguration config) {
        ServiceConfiguration serviceConfig = new ServiceConfiguration();
        serviceConfig.setClusterName(config.getClusterName());
        serviceConfig.setWebServicePort(config.getWebServicePort());
        serviceConfig.setWebServicePortTls(config.getWebServicePortTls());
        serviceConfig.setAuthenticationEnabled(config.isAuthenticationEnabled());
        serviceConfig.setAuthenticationProviders(config.getAuthenticationProviders());
        serviceConfig.setBrokerClientAuthenticationPlugin(config.getBrokerClientAuthenticationPlugin());
        serviceConfig.setBrokerClientAuthenticationParameters(config.getBrokerClientAuthenticationParameters());
        serviceConfig.setAuthorizationEnabled(config.isAuthorizationEnabled());
        serviceConfig.setAuthorizationAllowWildcardsMatching(config.getAuthorizationAllowWildcardsMatching());
        serviceConfig.setSuperUserRoles(config.getSuperUserRoles());
        serviceConfig.setGlobalZookeeperServers(config.getGlobalZookeeperServers());
        serviceConfig.setZooKeeperSessionTimeoutMillis(config.getZooKeeperSessionTimeoutMillis());
        serviceConfig.setTlsEnabled(config.isTlsEnabled());
        serviceConfig.setTlsTrustCertsFilePath(config.getTlsTrustCertsFilePath());
        serviceConfig.setTlsCertificateFilePath(config.getTlsCertificateFilePath());
        serviceConfig.setTlsKeyFilePath(config.getTlsKeyFilePath());
        serviceConfig.setTlsAllowInsecureConnection(config.isTlsAllowInsecureConnection());
        serviceConfig.setWebSocketNumIoThreads(config.getNumIoThreads());
        serviceConfig.setWebSocketConnectionsPerBroker(config.getConnectionsPerBroker());
        return serviceConfig;
    }

    private ClusterData retrieveClusterData() throws PulsarServerException {
        if (configurationCacheService == null) {
            throw new PulsarServerException("Failed to retrieve Cluster data due to empty GlobalZookeeperServers");
        }
        try {
            String path = "/admin/clusters/" + config.getClusterName();
            return localCluster = configurationCacheService.clustersCache().get(path)
                    .orElseThrow(() -> new KeeperException.NoNodeException(path));
        } catch (Exception e) {
            throw new PulsarServerException(e);
        }
    }

    public ProxyStats getProxyStats() {
        return proxyStats;
    }

    public ConfigurationCacheService getConfigurationCache() {
        return configurationCacheService;
    }

    public ScheduledExecutorService getExecutor() {
        return executor;
    }

    public boolean isAuthenticationEnabled() {
        if (this.config == null)
            return false;
        return this.config.isAuthenticationEnabled();
    }

    public boolean isAuthorizationEnabled() {
        if (this.config == null)
            return false;
        return this.config.isAuthorizationEnabled();
    }

    public boolean addProducer(ProducerHandler producer) {
        return topicProducerMap.computeIfAbsent(producer.getProducer().getTopic(), topic -> Lists.newArrayList())
                .add(producer);
    }

    public ConcurrentOpenHashMap<String, List<ProducerHandler>> getProducers() {
        return topicProducerMap;
    }

    public boolean removeProducer(ProducerHandler producer) {
        final String topicName = producer.getProducer().getTopic();
        if (topicProducerMap.containsKey(topicName)) {
            return topicProducerMap.get(topicName).remove(producer);
        }
        return false;
    }

    public boolean addConsumer(ConsumerHandler consumer) {
        return topicConsumerMap.computeIfAbsent(consumer.getConsumer().getTopic(), topic -> Lists.newArrayList())
                .add(consumer);
    }

    public ConcurrentOpenHashMap<String, List<ConsumerHandler>> getConsumers() {
        return topicConsumerMap;
    }

    public boolean removeConsumer(ConsumerHandler consumer) {
        final String topicName = consumer.getConsumer().getTopic();
        if (topicConsumerMap.containsKey(topicName)) {
            return topicConsumerMap.get(topicName).remove(consumer);
        }
        return false;
    }

    public ServiceConfiguration getConfig() {
        return config;
    }

    private static final Logger log = LoggerFactory.getLogger(WebSocketService.class);
}
