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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.servlet.ServletException;
import javax.websocket.DeploymentException;

import lombok.Setter;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.broker.authorization.AuthorizationService;
import org.apache.pulsar.broker.cache.ConfigurationCacheService;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashMap;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashSet;
import org.apache.pulsar.websocket.service.WebSocketProxyConfiguration;
import org.apache.pulsar.websocket.stats.ProxyStats;
import org.apache.pulsar.zookeeper.GlobalZooKeeperCache;
import org.apache.pulsar.zookeeper.ZooKeeperCache;
import org.apache.pulsar.zookeeper.ZooKeeperClientFactory;
import org.apache.pulsar.zookeeper.ZookeeperClientFactoryImpl;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.util.concurrent.DefaultThreadFactory;

/**
 * Socket proxy server which initializes other dependent services and starts server by opening web-socket end-point url.
 *
 */
public class WebSocketService implements Closeable {

    AuthenticationService authenticationService;
    AuthorizationService authorizationService;
    PulsarClient pulsarClient;

    private final ScheduledExecutorService executor = Executors
            .newScheduledThreadPool(WebSocketProxyConfiguration.WEBSOCKET_SERVICE_THREADS,
                    new DefaultThreadFactory("pulsar-websocket"));
    private final OrderedScheduler orderedExecutor = OrderedScheduler.newSchedulerBuilder()
            .numThreads(WebSocketProxyConfiguration.GLOBAL_ZK_THREADS).name("pulsar-websocket-ordered").build();
    private GlobalZooKeeperCache globalZkCache;
    private ZooKeeperClientFactory zkClientFactory;
    private ServiceConfiguration config;
    private ConfigurationCacheService configurationCacheService;

    @Setter
    private ClusterData localCluster;
    private final ConcurrentOpenHashMap<String, ConcurrentOpenHashSet<ProducerHandler>> topicProducerMap;
    private final ConcurrentOpenHashMap<String, ConcurrentOpenHashSet<ConsumerHandler>> topicConsumerMap;
    private final ConcurrentOpenHashMap<String, ConcurrentOpenHashSet<ReaderHandler>> topicReaderMap;
    private final ProxyStats proxyStats;

    public WebSocketService(WebSocketProxyConfiguration config) {
        this(createClusterData(config), PulsarConfigurationLoader.convertFrom(config));
    }

    public WebSocketService(ClusterData localCluster, ServiceConfiguration config) {
        this.config = config;
        this.localCluster = localCluster;
        this.topicProducerMap = new ConcurrentOpenHashMap<>();
        this.topicConsumerMap = new ConcurrentOpenHashMap<>();
        this.topicReaderMap = new ConcurrentOpenHashMap<>();
        this.proxyStats = new ProxyStats(this);
    }

    public void start() throws PulsarServerException, PulsarClientException, MalformedURLException, ServletException,
            DeploymentException {

        if (isNotBlank(config.getConfigurationStoreServers())) {
            this.globalZkCache = new GlobalZooKeeperCache(getZooKeeperClientFactory(),
                    (int) config.getZooKeeperSessionTimeoutMillis(),
                    (int) TimeUnit.MILLISECONDS.toSeconds(config.getZooKeeperSessionTimeoutMillis()),
                    config.getConfigurationStoreServers(), this.orderedExecutor, this.executor,
                    config.getZooKeeperCacheExpirySeconds());
            try {
                this.globalZkCache.start();
            } catch (IOException e) {
                throw new PulsarServerException(e);
            }
            this.configurationCacheService = new ConfigurationCacheService(getGlobalZkCache());
            log.info("Global Zookeeper cache started");
        }

        // start authorizationService
        if (config.isAuthorizationEnabled()) {
            if (configurationCacheService == null) {
                throw new PulsarServerException(
                        "Failed to initialize authorization manager due to empty ConfigurationStoreServers");
            }
            authorizationService = new AuthorizationService(this.config, configurationCacheService);
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

    public AuthorizationService getAuthorizationService() {
        return authorizationService;
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
        ClientBuilder clientBuilder = PulsarClient.builder() //
                .statsInterval(0, TimeUnit.SECONDS) //
                .enableTls(config.isTlsEnabled()) //
                .allowTlsInsecureConnection(config.isTlsAllowInsecureConnection()) //
                .tlsTrustCertsFilePath(config.getBrokerClientTrustCertsFilePath()) //
                .ioThreads(config.getWebSocketNumIoThreads()) //
                .connectionsPerBroker(config.getWebSocketConnectionsPerBroker());

        if (isNotBlank(config.getBrokerClientAuthenticationPlugin())
                && isNotBlank(config.getBrokerClientAuthenticationParameters())) {
            clientBuilder.authentication(config.getBrokerClientAuthenticationPlugin(),
                    config.getBrokerClientAuthenticationParameters());
        }

        if (config.isBrokerClientTlsEnabled()) {
			if (isNotBlank(clusterData.getBrokerServiceUrlTls())) {
					clientBuilder.serviceUrl(clusterData.getBrokerServiceUrlTls());
			} else if (isNotBlank(clusterData.getServiceUrlTls())) {
					clientBuilder.serviceUrl(clusterData.getServiceUrlTls());
			}
        } else if (isNotBlank(clusterData.getBrokerServiceUrl())) {
            clientBuilder.serviceUrl(clusterData.getBrokerServiceUrl());
        } else {
            clientBuilder.serviceUrl(clusterData.getServiceUrl());
        }

        return clientBuilder.build();
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

    private ClusterData retrieveClusterData() throws PulsarServerException {
        if (configurationCacheService == null) {
            throw new PulsarServerException(
                "Failed to retrieve Cluster data due to empty ConfigurationStoreServers");
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
        return topicProducerMap
                .computeIfAbsent(producer.getProducer().getTopic(), topic -> new ConcurrentOpenHashSet<>())
                .add(producer);
    }

    public ConcurrentOpenHashMap<String, ConcurrentOpenHashSet<ProducerHandler>> getProducers() {
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
        return topicConsumerMap
                .computeIfAbsent(consumer.getConsumer().getTopic(), topic -> new ConcurrentOpenHashSet<>())
                .add(consumer);
    }

    public ConcurrentOpenHashMap<String, ConcurrentOpenHashSet<ConsumerHandler>> getConsumers() {
        return topicConsumerMap;
    }

    public boolean removeConsumer(ConsumerHandler consumer) {
        final String topicName = consumer.getConsumer().getTopic();
        if (topicConsumerMap.containsKey(topicName)) {
            return topicConsumerMap.get(topicName).remove(consumer);
        }
        return false;
    }

    public boolean addReader(ReaderHandler reader) {
        return topicReaderMap.computeIfAbsent(reader.getConsumer().getTopic(), topic -> new ConcurrentOpenHashSet<>())
                .add(reader);
    }

    public ConcurrentOpenHashMap<String, ConcurrentOpenHashSet<ReaderHandler>> getReaders() {
        return topicReaderMap;
    }

    public boolean removeReader(ReaderHandler reader) {
        final String topicName = reader.getConsumer().getTopic();
        if (topicReaderMap.containsKey(topicName)) {
            return topicReaderMap.get(topicName).remove(reader);
        }
        return false;
    }

    public ServiceConfiguration getConfig() {
        return config;
    }

    private static final Logger log = LoggerFactory.getLogger(WebSocketService.class);
}
