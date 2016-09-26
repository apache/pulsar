/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.pulsar.websocket;

import java.io.Closeable;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.servlet.ServletException;
import javax.websocket.DeploymentException;

import org.apache.bookkeeper.util.OrderedSafeExecutor;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yahoo.pulsar.broker.PulsarServerException;
import com.yahoo.pulsar.broker.ServiceConfiguration;
import com.yahoo.pulsar.broker.authentication.AuthenticationService;
import com.yahoo.pulsar.broker.authorization.AuthorizationManager;
import com.yahoo.pulsar.broker.cache.ConfigurationCacheService;
import com.yahoo.pulsar.client.api.ClientConfiguration;
import com.yahoo.pulsar.client.api.PulsarClient;
import com.yahoo.pulsar.client.api.PulsarClientException;
import com.yahoo.pulsar.common.policies.data.ClusterData;
import com.yahoo.pulsar.zookeeper.GlobalZooKeeperCache;
import com.yahoo.pulsar.zookeeper.ZooKeeperCache;
import com.yahoo.pulsar.zookeeper.ZooKeeperClientFactory;
import com.yahoo.pulsar.zookeeper.ZookeeperClientFactoryImpl;

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

    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(20,
            new DefaultThreadFactory("pulsar-websocket"));
    private final OrderedSafeExecutor orderedExecutor = new OrderedSafeExecutor(8, "pulsar-websocket-ordered");
    private GlobalZooKeeperCache globalZkCache;
    private ZooKeeperClientFactory zkClientFactory;
    private ServiceConfiguration config;
    private ConfigurationCacheService configurationCacheService;

    private ClusterData localCluster;

    public WebSocketService(ServiceConfiguration config) throws PulsarClientException, MalformedURLException,
            ServletException, DeploymentException, PulsarServerException {
        this(null, config);
    }

    public WebSocketService(ClusterData localCluster, ServiceConfiguration config) throws PulsarClientException,
            MalformedURLException, ServletException, DeploymentException, PulsarServerException {
        this.config = config;
        this.localCluster = localCluster;
    }

    public void start() throws PulsarServerException, PulsarClientException, MalformedURLException, ServletException,
            DeploymentException {

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
        authenticationService = new AuthenticationService(this.config);
        authorizationManager = new AuthorizationManager(this.config, configurationCacheService);

        log.info("Pulsar WebSocket Service started");
    }

    @Override
    public void close() throws IOException {
        if (pulsarClient != null) {
            pulsarClient.close();
        }

        authenticationService.close();

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
        if (config.isAuthenticationEnabled()) {
            clientConf.setAuthentication(config.getBrokerClientAuthenticationPlugin(),
                    config.getBrokerClientAuthenticationParameters());
        }

        if (config.isTlsEnabled() && !clusterData.getServiceUrlTls().isEmpty()) {
            return PulsarClient.create(clusterData.getServiceUrlTls(), clientConf);
        } else {
            return PulsarClient.create(clusterData.getServiceUrl(), clientConf);
        }
    }

    private ClusterData retrieveClusterData() throws PulsarServerException {
        try {
            String path = "/admin/clusters/" + config.getClusterName();
            return localCluster = configurationCacheService.clustersCache().get(path)
                    .orElseThrow(() -> new KeeperException.NoNodeException(path));
        } catch (Exception e) {
            throw new PulsarServerException(e);
        }
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

    private static final Logger log = LoggerFactory.getLogger(WebSocketService.class);

}
