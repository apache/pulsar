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
package org.apache.pulsar.grpc;

import io.grpc.Server;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.netty.NettyServerBuilder;
import io.netty.handler.ssl.SslContext;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.broker.authorization.AuthorizationService;
import org.apache.pulsar.broker.cache.ConfigurationCacheService;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.util.SecurityUtility;
import org.apache.pulsar.grpc.service.GrpcProxyConfiguration;
import org.apache.pulsar.zookeeper.GlobalZooKeeperCache;
import org.apache.pulsar.zookeeper.ZooKeeperCache;
import org.apache.pulsar.zookeeper.ZooKeeperClientFactory;
import org.apache.pulsar.zookeeper.ZookeeperClientFactoryImpl;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.commons.lang3.StringUtils.isNotBlank;

/**
 * Socket proxy server which initializes other dependent services and starts server by opening web-socket end-point url.
 *
 */
public class GrpcProxyService implements Closeable {

    public static final int MaxTextFrameSize = 1024 * 1024;

    private AuthenticationService authenticationService;
    private AuthorizationService authorizationService;
    private PulsarClient pulsarClient;
    private Server server;

    private final ScheduledExecutorService executor = Executors
            .newScheduledThreadPool(GrpcProxyConfiguration.GRPC_SERVICE_THREADS,
                    new DefaultThreadFactory("pulsar-grpc"));
    private final OrderedScheduler orderedExecutor = OrderedScheduler.newSchedulerBuilder()
            .numThreads(GrpcProxyConfiguration.GLOBAL_ZK_THREADS).name("pulsar-grpc-ordered").build();
    private GlobalZooKeeperCache globalZkCache;
    private ZooKeeperClientFactory zkClientFactory;
    private ServiceConfiguration config;
    private ConfigurationCacheService configurationCacheService;

    private ClusterData localCluster;

    public GrpcProxyService(GrpcProxyConfiguration config) throws PulsarServerException {
        this(createClusterData(config), PulsarConfigurationLoader.convertFrom(config));
    }

    public GrpcProxyService(ClusterData localCluster, ServiceConfiguration config) throws PulsarServerException {
        this.config = config;
        this.localCluster = localCluster;
    }

    public void start() throws PulsarServerException {

        if (isNotBlank(config.getConfigurationStoreServers())) {
            this.globalZkCache = new GlobalZooKeeperCache(getZooKeeperClientFactory(),
                    (int) config.getZooKeeperSessionTimeoutMillis(), config.getConfigurationStoreServers(),
                    this.orderedExecutor, this.executor);
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

        List<ServerInterceptor> interceptors = new ArrayList<>();
        interceptors.add(new GrpcProxyServerInterceptor());

        if(config.isAuthenticationEnabled()) {
            interceptors.add(new AuthenticationServerInterceptor(authenticationService));
        }

        NettyServerBuilder serverBuilder = NettyServerBuilder.forPort(config.getGrpcServicePort())
                .addService(ServerInterceptors.intercept(new PulsarGrpcService(this), interceptors));

        if (config.isTlsEnabled()) {
            try {
                SslContext sslContext = SecurityUtility.createNettySslContextForServer(
                        config.isTlsAllowInsecureConnection(), config.getTlsTrustCertsFilePath(),
                        config.getTlsCertificateFilePath(), config.getTlsKeyFilePath(),
                        config.getTlsCiphers(), config.getTlsProtocols(),
                        config.getTlsRequireTrustedClientCertOnConnect());
                serverBuilder.sslContext(sslContext);
            } catch (GeneralSecurityException | IOException e) {
                throw new PulsarServerException(e);
            }
        }

        server = serverBuilder.build();

        // start grpc server
        try {
            server.start();
        } catch (IOException e) {
            throw new PulsarServerException("Failed to start gRPC server", e);
        }

        log.info("Pulsar Grpc Service started");
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

        if (server != null) {
            // TODO: shutdownNow ?
            server.shutdown();
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
                .ioThreads(config.getGrpcNumIoThreads()) //
                .connectionsPerBroker(config.getGrpcConnectionsPerBroker());

        if (isNotBlank(config.getBrokerClientAuthenticationPlugin())
                && isNotBlank(config.getBrokerClientAuthenticationParameters())) {
            clientBuilder.authentication(config.getBrokerClientAuthenticationPlugin(),
                    config.getBrokerClientAuthenticationParameters());
        }

        if (config.isTlsEnabled()) {
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

    private static ClusterData createClusterData(GrpcProxyConfiguration config) {
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

    public ServiceConfiguration getConfig() {
        return config;
    }

    private static final Logger log = LoggerFactory.getLogger(GrpcProxyService.class);
}
